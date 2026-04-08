"""
collect_bond_info.py — 从 chinamoney 批量采集债券基础信息
========================================================

数据源：中国外汇交易中心 (chinamoney.com.cn) BondDetailInfo 接口
采集内容：债券代码、债券名称、债券类型、发行日期、到期日期、期限、
          息票类型、票面利率、付息频率、发行价格、面值

核心设计：
1. 从 fund_bond_holdings 表提取所有去重债券代码和名称
2. 按债券名称在 chinamoney 搜索 → 获取 bondDefinedCode
3. 用 bondDefinedCode 查详情 → 存入 bond_info 表
4. 期限计算：使用原始到期日 + 采集日期 动态计算剩余期限

注意事项：
- chinamoney 有反爬（403），需要先访问曲线接口注册 session
- 16985 只去重债券中大量是信用债（SCP/CP/MTN/PPN），利率债约 800 只
- 同一只债券可能出现多次（不同基金持仓），按债券代码去重

使用方式：
    python3 scripts/collect_bond_info.py              # 全量采集（从基金持仓提取）
    python3 scripts/collect_bond_info.py --bond-type rate  # 仅采集利率债
    python3 scripts/collect_bond_info.py --bond-type credit  # 仅采集信用债
    python3 scripts/collect_bond_info.py --limit 50    # 限制采集数量（测试用）
    python3 scripts/collect_bond_info.py --update      # 增量更新（只采数据库中没有的）
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "fund_data.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# chinamoney API
SEARCH_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondMarketInfoList2"
DETAIL_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo"
CURVE_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/ClsYldCurvCurvGO"

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}


# ============================================================
# Session 管理（绕过 chinamoney 反爬）
# ============================================================

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """获取/创建 chinamoney session（带 cookie）"""
    global _session
    if _session is not None:
        return _session

    _session = requests.Session()
    _session.headers.update(HEADERS)

    # 先访问曲线接口注册 session cookie
    try:
        resp = _session.get(
            CURVE_URL,
            headers={"Referer": "https://www.chinamoney.com.cn/chinese/bkcurvclosedyhis/"},
            timeout=15,
        )
        logger.info(f"[session] 注册成功, cookies: {dict(resp.cookies)}")
    except Exception as e:
        logger.warning(f"[session] 注册失败: {e}")

    return _session


# ============================================================
# 数据提取
# ============================================================

def _extract_bonds_from_db(conn: sqlite3.Connection, bond_type: str | None = None) -> list[dict]:
    """
    从 fund_bond_holdings 提取去重债券列表。

    Args:
        conn: SQLite 连接
        bond_type: "rate"=仅利率债, "credit"=仅信用债, None=全部
    """
    cur = conn.execute("""
        SELECT DISTINCT "债券代码", "债券名称"
        FROM fund_bond_holdings
        ORDER BY "债券名称"
    """)
    bonds = [{"code": r[0], "name": r[1]} for r in cur.fetchall()]

    if bond_type == "rate":
        keywords = ["国债", "国开", "进出口", "农发", "央票", "地方政府", "政金"]
        bonds = [b for b in bonds if any(kw in str(b["name"]) for kw in keywords)]
    elif bond_type == "credit":
        keywords = ["国债", "国开", "进出口", "农发", "央票", "地方政府", "政金"]
        bonds = [b for b in bonds if not any(kw in str(b["name"]) for kw in keywords)]

    logger.info(f"[extract] 共 {len(bonds)} 只待采集债券 (bond_type={bond_type})")
    return bonds


def _clean_bond_code(code: str) -> str:
    """清理债券代码（去掉交易所后缀等）"""
    code = str(code).strip()
    # 去掉 .SH / .IB 等后缀
    code = re.sub(r"\.(SH|SZ|IB)$", "", code, flags=re.IGNORECASE)
    return code


def _search_bond(session: requests.Session, bond_name: str) -> str | None:
    """
    搜索债券，返回 bondDefinedCode。

    如果精确搜索无结果，尝试去掉括号和数字后缀再搜。
    """
    payload = {
        "pageNo": "1",
        "pageSize": "5",
        "bondName": bond_name,
        "bondCode": "",
        "issueEnty": "",
        "bondType": "",
        "bondSpclPrjctVrty": "",
        "couponType": "",
        "issueYear": "",
        "entyDefinedCode": "",
        "rtngShrt": "",
    }

    for _ in range(3):  # 最多重试 3 次
        try:
            resp = session.post(
                SEARCH_URL,
                data=payload,
                headers={"Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.debug(f"[search] HTTP {resp.status_code} for '{bond_name}'")
                time.sleep(2)
                continue

            data = resp.json()
            results = data.get("data", {}).get("resultList", [])
            if results:
                # 优先匹配精确名称
                for item in results:
                    if item.get("bondName") == bond_name:
                        return item["bondDefinedCode"]
                # 次优：返回第一个结果
                return results[0]["bondDefinedCode"]
            return None

        except Exception as e:
            logger.debug(f"[search] 异常 for '{bond_name}': {e}")
            time.sleep(2)

    return None


def _get_bond_detail(session: requests.Session, defined_code: str) -> dict | None:
    """
    获取债券详情。

    Returns:
        原始 bondBaseInfo dict，或 None
    """
    for _ in range(3):
        try:
            resp = session.post(
                DETAIL_URL,
                data={"bondDefinedCode": defined_code},
                headers={"Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"},
                timeout=15,
            )
            if resp.status_code == 403:
                # session 过期，重新注册
                logger.warning("[detail] 403, 重新注册 session")
                global _session
                _session = None
                session = _get_session()
                time.sleep(3)
                continue

            if resp.status_code != 200:
                time.sleep(2)
                continue

            data = resp.json()
            info = data.get("data", {}).get("bondBaseInfo")
            return info

        except Exception as e:
            logger.debug(f"[detail] 异常 for '{defined_code}': {e}")
            time.sleep(2)

    return None


def _parse_bond_period(period_str: str) -> float | None:
    """
    解析债券期限字符串为年数。

    输入示例：
        "10年" → 10.0
        "91日" → 0.25
        "182日" → 0.50
        "6月" → 0.5
        "3年" → 3.0
        "5Y" → 5.0
    """
    if not period_str or period_str == "---":
        return None

    s = period_str.strip()

    # 年
    m = re.match(r"([\d.]+)\s*年", s)
    if m:
        return float(m.group(1))

    # 月
    m = re.match(r"([\d.]+)\s*[月Mm]", s)
    if m:
        return round(float(m.group(1)) / 12, 4)

    # 日
    m = re.match(r"([\d.]+)\s*[日Dd]", s)
    if m:
        return round(float(m.group(1)) / 365, 4)

    # Y 后缀
    m = re.match(r"([\d.]+)\s*Y", s, re.IGNORECASE)
    if m:
        return float(m.group(1))

    return None


# ============================================================
# 数据库操作
# ============================================================

def _ensure_table(conn: sqlite3.Connection):
    """创建 bond_info 表（如果不存在）"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bond_info (
            -- 债券标识
            bond_code        TEXT PRIMARY KEY,      -- 债券代码（去重后，如 "240011"）
            bond_name        TEXT,                  -- 债券简称（如 "24附息国债11"）
            defined_code     TEXT,                  -- chinamoney 查询代码

            -- 基础信息（静态，不随时间变化）
            bond_type        TEXT,                  -- 债券类型（国债/国开债/企业债等）
            coupon_type      TEXT,                  -- 息票类型（附息式固定利率/贴现式等）
            bond_period      TEXT,                  -- 原始期限字符串（如 "10年"/"91日"）
            bond_period_years REAL,                 -- 期限（年，解析后数值）
            par_value        REAL,                  -- 面值
            par_coupon_rate  REAL,                  -- 票面利率（%）
            coupon_frequency TEXT,                  -- 付息频率（半年/年等）
            issue_price      REAL,                  -- 发行价格
            issuer           TEXT,                  -- 发行人

            -- 时间信息（关键：用原始到期日动态计算剩余期限）
            issue_date       TEXT,                  -- 发行日期（YYYY-MM-DD）
            maturity_date    TEXT,                  -- 到期日期（YYYY-MM-DD）
            first_value_date TEXT,                  -- 起息日

            -- 采集元数据
            collect_date     TEXT,                  -- 采集日期（YYYY-MM-DD）
            update_time      TEXT                   -- 最后更新时间（ISO）
        )
    """)
    conn.commit()


def _existing_codes(conn: sqlite3.Connection) -> set[str]:
    """获取数据库中已有的债券代码"""
    cur = conn.execute("SELECT bond_code FROM bond_info")
    return {r[0] for r in cur.fetchall()}


def _save_bond_info(conn: sqlite3.Connection, info: dict, bond_code: str, bond_name: str):
    """保存单只债券信息"""
    period_years = _parse_bond_period(info.get("bondPeriod", ""))
    now = datetime.now().isoformat()

    conn.execute("""
        INSERT OR REPLACE INTO bond_info (
            bond_code, bond_name, defined_code,
            bond_type, coupon_type, bond_period, bond_period_years,
            par_value, par_coupon_rate, coupon_frequency, issue_price,
            issuer, issue_date, maturity_date, first_value_date,
            collect_date, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        _clean_bond_code(bond_code),
        bond_name,
        info.get("bondDefinedCode"),
        info.get("bondType"),
        info.get("couponType"),
        info.get("bondPeriod"),
        period_years,
        _safe_float(info.get("parValue")),
        _safe_float(info.get("parCouponRate")),
        info.get("couponFrqncy"),
        _safe_float(info.get("issuePrice")),
        info.get("entyFullName"),
        info.get("issueDate"),
        info.get("mrtyDate"),       # 关键：原始到期日
        info.get("frstValueDate"),
        date.today().isoformat(),
        now,
    ))


def _safe_float(val) -> float | None:
    """安全转 float"""
    if val is None or val == "---" or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ============================================================
# 主流程
# ============================================================

def collect_bond_info(
    bond_type: str | None = None,
    limit: int | None = None,
    update_only: bool = False,
):
    """
    主采集流程。

    Args:
        bond_type: "rate" / "credit" / None
        limit: 最大采集数量（None=全部）
        update_only: True=只采集数据库中没有的
    """
    if not DB_PATH.exists():
        logger.error(f"数据库不存在: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_table(conn)

    # 提取待采集债券
    bonds = _extract_bonds_from_db(conn, bond_type)

    # 过滤已采集的
    if update_only:
        existing = _existing_codes(conn)
        bonds = [b for b in bonds if _clean_bond_code(b["code"]) not in existing]
        logger.info(f"[update] 过滤后剩余 {len(bonds)} 只新债券")

    if limit:
        bonds = bonds[:limit]

    if not bonds:
        logger.info("没有需要采集的债券")
        conn.close()
        return

    logger.info(f"开始采集 {len(bonds)} 只债券...")

    session = _get_session()
    success = 0
    fail = 0
    skip = 0
    batch_size = 50

    for i, bond in enumerate(bonds):
        code = _clean_bond_code(bond["code"])
        name = str(bond["name"]).strip()

        # 进度
        if (i + 1) % batch_size == 0 or i == 0:
            logger.info(f"[progress] {i+1}/{len(bonds)} (成功:{success} 失败:{fail} 跳过:{skip})")

        # 搜索
        defined_code = _search_bond(session, name)
        if not defined_code:
            skip += 1
            continue

        # 详情
        detail = _get_bond_detail(session, defined_code)
        if not detail:
            fail += 1
            continue

        # 保存
        try:
            _save_bond_info(conn, detail, code, name)
            success += 1
        except Exception as e:
            logger.error(f"[save] 保存失败 '{name}': {e}")
            fail += 1

        # 每 batch_size 只提交一次
        if (i + 1) % batch_size == 0:
            conn.commit()
            logger.info(f"[commit] 已提交 {batch_size} 条")
            time.sleep(1)  # 礼貌间隔

    conn.commit()
    conn.close()

    logger.info(
        f"\n采集完成！总计: {len(bonds)}, "
        f"成功: {success}, 失败: {fail}, 跳过(未找到): {skip}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从 chinamoney 采集债券基础信息")
    parser.add_argument("--bond-type", choices=["rate", "credit"], default=None,
                        help="债券类型过滤：rate=利率债, credit=信用债")
    parser.add_argument("--limit", type=int, default=None,
                        help="限制采集数量（测试用）")
    parser.add_argument("--update", action="store_true",
                        help="增量更新（只采集数据库中没有的）")
    args = parser.parse_args()

    collect_bond_info(
        bond_type=args.bond_type,
        limit=args.limit,
        update_only=args.update,
    )
