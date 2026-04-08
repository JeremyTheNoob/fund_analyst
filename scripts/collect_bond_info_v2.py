"""
collect_bond_info_v2.py — bond_info 全量重采脚本（增强版）
=========================================================

基于 collect_bond_info.py 优化：
1. 默认先清空 bond_info 表再全量采集
2. 每 20 只 sleep(2) 降低 403 概率
3. 搜索和详情接口都有 403 自动重注册
4. 进度每 100 只打印一次
5. 断点续传：ctrl-c 后下次运行自动跳过已采集的

使用方式：
    # 全量重采（清空后从头开始）
    python3 scripts/collect_bond_info_v2.py

    # 断点续传（跳过已有记录，从中断处继续）
    python3 scripts/collect_bond_info_v2.py --resume

    # 仅采集利率债
    python3 scripts/collect_bond_info_v2.py --bond-type rate

    # 仅采集信用债
    python3 scripts/collect_bond_info_v2.py --bond-type credit

    # 限制数量（测试用）
    python3 scripts/collect_bond_info_v2.py --limit 50
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import signal
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "fund_data.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

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

# 全局控制
_interrupted = False


def _sigint_handler(signum, frame):
    global _interrupted
    _interrupted = True
    logger.warning("\n[中断] 收到 Ctrl+C，正在保存已采集数据并退出...")


signal.signal(signal.SIGINT, _sigint_handler)


# ============================================================
# Session 管理
# ============================================================

_session: requests.Session | None = None


def _new_session() -> requests.Session:
    """创建全新 session 并注册 cookie"""
    global _session
    _session = requests.Session()
    _session.headers.update(HEADERS)
    try:
        resp = _session.get(
            CURVE_URL,
            headers={"Referer": "https://www.chinamoney.com.cn/chinese/bkcurvclosedyhis/"},
            timeout=15,
        )
        logger.info(f"[session] 新建 session, cookies={dict(resp.cookies)}")
    except Exception as e:
        logger.warning(f"[session] 注册失败: {e}")
    return _session


def _get_session() -> requests.Session:
    if _session is not None:
        return _session
    return _new_session()


def _reset_session():
    """强制重建 session（403 后调用）"""
    logger.info("[session] 403 检测，重建 session...")
    time.sleep(5)
    return _new_session()


# ============================================================
# 数据提取
# ============================================================

def _extract_bonds_from_db(conn: sqlite3.Connection, bond_type: str | None = None) -> list[dict]:
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
    code = str(code).strip()
    code = re.sub(r"\.(SH|SZ|IB)$", "", code, flags=re.IGNORECASE)
    return code


def _search_bond(session: requests.Session, bond_name: str) -> str | None:
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

    for attempt in range(3):
        if _interrupted:
            return None
        try:
            resp = session.post(
                SEARCH_URL,
                data=payload,
                headers={"Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"},
                timeout=15,
            )
            if resp.status_code == 403:
                _reset_session()
                return None  # 让外层重试

            if resp.status_code != 200:
                time.sleep(2)
                continue

            data = resp.json()
            results = data.get("data", {}).get("resultList", [])
            if results:
                for item in results:
                    if item.get("bondName") == bond_name:
                        return item["bondDefinedCode"]
                return results[0]["bondDefinedCode"]
            return None

        except Exception as e:
            logger.debug(f"[search] 异常 for '{bond_name}': {e}")
            time.sleep(2)

    return None


def _get_bond_detail(session: requests.Session, defined_code: str) -> dict | None:
    for attempt in range(3):
        if _interrupted:
            return None
        try:
            resp = session.post(
                DETAIL_URL,
                data={"bondDefinedCode": defined_code},
                headers={"Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"},
                timeout=15,
            )
            if resp.status_code == 403:
                _reset_session()
                time.sleep(2)
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
    if not period_str or period_str == "---":
        return None
    s = period_str.strip()

    m = re.match(r"([\d.]+)\s*年", s)
    if m:
        return float(m.group(1))

    m = re.match(r"([\d.]+)\s*[月Mm]", s)
    if m:
        return round(float(m.group(1)) / 12, 4)

    m = re.match(r"([\d.]+)\s*[日Dd]", s)
    if m:
        return round(float(m.group(1)) / 365, 4)

    m = re.match(r"([\d.]+)\s*Y", s, re.IGNORECASE)
    if m:
        return float(m.group(1))

    return None


# ============================================================
# 数据库
# ============================================================

def _ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bond_info (
            bond_code        TEXT PRIMARY KEY,
            bond_name        TEXT,
            defined_code     TEXT,
            bond_type        TEXT,
            coupon_type      TEXT,
            bond_period      TEXT,
            bond_period_years REAL,
            par_value        REAL,
            par_coupon_rate  REAL,
            coupon_frequency TEXT,
            issue_price      REAL,
            issuer           TEXT,
            issue_date       TEXT,
            maturity_date    TEXT,
            first_value_date TEXT,
            collect_date     TEXT,
            update_time      TEXT
        )
    """)
    conn.commit()


def _existing_codes(conn: sqlite3.Connection) -> set[str]:
    cur = conn.execute("SELECT bond_code FROM bond_info")
    return {r[0] for r in cur.fetchall()}


def _clear_table(conn: sqlite3.Connection):
    """清空 bond_info 表"""
    count = conn.execute("SELECT COUNT(*) FROM bond_info").fetchone()[0]
    conn.execute("DELETE FROM bond_info")
    conn.commit()
    logger.info(f"[clear] 已清空 bond_info 表，原有 {count} 条记录")


def _save_bond_info(conn: sqlite3.Connection, info: dict, bond_code: str, bond_name: str):
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
        info.get("mrtyDate"),
        info.get("frstValueDate"),
        date.today().isoformat(),
        now,
    ))


def _safe_float(val) -> float | None:
    if val is None or val == "---" or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ============================================================
# 主流程
# ============================================================

def collect(
    bond_type: str | None = None,
    limit: int | None = None,
    resume: bool = False,
):
    global _interrupted

    if not DB_PATH.exists():
        logger.error(f"数据库不存在: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_table(conn)

    # 清空 or 续传
    if not resume:
        _clear_table(conn)

    # 提取待采集债券
    bonds = _extract_bonds_from_db(conn, bond_type)

    # 续传模式：跳过已有
    if resume:
        existing = _existing_codes(conn)
        before = len(bonds)
        bonds = [b for b in bonds if _clean_bond_code(b["code"]) not in existing]
        logger.info(f"[resume] 已有 {len(existing)} 条，剩余 {len(bonds)} 只待采集")

    if limit:
        bonds = bonds[:limit]

    if not bonds:
        logger.info("没有需要采集的债券")
        conn.close()
        return

    total = len(bonds)
    logger.info(f"{'='*60}")
    logger.info(f"开始采集 {total} 只债券 (bond_type={bond_type}, resume={resume})")
    logger.info(f"预计耗时: {total * 2.5 / 60:.0f} 分钟（按 ~2.5秒/只 估算）")
    logger.info(f"按 Ctrl+C 可随时中断，已采集数据会保存")
    logger.info(f"{'='*60}")

    session = _get_session()
    success = 0
    fail = 0
    skip = 0
    batch_size = 20  # 每 20 只提交一次
    consecutive_403 = 0
    max_consecutive_403 = 5
    start_time = time.time()

    for i, bond in enumerate(bonds):
        if _interrupted:
            break

        code = _clean_bond_code(bond["code"])
        name = str(bond["name"]).strip()

        # 进度
        if (i + 1) % 100 == 0 or i == 0:
            elapsed = time.time() - start_time
            speed = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / speed if speed > 0 else 0
            logger.info(
                f"[{i+1}/{total}] 成功:{success} 失败:{fail} 跳过:{skip} | "
                f"速度:{speed:.1f}只/s 预计剩余:{eta/60:.0f}分钟"
            )

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

        consecutive_403 = 0  # 成功则重置

        # 保存
        try:
            _save_bond_info(conn, detail, code, name)
            success += 1
        except Exception as e:
            logger.error(f"[save] '{name}': {e}")
            fail += 1

        # 批量提交 + 速率控制
        if (i + 1) % batch_size == 0:
            conn.commit()
            time.sleep(2)

    # 最终提交
    conn.commit()
    elapsed = time.time() - start_time
    conn.close()

    logger.info(f"\n{'='*60}")
    if _interrupted:
        logger.info(f"[中断] 采集被中断。已保存 {success} 条。")
        logger.info(f"下次运行加 --resume 参数可从断点继续。")
    else:
        logger.info(f"采集完成！耗时 {elapsed/60:.1f} 分钟")
    logger.info(f"总计: {total}, 成功: {success}, 失败: {fail}, 跳过(未找到): {skip}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="bond_info 全量重采")
    parser.add_argument("--bond-type", choices=["rate", "credit"], default=None,
                        help="债券类型过滤")
    parser.add_argument("--limit", type=int, default=None,
                        help="限制采集数量（测试用）")
    parser.add_argument("--resume", action="store_true",
                        help="断点续传（跳过已有记录）")
    args = parser.parse_args()

    collect(
        bond_type=args.bond_type,
        limit=args.limit,
        resume=args.resume,
    )
