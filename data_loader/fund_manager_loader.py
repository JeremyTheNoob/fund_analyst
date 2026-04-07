#!/usr/bin/env python3
"""
基金经理数据加载器

提供两张本地表的读取和查询接口：
  1. fund_manager_current.csv  — 现任经理状态表（基金×经理）
  2. manager_start_date.csv   — 基金经理上任日期原始表

常用接口：
  get_fund_managers(symbol)     → 某只基金的现任经理列表
  get_manager_tenure(symbol)    → 最短/最长任职年限
  get_manager_info(symbol)      → 综合经理信息 dict（供 FundReport 使用）
  load_current_table()          → 返回完整 DataFrame（全量调用）

注意：本地 CSV 不存在时（如 Streamlit Cloud 部署），自动从 AkShare fallback。
"""

import logging
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

from data_loader.cache_paths import FUND_MANAGER_CURRENT, MANAGER_START_DATE

_CURRENT_TABLE_PATH = FUND_MANAGER_CURRENT
_START_DATE_TABLE_PATH = MANAGER_START_DATE


# ── AkShare Fallback（Cloud 部署时本地 CSV 不存在） ────────────────────────
_akshare_manager_cache: dict = {}   # symbol → list[dict]，进程内缓存


def _fetch_manager_from_akshare(symbol: str) -> list[dict[str, Any]]:
    """
    从 AkShare fund_manager_em 实时获取某只基金的当前经理信息。
    失败时返回空列表（不报错）。
    """
    if symbol in _akshare_manager_cache:
        return _akshare_manager_cache[symbol]

    try:
        import akshare as ak
        df = ak.fund_manager_em(symbol=symbol)
        if df is None or df.empty:
            _akshare_manager_cache[symbol] = []
            return []

        # AkShare 列名：基金经理, 任职日期, 离职日期, 任职天数
        # 只取在任（离职日期为空）的经理
        if "离职日期" in df.columns:
            df = df[df["离职日期"].isna() | (df["离职日期"] == "") | (df["离职日期"] == "至今")]

        result = []
        for i, row in df.iterrows():
            name = str(row.get("基金经理", "")).strip()
            if not name:
                continue
            start_str = str(row.get("任职日期", "")).strip()
            tenure_days_raw = row.get("任职天数", None)
            try:
                tenure_days = int(tenure_days_raw) if pd.notna(tenure_days_raw) else None
            except (ValueError, TypeError):
                tenure_days = None
            tenure_years = round(tenure_days / 365.25, 1) if tenure_days else None

            result.append({
                "name":         name,
                "company":      "",   # AkShare 此接口不含公司字段
                "start_date":   start_str,
                "tenure_days":  tenure_days,
                "tenure_years": tenure_years,
                "cum_days":     None,
                "change_type":  "未知",
                "is_multi":     len(df) > 1,
            })

        _akshare_manager_cache[symbol] = result
        return result

    except Exception as e:
        logger.warning(f"[manager_loader] AkShare fallback 失败 ({symbol}): {e}")
        _akshare_manager_cache[symbol] = []
        return []

# 模块级单例缓存（进程内复用，避免重复 IO）
_current_df: pd.DataFrame | None = None
_loaded_at: datetime | None = None
_CACHE_TTL_SECONDS = 3600  # 1小时内不重复读文件


def _load_current_table(force: bool = False) -> pd.DataFrame:
    """读取 fund_manager_current.csv，带内存 TTL 缓存"""
    global _current_df, _loaded_at

    now = datetime.now()
    if (
        not force
        and _current_df is not None
        and _loaded_at is not None
        and (now - _loaded_at).total_seconds() < _CACHE_TTL_SECONDS
    ):
        return _current_df

    if not _CURRENT_TABLE_PATH.exists():
        logger.warning(
            "fund_manager_current.csv 不存在，请先运行 "
            "python3 -m scripts.build_manager_current"
        )
        return pd.DataFrame()

    try:
        df = pd.read_csv(_CURRENT_TABLE_PATH, dtype=str)
        df["基金代码"] = df["基金代码"].str.strip()
        df["经理姓名"] = df["经理姓名"].str.strip()

        # 数值列转换
        for col in ("任职天数", "任职年限", "累计从业天数", "经理序号"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        _current_df = df
        _loaded_at = now
        logger.debug(f"fund_manager_current.csv 已加载：{len(df):,} 条")
        return df
    except Exception as e:
        logger.error(f"加载 fund_manager_current.csv 失败: {e}")
        return pd.DataFrame()


def load_current_table(force: bool = False) -> pd.DataFrame:
    """返回完整的现任经理状态表 DataFrame（公开接口）"""
    return _load_current_table(force=force)


def get_fund_managers(symbol: str) -> list[dict[str, Any]]:
    """
    返回某只基金的现任经理列表（按经理序号排序）。

    优先从本地 fund_manager_current.csv 读取；
    本地文件不存在时（如 Streamlit Cloud），fallback 到 AkShare 实时接口。

    每个 dict 包含：
      - name          经理姓名
      - company       所属公司
      - start_date    上任日期（str YYYY-MM-DD，可能为空）
      - tenure_days   任职天数（int，可能为 None）
      - tenure_years  任职年限（float，可能为 None）
      - cum_days      累计从业天数（int）
      - change_type   变更类型（增聘/调整/成立至今/未知）
      - is_multi      是否为多经理基金（bool）

    示例：
      >>> get_fund_managers("000001")
      [{'name': '刘睿聪', 'start_date': '2024-12-28', 'tenure_years': 1.3, ...},
       {'name': '郑晓辉', 'start_date': '2024-12-28', 'tenure_years': 1.3, ...}]
    """
    df = _load_current_table()
    if df.empty:
        # 本地 CSV 不存在，fallback 到 AkShare
        return _fetch_manager_from_akshare(symbol)

    rows = df[df["基金代码"] == symbol.strip()]
    if rows.empty:
        # 本地 CSV 中没有这只基金，也 fallback
        return _fetch_manager_from_akshare(symbol)

    result = []
    for _, row in rows.sort_values("经理序号").iterrows():
        result.append({
            "name":         str(row.get("经理姓名", "")).strip(),
            "company":      str(row.get("所属公司", "")).strip(),
            "start_date":   str(row.get("上任日期", "")).strip() if pd.notna(row.get("上任日期")) else "",
            "tenure_days":  int(row["任职天数"]) if pd.notna(row.get("任职天数")) else None,
            "tenure_years": float(row["任职年限"]) if pd.notna(row.get("任职年限")) else None,
            "cum_days":     int(row["累计从业天数"]) if pd.notna(row.get("累计从业天数")) else None,
            "change_type":  str(row.get("变更类型", "未知")).strip(),
            "is_multi":     str(row.get("多经理标记", "N")).strip() == "Y",
        })
    return result


def get_manager_tenure(symbol: str) -> dict[str, Any]:
    """
    返回某只基金的任职时长摘要（用于报告展示）。

    返回 dict：
      - managers        [str]  经理姓名列表
      - manager_str     str   "张三 / 李四"（展示用）
      - min_tenure_years float 最短在任年限（最近上任的那位）
      - max_tenure_years float 最长在任年限
      - is_stable        bool  最短任职 >= 3年
      - is_new           bool  最短任职 < 1年（新任经理风险）
      - is_multi         bool  是否多经理
      - start_date       str   最近上任日期（最短任职那位的上任日期）
    """
    managers = get_fund_managers(symbol)
    if not managers:
        return {
            "managers": [], "manager_str": "未知",
            "min_tenure_years": None, "max_tenure_years": None,
            "is_stable": False, "is_new": False,
            "is_multi": False, "start_date": "",
        }

    names = [m["name"] for m in managers if m["name"]]
    tenures = [m["tenure_years"] for m in managers if m["tenure_years"] is not None]

    min_t = min(tenures) if tenures else None
    max_t = max(tenures) if tenures else None

    # 最近上任的经理（任职最短 = 最新上任）
    newest = min(managers, key=lambda m: m["tenure_years"] if m["tenure_years"] is not None else 9999)

    return {
        "managers":          names,
        "manager_str":       " / ".join(names),
        "min_tenure_years":  min_t,
        "max_tenure_years":  max_t,
        "is_stable":         (min_t is not None and min_t >= 3.0),
        "is_new":            (min_t is not None and min_t < 1.0),
        "is_multi":          len(names) > 1,
        "start_date":        newest.get("start_date", ""),
    }


def get_manager_info(symbol: str) -> dict[str, Any]:
    """
    综合接口，返回经理信息 dict，供 FundReport / 报告生成器直接调用。

    示例输出：
    {
        'manager_names': ['张三', '李四'],
        'manager_str': '张三 / 李四',
        'manager_start_date': '2022-05-10',   # 最近上任日期
        'tenure_years': 1.8,                  # 最短任职（关键风险指标）
        'tenure_years_max': 4.2,              # 最长任职
        'cum_days': 2500,                     # 经验最丰富那位的累计从业天数
        'is_multi_manager': True,
        'is_new_manager': False,              # True = 最短任职 < 1 年
        'is_stable': False,                   # True = 最短任职 >= 3 年
        'manager_risk_flag': '新任经理',       # 文字风险标签
    }
    """
    tenure = get_manager_tenure(symbol)
    managers = get_fund_managers(symbol)

    cum_days = None
    if managers:
        cum_list = [m["cum_days"] for m in managers if m["cum_days"] is not None]
        cum_days = max(cum_list) if cum_list else None

    min_t = tenure["min_tenure_years"]
    if min_t is None:
        risk_flag = "任职信息待补充"
    elif min_t < 1.0:
        risk_flag = "新任经理"
    elif min_t < 3.0:
        risk_flag = "成长期"
    elif min_t < 5.0:
        risk_flag = "稳健期"
    else:
        risk_flag = "资深经理"

    return {
        "manager_names":      tenure["managers"],
        "manager_str":        tenure["manager_str"],
        "manager_start_date": tenure["start_date"],
        "tenure_years":       min_t,
        "tenure_years_max":   tenure["max_tenure_years"],
        "cum_days":           cum_days,
        "is_multi_manager":   tenure["is_multi"],
        "is_new_manager":     tenure["is_new"],
        "is_stable":          tenure["is_stable"],
        "manager_risk_flag":  risk_flag,
    }


# ── 便捷查询：按经理姓名找基金 ──────────────────────────────────

def get_manager_funds(manager_name: str) -> list[str]:
    """返回某位经理当前管理的所有基金代码列表"""
    df = _load_current_table()
    if df.empty:
        return []
    rows = df[df["经理姓名"].str.strip() == manager_name.strip()]
    return rows["基金代码"].tolist()


def search_managers(keyword: str, top_n: int = 20) -> pd.DataFrame:
    """
    按经理姓名关键词搜索，返回经理摘要 DataFrame。

    返回列：经理姓名, 所属公司, 管理基金数, 平均任职年限, 最长任职年限, 累计从业天数
    """
    df = _load_current_table()
    if df.empty:
        return pd.DataFrame()

    mask = df["经理姓名"].str.contains(keyword.strip(), na=False)
    filtered = df[mask].copy()
    if filtered.empty:
        return pd.DataFrame()

    agg = filtered.groupby(["经理姓名", "所属公司"]).agg(
        管理基金数=("基金代码", "count"),
        平均任职年限=("任职年限", "mean"),
        最长任职年限=("任职年限", "max"),
        累计从业天数=("累计从业天数", "first"),
    ).reset_index()

    agg["平均任职年限"] = agg["平均任职年限"].round(1)
    agg["最长任职年限"] = agg["最长任职年限"].round(1)

    return agg.head(top_n)
