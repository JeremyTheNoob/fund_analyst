#!/usr/bin/env python3
"""
基金经理数据加载器

数据源优先级：
  1. Supabase 全量表（fund_manager_all，Cloud 部署首选）
  2. 本地 fund_manager_current.csv（本地开发回退）

常用接口：
  get_fund_managers(symbol)     → 某只基金的现任经理列表
  get_manager_tenure(symbol)    → 最短/最长任职年限
  get_manager_info(symbol)      → 综合经理信息 dict（供 FundReport 使用）
  load_current_table()          → 返回完整 DataFrame（全量调用）

预热：
  python -m scripts.prewarm_manager   → 本地 CSV 上传到 Supabase
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

from data_loader.cache_paths import FUND_MANAGER_CURRENT, MANAGER_START_DATE

_CURRENT_TABLE_PATH = FUND_MANAGER_CURRENT
_START_DATE_TABLE_PATH = MANAGER_START_DATE

# Supabase 缓存键
_MANAGER_CACHE_KEY = "fund_manager_all"
_MANAGER_CACHE_TTL = 86_400  # 24 小时

# 模块级单例缓存（进程内复用，避免重复 IO）
_current_df: pd.DataFrame | None = None
_loaded_at: datetime | None = None
_IN_MEMORY_TTL = 3600  # 1 小时内不重复读取


def _load_current_table(force: bool = False) -> pd.DataFrame:
    """
    加载全量经理表，优先级：进程内存 → Supabase → 本地 CSV。

    Supabase 存储整张 fund_manager_current 表（~34000行，~3MB CSV），
    作为单一 cache_key fund_manager_all，Cloud 部署时不再依赖本地文件。
    """
    global _current_df, _loaded_at

    now = datetime.now()
    if (
        not force
        and _current_df is not None
        and _loaded_at is not None
        and (now - _loaded_at).total_seconds() < _IN_MEMORY_TTL
    ):
        return _current_df

    df = pd.DataFrame()

    # ── 数据源 1: Supabase 全量表 ──
    try:
        from data_loader.cache_layer import cache_get
        cached = cache_get(_MANAGER_CACHE_KEY, _MANAGER_CACHE_TTL, expect_df=True)
        if cached is not None and not cached.empty:
            df = cached
            logger.debug(f"[manager_loader] Supabase 全量表命中: {len(df):,} 条")
    except Exception as e:
        logger.debug(f"[manager_loader] Supabase 查询跳过: {e}")

    # ── 数据源 2: 本地 CSV（Supabase 未命中时回退） ──
    if df.empty and _CURRENT_TABLE_PATH.exists():
        try:
            raw = pd.read_csv(_CURRENT_TABLE_PATH, dtype=str)
            df = _normalize_df(raw)
            logger.debug(f"[manager_loader] 本地 CSV 回退: {len(df):,} 条")
        except Exception as e:
            logger.error(f"[manager_loader] 本地 CSV 加载失败: {e}")

    # 写入进程缓存
    _current_df = df
    _loaded_at = now
    return df


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """标准化经理表列类型"""
    df["基金代码"] = df["基金代码"].str.strip()
    df["经理姓名"] = df["经理姓名"].str.strip()
    for col in ("任职天数", "任职年限", "累计从业天数", "经理序号"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_current_table(force: bool = False) -> pd.DataFrame:
    """返回完整的现任经理状态表 DataFrame（公开接口）"""
    return _load_current_table(force=force)


def get_fund_managers(symbol: str) -> list[dict[str, Any]]:
    """
    返回某只基金的现任经理列表（按经理序号排序）。

    每个 dict 包含：
      - name          经理姓名
      - company       所属公司
      - start_date    上任日期（str YYYY-MM-DD，可能为空）
      - tenure_days   任职天数（int，可能为 None）
      - tenure_years  任职年限（float，可能为 None）
      - cum_days      累计从业天数（int）
      - change_type   变更类型（增聘/调整/成立至今/未知）
      - is_multi      是否为多经理基金（bool）
    """
    df = _load_current_table()
    if df.empty:
        return []

    rows = df[df["基金代码"] == symbol.strip()]
    if rows.empty:
        return []

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
        'manager_start_date': '2022-05-10',
        'tenure_years': 1.8,
        'tenure_years_max': 4.2,
        'cum_days': 2500,
        'is_multi_manager': True,
        'is_new_manager': False,
        'is_stable': False,
        'manager_risk_flag': '新任经理',
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
