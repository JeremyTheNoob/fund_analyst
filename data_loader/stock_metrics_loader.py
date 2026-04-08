"""
stock_metrics_loader.py — Top10 持仓个股指标加载（v3 SQLite 计算版）

获取个股的 PE(TTM)、PB、PEG、PE 分位、20日均成交额，
用于计算估值水位、PEG、流动性穿透(Ldays)等指标。

数据源（全部本地，零网络依赖）：
- stock_value 表 → PE(TTM)/PB/PEG + 历史PE分位实时计算
- stock_daily_amt CSV → 20日均成交额实时计算
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# 项目路径
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_AMT_CSV = _PROJECT_ROOT / "data" / "local_cache" / "history" / "stock_daily_amt.csv"


# ============================================================
# PE 分位计算
# ============================================================

_pe_percentile_cache: Dict[str, Optional[float]] = {}


def _ensure_pe_percentile_cache(codes: List[str]) -> None:
    """
    批量计算 PE 分位并缓存。

    对每只股票：当前 PE 在过去 3 年历史中的百分位。
    缓存是增量的——已缓存的股票不会重复计算，未缓存的会查询并缓存。
    """
    global _pe_percentile_cache

    missing = [c for c in codes if c not in _pe_percentile_cache]
    if not missing:
        return

    try:
        from data_loader.db_accessor import DB

        # 3 年前的日期
        cutoff = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")

        # 批量查询所有目标股票过去 3 年的 PE 数据
        placeholders = ",".join("?" for _ in missing)
        sql = (
            f'SELECT "股票代码", "PE(TTM)" FROM stock_value '
            f'WHERE "股票代码" IN ({placeholders}) '
            f'AND "数据日期" >= ? '
            f'AND "PE(TTM)" IS NOT NULL AND "PE(TTM)" != \'\' '
            f'ORDER BY "股票代码", "数据日期" ASC'
        )
        params = missing + [cutoff]
        df = DB.query_df(sql, params)

        if df is None or df.empty:
            for c in missing:
                _pe_percentile_cache[c] = None
            return

        # 转数值
        df["PE(TTM)"] = pd.to_numeric(df["PE(TTM)"], errors="coerce")
        df = df.dropna(subset=["PE(TTM)"])

        # 按股票分组计算分位
        for code in missing:
            sub = df[df["股票代码"] == code]
            if sub.empty or len(sub) < 60:  # 至少 3 个月数据
                _pe_percentile_cache[code] = None
                continue

            current_pe = sub["PE(TTM)"].iloc[-1]
            if current_pe <= 0:
                _pe_percentile_cache[code] = None
                continue

            # 当前 PE 在历史中的分位（值越小越低估）
            pct = (sub["PE(TTM)"] < current_pe).mean() * 100
            _pe_percentile_cache[code] = round(float(pct), 1)

    except Exception as e:
        logger.warning(f"[stock_metrics] PE 分位计算失败: {e}")
        for c in missing:
            _pe_percentile_cache.setdefault(c, None)


# ============================================================
# 20 日均成交额计算
# ============================================================

_amt_cache: Dict[str, Optional[float]] = {}
_amt_df: Optional[pd.DataFrame] = None


def _load_amt_csv() -> Optional[pd.DataFrame]:
    """加载 stock_daily_amt CSV（带缓存）"""
    global _amt_df
    if _amt_df is not None:
        return _amt_df

    if not _AMT_CSV.exists():
        logger.warning(f"[stock_metrics] 成交额文件不存在: {_AMT_CSV}")
        _amt_df = pd.DataFrame()
        return _amt_df

    try:
        # 只读取需要的列（节省内存），跳过可能的 __CSV__: 前缀行
        _amt_df = pd.read_csv(
            _AMT_CSV,
            usecols=["stock_code", "date", "amount"],
            dtype={"stock_code": str},
            low_memory=True,
            on_bad_lines="skip",
            skiprows=[0] if _AMT_CSV.stat().st_size > 0 else None,
        )
        _amt_df["amount"] = pd.to_numeric(_amt_df["amount"], errors="coerce")
        _amt_df["date"] = pd.to_datetime(_amt_df["date"], errors="coerce")
        logger.info(f"[stock_metrics] 成交额数据加载: {len(_amt_df)} 条")
    except Exception as e:
        logger.warning(f"[stock_metrics] 成交额数据加载失败: {e}")
        _amt_df = pd.DataFrame()

    return _amt_df


def _calc_avg_amount_20d(codes: List[str]) -> Dict[str, Optional[float]]:
    """
    从 stock_daily_amt CSV 计算每只股票最近 20 日均成交额（万元）。
    """
    df = _load_amt_csv()
    if df is None or df.empty:
        return {c: None for c in codes}

    result: Dict[str, Optional[float]] = {}
    cutoff = datetime.now() - timedelta(days=40)  # 留余量

    for code in codes:
        sub = df[
            (df["stock_code"] == code)
            & (df["date"] >= pd.Timestamp(cutoff))
            & (df["amount"].notna())
        ].tail(20)

        if not sub.empty and len(sub) >= 5:
            avg = sub["amount"].mean()
            # amount 单位是元，转为万元
            result[code] = round(float(avg) / 10000, 2)
        else:
            result[code] = None

    return result


# ============================================================
# 全量缓存读取（stock_value 表）
# ============================================================

_stock_metrics_cache: Optional[pd.DataFrame] = None
_stock_metrics_loaded_at: Optional[float] = None
_CACHE_TTL_SECONDS = 3600  # 内存缓存 1 小时


def _load_full_cache() -> Optional[pd.DataFrame]:
    """
    从 SQLite 加载全量个股指标（仅最新一条/每只股票）。

    Returns:
        DataFrame 或 None（列：code, pe_ttm, pb, peg, close, market_cap, date）
    """
    import time
    global _stock_metrics_cache, _stock_metrics_loaded_at

    now = time.time()
    if _stock_metrics_cache is not None and _stock_metrics_loaded_at is not None:
        if now - _stock_metrics_loaded_at < _CACHE_TTL_SECONDS:
            return _stock_metrics_cache

    try:
        from data_loader.db_accessor import DB
        df = DB.query_df("SELECT * FROM stock_value")
        if df is not None and not df.empty:
            col_map = {
                "股票代码": "code",
                "PE(TTM)": "pe_ttm",
                "市净率": "pb",
                "PEG值": "peg",
                "当日收盘价": "close",
                "总市值": "market_cap",
                "数据日期": "date",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if "code" not in df.columns:
                logger.warning(
                    f"[stock_metrics] 数据缺少 code 列，"
                    f"实际列: {list(df.columns)[:10]}"
                )
                return None
            df["code"] = df["code"].astype(str).str.zfill(6)
            _stock_metrics_cache = df
            _stock_metrics_loaded_at = now
            logger.info(f"[stock_metrics] 全量数据加载成功: {len(df)} 条")
            return df
        else:
            logger.debug("[stock_metrics] 全量数据为空")
            return None
    except Exception as e:
        logger.warning(f"[stock_metrics] 全量数据加载失败: {e}")
        return None


def _lookup_from_cache(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    从全量缓存中批量查找个股指标（只取最新一条/每只股票）。
    """
    df = _load_full_cache()
    if df is None or df.empty:
        return {}

    result = {}
    for code in codes:
        code_str = str(code).zfill(6)
        rows = df[df["code"] == code_str]
        if not rows.empty:
            row = rows.iloc[0]
            result[code_str] = {
                "code": code_str,
                "pe_ttm": _safe_float(row.get("pe_ttm")),
                "pb": _safe_float(row.get("pb")),
                "peg": _safe_float(row.get("peg")),
                "close": _safe_float(row.get("close")),
                "market_cap": _safe_float(row.get("market_cap")),
                "date": str(row.get("date", "")),
            }

    return result


# ============================================================
# 批量加载（主入口）
# ============================================================

def load_top10_stock_metrics(
    top10_stocks: List[Dict[str, Any]],
    fund_code: str = "",
    fund_aum_yi: Optional[float] = None,
    max_workers: int = 5,
) -> List[Dict[str, Any]]:
    """
    批量加载 Top10 持仓股的估值指标和成交额。

    数据源（全部本地）：
    1. stock_value 表 → PE/PB/PEG
    2. stock_value 表 → PE 分位（历史 3 年实时计算）
    3. stock_daily_amt CSV → 20 日均成交额

    Args:
        top10_stocks: [{code, name, ratio, ...}] 来自 HoldingsData
        fund_code: 基金代码，用于从 fund_meta 表自动获取基金规模
        fund_aum_yi: 基金规模（亿元），直接传入（优先于 fund_code 查询）
        max_workers: （保留参数，未使用）

    Returns:
        增强后的 list，每项新增 pe_ttm / pb / peg / pe_percentile / avg_amount_20d / ldays
    """
    if not top10_stocks:
        return []

    results = []
    codes_to_load = []

    for stock in top10_stocks:
        code = str(stock.get("code", "")).zfill(6)
        if not code or code == "000000":
            results.append({
                **stock,
                "pe_ttm": None, "pb": None, "peg": None,
                "pe_percentile": None, "avg_amount_20d": None, "ldays": None,
            })
            continue
        codes_to_load.append(code)
        results.append({**stock, "code": code})

    if not codes_to_load:
        return results

    # === Step 1: 从 stock_value 表查找 PE/PB/PEG ===
    cache_map = _lookup_from_cache(codes_to_load)
    if cache_map:
        logger.info(f"[stock_metrics] stock_value 命中 {len(cache_map)}/{len(codes_to_load)} 只股票")

    # === Step 2: 计算 PE 分位 ===
    _ensure_pe_percentile_cache(codes_to_load)

    # === Step 3: 计算 20 日均成交额 ===
    amt_map = _calc_avg_amount_20d(codes_to_load)
    amt_hit = sum(1 for v in amt_map.values() if v is not None)
    if amt_hit:
        logger.info(f"[stock_metrics] 成交额命中 {amt_hit}/{len(codes_to_load)} 只股票")

    # === Step 4: 合并结果 + 计算 Ldays ===
    A_SHARE_DAILY_TURNOVER = 0.05  # A 股日均换手率约 5%

    # 获取基金规模（用于 Ldays 计算）
    if fund_aum_yi is None or fund_aum_yi <= 0:
        if fund_code:
            fund_aum_yi = _get_fund_aum(fund_code)
        if fund_aum_yi is None or fund_aum_yi <= 0:
            fund_aum_yi = 10.0  # 默认 10 亿

    for result in results:
        code = result.get("code", "")
        entry = cache_map.get(code, {})

        result["pe_ttm"] = entry.get("pe_ttm")
        result["pb"] = entry.get("pb")
        result["peg"] = entry.get("peg")
        result["pe_percentile"] = _pe_percentile_cache.get(code)
        result["avg_amount_20d"] = amt_map.get(code)

        # Ldays = 基金持有该股市值 / 日可交易量
        # 持股市值 = fund_aum_yi * ratio% (亿元)
        # 日可交易量 = avg_amount_20d(万元) * 换手率
        # 统一为亿元：日可交易量 = avg_amount_20d * 换手率 / 10000 (亿元)
        # Ldays = 持股市值(亿元) / 日可交易量(亿元)
        ratio = _safe_float(result.get("ratio") or result.get("占净值比例", 0))
        amt = result.get("avg_amount_20d")  # 万元
        if amt and amt > 0 and ratio and ratio > 0:
            hold_value_yi = fund_aum_yi * ratio / 100  # 亿元
            daily_tradable_yi = amt * A_SHARE_DAILY_TURNOVER / 10000  # 亿元
            if daily_tradable_yi > 0:
                ldays = hold_value_yi / daily_tradable_yi
                result["ldays"] = round(ldays, 1)
            else:
                result["ldays"] = None
        else:
            result["ldays"] = None

    return results


# ============================================================
# 基金规模获取
# ============================================================

_fund_aum_cache: Dict[str, Optional[float]] = {}


def _get_fund_aum(fund_code: str) -> Optional[float]:
    """
    从 fund_meta 表获取基金规模（亿元）。

    latest_aum 列格式如 "4.74亿元"，提取数值。
    """
    if fund_code in _fund_aum_cache:
        return _fund_aum_cache[fund_code]

    try:
        from data_loader.db_accessor import DB
        df = DB.query_df(
            'SELECT latest_aum FROM fund_meta WHERE code = ?',
            [fund_code],
        )
        if df is not None and not df.empty:
            raw = str(df.iloc[0]["latest_aum"])
            import re
            m = re.search(r"([\d.]+)", raw)
            if m:
                val = float(m.group(1))
                _fund_aum_cache[fund_code] = val
                return val
    except Exception as e:
        logger.warning(f"[stock_metrics] 获取基金规模失败: {e}")

    _fund_aum_cache[fund_code] = None
    return None


# ============================================================
# 工具函数
# ============================================================

def _safe_float(val) -> Optional[float]:
    """安全转换为 float，None/NaN/Inf → None"""
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return round(v, 4)
    except (ValueError, TypeError):
        return None
