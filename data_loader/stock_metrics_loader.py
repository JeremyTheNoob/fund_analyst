"""
stock_metrics_loader.py — Top10 持仓个股指标加载（v2 缓存版）

获取个股的 PE(TTM)、PB、PEG、20日均成交额，
用于计算估值水位、PEG、流动性穿透(Ldays)等指标。

缓存策略：
- 优先从 Supabase 全量缓存表（stock_metrics_all）批量查找
- 缓存未命中时，回退到 AkShare API 单只拉取
- 全量表由 scripts/prewarm_stock_metrics.py 每日预热

存储估算：
- 全量 ~5400 只股票 × 9 列 ≈ 800 KB（JSON records）
- TTL: 24h（每日预热更新）
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# 全量缓存读取
# ============================================================

_stock_metrics_cache: Optional[pd.DataFrame] = None
_stock_metrics_loaded_at: Optional[float] = None
_CACHE_TTL_SECONDS = 3600  # 内存缓存 1 小时


def _load_full_cache() -> Optional[pd.DataFrame]:
    """
    从 Supabase 加载全量个股指标缓存。

    Returns:
        DataFrame 或 None（列：code, pe_ttm, pb, peg, close, market_cap,
                               date, pe_percentile, avg_amount_20d）
    """
    import time
    global _stock_metrics_cache, _stock_metrics_loaded_at

    # 内存缓存有效期内直接返回
    now = time.time()
    if _stock_metrics_cache is not None and _stock_metrics_loaded_at is not None:
        if now - _stock_metrics_loaded_at < _CACHE_TTL_SECONDS:
            return _stock_metrics_cache

    # 从 Supabase 读取（Storage 优先）
    try:
        from data_loader.cache_layer import cache_get_large
        from config import CACHE_TTL

        ttl = CACHE_TTL.get("long", 86400)
        df = cache_get_large("stock_metrics_all", ttl)

        if df is not None and not df.empty:
            # 确保 code 列存在
            if "code" not in df.columns:
                logger.warning(
                    f"[stock_metrics] 缓存数据缺少 code 列，"
                    f"实际列: {list(df.columns)[:10]}"
                )
                return None
            df["code"] = df["code"].astype(str).str.zfill(6)
            _stock_metrics_cache = df
            _stock_metrics_loaded_at = now
            logger.info(f"[stock_metrics] 全量缓存加载成功: {len(df)} 只股票")
            return df
        else:
            logger.debug("[stock_metrics] 全量缓存未命中")
            return None
    except Exception as e:
        logger.warning(f"[stock_metrics] 全量缓存加载失败: {e}")
        return None


def _lookup_from_cache(
    codes: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    从全量缓存中批量查找个股指标。

    Args:
        codes: 6位股票代码列表

    Returns:
        {code: {pe_ttm, pb, peg, close, market_cap, date, pe_percentile, avg_amount_20d}}
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
                "pe_percentile": _safe_float(row.get("pe_percentile")),
                "avg_amount_20d": _safe_float(row.get("avg_amount_20d")),
            }

    return result


# ============================================================
# AkShare 单只拉取（回退方案）
# ============================================================

def _load_single_stock_value(code: str) -> Optional[Dict[str, Any]]:
    """
    AkShare 实时拉取单只股票估值指标。

    Returns:
        {code, pe_ttm, pb, peg, close, market_cap, date, pe_percentile} 或 None
    """
    try:
        import akshare as ak
        from data_loader.akshare_timeout import call_with_timeout

        df = call_with_timeout(
            ak.stock_value_em, kwargs={"symbol": code}, timeout=15
        )
        if df is None or df.empty:
            return None

        latest = df.iloc[-1]
        result = {
            "code": code,
            "pe_ttm": _safe_float(latest.get("PE(TTM)")),
            "pb": _safe_float(latest.get("市净率")),
            "peg": _safe_float(latest.get("PEG值")),
            "close": _safe_float(latest.get("当日收盘价")),
            "market_cap": _safe_float(latest.get("总市值")),
            "date": str(latest.get("数据日期", "")),
        }

        # PE 历史分位
        pe_series = pd.to_numeric(df["PE(TTM)"], errors="coerce").dropna()
        if len(pe_series) > 60:
            result["pe_percentile"] = round(
                (pe_series.iloc[-1] < pe_series).mean() * 100, 1
            )
        else:
            result["pe_percentile"] = None

        return result

    except Exception as e:
        logger.debug(f"[stock_metrics] {code} AkShare拉取失败: {e}")
        return None


def _load_single_stock_amount(code: str, days: int = 20) -> Optional[float]:
    """
    AkShare 实时拉取单只股票最近 N 天平均日成交额。

    Returns:
        平均日成交额（元），或 None
    """
    try:
        import akshare as ak
        from data_loader.akshare_timeout import call_with_timeout

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")

        df = call_with_timeout(
            ak.stock_zh_a_hist,
            kwargs={
                "symbol": code,
                "period": "daily",
                "start_date": start_date,
                "end_date": end_date,
                "adjust": "qfq",
            },
            timeout=15,
        )

        if df is None or df.empty or "成交额" not in df.columns:
            return None

        amounts = pd.to_numeric(df["成交额"], errors="coerce").dropna()
        if len(amounts) < 5:
            return None

        return float(amounts.tail(days).mean())

    except Exception as e:
        logger.debug(f"[stock_metrics] {code} 成交额拉取失败: {e}")
        return None


# ============================================================
# 批量加载（缓存优先 + 回退）
# ============================================================

def load_top10_stock_metrics(
    top10_stocks: List[Dict[str, Any]],
    max_workers: int = 5,
) -> List[Dict[str, Any]]:
    """
    批量加载 Top10 持仓股的估值指标和成交额。

    策略：
    1. 先从 Supabase 全量缓存表批量查找
    2. 缓存未命中的股票回退到 AkShare 实时拉取（并发）

    Args:
        top10_stocks: [{code, name, ratio, ...}] 来自 HoldingsData
        max_workers: AkShare 回退时的并发线程数

    Returns:
        增强后的 list，每项新增 pe_ttm / pb / peg / avg_amount_20d / ldays
    """
    if not top10_stocks:
        return []

    results = []
    codes_to_load = []

    for i, stock in enumerate(top10_stocks):
        code = str(stock.get("code", "")).zfill(6)
        if not code or code == "000000":
            results.append({
                **stock,
                "pe_ttm": None, "pb": None, "peg": None,
                "avg_amount_20d": None, "ldays": None,
            })
            continue
        codes_to_load.append(code)
        results.append({**stock, "code": code})

    if not codes_to_load:
        return results

    # === Step 1: 全量缓存查找 ===
    cache_map = _lookup_from_cache(codes_to_load)
    cache_hit_count = len(cache_map)
    if cache_hit_count > 0:
        logger.info(f"[stock_metrics] 缓存命中 {cache_hit_count}/{len(codes_to_load)} 只股票")

    # === Step 2: 缓存未命中的回退到 AkShare ===
    missing_codes = [c for c in codes_to_load if c not in cache_map]

    if missing_codes:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # 并发拉取估值数据
        value_map: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_load_single_stock_value, code): code
                for code in missing_codes
            }
            for future in as_completed(futures, timeout=30):
                code = futures[future]
                try:
                    val = future.result()
                    if val is not None:
                        value_map[code] = val
                except Exception:
                    pass

        # 并发拉取成交额
        amount_map: Dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_load_single_stock_amount, code): code
                for code in missing_codes
            }
            for future in as_completed(futures, timeout=30):
                code = futures[future]
                try:
                    amt = future.result()
                    if amt is not None:
                        amount_map[code] = amt
                except Exception:
                    pass

        # 合并回退数据到 cache_map
        for code in missing_codes:
            val = value_map.get(code, {})
            amt = amount_map.get(code)
            entry = {
                "code": code,
                "pe_ttm": val.get("pe_ttm"),
                "pb": val.get("pb"),
                "peg": val.get("peg"),
                "close": val.get("close"),
                "market_cap": val.get("market_cap"),
                "date": val.get("date"),
                "pe_percentile": val.get("pe_percentile"),
                "avg_amount_20d": amt,
            }
            cache_map[code] = entry

    # === Step 3: 合并结果 + 计算 Ldays ===
    A_SHARE_DAILY_TURNOVER = 0.05

    for result in results:
        code = result.get("code", "")
        entry = cache_map.get(code, {})

        result["pe_ttm"] = entry.get("pe_ttm")
        result["pb"] = entry.get("pb")
        result["peg"] = entry.get("peg")
        result["pe_percentile"] = entry.get("pe_percentile")
        result["avg_amount_20d"] = entry.get("avg_amount_20d")

        # Ldays = 持仓占比 / (20日均成交额 × 换手率 / 总规模)
        ratio = _safe_float(result.get("ratio") or result.get("占净值比例", 0))
        amt = entry.get("avg_amount_20d")
        if amt and amt > 0 and ratio and ratio > 0:
            daily_tradable = amt * A_SHARE_DAILY_TURNOVER
            result["ldays"] = round(ratio / (daily_tradable / 1e8), 1) if daily_tradable > 0 else None
        else:
            result["ldays"] = None

    return results


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
