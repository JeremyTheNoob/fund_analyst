"""
指数/ETF 数据加载器 — fund_quant_v2
负责：ETF净值/二级市场价格 / 折溢价 / 流动性 / 基准指数
"""

from __future__ import annotations
import logging
from typing import Optional

import akshare as ak
import pandas as pd
import numpy as np

from config import CACHE_TTL
from data_loader.base_api import (
    cached, _ak_fund_nav, _ak_fund_asset_allocation,
    _ak_index_daily_main, _ak_index_daily_em,
)
from models.schema import HoldingsData

logger = logging.getLogger(__name__)


# ============================================================
# ETF 净值 + 二级市场价格
# ============================================================

@cached(ttl=CACHE_TTL["short"])
def load_etf_nav_and_price(symbol: str, years: int = 3) -> dict:
    """
    加载 ETF 净值（场内 IOPV）+ 二级市场价格，用于折溢价分析。
    带缓存（24h TTL）。

    返回：
        nav_df:   DataFrame(date / nav)   场内估算净值
        price_df: DataFrame(date / close) 二级市场收盘价
        premium_df: DataFrame(date / premium_pct) 折溢价率（%）
    """
    from datetime import datetime, timedelta

    end_str   = datetime.now().strftime("%Y%m%d")
    start_str = (datetime.now() - timedelta(days=years * 365)).strftime("%Y%m%d")

    # --- ETF 净值（用基金净值走势代替 IOPV，精度足够）---
    nav_df = pd.DataFrame(columns=["date", "nav"])
    try:
        raw = _ak_fund_nav(symbol)
        if raw is not None and not raw.empty:
            raw = raw.iloc[:, :2].copy()
            raw.columns = ["date", "nav"]
            raw["date"] = pd.to_datetime(raw["date"])
            raw["nav"]  = pd.to_numeric(raw["nav"], errors="coerce")
            raw = raw.dropna().sort_values("date")
            raw = raw[raw["date"] >= pd.to_datetime(start_str)]
            nav_df = raw.reset_index(drop=True)
    except Exception as e:
        logger.warning(f"[load_etf_nav_and_price] {symbol} 净值获取失败: {e}")

    # --- ETF 二级市场价格（带缓存） ---
    price_df = pd.DataFrame(columns=["date", "close"])
    try:
        # 尝试读缓存
        from data_loader.cache_layer import cache_get, cache_set
        cached_price = cache_get("etf_price", ttl_seconds=86400, expect_df=True, symbol=symbol, start_date=start_str, end_date=end_str, adjust="qfq")
        if cached_price is not None and not cached_price.empty:
            price_df = cached_price
        else:
            df_etf = ak.fund_etf_hist_em(symbol=symbol, period="daily",
                                          start_date=start_str, end_date=end_str,
                                          adjust="qfq")
            if df_etf is not None and not df_etf.empty:
                date_col  = "日期" if "日期" in df_etf.columns else df_etf.columns[0]
                close_col = "收盘" if "收盘" in df_etf.columns else "close"
                if close_col in df_etf.columns:
                    df_etf = df_etf[[date_col, close_col]].copy()
                    df_etf.columns = ["date", "close"]
                    df_etf["date"]  = pd.to_datetime(df_etf["date"])
                    df_etf["close"] = pd.to_numeric(df_etf["close"], errors="coerce")
                    price_df = df_etf.dropna().sort_values("date").reset_index(drop=True)
                    # 写入缓存
                    try:
                        from data_loader.cache_layer import cache_set as _cs
                        _cs("etf_price", price_df, expect_df=True, symbol=symbol, start_date=start_str, end_date=end_str, adjust="qfq")
                    except Exception:
                        pass
    except Exception:
        pass

    # --- 折溢价率 ---
    premium_df = pd.DataFrame(columns=["date", "premium_pct"])
    if not nav_df.empty and not price_df.empty:
        merged = nav_df.merge(price_df, on="date", how="inner")
        if not merged.empty and merged["nav"].gt(0).any():
            merged = merged[merged["nav"] > 0]
            merged["premium_pct"] = (merged["close"] - merged["nav"]) / merged["nav"] * 100
            premium_df = merged[["date", "premium_pct"]].copy()

    return {
        "nav_df":     nav_df,
        "price_df":   price_df,
        "premium_df": premium_df,
    }


@cached(ttl=CACHE_TTL["long"])
def load_benchmark_index(index_code: str, start: str, end: str) -> pd.DataFrame:
    """
    加载标的指数日行情，返回 date / ret / close。
    供跟踪误差 / 相关性计算使用。
    """
    def _build(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "close", "ret"])
        needed = {"date", "close"}
        if not needed.issubset(set(df.columns)):
            return pd.DataFrame(columns=["date", "close", "ret"])
        df = df[["date", "close"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
        df["ret"] = df["close"].pct_change().fillna(0)
        return df.reset_index(drop=True)

    raw = _ak_index_daily_main(index_code)
    result = _build(raw)
    if not result.empty:
        return result

    raw = _ak_index_daily_em(index_code)
    return _build(raw)


@cached(ttl=CACHE_TTL["short"])
def load_etf_holdings_ratios(symbol: str) -> HoldingsData:
    """
    ETF 资产配置（现金占比 / 股票占比），用于现金拖累计算。
    """
    r = dict(
        symbol=symbol,
        stock_ratio=0.95, bond_ratio=0.0, cash_ratio=0.05,
        cb_ratio=0.0, top10_stocks=[], bond_details=[], asset_allocation={},
    )

    df_asset = _ak_fund_asset_allocation(symbol, date="2024")
    if df_asset is not None and not df_asset.empty and "资产类型" in df_asset.columns:
        for _, row in df_asset.iterrows():
            asset = str(row.get("资产类型", ""))
            try:
                ratio = float(row.get("占净值比例(%)", 0) or 0) / 100
            except Exception:
                ratio = 0.0
            if "股票" in asset:
                r["stock_ratio"] = ratio
            elif "债券" in asset:
                r["bond_ratio"] = ratio
            elif "现金" in asset or "银行存款" in asset:
                r["cash_ratio"] = ratio

    return HoldingsData(**r)


@cached(ttl=CACHE_TTL["long"])
def load_etf_daily_trading(symbol: str, years: int = 2) -> pd.DataFrame:
    """
    加载 ETF 日成交数据（成交量 / 成交额），用于流动性分析。
    带缓存（24h TTL）。返回 date / volume / amount / turnover_rate
    """
    from datetime import datetime, timedelta

    end_str   = datetime.now().strftime("%Y%m%d")
    start_str = (datetime.now() - timedelta(days=years * 365)).strftime("%Y%m%d")

    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("etf_trading", ttl_seconds=86400, expect_df=True, symbol=symbol, start_date=start_str, end_date=end_str)
        if cached is not None:
            return cached
    except Exception:
        pass

    try:
        df = ak.fund_etf_hist_em(symbol=symbol, period="daily",
                                  start_date=start_str, end_date=end_str,
                                  adjust="")
        if df is not None and not df.empty:
            col_map = {
                "日期": "date",
                "成交量": "volume",
                "成交额": "amount",
                "换手率": "turnover_rate",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            needed = ["date"]
            for col in ["volume", "amount", "turnover_rate"]:
                if col not in df.columns:
                    df[col] = np.nan
                needed.append(col)
            df = df[needed].copy()
            df["date"] = pd.to_datetime(df["date"])
            result = df.sort_values("date").reset_index(drop=True)

            # 写入缓存
            try:
                from data_loader.cache_layer import cache_set as _cs
                _cs("etf_trading", result, expect_df=True, symbol=symbol, start_date=start_str, end_date=end_str)
            except Exception:
                pass

            return result
    except Exception as e:
        logger.warning(f"[load_etf_daily_trading] {symbol} 成交数据获取失败: {e}")

    return pd.DataFrame(columns=["date", "volume", "amount", "turnover_rate"])


# ============================================================
# 辅助函数：指数代码推断
# ============================================================

def infer_benchmark_code(fund_name: str, benchmark_parsed: dict) -> str:
    """
    从基金名称 / 业绩基准推断标的指数代码。
    优先从 benchmark_parsed 取，再从名称关键字推断。
    """
    # 从业绩基准解析结果取
    if benchmark_parsed and benchmark_parsed.get("components"):
        comps = benchmark_parsed["components"]
        # 找权重最大的成分
        comps_sorted = sorted(comps, key=lambda x: x.get("weight", 0), reverse=True)
        for c in comps_sorted:
            code = c.get("code", "")
            if code and not code.startswith("hk:"):
                return code

    # 从名称推断
    name_to_code = {
        "沪深300": "sh000300",
        "300":     "sh000300",
        "中证500": "sh000905",
        "500":     "sh000905",
        "中证1000": "sh000852",
        "1000":    "sh000852",
        "上证50":  "sh000016",
        "50ETF":   "sh000016",
        "创业板":  "sz399006",
        "科创":    "sh000688",
        "恒生":    "HSI",
    }
    for kw, code in name_to_code.items():
        if kw in fund_name:
            return code if not code.startswith("HSI") else f"hk:{code}"

    return "sh000300"  # 默认沪深300
