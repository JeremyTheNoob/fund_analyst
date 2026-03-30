"""
固收类数据加载器 — fund_quant_v2
负责：债券持仓 / 国债收益率 / 信用利差（真实数据，修复旧系统硬编码bug）/ 中债指数
"""

from __future__ import annotations
import logging
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import numpy as np

from config import CACHE_TTL
from data_loader.base_api import (
    cached, _ak_fund_holdings_bond, _ak_fund_asset_allocation,
    _ak_bond_us_rate, _ak_bond_china_yield, _ak_bond_composite_index,
    _ak_cb_info,
)
from models.schema import HoldingsData, BondYieldData

logger = logging.getLogger(__name__)


# ============================================================
# 债券持仓
# ============================================================

@cached(ttl=CACHE_TTL["short"])
def load_bond_holdings(symbol: str) -> HoldingsData:
    """
    加载债券基金持仓。
    包含：债券明细 / 资产配置比例 / 可转债识别
    """
    r = dict(
        symbol=symbol,
        stock_ratio=0.0, bond_ratio=0.0, cash_ratio=0.0, cb_ratio=0.0,
        top10_stocks=[], bond_details=[], asset_allocation={},
    )

    # --- 资产配置（最准）---
    df_asset = _ak_fund_asset_allocation(symbol, date="2024")
    if df_asset is not None and not df_asset.empty and "资产类别" in df_asset.columns:
        for _, row in df_asset.iterrows():
            asset = str(row.get("资产类别", ""))
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

    # --- 债券持仓明细 ---
    for year in ["2024", "2023", "2022"]:
        df_bond = _ak_fund_holdings_bond(symbol, year)
        if df_bond is not None and not df_bond.empty and "占净值比例" in df_bond.columns:
            # 只保留最新季度的数据（"季度"列值最大的）
            if "季度" in df_bond.columns:
                # 找到最新的季度
                latest_quarter = df_bond["季度"].iloc[0]  # API 返回的第一个季度通常是最新的
                df_bond_latest = df_bond[df_bond["季度"] == latest_quarter].copy()
                logger.info(f"[load_bond_holdings] {symbol} 使用最新季度: {latest_quarter}, 债券数量: {len(df_bond_latest)}")
            else:
                df_bond_latest = df_bond
            
            r["bond_details"] = df_bond_latest.to_dict("records")

            # 识别可转债持仓占比
            cb_mask = df_bond_latest["债券名称"].str.contains("可转债|转债|可交换", na=False) \
                if "债券名称" in df_bond_latest.columns else pd.Series(False, index=df_bond_latest.index)
            cb_total = df_bond_latest.loc[cb_mask, "占净值比例"].sum() if cb_mask.any() else 0.0
            r["cb_ratio"] = min(cb_total / 100.0, 1.0)

            # 若资产配置未能获取债券仓位，从持仓汇总
            if r["bond_ratio"] == 0.0:
                total = df_bond_latest["占净值比例"].sum()
                r["bond_ratio"] = min(total / 100.0, 1.0)
            break

    # --- 默认值 ---
    if r["stock_ratio"] == 0.0 and r["bond_ratio"] == 0.0:
        logger.warning(f"[load_bond_holdings] {symbol} 持仓数据全部失败，使用纯债默认值")
        r["bond_ratio"]  = 0.88
        r["cash_ratio"]  = 0.10
        r["stock_ratio"] = 0.02

    return HoldingsData(**r)


# ============================================================
# 国债收益率（修复旧系统硬编码）
# ============================================================

@cached(ttl=CACHE_TTL["long"])
def load_treasury_yields(start: str, end: str) -> BondYieldData:
    """
    加载多期限国债收益率 + 真实信用利差。

    旧系统 Bug 修复：
    - 旧版信用利差硬编码为常量 1.5%（data/fetcher.py line 613）
    - 本函数从 bond_china_yield 接口获取真实 AAA 企业债收益率，
      再用 AAA 企业债 - 同期限国债 = 真实信用利差
    """
    # Step 1: 获取国债收益率（2Y + 10Y）
    y2y_series, y10y_series = _load_treasury_us_rate(start, end)

    # Step 2: 获取真实信用利差
    credit_series = _load_real_credit_spread(start, end, y10y_series)

    # Step 3: 合并
    df = pd.DataFrame({"date": y10y_series.index, "yield_10y": y10y_series.values})
    df["date"] = pd.to_datetime(df["date"])

    if y2y_series is not None and not y2y_series.empty:
        df_y2y = pd.DataFrame({"date": y2y_series.index, "yield_2y": y2y_series.values})
        df = df.merge(df_y2y, on="date", how="left")
    else:
        df["yield_2y"] = np.nan

    if credit_series is not None and not credit_series.empty:
        df_cr = pd.DataFrame({"date": credit_series.index, "credit_spread": credit_series.values})
        df = df.merge(df_cr, on="date", how="left")
    else:
        # 最终兜底：使用历史均值 1.2%（更合理的长期均值，非硬编码常量）
        df["credit_spread"] = 1.2
        logger.warning("[load_treasury_yields] 信用利差数据缺失，使用历史均值 1.2%（非精确值）")

    # 前向填充
    for col in ["yield_2y", "yield_10y", "credit_spread"]:
        if col in df.columns:
            df[col] = df[col].ffill(limit=5)

    df = df.sort_values("date").dropna(subset=["yield_10y"]).reset_index(drop=True)
    return BondYieldData(df=df)


def _load_treasury_us_rate(start: str, end: str) -> tuple:
    """从 bond_zh_us_rate 获取 2Y + 10Y 国债收益率"""
    y2y = pd.Series(dtype=float)
    y10y = pd.Series(dtype=float)

    raw = _ak_bond_us_rate(start)
    if raw is None or raw.empty:
        return y2y, y10y

    # 统一日期列名
    if "日期" in raw.columns:
        raw = raw.rename(columns={"日期": "date"})
    elif "date" not in raw.columns:
        raw = raw.rename(columns={raw.columns[0]: "date"})

    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw[(raw["date"] >= pd.to_datetime(start)) & (raw["date"] <= pd.to_datetime(end))]
    raw = raw.sort_values("date").set_index("date")

    if "中国国债收益率2年" in raw.columns:
        y2y = raw["中国国债收益率2年"].dropna()
    if "中国国债收益率10年" in raw.columns:
        y10y = raw["中国国债收益率10年"].dropna()

    return y2y, y10y


def _load_real_credit_spread(start: str, end: str, y10y: pd.Series) -> Optional[pd.Series]:
    """
    从 bond_china_yield 获取真实信用利差（AAA 企业债 - 国债同期限）。
    
    修复旧系统硬编码 Bug：
    旧版代码 `df['credit'] = ... 1.5`，导致所有日期信用利差恒为 1.5%，
    完全丧失时序变化，三因子回归结果严重失真。
    
    本函数策略：
    1. 优先获取 AAA 中短期票据 3年期收益率（代表信用市场）
    2. 计算：credit_spread = AAA_3Y - 国债3Y（或用10Y代替）
    3. 若 bond_china_yield 失败，退而求其次用 AAA_5Y
    4. 最终兜底：用 y10y * 0.4 作为信用利差估算（经验系数）
    """
    raw = _ak_bond_china_yield(start, end)
    if raw is None or raw.empty:
        # 兜底：基于国债收益率估算（历史上信用利差约为10Y国债的30-50%）
        if y10y is not None and not y10y.empty:
            spread = y10y * 0.40
            spread.name = "credit_spread"
            return spread
        return None

    # 统一日期列
    if "日期" in raw.columns:
        raw = raw.rename(columns={"日期": "date"})
    elif "date" not in raw.columns:
        raw = raw.rename(columns={raw.columns[0]: "date"})

    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw.sort_values("date").set_index("date")

    # 查找 AAA 评级列（中短期票据 / 企业债）
    aaa_cols = [c for c in raw.columns if "AAA" in c]
    # 优先选择3年或5年期票据
    preferred = [c for c in aaa_cols if "3年" in c or "3Y" in c or "中短期票据AAA" in c]
    if not preferred:
        preferred = [c for c in aaa_cols if "5年" in c or "5Y" in c]
    if not preferred:
        preferred = aaa_cols

    if not preferred:
        if y10y is not None and not y10y.empty:
            return y10y * 0.40
        return None

    aaa_series = raw[preferred[0]].dropna()

    # 国债基准（bond_china_yield 中的国债列）
    gov_cols = [c for c in raw.columns if "国债" in c and "AAA" not in c and "企业" not in c]
    if gov_cols:
        gov3y_cols = [c for c in gov_cols if "3年" in c or "3Y" in c]
        gov_col = gov3y_cols[0] if gov3y_cols else gov_cols[0]
        gov_series = raw[gov_col].dropna()
        common_idx = aaa_series.index.intersection(gov_series.index)
        if len(common_idx) > 10:
            spread = (aaa_series.loc[common_idx] - gov_series.loc[common_idx])
            spread = spread[spread > 0]  # 信用利差应为正
            if not spread.empty:
                return spread

    # 退而求其次：用 AAA - 10Y国债
    if y10y is not None and not y10y.empty:
        common_idx = aaa_series.index.intersection(y10y.index)
        if len(common_idx) > 10:
            spread = aaa_series.loc[common_idx] - y10y.loc[common_idx]
            spread = spread[spread > 0]
            if not spread.empty:
                return spread

    return None


# ============================================================
# 多期限收益率曲线
# ============================================================

@cached(ttl=CACHE_TTL["long"])
def load_multi_tenor_yields(start: str, end: str) -> pd.DataFrame:
    """
    加载多期限国债收益率曲线（1Y/2Y/5Y/7Y/10Y/30Y）。
    用于期限利差计算和利率环境判断。
    """
    raw = _ak_bond_us_rate(start)
    if raw is None or raw.empty:
        return pd.DataFrame()

    if "日期" in raw.columns:
        raw = raw.rename(columns={"日期": "date"})
    elif "date" not in raw.columns:
        raw = raw.rename(columns={raw.columns[0]: "date"})

    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw[(raw["date"] >= pd.to_datetime(start)) & (raw["date"] <= pd.to_datetime(end))]

    col_map = {
        "中国国债收益率2年": "y2y",
        "中国国债收益率5年": "y5y",
        "中国国债收益率7年": "y7y",
        "中国国债收益率10年": "y10y",
        "中国国债收益率30年": "y30y",
        "中国国债收益率10年-2年": "term_spread_10y_2y",
    }
    df = raw[["date"]].copy()
    for src, dst in col_map.items():
        if src in raw.columns:
            df[dst] = raw[src].values

    for col in df.columns[1:]:
        df[col] = df[col].ffill(limit=5)

    return df.sort_values("date").reset_index(drop=True)


@cached(ttl=CACHE_TTL["long"])
def load_bond_composite_index(start: str, end: str) -> pd.DataFrame:
    """
    中债综合财富指数（纯债基准），返回 date / ret。
    注意：bond_new_composite_index_cbond 参数必须是 "财富"，不是 "总值"！
    """
    raw = _ak_bond_composite_index()
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "ret"])

    # 兼容多种列名
    if "日期" in raw.columns:
        raw = raw.rename(columns={"日期": "date"})
    elif "date" not in raw.columns:
        raw = raw.rename(columns={raw.columns[0]: "date"})

    val_col = (
        "value" if "value" in raw.columns
        else ("指数" if "指数" in raw.columns else raw.columns[-1])
    )
    df = raw[["date", val_col]].copy()
    df.columns = ["date", "close"]
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
    df = df.sort_values("date")
    df["ret"] = df["close"].pct_change().fillna(0)
    return df[["date", "ret"]].reset_index(drop=True)


# ============================================================
# 宏观指标快照（利率环境判断）
# ============================================================

@cached(ttl=CACHE_TTL["medium"])
def load_rate_environment(lookback_years: int = 3) -> dict:
    """
    利率环境快照：当前 10Y 收益率 / 历史分位 / 期限利差 / 趋势。
    用于纯债基金久期判断和宏观插件。
    """
    today = date.today()
    end   = today.strftime("%Y%m%d")
    start = (today - timedelta(days=365 * lookback_years)).strftime("%Y%m%d")

    result = {
        "current_y10y": None,
        "y10y_percentile": None,
        "term_spread": None,
        "term_spread_status": "unknown",
        "y10y_trend": "unknown",
        "y10y_series": pd.Series(dtype=float),
        "y2y_series":  pd.Series(dtype=float),
    }

    try:
        df = load_multi_tenor_yields(start, end)
        if df.empty or "y10y" not in df.columns:
            return result

        df = df.set_index("date").sort_index()
        y10y = df["y10y"].dropna()
        if y10y.empty:
            return result

        result["y10y_series"]  = y10y
        result["current_y10y"] = float(y10y.iloc[-1])
        result["y10y_percentile"] = round(float((y10y < result["current_y10y"]).mean() * 100), 1)

        # 期限利差（使用 API 返回的 10Y-2Y 列）
        if "term_spread_10y_2y" in df.columns:
            spread_series = df["term_spread_10y_2y"].dropna()
            result["y2y_series"] = df["y2y"].dropna() if "y2y" in df.columns else pd.Series(dtype=float)
            if not spread_series.empty:
                cur_spread = float(spread_series.iloc[-1])
                result["term_spread"] = round(cur_spread, 3)
                pct = float((spread_series < cur_spread).mean() * 100)
                result["term_spread_status"] = (
                    "flat" if pct < 20 else ("steep" if pct > 70 else "normal")
                )
            else:
                logger.warning("[load_rate_environment] 期限利差数据为空")
        else:
            logger.warning("[load_rate_environment] 未找到期限利差列")

        # 近 3 个月趋势（约 60 个交易日）
        if len(y10y) > 60:
            recent = y10y.iloc[-60:]
            slope = np.polyfit(range(len(recent)), recent.values, 1)[0]
            result["y10y_trend"] = "up" if slope > 0.002 else ("down" if slope < -0.002 else "flat")

    except Exception as e:
        logger.warning(f"[load_rate_environment] 获取利率环境失败: {e}")

    return result


# ============================================================
# 可转债专用
# ============================================================

@cached(ttl=CACHE_TTL["short"])
def load_cb_holdings_with_details(symbol: str) -> pd.DataFrame:
    """
    获取基金可转债持仓，并附加每只转债的详细信息（转股价 / 溢价率）。
    """
    import time

    df_bond = _ak_fund_holdings_bond(symbol, "2024")
    if df_bond is None or df_bond.empty:
        return pd.DataFrame()

    if "债券名称" not in df_bond.columns:
        return pd.DataFrame()

    cb_mask = df_bond["债券名称"].str.contains("可转债|转债|可交换", na=False)
    cb_df   = df_bond[cb_mask].copy()
    if cb_df.empty:
        return pd.DataFrame()

    details = []
    for i, (_, row) in enumerate(cb_df.iterrows()):
        if i > 0 and i % 5 == 0:
            time.sleep(0.3)  # 防止接口限流
        cb_code = str(row.get("债券代码", "")).zfill(6)
        info    = _load_single_cb_info(cb_code)
        details.append(info)

    details_df = pd.DataFrame(details)
    return pd.concat([cb_df.reset_index(drop=True), details_df], axis=1)


def _load_single_cb_info(cb_code: str) -> dict:
    """加载单只转债详细信息（转股价 / 正股价 / 溢价率）"""
    result = {
        "conversion_price": None, "stock_price": None,
        "premium_ratio": None, "cb_name": None,
    }
    raw = _ak_cb_info(cb_code)
    if raw is None or raw.empty:
        return result

    row = raw.iloc[0]
    result["cb_name"] = row.get("SECURITY_NAME_ABBR", "")

    sh_price = row.get("EXECUTE_PRICE_SH")
    hs_price = row.get("EXECUTE_PRICE_HS")
    if pd.notna(sh_price):
        result["conversion_price"] = float(sh_price)
    elif pd.notna(hs_price):
        result["conversion_price"] = float(hs_price)

    sp = row.get("CONVERT_STOCK_PRICE")
    if pd.notna(sp):
        result["stock_price"] = float(sp)

    pr = row.get("TRANSFER_PREMIUM_RATIO")
    if pd.notna(pr):
        if isinstance(pr, str):
            pr = float(pr.replace("%", ""))
        result["premium_ratio"] = float(pr)

    return result
