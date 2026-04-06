"""
cb_engine_v2.py — 资产维度·可转债指标计算

指标清单：
  拟买入：转股溢价率、纯债溢价率/债底溢价率、价格、类股化提示
  已持有：YTM、双高检测（价格+溢价率）、债底保护失效、股债双杀模拟

数据来源：
- 转债估值：bond_zh_cov_value_analysis（已有缓存）
- 基金持仓中的可转债：HoldingsData.cb_ratio + bond_details 中筛选
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from models.schema import HoldingsData, CleanNavData
from models.schema_v2 import CBMetrics

logger = logging.getLogger(__name__)


# 双高阈值
DOUBLE_HIGH_PRICE = 130.0       # 价格 > 130
DOUBLE_HIGH_PREMIUM = 30.0      # 溢价率 > 30%
STOCK_LIKE_PRICE = 130.0        # 类股化价格阈值
STOCK_LIKE_PREMIUM = 30.0       # 类股化溢价率阈值


def run_cb_analysis(
    holdings: HoldingsData,
    nav: Optional[CleanNavData] = None,
    mode: str = "buy",
) -> CBMetrics:
    """
    可转债维度分析。

    Args:
        holdings: 持仓数据
        nav: 清洗后净值（用于股债双杀模拟）
        mode: "buy" / "hold"

    Returns:
        CBMetrics
    """
    result = CBMetrics()

    # 获取可转债持仓
    cb_details = _filter_convertible_bonds(holdings.bond_details)
    if not cb_details:
        return result

    # 加载可转债估值数据
    cb_val_df = _load_cb_value_analysis()

    # === 加权指标 ===
    premiums = []
    bond_floor_premiums = []
    prices = []
    ytms = []
    weights = []
    double_high_list = []

    for bond in cb_details:
        name = str(bond.get("债券名称", "") or "")
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue

        # 尝试从估值数据中获取
        cb_code = bond.get("债券代码", "")
        cb_row = None
        if cb_val_df is not None and not cb_val_df.empty and cb_code:
            cb_rows = cb_val_df[cb_val_df.iloc[:, 0].astype(str) == str(cb_code)]
            if not cb_rows.empty:
                cb_row = cb_rows.iloc[-1]

        # 转股溢价率
        premium = _safe_float(cb_row.get("转股溢价率") if cb_row is not None else bond.get("转股溢价率"))
        if premium is not None:
            premiums.append(premium)
            weights.append(ratio)

        # 纯债溢价率 / 债底溢价率
        bfp = _safe_float(
            cb_row.get("纯债溢价率") if cb_row is not None else bond.get("纯债溢价率")
        )
        if bfp is not None:
            bond_floor_premiums.append(bfp)

        # 转债价格
        price = _safe_float(
            cb_row.get("转债价格") if cb_row is not None else bond.get("价格")
        )
        if price is not None:
            prices.append(price)

        # YTM
        ytm_val = _safe_float(
            cb_row.get("到期收益率") if cb_row is not None else bond.get("YTM")
        )
        if ytm_val is not None:
            ytms.append(ytm_val)

        # 双高检测
        if (price and price > DOUBLE_HIGH_PRICE and
                premium and premium > DOUBLE_HIGH_PREMIUM):
            double_high_list.append({
                "name": name,
                "code": cb_code,
                "price": price,
                "premium": premium,
            })

    # 计算加权值
    total_w = sum(weights) if weights else 0
    if total_w > 0:
        if premiums:
            result.conv_premium_rate = round(
                sum(p * w for p, w in zip(premiums, weights)) / total_w, 2
            )
        if bond_floor_premiums:
            result.bond_floor_premium = round(
                sum(p * w for p, w in zip(bond_floor_premiums, weights)) / total_w, 2
            )
        if prices:
            result.avg_conv_price = round(
                sum(p * w for p, w in zip(prices, weights)) / total_w, 2
            )

    # YTM（简单平均）
    if ytms:
        result.ytm = round(sum(ytms) / len(ytms), 4)

    # === 双高检测 ===
    if double_high_list:
        result.is_double_high = True
        result.double_high_list = double_high_list

    # === 已持有模式额外指标 ===
    if mode == "hold":
        # 债底保护失效（YTM 转负）
        if result.ytm is not None:
            result.bond_floor_failed = result.ytm < 0

        # 股债双杀模拟
        result.blackswan_cb_loss = _simulate_cb_blackswan(
            prices, premiums, weights, total_w
        )

    # === 动态处方（拟买入） ===
    if mode == "buy":
        if (result.avg_conv_price and result.avg_conv_price > STOCK_LIKE_PRICE and
                result.conv_premium_rate and result.conv_premium_rate > STOCK_LIKE_PREMIUM):
            result.stock_like_warning = (
                f"组合平均价格 {result.avg_conv_price:.0f} 元、"
                f"溢价率 {result.conv_premium_rate:.1f}%，"
                f"转债已\"股票化\"且债底保护较弱"
            )

    return result


# ============================================================
# 辅助函数
# ============================================================

def _filter_convertible_bonds(bond_details: List[Dict]) -> List[Dict]:
    """筛选可转债"""
    if not bond_details:
        return []

    result = []
    for bond in bond_details:
        name = str(bond.get("债券名称", "")).upper()
        if "转债" in name:
            result.append(bond)
    return result


def _load_cb_value_analysis() -> Optional[pd.DataFrame]:
    """加载可转债估值数据（从本地缓存或API）"""
    try:
        from data_loader.bond_loader import load_cb_holdings_with_details
        # 复用已有加载函数
        df = load_cb_holdings_with_details("placeholder")
        # 这返回的不是估值数据，直接尝试其他路径
    except Exception:
        pass

    try:
        from data_loader.base_api import cached
        from data_loader.akshare_timeout import call_with_timeout
        import akshare as ak

        df = call_with_timeout(
            ak.bond_zh_cov_value_analysis,
            timeout=15,
        )
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.debug(f"[cb_engine_v2] 转债估值数据加载失败: {e}")

    return None


def _simulate_cb_blackswan(
    prices: List[float],
    premiums: List[float],
    weights: List[float],
    total_w: float,
) -> Optional[float]:
    """
    股债双杀模拟。

    假设：股票下跌 20% + 利率上行 50bp
    转债价格近似变化 = Delta × 股票跌幅 - 久期 × 利率变化
    简化：Delta ≈ 1 / (1 + 溢价率/100)
    """
    if not prices or not premiums or total_w <= 0:
        return None

    stock_drop = -0.20      # 股票跌 20%
    rate_change = 0.005     # 利率上行 50bp
    cb_duration = 2.5       # 转债平均久期（年）

    losses = []
    for price, premium, w in zip(prices, premiums, weights):
        # Delta 近似
        delta = 1.0 / (1.0 + premium / 100.0) if premium >= 0 else 0.8
        # 转债跌幅
        loss = delta * stock_drop - cb_duration * rate_change
        losses.append(loss * w)

    total_loss = sum(losses) / total_w
    return round(total_loss * 100, 2)  # 转为百分比


def _safe_ratio(bond: Dict) -> float:
    ratio = float(bond.get("占净值比例", 0) or 0)
    if ratio > 1.5:
        ratio = ratio / 100.0
    return ratio


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None
