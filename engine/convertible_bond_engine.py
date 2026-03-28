"""
转债/固收+ 分析引擎 — fund_quant_v2
转债基金识别 / 资产分解 / Delta 估算 / 权益暴露分析
"""

from __future__ import annotations
import logging
from typing import Optional

import numpy as np
import pandas as pd

from engine.common_metrics import (
    annualized_return, max_drawdown, volatility,
    sharpe_ratio, sortino_ratio, monthly_win_rate,
    normalize_score,
)
from models.schema import (
    CleanNavData, HoldingsData, FundBasicInfo,
    CommonMetrics, ConvertibleBondMetrics,
)

logger = logging.getLogger(__name__)

# 可转债 Delta 估算分段表（溢价率 → Delta）
DELTA_BY_PREMIUM = [
    (100, 0.95),  # 溢价率 ≤ 100%（深度实值）→ Delta ≈ 0.95
    (50,  0.80),
    (20,  0.60),
    (10,  0.50),
    (5,   0.35),
    (0,   0.25),
    (-5,  0.15),  # 溢价率 < -5%（深度虚值）→ Delta ≈ 0.15
]


# ============================================================
# 主入口
# ============================================================

def run_cb_analysis(
    nav: CleanNavData,
    holdings: HoldingsData,
    basic: FundBasicInfo,
    cb_holdings_df: Optional[pd.DataFrame] = None,
) -> ConvertibleBondMetrics:
    """
    可转债/固收+ 基金完整分析流程。

    步骤：
    1. 识别基金类型（转债主题/偏债混合/固收+）
    2. 计算通用指标
    3. 权益暴露分析（综合 Delta 估算）
    4. 估值指标（IV Spread / YTM / 债底）
    5. 综合评分
    """
    nav_df = nav.df.copy()
    if nav_df.empty or "ret" not in nav_df.columns:
        return _empty_cb_metrics()

    fund_rets = nav_df["ret"].values
    dates     = pd.DatetimeIndex(nav_df["date"]) if "date" in nav_df.columns else None

    # --- Step 1: 基金类型识别 ---
    cb_type, confidence = _identify_cb_fund_type(basic, holdings)

    # --- Step 2: 通用指标 ---
    common = CommonMetrics(
        annualized_return=round(annualized_return(fund_rets), 4),
        cumulative_return=round(float(np.prod(1 + fund_rets) - 1), 4),
        volatility=round(volatility(fund_rets), 4),
        max_drawdown=round(max_drawdown(fund_rets), 4),
        sharpe_ratio=round(sharpe_ratio(fund_rets), 3),
        sortino_ratio=round(sortino_ratio(fund_rets), 3),
        monthly_win_rate=0.5,
    )

    # --- Step 3: 权益暴露 ---
    delta_avg, premium_avg, equity_exposure = _compute_equity_exposure(holdings, cb_holdings_df)

    # --- Step 4: 估值指标 ---
    iv_spread, ytm, bond_floor = _compute_valuation_metrics(cb_holdings_df)

    # --- Step 5: 综合评分 ---
    overall, grade = _compute_cb_score(common, delta_avg, premium_avg, iv_spread, cb_type)

    return ConvertibleBondMetrics(
        common=common,
        cb_fund_type=cb_type,
        cb_confidence=confidence,
        equity_exposure=round(equity_exposure, 4),
        delta_avg=round(delta_avg, 3),
        premium_avg=round(premium_avg, 2),
        iv_spread=round(iv_spread, 4),
        ytm=round(ytm, 4),
        bond_floor=round(bond_floor, 2),
        stock_alpha=0.0,  # 需要更多数据，留待后续
        overall_score=round(overall, 1),
        score_grade=grade,
    )


# ============================================================
# 基金类型识别（三重验证）
# ============================================================

def _identify_cb_fund_type(
    basic: FundBasicInfo,
    holdings: HoldingsData,
) -> tuple[str, str]:
    """
    三重验证识别转债基金类型：
    1. 静态扫描（持仓比例）
    2. 命名过滤（基金名称关键词）
    3. 结合股票 + 转债综合判断

    Returns:
        (type, confidence)
        type: pure_bond / cb_fund / mixed / fixed_plus
        confidence: high / medium / low
    """
    name    = basic.name
    cb_ratio   = holdings.cb_ratio
    stock_ratio = holdings.stock_ratio
    bond_ratio  = holdings.bond_ratio

    confidence = "medium"
    clues      = []

    # 命名关键词
    cb_name_keywords = ["转债", "可转债", "可转换", "固收+", "打新", "增强收益"]
    for kw in cb_name_keywords:
        if kw in name:
            clues.append(f"名称含'{kw}'")
            break

    # 基于持仓比例分类
    if cb_ratio >= 0.70:
        # 转债主题基金
        if clues:
            confidence = "high"
        return "cb_fund", confidence

    elif cb_ratio >= 0.30 and stock_ratio < 0.20:
        # 偏债混合或固收+
        if "固收" in name or "+" in name or "稳健" in name:
            return "fixed_plus", "high"
        return "fixed_plus", confidence

    elif cb_ratio >= 0.10 and stock_ratio >= 0.20:
        # 偏债混合（有股票 + 有转债）
        return "mixed", confidence

    elif bond_ratio >= 0.80 and cb_ratio < 0.10:
        # 纯债（转债占比很小）
        return "pure_bond", "high"

    else:
        confidence = "low"
        return "mixed", confidence


# ============================================================
# 权益暴露分析
# ============================================================

def _compute_equity_exposure(
    holdings: HoldingsData,
    cb_holdings_df: Optional[pd.DataFrame],
) -> tuple[float, float, float]:
    """
    计算综合权益暴露。

    E_total = W_stock + Σ(W_cb_i × Delta_i)

    Returns:
        (delta_avg, premium_avg, equity_exposure)
    """
    stock_ratio = holdings.stock_ratio

    # 没有转债持仓数据
    if cb_holdings_df is None or cb_holdings_df.empty:
        # 用转债占比 × 平均 Delta 0.4 估算
        cb_ratio = holdings.cb_ratio
        delta_avg   = 0.40
        premium_avg = 20.0
        equity_exposure = stock_ratio + cb_ratio * delta_avg
        return delta_avg, premium_avg, equity_exposure

    deltas    = []
    premiums  = []
    weights   = []

    for _, row in cb_holdings_df.iterrows():
        ratio   = float(row.get("占净值比例", 0) or 0) / 100
        premium = row.get("premium_ratio")

        if ratio <= 0:
            continue

        # 估算 Delta
        delta = _estimate_delta(premium)
        weights.append(ratio)
        deltas.append(delta)

        if premium is not None and not pd.isna(premium):
            premiums.append(float(premium))

    if not weights:
        delta_avg   = 0.40
        premium_avg = 20.0
    else:
        total_w   = sum(weights)
        delta_avg = sum(d * w / total_w for d, w in zip(deltas, weights))
        premium_avg = float(np.mean(premiums)) if premiums else 20.0

    cb_ratio = sum(weights)
    equity_exposure = stock_ratio + cb_ratio * delta_avg
    return float(delta_avg), float(premium_avg), float(equity_exposure)


def _estimate_delta(premium_ratio: Optional[float]) -> float:
    """
    基于转股溢价率分段估算 Delta。

    转股溢价率越低（越接近/低于转股价），Delta 越高（越像股票）。
    """
    if premium_ratio is None or pd.isna(premium_ratio):
        return 0.40  # 默认

    pr = float(premium_ratio)
    for threshold, delta in DELTA_BY_PREMIUM:
        if pr >= threshold:
            return delta
    return 0.10  # 极度虚值


# ============================================================
# 估值指标
# ============================================================

def _compute_valuation_metrics(
    cb_holdings_df: Optional[pd.DataFrame],
) -> tuple[float, float, float]:
    """
    估算持仓转债的平均估值指标。
    Returns: (iv_spread, ytm, bond_floor)
    iv_spread: 隐含波动率利差（转债期权价值 - 正股波动率，需更多数据，此处估算）
    ytm:       到期收益率（从债底估算）
    bond_floor: 债底价格
    """
    if cb_holdings_df is None or cb_holdings_df.empty:
        return 0.0, 0.03, 100.0  # 默认值

    # 从持仓数据估算（实际生产中应调用更多 API）
    # 此处为简化实现，基于溢价率推算
    premiums = []
    for _, row in cb_holdings_df.iterrows():
        pr = row.get("premium_ratio")
        if pr is not None and not pd.isna(pr):
            premiums.append(float(pr))

    if not premiums:
        return 0.0, 0.03, 100.0

    avg_premium = float(np.mean(premiums))

    # 粗略估算 YTM：溢价率越高，YTM 越低（偏向股性）
    # 基于经验：溢价率 20% 对应 YTM ≈ 2%，溢价率 0 对应 YTM ≈ 4%
    ytm = max(0.01, 0.04 - avg_premium / 1000)

    # 债底（面值 100 为基准，根据市场利率估算）
    # 简化：债底 = 100 / (1 + ytm)^3（3年期）
    bond_floor = 100 / (1 + ytm) ** 3

    # IV Spread（估算：无充分数据时使用 0）
    iv_spread = 0.0

    return round(iv_spread, 4), round(ytm, 4), round(bond_floor, 2)


# ============================================================
# 综合评分
# ============================================================

def _compute_cb_score(
    common: CommonMetrics,
    delta_avg: float,
    premium_avg: float,
    iv_spread: float,
    cb_type: str,
) -> tuple[float, str]:
    """转债基金综合评分"""
    # 收益风险评分
    sharp_s = normalize_score(common.sharpe_ratio, -0.5, 2.0)
    sort_s  = normalize_score(common.sortino_ratio, -0.5, 2.5)

    # 溢价率评分（低溢价 = 便宜）
    if premium_avg <= 5:
        prem_s = 90.0
    elif premium_avg <= 15:
        prem_s = 70.0
    elif premium_avg <= 30:
        prem_s = 50.0
    else:
        prem_s = 30.0

    # 整体
    overall = sharp_s * 0.40 + sort_s * 0.30 + prem_s * 0.30
    overall = max(0.0, min(100.0, overall))

    grade = _score_to_grade(overall)
    return round(overall, 1), grade


def _score_to_grade(score: float) -> str:
    if score >= 85:  return "A+"
    elif score >= 70: return "A"
    elif score >= 55: return "B"
    elif score >= 40: return "C"
    return "D"


def _empty_cb_metrics() -> ConvertibleBondMetrics:
    return ConvertibleBondMetrics(common=CommonMetrics())
