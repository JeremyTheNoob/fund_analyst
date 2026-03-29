"""
指数/ETF 效率分析引擎 — fund_quant_v2
现金拖累 / 跟踪误差 / 折溢价分析 / 持有成本拆解 / 工具评分
"""

from __future__ import annotations
import logging
from typing import Optional

import numpy as np
import pandas as pd

from config import MODEL_CONFIG
from engine.common_metrics import (
    annualized_return, max_drawdown, volatility,
    sharpe_ratio, tracking_error, information_ratio,
    normalize_score,
)
from models.schema import (
    CleanNavData, HoldingsData, FundBasicInfo,
    CommonMetrics, IndexMetrics,
)

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def run_index_analysis(
    nav: CleanNavData,
    holdings: HoldingsData,
    basic: FundBasicInfo,
    benchmark_df: pd.DataFrame,
    etf_data: Optional[dict] = None,
) -> IndexMetrics:
    """
    指数/ETF 效率分析完整流程。

    步骤：
    1. 通用指标
    2. 跟踪误差 & 相关性
    3. 现金拖累计算
    4. 折溢价分析（ETF）
    5. 持有成本拆解
    6. 增强型基金识别 & 归因
    7. 工具推荐度评分
    """
    nav_df  = nav.df.copy()
    if nav_df.empty or "ret" not in nav_df.columns:
        return _empty_index_metrics()

    fund_rets = nav_df["ret"].values

    # 对齐基准
    bm_rets = _align_bm(nav_df, benchmark_df)

    # --- Step 1: 通用指标 ---
    common = CommonMetrics(
        annualized_return=round(annualized_return(fund_rets), 4),
        cumulative_return=round(float(np.prod(1 + fund_rets) - 1), 4),
        volatility=round(volatility(fund_rets), 4),
        max_drawdown=round(max_drawdown(fund_rets), 4),
        sharpe_ratio=round(sharpe_ratio(fund_rets), 3),
        monthly_win_rate=0.5,
    )

    # --- Step 2: 跟踪误差 & 信息比率 ---
    te       = tracking_error(fund_rets, bm_rets)
    te_ann   = te  # tracking_error 已年化
    ir       = information_ratio(fund_rets, bm_rets)
    corr     = _compute_correlation(fund_rets, bm_rets)

    # --- Step 3: 现金拖累 ---
    cash_drag = _compute_cash_drag(
        cash_ratio=holdings.cash_ratio,
        bm_annual_ret=annualized_return(bm_rets) if len(bm_rets) > 0 else 0.10,
    )

    # --- Step 4: 折溢价分析 ---
    pd_mean, pd_std, pd_grade = _analyze_premium_discount(etf_data)

    # --- Step 5: 持有成本拆解 ---
    cost_breakdown = _compute_holding_costs(
        fee_total=basic.fee_total,
        cash_drag=cash_drag,
        tracking_error_ann=te_ann,
        bm_annual_ret=annualized_return(bm_rets) if len(bm_rets) > 0 else 0.10,
    )

    # --- Step 6: 增强型识别 ---
    enhanced_return = _compute_enhanced_return(fund_rets, bm_rets)

    # --- Step 7: 工具评分 ---
    tool_score, tool_grade = _compute_tool_score(
        te_ann=te_ann,
        corr=corr,
        fee_total=basic.fee_total,
        pd_mean=pd_mean,
        pd_std=pd_std,
        enhanced_return=enhanced_return,
        is_enhanced="增强" in basic.name,
    )

    return IndexMetrics(
        common=common,
        tracking_error=round(te, 4),
        tracking_error_annualized=round(te_ann, 4),
        information_ratio=round(ir, 3),
        correlation=round(corr, 4),
        total_expense_ratio=round(basic.fee_total, 4),
        cash_drag=round(cash_drag, 4),
        rebalance_impact=round(cost_breakdown.get("rebalance", 0.0), 4),
        enhanced_return=round(enhanced_return, 4),
        premium_discount_mean=round(pd_mean, 4),
        premium_discount_std=round(pd_std, 4),
        premium_discount_grade=pd_grade,
        tool_score=round(tool_score, 1),
        tool_grade=tool_grade,
    )


# ============================================================
# 跟踪 & 相关性
# ============================================================

def _align_bm(
    nav_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> np.ndarray:
    """对齐基准收益率，返回 ndarray（与 nav_df 按日期对齐的 ret 序列）"""
    if benchmark_df is None or benchmark_df.empty:
        return np.array([])

    bm_df = benchmark_df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    nav_df = nav_df.copy()
    nav_df["date"] = pd.to_datetime(nav_df["date"])

    # 明确使用 ret 列（不要误取 close 价格列）
    if "ret" not in bm_df.columns:
        return np.array([])

    merged = nav_df[["date"]].merge(bm_df[["date", "ret"]], on="date", how="inner")
    if merged.empty:
        return np.array([])

    return merged["ret"].fillna(0).values


def _compute_correlation(fund_rets: np.ndarray, bm_rets: np.ndarray) -> float:
    """基金与基准的相关系数"""
    n = min(len(fund_rets), len(bm_rets))
    if n < 10:
        return 0.0
    try:
        return float(np.corrcoef(fund_rets[:n], bm_rets[:n])[0, 1])
    except Exception:
        return 0.0


# ============================================================
# 现金拖累
# ============================================================

def _compute_cash_drag(
    cash_ratio: float,
    bm_annual_ret: float,
    cash_yield: float = 0.015,
) -> float:
    """
    现金拖累 = 现金仓位 × (基准年化收益 - 货币收益)

    Args:
        cash_ratio: 现金占比（如 0.05 = 5%）
        bm_annual_ret: 基准年化收益
        cash_yield: 货币资金年化收益（默认 1.5%）

    Returns:
        年化现金拖累（负值表示损耗）
    """
    drag = -cash_ratio * (bm_annual_ret - cash_yield)
    return float(drag)


# ============================================================
# 折溢价分析（ETF）
# ============================================================

def _analyze_premium_discount(etf_data: Optional[dict]) -> tuple[float, float, str]:
    """
    分析 ETF 折溢价率的统计特征。

    Returns:
        (均值, 标准差, 评级)
    """
    if not etf_data or "premium_df" not in etf_data:
        return 0.0, 0.0, "无数据"

    premium_df = etf_data["premium_df"]
    if premium_df is None or premium_df.empty or "premium_pct" not in premium_df.columns:
        return 0.0, 0.0, "无数据"

    pcts = premium_df["premium_pct"].dropna()
    if len(pcts) < 10:
        return 0.0, 0.0, "数据不足"

    mean = float(pcts.mean())
    std  = float(pcts.std())

    # 折溢价评级（越稳定越好）
    # 评级标准：std < 0.1% 优秀 / < 0.3% 良好 / < 0.5% 一般 / ≥ 0.5% 较差
    if std < 0.10:
        grade = "优秀"
    elif std < 0.30:
        grade = "良好"
    elif std < 0.50:
        grade = "一般"
    else:
        grade = "较差"

    # 折溢价偏差异常检测（3σ 法则）
    outlier_threshold = MODEL_CONFIG["index"]["premium_disc_outlier"] * std
    outlier_count = int((pcts.abs() > outlier_threshold).sum())
    if outlier_count > len(pcts) * 0.05:
        grade = "一般" if grade in ("优秀", "良好") else grade

    return round(mean, 4), round(std, 4), grade


# ============================================================
# 持有成本拆解
# ============================================================

def _compute_holding_costs(
    fee_total: float,
    cash_drag: float,
    tracking_error_ann: float,
    bm_annual_ret: float,
) -> dict:
    """
    持有成本拆解：
    - 管理损耗：费率（管理费 + 托管费）
    - 现金拖累：现金仓位造成的收益损失
    - 调仓磨损：跟踪误差中不可解释的部分
    - 增强补偿：增强型基金的超额收益（抵消损耗）
    """
    management_drag = -fee_total                      # 负值（损耗）
    cash_drag_val   = cash_drag                       # 通常为负

    # 调仓磨损：近似为跟踪误差扣除费率损耗后的残差
    rebalance_drag = -(tracking_error_ann * 0.3)      # 经验系数

    return {
        "management":  round(management_drag, 4),
        "cash_drag":   round(cash_drag_val, 4),
        "rebalance":   round(rebalance_drag, 4),
        "total_cost":  round(management_drag + cash_drag_val + rebalance_drag, 4),
    }


# ============================================================
# 增强型基金归因
# ============================================================

def _compute_enhanced_return(
    fund_rets: np.ndarray,
    bm_rets: np.ndarray,
) -> float:
    """
    增强型基金超额收益（年化）。
    = 年化(基金) - 年化(基准)
    """
    n = min(len(fund_rets), len(bm_rets))
    if n < 20:
        return 0.0
    fund_ann = annualized_return(fund_rets[:n])
    bm_ann   = annualized_return(bm_rets[:n])
    return float(fund_ann - bm_ann)


# ============================================================
# 工具推荐度评分
# ============================================================

def _compute_tool_score(
    te_ann: float,
    corr: float,
    fee_total: float,
    pd_mean: float,
    pd_std: float,
    enhanced_return: float = 0.0,
    is_enhanced: bool = False,
) -> tuple[float, str]:
    """
    工具推荐度评分（四维：精度/费率/折溢价/增强）。

    Returns:
        (评分 0-100, 评级 A+/A/B/C/D)
    """
    # 1. 跟踪精度（权重 40% 普通 / 20% 增强）
    corr_s = normalize_score(corr, 0.7, 1.0)
    te_s   = normalize_score(te_ann * 100, 0.0, 3.0, invert=True)  # TE 以 % 为单位
    precision = corr_s * 0.5 + te_s * 0.5

    # 2. 费率（权重 30%）
    fee_pct = fee_total * 100  # 转百分比
    fee_s   = normalize_score(fee_pct, 0.0, 1.5, invert=True)

    # 3. 折溢价（权重 20%；无数据时取中性分）
    if pd_std > 0:
        pd_s = normalize_score(pd_std, 0.0, 1.0, invert=True)
    else:
        pd_s = 70.0

    # 4. 增强效果（权重 10%；普通指数基金不计）
    if is_enhanced and enhanced_return != 0:
        enh_s = normalize_score(enhanced_return * 100, -1.0, 3.0)
    else:
        enh_s = 50.0

    if is_enhanced:
        score = precision * 0.30 + fee_s * 0.25 + pd_s * 0.15 + enh_s * 0.30
    else:
        score = precision * 0.40 + fee_s * 0.30 + pd_s * 0.20 + enh_s * 0.10

    score = max(0.0, min(100.0, score))
    grade = _score_to_grade(score)
    return round(score, 1), grade


def _score_to_grade(score: float) -> str:
    if score >= 85:  return "A+"
    elif score >= 70: return "A"
    elif score >= 55: return "B"
    elif score >= 40: return "C"
    return "D"


def _empty_index_metrics() -> IndexMetrics:
    return IndexMetrics(common=CommonMetrics())
