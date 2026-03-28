"""
固收类分析引擎 — fund_quant_v2
三因子回归 / 久期估算 / WACS 评分 / HHI 集中度 / 压力测试

关键修复（旧系统 Bug）：
- 旧版久期硬编码 3.0 年（bond_model.py line 174），本版从持仓计算加权平均久期
"""

from __future__ import annotations
import logging
from typing import Optional

import numpy as np
import pandas as pd
import statsmodels.api as sm

from config import MODEL_CONFIG, RISK_THRESHOLDS
from engine.common_metrics import (
    annualized_return, cumulative_return, max_drawdown,
    max_drawdown_duration, recovery_days, volatility,
    sharpe_ratio, sortino_ratio, calmar_ratio,
    information_ratio, tracking_error, skewness, kurtosis,
    monthly_win_rate, normalize_score,
)
from models.schema import (
    CleanBondData, HoldingsData,
    CommonMetrics, BondMetrics,
)

logger = logging.getLogger(__name__)

# 债券期限 → 久期近似值（年）
BOND_MATURITY_DURATION_MAP = {
    "国债":     {"1Y": 0.9, "3Y": 2.7, "5Y": 4.4, "7Y": 6.0, "10Y": 8.5, "30Y": 18.0},
    "政金债":   {"1Y": 0.9, "3Y": 2.6, "5Y": 4.2, "7Y": 5.8, "10Y": 8.0},
    "信用债":   {"1Y": 0.9, "2Y": 1.8, "3Y": 2.5, "5Y": 4.0},
    "城投债":   {"1Y": 0.9, "2Y": 1.7, "3Y": 2.4, "5Y": 3.8},
    "可转债":   {"3Y": 0.5, "5Y": 0.8},  # 转债久期短（含期权）
    "同业存单": {"1Y": 0.9, "6M": 0.5, "3M": 0.25},
}


# ============================================================
# 主入口
# ============================================================

def run_bond_analysis(
    clean_data: CleanBondData,
    holdings: HoldingsData,
    fund_type: str = "bond",
) -> BondMetrics:
    """
    纯债 / 固收类基金完整分析流程。

    步骤：
    1. 通用指标
    2. 三因子回归（短端利率 + 长端利率 + 信用利差）
    3. 久期估算（从持仓计算加权平均，修复旧 Bug）
    4. WACS 信用评分
    5. HHI 持仓集中度
    6. 压力测试
    7. 综合评分
    """
    nav_df   = clean_data.nav_df.copy()
    yield_df = clean_data.yield_df.copy()

    if nav_df.empty or "ret" not in nav_df.columns:
        return _empty_bond_metrics()

    fund_rets = nav_df["ret"].values
    dates     = pd.DatetimeIndex(nav_df["date"]) if "date" in nav_df.columns else None

    # --- Step 1: 通用指标 ---
    common = _compute_bond_common_metrics(fund_rets, dates)

    # --- Step 2: 三因子回归 ---
    ff3_result = _run_bond_three_factor(nav_df, yield_df)

    # --- Step 2.5: 信用利差历史数据（用于图表）---
    # 从 yield_df 中提取信用利差序列，传递给 BondMetrics
    credit_spread_history = None
    if "credit_spread" in yield_df.columns and not yield_df.empty:
        # 按日期排序
        yield_sorted = yield_df.sort_values("date")
        # 提取日期和利差列
        credit_spread_history = yield_sorted[["date", "credit_spread"]].copy()
        logger.info(f"[_run_bond_analysis] 提取信用利差历史数据: {len(credit_spread_history)} 条记录")

    # --- Step 3: 久期估算（修复硬编码 Bug）---
    duration, convexity = _estimate_duration_from_holdings(holdings)

    # --- Step 4: WACS 信用评分 ---
    wacs, credit_breakdown = _compute_wacs(holdings)

    # --- Step 5: HHI 集中度 ---
    hhi = _compute_bond_hhi(holdings)

    # --- Step 6: 压力测试 ---
    stress = _run_stress_test(duration, convexity, yield_df)

    # --- Step 7: 综合评分 ---
    overall, grade = _compute_bond_score(common, wacs, hhi, duration, stress)

    # --- Step 8: 提取信用利差历史数据（用于信用利差走势图）---
    credit_spread_history = None
    if not yield_df.empty and "credit_spread" in yield_df.columns:
        try:
            # 提取日期和信用利差数据
            spread_df = yield_df[["date", "credit_spread"]].copy()
            spread_df["date"] = pd.to_datetime(spread_df["date"])
            spread_df = spread_df.sort_values("date")
            # 重命名列以匹配图表函数期望的格式
            spread_df = spread_df.rename(columns={"credit_spread": "spread"})
            # 移除 NaN 值
            spread_df = spread_df.dropna(subset=["spread"])
            credit_spread_history = spread_df
            logger.info(f"[run_bond_analysis] 提取信用利差历史数据: {len(credit_spread_history)} 条记录")
        except Exception as e:
            logger.warning(f"[run_bond_analysis] 提取信用利差历史数据失败: {e}")

    return BondMetrics(
        common=common,
        alpha_bond=ff3_result.get("alpha", 0.0),
        factor_loadings={
            "short_rate": ff3_result.get("b_short", 0.0),
            "long_rate":  ff3_result.get("b_long", 0.0),
            "credit":     ff3_result.get("b_credit", 0.0),
        },
        r_squared=ff3_result.get("r_squared", 0.0),
        duration=round(duration, 2),
        convexity=round(convexity, 4),
        wacs_score=round(wacs, 1),
        credit_breakdown=credit_breakdown,
        hhi=round(hhi, 1),
        stress_results=stress,
        credit_spread_history=credit_spread_history,  # 新增：信用利差历史数据
        overall_score=round(overall, 1),
        score_grade=grade,
    )


# ============================================================
# 三因子回归
# ============================================================

def _run_bond_three_factor(
    nav_df: pd.DataFrame,
    yield_df: pd.DataFrame,
) -> dict:
    """
    债券三因子回归。
    因子：
    - dY2  = 2Y国债收益率变化（短端利率因子）
    - dY10 = 10Y国债收益率变化（长端利率因子）
    - dCS  = 信用利差变化（信用风险因子，使用真实数据，修复旧 Bug）
    """
    if nav_df.empty or yield_df.empty:
        return {"alpha": 0.0, "b_short": 0.0, "b_long": 0.0, "b_credit": 0.0, "r_squared": 0.0}

    nav_df   = nav_df.copy()
    yield_df = yield_df.copy()
    nav_df["date"]   = pd.to_datetime(nav_df["date"])
    yield_df["date"] = pd.to_datetime(yield_df["date"])

    # 构建因子变化序列
    yield_df_sorted = yield_df.sort_values("date")
    if "yield_2y" in yield_df.columns:
        yield_df_sorted["dY2"] = yield_df_sorted["yield_2y"].diff()
    else:
        yield_df_sorted["dY2"] = 0.0

    if "yield_10y" in yield_df.columns:
        yield_df_sorted["dY10"] = yield_df_sorted["yield_10y"].diff()
    else:
        yield_df_sorted["dY10"] = 0.0

    if "credit_spread" in yield_df.columns:
        yield_df_sorted["dCS"] = yield_df_sorted["credit_spread"].diff()
    else:
        yield_df_sorted["dCS"] = 0.0

    merged = nav_df[["date", "ret"]].merge(
        yield_df_sorted[["date", "dY2", "dY10", "dCS"]],
        on="date", how="inner"
    ).dropna()

    if len(merged) < 30:
        logger.warning("[_run_bond_three_factor] 样本量不足，跳过三因子回归")
        return {"alpha": 0.0, "b_short": 0.0, "b_long": 0.0, "b_credit": 0.0, "r_squared": 0.0}

    y = merged["ret"].values
    X = sm.add_constant(merged[["dY2", "dY10", "dCS"]].values)

    try:
        result = sm.OLS(y, X).fit()
        params = result.params
        return {
            "alpha":    round(float(params[0]) * 252, 4),
            "b_short":  round(float(params[1]), 4),
            "b_long":   round(float(params[2]), 4),
            "b_credit": round(float(params[3]), 4),
            "r_squared": round(float(result.rsquared), 4),
        }
    except Exception as e:
        logger.warning(f"[_run_bond_three_factor] OLS 失败: {e}")
        return {"alpha": 0.0, "b_short": 0.0, "b_long": 0.0, "b_credit": 0.0, "r_squared": 0.0}


# ============================================================
# 久期估算（修复旧系统硬编码 Bug）
# ============================================================

def _estimate_duration_from_holdings(holdings: HoldingsData) -> tuple[float, float]:
    """
    从持仓数据估算加权平均久期和凸性。

    旧系统 Bug（bond_model.py lines 174-176）：
        duration_est = 3.0  # 硬编码！
        convexity    = duration_est ** 2 / 100  # = 0.09 对所有基金都一样

    本版策略：
    1. 解析债券持仓的名称，判断期限类型
    2. 查找 BOND_MATURITY_DURATION_MAP 获取期限对应久期
    3. 按持仓比例加权计算
    4. 若数据不足，使用更合理的分类型默认值（而非统一 3.0）
    """
    bond_details = holdings.bond_details
    if not bond_details:
        # 数据不足时按基金类型使用合理默认值
        # 纯债基金平均久期约 3-5 年（中等久期）
        default_dur = 3.5
        return default_dur, default_dur ** 2 / 100

    durations = []
    weights   = []

    for bond in bond_details:
        name  = str(bond.get("债券名称", "") or "")
        ratio = float(bond.get("占净值比例", 0) or 0)
        if ratio <= 0:
            continue

        dur = _infer_bond_duration(name)
        durations.append(dur)
        weights.append(ratio)

    if not durations:
        default_dur = 3.5
        return default_dur, default_dur ** 2 / 100

    total_w = sum(weights)
    if total_w <= 0:
        default_dur = 3.5
        return default_dur, default_dur ** 2 / 100

    # 加权平均久期
    wav_duration = sum(d * w / total_w for d, w in zip(durations, weights))
    wav_duration = min(max(wav_duration, 0.1), MODEL_CONFIG["duration"]["max_duration"])

    # 凸性：近似公式 = D^2 / 100（修正版，非旧版硬编码 0.09）
    convexity = wav_duration ** 2 / 100

    return round(wav_duration, 2), round(convexity, 4)


def _infer_bond_duration(bond_name: str) -> float:
    """
    从债券名称推断久期（年）。
    规则：关键词 + 期限匹配。
    """
    name = bond_name.lower()

    # 同业存单（极短期）
    if "同业存单" in bond_name or "NCD" in bond_name:
        if "6M" in bond_name or "半年" in bond_name:
            return 0.45
        if "3M" in bond_name or "季度" in bond_name:
            return 0.25
        return 0.8  # 1年期最常见

    # 可转债（含权，久期短）
    if "可转债" in bond_name or "转债" in bond_name:
        return 0.6

    # 根据名称中的期限数字推断
    import re
    match = re.search(r"(\d+)(年|Y)", bond_name)
    if match:
        years = int(match.group(1))
        if years <= 1:
            return 0.9
        elif years <= 3:
            return years * 0.87
        elif years <= 5:
            return years * 0.86
        elif years <= 10:
            return years * 0.84
        else:
            return years * 0.60

    # 默认：中短期信用债（最常见）
    return 2.5


# ============================================================
# WACS 信用评分
# ============================================================

def _compute_wacs(holdings: HoldingsData) -> tuple[float, dict]:
    """
    加权平均信用评分（WACS）。
    映射：AAA=100, AA+=80, AA=60, AA-=40, A+=20, A=10
    """
    wacs_map   = MODEL_CONFIG["wacs_map"]
    bond_details = holdings.bond_details

    if not bond_details:
        return 60.0, {}  # 默认中等信用

    scores  = []
    weights = []
    credit_count: dict[str, float] = {}

    for bond in bond_details:
        name   = str(bond.get("债券名称", "") or "")
        rating = str(bond.get("信用等级", "") or bond.get("评级", "") or "")
        ratio  = float(bond.get("占净值比例", 0) or 0)

        if ratio <= 0:
            continue

        # 从评级或名称推断信用等级
        credit_score = _infer_credit_score(name, rating, wacs_map)
        scores.append(credit_score)
        weights.append(ratio)

        # 统计各评级分布
        label = _credit_label(credit_score)
        credit_count[label] = credit_count.get(label, 0) + ratio

    if not scores:
        return 60.0, {}

    total_w = sum(weights)
    if total_w <= 0:
        return 60.0, {}

    wacs = sum(s * w / total_w for s, w in zip(scores, weights))
    return round(wacs, 1), credit_count


def _infer_credit_score(name: str, rating: str, wacs_map: dict) -> float:
    """从评级字符串推断 WACS 分数"""
    for rating_key, score in wacs_map.items():
        if rating_key in rating:
            return score

    # 从债券名称推断
    if "国债" in name or "政金债" in name or "国开" in name or "农发" in name or "口行" in name:
        return 100.0  # 国家信用
    if "城投" in name:
        return 50.0   # 城投债中等偏下
    if "可转债" in name or "转债" in name:
        return 60.0

    return 60.0  # 默认 AA 级


def _credit_label(score: float) -> str:
    if score >= 95:  return "AAA"
    if score >= 75:  return "AA+"
    if score >= 55:  return "AA"
    if score >= 35:  return "AA-"
    return "A级及以下"


# ============================================================
# HHI 持仓集中度
# ============================================================

def _compute_bond_hhi(holdings: HoldingsData) -> float:
    """
    债券持仓 HHI 集中度指数。
    HHI = Σ(wi²)×10000，越大越集中。
    """
    bond_details = holdings.bond_details
    if not bond_details:
        return 500.0  # 默认中等集中度

    ratios = []
    for bond in bond_details:
        ratio = float(bond.get("占净值比例", 0) or 0)
        if ratio > 0:
            ratios.append(ratio / 100.0)  # 转为小数

    if not ratios:
        return 500.0

    total = sum(ratios)
    if total <= 0:
        return 500.0

    normalized = [r / total for r in ratios]
    hhi = sum(r ** 2 for r in normalized) * 10000
    return round(hhi, 1)


# ============================================================
# 压力测试
# ============================================================

def _run_stress_test(
    duration: float,
    convexity: float,
    yield_df: pd.DataFrame,
) -> list[dict]:
    """
    利率压力测试：四情景（资金面收紧/债市熊平/信用风险/极端冲击）。

    价格变化近似：
    ΔP/P ≈ -D × Δy + 0.5 × C × Δy²
    """
    results = []
    for scenario in MODEL_CONFIG["stress_scenarios"]:
        # 利率敏感性（用10Y代表组合）
        delta_y_long   = scenario["long_bp"] / 10000  # BP → 小数
        delta_y_credit = scenario["credit_bp"] / 10000

        # 久期效应（主要）
        price_change_rate  = -duration * delta_y_long
        # 凸性修正（二阶）
        price_change_convex = 0.5 * convexity * delta_y_long ** 2
        # 信用利差冲击（用持仓中非国债部分承受）
        credit_impact = -duration * 0.6 * delta_y_credit  # 60% 非国债假设

        total_impact = price_change_rate + price_change_convex + credit_impact

        results.append({
            "scenario":      scenario["name"],
            "long_bp":       scenario["long_bp"],
            "credit_bp":     scenario["credit_bp"],
            "price_impact":  round(total_impact * 100, 2),  # 百分比
            "duration_used": duration,
        })

    return results


# ============================================================
# 综合评分
# ============================================================

def _compute_bond_score(
    common: CommonMetrics,
    wacs: float,
    hhi: float,
    duration: float,
    stress: list,
) -> tuple[float, str]:
    """
    纯债基金综合评分。
    S = 0.4×信用评分 + 0.3×集中度评分 + 0.3×结构评分
    """
    cfg = MODEL_CONFIG["bond_scoring"]

    # 信用评分（WACS 线性映射到 0-100）
    credit_s = normalize_score(wacs, 0, 100)

    # 集中度评分（HHI 越小越好）
    conc_s = normalize_score(hhi, 0, 3000, invert=True)

    # 结构评分（夏普 + 最大回撤）
    sharp_s = normalize_score(common.sharpe_ratio, -1.0, 2.5)
    mdd_s   = normalize_score(abs(common.max_drawdown), 0, 0.20, invert=True)
    struct_s = sharp_s * 0.5 + mdd_s * 0.5

    overall = (
        cfg["credit"]    * credit_s
        + cfg["conc"]    * conc_s
        + cfg["structure"] * struct_s
    )

    # 综合风险指数 R（一票否决）
    R = duration * hhi * (100 - wacs)
    if R > RISK_THRESHOLDS["bond_risk_veto"]:
        overall = min(overall, 40.0)  # 一票否决，最高 C 级

    grade = _score_to_grade(overall)
    return round(overall, 1), grade


def _compute_bond_common_metrics(
    fund_rets: np.ndarray,
    dates: Optional[pd.DatetimeIndex],
) -> CommonMetrics:
    """债券基金通用指标（不需要基准）"""
    return CommonMetrics(
        annualized_return=round(annualized_return(fund_rets), 4),
        cumulative_return=round(cumulative_return(fund_rets), 4),
        volatility=round(volatility(fund_rets), 4),
        max_drawdown=round(max_drawdown(fund_rets), 4),
        max_drawdown_duration=int(max_drawdown_duration(fund_rets)),
        recovery_days=recovery_days(fund_rets),
        sharpe_ratio=round(sharpe_ratio(fund_rets), 3),
        sortino_ratio=round(sortino_ratio(fund_rets), 3),
        calmar_ratio=round(calmar_ratio(fund_rets), 3),
        skewness=round(skewness(fund_rets), 3),
        kurtosis=round(kurtosis(fund_rets), 3),
        monthly_win_rate=0.5,  # 纯债通常与自身比较，胜率意义不大
    )


def _score_to_grade(score: float) -> str:
    if score >= 85:  return "A+"
    elif score >= 70: return "A"
    elif score >= 55: return "B"
    elif score >= 40: return "C"
    return "D"


def _empty_bond_metrics() -> BondMetrics:
    return BondMetrics(common=CommonMetrics())
