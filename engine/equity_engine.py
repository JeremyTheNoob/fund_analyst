"""
权益类分析引擎 — fund_quant_v2
FF 因子回归 / Brinson 归因 / 风格分析 / 雷达图评分
"""

from __future__ import annotations
import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm

from config import MODEL_CONFIG, RADAR_WEIGHTS, RISK_THRESHOLDS
from engine.common_metrics import (
    annualized_return, cumulative_return, max_drawdown,
    max_drawdown_duration, recovery_days, volatility,
    sharpe_ratio, sortino_ratio, calmar_ratio,
    information_ratio, tracking_error, beta, skewness, kurtosis, monthly_win_rate,
    normalize_score,
)
from utils.common import FinancialConfig
from models.schema import (
    CleanNavData, FactorData, HoldingsData, BenchmarkData,
    CommonMetrics, EquityMetrics,
)
from utils.common import audit_logger

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

@audit_logger
def run_equity_analysis(
    nav: CleanNavData,
    factors: FactorData,
    holdings: HoldingsData,
    benchmark: BenchmarkData,
    fund_type: str = "equity",
) -> EquityMetrics:
    """
    权益类基金完整分析流程。

    步骤：
    1. 通用指标计算
    2. FF 因子回归（自动选择 CAPM/FF3/FF5/Carhart）
    3. Brinson 归因
    4. 风格分析（滚动 Beta）
    5. 雷达图评分
    6. 综合评分
    """
    nav_df = nav.df.copy()

    if nav_df.empty or "ret" not in nav_df.columns:
        return _empty_equity_metrics()

    fund_ret  = nav_df.set_index("date")["ret"]
    fund_rets = fund_ret.values
    dates     = pd.DatetimeIndex(nav_df["date"])

    # --- 基准对齐 ---
    bm_rets = _align_benchmark(fund_ret, benchmark)

    # --- Step 1: 通用指标 ---
    common = _compute_common_metrics(fund_rets, bm_rets, dates)

    # --- Step 2: FF 因子回归 ---
    ff_result = _run_ff_regression(fund_ret, factors)

    # --- Step 3: Brinson 归因 ---
    brinson = _run_brinson(fund_ret, bm_rets, holdings)

    # --- Step 4: 滚动 Beta 风格分析 ---
    rolling_beta_20, rolling_beta_60, style_drift = _analyze_style(fund_ret, factors)

    # --- Step 5: 跟踪误差 & 信息比率 ---
    te  = tracking_error(fund_rets, bm_rets)
    ir  = information_ratio(fund_rets, bm_rets)

    # --- Step 6: 雷达图评分 ---
    radar = _compute_radar_scores(
        common=common,
        ff_result=ff_result,
        fund_rets=fund_rets,
        bm_rets=bm_rets,
        fund_type=fund_type,
    )

    # --- Step 7: 综合评分 ---
    weights = RADAR_WEIGHTS.get(fund_type, RADAR_WEIGHTS["others"])
    overall = sum(radar.get(k, 50) * v for k, v in weights.items())
    grade   = _score_to_grade(overall)
    
    # --- Step 8: 风格箱（Morningstar Style Box）---
    style_box = _compute_style_box(ff_result)

    return EquityMetrics(
        common=common,
        model_type=ff_result.get("model_type", "ff3"),
        alpha=ff_result.get("alpha", 0.0),
        beta=ff_result.get("beta_mkt", 0.0),
        r_squared=ff_result.get("r_squared", 0.0),
        factor_loadings={
            k: ff_result.get(k, 0.0)
            for k in ["SMB", "HML", "Short_MOM", "RMW"]
            if k in ff_result
        },
        information_ratio=ir,
        tracking_error=te,
        brinson=brinson,
        style_drift_flag=style_drift,
        rolling_beta_20d=rolling_beta_20,
        rolling_beta_60d=rolling_beta_60,
        radar_scores=radar,
        style_box=style_box,  # 新增：风格箱数据
        overall_score=round(overall, 1),
        score_grade=grade,
    )


# ============================================================
# FF 因子回归
# ============================================================

def _run_ff_regression(
    fund_ret: pd.Series,
    factors: FactorData,
) -> dict:
    """
    多因子 OLS 回归，自动选择最优模型。
    优先级：Carhart(有RMW) > FF5(有RMW) > FF3 > CAPM
    """
    ff_df = factors.df.copy()
    ff_df["date"] = pd.to_datetime(ff_df["date"])

    fund_df = pd.DataFrame({"date": fund_ret.index, "ret": fund_ret.values})
    fund_df["date"] = pd.to_datetime(fund_df["date"])

    merged = fund_df.merge(ff_df, on="date", how="inner").dropna()

    if len(merged) < MODEL_CONFIG["ff"]["min_obs"]:
        logger.warning(f"[_run_ff_regression] 样本量不足（{len(merged)}），跳过回归")
        return {"model_type": "capm", "alpha": 0.0, "beta_mkt": 1.0, "r_squared": 0.0}

    # 选择因子组合
    available = set(merged.columns)
    has_rmw = "RMW" in available
    has_mom = "Short_MOM" in available

    if has_rmw and has_mom:
        factor_cols = ["Mkt", "SMB", "HML", "Short_MOM", "RMW"]
        model_type  = "carhart"
    elif has_rmw:
        factor_cols = ["Mkt", "SMB", "HML", "RMW"]
        model_type  = "ff5"
    elif "SMB" in available and "HML" in available:
        factor_cols = ["Mkt", "SMB", "HML"]
        model_type  = "ff3"
    else:
        factor_cols = ["Mkt"]
        model_type  = "capm"

    # 检查 RMW 缺失比例
    if "RMW" in factor_cols:
        rmw_missing = merged["RMW"].isna().mean()
        if rmw_missing > MODEL_CONFIG["ff"]["rmw_missing_drop"]:
            factor_cols = [c for c in factor_cols if c != "RMW"]
            if "Short_MOM" in factor_cols:
                model_type = "carhart"
            else:
                model_type = "ff3" if "SMB" in factor_cols else "capm"

    y = merged["ret"].values
    X = sm.add_constant(merged[factor_cols].values)

    try:
        result = sm.OLS(y, X).fit()
    except Exception as e:
        logger.warning(f"[_run_ff_regression] OLS 失败: {e}")
        return {"model_type": model_type, "alpha": 0.0, "beta_mkt": 1.0, "r_squared": 0.0}

    params = result.params
    # alpha 日频 → 年化
    alpha_annual = float(params[0]) * FinancialConfig.TRADING_DAYS_YEAR
    beta_mkt     = float(params[1]) if len(params) > 1 else 1.0

    factor_loadings = {
        fc: float(params[i + 1])
        for i, fc in enumerate(factor_cols[1:])
        if i + 1 < len(params) - 1
    }

    return {
        "model_type":    model_type,
        "alpha":         round(alpha_annual, 4),
        "beta_mkt":      round(beta_mkt, 4),
        "r_squared":     round(float(result.rsquared), 4),
        **{k: round(v, 4) for k, v in factor_loadings.items()},
        "SMB":       round(float(params[factor_cols.index("SMB") + 1] if "SMB" in factor_cols else 0), 4),
        "HML":       round(float(params[factor_cols.index("HML") + 1] if "HML" in factor_cols else 0), 4),
        "Short_MOM": round(float(params[factor_cols.index("Short_MOM") + 1] if "Short_MOM" in factor_cols else 0), 4),
        "RMW":       round(float(params[factor_cols.index("RMW") + 1] if "RMW" in factor_cols else 0), 4),
    }


# ============================================================
# Brinson 归因（修复债券贡献=0的旧Bug）
# ============================================================

def _run_brinson(
    fund_ret: pd.Series,
    bm_rets: np.ndarray,
    holdings: HoldingsData,
) -> dict:
    """
    Brinson 资产配置归因。

    修复旧系统 Bug（models/equity_model.py line 436）：
    旧版债券贡献硬编码为 0，本版使用真实债券基准估算。
    债券基准：中债综合指数历史年化收益约 3.5%（经验值，可后续接入真实数据）
    """
    try:
        stock_ratio = holdings.stock_ratio
        bond_ratio  = holdings.bond_ratio

        # 基准权重（经验值：偏股混合基金的基准约 60% 股票 + 40% 债券）
        cfg = MODEL_CONFIG["brinson"]
        bm_stock_weight = 1 - cfg["default_bond_bm_weight"]
        bm_bond_weight  = cfg["default_bond_bm_weight"]

        # 基准年化收益（股票用沪深300，债券用中债综合）
        fund_annual = annualized_return(fund_ret.values)
        # 基准股票收益：从 bm_rets 估算
        bm_annual = annualized_return(bm_rets) if len(bm_rets) > 0 else 0.08
        # 债券基准年化（中债综合历史约 3.5%）
        bond_bm_annual = 0.035

        # 配置效应 = (实际权重 - 基准权重) × 基准收益
        allocation = (
            (stock_ratio - bm_stock_weight) * bm_annual
            + (bond_ratio  - bm_bond_weight)  * bond_bm_annual  # 修复：非 0
        )

        # 选股效应 = 基准权重 × (实际收益 - 基准收益)
        selection = (
            bm_stock_weight * (fund_annual - bm_annual)
            + bm_bond_weight  * 0  # 债券选券效应（暂无数据）
        )

        # 交互效应
        interaction = (stock_ratio - bm_stock_weight) * (fund_annual - bm_annual)

        return {
            "allocation":  round(allocation, 4),
            "selection":   round(selection, 4),
            "interaction": round(interaction, 4),
            "total":       round(allocation + selection + interaction, 4),
        }
    except Exception as e:
        logger.warning(f"[_run_brinson] 归因计算失败: {e}")
        return {"allocation": 0.0, "selection": 0.0, "interaction": 0.0, "total": 0.0}


# ============================================================
# 风格分析（滚动 Beta）
# ============================================================

def _analyze_style(
    fund_ret: pd.Series,
    factors: FactorData,
) -> tuple[list, list, bool]:
    """
    滚动 Beta 风格分析。
    返回：rolling_20d / rolling_60d / style_drift_flag
    """
    ff_df = factors.df.copy()
    ff_df["date"] = pd.to_datetime(ff_df["date"])
    fund_df = pd.DataFrame({"date": fund_ret.index, "ret": fund_ret.values})
    fund_df["date"] = pd.to_datetime(fund_df["date"])

    merged = fund_df.merge(ff_df[["date", "Mkt"]], on="date", how="inner").dropna()

    if len(merged) < 30:
        return [], [], False

    rolling_20 = []
    rolling_60 = []

    for w in [20, 60]:
        betas = []
        for i in range(w, len(merged)):
            window = merged.iloc[i - w:i]
            if len(window) < w:
                continue
            b = beta(window["ret"].values, window["Mkt"].values)
            betas.append(round(b, 3))
        if w == 20:
            rolling_20 = betas
        else:
            rolling_60 = betas

    # 风格漂移检测：近20日Beta与全期Beta偏差是否超过阈值
    style_drift = False
    if rolling_20 and rolling_60:
        full_beta   = beta(merged["ret"].values, merged["Mkt"].values)
        recent_beta = np.mean(rolling_20[-20:]) if len(rolling_20) >= 20 else rolling_20[-1]
        drift_threshold = RISK_THRESHOLDS["style_drift"]
        if abs(recent_beta - full_beta) > drift_threshold:
            style_drift = True

    return rolling_20, rolling_60, style_drift


# ============================================================
# 雷达图评分
# ============================================================

def _compute_radar_scores(
    common: CommonMetrics,
    ff_result: dict,
    fund_rets: np.ndarray,
    bm_rets: np.ndarray,
    fund_type: str = "equity",
) -> dict[str, float]:
    """
    五维雷达图评分（0-100）：
    超额能力 / 风险控制 / 性价比 / 风格稳定 / 业绩持续
    """
    # 1. 超额能力（Alpha + IR）
    alpha   = ff_result.get("alpha", 0.0)
    ir      = information_ratio(fund_rets, bm_rets)
    alpha_s = normalize_score(alpha, -0.10, 0.15)
    ir_s    = normalize_score(ir, -1.0, 2.0)
    excess  = round(alpha_s * 0.6 + ir_s * 0.4, 1)

    # 2. 风险控制（最大回撤 + 波动率）
    mdd_s   = normalize_score(abs(common.max_drawdown), 0, 0.50, invert=True)
    vol_s   = normalize_score(common.volatility, 0.05, 0.40, invert=True)
    risk    = round(mdd_s * 0.6 + vol_s * 0.4, 1)

    # 3. 性价比（夏普 + 卡玛）
    sharp_s = normalize_score(common.sharpe_ratio, -1.0, 2.5)
    cal_s   = normalize_score(common.calmar_ratio if hasattr(common, "calmar_ratio") else 0, 0, 3.0)
    value   = round(sharp_s * 0.6 + cal_s * 0.4, 1)

    # 4. 风格稳定（R² + 无漂移）
    r2      = ff_result.get("r_squared", 0.5)
    r2_s    = normalize_score(r2, 0.1, 0.9)
    style   = round(r2_s, 1)

    # 5. 业绩持续（月度胜率）
    wr      = common.monthly_win_rate
    wr_s    = normalize_score(wr, 0.3, 0.7)
    persist = round(wr_s, 1)

    return {
        "超额能力": excess,
        "风险控制": risk,
        "性价比":   value,
        "风格稳定": style,
        "业绩持续": persist,
    }


# ============================================================
# 通用指标计算
# ============================================================

def _compute_common_metrics(
    fund_rets: np.ndarray,
    bm_rets: np.ndarray,
    dates: pd.DatetimeIndex,
) -> CommonMetrics:
    """计算通用量化指标"""
    ann_ret = annualized_return(fund_rets)
    cum_ret = cumulative_return(fund_rets)
    mdd     = max_drawdown(fund_rets)
    mdd_dur = max_drawdown_duration(fund_rets)
    rec_d   = recovery_days(fund_rets)
    vol     = volatility(fund_rets)
    sharp   = sharpe_ratio(fund_rets)
    sort    = sortino_ratio(fund_rets)
    calmar  = calmar_ratio(fund_rets)
    skew    = skewness(fund_rets)
    kurt    = kurtosis(fund_rets)
    wr      = monthly_win_rate(fund_rets, bm_rets, dates=dates) if len(bm_rets) > 0 else 0.5

    return CommonMetrics(
        annualized_return=round(ann_ret, 4),
        cumulative_return=round(cum_ret, 4),
        volatility=round(vol, 4),
        max_drawdown=round(mdd, 4),
        max_drawdown_duration=int(mdd_dur),
        recovery_days=int(rec_d) if rec_d is not None else None,
        sharpe_ratio=round(sharp, 3),
        sortino_ratio=round(sort, 3),
        calmar_ratio=round(calmar, 3),
        skewness=round(skew, 3),
        kurtosis=round(kurt, 3),
        monthly_win_rate=round(wr, 3),
    )


def _align_benchmark(
    fund_ret: pd.Series,
    benchmark: BenchmarkData,
) -> np.ndarray:
    """
    对齐基准收益率序列，返回与 fund_ret 等长的 ndarray
    
    新增功能：
    - 对齐率低于 95% 时记录警告
    - 对齐天数少于 10 天时记录警告并返回零数组
    """
    if benchmark.df.empty:
        logger.warning("[_align_benchmark] 基准数据为空")
        return np.zeros(len(fund_ret))

    bm_df = benchmark.df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    fund_dates = pd.to_datetime(fund_ret.index)

    bm_s = bm_df.set_index("date")["bm_ret"]
    common_dates = fund_dates.intersection(bm_s.index)
    
    # 计算对齐率
    alignment_rate = len(common_dates) / len(fund_dates) if len(fund_dates) > 0 else 0.0
    
    # 检查对齐率是否低于 95%
    if alignment_rate < 0.95:
        logger.warning(
            f"[_align_benchmark] 基准对齐率较低: {alignment_rate:.1%} "
            f"({len(common_dates)}/{len(fund_dates)} 天)，可能影响分析结果的准确性"
        )
    
    # 检查对齐天数是否过少
    if len(common_dates) < 10:
        logger.warning(
            f"[_align_benchmark] 基准对齐天数不足: {len(common_dates)} 天 < 10 天，"
            "将返回零数组，分析结果的可靠性较低"
        )
        return np.zeros(len(fund_ret))

    aligned_bm = bm_s.reindex(fund_dates).fillna(0)
    return aligned_bm.values


def _score_to_grade(score: float) -> str:
    if score >= 85:
        return "A+"
    elif score >= 70:
        return "A"
    elif score >= 55:
        return "B"
    elif score >= 40:
        return "C"
    return "D"


def _compute_style_box(ff_result: dict) -> dict:
    """
    计算 Morningstar 风格箱
    
    Args:
        ff_result: FF 因子回归结果，包含 SMB 和 HML 系数
    
    Returns:
        {
            "size": 1-3 (1=大盘, 2=中盘, 3=小盘),
            "style": 1-3 (1=价值, 2=平衡, 3=成长),
            "smb_coef": SMB系数,
            "hml_coef": HML系数,
        }
    """
    smb_coef = ff_result.get("SMB", 0.0)
    hml_coef = ff_result.get("HML", 0.0)
    
    # 市值分类（基于 SMB 系数）
    # SMB > 0.5 → 小盘(3), SMB < -0.5 → 大盘(1), 其他 → 中盘(2)
    if smb_coef > 0.5:
        size = 3
    elif smb_coef < -0.5:
        size = 1
    else:
        size = 2
    
    # 风格分类（基于 HML 系数）
    # HML > 0.5 → 价值(1), HML < -0.5 → 成长(3), 其他 → 平衡(2)
    if hml_coef > 0.5:
        style = 1
    elif hml_coef < -0.5:
        style = 3
    else:
        style = 2
    
    return {
        "size": size,
        "style": style,
        "smb_coef": smb_coef,
        "hml_coef": hml_coef,
    }


def _empty_equity_metrics() -> EquityMetrics:
    return EquityMetrics(
        common=CommonMetrics(),
        model_type="capm",
    )
