"""
Alpha（超额收益）计算模块 v2.0 - 专业分层版
核心特性：
  - 周频回归（过滤日内噪音）
  - 三层次Alpha（CAPM → FF3/5 → 行业中性化）
  - Treynor-Mazuy择时检测（独立轨道）
  - 月度Alpha胜率分析
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from typing import Dict, List, Tuple, Optional


# ============================================================
# 📊 数据频率转换模块
# ============================================================

def resample_to_weekly(
    daily_ret: pd.Series,
    freq: str = 'W-FRI'
) -> pd.Series:
    """
    将日频收益率转换为周频收益率

    Args:
        daily_ret: 日收益率序列（index为日期）
        freq: 周频频率，默认'W-FRI'（以周五为基准）

    Returns:
        周收益率序列（index为周五日期）

    说明:
        使用复合收益率公式：(1+r₁)×(1+r₂)×...×(1+rₙ) - 1
        既过滤了日内反转噪音，又保留了足够的样本量
    """
    if daily_ret.empty:
        return pd.Series(dtype=float, name='ret_weekly')

    # 确保index为datetime类型
    if not pd.api.types.is_datetime64_any_dtype(daily_ret.index):
        daily_ret.index = pd.to_datetime(daily_ret.index)

    # 周频复利
    weekly_ret = (1 + daily_ret).resample(freq).prod() - 1

    # 去除NaN（该周可能没有交易日）
    weekly_ret = weekly_ret.dropna()

    return weekly_ret


def resample_to_monthly(daily_ret: pd.Series) -> pd.Series:
    """
    将日频收益率转换为月频收益率（用于月度胜率分析）

    Args:
        daily_ret: 日收益率序列（index为日期）

    Returns:
        月收益率序列（index为月末日期）
    """
    if daily_ret.empty:
        return pd.Series(dtype=float, name='ret_monthly')

    if not pd.api.types.is_datetime64_any_dtype(daily_ret.index):
        daily_ret.index = pd.to_datetime(daily_ret.index)

    monthly_ret = (1 + daily_ret).resample('ME').prod() - 1
    monthly_ret = monthly_ret.dropna()

    return monthly_ret


# ============================================================
# 🎯 三层次Alpha计算引擎
# ============================================================

def calculate_alpha_hierarchical(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    ff_factors: Optional[pd.DataFrame] = None,
    industry_returns: Optional[Dict[str, pd.Series]] = None,
    risk_free_rate: float = 0.03,
    frequency: str = 'weekly',
) -> dict:
    """
    三层次Alpha计算引擎（CAPM → FF3/5 → 行业中性化）

    Args:
        fund_ret: 基金日收益率序列
        benchmark_ret: 基准日收益率序列
        ff_factors: FF因子数据（Mkt, SMB, HML, RMW, CMA等）
        industry_returns: 申万一级行业指数收益率字典 {sw_code: ret_series}
        risk_free_rate: 无风险利率（年化）
        frequency: 回归频率，'daily'/'weekly'/'monthly'（默认weekly）

    Returns:
        三层次Alpha结果：
        - capm: CAPM单因子结果
        - ff: FF三/五因子结果
        - industry_neutral: 行业中性化结果
        - summary: 综合解读
    """
    # ========== 1. 频率转换 ==========
    if frequency == 'weekly':
        fund_ret_adj = resample_to_weekly(fund_ret)
        benchmark_ret_adj = resample_to_weekly(benchmark_ret)
    elif frequency == 'monthly':
        fund_ret_adj = resample_to_monthly(fund_ret)
        benchmark_ret_adj = resample_to_monthly(benchmark_ret)
    else:
        fund_ret_adj = fund_ret
        benchmark_ret_adj = benchmark_ret

    if fund_ret_adj.empty or benchmark_ret_adj.empty:
        return {
            'capm': None,
            'ff': None,
            'industry_neutral': None,
            'summary': '数据不足，无法计算Alpha'
        }

    # 调整无风险利率为对应频率
    if frequency == 'weekly':
        rf_adj = risk_free_rate / 52
    elif frequency == 'monthly':
        rf_adj = risk_free_rate / 12
    else:
        rf_adj = risk_free_rate / 252

    # ========== 2. 阶段1：CAPM单因子模型 ==========
    capm_result = _run_capm_model(
        fund_ret_adj, benchmark_ret_adj, rf_adj, frequency
    )

    # ========== 3. 阶段2：FF三/五因子模型 ==========
    ff_result = _run_ff_model(
        fund_ret_adj, benchmark_ret_adj, ff_factors, rf_adj, frequency
    )

    # ========== 4. 阶段3：行业中性化模型 ==========
    industry_result = _run_industry_neutral_model(
        fund_ret_adj, benchmark_ret_adj, ff_factors, industry_returns, rf_adj, frequency
    )

    # ========== 5. 综合解读 ==========
    summary = _interpret_hierarchical_alpha(capm_result, ff_result, industry_result)

    return {
        'capm': capm_result,
        'ff': ff_result,
        'industry_neutral': industry_result,
        'summary': summary
    }


def _run_capm_model(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    rf: float,
    frequency: str
) -> dict:
    """运行CAPM单因子回归"""
    merged = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()

    if len(merged) < 30:
        return None

    fund_excess = merged['fund'] - rf
    benchmark_excess = merged['benchmark'] - rf

    # 将Series转为DataFrame以确保列名正确
    X = pd.DataFrame({'market': benchmark_excess})
    X = sm.add_constant(X)
    model = sm.OLS(fund_excess, X).fit()

    alpha_daily = model.params['const']
    alpha_annual = _annualize_alpha(alpha_daily, frequency)
    beta = model.params['market']
    alpha_pval = model.pvalues['const']
    r_squared = model.rsquared

    return {
        'alpha': alpha_annual,
        'alpha_pval': alpha_pval,
        'beta': beta,
        'r_squared': r_squared,
        'frequency': frequency,
        'model_name': 'CAPM'
    }


def _run_ff_model(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    ff_factors: Optional[pd.DataFrame],
    rf: float,
    frequency: str
) -> dict:
    """运行FF三/五因子回归"""
    if ff_factors is None or ff_factors.empty:
        return None

    merged = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()
    merged = merged.merge(ff_factors, left_index=True, right_on='date', how='inner')

    if len(merged) < 30:
        return None

    fund_excess = merged['fund'] - rf
    benchmark_excess = merged['benchmark'] - rf

    # 选择可用的FF因子
    factor_cols = []
    if 'Mkt' in merged.columns:
        factor_cols.append('Mkt')
    if 'SMB' in merged.columns:
        factor_cols.append('SMB')
    if 'HML' in merged.columns:
        factor_cols.append('HML')
    if 'RMW' in merged.columns:
        factor_cols.append('RMW')
    if 'CMA' in merged.columns:
        factor_cols.append('CMA')

    if not factor_cols:
        return None

    X = sm.add_constant(merged[factor_cols])
    model = sm.OLS(fund_excess, X).fit()

    alpha_daily = model.params['const']
    alpha_annual = _annualize_alpha(alpha_daily, frequency)
    alpha_pval = model.pvalues['const']
    r_squared = model.rsquared

    # 提取因子Beta
    factor_betas = {}
    for col in factor_cols:
        factor_betas[col] = model.params[col]

    return {
        'alpha': alpha_annual,
        'alpha_pval': alpha_pval,
        'r_squared': r_squared,
        'factor_betas': factor_betas,
        'frequency': frequency,
        'model_name': f'FF{len(factor_cols)}' if factor_cols else 'FF0'
    }


def _run_industry_neutral_model(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    ff_factors: Optional[pd.DataFrame],
    industry_returns: Optional[Dict[str, pd.Series]],
    rf: float,
    frequency: str
) -> dict:
    """运行行业中性化模型（FF + 行业指数）"""
    if industry_returns is None or not industry_returns:
        return None

    merged = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()

    # 合并行业指数
    for sw_code, ind_ret in industry_returns.items():
        merged[sw_code] = ind_ret.reindex(merged.index)

    if ff_factors is not None and not ff_factors.empty:
        merged = merged.merge(ff_factors, left_index=True, right_on='date', how='inner')

    merged = merged.dropna()

    if len(merged) < 30:
        return None

    fund_excess = merged['fund'] - rf

    # 构建回归矩阵（市场 + FF因子 + 行业指数）
    regressors = ['benchmark']
    if ff_factors is not None:
        if 'SMB' in merged.columns:
            regressors.append('SMB')
        if 'HML' in merged.columns:
            regressors.append('HML')

    # 添加行业指数（最多5个主要行业）
    industry_cols = [col for col in merged.columns
                      if col not in ['fund', 'date', 'Mkt', 'SMB', 'HML', 'RMW', 'CMA', 'benchmark']]
    industry_cols = sorted(industry_cols, key=lambda x: merged[x].mean(), reverse=True)[:5]
    regressors.extend(industry_cols)

    X = merged[regressors]
    X = sm.add_constant(X)
    model = sm.OLS(fund_excess, X).fit()

    alpha_daily = model.params['const']
    alpha_annual = _annualize_alpha(alpha_daily, frequency)
    alpha_pval = model.pvalues['const']
    r_squared = model.rsquared

    return {
        'alpha': alpha_annual,
        'alpha_pval': alpha_pval,
        'r_squared': r_squared,
        'industries': industry_cols,
        'frequency': frequency,
        'model_name': 'Industry Neutral'
    }


def _annualize_alpha(alpha_daily: float, frequency: str) -> float:
    """将日频/周频/月频Alpha年化"""
    if frequency == 'daily':
        return alpha_daily * 252
    elif frequency == 'weekly':
        return alpha_daily * 52
    elif frequency == 'monthly':
        return alpha_daily * 12
    else:
        return alpha_daily


def _interpret_hierarchical_alpha(capm, ff, industry) -> str:
    """解读三层次Alpha结果"""
    parts = []

    if capm:
        parts.append(f"📊 CAPM单因子: Alpha={capm['alpha']*100:.2f}%, p={capm['alpha_pval']:.3f}")

    if ff:
        parts.append(f"📊 FF{ff['model_name'][2:]}因子: Alpha={ff['alpha']*100:.2f}%, p={ff['alpha_pval']:.3f}")

    if industry:
        parts.append(f"📊 行业中性化: Alpha={industry['alpha']*100:.2f}%, p={industry['alpha_pval']:.3f}")

    # 判断Alpha稳定性
    if capm and ff and industry:
        alpha_drop = abs(capm['alpha'] - ff['alpha']) + abs(ff['alpha'] - industry['alpha'])
        if alpha_drop < 0.02:
            parts.append("✅ Alpha稳定，选股能力扎实")
        else:
            parts.append("⚠️ Alpha随因子剥离显著变化，风格暴露占比较高")

    return '\n'.join(parts)


# ============================================================
# 🕐 Treynor-Mazuy择时检测（独立轨道）
# ============================================================

def calculate_timing_ability(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    risk_free_rate: float = 0.03,
    frequency: str = 'weekly'
) -> dict:
    """
    Treynor-Mazuy择时能力检测（独立轨道，严禁与FF因子混用）

    回归方程: Rₚ - R_f = α + β·(Rₘ - R_f) + γ·(Rₘ - R_f)² + ε

    Args:
        fund_ret: 基金日收益率序列
        benchmark_ret: 基准日收益率序列
        risk_free_rate: 无风险利率（年化）
        frequency: 回归频率

    Returns:
        择时检测结果：
        - alpha: 选股Alpha
        - beta: 市场Beta
        - gamma: 择时系数（>0表示择时能力强）
        - gamma_pval: 择时显著性
        - timing_score: 择时能力得分（0-100）
        - interpretation: 文字解读
    """
    # 频率转换
    if frequency == 'weekly':
        fund_ret_adj = resample_to_weekly(fund_ret)
        benchmark_ret_adj = resample_to_weekly(benchmark_ret)
    elif frequency == 'monthly':
        fund_ret_adj = resample_to_monthly(fund_ret)
        benchmark_ret_adj = resample_to_monthly(benchmark_ret)
    else:
        fund_ret_adj = fund_ret
        benchmark_ret_adj = benchmark_ret

    if fund_ret_adj.empty or benchmark_ret_adj.empty:
        return {
            'alpha': 0.0, 'beta': 0.0, 'gamma': 0.0, 'gamma_pval': 1.0,
            'timing_score': 0.0, 'interpretation': '数据不足，无法检测择时能力'
        }

    # 调整无风险利率
    if frequency == 'weekly':
        rf_adj = risk_free_rate / 52
    elif frequency == 'monthly':
        rf_adj = risk_free_rate / 12
    else:
        rf_adj = risk_free_rate / 252

    # 合并数据
    merged = pd.DataFrame({'fund': fund_ret_adj, 'benchmark': benchmark_ret_adj}).dropna()

    if len(merged) < 30:
        return {
            'alpha': 0.0, 'beta': 0.0, 'gamma': 0.0, 'gamma_pval': 1.0,
            'timing_score': 0.0, 'interpretation': f'数据不足({len(merged)}<30)，无法检测择时能力'
        }

    # 计算超额收益
    fund_excess = merged['fund'] - rf_adj
    benchmark_excess = merged['benchmark'] - rf_adj

    # T-M模型：加入市场超额收益的平方项
    X = pd.DataFrame({
        'const': 1,
        'market': benchmark_excess,
        'market_squared': benchmark_excess ** 2
    })

    model = sm.OLS(fund_excess, X).fit()

    alpha_daily = model.params['const']
    alpha_annual = _annualize_alpha(alpha_daily, frequency)
    beta = model.params['market']
    gamma = model.params['market_squared']
    gamma_pval = model.pvalues['market_squared']

    # 择时能力得分（0-100）
    if gamma_pval < 0.05:
        if gamma > 0:
            timing_score = min(100, 50 + gamma * 1000)
        else:
            timing_score = max(0, 50 - abs(gamma) * 1000)
    else:
        timing_score = 50 + min(20, gamma * 500)

    interpretation = _interpret_timing_ability(gamma, gamma_pval, timing_score)

    return {
        'alpha': alpha_annual,
        'beta': beta,
        'gamma': gamma,
        'gamma_pval': gamma_pval,
        'timing_score': timing_score,
        'interpretation': interpretation
    }


def _interpret_timing_ability(gamma: float, gamma_pval: float, timing_score: float) -> str:
    """解读择时能力"""
    if gamma_pval > 0.1:
        return f"📊 择时能力不显著（γ={gamma:.4f}, p={gamma_pval:.3f}），难以判断经理的择时能力"

    if gamma > 0 and gamma_pval < 0.05:
        if gamma > 0.1:
            return f"✨ 择时能力极强（γ={gamma:.4f}, p={gamma_pval:.3f}），经理在牛市中能显著加大仓位，熊市中能快速降低仓位"
        else:
            return f"✅ 择时能力良好（γ={gamma:.4f}, p={gamma_pval:.3f}），具备一定的市场时机把握能力"

    if gamma < 0 and gamma_pval < 0.05:
        return f"❌ 反向择时（γ={gamma:.4f}, p={gamma_pval:.3f}），经理的择时操作反而拖累了收益"

    return f"📊 择时能力一般（γ={gamma:.4f}, p={gamma_pval:.3f}）"


# ============================================================
# 📈 月度Alpha胜率分析
# ============================================================

def calculate_monthly_win_rate(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    months: int = 36
) -> dict:
    """
    计算月度Alpha胜率（Alpha>0的月份数占比）

    Args:
        fund_ret: 基金日收益率序列
        benchmark_ret: 基准日收益率序列
        months: 分析最近几个月（默认36个月=3年）

    Returns:
        胜率分析结果：
        - win_rate: 胜率（0-1）
        - win_months: 获胜月份数
        - total_months: 总月份数
        - monthly_alpha_series: 月度Alpha序列
        - interpretation: 文字解读
    """
    fund_monthly = resample_to_monthly(fund_ret)
    benchmark_monthly = resample_to_monthly(benchmark_ret)

    if fund_monthly.empty or benchmark_monthly.empty:
        return {
            'win_rate': 0.0, 'win_months': 0, 'total_months': 0,
            'monthly_alpha_series': pd.Series(dtype=float),
            'interpretation': '数据不足，无法计算胜率'
        }

    merged = pd.DataFrame({
        'fund': fund_monthly,
        'benchmark': benchmark_monthly
    }).dropna()

    merged = merged.tail(months)

    if len(merged) < 12:
        return {
            'win_rate': 0.0, 'win_months': 0, 'total_months': len(merged),
            'monthly_alpha_series': pd.Series(dtype=float),
            'interpretation': f'数据不足({len(merged)}<12个月)，无法计算胜率'
        }

    monthly_alpha = merged['fund'] - merged['benchmark']

    win_months = (monthly_alpha > 0).sum()
    total_months = len(monthly_alpha)
    win_rate = win_months / total_months

    interpretation = _interpret_monthly_win_rate(win_rate, win_months, total_months)

    return {
        'win_rate': win_rate,
        'win_months': win_months,
        'total_months': total_months,
        'monthly_alpha_series': monthly_alpha,
        'interpretation': interpretation
    }


def _interpret_monthly_win_rate(win_rate: float, win_months: int, total_months: int) -> str:
    """解读月度胜率"""
    if win_rate >= 0.70:
        return f"✨ 月度胜率极高（{win_months}/{total_months}={win_rate*100:.1f}%），超额收益非常稳定"
    elif win_rate >= 0.60:
        return f"✅ 月度胜率优秀（{win_months}/{total_months}={win_rate*100:.1f}%），超额收益较为稳定"
    elif win_rate >= 0.50:
        return f"📊 月度胜率一般（{win_months}/{total_months}={win_rate*100:.1f}%），超额收益波动较大"
    else:
        return f"❌ 月度胜率较低（{win_months}/{total_months}={win_rate*100:.1f}%），超额收益不稳定"


# ============================================================
# 🔄 兼容旧版接口（保留向后兼容）
# ============================================================

def calculate_alpha(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    risk_free_rate: float = 0.03,
) -> dict:
    """
    计算基金的超额收益和Alpha

    Args:
        fund_ret: 基金日收益率序列（index为日期）
        benchmark_ret: 基准日收益率序列（index为日期）
        risk_free_rate: 无风险利率（年化，默认3%）

    Returns:
        Alpha分析结果：
        - excess_return: 超额收益（年化）
        - alpha: Jensen's Alpha（年化）
        - alpha_pval: Alpha显著性p值
        - beta: 系统性风险系数
        - tracking_error: 跟踪误差
        - information_ratio: 信息比率
        - treynor_ratio: 特雷纳比率
        - jensen_alpha: Jensen's Alpha（年化）
        - r_squared: 拟合优度R²
        - interpretation: 文字解读
    """
    if fund_ret.empty or benchmark_ret.empty:
        return {
            'excess_return': 0.0,
            'alpha': 0.0,
            'alpha_pval': 1.0,
            'beta': 0.0,
            'tracking_error': 0.0,
            'information_ratio': 0.0,
            'treynor_ratio': 0.0,
            'jensen_alpha': 0.0,
            'r_squared': 0.0,
            'interpretation': '数据不足，无法计算Alpha',
        }

    # ========== 1. 数据对齐 ==========
    merged = pd.DataFrame({
        'fund': fund_ret,
        'benchmark': benchmark_ret
    }).dropna()

    if len(merged) < 60:
        return {
            'excess_return': 0.0,
            'alpha': 0.0,
            'alpha_pval': 1.0,
            'beta': 0.0,
            'tracking_error': 0.0,
            'information_ratio': 0.0,
            'treynor_ratio': 0.0,
            'jensen_alpha': 0.0,
            'r_squared': 0.0,
            'interpretation': f'数据不足({len(merged)}天<60天)，无法计算Alpha',
        }

    # ========== 2. 计算超额收益 ==========
    daily_rf = risk_free_rate / 252  # 日无风险利率

    fund_excess = merged['fund'] - daily_rf
    benchmark_excess = merged['benchmark'] - daily_rf

    # 超额收益（基金 - 基准，年化）
    excess_return = (1 + (merged['fund'] - merged['benchmark'])).prod() ** (252 / len(merged)) - 1

    # ========== 3. CAPM回归计算Jensen's Alpha ==========
    X = sm.add_constant(benchmark_excess)
    model = sm.OLS(fund_excess, X).fit()

    alpha_daily = model.params['const']  # 日Alpha
    alpha = alpha_daily * 252  # 年化Alpha
    alpha_pval = model.pvalues['const']
    beta = model.params['benchmark_excess']  # Beta系数
    r_squared = model.rsquared  # R²拟合优度

    # ========== 4. 计算风险调整收益指标 ==========
    
    # 跟踪误差（Tracking Error）
    excess_ret_series = merged['fund'] - merged['benchmark']
    tracking_error = excess_ret_series.std() * np.sqrt(252)

    # 信息比率（Information Ratio）
    information_ratio = excess_return / tracking_error if tracking_error > 0 else 0.0

    # 特雷纳比率（Treynor Ratio）
    fund_annual = (1 + merged['fund']).prod() ** (252 / len(merged)) - 1
    treynor_ratio = (fund_annual - risk_free_rate) / beta if beta != 0 else 0.0

    # ========== 5. 文字解读 ==========
    interpretation = _interpret_alpha(alpha, alpha_pval, excess_return, 
                                  information_ratio, treynor_ratio, r_squared)

    return {
        'excess_return': excess_return,
        'alpha': alpha,
        'alpha_pval': alpha_pval,
        'beta': beta,
        'tracking_error': tracking_error,
        'information_ratio': information_ratio,
        'treynor_ratio': treynor_ratio,
        'jensen_alpha': alpha,  # Jensen's Alpha与Alpha相同
        'r_squared': r_squared,
        'interpretation': interpretation,
    }


def _interpret_alpha(
    alpha: float,
    alpha_pval: float,
    excess_return: float,
    information_ratio: float,
    treynor_ratio: float,
    r_squared: float,
) -> str:
    """解读Alpha分析结果"""
    parts = []

    # 1. Alpha解读（核心指标）
    if alpha_pval < 0.05:  # 显著性检验
        if alpha > 0:
            if alpha > 0.10:
                parts.append(f"✨ Alpha={alpha*100:.2f}%（显著优秀）：大幅跑赢市场，经理具备极强的选股能力")
            elif alpha > 0.05:
                parts.append(f"✅ Alpha={alpha*100:.2f}%（显著优秀）：显著跑赢市场，经理具备出色的选股能力")
            else:
                parts.append(f"✅ Alpha={alpha*100:.2f}%（显著正）：小幅跑赢市场，经理具备一定的选股能力")
        else:
            if alpha < -0.10:
                parts.append(f"❌ Alpha={alpha*100:.2f}%（显著差）：大幅跑输市场，经理能力严重不足")
            elif alpha < -0.05:
                parts.append(f"❌ Alpha={alpha*100:.2f}%（显著差）：显著跑输市场，经理能力欠佳")
            else:
                parts.append(f"⚠️ Alpha={alpha*100:.2f}%（显著负）：小幅跑输市场，经理选股能力偏弱")
    else:  # 不显著
        if alpha > 0.02:
            parts.append(f"📊 Alpha={alpha*100:.2f}%（不显著）：超额收益可能来自运气，需持续观察")
        elif alpha < -0.02:
            parts.append(f"📊 Alpha={alpha*100:.2f}%（不显著）：负向Alpha不显著，可能只是短期波动")
        else:
            parts.append(f"📊 Alpha={alpha*100:.2f}%（不显著）：与市场持平，无明显超额能力")

    # 2. 超额收益解读
    if excess_return > 0.10:
        parts.append(f"💰 超额收益={excess_return*100:.2f}%：大幅跑赢基准")
    elif excess_return > 0.05:
        parts.append(f"💰 超额收益={excess_return*100:.2f}%：显著跑赢基准")
    elif excess_return > 0.02:
        parts.append(f"💰 超额收益={excess_return*100:.2f}%：小幅跑赢基准")
    elif excess_return > 0:
        parts.append(f"📊 超额收益={excess_return*100:.2f}%：微弱跑赢基准")
    elif excess_return < -0.05:
        parts.append(f"💔 超额收益={excess_return*100:.2f}%：显著跑输基准")
    elif excess_return < -0.02:
        parts.append(f"💔 超额收益={excess_return*100:.2f}%：小幅跑输基准")
    else:
        parts.append(f"📊 超额收益={excess_return*100:.2f}%：与基准基本持平")

    # 3. 信息比率解读
    if information_ratio > 1.0:
        parts.append(f"🎯 信息比率={information_ratio:.2f}（优秀）：超额收益稳定且显著")
    elif information_ratio > 0.5:
        parts.append(f"✅ 信息比率={information_ratio:.2f}（良好）：超额收益较为稳定")
    elif information_ratio > 0.2:
        parts.append(f"📊 信息比率={information_ratio:.2f}（一般）：超额收益一般，波动较大")
    elif information_ratio < -0.5:
        parts.append(f"❌ 信息比率={information_ratio:.2f}（不佳）：持续跑输基准")
    else:
        parts.append(f"📊 信息比率={information_ratio:.2f}（较弱）：缺乏持续的超额能力")

    # 4. 特雷纳比率解读
    if treynor_ratio > 0.3:
        parts.append(f"⚡ 特雷纳比率={treynor_ratio:.2f}（优秀）：单位系统性风险收益极高")
    elif treynor_ratio > 0.15:
        parts.append(f"✅ 特雷纳比率={treynor_ratio:.2f}（良好）：单位系统性风险收益较高")
    elif treynor_ratio < 0:
        parts.append(f"❌ 特雷纳比率={treynor_ratio:.2f}（不佳）：系统性风险收益为负")

    # 5. R²解读（模型拟合度）
    if r_squared > 0.80:
        parts.append(f"📈 R²={r_squared:.2f}（高拟合）：基金收益高度可被市场基准解释")
    elif r_squared > 0.60:
        parts.append(f"📈 R²={r_squared:.2f}（中高拟合）：基金收益大部分可被市场基准解释")
    elif r_squared > 0.40:
        parts.append(f"📈 R²={r_squared:.2f}（中等拟合）：基金收益部分可被市场基准解释")
    else:
        parts.append(f"📈 R²={r_squared:.2f}（低拟合）：基金收益难以被市场基准解释，风格独立")

    return '\n'.join(parts)


def calculate_alpha_rolling(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    window: int = 60,
    risk_free_rate: float = 0.03,
) -> dict:
    """
    计算滚动窗口的Alpha（监测Alpha的稳定性）

    Args:
        fund_ret: 基金日收益率序列（index为日期）
        benchmark_ret: 基准日收益率序列（index为日期）
        window: 滚动窗口（默认60天）
        risk_free_rate: 无风险利率（年化，默认3%）

    Returns:
        滚动Alpha分析结果：
        - alpha_rolling: 滚动Alpha序列
        - alpha_latest: 最新Alpha值
        - alpha_stable: Alpha是否稳定
        - alpha_trend: Alpha趋势（上升/下降/稳定）
        - interpretation: 文字解读
    """
    if fund_ret.empty or benchmark_ret.empty:
        return {
            'alpha_rolling': pd.Series(dtype=float),
            'alpha_latest': 0.0,
            'alpha_stable': False,
            'alpha_trend': '数据不足',
            'interpretation': '数据不足，无法计算滚动Alpha',
        }

    # ========== 1. 数据对齐 ==========
    merged = pd.DataFrame({
        'fund': fund_ret,
        'benchmark': benchmark_ret
    }).dropna()

    if len(merged) < window:
        return {
            'alpha_rolling': pd.Series(dtype=float),
            'alpha_latest': 0.0,
            'alpha_stable': False,
            'alpha_trend': '数据不足',
            'interpretation': f'数据不足({len(merged)}天<{window}天)，无法计算滚动Alpha',
        }

    # ========== 2. 计算滚动Alpha ==========
    daily_rf = risk_free_rate / 252

    alpha_list = []
    dates = []

    for i in range(window, len(merged) + 1):
        window_data = merged.iloc[i-window:i]
        
        fund_excess = window_data['fund'] - daily_rf
        benchmark_excess = window_data['benchmark'] - daily_rf

        if len(fund_excess) < 30:  # 窗口内至少需要30天数据
            alpha_list.append(np.nan)
            dates.append(window_data.index[-1])
            continue

        try:
            X = sm.add_constant(benchmark_excess)
            model = sm.OLS(fund_excess, X).fit()
            alpha_daily = model.params['const']
            alpha_annual = alpha_daily * 252
            alpha_list.append(alpha_annual)
            dates.append(window_data.index[-1])
        except:
            alpha_list.append(np.nan)
            dates.append(window_data.index[-1])

    alpha_rolling = pd.Series(alpha_list, index=dates)

    # ========== 3. 分析Alpha稳定性和趋势 ==========
    alpha_latest = alpha_rolling.iloc[-1] if not alpha_rolling.empty else 0.0
    
    # 稳定性判断（标准差）
    alpha_valid = alpha_rolling.dropna()
    if len(alpha_valid) >= 10:
        alpha_std = alpha_valid.std()
        alpha_stable = alpha_std < 0.05  # 标准差<5%认为稳定
    else:
        alpha_std = np.nan
        alpha_stable = False

    # 趋势判断（线性回归）
    if len(alpha_valid) >= 10:
        alpha_trend = _detect_alpha_trend(alpha_valid)
    else:
        alpha_trend = '数据不足'

    # ========== 4. 文字解读 ==========
    interpretation = _interpret_alpha_rolling(alpha_latest, alpha_stable, alpha_trend, alpha_std)

    return {
        'alpha_rolling': alpha_rolling,
        'alpha_latest': alpha_latest,
        'alpha_stable': alpha_stable,
        'alpha_trend': alpha_trend,
        'interpretation': interpretation,
    }


def _detect_alpha_trend(alpha_series: pd.Series) -> str:
    """检测Alpha趋势"""
    # 线性回归检测趋势
    x = np.arange(len(alpha_series))
    y = alpha_series.values
    
    try:
        model = np.polyfit(x, y, 1)
        slope = model[0]
        
        # 年化趋势（假设252个交易日）
        annual_slope = slope * 252
        
        if annual_slope > 0.02:  # 年化上升>2%
            return '上升'
        elif annual_slope < -0.02:  # 年化下降<-2%
            return '下降'
        else:
            return '稳定'
    except:
        return '数据不足'


def _interpret_alpha_rolling(
    alpha_latest: float,
    alpha_stable: bool,
    alpha_trend: str,
    alpha_std: float,
) -> str:
    """解读滚动Alpha分析结果"""
    parts = []

    # 1. 最新Alpha状态
    if alpha_latest > 0.05:
        parts.append(f"✨ 最新Alpha={alpha_latest*100:.2f}%（优秀）：近期超额收益能力出色")
    elif alpha_latest > 0.02:
        parts.append(f"✅ 最新Alpha={alpha_latest*100:.2f}%（良好）：近期具备超额收益能力")
    elif alpha_latest > 0:
        parts.append(f"📊 最新Alpha={alpha_latest*100:.2f}%（一般）：近期小幅超额收益")
    elif alpha_latest < -0.02:
        parts.append(f"❌ 最新Alpha={alpha_latest*100:.2f}%（不佳）：近期超额收益为负")
    else:
        parts.append(f"📊 最新Alpha={alpha_latest*100:.2f}%（持平）：近期与市场持平")

    # 2. 稳定性解读
    if alpha_stable:
        parts.append(f"✅ Alpha稳定（标准差={alpha_std*100:.2f}%）：超额收益能力持续稳定")
    else:
        parts.append(f"⚠️ Alpha不稳定（标准差={alpha_std*100:.2f}%）：超额收益能力波动较大")

    # 3. 趋势解读
    if alpha_trend == '上升':
        parts.append("📈 Alpha趋势：上升，超额收益能力在增强")
    elif alpha_trend == '下降':
        parts.append("📉 Alpha趋势：下降，超额收益能力在减弱")
    elif alpha_trend == '稳定':
        parts.append("📊 Alpha趋势：稳定，超额收益能力保持稳定")
    else:
        parts.append("📊 Alpha趋势：数据不足，无法判断趋势")

    return '\n'.join(parts)


def decompose_excess_return(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    ff_factors: pd.DataFrame,
) -> dict:
    """
    分解超额收益的来源

    Args:
        fund_ret: 基金日收益率序列
        benchmark_ret: 基准日收益率序列
        ff_factors: FF因子数据（Mkt, SMB, HML等）

    Returns:
        收益分解结果：
        - excess_total: 总超额收益（年化）
        - alpha_contribution: Alpha贡献（年化）
        - beta_contribution: Beta贡献（年化）
        - size_contribution: 大小盘因子贡献（年化）
        - value_contribution: 价值因子贡献（年化）
        - residual_contribution: 残差贡献（年化）
        - interpretation: 文字解读
    """
    if fund_ret.empty or benchmark_ret.empty:
        return {
            'excess_total': 0.0,
            'alpha_contribution': 0.0,
            'beta_contribution': 0.0,
            'size_contribution': 0.0,
            'value_contribution': 0.0,
            'residual_contribution': 0.0,
            'interpretation': '数据不足，无法分解超额收益',
        }

    # ========== 1. 数据对齐 ==========
    merged = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()
    merged = merged.merge(ff_factors, on='date', how='left')
    merged = merged.dropna()

    if len(merged) < 60:
        return {
            'excess_total': 0.0,
            'alpha_contribution': 0.0,
            'beta_contribution': 0.0,
            'size_contribution': 0.0,
            'value_contribution': 0.0,
            'residual_contribution': 0.0,
            'interpretation': f'数据不足({len(merged)}天<60天)，无法分解超额收益',
        }

    # ========== 2. 计算总超额收益 ==========
    excess_ret = merged['fund'] - merged['benchmark']
    excess_total = (1 + excess_ret).prod() ** (252 / len(merged)) - 1

    # ========== 3. FF三因子回归 ==========
    factor_cols = []
    if 'Mkt' in merged.columns:
        factor_cols.append('Mkt')
    if 'SMB' in merged.columns:
        factor_cols.append('SMB')
    if 'HML' in merged.columns:
        factor_cols.append('HML')

    if not factor_cols:
        return {
            'excess_total': excess_total,
            'alpha_contribution': excess_total,
            'beta_contribution': 0.0,
            'size_contribution': 0.0,
            'value_contribution': 0.0,
            'residual_contribution': 0.0,
            'interpretation': 'FF因子数据缺失，无法分解超额收益',
        }

    X = sm.add_constant(merged[factor_cols])
    y = merged['fund'] - merged['benchmark']  # 超额收益

    model = sm.OLS(y, X).fit()

    # ========== 4. 计算各因子贡献 ==========
    # Alpha贡献（年化）
    alpha_daily = model.params['const']
    alpha_contribution = alpha_daily * 252

    # 市场因子贡献
    if 'Mkt' in model.params:
        beta = model.params['Mkt']
        mkt_annual = merged['Mkt'].mean() * 252
        beta_contribution = beta * mkt_annual
    else:
        beta_contribution = 0.0

    # 大小盘因子贡献
    if 'SMB' in model.params:
        smb_beta = model.params['SMB']
        smb_annual = merged['SMB'].mean() * 252
        size_contribution = smb_beta * smb_annual
    else:
        size_contribution = 0.0

    # 价值因子贡献
    if 'HML' in model.params:
        hml_beta = model.params['HML']
        hml_annual = merged['HML'].mean() * 252
        value_contribution = hml_beta * hml_annual
    else:
        value_contribution = 0.0

    # 残差贡献（其他因素）
    residual_contribution = excess_total - (alpha_contribution + beta_contribution + 
                                       size_contribution + value_contribution)

    # ========== 5. 文字解读 ==========
    interpretation = _interpret_decomposition(
        excess_total, alpha_contribution, beta_contribution,
        size_contribution, value_contribution, residual_contribution
    )

    return {
        'excess_total': excess_total,
        'alpha_contribution': alpha_contribution,
        'beta_contribution': beta_contribution,
        'size_contribution': size_contribution,
        'value_contribution': value_contribution,
        'residual_contribution': residual_contribution,
        'interpretation': interpretation,
    }


def _interpret_decomposition(
    excess_total: float,
    alpha_contribution: float,
    beta_contribution: float,
    size_contribution: float,
    value_contribution: float,
    residual_contribution: float,
) -> str:
    """解读超额收益分解结果"""
    parts = []

    # 总超额收益
    parts.append(f"💰 总超额收益：{excess_total*100:+.2f}%")

    # 各因子贡献
    contributions = [
        ('Alpha选股', alpha_contribution),
        ('Beta市场', beta_contribution),
        ('大小盘风格', size_contribution),
        ('价值成长', value_contribution),
        ('其他因素', residual_contribution),
    ]

    # 找出最大贡献因子
    max_contrib = max(contributions, key=lambda x: abs(x[1]))
    if abs(max_contrib[1]) > 0.01:  # 贡献>1%才提及
        direction = '正' if max_contrib[1] > 0 else '负'
        parts.append(f"✨ 最大贡献：{max_contrib[0]}（{direction}{max_contrib[1]*100:.2f}%）")

    # 详细分解
    parts.append("\n📊 收益来源分解：")
    for name, contrib in contributions:
        if abs(contrib) > 0.005:  # 贡献>0.5%才显示
            emoji = "✅" if contrib > 0 else "❌"
            parts.append(f"  {emoji} {name}：{contrib*100:+.2f}%")

    return '\n'.join(parts)


def calculate_radar_scores_alpha(
    alpha: float,
    alpha_pval: float,
    information_ratio: float,
    excess_return: float,
    r_squared: float,
) -> dict:
    """
    基于Alpha分析结果计算雷达图评分

    Args:
        alpha: Alpha值（年化）
        alpha_pval: Alpha显著性p值
        information_ratio: 信息比率
        excess_return: 超额收益
        r_squared: R²拟合优度

    Returns:
        雷达图评分字典（0-100）
    """
    from utils.helpers import normalize_score

    # 1. 超额能力评分（Alpha）
    # 显著的Alpha给予更高分
    if alpha_pval < 0.05:
        excess_score = normalize_score(alpha, -0.10, 0.15)
    else:
        # 不显著的Alpha给予较低分
        excess_score = normalize_score(alpha, -0.05, 0.05)

    # 2. 信息比率评分
    ir_score = normalize_score(information_ratio, -0.5, 1.0)

    # 3. 稳定性评分（R²）
    stability_score = normalize_score(r_squared, 0.0, 1.0)

    # 4. 风险调整收益评分（综合考虑Alpha和R²）
    risk_adj_score = (excess_score * 0.6 + stability_score * 0.4)

    return {
        '超额能力': excess_score,
        '信息比率': ir_score,
        '稳定性': stability_score,
        '风险调整收益': risk_adj_score,
    }
