"""
风格分析模块
包含基金风格分析、滚动Beta监控、风格漂移检测等功能
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


def analyze_style(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    ff_results: dict,
) -> dict:
    """
    分析基金风格（Beta、相关性、风格标签）

    Args:
        fund_ret: 基金日收益率序列（index为日期）
        benchmark_ret: 基准日收益率序列（index为日期）
        ff_results: FF因子回归结果（包含因子暴露）

    Returns:
        风格分析结果字典：
        - beta: 系统性风险系数
        - correlation: 与基准相关性
        - style: 风格标签（激进型/平衡型/防御型/低相关）
        - style_breakdown: 风格细分（大小盘、价值/成长等）
        - volatility: 基金年化波动率
        - benchmark_volatility: 基准年化波动率
        - tracking_error: 跟踪误差
        - information_ratio: 信息比率
        - interpretation: 文字解读
    """
    if fund_ret.empty:
        return {
            'beta': 0.0,
            'correlation': 0.0,
            'style': '数据不足',
            'style_breakdown': {},
            'volatility': 0.0,
            'benchmark_volatility': 0.0,
            'tracking_error': 0.0,
            'information_ratio': 0.0,
            'interpretation': '数据不足，无法进行风格分析',
        }

    # ========== 1. 数据对齐 ==========
    if not benchmark_ret.empty:
        merged = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()
        if len(merged) < 60:
            return {
                'beta': 0.0,
                'correlation': 0.0,
                'style': '数据不足',
                'style_breakdown': {},
                'volatility': 0.0,
                'benchmark_volatility': 0.0,
                'tracking_error': 0.0,
                'information_ratio': 0.0,
                'interpretation': f'数据不足({len(merged)}天<60天)，无法进行风格分析',
            }
    else:
        merged = pd.DataFrame({'fund': fund_ret}).dropna()
        if len(merged) < 60:
            return {
                'beta': 0.0,
                'correlation': 0.0,
                'style': '数据不足',
                'style_breakdown': {},
                'volatility': 0.0,
                'benchmark_volatility': 0.0,
                'tracking_error': 0.0,
                'information_ratio': 0.0,
                'interpretation': f'数据不足({len(merged)}天<60天)，无法进行风格分析',
            }

    # ========== 2. 波动率分析 ==========
    fund_vol = merged['fund'].std() * np.sqrt(252)
    benchmark_vol = merged['benchmark'].std() * np.sqrt(252) if 'benchmark' in merged.columns else 0.15

    # ========== 3. 相关性分析 ==========
    if 'benchmark' in merged.columns:
        correlation = merged['fund'].corr(merged['benchmark'])

        # 跟踪误差（Tracking Error）
        excess_ret = merged['fund'] - merged['benchmark']
        tracking_error = excess_ret.std() * np.sqrt(252)

        # 信息比率（Information Ratio）
        information_ratio = excess_ret.mean() * 252 / tracking_error if tracking_error > 0 else 0.0
    else:
        correlation = 0.0
        tracking_error = 0.0
        information_ratio = 0.0

    # ========== 4. Beta计算 ==========
    if 'benchmark' in merged.columns and benchmark_vol > 0:
        beta = correlation * (fund_vol / benchmark_vol)
    else:
        beta = 0.0

    # ========== 5. 风格标签判定 ==========
    style = _determine_style(beta, correlation, fund_vol, benchmark_vol)

    # ========== 6. 风格细分（基于FF因子） ==========
    style_breakdown = _analyze_factor_exposure(ff_results)

    # ========== 7. 文字解读 ==========
    interpretation = _interpret_style(
        beta, correlation, fund_vol, benchmark_vol,
        tracking_error, information_ratio, style_breakdown
    )

    return {
        'beta': beta,
        'correlation': correlation,
        'style': style,
        'style_breakdown': style_breakdown,
        'volatility': fund_vol,
        'benchmark_volatility': benchmark_vol,
        'tracking_error': tracking_error,
        'information_ratio': information_ratio,
        'interpretation': interpretation,
    }


def _determine_style(
    beta: float,
    correlation: float,
    fund_vol: float,
    benchmark_vol: float,
) -> str:
    """判定基金风格标签"""
    # 优先级：低相关 > Beta > 波动率
    if abs(correlation) < 0.3:
        return '低相关（独立风格）'

    if beta > 1.2:
        return '激进型（高Beta）'
    elif beta > 0.8:
        return '平衡型'
    elif beta > 0.5:
        return '防御型（低Beta）'
    else:
        return '低Beta（防御或避险）'


def _analyze_factor_exposure(ff_results: dict) -> dict:
    """分析FF因子暴露"""
    factor_betas = ff_results.get('factor_betas', {})

    breakdown = {}

    # 大小盘风格
    smb = factor_betas.get('SMB', 0.0)
    if smb > 0.15:
        breakdown['size'] = '小盘成长（高SMB）'
    elif smb < -0.15:
        breakdown['size'] = '大盘价值（低SMB）'
    else:
        breakdown['size'] = '均衡'

    # 价值风格
    hml = factor_betas.get('HML', 0.0)
    if hml > 0.15:
        breakdown['value'] = '价值型（高HML）'
    elif hml < -0.15:
        breakdown['value'] = '成长型（低HML）'
    else:
        breakdown['value'] = '均衡'

    # 质量风格
    rmw = factor_betas.get('RMW', 0.0)
    if rmw > 0.15:
        breakdown['quality'] = '高质量（高RMW）'
    elif rmw < -0.15:
        breakdown['quality'] = '低质量（低RMW）'
    else:
        breakdown['quality'] = '均衡'

    # 动量风格
    mom = factor_betas.get('Short_MOM', 0.0)
    if mom > 0.15:
        breakdown['momentum'] = '高动量'
    elif mom < -0.15:
        breakdown['momentum'] = '低动量'
    else:
        breakdown['momentum'] = '均衡'

    return breakdown


def _interpret_style(
    beta: float,
    correlation: float,
    fund_vol: float,
    benchmark_vol: float,
    tracking_error: float,
    information_ratio: float,
    style_breakdown: dict,
) -> str:
    """解读风格分析结果"""
    parts = []

    # Beta解读
    if beta > 1.2:
        parts.append(f"🎯 Beta={beta:.2f}（激进）：市场上涨时涨幅更大，下跌时跌幅更深")
    elif beta > 0.8:
        parts.append(f"🎯 Beta={beta:.2f}（平衡）：与市场同步波动")
    elif beta > 0.5:
        parts.append(f"🎯 Beta={beta:.2f}（防御）：波动小于市场，抗跌性强")
    elif beta > 0:
        parts.append(f"🎯 Beta={beta:.2f}（低Beta）：高度防御，受市场影响小")
    else:
        parts.append(f"🎯 Beta={beta:.2f}（负相关）：与市场反向波动，可能是对冲或避险策略")

    # 相关性解读
    if abs(correlation) > 0.8:
        parts.append(f"📊 与基准高度相关（r={correlation:.2f}），风格与市场高度一致")
    elif abs(correlation) > 0.5:
        parts.append(f"📊 与基准中度相关（r={correlation:.2f}），有一定独立性")
    else:
        parts.append(f"📊 与基准低相关（r={correlation:.2f}），具有独特风格")

    # 波动率解读
    if fund_vol > benchmark_vol * 1.2:
        parts.append(f"📈 波动率={fund_vol*100:.1f}%（高于基准{benchmark_vol*100:.1f}%），风险偏高")
    elif fund_vol < benchmark_vol * 0.8:
        parts.append(f"📉 波动率={fund_vol*100:.1f}%（低于基准{benchmark_vol*100:.1f}%），风控较好")
    else:
        parts.append(f"📊 波动率={fund_vol*100:.1f}%（与基准{benchmark_vol*100:.1f}%相近），风险适中")

    # 信息比率解读
    if information_ratio > 0.5:
        parts.append(f"✅ 信息比率={information_ratio:.2f}（优秀）：显著跑赢基准，选股能力突出")
    elif information_ratio > 0.2:
        parts.append(f"✅ 信息比率={information_ratio:.2f}（良好）：小幅跑赢基准")
    elif information_ratio < -0.2:
        parts.append(f"❌ 信息比率={information_ratio:.2f}（不佳）：持续跑输基准")

    # 风格细分解读
    size_style = style_breakdown.get('size', '')
    value_style = style_breakdown.get('value', '')
    if size_style and size_style != '均衡':
        parts.append(f"🏢 大小盘风格：{size_style}")
    if value_style and value_style != '均衡':
        parts.append(f"💰 价值成长风格：{value_style}")

    return '\n'.join(parts)


def run_rolling_beta(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    window_20: int = 20,
    window_60: int = 60,
) -> dict:
    """
    滚动Beta监控（20/60日双窗口）

    Args:
        fund_ret: 基金日收益率序列（index为日期）
        benchmark_ret: 基准日收益率序列（index为日期）
        window_20: 短期窗口（默认20天）
        window_60: 长期窗口（默认60天）

    Returns:
        滚动Beta分析结果：
        - beta_20: 最新20日Beta
        - beta_60: 最新60日Beta
        - beta_status: Beta状态（激进/平衡/防御/低相关）
        - drift_warning: 风格漂移预警列表
        - beta_series_20: 20日滚动Beta序列
        - beta_series_60: 60日滚动Beta序列
        - interpretation: 文字解读
    """
    if fund_ret.empty or benchmark_ret.empty:
        return {
            'beta_20': 0.0,
            'beta_60': 0.0,
            'beta_status': '数据不足',
            'drift_warning': ['数据不足，无法监测'],
            'beta_series_20': pd.Series(dtype=float),
            'beta_series_60': pd.Series(dtype=float),
            'interpretation': '数据不足，无法进行滚动Beta分析',
        }

    # ========== 1. 数据对齐 ==========
    merged = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()
    if len(merged) < 60:
        return {
            'beta_20': 0.0,
            'beta_60': 0.0,
            'beta_status': '数据不足',
            'drift_warning': [f'数据不足({len(merged)}天<60天)，无法监测'],
            'beta_series_20': pd.Series(dtype=float),
            'beta_series_60': pd.Series(dtype=float),
            'interpretation': f'数据不足({len(merged)}天<60天)，无法进行滚动Beta分析',
        }

    # ========== 2. 计算滚动Beta ==========
    def calc_beta(window: int) -> pd.Series:
        """计算指定窗口的Beta"""
        if len(merged) < window:
            return pd.Series(dtype=float)

        fund_rolling = merged['fund'].rolling(window=window)
        benchmark_rolling = merged['benchmark'].rolling(window=window)

        # 协方差和方差
        cov = fund_rolling.cov(benchmark_rolling)
        bm_var = benchmark_rolling.var()

        beta = cov / bm_var if bm_var.abs() > 0.001 else 0.0
        return beta

    # 20日滚动Beta
    beta_20_series = calc_beta(window_20)

    # 60日滚动Beta
    beta_60_series = calc_beta(window_60)

    # ========== 3. 获取最新Beta值 ==========
    beta_20 = beta_20_series.iloc[-1] if not beta_20_series.empty else 0.0
    beta_60 = beta_60_series.iloc[-1] if not beta_60_series.empty else 0.0

    # ========== 4. 风格漂移检测 ==========
    drift_warnings = _detect_style_drift(beta_20, beta_60, beta_20_series, beta_60_series)

    # ========== 5. Beta状态判定 ==========
    beta_status = _determine_beta_status(beta_20, beta_60)

    # ========== 6. 文字解读 ==========
    interpretation = _interpret_rolling_beta(beta_20, beta_60, drift_warnings)

    return {
        'beta_20': beta_20,
        'beta_60': beta_60,
        'beta_status': beta_status,
        'drift_warning': drift_warnings,
        'beta_series_20': beta_20_series,
        'beta_series_60': beta_60_series,
        'interpretation': interpretation,
    }


def _detect_style_drift(
    beta_20: float,
    beta_60: float,
    beta_20_series: pd.Series,
    beta_60_series: pd.Series,
) -> List[str]:
    """检测风格漂移"""
    warnings = []

    # 1. 短期与长期Beta偏离
    beta_diff = abs(beta_20 - beta_60)
    if beta_diff > 0.4:
        warnings.append(f'⚠️ 风格剧烈漂移：短期Beta({beta_20:.2f})与长期Beta({beta_60:.2f})偏离较大（{beta_diff:.2f}），需重点关注')
    elif beta_diff > 0.3:
        warnings.append(f'📊 风格明显漂移：短期Beta({beta_20:.2f})与长期Beta({beta_60:.2f})偏离（{beta_diff:.2f}），需关注')

    # 2. 风格转向防御
    if beta_20 < 0.5 and beta_60 > 0.8:
        warnings.append(f'🛡️ 风格转向防御：近期Beta显著下降({beta_20:.2f}<0.5)，可能调整仓位或增加对冲')

    # 3. 风格转向激进
    if beta_20 > 1.2 and beta_60 < 0.8:
        warnings.append(f'🚀 风格转向激进：近期Beta显著上升({beta_20:.2f}>1.2)，可能提高股票仓位或降低对冲')

    # 4. 风格反转（激进↔防御）
    if (beta_20 > 1.0 and beta_60 < 0.5) or (beta_20 < 0.5 and beta_60 > 1.0):
        warnings.append(f'🔄 风格反转：从{"激进" if beta_60 > 1.0 else "防御"}转向{"防御" if beta_20 < 0.5 else "激进"}，需确认投资策略变更')

    # 5. Beta持续下降（60日窗口）
    if len(beta_60_series) >= 10:
        recent_60 = beta_60_series.iloc[-10:].values
        if (np.diff(recent_60) < -0.05).sum() >= 7:  # 过去10期中至少7期在下降
            warnings.append(f'📉 Beta持续下降：长期Beta呈下降趋势，可能持续降低风险暴露')

    # 6. Beta持续上升（60日窗口）
    if len(beta_60_series) >= 10:
        recent_60 = beta_60_series.iloc[-10:].values
        if (np.diff(recent_60) > 0.05).sum() >= 7:  # 过去10期中至少7期在上升
            warnings.append(f'📈 Beta持续上升：长期Beta呈上升趋势，可能持续提高风险暴露')

    # 7. Beta波动剧烈（20日窗口）
    if len(beta_20_series) >= 10:
        recent_20 = beta_20_series.iloc[-10:].values
        beta_vol = recent_20.std()
        if beta_vol > 0.3:
            warnings.append(f'⚠️ Beta波动剧烈：短期Beta标准差{beta_vol:.2f}，风格不稳定')

    return warnings


def _determine_beta_status(beta_20: float, beta_60: float) -> str:
    """判定Beta状态"""
    # 优先使用短期Beta判定
    if beta_20 > 1.2:
        return '激进'
    elif beta_20 < 0.5:
        return '防御'
    elif 0.8 <= beta_20 <= 1.2:
        return '平衡'
    else:
        return '低相关'


def _interpret_rolling_beta(
    beta_20: float,
    beta_60: float,
    drift_warnings: List[str],
) -> str:
    """解读滚动Beta分析结果"""
    parts = []

    # 1. 当前Beta状态
    if beta_20 > 1.2:
        parts.append(f"🎯 当前状态：激进型（Beta={beta_20:.2f}）- 市场敏感度高")
    elif beta_20 > 0.8:
        parts.append(f"🎯 当前状态：平衡型（Beta={beta_20:.2f}）- 与市场同步")
    elif beta_20 > 0.5:
        parts.append(f"🎯 当前状态：防御型（Beta={beta_20:.2f}）- 抗跌性强")
    elif beta_20 > 0:
        parts.append(f"🎯 当前状态：低Beta（Beta={beta_20:.2f}）- 高度防御")
    else:
        parts.append(f"🎯 当前状态：负相关（Beta={beta_20:.2f}）- 可能对冲策略")

    # 2. 长期Beta对比
    if abs(beta_20 - beta_60) < 0.1:
        parts.append(f"✅ 风格稳定：短期Beta({beta_20:.2f})与长期Beta({beta_60:.2f})高度一致")
    elif beta_20 > beta_60:
        parts.append(f"📈 风格转向激进：短期Beta({beta_20:.2f})高于长期Beta({beta_60:.2f})")
    else:
        parts.append(f"📉 风格转向防御：短期Beta({beta_20:.2f})低于长期Beta({beta_60:.2f})")

    # 3. 风格漂移预警
    if drift_warnings:
        parts.append("\n⚠️ 风格漂移预警：")
        for warning in drift_warnings:
            parts.append(f"  • {warning}")
    else:
        parts.append("✅ 风格稳定，无明显漂移")

    return '\n'.join(parts)


def calculate_radar_scores_style(
    beta: float,
    correlation: float,
    information_ratio: float,
    volatility: float,
    benchmark_volatility: float,
) -> dict:
    """
    基于风格分析结果计算雷达图评分

    Args:
        beta: Beta系数
        correlation: 与基准相关性
        information_ratio: 信息比率
        volatility: 基金年化波动率
        benchmark_volatility: 基准年化波动率

    Returns:
        雷达图评分字典（0-100）
    """
    from utils.helpers import normalize_score

    # 1. 风格稳定性评分（相关性越高，风格越稳定）
    stability_score = normalize_score(abs(correlation), 0.0, 1.0)

    # 2. 超额能力评分（信息比率）
    excess_score = normalize_score(information_ratio, -0.5, 1.0)

    # 3. 风险控制评分（波动率相对基准）
    if benchmark_volatility > 0:
        relative_vol = volatility / benchmark_volatility
        risk_score = normalize_score(-relative_vol, -1.5, -0.5)
    else:
        risk_score = 50.0  # 无基准时给予中等分数

    # 4. 风格一致性评分（Beta接近1.0为理想）
    consistency_score = normalize_score(-abs(beta - 1.0), -0.5, 0.0)

    return {
        '风格稳定': stability_score,
        '超额能力': excess_score,
        '风险控制': risk_score,
        '风格一致': consistency_score,
    }
