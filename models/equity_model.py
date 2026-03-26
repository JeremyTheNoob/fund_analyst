"""
权益模型
权益类基金量化分析（FF因子、Brinson归因、风格分析）
依赖：config, utils, data
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from datetime import datetime, timedelta

import config
from utils.helpers import (
    normalize_score,
    safe_divide,
    annualize_return,
    annualize_volatility,
    calculate_sharpe,
    calculate_max_drawdown,
)
from data import (
    fetch_ff_factors,
    fetch_index_daily,
    fetch_sw_industry_ret,
    build_benchmark_ret,
    fetch_hk_index_daily,
)
from models.alpha_analysis import (
    calculate_alpha_hierarchical,
    calculate_timing_ability,
    calculate_monthly_win_rate,
    resample_to_weekly,
)
from models.holdings_analysis import analyze_holdings_penetration


def run_ff_model(
    fund_ret: pd.Series,
    factors: pd.DataFrame,
    model_type: str = 'ff3'
) -> dict:
    """
    权益类多因子回归

    Args:
        fund_ret: 基金收益率序列
        factors: 因子DataFrame，包含date和各因子列
        model_type: 模型类型（'capm'/'ff3'/'ff5'/'carhart'）

    Returns:
        回归结果字典
    """
    # 数据对齐
    df_base = pd.DataFrame({'fund_ret': fund_ret}).reset_index()
    df_base.columns = ['date', 'fund_ret']
    df_base['date'] = pd.to_datetime(df_base['date'])

    factors_aligned = factors.copy()
    factors_aligned['date'] = pd.to_datetime(factors_aligned['date'])

    df_base = df_base.merge(factors_aligned, on='date', how='left')

    # 选择因子列
    factor_map = {
        'capm': ['Mkt'],
        'ff3': ['Mkt', 'SMB', 'HML'],
        'ff5': ['Mkt', 'SMB', 'HML', 'RMW'],
        'carhart': ['Mkt', 'SMB', 'HML', 'Short_MOM'],
    }
    use_cols = [c for c in factor_map.get(model_type, ['Mkt', 'SMB', 'HML'])
                if c in df_base.columns and df_base[c].notna().sum() > 30]

    if not use_cols:
        return _empty_ff_result('因子列缺失，无法回归')

    # 前向填充
    df_base[use_cols] = df_base[use_cols].ffill(limit=3)
    df_full = df_base[['date', 'fund_ret'] + use_cols].dropna()

    if len(df_full) < 60:
        return _empty_ff_result('数据不足(<60天)，无法回归')

    # 运行回归
    res = _run_single_ff(df_full, use_cols)
    if res is None:
        return _empty_ff_result('回归失败')

    return res


def _run_single_ff(df: pd.DataFrame, factor_cols: list) -> dict:
    """运行单次FF回归"""
    X = df[factor_cols]
    y = df['fund_ret']

    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()

    alpha_annual = model.params['const'] * 252
    alpha_pval = model.pvalues['const']
    r_squared = model.rsquared

    factor_betas = {}
    factor_betas_raw = {}
    for col in factor_cols:
        beta = model.params.get(col, 0)
        factor_betas_raw[col] = beta
        # 标准化beta（因子收益率标准差归一化）
        beta_std = X[col].std()
        if beta_std > 0:
            factor_betas[col] = beta * beta_std
        else:
            factor_betas[col] = 0

    interpretation = _interpret_ff_results(alpha_annual, alpha_pval, factor_betas, factor_cols)

    return {
        'alpha': alpha_annual,
        'alpha_pval': alpha_pval,
        'r_squared': r_squared,
        'factor_betas': factor_betas,
        'factor_betas_raw': factor_betas_raw,
        'interpretation': interpretation,
    }


def _empty_ff_result(note: str) -> dict:
    """空结果"""
    return {
        'alpha': 0.0,
        'alpha_pval': 1.0,
        'r_squared': 0.0,
        'factor_betas': {},
        'factor_betas_raw': {},
        'interpretation': note,
    }


def _interpret_ff_results(
    alpha: float,
    alpha_pval: float,
    betas: dict,
    factor_cols: list
) -> str:
    """解读FF模型结果"""
    parts = []

    # Alpha解读
    if alpha_pval < 0.05:
        if alpha > 0:
            parts.append(f"✅ 显著正Alpha {alpha*100:+.1f}%/年，经理具备选股能力")
        else:
            parts.append(f"❌ 显著负Alpha {alpha*100:+.1f}%/年，持续跑输市场")
    else:
        parts.append(f"Alpha不显著({alpha*100:+.1f}%/年)，超额收益可能来自运气")

    # 因子暴露解读
    factor_names = {
        'Mkt': '市场',
        'SMB': '小盘',
        'HML': '价值',
        'RMW': '质量',
        'Short_MOM': '动量',
    }

    for col in factor_cols:
        if col in betas:
            beta = betas[col]
            name = factor_names.get(col, col)
            if abs(beta) > 0.1:
                direction = '超配' if beta > 0 else '低配'
                parts.append(f"• {name}因子{direction}（beta={beta:+.2f}）")

    return '\n'.join(parts) if parts else '无明显风格特征'


def run_equity_analysis(
    symbol: str,
    nav_data: pd.DataFrame,
    basic_info: dict,
    holdings_data: dict,
    model_type: str = 'equity',
) -> dict:
    """
    运行权益基金分析

    Args:
        symbol: 基金代码
        nav_data: 净值数据
        basic_info: 基金基本信息
        holdings_data: 持仓数据
        model_type: 模型类型（equity/mixed/index/sector/qdii）

    Returns:
        分析结果字典
    """
    nav_series = nav_data.set_index('date')['nav']
    ret_series = nav_data.set_index('date')['ret']

    # 1. 业绩基准
    benchmark_ret = _build_benchmark(basic_info.get('benchmark_parsed', {}), nav_data)
    if benchmark_ret.empty:
        # 使用默认基准
        if model_type in ('equity', 'mixed'):
            mkt_df = fetch_index_daily('sh000300', nav_data['date'].min(), nav_data['date'].max())
            if not mkt_df.empty:
                benchmark_ret = mkt_df.set_index('date')['ret']
        elif model_type == 'index':
            # 指数基金用自身类型对应基准
            benchmark_ret = pd.Series(0, index=ret_series.index)
        else:
            benchmark_ret = pd.Series(0, index=ret_series.index)

    # 2. FF因子模型
    start_date = nav_data['date'].min().strftime('%Y-%m-%d')
    end_date = nav_data['date'].max().strftime('%Y-%m-%d')

    ff_factors = fetch_ff_factors(start_date, end_date)
    ff_results = {}
    if not ff_factors.empty:
        # 自动选择模型
        if 'RMW' in ff_factors.columns and ff_factors['RMW'].notna().sum() > 30:
            ff_model_type = 'ff5'
        elif 'Short_MOM' in ff_factors.columns:
            ff_model_type = 'carhart'
        else:
            ff_model_type = 'ff3'

        ff_results = run_ff_model(ret_series, ff_factors, ff_model_type)
        ff_results['model_type'] = ff_model_type
    else:
        ff_results = _empty_ff_result('FF因子数据缺失')

    # 3. Brinson归因（混合型基金）
    brinson_results = {}
    if model_type == 'mixed':
        brinson_results = run_brinson(
            fund_ret=ret_series,
            stock_ratio=holdings_data['stock_ratio'],
            bond_ratio=holdings_data['bond_ratio'],
            benchmark_ret=benchmark_ret,
        )

    # 4. 风格分析
    style_analysis = analyze_style(ret_series, benchmark_ret, ff_results)

    # 5. Alpha v2.0专业分层版（周频）
    alpha_v2_results = {}
    industry_returns = {}  # 行业中性化所需数据（暂不实现，预留接口）

    # 只有权益型和混合型基金才进行Alpha分析
    if model_type in ('equity', 'mixed', 'sector'):
        try:
            # 5.1 三层次Alpha计算（周频）
            alpha_v2_results['hierarchical'] = calculate_alpha_hierarchical(
                fund_ret=ret_series,
                benchmark_ret=benchmark_ret,
                ff_factors=ff_factors if isinstance(ff_factors, pd.DataFrame) else None,  # 只传递DataFrame
                industry_returns=industry_returns,  # 可选：行业中性化
                risk_free_rate=0.03,
                frequency='weekly'  # 周频回归
            )

            # 5.2 Treynor-Mazuy择时检测（独立轨道）
            alpha_v2_results['timing'] = calculate_timing_ability(
                fund_ret=ret_series,
                benchmark_ret=benchmark_ret,
                risk_free_rate=0.03,
                frequency='weekly'
            )

            # 5.3 月度Alpha胜率分析
            alpha_v2_results['monthly_win_rate'] = calculate_monthly_win_rate(
                fund_ret=ret_series,
                benchmark_ret=benchmark_ret,
                months=36
            )

        except Exception as e:
            # Alpha分析失败，不影响主流程
            alpha_v2_results = {'error': str(e)}

    # 6. 综合评分
    radar_scores = calculate_radar_scores(
        model_type=model_type,
        ff_results=ff_results,
        nav_data=nav_data,
        benchmark_df=pd.DataFrame({'date': benchmark_ret.index, 'ret': benchmark_ret}),
    )

    # 7. 持仓穿透分析(权益型基金)
    holdings_penetration = {}
    if model_type in ('equity', 'mixed', 'sector'):
        top10 = holdings_data.get('top10', pd.DataFrame())
        historical_holdings = holdings_data.get('historical_holdings', {})
        holdings_penetration = analyze_holdings_penetration(
            holdings_df=top10,
            historical_holdings=historical_holdings,
            ff_results=ff_results,
        )

    return {
        'model_name': f'{model_type}_model',
        'ff_results': ff_results,
        'brinson_results': brinson_results,
        'style_analysis': style_analysis,
        'radar_scores': radar_scores,
        'benchmark_ret': benchmark_ret,  # 添加基准收益率
        'alpha_v2': alpha_v2_results,  # Alpha v2.0专业分层版结果
        'holdings_penetration': holdings_penetration,  # 持仓穿透分析
    }


def _build_benchmark(benchmark_parsed: dict, nav_data: pd.DataFrame) -> pd.Series:
    """构建业绩基准"""
    if not benchmark_parsed or not benchmark_parsed.get('components'):
        return pd.Series()

    start_date = nav_data['date'].min()
    end_date = nav_data['date'].max()

    parts = []
    for comp in benchmark_parsed['components']:
        try:
            code = comp['code']
            weight = comp['weight']

            # 港股指数特殊处理
            if code.startswith('hk:'):
                from data import fetch_hk_index_daily
                df = fetch_hk_index_daily(code[3:], start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            else:
                df = fetch_index_daily(code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

            if not df.empty:
                df['weighted'] = df['ret'] * weight
                parts.append(df[['date', 'weighted']])
        except Exception:
            pass

    if not parts:
        return pd.Series()

    merged = parts[0].rename(columns={'weighted': 'bm_ret'})
    for part in parts[1:]:
        merged = merged.merge(part, on='date', how='outer', suffixes=('', '_dup'))
        merged['bm_ret'] = merged['bm_ret'].fillna(0) + merged['weighted'].fillna(0)
        merged.drop(columns=['weighted'], inplace=True, errors='ignore')

    merged['bm_ret'] = merged['bm_ret'].fillna(0)
    return merged.set_index('date')['bm_ret']


def run_brinson(
    fund_ret: pd.Series,
    stock_ratio: float,
    bond_ratio: float,
    benchmark_ret: pd.Series,
) -> dict:
    """
    Brinson归因（配置效应+选择效应+交互效应）

    基于原始 fund_analysis.py 中的实现逻辑

    Args:
        fund_ret: 基金收益率序列
        stock_ratio: 基金股票仓位
        bond_ratio: 基金债券仓位
        benchmark_ret: 基准收益率序列

    Returns:
        归因结果字典，包含:
        - allocation: 配置效应（年化）
        - selection: 选择效应（年化）
        - interaction: 交互效应（年化）
        - excess_return: 超额收益（年化）
        - fund_annual: 基金年化收益
        - benchmark_annual: 基准年化收益
        - interpretation: 文字解读
    """
    # 检查数据有效性
    if fund_ret.empty or benchmark_ret.empty:
        return {
            'allocation': 0.0,
            'selection': 0.0,
            'interaction': 0.0,
            'excess_return': 0.0,
            'fund_annual': 0.0,
            'benchmark_annual': 0.0,
            'interpretation': '数据不足，无法进行Brinson归因',
        }

    # 数据对齐
    df = pd.DataFrame({'fund': fund_ret, 'benchmark': benchmark_ret}).dropna()
    if len(df) < 60:  # 至少需要60天数据
        return {
            'allocation': 0.0,
            'selection': 0.0,
            'interaction': 0.0,
            'excess_return': 0.0,
            'fund_annual': 0.0,
            'benchmark_annual': 0.0,
            'interpretation': f'数据不足({len(df)}天<60天)，无法进行Brinson归因',
        }

    # 年化收益率（复合收益率）
    fund_annual = (1 + df['fund']).prod() ** (252 / len(df)) - 1
    benchmark_annual = (1 + df['benchmark']).prod() ** (252 / len(df)) - 1
    excess_return = fund_annual - benchmark_annual

    # 默认基准权重（60%股票+40%债券）
    bm_stock_weight = 0.6
    bm_bond_weight = 0.4

    # 计算配置效应（Allocation Effect）
    # 配置效应 = (基金股票仓位 - 基准股票仓位) * 基准股票年化收益
    #           + (基金债券仓位 - 基准债券仓位) * 基准债券年化收益
    # 注意：这里简化处理，用基准整体收益率代替股票/债券的分别收益率
    # 在完整实现中，需要分别计算股票指数和债券指数的收益率
    allocation = ((stock_ratio - bm_stock_weight) * benchmark_annual +
                  (bond_ratio - bm_bond_weight) * 0)  # 债券部分暂设为0

    # 选择效应（Selection Effect）
    # 选择效应 = 基准股票权重 * (基金股票收益率 - 基准股票收益率)
    #           + 基准债券权重 * (基金债券收益率 - 基准债券收益率)
    # 简化处理：基金股票收益率 ≈ 基金整体收益率 / stock_ratio（假设全部收益来自股票）
    if stock_ratio > 0:
        fund_stock_return = fund_annual / stock_ratio
        # 假设基准股票收益率 ≈ 基准整体收益率 / bm_stock_weight
        benchmark_stock_return = benchmark_annual / bm_stock_weight if bm_stock_weight > 0 else benchmark_annual
        selection = bm_stock_weight * (fund_stock_return - benchmark_stock_return)
    else:
        selection = 0.0

    # 交互效应（Interaction Effect）
    # 交互效应 = (基金股票仓位 - 基准股票仓位) * (基金股票收益率 - 基准股票收益率)
    #           + (基金债券仓位 - 基准债券仓位) * (基金债券收益率 - 基准债券收益率)
    if stock_ratio > 0 and bm_stock_weight > 0:
        fund_stock_return = fund_annual / stock_ratio
        benchmark_stock_return = benchmark_annual / bm_stock_weight
        interaction = ((stock_ratio - bm_stock_weight) * (fund_stock_return - benchmark_stock_return) +
                       (bond_ratio - bm_bond_weight) * 0)  # 债券部分暂设为0
    else:
        interaction = 0.0

    # 校验：配置效应 + 选择效应 + 交互效应 ≈ 超额收益
    # 如果误差较大，进行调整
    calculated_excess = allocation + selection + interaction
    if abs(calculated_excess - excess_return) > 0.001:
        # 调整选择效应，使三者之和等于超额收益
        selection = excess_return - allocation - interaction

    # 生成文字解读
    interpretation = _interpret_brinson(allocation, selection, excess_return)

    return {
        'allocation': allocation,
        'selection': selection,
        'interaction': interaction,
        'excess_return': excess_return,
        'fund_annual': fund_annual,
        'benchmark_annual': benchmark_annual,
        'interpretation': interpretation,
    }


def _interpret_brinson(alloc: float, selection: float, excess: float) -> str:
    """解读Brinson归因结果"""
    parts = []

    if abs(alloc) > 0.01:
        direction = '多配股票' if alloc > 0 else '多配债券'
        parts.append(f"配置效应{direction}贡献{alloc*100:+.2f}%")

    if abs(selection) > 0.01:
        direction = '超额' if selection > 0 else '落后'
        parts.append(f"选股效应{direction}贡献{selection*100:+.2f}%")

    if not parts:
        return "资产配置与选股效果均不明显"

    return '\n'.join(parts)


def analyze_style(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
    ff_results: dict,
) -> dict:
    """分析基金风格"""
    if fund_ret.empty:
        return {'note': '数据不足'}

    # 与基准相关性
    if not benchmark_ret.empty:
        merged = pd.DataFrame({'fund': fund_ret, 'bm': benchmark_ret}).dropna()
        correlation = merged['fund'].corr(merged['bm'])
    else:
        correlation = 0.0

    # 波动率对比
    fund_vol = fund_ret.std() * np.sqrt(252)
    bm_vol = benchmark_ret.std() * np.sqrt(252) if not benchmark_ret.empty else 0.15

    # Beta
    beta = correlation * (fund_vol / bm_vol) if bm_vol > 0 else 0

    # 风格解读
    if beta > 1.2:
        style = '激进型（高Beta）'
    elif beta > 0.8:
        style = '平衡型'
    elif beta > 0.5:
        style = '防御型（低Beta）'
    else:
        style = '低相关（独立风格）'

    return {
        'beta': beta,
        'correlation': correlation,
        'style': style,
    }


def calculate_radar_scores(
    model_type: str,
    ff_results: dict,
    nav_data: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> dict:
    """
    计算雷达图五维评分

    Args:
        model_type: 模型类型
        ff_results: FF因子结果
        nav_data: 净值数据
        benchmark_df: 基准数据

    Returns:
        五维评分字典
    """
    from utils.helpers import calculate_sharpe, calculate_max_drawdown

    # 获取权重
    weights = config.RADAR_WEIGHTS.get(model_type, config.RADAR_WEIGHTS['others'])

    nav_series = nav_data.set_index('date')['nav']
    ret_series = nav_data.set_index('date')['ret']

    # 1. 超额能力（Alpha）
    alpha = ff_results.get('alpha', 0)
    alpha_score = normalize_score(alpha, -0.10, 0.15)

    # 2. 风险控制（回撤+波动）
    max_dd, _ = calculate_max_drawdown(nav_series)
    vol = ret_series.std() * np.sqrt(252)
    risk_score = normalize_score(-max_dd, -0.30, 0) * 0.6 + normalize_score(-vol, -0.30, 0) * 0.4

    # 3. 性价比（夏普比率）
    sharpe = calculate_sharpe(ret_series)
    efficiency_score = normalize_score(sharpe, -0.5, 2.0)

    # 4. 风格稳定性（R²）
    r2 = ff_results.get('r_squared', 0)
    stability_score = normalize_score(r2, 0, 1.0)

    # 5. 业绩持续性（简化版）
    # 胜率
    win_rate = (ret_series > 0).mean()
    persistency_score = normalize_score(win_rate, 0.4, 0.6)

    # 加权总分
    scores = {
        '超额能力': alpha_score,
        '风险控制': risk_score,
        '性价比': efficiency_score,
        '风格稳定': stability_score,
        '业绩持续': persistency_score,
    }

    total_score = sum(scores[k] * weights[k] for k in scores)

    return {
        'scores': scores,
        'weights': weights,
        'total_score': total_score,
    }


def _build_benchmark(benchmark_parsed: dict, nav_data: pd.DataFrame) -> pd.Series:
    """
    构建业绩基准收益率序列

    Args:
        benchmark_parsed: 解析后的基准配置（从基本信息获取）
        nav_data: 净值数据（用于确定日期范围）

    Returns:
        基准收益率序列（index为日期）
    """
    if not benchmark_parsed or not benchmark_parsed.get('components'):
        # 返回空Series，使用默认基准
        return pd.Series(dtype=float)

    start_str = nav_data['date'].min().strftime('%Y-%m-%d')
    end_str = nav_data['date'].max().strftime('%Y-%m-%d')

    try:
        bm_df = build_benchmark_ret(benchmark_parsed, start_str, end_str)
        if bm_df.empty:
            return pd.Series(dtype=float)
        return bm_df.set_index('date')['bm_ret']
    except Exception:
        # 构建失败，返回空Series
        return pd.Series(dtype=float)
