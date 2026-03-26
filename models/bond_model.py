"""
债券模型
债券类基金量化分析（久期归因、压力测试、持仓穿透）
依赖：config, utils, data
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm

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
    fetch_treasury_10y,
    fetch_bond_three_factors,
    fetch_bond_index,
    fetch_holdings,
)


def run_bond_analysis(
    symbol: str,
    nav_data: pd.DataFrame,
    basic_info: dict,
    holdings_data: dict,
) -> dict:
    """
    运行债券基金分析

    Args:
        symbol: 基金代码
        nav_data: 净值数据
        basic_info: 基金基本信息
        holdings_data: 持仓数据

    Returns:
        分析结果字典
    """
    nav_series = nav_data.set_index('date')['nav']
    ret_series = nav_data.set_index('date')['ret']

    # 1. 久期归因（T-Model）
    duration_results = run_duration_model(
        fund_ret=ret_series,
        nav_data=nav_data,
    )

    # 2. 债券三因子模型
    three_factor_results = run_bond_three_factors(
        fund_ret=ret_series,
        nav_data=nav_data,
    )

    # 3. 压力测试
    stress_test_results = bond_stress_test(
        duration=duration_results.get('duration', 0),
        convexity=duration_results.get('convexity', 0),
    )

    # 4. 债券持仓穿透
    bond_holdings = holdings_data.get('bond_holdings', pd.DataFrame())
    bond_structure = analyze_bond_structure(bond_holdings)

    # 5. 综合评分
    radar_scores = calculate_radar_scores(
        nav_data=nav_data,
        duration_results=duration_results,
        three_factor_results=three_factor_results,
    )

    return {
        'model_name': 'bond_model',
        'duration_results': duration_results,
        'three_factor_results': three_factor_results,
        'stress_test_results': stress_test_results,
        'bond_structure': bond_structure,
        'radar_scores': radar_scores,
    }


def run_duration_model(
    fund_ret: pd.Series,
    nav_data: pd.DataFrame,
) -> dict:
    """
    T-Model久期归因（从净值反推Duration + Convexity）

    Args:
        fund_ret: 基金收益率序列
        nav_data: 净值数据

    Returns:
        久期分析结果
    """
    if fund_ret.empty:
        return {'note': '数据不足，无法久期归因'}

    # 检查 nav_data 是否有 date 列
    if 'date' in nav_data.columns:
        start_date = nav_data['date'].min().strftime('%Y-%m-%d')
        end_date = nav_data['date'].max().strftime('%Y-%m-%d')
    else:
        start_date = nav_data.index.min().strftime('%Y-%m-%d')
        end_date = nav_data.index.max().strftime('%Y-%m-%d')

    # 获取10年国债收益率
    treasury_df = fetch_treasury_10y(start_date, end_date)

    if treasury_df.empty:
        return {'note': '国债数据缺失，无法久期归因'}

    # 对齐数据
    fund_df = pd.DataFrame({'date': fund_ret.index, 'fund_ret': fund_ret.values})
    fund_df['date'] = pd.to_datetime(fund_df['date'])

    merged = fund_df.merge(treasury_df, on='date', how='inner')
    merged = merged.sort_values('date')

    if len(merged) < 60:
        return {'note': '数据不足(<60天)，无法久期归因'}

    # 计算利率变化
    merged['rate_change'] = merged['rate'].diff()

    # 回归：基金收益率 = α + β * (-Δ利率) + ε
    X = -merged['rate_change'].fillna(0).values
    y = merged['fund_ret'].values

    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()

    duration = model.params[1]  # 系数即为久期
    r_squared = model.rsquared

    # 凸性估算（二阶导数）
    convexity = estimate_convexity_from_holdings(nav_data, holdings_data={})

    interpretation = _interpret_duration(duration, r_squared, convexity)

    return {
        'duration': duration,
        'convexity': convexity,
        'r_squared': r_squared,
        'interpretation': interpretation,
    }


def estimate_convexity_from_holdings(nav_data: pd.DataFrame, holdings_data: dict) -> float:
    """
    从持仓估算凸性（简化版）

    Returns:
        凸性值
    """
    # 简化：假设凸性与久期平方成正比
    duration_est = 3.0  # 默认久期
    convexity = duration_est ** 2 / 100
    return convexity


def _interpret_duration(duration: float, r_squared: float, convexity: float) -> str:
    """解读久期结果"""
    parts = []

    # 久期水平
    if duration < 2:
        level = '短久期（<2年）'
    elif duration < 5:
        level = '中久期（2-5年）'
    elif duration < 8:
        level = '中长久期（5-8年）'
    else:
        level = '长久期（>8年）'

    parts.append(f"久期估计：{duration:.2f}年（{level}）")

    # 模型解释力
    if r_squared < 0.3:
        parts.append(f"⚠️ 模型解释力低(R²={r_squared:.2f})，可能包含非利率因素（信用、杠杆等）")
    else:
        parts.append(f"利率敏感度：收益率与利率变动相关性R²={r_squared:.2f}")

    # 凸性
    if abs(convexity) < config.MODEL_CONFIG['duration']['convexity_threshold']:
        parts.append(f"⚠️ 凸性较小({convexity:.2f})，样本期内利率波动不足")

    return '\n'.join(parts)


def run_bond_three_factors(
    fund_ret: pd.Series,
    nav_data: pd.DataFrame,
) -> dict:
    """
    债券三因子模型：短端利率 + 长端利率 + 信用利差

    Args:
        fund_ret: 基金收益率序列
        nav_data: 净值数据

    Returns:
        三因子回归结果
    """
    if fund_ret.empty:
        return {'note': '数据不足，无法三因子回归'}

    # 检查 nav_data 是否有 date 列
    if 'date' in nav_data.columns:
        start_date = nav_data['date'].min().strftime('%Y-%m-%d')
        end_date = nav_data['date'].max().strftime('%Y-%m-%d')
    else:
        start_date = nav_data.index.min().strftime('%Y-%m-%d')
        end_date = nav_data.index.max().strftime('%Y-%m-%d')

    # 获取债券三因子
    three_factors = fetch_bond_three_factors(start_date, end_date)

    if three_factors.empty or len(three_factors) < 60:
        return {'note': '三因子数据不足'}

    # 对齐数据
    fund_df = pd.DataFrame({'date': fund_ret.index, 'fund_ret': fund_ret.values})
    fund_df['date'] = pd.to_datetime(fund_df['date'])

    merged = fund_df.merge(three_factors, on='date', how='inner')
    merged = merged.sort_values('date')

    # 前向填充
    merged = merged.fillna(method='ffill', limit=3)

    # 选择因子列
    factor_cols = []
    if 'y2y' in merged.columns and merged['y2y'].notna().sum() > 30:
        factor_cols.append('y2y')
    if 'y10y' in merged.columns:
        factor_cols.append('y10y')
    if 'credit_spread' in merged.columns:
        factor_cols.append('credit_spread')

    if len(factor_cols) < 2:
        return {'note': '可用因子不足，无法三因子回归'}

    df_full = merged[['date', 'fund_ret'] + factor_cols].dropna()

    if len(df_full) < 60:
        return {'note': '对齐后数据不足(<60天)'}

    # 运行回归
    X = df_full[factor_cols]
    y = df_full['fund_ret']

    X = sm.add_constant(X)
    model = sm.OLS(y, X).fit()

    alpha_annual = model.params['const'] * 252
    alpha_pval = model.pvalues['const']
    r_squared = model.rsquared

    factor_betas = {}
    for col in factor_cols:
        factor_betas[col] = model.params.get(col, 0)

    interpretation = _interpret_bond_three_factors(alpha_annual, alpha_pval, factor_betas)

    return {
        'alpha': alpha_annual,
        'alpha_pval': alpha_pval,
        'r_squared': r_squared,
        'factor_betas': factor_betas,
        'interpretation': interpretation,
    }


def _interpret_bond_three_factors(
    alpha: float,
    alpha_pval: float,
    factor_betas: dict,
) -> str:
    """解读债券三因子结果"""
    parts = []

    # Alpha解读
    if alpha_pval < 0.05:
        if alpha > 0:
            parts.append(f"✅ 显著信用Alpha {alpha*100:+.1f}%/年")
        else:
            parts.append(f"❌ 显著负Alpha {alpha*100:+.1f}%/年")
    else:
        parts.append(f"Alpha不显著({alpha*100:+.1f}%/年)")

    # 因子暴露
    if 'y2y' in factor_betas:
        beta_y2y = factor_betas['y2y']
        if abs(beta_y2y) > 0.5:
            direction = '短端敏感' if beta_y2y > 0 else '短端免疫'
            parts.append(f"• {direction}（β_y2y={beta_y2y:+.2f}）")

    if 'y10y' in factor_betas:
        beta_y10y = factor_betas['y10y']
        if abs(beta_y10y) > 0.5:
            direction = '长端敏感' if beta_y10y > 0 else '长端免疫'
            parts.append(f"• {direction}（β_y10y={beta_y10y:+.2f}）")

    if 'credit_spread' in factor_betas:
        beta_credit = factor_betas['credit_spread']
        if abs(beta_credit) > 0.5:
            direction = '信用利空受损' if beta_credit < 0 else '信用利空受益'
            parts.append(f"• {direction}（β_credit={beta_credit:+.2f}）")

    return '\n'.join(parts) if parts else '无明显风格特征'


def bond_stress_test(
    duration: float,
    convexity: float,
) -> dict:
    """
    债券压力测试（多场景）

    Args:
        duration: 久期
        convexity: 凸性

    Returns:
        压力测试结果
    """
    scenarios = config.MODEL_CONFIG['stress_test']['scenarios']

    results = []
    for scenario in scenarios:
        name = scenario['name']
        short_bp = scenario['short']
        long_bp = scenario['long']
        credit_bp = scenario['credit']

        # 计算预估收益：ΔP ≈ -D * Δy + 0.5 * Conv * (Δy)²
        delta_y = (short_bp + long_bp) / 2 / 10000  # 转为小数
        delta_credit = credit_bp / 10000

        # 利率冲击
        rate_impact = -duration * delta_y + 0.5 * convexity * (delta_y ** 2)

        # 信用利差冲击（简化：信用利差扩大1bp = -1bp收益）
        credit_impact = -delta_credit * duration * 0.3  # 假设信用利差beta=0.3

        total_impact = rate_impact + credit_impact

        results.append({
            'name': name,
            'short_bp': short_bp,
            'long_bp': long_bp,
            'credit_bp': credit_bp,
            'rate_impact_pct': rate_impact * 100,
            'credit_impact_pct': credit_impact * 100,
            'total_impact_pct': total_impact * 100,
        })

    # 找到最坏情况
    worst = min(results, key=lambda x: x['total_impact_pct'])

    return {
        'scenarios': results,
        'worst_case': worst,
        'interpretation': _interpret_stress_test(worst),
    }


def _interpret_stress_test(worst: dict) -> str:
    """解读压力测试结果"""
    impact = worst['total_impact_pct']
    name = worst['name']

    if impact > -1:
        return f"极端情况({name})下预估回撤约{abs(impact):.1f}%，抗风险能力较强"
    elif impact > -3:
        return f"⚠️ 极端情况({name})下预估回撤约{abs(impact):.1f}%，需关注风险"
    elif impact > -5:
        return f"⚠️ 极端情况({name})下预估回撤约{abs(impact):.1f}%，风险较高"
    else:
        return f"❌ 极端情况({name})下预估回撤约{abs(impact):.1f}%，需严格控制仓位"


def analyze_bond_structure(bond_holdings: pd.DataFrame) -> dict:
    """
    债券持仓穿透分析

    Args:
        bond_holdings: 债券持仓DataFrame

    Returns:
        债券结构分析结果
    """
    if bond_holdings.empty:
        return {
            'total_weight': 0,
            'rate_ratio': 0,
            'credit_ratio': 0,
            'convert_ratio': 0,
            'top_holdings': pd.DataFrame(),
        }

    # 统计债券类型分布
    type_dist = {}
    for _, row in bond_holdings.iterrows():
        bond_name = str(row.get('债券名称', ''))
        weight = float(row.get('占净值比例', 0))

        # 简化分类
        if '国债' in bond_name or '国开' in bond_name:
            btype = '利率债'
        elif '可转债' in bond_name or '转债' in bond_name:
            btype = '可转债'
        elif '信用' in bond_name:
            btype = '信用债'
        else:
            btype = '其他'

        type_dist[btype] = type_dist.get(btype, 0) + weight

    total_weight = sum(type_dist.values())
    if total_weight == 0:
        return {
            'total_weight': 0,
            'rate_ratio': 0,
            'credit_ratio': 0,
            'convert_ratio': 0,
            'type_distribution': {},
            'top_holdings': bond_holdings.head(10),
        }

    # 计算比例
    rate_ratio = type_dist.get('利率债', 0) / total_weight
    credit_ratio = type_dist.get('信用债', 0) / total_weight
    convert_ratio = type_dist.get('可转债', 0) / total_weight

    return {
        'total_weight': total_weight,
        'rate_ratio': rate_ratio,
        'credit_ratio': credit_ratio,
        'convert_ratio': convert_ratio,
        'type_distribution': type_dist,
        'top_holdings': bond_holdings.head(10),
    }


def calculate_radar_scores(
    nav_data: pd.DataFrame,
    duration_results: dict,
    three_factor_results: dict,
) -> dict:
    """
    计算债券基金雷达图五维评分

    Args:
        nav_data: 净值数据
        duration_results: 久期归因结果
        three_factor_results: 三因子结果

    Returns:
        五维评分字典
    """
    from utils.helpers import calculate_sharpe, calculate_max_drawdown

    # 债券型权重
    weights = config.RADAR_WEIGHTS['bond']

    # 检查 nav_data 是否已经有 date 列
    if 'date' in nav_data.columns:
        nav_series = nav_data.set_index('date')['nav']
        ret_series = nav_data.set_index('date')['ret']
    else:
        # 假设已经是 index
        nav_series = nav_data['nav']
        ret_series = nav_data['ret']

    # 1. 超额能力（Alpha）
    alpha = three_factor_results.get('alpha', 0)
    alpha_score = normalize_score(alpha, -0.02, 0.03)

    # 2. 风险控制（回撤+波动，债券基金更看重回撤）
    max_dd, _ = calculate_max_drawdown(nav_series)
    vol = ret_series.std() * np.sqrt(252)
    risk_score = normalize_score(-max_dd, -0.10, 0) * 0.7 + normalize_score(-vol, -0.10, 0) * 0.3

    # 3. 性价比（夏普比率）
    sharpe = calculate_sharpe(ret_series)
    efficiency_score = normalize_score(sharpe, -0.5, 2.0)

    # 4. 风格稳定性（R²）
    r2 = three_factor_results.get('r_squared', 0)
    stability_score = normalize_score(r2, 0, 1.0)

    # 5. 业绩持续性（胜率）
    win_rate = (ret_series > 0).mean()
    persistency_score = normalize_score(win_rate, 0.4, 0.6)

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
