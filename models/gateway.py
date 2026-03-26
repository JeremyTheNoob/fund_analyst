"""
逻辑网关
根据基金特征自动选择合适的模型
依赖：config, utils, data
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np
import config
from utils.helpers import safe_divide
from data import (
    fetch_basic_info,
    fetch_nav,
    fetch_ff_factors,
    fetch_bond_three_factors,
    fetch_holdings,
    fetch_index_daily,
    fetch_bond_index,
    fetch_sw_industry_ret,
)
from data.processor import (
    detect_sw_industry,
    run_rolling_beta,
    run_sector_model,
    performance_decomposition,
)


def classify_fund_type(
    stock_ratio: float,
    bond_ratio: float,
    type_category: str
) -> str:
    """
    根据股票仓位和基金类型分类

    Args:
        stock_ratio: 股票仓位（0-1）
        bond_ratio: 债券仓位（0-1）
        type_category: 基金类型分类（equity/bond/mixed/index/sector/qdii）

    Returns:
        最终的模型类型：'equity' / 'bond' / 'mixed' / 'index' / 'sector' / 'qdii'
    """
    # 货币基金
    if type_category == 'money':
        return 'money'

    # QDII单独处理
    if type_category == 'qdii':
        return 'qdii'

    # 指数/ETF/行业/主题基金直接使用分类结果
    if type_category in ('index', 'sector'):
        return type_category

    # 根据股票仓位判断
    if stock_ratio >= config.STOCK_RATIO_THRESHOLDS['equity_high']:
        return 'equity'
    elif stock_ratio <= config.STOCK_RATIO_THRESHOLDS['bond_low']:
        return 'bond'
    else:
        return 'mixed'


def analyze_fund(
    symbol: str,
    years: int = 5,
    since_inception: bool = False
) -> dict:
    """
    统一的基金分析入口
    自动识别基金类型，选择合适的模型

    Args:
        symbol: 基金代码
        years: 分析年数
        since_inception: 是否从成立日开始

    Returns:
        分析结果字典，包含：
        - basic_info: 基金基本信息
        - nav_data: 净值数据
        - model_type: 使用的模型类型
        - model_results: 模型分析结果
        - risk_metrics: 风险指标
        - performance: 业绩指标
    """
    from models.equity_model import run_equity_analysis
    from models.bond_model import run_bond_analysis

    # 1. 获取基本信息
    basic_info = fetch_basic_info(symbol)
    type_category = basic_info['type_category']

    # 2. 获取净值数据
    nav_data = fetch_nav(symbol, years=years, since_inception=since_inception)

    if nav_data.empty:
        return {
            'basic_info': basic_info,
            'nav_data': nav_data,
            'model_type': 'none',
            'model_results': {},
            'error': '净值数据为空'
        }

    # 3. 获取持仓数据
    holdings_data = fetch_holdings(symbol, type_category=type_category)

    # 4. 判断基金类型
    stock_ratio = holdings_data['stock_ratio']
    bond_ratio = holdings_data['bond_ratio']
    model_type = classify_fund_type(stock_ratio, bond_ratio, type_category)

    # 5. 根据类型运行对应模型
    if model_type in ('equity', 'mixed', 'index', 'sector', 'qdii'):
        # 权益/混合/指数模型
        model_results = run_equity_analysis(
            symbol=symbol,
            nav_data=nav_data,
            basic_info=basic_info,
            holdings_data=holdings_data,
            model_type=model_type,
        )
    elif model_type == 'bond':
        # 债券模型
        model_results = run_bond_analysis(
            symbol=symbol,
            nav_data=nav_data,
            basic_info=basic_info,
            holdings_data=holdings_data,
        )
    else:
        # 货币基金等不分析
        model_results = {
            'model_name': f'{model_type}_skip',
            'note': f'{type_category}类型基金暂不支持深度分析'
        }

    # 6. 计算基础指标
    nav_series = nav_data.set_index('date')['nav']
    ret_series = nav_data.set_index('date')['ret']

    from utils.helpers import (
        annualize_return,
        annualize_volatility,
        calculate_sharpe,
        calculate_max_drawdown,
    )

    total_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1
    annual_return = annualize_return(ret_series.mean())
    annual_vol = annualize_volatility(ret_series.std())
    sharpe_ratio = calculate_sharpe(ret_series)
    max_dd, recovery_days = calculate_max_drawdown(nav_series)

    performance = {
        'total_return': total_return,
        'annual_return': annual_return,
        'annual_volatility': annual_vol,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_dd,
        'recovery_days': recovery_days,
    }

    # 7. 第二阶段增强功能(仅权益类基金)
    sector_results = {}
    if model_type in ('equity', 'mixed', 'index', 'sector', 'qdii'):
        model_results['stage2'] = {}

        # 7.1 申万行业检测(所有权益类基金,不仅仅限于sector类型)
        try:
            sw_code, sw_name = detect_sw_industry(
                basic_info['name'],
                holdings_data.get('sector_weights', {})
            )

            if sw_code:
                # 获取申万行业指数数据
                start_date = nav_data['date'].iloc[0].strftime('%Y%m%d')
                end_date = nav_data['date'].iloc[-1].strftime('%Y%m%d')
                sw_ret = fetch_sw_industry_ret(sw_code, start_date, end_date)
                if not sw_ret.empty:
                    sector_results = run_sector_model(
                        fund_ret=ret_series,
                        bm_ret=model_results.get('benchmark_ret', pd.Series()),
                        sw_industry_ret=sw_ret,
                        sw_industry_name=sw_name,
                        fund_name=basic_info['name'],
                    )
                    sector_results['sw_code'] = sw_code
                    sector_results['sector_weight'] = holdings_data.get('sector_weights', {}).get(sw_name, 0)
        except Exception as e:
            import traceback
            traceback.print_exc()
            sector_results = {'error': f'申万行业模型失败: {str(e)}'}

        if sector_results:
            model_results['stage2']['sector'] = sector_results

        # 7.2 滚动Beta监控(所有权益类基金,不管bond_ratio)
        try:
            start_date = nav_data['date'].iloc[0].strftime('%Y%m%d')
            end_date = nav_data['date'].iloc[-1].strftime('%Y%m%d')
            bond_index_df = fetch_bond_index(start_date, end_date)
            bond_index_ret = bond_index_df.set_index('date')['ret'] if not bond_index_df.empty else pd.Series()

            rolling_df = run_rolling_beta(
                fund_ret=ret_series,
                stock_index_ret=model_results.get('benchmark_ret', pd.Series()),  # 用股票指数
                bond_index_ret=bond_index_ret,  # 债券指数
                window=20
            )

            # 风格漂移检测
            static_stock_ratio = holdings_data['stock_ratio']
            drift = abs(rolling_df['equity_beta'].iloc[-1] - static_stock_ratio) if not rolling_df.empty else 0

            model_results['stage2']['rolling'] = {
                'data': rolling_df,
                'drift': drift,
                'has_drift': drift > 0.15,
            }
        except Exception as e:
            import traceback
            traceback.print_exc()
            model_results['stage2']['rolling'] = {'error': f'滚动Beta失败: {str(e)}'}

        # 7.3 收益分解(有行业模型结果的所有基金)
        if sector_results and isinstance(sector_results, dict) and 'error' not in sector_results:
            try:
                perf_decomp = performance_decomposition(
                    model_results=model_results,
                    sector_results=sector_results,
                    nav_df=nav_data,
                    bm_df=None,
                )
                model_results['stage2']['decomposition'] = perf_decomp
            except Exception as e:
                import traceback
                traceback.print_exc()
                model_results['stage2']['decomposition'] = {'error': f'收益分解失败: {str(e)}'}

    return {
        'basic_info': basic_info,
        'nav_data': nav_data,
        'holdings_data': holdings_data,
        'model_type': model_type,
        'model_results': model_results,
        'performance': performance,
    }
