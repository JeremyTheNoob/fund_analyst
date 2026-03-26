"""
数据获取模块
从外部数据源(AkShare等)获取原始数据
"""

from .fetcher import (
    # 基金数据
    fetch_basic_info,
    fetch_nav,
    fetch_holdings,
    fetch_stock_valuation_alert,

    # 指数数据
    fetch_index_daily,
    fetch_hk_index_daily,
    fetch_bond_index,
    fetch_sw_industry_ret,

    # FF因子
    fetch_ff_factors,

    # 债券数据
    fetch_treasury_10y,
    fetch_bond_three_factors,

    # 基准构建
    build_benchmark_ret,
)

from .stock_data import (
    # 股票→行业映射
    build_stock_industry_mapping,
    load_stock_industry_mapping,

    # 行业权重计算
    calculate_industry_weights,

    # 持仓集中度
    calculate_concentration,

    # 个股基本面
    fetch_stock_fundamentals,

    # 风格对比
    compare_style_with_ff,

    # 持仓变动追踪
    analyze_holdings_change,
)

__all__ = [
    # fetcher.py
    'fetch_basic_info',
    'fetch_nav',
    'fetch_holdings',
    'fetch_stock_valuation_alert',
    'fetch_index_daily',
    'fetch_hk_index_daily',
    'fetch_bond_index',
    'fetch_sw_industry_ret',
    'fetch_ff_factors',
    'fetch_treasury_10y',
    'fetch_bond_three_factors',
    'build_benchmark_ret',

    # stock_data.py
    'build_stock_industry_mapping',
    'load_stock_industry_mapping',
    'calculate_industry_weights',
    'calculate_concentration',
    'fetch_stock_fundamentals',
    'compare_style_with_ff',
]
