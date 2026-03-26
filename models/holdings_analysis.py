"""
持仓穿透分析模块
权益类基金的持仓穿透分析(行业权重/集中度/基本面/风格对比/持仓变动)
"""

import pandas as pd
import numpy as np
from typing import Dict

from data import (
    load_stock_industry_mapping,
    calculate_industry_weights,
    calculate_concentration,
    fetch_stock_fundamentals,
    compare_style_with_ff,
    analyze_holdings_change,
)


def analyze_holdings_penetration(
    holdings_df: pd.DataFrame,
    historical_holdings: Dict[str, pd.DataFrame],
    ff_results: Dict,
) -> Dict:
    """
    权益基金持仓穿透分析(完整版)

    Args:
        holdings_df: 持仓DataFrame(前十大重仓股)
        historical_holdings: 历史持仓字典 {date: DataFrame}
        ff_results: FF因子模型结果

    Returns:
        {
            'industry_weights': {...},  # 行业权重分布
            'concentration': {...},  # 持仓集中度
            'fundamentals': {...},  # 个股基本面
            'style_comparison': {...},  # 风格对比
            'holdings_change': {...},  # 持仓变动
        }
    """
    if holdings_df.empty:
        return {
            'industry_weights': {'note': '持仓数据为空'},
            'concentration': {'note': '持仓数据为空'},
            'fundamentals': {'note': '持仓数据为空'},
            'style_comparison': {'note': '持仓数据为空'},
            'holdings_change': {'note': '持仓数据为空'},
        }

    # 1. 加载股票→行业映射表
    stock_mapping = load_stock_industry_mapping()

    # 2. 计算行业权重
    industry_weights = calculate_industry_weights(
        holdings_df=holdings_df,
        stock_mapping=stock_mapping,
        top_n=10
    )

    # 3. 计算持仓集中度
    concentration = calculate_concentration(holdings_df)

    # 4. 获取个股基本面数据
    stock_codes = holdings_df['证券代码'].tolist() if '证券代码' in holdings_df.columns else []
    fundamentals = fetch_stock_fundamentals(stock_codes)

    # 5. 风格对比
    style_comparison = compare_style_with_ff(fundamentals, ff_results)

    # 6. 持仓变动追踪
    holdings_change = analyze_holdings_change(
        current_holdings=holdings_df,
        historical_holdings=historical_holdings,
    )

    return {
        'industry_weights': industry_weights,
        'concentration': concentration,
        'fundamentals': fundamentals,
        'style_comparison': style_comparison,
        'holdings_change': holdings_change,
    }


def format_holdings_penetration_report(penetration: Dict) -> str:
    """
    格式化持仓穿透分析报告(大白话解读)

    Args:
        penetration: 持仓穿透分析结果

    Returns:
        格式化的文本报告
    """
    parts = []

    # 1. 行业权重
    iw = penetration.get('industry_weights', {})
    if 'note' in iw and '数据不足' not in iw['note']:
        parts.append(f"**行业配置**: {iw['note']}")
        top3 = iw.get('top_industries', [])[:3]
        if top3:
            industry_list = ', '.join([f"{i['industry']}({i['weight']}%)" for i in top3])
            parts.append(f"前三大行业: {industry_list}")

    # 2. 持仓集中度
    conc = penetration.get('concentration', {})
    if 'note' in conc and '数据不足' not in conc['note']:
        parts.append(f"**持仓集中度**: {conc['note']}")
        parts.append(f"前三大重仓股占比: {conc['top3_ratio']:.1f}%")

    # 3. 持仓变动
    hc = penetration.get('holdings_change', {})
    if 'note' in hc and '数据不足' not in hc['note']:
        parts.append(f"**持仓变动**: {hc['note']}")

    # 4. 风格一致性
    style = penetration.get('style_comparison', {})
    if 'note' in style and '数据不足' not in style['note']:
        parts.append(f"**风格一致性**: {style['note']}")

    return '\n\n'.join(parts) if parts else '持仓穿透数据不足'
