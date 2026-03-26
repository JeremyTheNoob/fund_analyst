"""
测试持仓穿透功能完整版(包含P0-P3所有功能)
"""

import pandas as pd
from data import (
    load_stock_industry_mapping,
    fetch_stock_fundamentals,
    analyze_holdings_change,
)
from models.holdings_analysis import analyze_holdings_penetration


def test_enhanced_concentration():
    """测试增强版持仓集中度分析"""
    print("\n=== 测试1: 增强版持仓集中度分析 ===")

    # 模拟持仓数据
    mock_holdings = pd.DataFrame({
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3, 4.2, 3.8, 3.5, 3.2, 2.9],
    })

    from data import calculate_concentration

    result = calculate_concentration(mock_holdings)

    print(f"✅ HHI指数: {result['hhi']}")
    print(f"✅ 前三大占比: {result['top3_ratio']:.1f}%")
    print(f"✅ 前五大占比: {result['top5_ratio']:.1f}%")
    print(f"✅ 前十大占比: {result['top10_ratio']:.1f}%")
    print(f"✅ 分散度评分: {result['dispersion_score']:.1f}")
    print(f"✅ 集中度评级: {result['concentration_level']}")


def test_enhanced_fundamentals():
    """测试增强版个股基本面获取"""
    print("\n=== 测试2: 增强版个股基本面获取 ===")

    # 测试几只典型股票
    test_codes = ['000001', '600000', '600519']

    fundamentals = fetch_stock_fundamentals(test_codes)

    print(f"\n✅ 获取到 {len(fundamentals)} 只股票的基本面数据")

    for code, info in fundamentals.items():
        print(f"\n{code}:")
        print(f"  名称: {info['name']}")
        print(f"  价格: ¥{info['price']}")
        print(f"  市值: {info['market_cap']}亿")
        print(f"  规模: {info['size_tag']}")
        print(f"  风格: {info['style_tag']}")
        print(f"  PE: {info['pe_ratio']}")
        print(f"  PB: {info['pb_ratio']}")
        print(f"  ROE: {info['roe']}%")


def test_enhanced_style_comparison():
    """测试增强版风格对比"""
    print("\n=== 测试3: 增强版风格对比 ===")

    # 模拟基本面数据(包含投资风格)
    mock_fundamentals = {
        '000001': {'name': '平安银行', 'market_cap': 2100.5, 'size_tag': '大盘', 'style_tag': '价值'},
        '600000': {'name': '浦发银行', 'market_cap': 3360.0, 'size_tag': '大盘', 'style_tag': '价值'},
        '600519': {'name': '贵州茅台', 'market_cap': 17600.0, 'size_tag': '大盘', 'style_tag': '价值'},
    }

    # 模拟FF结果(包含HML因子)
    mock_ff_results = {
        'factor_betas': {
            'Mkt': 0.95,
            'SMB': 0.15,  # 小盘暴露
            'HML': -0.08,  # 成长暴露
        },
    }

    from data import compare_style_with_ff

    result = compare_style_with_ff(mock_fundamentals, mock_ff_results)

    print(f"\n持仓风格:")
    print(f"  市值: {result['holding_style']['size']}")
    print(f"  投资: {result['holding_style']['style']}")

    print(f"\nFF模型风格:")
    print(f"  市值: {result['ff_style']['size']}")
    print(f"  投资: {result['ff_style']['style']}")

    print(f"\n一致性:")
    print(f"  市值: {result['is_size_consistent']}")
    print(f"  投资: {result['is_style_consistent']}")

    print(f"\n解读: {result['note']}")


def test_holdings_change_tracking():
    """测试持仓变动追踪"""
    print("\n=== 测试4: 持仓变动追踪 ===")

    # 模拟最新持仓
    current_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '600519', '000858', '002594'],
        '证券名称': ['平安银行', '浦发银行', '贵州茅台', '五粮液', '比亚迪'],
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3],
    })

    # 模拟上期持仓
    previous_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '000858', '600036', '601318'],
        '证券名称': ['平安银行', '浦发银行', '五粮液', '招商银行', '中国平安'],
        '占净值比例': [8.5, 9.2, 7.5, 6.0, 5.0],
    })

    historical_holdings = {'2023': previous_holdings}

    result = analyze_holdings_change(current_holdings, historical_holdings)

    print(f"\n换手率: {result['turnover_rate']:.1f}%")
    print(f"持仓稳定性: {result['stability_score']:.1f}")
    print(f"解读: {result['note']}")

    print(f"\n新进股票({len(result['new_stocks'])}只):")
    for s in result['new_stocks']:
        print(f"  {s['name']}: {s['current_ratio']}%")

    print(f"\n退出股票({len(result['exited_stocks'])}只):")
    for s in result['exited_stocks']:
        print(f"  {s['name']}: {s['previous_ratio']}%")

    print(f"\n加仓股票({len(result['increased_stocks'])}只):")
    for s in result['increased_stocks']:
        print(f"  {s['name']}: {s['previous_ratio']}% → {s['current_ratio']}% (+{s['change']}%)")

    print(f"\n减仓股票({len(result['decreased_stocks'])}只):")
    for s in result['decreased_stocks']:
        print(f"  {s['name']}: {s['previous_ratio']}% → {s['current_ratio']}% ({s['change']}%)")


def test_full_holdings_penetration_v2():
    """测试完整持仓穿透分析(v2版本)"""
    print("\n=== 测试5: 完整持仓穿透分析(v2版本) ===")

    # 模拟最新持仓
    current_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '600519', '000858', '002594', '002415', '600036', '601318', '000333', '600276'],
        '证券名称': ['平安银行', '浦发银行', '贵州茅台', '五粮液', '比亚迪', '海康威视', '招商银行', '中国平安', '美的集团', '恒瑞医药'],
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3, 4.2, 3.8, 3.5, 3.2, 2.9],
    })

    # 模拟上期持仓
    previous_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '000858', '600036', '601318', '000333', '600276', '002475', '000651', '601328'],
        '证券名称': ['平安银行', '浦发银行', '五粮液', '招商银行', '中国平安', '美的集团', '恒瑞医药', '立讯精密', '格力电器', '交通银行'],
        '占净值比例': [8.5, 9.2, 7.5, 6.0, 5.0, 4.5, 4.0, 3.5, 3.0, 2.5],
    })

    historical_holdings = {'2023': previous_holdings}

    # 模拟FF结果
    mock_ff_results = {
        'factor_betas': {
            'Mkt': 0.95,
            'SMB': 0.15,  # 小盘暴露
            'HML': -0.08,  # 成长暴露
        },
        'factor_betas_raw': {
            'Mkt': 0.95,
            'SMB': 0.15,
            'HML': -0.08,
        },
    }

    # 运行完整分析
    result = analyze_holdings_penetration(
        holdings_df=current_holdings,
        historical_holdings=historical_holdings,
        ff_results=mock_ff_results,
    )

    print(f"\n=== 持仓穿透分析结果(v2版本) ===")

    # 1. 行业权重
    iw = result['industry_weights']
    print(f"\n【行业配置】")
    print(f"  集中度: {iw['concentration']}")
    print(f"  解读: {iw['note']}")
    print(f"  前十大行业:")
    for ind in iw['top_industries'][:5]:
        print(f"    {ind['industry']}: {ind['weight']}%")

    # 2. 持仓集中度
    conc = result['concentration']
    print(f"\n【持仓集中度】")
    print(f"  HHI指数: {conc['hhi']}")
    print(f"  前三大占比: {conc['top3_ratio']:.1f}%")
    print(f"  前五大占比: {conc['top5_ratio']:.1f}%")
    print(f"  前十大占比: {conc['top10_ratio']:.1f}%")
    print(f"  分散度评分: {conc['dispersion_score']:.1f}")
    print(f"  评级: {conc['concentration_level']}")

    # 3. 个股基本面
    fund = result['fundamentals']
    print(f"\n【个股基本面】")
    print(f"  覆盖股票数: {len(fund)}")
    for code, info in list(fund.items())[:3]:
        print(f"  {code}: {info['name']} ({info['size_tag']}{info['style_tag']}, {info['market_cap']}亿)")

    # 4. 风格对比
    style = result['style_comparison']
    print(f"\n【风格一致性】")
    print(f"  持仓: {style['holding_style']['size']}{style['holding_style']['style']}")
    print(f"  FF: {style['ff_style']['size']}{style['ff_style']['style']}")
    print(f"  市值一致性: {style['is_size_consistent']}")
    print(f"  投资风格一致性: {style['is_style_consistent']}")
    print(f"  解读: {style['note']}")

    # 5. 持仓变动
    hc = result['holdings_change']
    print(f"\n【持仓变动】")
    print(f"  换手率: {hc['turnover_rate']:.1f}%")
    print(f"  持仓稳定性: {hc['stability_score']:.1f}")
    print(f"  新进: {len(hc['new_stocks'])}只")
    print(f"  退出: {len(hc['exited_stocks'])}只")
    print(f"  加仓: {len(hc['increased_stocks'])}只")
    print(f"  减仓: {len(hc['decreased_stocks'])}只")
    print(f"  解读: {hc['note']}")


if __name__ == '__main__':
    print("🚀 开始测试持仓穿透功能完整版(v2)\n")

    # 顺序执行测试
    test_enhanced_concentration()
    test_enhanced_fundamentals()
    test_enhanced_style_comparison()
    test_holdings_change_tracking()
    test_full_holdings_penetration_v2()

    print("\n✅ 所有测试完成!")
