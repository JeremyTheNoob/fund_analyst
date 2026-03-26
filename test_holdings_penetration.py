"""
测试持仓穿透功能
"""

import pandas as pd
from data import (
    load_stock_industry_mapping,
    fetch_stock_fundamentals,
)
from models.holdings_analysis import analyze_holdings_penetration
from ui.holdings_components import render_holdings_penetration_dashboard


def test_load_stock_industry_mapping():
    """测试股票→行业映射表加载"""
    print("\n=== 测试1: 加载股票→行业映射表 ===")

    mapping = load_stock_industry_mapping()

    print(f"✅ 映射表大小: {len(mapping)} 只股票")
    print(f"✅ 示例映射:")
    for i, (code, info) in enumerate(list(mapping.items())[:5]):
        print(f"  {code}: {info['industry_name']}")

    return mapping


def test_calculate_industry_weights():
    """测试行业权重计算"""
    print("\n=== 测试2: 行业权重计算 ===")

    # 模拟持仓数据
    mock_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '600519', '000858', '002594'],
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3],
    })

    print(f"模拟持仓:\n{mock_holdings}")

    # 加载映射表
    mapping = load_stock_industry_mapping()

    # 计算行业权重
    from data import calculate_industry_weights

    result = calculate_industry_weights(
        holdings_df=mock_holdings,
        stock_mapping=mapping,
        top_n=10
    )

    print(f"\n✅ 行业权重: {result['industry_weights']}")
    print(f"✅ 前十大行业: {result['top_industries']}")
    print(f"✅ 集中度: {result['concentration']}")
    print(f"✅ 解读: {result['note']}")


def test_calculate_concentration():
    """测试持仓集中度计算"""
    print("\n=== 测试3: 持仓集中度计算 ===")

    # 模拟持仓数据(前十大)
    mock_holdings = pd.DataFrame({
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3, 4.2, 3.8, 3.5, 3.2, 2.9],
    })

    print(f"模拟持仓:\n{mock_holdings}")

    from data import calculate_concentration

    result = calculate_concentration(mock_holdings)

    print(f"\n✅ HHI指数: {result['hhi']}")
    print(f"✅ 前三大占比: {result['top3_ratio']:.1f}%")
    print(f"✅ 前十大占比: {result['top10_ratio']:.1f}%")
    print(f"✅ 集中度评级: {result['concentration_level']}")
    print(f"✅ 解读: {result['note']}")


def test_fetch_stock_fundamentals():
    """测试个股基本面获取"""
    print("\n=== 测试4: 个股基本面获取 ===")

    # 测试几只典型股票
    test_codes = ['000001', '600000', '600519']  # 平安银行、浦发银行、贵州茅台

    fundamentals = fetch_stock_fundamentals(test_codes)

    print(f"\n✅ 获取到 {len(fundamentals)} 只股票的基本面数据")

    for code, info in fundamentals.items():
        print(f"\n{code}:")
        print(f"  名称: {info['name']}")
        print(f"  价格: ¥{info['price']}")
        print(f"  市值: {info['market_cap']}亿")
        print(f"  规模: {info['size_tag']}")
        print(f"  PE: {info['pe_ratio']}")


def test_holdings_penetration_full():
    """测试完整的持仓穿透分析"""
    print("\n=== 测试5: 完整持仓穿透分析 ===")

    # 模拟持仓数据
    mock_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '600519', '000858', '002594', '002415', '600036', '601318', '000333', '600276'],
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3, 4.2, 3.8, 3.5, 3.2, 2.9],
    })

    print(f"模拟持仓:\n{mock_holdings}")

    # 模拟FF结果
    mock_ff_results = {
        'alpha': 0.15,
        'alpha_pval': 0.02,
        'r_squared': 0.75,
        'factor_betas': {
            'Mkt': 0.95,
            'SMB': 0.15,  # 小盘暴露
            'HML': -0.05,
        },
        'factor_betas_raw': {
            'Mkt': 0.95,
            'SMB': 0.15,
            'HML': -0.05,
        },
        'interpretation': '✅ 显著正Alpha 15.0%/年,经理具备选股能力',
    }

    print(f"\n模拟FF结果: SMB={mock_ff_results['factor_betas']['SMB']} (小盘暴露)")

    # 运行完整分析
    result = analyze_holdings_penetration(
        holdings_df=mock_holdings,
        ff_results=mock_ff_results,
    )

    print(f"\n=== 持仓穿透分析结果 ===")

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
    print(f"  前十大占比: {conc['top10_ratio']:.1f}%")
    print(f"  评级: {conc['concentration_level']}")
    print(f"  解读: {conc['note']}")

    # 3. 个股基本面
    fund = result['fundamentals']
    print(f"\n【个股基本面】")
    print(f"  覆盖股票数: {len(fund)}")
    for code, info in list(fund.items())[:3]:
        print(f"  {code}: {info['name']} ({info['size_tag']}, {info['market_cap']}亿)")

    # 4. 风格对比
    style = result['style_comparison']
    print(f"\n【风格一致性】")
    print(f"  持仓风格: {style['holding_style']}")
    print(f"  FF模型风格: {style['ff_style']}")
    print(f"  是否一致: {style['is_consistent']}")
    print(f"  解读: {style['note']}")


def test_format_report():
    """测试报告格式化"""
    print("\n=== 测试6: 报告格式化 ===")

    # 使用测试5的结果
    mock_holdings = pd.DataFrame({
        '证券代码': ['000001', '600000', '600519', '000858', '002594', '002415', '600036', '601318', '000333', '600276'],
        '占净值比例': [10.5, 8.2, 7.8, 6.5, 5.3, 4.2, 3.8, 3.5, 3.2, 2.9],
    })

    mock_ff_results = {
        'factor_betas': {'SMB': 0.15},
    }

    result = analyze_holdings_penetration(mock_holdings, mock_ff_results)

    from models.holdings_analysis import format_holdings_penetration_report

    report = format_holdings_penetration_report(result)

    print(f"\n=== 持仓穿透分析报告 ===\n")
    print(report)


if __name__ == '__main__':
    print("🚀 开始测试持仓穿透功能\n")

    # 顺序执行测试
    test_load_stock_industry_mapping()
    test_calculate_industry_weights()
    test_calculate_concentration()
    test_fetch_stock_fundamentals()
    test_holdings_penetration_full()
    test_format_report()

    print("\n✅ 所有测试完成!")
