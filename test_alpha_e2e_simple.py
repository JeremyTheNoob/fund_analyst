"""
Alpha v2.0 简化端到端测试（使用模拟数据）
"""

import sys
sys.path.append('/Users/liuweihua/WorkBuddy/基金穿透式分析')

import pandas as pd
import numpy as np
from models.equity_model import run_equity_analysis

print("=" * 80)
print("🧪 Alpha v2.0 简化端到端测试")
print("=" * 80)

# 模拟3年净值数据
np.random.seed(42)
dates = pd.date_range('2023-01-01', '2026-03-25', freq='D')

# 模拟净值（初始1.0，每日波动）
nav_values = 1.0
nav_list = []
for i in range(len(dates)):
    daily_ret = np.random.normal(0.0005, 0.015)
    nav_values *= (1 + daily_ret)
    nav_list.append(nav_values)

nav_data = pd.DataFrame({
    'date': dates,
    'nav': nav_list
})
nav_data['ret'] = nav_data['nav'].pct_change().fillna(0)

print(f"✅ 模拟净值数据: {len(nav_data)} 天")

# 模拟基本信息
basic_info = {
    'name': '测试基金',
    'type_raw': '混合型-灵活配置',
    'type_category': 'mixed',
    'establish_date': '2020-01-01',
    'scale': '50.00亿元',
    'company': '测试基金公司',
    'benchmark_text': '沪深300指数收益率×60%+中债综合指数收益率×40%',
    'benchmark_parsed': {
        'text': '沪深300指数收益率×60%+中债综合指数收益率×40%',
        'components': [
            {'code': 'sh000300', 'name': '沪深300', 'weight': 0.60},
            {'code': 'bond_composite', 'name': '中债综合', 'weight': 0.40}
        ]
    }
}

# 模拟持仓数据
holdings_data = {
    'stock_ratio': 0.60,
    'bond_ratio': 0.30,
}

print(f"✅ 模拟基本信息: {basic_info['name']}")
print(f"✅ 模拟持仓: 股票{holdings_data['stock_ratio']*100:.0f}% + 债券{holdings_data['bond_ratio']*100:.0f}%")

# 运行权益分析
print(f"\n🔄 运行权益分析...")
try:
    results = run_equity_analysis(
        symbol='TEST001',
        nav_data=nav_data,
        basic_info=basic_info,
        holdings_data=holdings_data,
        model_type='mixed'
    )

    # 验证结果
    print(f"\n{'=' * 80}")
    print(f"✅ 权益分析完成")
    print(f"{'=' * 80}")

    if 'alpha_v2' in results and results['alpha_v2']:
        alpha_v2 = results['alpha_v2']
        print(f"\n✅ Alpha v2.0结果存在")

        # 三层次Alpha
        if 'hierarchical' in alpha_v2 and alpha_v2['hierarchical']:
            hierarchical = alpha_v2['hierarchical']
            print(f"\n📊 三层次Alpha:")
            if hierarchical['capm']:
                print(f"  CAPM: Alpha={hierarchical['capm']['alpha']*100:.2f}%, p={hierarchical['capm']['alpha_pval']:.4f}")
            if hierarchical['ff']:
                print(f"  FF3: Alpha={hierarchical['ff']['alpha']*100:.2f}%, p={hierarchical['ff']['alpha_pval']:.4f}")

        # 择时检测
        if 'timing' in alpha_v2 and alpha_v2['timing']:
            timing = alpha_v2['timing']
            print(f"\n🕐 择时检测:")
            print(f"  γ={timing['gamma']:.4f}, p={timing['gamma_pval']:.4f}, 得分={timing['timing_score']:.1f}/100")

        # 月度胜率
        if 'monthly_win_rate' in alpha_v2 and alpha_v2['monthly_win_rate']:
            win_rate = alpha_v2['monthly_win_rate']
            print(f"\n📈 月度胜率:")
            print(f"  胜率={win_rate['win_rate']*100:.1f}% ({win_rate['win_months']}/{win_rate['total_months']})")

        print(f"\n{'=' * 80}")
        print(f"✅ 测试通过！")
        print(f"{'=' * 80}")
    else:
        print(f"\n❌ Alpha v2.0结果缺失")
        print(f"alpha_v2: {results.get('alpha_v2', 'not found')}")

except Exception as e:
    print(f"\n❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()
