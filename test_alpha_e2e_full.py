"""
Alpha v2.0 完整端到端测试（包含模拟FF因子）
"""

import sys
sys.path.append('/Users/liuweihua/WorkBuddy/基金穿透式分析')

import pandas as pd
import numpy as np

print("=" * 80)
print("🧪 Alpha v2.0 完整端到端测试")
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

# 模拟FF因子数据
ff_factors = pd.DataFrame({
    'date': dates,
    'Mkt': np.random.normal(0.0003, 0.012, len(dates)),
    'SMB': np.random.normal(0.0001, 0.008, len(dates)),
    'HML': np.random.normal(0.0000, 0.007, len(dates))
})

print(f"✅ 模拟FF因子数据: {len(ff_factors)} 天")

# 现在导入并测试
from models.alpha_analysis import (
    calculate_alpha_hierarchical,
    calculate_timing_ability,
    calculate_monthly_win_rate,
)

ret_series = nav_data.set_index('date')['ret']

# 测试1: 三层次Alpha
print(f"\n{'=' * 80}")
print(f"📊 测试1: 三层次Alpha计算（周频）")
print(f"{'=' * 80}")

# 模拟基准收益率
benchmark_ret = pd.Series(np.random.normal(0.0004, 0.013, len(dates)), index=dates)

hierarchical = calculate_alpha_hierarchical(
    fund_ret=ret_series,
    benchmark_ret=benchmark_ret,
    ff_factors=ff_factors,
    frequency='weekly'
)

if hierarchical['capm']:
    print(f"✅ CAPM: Alpha={hierarchical['capm']['alpha']*100:.2f}%, p={hierarchical['capm']['alpha_pval']:.4f}")
if hierarchical['ff']:
    print(f"✅ FF3: Alpha={hierarchical['ff']['alpha']*100:.2f}%, p={hierarchical['ff']['alpha_pval']:.4f}")
    print(f"   因子Beta: {hierarchical['ff']['factor_betas']}")

print(f"\n📝 综合解读:\n{hierarchical['summary']}")

# 测试2: 择时检测
print(f"\n{'=' * 80}")
print(f"🕐 测试2: Treynor-Mazuy择时检测")
print(f"{'=' * 80}")

timing = calculate_timing_ability(
    fund_ret=ret_series,
    benchmark_ret=benchmark_ret,
    frequency='weekly'
)

print(f"✅ 选股Alpha: {timing['alpha']*100:.2f}%")
print(f"✅ Beta: {timing['beta']:.3f}")
print(f"✅ 择时γ: {timing['gamma']:.4f}, p={timing['gamma_pval']:.4f}")
print(f"✅ 择时得分: {timing['timing_score']:.1f}/100")
print(f"\n📝 择时解读:\n{timing['interpretation']}")

# 测试3: 月度胜率
print(f"\n{'=' * 80}")
print(f"📈 测试3: 月度Alpha胜率分析")
print(f"{'=' * 80}")

win_rate = calculate_monthly_win_rate(
    fund_ret=ret_series,
    benchmark_ret=benchmark_ret,
    months=36
)

print(f"✅ 胜率: {win_rate['win_rate']*100:.1f}% ({win_rate['win_months']}/{win_rate['total_months']})")
print(f"\n📝 胜率解读:\n{win_rate['interpretation']}")

print(f"\n最近6个月Alpha:")
recent = win_rate['monthly_alpha_series'].tail(6)
for date, alpha in recent.items():
    print(f"  {date.strftime('%Y-%m')}: {alpha*100:+.2f}%")

print(f"\n{'=' * 80}")
print(f"✅ 所有测试通过！")
print(f"{'=' * 80}")
