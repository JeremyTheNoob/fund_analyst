"""
测试Alpha v2.0新功能
- 周频转换
- 三层次Alpha
- Treynor-Mazuy择时检测
- 月度胜率分析
"""

import sys
sys.path.append('/Users/liuweihua/WorkBuddy/基金穿透式分析')

import pandas as pd
import numpy as np
from models.alpha_analysis import (
    resample_to_weekly,
    resample_to_monthly,
    calculate_alpha_hierarchical,
    calculate_timing_ability,
    calculate_monthly_win_rate
)

# 模拟数据
np.random.seed(42)
dates = pd.date_range('2023-01-01', '2025-12-31', freq='D')

# 模拟基金收益率（基准 + Alpha + 噪音）
benchmark_ret = pd.Series(np.random.normal(0.0005, 0.015, len(dates)), index=dates)
fund_ret = benchmark_ret + pd.Series(np.random.normal(0.0002, 0.008, len(dates)), index=dates)

# 模拟FF因子
ff_factors = pd.DataFrame({
    'date': dates,
    'Mkt': np.random.normal(0.0003, 0.012, len(dates)),
    'SMB': np.random.normal(0.0001, 0.008, len(dates)),
    'HML': np.random.normal(0.0000, 0.007, len(dates))
})

print("=" * 80)
print("📊 测试1: 周频转换")
print("=" * 80)

weekly_fund = resample_to_weekly(fund_ret)
weekly_benchmark = resample_to_weekly(benchmark_ret)

print(f"日频数据量: {len(fund_ret)} 天")
print(f"周频数据量: {len(weekly_fund)} 周")
print(f"周频收益率统计: 均值={weekly_fund.mean()*100:.4f}%, 标准差={weekly_fund.std()*100:.4f}%")

print("\n" + "=" * 80)
print("🎯 测试2: 三层次Alpha计算（周频）")
print("=" * 80)

hierarchical_result = calculate_alpha_hierarchical(
    fund_ret, benchmark_ret,
    ff_factors=ff_factors,
    industry_returns=None,  # 暂不测试行业中性化
    frequency='weekly'
)

if hierarchical_result['capm']:
    print(f"✅ CAPM: Alpha={hierarchical_result['capm']['alpha']*100:.2f}%, p={hierarchical_result['capm']['alpha_pval']:.3f}")
else:
    print("❌ CAPM计算失败")

if hierarchical_result['ff']:
    print(f"✅ FF3: Alpha={hierarchical_result['ff']['alpha']*100:.2f}%, p={hierarchical_result['ff']['alpha_pval']:.3f}")
else:
    print("❌ FF3计算失败")

print(f"\n📝 综合解读:\n{hierarchical_result['summary']}")

print("\n" + "=" * 80)
print("🕐 测试3: Treynor-Mazuy择时检测（周频）")
print("=" * 80)

timing_result = calculate_timing_ability(
    fund_ret, benchmark_ret,
    frequency='weekly'
)

print(f"γ系数（择时能力）: {timing_result['gamma']:.4f}")
print(f"显著性p值: {timing_result['gamma_pval']:.4f}")
print(f"择时得分: {timing_result['timing_score']:.1f}/100")
print(f"\n📝 择时解读:\n{timing_result['interpretation']}")

print("\n" + "=" * 80)
print("📈 测试4: 月度Alpha胜率分析")
print("=" * 80)

win_rate_result = calculate_monthly_win_rate(
    fund_ret, benchmark_ret,
    months=36
)

print(f"胜率: {win_rate_result['win_rate']*100:.1f}% ({win_rate_result['win_months']}/{win_rate_result['total_months']})")
print(f"\n📝 胜率解读:\n{win_rate_result['interpretation']}")

# 显示月度Alpha序列
print(f"\n最近6个月Alpha:")
print(win_rate_result['monthly_alpha_series'].tail(6).apply(lambda x: f"{x*100:+.2f}%"))

print("\n" + "=" * 80)
print("✅ 所有测试完成！")
print("=" * 80)
