"""
Alpha v2.0 端到端测试
验证与完整流程的集成
"""

import sys
sys.path.append('/Users/liuweihua/WorkBuddy/基金穿透式分析')

import pandas as pd
import numpy as np
from models.equity_model import run_equity_analysis
from data import fetch_nav, fetch_basic_info

print("=" * 80)
print("🧪 Alpha v2.0 端到端测试")
print("=" * 80)

# 测试基金列表（混合型 + 权益型）
test_funds = [
    {'code': '000069', 'name': '汇添富成长焦点混合', 'type': 'mixed'},
    {'code': '110022', 'name': '易方达消费行业股票', 'type': 'equity'},
]

for fund in test_funds:
    print(f"\n{'=' * 80}")
    print(f"📊 测试基金: {fund['name']} ({fund['code']}) - {fund['type']}")
    print(f"{'=' * 80}")

    try:
        # 1. 获取净值数据
        nav_data = fetch_nav(fund['code'], years=3)
        if nav_data.empty:
            print(f"❌ 净值数据为空")
            continue

        print(f"✅ 净值数据: {len(nav_data)} 天 ({nav_data['date'].min()} ~ {nav_data['date'].max()})")

        # 2. 获取基本信息
        basic_info = fetch_basic_info(fund['code'])
        if not basic_info:
            print(f"❌ 基本信息为空")
            continue

        print(f"✅ 基金名称: {basic_info.get('name', fund['code'])}")
        print(f"✅ 基金类型: {basic_info.get('type_raw', 'unknown')}")
        print(f"✅ 成立日期: {basic_info.get('establish_date', 'unknown')}")

        # 3. 模拟持仓数据（简化测试）
        holdings_data = {
            'stock_ratio': 0.80 if fund['type'] == 'equity' else 0.60,
            'bond_ratio': 0.10 if fund['type'] == 'equity' else 0.30,
        }

        # 4. 运行权益分析（包含Alpha v2.0）
        print(f"\n🔄 运行权益分析...")
        results = run_equity_analysis(
            symbol=fund['code'],
            nav_data=nav_data,
            basic_info=basic_info,
            holdings_data=holdings_data,
            model_type=fund['type']
        )

        # 5. 验证Alpha v2.0结果
        if 'alpha_v2' not in results or not results['alpha_v2']:
            print(f"❌ Alpha v2.0结果缺失")
            continue

        alpha_v2 = results['alpha_v2']
        print(f"\n{'=' * 80}")
        print(f"✅ Alpha v2.0 测试结果")
        print(f"{'=' * 80}")

        # 5.1 三层次Alpha
        if 'hierarchical' in alpha_v2 and alpha_v2['hierarchical']:
            hierarchical = alpha_v2['hierarchical']

            if hierarchical['capm']:
                print(f"\n📊 CAPM单因子:")
                print(f"  Alpha: {hierarchical['capm']['alpha']*100:.2f}%")
                print(f"  p-value: {hierarchical['capm']['alpha_pval']:.4f}")
                print(f"  Beta: {hierarchical['capm']['beta']:.3f}")
                print(f"  R²: {hierarchical['capm']['r_squared']:.3f}")

            if hierarchical['ff']:
                print(f"\n📊 FF{hierarchical['ff']['model_name'][2:]}因子:")
                print(f"  Alpha: {hierarchical['ff']['alpha']*100:.2f}%")
                print(f"  p-value: {hierarchical['ff']['alpha_pval']:.4f}")
                print(f"  R²: {hierarchical['ff']['r_squared']:.3f}")
                if 'factor_betas' in hierarchical['ff']:
                    print(f"  因子Beta:")
                    for factor, beta in hierarchical['ff']['factor_betas'].items():
                        print(f"    {factor}: {beta:.3f}")

            if hierarchical['industry_neutral']:
                print(f"\n📊 行业中性化:")
                print(f"  Alpha: {hierarchical['industry_neutral']['alpha']*100:.2f}%")
                print(f"  p-value: {hierarchical['industry_neutral']['alpha_pval']:.4f}")
                print(f"  R²: {hierarchical['industry_neutral']['r_squared']:.3f}")

            # Alpha稳定性判断
            if hierarchical['capm'] and hierarchical['ff']:
                alpha_drop = abs(hierarchical['capm']['alpha'] - hierarchical['ff']['alpha'])
                if alpha_drop < 0.02:
                    print(f"\n✅ Alpha稳定（波动={alpha_drop*100:.2f}%），选股能力扎实")
                else:
                    print(f"\n⚠️ Alpha波动较大（波动={alpha_drop*100:.2f}%），风格暴露占比较高")

            print(f"\n📝 综合解读:\n{hierarchical['summary']}")
        else:
            print(f"❌ 三层次Alpha结果缺失")

        # 5.2 Treynor-Mazuy择时检测
        if 'timing' in alpha_v2 and alpha_v2['timing']:
            timing = alpha_v2['timing']

            print(f"\n{'=' * 80}")
            print(f"🕐 Treynor-Mazuy择时检测")
            print(f"{'=' * 80}")
            print(f"  选股Alpha: {timing['alpha']*100:.2f}%")
            print(f"  Beta: {timing['beta']:.3f}")
            print(f"  择时γ: {timing['gamma']:.4f}")
            print(f"  γ显著性: {timing['gamma_pval']:.4f}")
            print(f"  择时得分: {timing['timing_score']:.1f}/100")

            print(f"\n📝 择时解读:\n{timing['interpretation']}")

            # 择时能力判定
            if timing['gamma_pval'] < 0.05:
                if timing['gamma'] > 0:
                    print(f"\n✨ 显著择时能力（γ={timing['gamma']:.4f} > 0）")
                else:
                    print(f"\n❌ 显著反向择时（γ={timing['gamma']:.4f} < 0）")
            else:
                print(f"\n📊 择时能力不显著（p={timing['gamma_pval']:.3f}）")
        else:
            print(f"❌ 择时检测结果缺失")

        # 5.3 月度Alpha胜率
        if 'monthly_win_rate' in alpha_v2 and alpha_v2['monthly_win_rate']:
            win_rate = alpha_v2['monthly_win_rate']

            print(f"\n{'=' * 80}")
            print(f"📈 月度Alpha胜率分析")
            print(f"{'=' * 80}")
            print(f"  胜率: {win_rate['win_rate']*100:.1f}% ({win_rate['win_months']}/{win_rate['total_months']})")

            print(f"\n📝 胜率解读:\n{win_rate['interpretation']}")

            # 显示最近6个月Alpha
            print(f"\n最近6个月Alpha:")
            recent_alpha = win_rate['monthly_alpha_series'].tail(6)
            for date, alpha in recent_alpha.items():
                print(f"  {date.strftime('%Y-%m')}: {alpha*100:+.2f}%")
        else:
            print(f"❌ 月度胜率结果缺失")

        print(f"\n{'=' * 80}")
        print(f"✅ {fund['name']} 测试完成")
        print(f"{'=' * 80}")

    except Exception as e:
        print(f"\n❌ {fund['name']} 测试失败: {e}")
        import traceback
        traceback.print_exc()

print(f"\n{'=' * 80}")
print(f"✅ 所有端到端测试完成")
print(f"{'=' * 80}")
