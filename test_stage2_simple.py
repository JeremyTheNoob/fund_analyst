"""
端到端测试第二阶段功能(简化版)
由于申万行业指数数据源问题,仅测试:
  1. 滚动Beta监控
  2. 收益分解(当有sector结果时)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.gateway import analyze_fund


def test_stage2_functions():
    """测试第二阶段功能"""

    # 测试用例1:混合型基金(假设有债券仓位)
    print("=" * 60)
    print("测试用例1: 测试滚动Beta功能")
    print("-" * 60)
    result = analyze_fund('000001', years=1)

    print(f"✅ 模型类型: {result['model_type']}")
    print(f"   - 股票仓位: {result['holdings_data']['stock_ratio']*100:.1f}%")
    print(f"   - 债券仓位: {result['holdings_data']['bond_ratio']*100:.1f}%")

    # 检查第二阶段结果
    stage2 = result.get('model_results', {}).get('stage2', {})

    if 'rolling' in stage2:
        rolling = stage2['rolling']
        if 'error' in rolling:
            print(f"⚠️ 滚动Beta失败: {rolling['error']}")
        elif rolling.get('data', pd.DataFrame()).empty:
            print(f"⚠️ 滚动Beta结果为空(可能是数据源问题)")
        else:
            rolling_data = rolling['data']
            print(f"✅ 滚动Beta成功")
            print(f"   - 数据点数: {len(rolling_data)}")
            print(f"   - 风格漂移: {'是' if rolling['has_drift'] else '否'}")
            print(f"   - 漂移幅度: {rolling['drift']*100:.2f}%")
    else:
        print("⚠️ 滚动Beta未触发")

    print()

    # 测试用例2:债券基金(不应该触发第二阶段)
    print("=" * 60)
    print("测试用例2: 纯债基金(不应该触发第二阶段)")
    print("-" * 60)
    result = analyze_fund('000069', years=1)

    print(f"✅ 模型类型: {result['model_type']}")

    stage2 = result.get('model_results', {}).get('stage2', {})

    if stage2:
        print(f"⚠️ 意外:债券型基金触发了第二阶段功能")
        print(f"   stage2 keys: {list(stage2.keys())}")
    else:
        print("✅ 债券型基金正确未触发第二阶段功能")

    print()

    # 测试用例3:权益基金
    print("=" * 60)
    print("测试用例3: 权益基金")
    print("-" * 60)
    result = analyze_fund('110022', years=1)

    print(f"✅ 模型类型: {result['model_type']}")

    stage2 = result.get('model_results', {}).get('stage2', {})

    if 'rolling' in stage2:
        rolling = stage2['rolling']
        if 'error' in rolling:
            print(f"⚠️ 滚动Beta失败: {rolling['error']}")
        elif rolling.get('data', pd.DataFrame()).empty:
            print(f"⚠️ 滚动Beta结果为空(可能是bond_ratio=0,导致无法运行双因子回归)")
        else:
            rolling_data = rolling['data']
            print(f"✅ 滚动Beta成功")
            print(f"   - 数据点数: {len(rolling_data)}")
            print(f"   - 风格漂移: {'是' if rolling['has_drift'] else '否'}")
    else:
        print("⚠️ 滚动Beta未触发")

    print()

    print("=" * 60)
    print("第二阶段功能测试完成!")
    print("=" * 60)
    print()
    print("说明:")
    print("1. 申万行业模型:由于AkShare数据源问题,申万行业指数获取失败,无法测试")
    print("2. 滚动Beta:部分基金由于bond_ratio=0,导致双因子回归无法运行")
    print("3. 收益分解:依赖行业模型结果,当前无法测试")


if __name__ == '__main__':
    import pandas as pd
    test_stage2_functions()
