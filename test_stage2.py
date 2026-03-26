"""
端到端测试第二阶段功能
测试:
  1. 申万行业检测
  2. 行业模型
  3. 滚动Beta监控
  4. 收益分解
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.gateway import analyze_fund


def test_stage2_functions():
    """测试第二阶段的四个功能"""

    # 测试用例1:医药行业基金(000001) - 行业型
    print("=" * 60)
    print("测试用例1: 行业型基金(000001) - 华夏成长")
    print("-" * 60)
    result = analyze_fund('000001', years=1)

    print(f"✅ 模型类型: {result['model_type']}")

    # 检查第二阶段结果
    stage2 = result.get('model_results', {}).get('stage2', {})

    if 'sector' in stage2:
        print(f"✅ 行业模型结果: {stage2['sector'].get('sw_name', 'N/A')}")
        print(f"   - 基准来源: {stage2['sector'].get('bm_source', 'N/A')}")
        print(f"   - 中性化Alpha: {stage2['sector'].get('neutral_alpha', 0)*100:.2f}%")
    else:
        print("⚠️ 行业模型未触发")

    if 'rolling' in stage2:
        print(f"✅ 滚动Beta结果: 风格漂移={stage2['rolling'].get('has_drift', False)}")
    else:
        print("   - 滚动Beta未触发(仅混合型基金)")

    if 'decomposition' in stage2:
        print(f"✅ 收益分解结果: 总超额={stage2['decomposition'].get('total_excess', 0)*100:.2f}%")
        print(f"   - 叙事: {stage2['decomposition'].get('narrative', 'N/A')}")
    else:
        print("⚠️ 收益分解未触发")

    print()

    # 测试用例2:混合型基金(110022) - 易方达消费行业
    print("=" * 60)
    print("测试用例2: 混合型基金(110022) - 易方达消费行业")
    print("-" * 60)
    result = analyze_fund('110022', years=1)

    print(f"✅ 模型类型: {result['model_type']}")

    stage2 = result.get('model_results', {}).get('stage2', {})

    if 'rolling' in stage2:
        print(f"✅ 滚动Beta结果: 风格漂移={stage2['rolling'].get('has_drift', False)}")
        print(f"   - 漂移幅度: {stage2['rolling'].get('drift', 0)*100:.2f}%")
    else:
        print("⚠️ 滚动Beta未触发")

    if 'decomposition' in stage2:
        print(f"✅ 收益分解结果: 总超额={stage2['decomposition'].get('total_excess', 0)*100:.2f}%")
        print(f"   - 数据质量: {stage2['decomposition'].get('data_quality', 'N/A')}")
    else:
        print("⚠️ 收益分解未触发")

    print()

    # 测试用例3:纯债基金(000069) - 债券型(不触发第二阶段)
    print("=" * 60)
    print("测试用例3: 纯债基金(000069) - 债券型(不触发第二阶段)")
    print("-" * 60)
    result = analyze_fund('000069', years=1)

    print(f"✅ 模型类型: {result['model_type']}")

    stage2 = result.get('model_results', {}).get('stage2', {})

    if stage2:
        print(f"⚠️ 意外:债券型基金触发了第二阶段功能")
    else:
        print("✅ 债券型基金正确未触发第二阶段功能")

    print()

    print("=" * 60)
    print("第二阶段功能测试完成!")
    print("=" * 60)


if __name__ == '__main__':
    test_stage2_functions()
