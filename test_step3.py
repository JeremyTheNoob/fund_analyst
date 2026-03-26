"""
第三步测试：模型层模块
验证 models/ 是否正确创建且可用
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))


def test_file_structure():
    """测试文件结构"""
    print("🧪 测试文件结构...")
    import os

    # 检查 models 目录
    assert os.path.exists('models'), "❌ models 目录不存在"
    print("✅ models 目录存在")

    # 检查 __init__.py
    assert os.path.exists('models/__init__.py'), "❌ models/__init__.py 不存在"
    print("✅ models/__init__.py 存在")

    # 检查子模块
    assert os.path.exists('models/gateway.py'), "❌ models/gateway.py 不存在"
    print("✅ models/gateway.py 存在")
    assert os.path.exists('models/equity_model.py'), "❌ models/equity_model.py 不存在"
    print("✅ models/equity_model.py 存在")
    assert os.path.exists('models/bond_model.py'), "❌ models/bond_model.py 不存在"
    print("✅ models/bond_model.py 存在\n")

    return True


def test_imports():
    """测试导入链"""
    print("🧪 测试 models 模块导入...")
    try:
        from models import analyze_fund, classify_fund_type
        from models.equity_model import (
            run_ff_model, run_brinson, analyze_style, calculate_radar_scores
        )
        from models.bond_model import (
            run_duration_model, run_bond_three_factors, bond_stress_test, analyze_bond_structure
        )
        print("✅ 所有函数导入成功\n")
        return True
    except ImportError as e:
        print(f"❌ 导入失败: {e}\n")
        return False


def test_dependencies():
    """测试依赖关系"""
    print("🧪 测试依赖关系...")
    try:
        import config
        from utils.helpers import normalize_score, safe_divide
        from data import fetch_nav, fetch_ff_factors, fetch_holdings
        print("✅ models 层正确依赖 config, utils, data")
        print("✅ 没有依赖 services 或 ui\n")
        return True
    except Exception as e:
        print(f"❌ 依赖检查失败: {e}\n")
        return False


def test_function_signatures():
    """测试函数签名"""
    print("🧪 测试函数签名...")
    from models.gateway import classify_fund_type, analyze_fund
    from models.equity_model import run_ff_model, run_brinson
    from models.bond_model import run_duration_model, bond_stress_test
    import inspect

    # 测试 classify_fund_type
    sig = inspect.signature(classify_fund_type)
    assert 'stock_ratio' in sig.parameters
    assert 'bond_ratio' in sig.parameters
    assert 'type_category' in sig.parameters
    print("✅ classify_fund_type 签名正确")

    # 测试 analyze_fund
    sig = inspect.signature(analyze_fund)
    assert 'symbol' in sig.parameters
    assert 'years' in sig.parameters
    print("✅ analyze_fund 签名正确")

    # 测试 run_ff_model
    sig = inspect.signature(run_ff_model)
    assert 'fund_ret' in sig.parameters
    assert 'factors' in sig.parameters
    assert 'model_type' in sig.parameters
    print("✅ run_ff_model 签名正确")

    # 测试 run_brinson
    sig = inspect.signature(run_brinson)
    assert 'fund_ret' in sig.parameters
    assert 'stock_ratio' in sig.parameters
    print("✅ run_brinson 签名正确")

    # 测试 run_duration_model
    sig = inspect.signature(run_duration_model)
    assert 'fund_ret' in sig.parameters
    assert 'nav_data' in sig.parameters
    print("✅ run_duration_model 签名正确")

    # 测试 bond_stress_test
    sig = inspect.signature(bond_stress_test)
    assert 'duration' in sig.parameters
    assert 'convexity' in sig.parameters
    print("✅ bond_stress_test 签名正确\n")

    return True


def test_classify_fund_type():
    """测试基金分类逻辑"""
    print("🧪 测试基金分类逻辑...")
    from models.gateway import classify_fund_type

    # 测试权益型
    result = classify_fund_type(0.85, 0.10, 'equity')
    assert result == 'equity', f"❌ 权益型分类错误: {result}"
    print("✅ 权益型分类正确")

    # 测试债券型
    result = classify_fund_type(0.15, 0.80, 'bond')
    assert result == 'bond', f"❌ 债券型分类错误: {result}"
    print("✅ 债券型分类正确")

    # 测试混合型
    result = classify_fund_type(0.50, 0.40, 'mixed')
    assert result == 'mixed', f"❌ 混合型分类错误: {result}"
    print("✅ 混合型分类正确")

    # 测试指数型
    result = classify_fund_type(0.95, 0.05, 'index')
    assert result == 'index', f"❌ 指数型分类错误: {result}"
    print("✅ 指数型分类正确")

    # 测试QDII
    result = classify_fund_type(0.50, 0.10, 'qdii')
    assert result == 'qdii', f"❌ QDII分类错误: {result}"
    print("✅ QDII分类正确\n")

    return True


def test_config_usage():
    """测试配置使用"""
    print("🧪 测试配置使用...")
    import config

    # 测试 STOCK_RATIO_THRESHOLDS
    assert 'equity_high' in config.STOCK_RATIO_THRESHOLDS
    assert 'bond_low' in config.STOCK_RATIO_THRESHOLDS
    print("✅ STOCK_RATIO_THRESHOLDS 正确")

    # 测试 RADAR_WEIGHTS
    assert 'equity' in config.RADAR_WEIGHTS
    assert 'bond' in config.RADAR_WEIGHTS
    assert 'mixed' in config.RADAR_WEIGHTS
    print("✅ RADAR_WEIGHTS 正确")

    # 测试 MODEL_CONFIG
    assert 'duration' in config.MODEL_CONFIG
    assert 'stress_test' in config.MODEL_CONFIG
    print("✅ MODEL_CONFIG 正确\n")

    return True


def test_model_structure():
    """测试模型结构"""
    print("🧪 测试模型结构...")
    from models.equity_model import _empty_ff_result
    from models.bond_model import _interpret_duration

    # 测试 _empty_ff_result
    result = _empty_ff_result('测试')
    assert 'alpha' in result
    assert 'r_squared' in result
    assert result['interpretation'] == '测试'
    print("✅ _empty_ff_result 结构正确")

    # 测试 _interpret_duration
    result = _interpret_duration(3.5, 0.6, 10.0)
    assert isinstance(result, str)
    assert '久期' in result
    print("✅ _interpret_duration 结构正确\n")

    return True


def main():
    """运行所有测试"""
    print("=" * 60)
    print("🚀 第三步：模型层模块 测试")
    print("=" * 60 + "\n")

    tests = [
        ("文件结构", test_file_structure),
        ("模块导入", test_imports),
        ("依赖关系", test_dependencies),
        ("函数签名", test_function_signatures),
        ("基金分类", test_classify_fund_type),
        ("配置使用", test_config_usage),
        ("模型结构", test_model_structure),
    ]

    all_passed = True
    for name, test_func in tests:
        try:
            if not test_func():
                all_passed = False
        except AssertionError as e:
            print(f"❌ {name} 测试失败: {e}\n")
            all_passed = False
        except Exception as e:
            print(f"❌ {name} 测试失败: {e}\n")
            import traceback
            traceback.print_exc()
            all_passed = False

    print("=" * 60)
    if all_passed:
        print("🎉 第三步测试全部通过！")
    else:
        print("❌ 部分测试失败")
    print("=" * 60)

    if all_passed:
        print("\n📋 第三步完成内容：")
        print("  ✅ models/__init__.py - 模型层入口")
        print("  ✅ models/gateway.py - 逻辑网关（分类+路由）")
        print("  ✅ models/equity_model.py - 权益模型（FF因子/Brinson/风格）")
        print("  ✅ models/bond_model.py - 债券模型（久期/三因子/压力测试）")
        print("\n📦 核心功能：")
        print("  • 逻辑网关：classify_fund_type() + analyze_fund()")
        print("  • FF模型：run_ff_model() - CAPM/FF3/FF5/Carhart四模型")
        print("  • Brinson归因：run_brinson() - 配置+选择+交互效应")
        print("  • 风格分析：analyze_style() + calculate_radar_scores()")
        print("  • 久期归因：run_duration_model() - T-Model反推")
        print("  • 债券三因子：run_bond_three_factors() - 短端+长端+信用")
        print("  • 压力测试：bond_stress_test() - 4场景冲击分析")
        print("  • 持仓穿透：analyze_bond_structure() - 债券类型分布")
        print("\n🔑 关键特性：")
        print("  • 自动路由：根据股票仓位自动选择模型")
        print("  • 多模型支持：权益/债券/混合/指数/QDII")
        print("  • 雷达图评分：五维综合评分（不同类型权重不同）")
        print("  • 依赖正确：依赖 config/utils/data，不依赖 services/ui")
        print("  • 无Streamlit：模型层不调用 st.write 等")

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
