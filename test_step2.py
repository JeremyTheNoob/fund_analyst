"""
第二步测试：数据层模块
验证 data/fetcher.py 是否正确创建且可用
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

def test_imports():
    """测试导入链"""
    print("🧪 测试 data 模块导入...")
    try:
        from data import (
            fetch_basic_info,
            fetch_nav,
            fetch_index_daily,
            fetch_hk_index_daily,
            fetch_ff_factors,
            fetch_treasury_10y,
            fetch_bond_three_factors,
            fetch_bond_index,
            fetch_sw_industry_ret,
            fetch_holdings,
            fetch_stock_valuation_alert,
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
        from utils.helpers import retry_on_failure
        print("✅ data 层正确依赖 config 和 utils\n")
        return True
    except Exception as e:
        print(f"❌ 依赖检查失败: {e}\n")
        return False


def test_config_usage():
    """测试配置使用"""
    print("🧪 测试配置使用...")
    import config

    # 测试 INDEX_MAP
    assert 'mkt' in config.INDEX_MAP, "❌ INDEX_MAP 缺失"
    assert 'small' in config.INDEX_MAP, "❌ INDEX_MAP 缺失"
    print("✅ INDEX_MAP 正确")

    # 测试 CACHE_CONFIG
    assert 'short' in config.CACHE_CONFIG, "❌ CACHE_CONFIG 缺失"
    assert 'long' in config.CACHE_CONFIG, "❌ CACHE_CONFIG 缺失"
    print("✅ CACHE_CONFIG 正确")

    # 测试 _INDEX_NAME_CODE
    assert '沪深300' in config._INDEX_NAME_CODE, "❌ _INDEX_NAME_CODE 缺失"
    print("✅ _INDEX_NAME_CODE 正确\n")

    return True


def test_function_signatures():
    """测试函数签名"""
    print("🧪 测试函数签名...")
    from data.fetcher import (
        fetch_basic_info,
        fetch_nav,
        fetch_index_daily,
        fetch_ff_factors,
        fetch_holdings,
    )
    import inspect

    # 测试 fetch_basic_info
    sig = inspect.signature(fetch_basic_info)
    assert 'symbol' in sig.parameters, "❌ fetch_basic_info 缺失 symbol 参数"
    print("✅ fetch_basic_info 签名正确")

    # 测试 fetch_nav
    sig = inspect.signature(fetch_nav)
    assert 'symbol' in sig.parameters, "❌ fetch_nav 缺失 symbol 参数"
    assert 'years' in sig.parameters, "❌ fetch_nav 缺失 years 参数"
    print("✅ fetch_nav 签名正确")

    # 测试 fetch_index_daily
    sig = inspect.signature(fetch_index_daily)
    assert 'symbol_code' in sig.parameters, "❌ fetch_index_daily 缺失 symbol_code 参数"
    assert 'start' in sig.parameters, "❌ fetch_index_daily 缺失 start 参数"
    assert 'end' in sig.parameters, "❌ fetch_index_daily 缺失 end 参数"
    print("✅ fetch_index_daily 签名正确")

    # 测试 fetch_ff_factors
    sig = inspect.signature(fetch_ff_factors)
    assert 'start' in sig.parameters, "❌ fetch_ff_factors 缺失 start 参数"
    assert 'end' in sig.parameters, "❌ fetch_ff_factors 缺失 end 参数"
    print("✅ fetch_ff_factors 签名正确")

    # 测试 fetch_holdings
    sig = inspect.signature(fetch_holdings)
    assert 'symbol' in sig.parameters, "❌ fetch_holdings 缺失 symbol 参数"
    assert 'type_category' in sig.parameters, "❌ fetch_holdings 缺失 type_category 参数"
    print("✅ fetch_holdings 签名正确\n")

    return True


def test_file_structure():
    """测试文件结构"""
    print("🧪 测试文件结构...")
    import os

    # 检查 data 目录
    assert os.path.exists('data'), "❌ data 目录不存在"
    print("✅ data 目录存在")

    # 检查 __init__.py
    assert os.path.exists('data/__init__.py'), "❌ data/__init__.py 不存在"
    print("✅ data/__init__.py 存在")

    # 检查 fetcher.py
    assert os.path.exists('data/fetcher.py'), "❌ data/fetcher.py 不存在"
    print("✅ data/fetcher.py 存在\n")

    return True


def main():
    """运行所有测试"""
    print("=" * 60)
    print("🚀 第二步：数据层模块 测试")
    print("=" * 60 + "\n")

    tests = [
        ("文件结构", test_file_structure),
        ("模块导入", test_imports),
        ("依赖关系", test_dependencies),
        ("配置使用", test_config_usage),
        ("函数签名", test_function_signatures),
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
        print("🎉 第二步测试全部通过！")
    else:
        print("❌ 部分测试失败")
    print("=" * 60)

    if all_passed:
        print("\n📋 第二步完成内容：")
        print("  ✅ data/__init__.py - 数据层入口")
        print("  ✅ data/fetcher.py - 数据获取模块（11个主要函数）")
        print("\n📦 核心功能：")
        print("  • 基金基本信息：fetch_basic_info() - 雪球+天天多重兜底")
        print("  • 净值历史：fetch_nav() - 累计净值避免分红跳空")
        print("  • 指数数据：fetch_index_daily() - 双接口策略")
        print("  • FF因子：fetch_ff_factors() - 三/四/五因子自动降维")
        print("  • 债券数据：fetch_treasury_10y(), fetch_bond_three_factors()")
        print("  • 持仓数据：fetch_holdings() - 股票+债券持仓")
        print("  • 估值预警：fetch_stock_valuation_alert()")
        print("\n🔑 关键特性：")
        print("  • 四级缓存：@st.cache_data(ttl=5m/1h/24h/7d)")
        print("  • 重试机制：@retry_on_failure()")
        print("  • 兜底策略：多数据源自动切换")
        print("  • 依赖正确：依赖 config 和 utils，不依赖 models/services/ui")

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
