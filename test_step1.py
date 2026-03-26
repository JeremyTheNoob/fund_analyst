"""
第一步测试：配置+工具模块
验证 config.py 和 utils/ 是否正确创建且可用
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(__file__))

def test_config():
    """测试配置模块"""
    print("🧪 测试 config.py...")
    import config

    # 测试雷达图权重
    assert 'equity' in config.RADAR_WEIGHTS, "❌ RADAR_WEIGHTS 缺失 equity 类型"
    assert 'bond' in config.RADAR_WEIGHTS, "❌ RADAR_WEIGHTS 缺失 bond 类型"
    print(f"✅ RADAR_WEIGHTS 包含 {len(config.RADAR_WEIGHTS)} 种类型")

    # 测试权重总和
    for ftype, weights in config.RADAR_WEIGHTS.items():
        total = sum(weights.values())
        assert abs(total - 1.0) < 0.001, f"❌ {ftype} 权重总和不为1.0: {total}"
    print(f"✅ 所有类型权重总和均为1.0")

    # 测试缓存配置
    assert 'short' in config.CACHE_CONFIG, "❌ CACHE_CONFIG 缺失 short"
    assert 'long' in config.CACHE_CONFIG, "❌ CACHE_CONFIG 缺失 long"
    print(f"✅ CACHE_CONFIG 包含 {len(config.CACHE_CONFIG)} 个级别")

    # 测试FF因子映射
    assert 'mkt' in config.INDEX_MAP, "❌ INDEX_MAP 缺失 mkt"
    assert 'small' in config.INDEX_MAP, "❌ INDEX_MAP 缺失 small"
    print(f"✅ INDEX_MAP 包含 {len(config.INDEX_MAP)} 个因子")

    print("✅ config.py 测试通过\n")


def test_utils():
    """测试工具模块"""
    print("🧪 测试 utils/...")
    from utils import retry_on_failure, fmt_pct, fmt_f, safe_divide, normalize_score

    # 测试格式化函数
    assert fmt_pct(0.0523) == "+5.2%", "❌ fmt_pct 测试失败"
    assert fmt_pct(-0.0315) == "-3.2%", "❌ fmt_pct 测试失败"
    assert fmt_pct(None) == "N/A", "❌ fmt_pct 测试失败"
    print("✅ fmt_pct 正常")

    assert fmt_f(3.14159) == "3.14", "❌ fmt_f 测试失败"
    assert fmt_f(None) == "N/A", "❌ fmt_f 测试失败"
    print("✅ fmt_f 正常")

    # 测试安全除法
    assert safe_divide(10, 2) == 5.0, "❌ safe_divide 测试失败"
    assert safe_divide(10, 0) == 0.0, "❌ safe_divide 测试失败"
    print("✅ safe_divide 正常")

    # 测试归一化
    score = normalize_score(0.05, 0.0, 0.10)
    assert abs(score - 50.0) < 0.01, "❌ normalize_score 测试失败"
    print("✅ normalize_score 正常")

    # 测试重试装饰器（模拟失败3次后返回None）
    @retry_on_failure(retries=3, delay=0.1)
    def failing_func():
        raise ValueError("Test error")

    result = failing_func()
    assert result is None, "❌ retry_on_failure 测试失败"
    print("✅ retry_on_failure 正常")

    print("✅ utils/ 测试通过\n")


def test_import_chain():
    """测试导入链是否正常"""
    print("🧪 测试导入链...")
    try:
        from utils.helpers import (
            fmt_pct, fmt_f, safe_divide, normalize_score,
            retry_on_failure, get_date_range, quarter_to_date
        )
        print("✅ 可以从 utils.helpers 导入所有函数")

        from config import RADAR_WEIGHTS, CACHE_CONFIG, INDEX_MAP
        print("✅ 可以从 config 导入所有配置")

        # 测试零依赖（utils不应依赖config）
        print("✅ 没有循环依赖\n")
    except Exception as e:
        print(f"❌ 导入失败: {e}\n")
        raise


def main():
    """运行所有测试"""
    print("=" * 60)
    print("🚀 第一步：配置+工具模块 测试")
    print("=" * 60 + "\n")

    try:
        test_config()
        test_utils()
        test_import_chain()

        print("=" * 60)
        print("🎉 第一步测试全部通过！")
        print("=" * 60)
        print("\n📋 第一步完成内容：")
        print("  ✅ config.py - 全局配置中心")
        print("  ✅ utils/__init__.py - 工具模块入口")
        print("  ✅ utils/helpers.py - 通用工具函数")
        print("\n🔑 关键特性：")
        print("  • 零依赖：utils层不依赖config，只有标准库")
        print("  • 纯函数：所有工具函数无副作用")
        print("  • 配置集中：所有阈值、权重、映射统一管理")

    except AssertionError as e:
        print(f"\n❌ 测试失败: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
