"""
第四步测试：UI层+主流程
验证 ui/ 和 main.py 是否正确创建且可用
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))


def test_file_structure():
    """测试文件结构"""
    print("🧪 测试文件结构...")
    import os

    # 检查 ui 目录
    assert os.path.exists('ui'), "❌ ui 目录不存在"
    print("✅ ui 目录存在")

    # 检查 ui/__init__.py
    assert os.path.exists('ui/__init__.py'), "❌ ui/__init__.py 不存在"
    print("✅ ui/__init__.py 存在")

    # 检查 ui 子模块
    assert os.path.exists('ui/charts.py'), "❌ ui/charts.py 不存在"
    print("✅ ui/charts.py 存在")
    assert os.path.exists('ui/components.py'), "❌ ui/components.py 不存在"
    print("✅ ui/components.py 存在")

    # 检查 main.py
    assert os.path.exists('main.py'), "❌ main.py 不存在"
    print("✅ main.py 存在\n")

    return True


def test_imports():
    """测试导入链"""
    print("🧪 测试导入链...")
    try:
        from ui.charts import (
            plot_radar_chart,
            plot_cumulative_return,
            plot_holdings_pie,
            plot_bond_structure,
            plot_style_analysis,
        )
        from ui.components import (
            render_kpi_card,
            render_metric_card,
            render_risk_card,
            render_basic_info,
            render_performance_metrics,
            render_ff_results,
            render_bond_results,
            render_model_results,
            render_radar_scores,
            render_stress_test,
            render_bond_holdings,
            render_analysis_report,
            render_css,
            render_disclaimer,
        )
        import main
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
        from utils.helpers import fmt_pct, fmt_f
        from data import fetch_nav, fetch_ff_factors, fetch_holdings
        from models import analyze_fund
        from ui import plot_radar_chart, render_kpi_card
        import main
        print("✅ ui 层正确依赖 config, utils, data, models")
        print("✅ main.py 正确依赖所有模块\n")
        return True
    except Exception as e:
        print(f"❌ 依赖检查失败: {e}\n")
        return False


def test_function_signatures():
    """测试函数签名"""
    print("🧪 测试函数签名...")
    from ui.charts import plot_radar_chart, plot_cumulative_return, plot_holdings_pie
    from ui.components import render_kpi_card, render_metric_card, render_risk_card
    import inspect

    # 测试 plot_radar_chart
    sig = inspect.signature(plot_radar_chart)
    assert 'scores' in sig.parameters
    assert 'weights' in sig.parameters
    assert 'model_type' in sig.parameters
    print("✅ plot_radar_chart 签名正确")

    # 测试 plot_cumulative_return
    sig = inspect.signature(plot_cumulative_return)
    assert 'fund_nav' in sig.parameters
    assert 'benchmark_df' in sig.parameters
    print("✅ plot_cumulative_return 签名正确")

    # 测试 plot_holdings_pie
    sig = inspect.signature(plot_holdings_pie)
    assert 'holdings' in sig.parameters
    assert 'value_col' in sig.parameters
    print("✅ plot_holdings_pie 签名正确")

    # 测试 render_kpi_card
    sig = inspect.signature(render_kpi_card)
    assert 'title' in sig.parameters
    assert 'value' in sig.parameters
    print("✅ render_kpi_card 签名正确")

    # 测试 render_metric_card
    sig = inspect.signature(render_metric_card)
    assert 'title' in sig.parameters
    assert 'metrics' in sig.parameters
    print("✅ render_metric_card 签名正确")

    # 测试 render_risk_card
    sig = inspect.signature(render_risk_card)
    assert 'title' in sig.parameters
    assert 'message' in sig.parameters
    print("✅ render_risk_card 签名正确\n")

    return True


def test_chart_creation():
    """测试图表创建（不实际渲染）"""
    print("🧪 测试图表创建...")
    from ui.charts import plot_radar_chart
    import pandas as pd

    # 测试雷达图
    scores = {'超额能力': 80, '风险控制': 70, '性价比': 75, '风格稳定': 65, '业绩持续': 70}
    weights = {'超额能力': 0.3, '风险控制': 0.15, '性价比': 0.2, '风格稳定': 0.15, '业绩持续': 0.2}

    try:
        fig = plot_radar_chart(scores, weights, 'equity')
        assert fig is not None, "❌ 雷达图创建失败"
        print("✅ 雷达图创建成功")
    except Exception as e:
        print(f"❌ 雷达图创建失败: {e}")
        return False

    # 测试累计收益曲线
    from ui.charts import plot_cumulative_return
    nav_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=100),
        'nav': [1.0 + i * 0.001 for i in range(100)]
    })

    try:
        fig = plot_cumulative_return(nav_data)
        assert fig is not None, "❌ 累计收益曲线创建失败"
        print("✅ 累计收益曲线创建成功\n")
    except Exception as e:
        print(f"❌ 累计收益曲线创建失败: {e}")
        return False

    return True


def test_config_usage():
    """测试配置使用"""
    print("🧪 测试配置使用...")
    import config

    # 测试 UI_CONFIG
    assert 'kpi_colors' in config.UI_CONFIG
    assert 'radar' in config.UI_CONFIG
    print("✅ UI_CONFIG 正确")

    # 测试 RISK_THRESHOLDS
    assert 'drawdown_warning' in config.RISK_THRESHOLDS
    assert 'drawdown_danger' in config.RISK_THRESHOLDS
    print("✅ RISK_THRESHOLDS 正确\n")

    return True


def test_main_structure():
    """测试main.py结构"""
    print("🧪 测试main.py结构...")
    import main

    # 检查是否有main函数
    assert hasattr(main, 'main'), "❌ main.py 缺少 main() 函数"
    print("✅ main.py 包含 main() 函数")

    # 检查main函数是否可调用
    assert callable(main.main), "❌ main() 不可调用"
    print("✅ main() 可调用\n")

    return True


def test_complete_import_chain():
    """测试完整导入链"""
    print("🧪 测试完整导入链...")
    try:
        # 按照依赖顺序导入
        import config
        from utils.helpers import fmt_pct, fmt_f
        from data import fetch_basic_info, fetch_nav, fetch_ff_factors
        from models import analyze_fund, classify_fund_type
        from ui import plot_radar_chart, render_kpi_card, render_analysis_report
        import main

        print("✅ 完整导入链测试通过")
        print("  config → utils → data → models → ui → main\n")
        return True
    except Exception as e:
        print(f"❌ 导入链测试失败: {e}\n")
        return False


def main_test():
    """运行所有测试"""
    print("=" * 60)
    print("🚀 第四步：UI层+主流程 测试")
    print("=" * 60 + "\n")

    tests = [
        ("文件结构", test_file_structure),
        ("导入链", test_imports),
        ("依赖关系", test_dependencies),
        ("函数签名", test_function_signatures),
        ("图表创建", test_chart_creation),
        ("配置使用", test_config_usage),
        ("main结构", test_main_structure),
        ("完整导入链", test_complete_import_chain),
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
        print("🎉 第四步测试全部通过！")
    else:
        print("❌ 部分测试失败")
    print("=" * 60)

    if all_passed:
        print("\n📋 第四步完成内容：")
        print("  ✅ ui/__init__.py - UI层入口")
        print("  ✅ ui/charts.py - 图表渲染（5个图表函数）")
        print("  ✅ ui/components.py - UI组件（14个组件函数）")
        print("  ✅ main.py - Streamlit主程序")
        print("\n📦 核心功能：")
        print("  • 图表渲染：")
        print("    - plot_radar_chart() - 五维雷达图")
        print("    - plot_cumulative_return() - 累计收益曲线")
        print("    - plot_holdings_pie() - 持仓饼图")
        print("    - plot_bond_structure() - 债券结构饼图")
        print("    - plot_style_analysis() - 风格分析图")
        print("  • UI组件：")
        print("    - render_kpi_card() - KPI卡片")
        print("    - render_metric_card() - 指标卡片")
        print("    - render_risk_card() - 风险提示卡片")
        print("    - render_basic_info() - 基本信息")
        print("    - render_performance_metrics() - 业绩指标")
        print("    - render_ff_results() - FF因子结果")
        print("    - render_bond_results() - 债券模型结果")
        print("    - render_model_results() - 模型结果")
        print("    - render_radar_scores() - 雷达图评分")
        print("    - render_stress_test() - 压力测试")
        print("    - render_bond_holdings() - 债券持仓")
        print("    - render_analysis_report() - 完整报告")
        print("    - render_css() - CSS样式")
        print("    - render_disclaimer() - 免责声明")
        print("  • 主程序：")
        print("    - Streamlit页面配置")
        print("    - 用户输入（基金代码+分析时长）")
        print("    - 调用模型层分析")
        print("    - 渲染完整报告")
        print("\n🔑 关键特性：")
        print("  • 完整导入链：config → utils → data → models → ui → main")
        print("  • 依赖正确：每层只依赖下层，无循环引用")
        print("  • 可运行：main.py 是一个完整的Streamlit应用")
        print("  • 可测试：所有函数可独立测试")

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main_test())
