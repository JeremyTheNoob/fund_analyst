"""
诊断基金 014416 的基准数据问题
"""
import sys
import logging

# 设置详细日志
logging.basicConfig(level=logging.DEBUG)

from data_loader.equity_loader import load_basic_info, build_benchmark, load_nav
from pipeline import _get_date_range

print("=== 步骤 1: 加载基金基本信息 ===")
basic = load_basic_info('014416')
print(f"基金名称: {basic.name}")
print(f"基准解析: {basic.benchmark_parsed}")

print("\n=== 步骤 2: 获取日期范围 ===")
nav_data = load_nav('014416', years=3)
if nav_data and nav_data.df is not None:
    start_str, end_str = _get_date_range(nav_data.df)
    print(f"分析周期: {start_str} ~ {end_str}")
    print(f"净值数据行数: {len(nav_data.df)}")
    print(f"净值日期范围: {nav_data.df['date'].min()} ~ {nav_data.df['date'].max()}")
else:
    print("❌ 无法加载净值数据")
    sys.exit(1)

print("\n=== 步骤 3: 构建基准数据 ===")
benchmark = build_benchmark(basic, start_str, end_str)
print(f"基准描述: {benchmark.description}")
print(f"基准数据形状: {benchmark.df.shape}")
print(f"基准数据列名: {benchmark.df.columns.tolist()}")

if benchmark.df.empty:
    print("\n❌ 基准数据为空！")
else:
    print(f"基准日期范围: {benchmark.df['date'].min()} ~ {benchmark.df['date'].max()}")
    print(f"基准数据前5行:\n{benchmark.df.head()}")
    print(f"基准数据后5行:\n{benchmark.df.tail()}")
    print(f"\n基准数据统计:")
    print(f"  平均收益率: {benchmark.df['bm_ret'].mean():.4f}")
    print(f"  标准差: {benchmark.df['bm_ret'].std():.4f}")
    print(f"  最大值: {benchmark.df['bm_ret'].max():.4f}")
    print(f"  最小值: {benchmark.df['bm_ret'].min():.4f}")

print("\n=== 步骤 4: 检查 pipeline 条件 ===")
has_benchmark = (
    basic.benchmark_parsed and
    basic.benchmark_parsed.get("components") and
    len(basic.benchmark_parsed.get("components", [])) > 0
)
print(f"has_benchmark: {has_benchmark}")
print(f"benchmark_parsed: {basic.benchmark_parsed}")
print(f"components: {basic.benchmark_parsed.get('components')}")
print(f"components 长度: {len(basic.benchmark_parsed.get('components', []))}")
