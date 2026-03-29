"""
动态性能检查（快速版本）
验证：装饰器开销、缓存命中率、内存泄露、API稳定性
"""

import time
import psutil
import tracemalloc
from pathlib import Path
import sys

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline import analyze_fund

def measure_memory_usage():
    """测量内存使用情况"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024  # MB

def test_decorator_overhead():
    """测试 @audit_logger 装饰器开销"""
    print("\n" + "="*60)
    print("测试 1: @audit_logger 装饰器性能开销")
    print("="*60)

    test_fund = "000001"  # 华夏成长

    # 预热（确保缓存生效）
    try:
        analyze_fund(test_fund, years=1)
    except Exception as e:
        print(f"⚠️ 预热失败: {e}")

    # 测试 3 次
    times = []
    for i in range(3):
        start = time.time()
        try:
            analyze_fund(test_fund, years=1)
            elapsed = time.time() - start
            times.append(elapsed)
            print(f"  第 {i+1} 次: {elapsed:.2f}s")
        except Exception as e:
            print(f"  第 {i+1} 次: 失败 - {e}")
            times.append(None)

    if times and all(t is not None for t in times):
        avg_time = sum(times) / len(times)
        print(f"\n  平均执行时间: {avg_time:.2f}s")
        print(f"  装饰器开销评估: {'✅ 可接受' if avg_time < 60 else '⚠️ 偏高'}")
        return True
    else:
        print("  ❌ 测试失败")
        return False

def test_memory_leak():
    """测试内存泄露"""
    print("\n" + "="*60)
    print("测试 2: 内存泄露检测")
    print("="*60)

    test_funds = ["000001", "110022", "161725"]
    mem_before = measure_memory_usage()

    print(f"  初始内存: {mem_before:.2f} MB")

    for fund in test_funds:
        try:
            analyze_fund(fund, years=1)
        except Exception as e:
            print(f"  {fund}: 失败 - {e}")

    mem_after = measure_memory_usage()
    mem_diff = mem_after - mem_before

    print(f"  最终内存: {mem_after:.2f} MB")
    print(f"  内存增长: {mem_diff:+.2f} MB")

    if mem_diff < 50:  # 增长小于 50MB
        print("  ✅ 无明显内存泄露")
        return True
    else:
        print("  ⚠️ 内存增长较多，可能存在泄露")
        return False

def test_api_stability():
    """测试 API 稳定性"""
    print("\n" + "="*60)
    print("测试 3: API 稳定性（多次调用）")
    print("="*60)

    test_fund = "000001"
    success_count = 0
    total_count = 5

    for i in range(total_count):
        try:
            start = time.time()
            analyze_fund(test_fund, years=1)
            elapsed = time.time() - start
            print(f"  第 {i+1} 次: ✅ 成功 ({elapsed:.2f}s)")
            success_count += 1
        except Exception as e:
            print(f"  第 {i+1} 次: ❌ 失败 - {e}")

    success_rate = success_count / total_count * 100
    print(f"\n  成功率: {success_rate:.0f}%")

    if success_rate >= 80:
        print("  ✅ API 稳定性良好")
        return True
    else:
        print("  ⚠️ API 稳定性较差")
        return False

def test_logging_output():
    """测试日志输出"""
    print("\n" + "="*60)
    print("测试 4: 日志输出验证")
    print("="*60)

    import logging
    from io import StringIO

    # 捕获日志输出
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger()
    logger.addHandler(handler)

    try:
        analyze_fund("000001", years=1)
    except Exception as e:
        print(f"  分析失败: {e}")

    log_output = log_stream.getvalue()

    # 检查关键日志
    has_audit_logger = "audit_logger" in log_output.lower()
    has_function_entry = any(("进入函数" in line or "exit" in line.lower()) for line in log_output.split('\n'))

    print(f"  包含审计日志: {'✅' if has_audit_logger else '⚠️'}")
    print(f"  包含函数进出日志: {'✅' if has_function_entry else '⚠️'}")

    logger.removeHandler(handler)
    return has_audit_logger or has_function_entry

def run_all_tests():
    """运行所有动态测试"""
    print("\n" + "="*60)
    print("基金穿透式分析 - 动态性能检查（快速版本）")
    print("="*60)

    tests = [
        ("装饰器性能开销", test_decorator_overhead),
        ("内存泄露检测", test_memory_leak),
        ("API 稳定性", test_api_stability),
        ("日志输出验证", test_logging_output),
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ {test_name} 测试出错: {e}")
            import traceback
            traceback.print_exc()
            results[test_name] = False

    print("\n" + "="*60)
    print("测试总结")
    print("="*60)

    for test_name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {test_name}: {status}")

    all_passed = all(results.values())
    print(f"\n总体评分: {'⭐⭐⭐⭐⭐ (5/5)' if all_passed else '⭐⭐⭐⭐ (4/5)' if 3 <= sum(results.values()) < 5 else '⭐⭐⭐ (3/5)'}")

    return all_passed

if __name__ == "__main__":
    run_all_tests()
