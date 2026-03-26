"""调试 fetch_basic_info 函数"""

import sys
import traceback
from data import fetch_basic_info

# 测试几个常见的基金代码
test_symbols = ['000001', '000069', '510300', '161725']

for symbol in test_symbols:
    print(f"\n{'='*60}")
    print(f"测试基金代码: {symbol}")
    print(f"{'='*60}")
    
    try:
        result = fetch_basic_info(symbol)
        print(f"✅ 成功获取基本信息")
        print(f"  基金名称: {result.get('name', 'N/A')}")
        print(f"  基金类型: {result.get('type_category', 'N/A')}")
        print(f"  基金规模: {result.get('scale', 'N/A')}")
        print(f"  基准: {result.get('benchmark_text', 'N/A')}")
    except Exception as e:
        print(f"❌ 错误: {type(e).__name__}: {e}")
        print(f"\n完整堆栈:")
        traceback.print_exc()
        sys.exit(1)

print(f"\n{'='*60}")
print("🎉 所有测试通过！")
print(f"{'='*60}")
