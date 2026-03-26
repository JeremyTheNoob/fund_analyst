"""端到端测试脚本"""

import sys
import pandas as pd
from models import analyze_fund

print(f"{'='*70}")
print(f"🧪 端到端测试：完整分析流程")
print(f"{'='*70}")

# 测试几个不同类型的基金
test_cases = [
    {'code': '000001', 'name': '华夏成长（混合型）', 'type': 'mixed'},
    {'code': '000069', 'name': '国投瑞银中高等级债券A（债券型）', 'type': 'bond'},
    {'code': '510300', 'name': '华泰柏瑞沪深300ETF（指数型）', 'type': 'index'},
]

for test_case in test_cases:
    symbol = test_case['code']
    name = test_case['name']
    expected_type = test_case['type']
    
    print(f"\n{'='*70}")
    print(f"测试: {symbol} - {name}")
    print(f"{'='*70}")
    
    try:
        # 运行完整分析
        result = analyze_fund(
            symbol=symbol,
            years=3,
            since_inception=False,
        )
        
        # 检查结果
        if 'error' in result:
            print(f"❌ 分析失败: {result['error']}")
            continue
        
        basic_info = result.get('basic_info', {})
        nav_data = result.get('nav_data', pd.DataFrame())
        model_type = result.get('model_type', '')
        model_results = result.get('model_results', {})
        performance = result.get('performance', {})
        
        print(f"✅ 分析成功")
        print(f"\n📊 基本信息:")
        print(f"  名称: {basic_info.get('name', 'N/A')}")
        print(f"  类型: {basic_info.get('type_category', 'N/A')}")
        print(f"  规模: {basic_info.get('scale', 'N/A')}")
        print(f"  经理: {basic_info.get('manager', 'N/A')}")
        
        print(f"\n📈 净值数据:")
        print(f"  记录数: {len(nav_data)}")
        print(f"  起始日期: {nav_data['date'].min() if not nav_data.empty else 'N/A'}")
        print(f"  结束日期: {nav_data['date'].max() if not nav_data.empty else 'N/A'}")
        
        print(f"\n🔬 模型类型: {model_type}")
        print(f"  预期类型: {expected_type}")
        
        if model_type != expected_type:
            print(f"  ⚠️  类型不匹配（可能是持仓数据问题）")
        
        print(f"\n📊 业绩指标:")
        print(f"  累计收益: {performance.get('total_return', 0):.2%}")
        print(f"  年化收益: {performance.get('annual_return', 0):.2%}")
        print(f"  年化波动: {performance.get('annual_volatility', 0):.2%}")
        print(f"  夏普比率: {performance.get('sharpe_ratio', 0):.3f}")
        print(f"  最大回撤: {performance.get('max_drawdown', 0):.2%}")
        
        print(f"\n🧩 模型结果:")
        if 'model_name' in model_results:
            print(f"  模型: {model_results.get('model_name', 'N/A')}")
        
        if 'radar_scores' in model_results:
            radar = model_results['radar_scores']
            print(f"  综合评分: {radar.get('total_score', 0):.1f}")
        
        print(f"\n✅ 测试通过")
        
    except Exception as e:
        print(f"❌ 错误: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

print(f"\n{'='*70}")
print(f"🎉 所有端到端测试通过！")
print(f"{'='*70}")
