"""
Alpha v2.0 UI组件测试
"""

import sys
sys.path.append('/Users/liuweihua/WorkBuddy/基金穿透式分析')

import streamlit as st
import pandas as pd
import numpy as np
from models.alpha_analysis import (
    calculate_alpha_hierarchical,
    calculate_timing_ability,
    calculate_monthly_win_rate,
)
from ui.alpha_v2_components import (
    render_alpha_v2_dashboard,
)

st.set_page_config(
    page_title="Alpha v2.0 UI测试",
    layout="wide"
)

st.title("🧪 Alpha v2.0 UI组件测试")

st.markdown("""
这个页面用于测试Alpha v2.0的UI展示组件。
使用模拟数据，不依赖真实数据源。
""")

# 生成模拟数据
@st.cache_data
def generate_mock_data(seed=42):
    np.random.seed(seed)
    dates = pd.date_range('2023-01-01', '2026-03-25', freq='D')

    # 净值数据
    nav_values = 1.0
    nav_list = []
    for i in range(len(dates)):
        daily_ret = np.random.normal(0.0005, 0.015)
        nav_values *= (1 + daily_ret)
        nav_list.append(nav_values)

    nav_data = pd.DataFrame({
        'date': dates,
        'nav': nav_list
    })
    nav_data['ret'] = nav_data['nav'].pct_change().fillna(0)

    # FF因子
    ff_factors = pd.DataFrame({
        'date': dates,
        'Mkt': np.random.normal(0.0003, 0.012, len(dates)),
        'SMB': np.random.normal(0.0001, 0.008, len(dates)),
        'HML': np.random.normal(0.0000, 0.007, len(dates))
    })

    # 基准
    benchmark_ret = pd.Series(np.random.normal(0.0004, 0.013, len(dates)), index=dates)

    return nav_data, ff_factors, benchmark_ret

# 生成数据
nav_data, ff_factors, benchmark_ret = generate_mock_data()

st.markdown(f"""
- 净值数据: {len(nav_data)} 天 ({nav_data['date'].min()} ~ {nav_data['date'].max()})
- FF因子: {len(ff_factors)} 天
- 基准: {len(benchmark_ret)} 天
""")

# 计算Alpha v2.0
ret_series = nav_data.set_index('date')['ret']

st.markdown("## 🔬 计算Alpha v2.0")

with st.spinner("正在计算Alpha v2.0..."):
    alpha_v2_result = {
        'hierarchical': calculate_alpha_hierarchical(
            fund_ret=ret_series,
            benchmark_ret=benchmark_ret,
            ff_factors=ff_factors,
            frequency='weekly'
        ),
        'timing': calculate_timing_ability(
            fund_ret=ret_series,
            benchmark_ret=benchmark_ret,
            frequency='weekly'
        ),
        'monthly_win_rate': calculate_monthly_win_rate(
            fund_ret=ret_series,
            benchmark_ret=benchmark_ret,
            months=36
        )
    }

st.success("✅ Alpha v2.0计算完成")

# 展示结果
st.markdown("## 📊 UI展示")

render_alpha_v2_dashboard(alpha_v2_result)

# 原始数据（可选）
with st.expander("🔍 查看原始数据"):
    st.markdown("### 三层次Alpha")
    st.json(alpha_v2_result['hierarchical'])

    st.markdown("### 择时能力")
    st.json(alpha_v2_result['timing'])

    st.markdown("### 月度胜率")
    st.json({
        'win_rate': alpha_v2_result['monthly_win_rate']['win_rate'],
        'win_months': alpha_v2_result['monthly_win_rate']['win_months'],
        'total_months': alpha_v2_result['monthly_win_rate']['total_months'],
    })
