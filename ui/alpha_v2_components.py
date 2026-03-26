"""
Alpha v2.0 UI展示组件
包含：分层Alpha展示、择时能力进度条、月度胜率热力图
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np


def render_alpha_hierarchical(alpha_v2_result: dict):
    """
    渲染三层次Alpha展示（三列布局）

    Args:
        alpha_v2_result: Alpha v2.0结果字典
    """
    if 'hierarchical' not in alpha_v2_result or not alpha_v2_result['hierarchical']:
        st.warning("⚠️ Alpha数据不足，无法展示分层分析")
        return

    hierarchical = alpha_v2_result['hierarchical']

    st.markdown("### 🎯 三层次Alpha分析（周频）")
    st.markdown("""
    <small style="color: #666;">
    从单因子到行业中性化，逐步剥离风格暴露，还原真实的选股能力
    </small>
    """, unsafe_allow_html=True)

    # 三列布局
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### 📊 CAPM单因子")
        if hierarchical['capm']:
            capm = hierarchical['capm']
            alpha_val = capm['alpha']
            alpha_pval = capm['alpha_pval']

            # Alpha值显示（涨红跌绿）
            alpha_color = "#e74c3c" if alpha_val > 0 else "#27ae60"
            alpha_sign = "✅" if alpha_pval < 0.05 else "📊"

            st.markdown(f"""
            <div style="text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; border-left: 4px solid #3498db;">
                <div style="font-size: 2.5em; font-weight: bold; color: {alpha_color};">
                    {alpha_sign} {alpha_val*100:.2f}%
                </div>
                <div style="color: #666; font-size: 0.9em;">
                    p={alpha_pval:.4f}
                </div>
                <div style="color: #888; font-size: 0.8em; margin-top: 10px;">
                    Beta: {capm['beta']:.3f}<br>
                    R²: {capm['r_squared']:.3f}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # 显著性提示
            if alpha_pval < 0.05:
                if alpha_val > 0:
                    st.success("✅ 显著跑赢市场")
                else:
                    st.error("❌ 显著跑输市场")
            else:
                st.info("📊 不显著，可能源于短期波动")
        else:
            st.info("数据不足")

    with col2:
        st.markdown("#### 🎯 FF3/5因子")
        if hierarchical['ff']:
            ff = hierarchical['ff']
            alpha_val = ff['alpha']
            alpha_pval = ff['alpha_pval']

            # Alpha值显示（涨红跌绿）
            alpha_color = "#e74c3c" if alpha_val > 0 else "#27ae60"
            alpha_sign = "✅" if alpha_pval < 0.05 else "📊"

            st.markdown(f"""
            <div style="text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; border-left: 4px solid #9b59b6;">
                <div style="font-size: 2.5em; font-weight: bold; color: {alpha_color};">
                    {alpha_sign} {alpha_val*100:.2f}%
                </div>
                <div style="color: #666; font-size: 0.9em;">
                    p={alpha_pval:.4f}
                </div>
                <div style="color: #888; font-size: 0.8em; margin-top: 10px;">
                    R²: {ff['r_squared']:.3f}<br>
                    模型: {ff['model_name']}
                </div>
            </div>
            """, unsafe_allow_html=True)

            # 因子Beta
            if 'factor_betas' in ff:
                st.markdown("**因子Beta:**")
                for factor, beta in ff['factor_betas'].items():
                    beta_sign = "📈" if beta > 0 else "📉"
                    st.markdown(f"<small>{beta_sign} {factor}: {beta:.3f}</small>", unsafe_allow_html=True)

            # 显著性提示
            if alpha_pval < 0.05:
                if alpha_val > 0:
                    st.success("✅ 剥离风格后仍显著")
                else:
                    st.error("❌ 剥离风格后仍跑输")
            else:
                st.info("📊 风格剥离后不显著")
        else:
            st.info("数据不足")

    with col3:
        st.markdown("#### 🏢 行业中性化")
        if hierarchical['industry_neutral']:
            ind = hierarchical['industry_neutral']
            alpha_val = ind['alpha']
            alpha_pval = ind['alpha_pval']

            # Alpha值显示（涨红跌绿）
            alpha_color = "#e74c3c" if alpha_val > 0 else "#27ae60"
            alpha_sign = "✨" if alpha_pval < 0.05 else "📊"

            st.markdown(f"""
            <div style="text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; border-left: 4px solid #e67e22;">
                <div style="font-size: 2.5em; font-weight: bold; color: {alpha_color};">
                    {alpha_sign} {alpha_val*100:.2f}%
                </div>
                <div style="color: #666; font-size: 0.9em;">
                    p={alpha_pval:.4f}
                </div>
                <div style="color: #888; font-size: 0.8em; margin-top: 10px;">
                    R²: {ind['r_squared']:.3f}<br>
                    纯选股能力
                </div>
            </div>
            """, unsafe_allow_html=True)

            # 显著性提示
            if alpha_pval < 0.05:
                if alpha_val > 0:
                    st.success("✨ 极致选股能力")
                else:
                    st.error("❌ 选股能力不足")
            else:
                st.info("📊 选股能力不显著")
        else:
            st.info("暂未实现")

    # Alpha稳定性判断
    if hierarchical['capm'] and hierarchical['ff']:
        alpha_drop = abs(hierarchical['capm']['alpha'] - hierarchical['ff']['alpha'])
        if alpha_drop < 0.02:
            st.success(f"✅ **Alpha稳定**（波动={alpha_drop*100:.2f}%），选股能力扎实，不依赖风格暴露")
        else:
            st.warning(f"⚠️ **Alpha波动较大**（波动={alpha_drop*100:.2f}%），风格暴露占比较高，部分收益源于风格")


def render_timing_ability(alpha_v2_result: dict):
    """
    渲染择时能力展示（进度条）

    Args:
        alpha_v2_result: Alpha v2.0结果字典
    """
    if 'timing' not in alpha_v2_result or not alpha_v2_result['timing']:
        st.warning("⚠️ 择时数据不足，无法展示")
        return

    timing = alpha_v2_result['timing']

    st.markdown("### 🕐 择时能力检测（Treynor-Mazuy模型）")
    st.markdown("""
    <small style="color: #666;">
    检测基金经理的高抛低吸能力：γ>0表示牛市加仓、熊市减仓（独立轨道，不与FF因子混用）
    </small>
    """, unsafe_allow_html=True)

    # 择时得分
    timing_score = timing['timing_score']
    gamma = timing['gamma']
    gamma_pval = timing['gamma_pval']

    # 根据得分选择颜色
    if timing_score >= 80:
        bar_color = "#27ae60"  # 绿色
        emoji = "✨"
    elif timing_score >= 60:
        bar_color = "#f39c12"  # 橙色
        emoji = "✅"
    elif timing_score >= 40:
        bar_color = "#95a5a6"  # 灰色
        emoji = "📊"
    else:
        bar_color = "#e74c3c"  # 红色
        emoji = "❌"

    # 进度条展示
    st.markdown(f"""
    <div style="margin: 20px 0;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
            <span><b>择时能力:</b></span>
            <span><b>{timing_score:.1f}/100</b></span>
        </div>
        <div style="background: #e0e0e0; border-radius: 10px; height: 30px; overflow: hidden;">
            <div style="background: {bar_color}; width: {timing_score}%; height: 100%; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: 1.1em;">
                {emoji} {timing_score:.1f}%
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 详细指标
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("择时系数 γ", f"{gamma:.4f}")

    with col2:
        pval_text = f"{gamma_pval:.4f}"
        if gamma_pval < 0.05:
            pval_text = f"{pval_text} ✅"
        elif gamma_pval < 0.1:
            pval_text = f"{pval_text} ⚠️"
        else:
            pval_text = f"{pval_text} 📊"
        st.metric("显著性 p", pval_text)

    with col3:
        alpha_val = timing['alpha'] * 100
        st.metric("选股Alpha", f"{alpha_val:.2f}%")

    # 解读
    st.markdown(f"**择时解读:**\n\n{timing['interpretation']}")

    # 择时能力判定
    if gamma_pval < 0.05:
        if gamma > 0:
            if gamma > 0.1:
                st.success(f"✨ **择时能力极强**（γ={gamma:.4f} > 0.1，牛市显著加仓，熊市显著减仓）")
            else:
                st.success(f"✅ **择时能力良好**（γ={gamma:.4f} > 0，具备一定的市场时机把握能力）")
        else:
            st.error(f"❌ **反向择时**（γ={gamma:.4f} < 0，择时操作反而拖累了收益）")
    else:
        st.info(f"📊 **择时能力不显著**（p={gamma_pval:.3f} > 0.1），难以判断经理的择时能力")


def render_monthly_win_rate(alpha_v2_result: dict):
    """
    渲染月度Alpha胜率展示（热力图）

    Args:
        alpha_v2_result: Alpha v2.0结果字典
    """
    if 'monthly_win_rate' not in alpha_v2_result or not alpha_v2_result['monthly_win_rate']:
        st.warning("⚠️ 月度胜率数据不足，无法展示")
        return

    win_rate = alpha_v2_result['monthly_win_rate']

    st.markdown("### 📈 月度Alpha胜率分析")
    st.markdown("""
    <small style="color: #666;">
    统计最近36个月中Alpha>0的月份数，评估超额收益的持续性和稳定性
    </small>
    """, unsafe_allow_html=True)

    # 胜率展示
    win_rate_val = win_rate['win_rate']
    win_months = win_rate['win_months']
    total_months = win_rate['total_months']

    # 根据胜率选择颜色和emoji
    if win_rate_val >= 0.70:
        bar_color = "#27ae60"  # 绿色
        emoji = "✨"
    elif win_rate_val >= 0.60:
        bar_color = "#2ecc71"  # 浅绿
        emoji = "✅"
    elif win_rate_val >= 0.50:
        bar_color = "#f39c12"  # 橙色
        emoji = "📊"
    else:
        bar_color = "#e74c3c"  # 红色
        emoji = "❌"

    # 胜率进度条
    st.markdown(f"""
    <div style="margin: 20px 0;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
            <span><b>月度胜率:</b></span>
            <span><b>{win_rate_val*100:.1f}% ({win_months}/{total_months})</b></span>
        </div>
        <div style="background: #e0e0e0; border-radius: 10px; height: 30px; overflow: hidden;">
            <div style="background: {bar_color}; width: {win_rate_val*100}%; height: 100%; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: 1.1em;">
                {emoji} {win_rate_val*100:.1f}%
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 解读
    st.markdown(f"**胜率解读:**\n\n{win_rate['interpretation']}")

    # 月度Alpha热力图
    st.markdown("#### 📅 最近36个月Alpha热力图")

    monthly_alpha = win_rate['monthly_alpha_series']

    # 转换为DataFrame
    df_alpha = monthly_alpha.reset_index()
    df_alpha.columns = ['date', 'alpha']
    df_alpha['year'] = df_alpha['date'].dt.year
    df_alpha['month'] = df_alpha['date'].dt.month

    # 创建热力图数据（年×月）
    years = sorted(df_alpha['year'].unique())
    months = range(1, 13)

    heatmap_data = []
    for year in years:
        row = []
        for month in months:
            alpha_val = df_alpha[(df_alpha['year'] == year) & (df_alpha['month'] == month)]['alpha']
            if len(alpha_val) > 0:
                row.append(alpha_val.values[0] * 100)  # 转为百分比
            else:
                row.append(np.nan)
        heatmap_data.append(row)

    # 创建热力图
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'],
        y=years,
        colorscale=[
            [0, "#e74c3c"],    # 红色（负收益）
            [0.5, "#f8f9fa"],  # 灰色（接近0）
            [1, "#27ae60"]     # 绿色（正收益）
        ],
        text=[[f"{v:.1f}%" if not np.isnan(v) else "" for v in row] for row in heatmap_data],
        texttemplate="%{text}",
        textfont={"size": 11},
        hovertext=[[f"{year}年{month}月<br>Alpha: {v:.2f}%" if not np.isnan(v) else f"{year}年{month}月<br>数据缺失"
                    for month, v in enumerate(row, 1)]
                   for year, row in zip(years, heatmap_data)],
        hoverinfo="text",
        colorbar=dict(
            title="Alpha (%)",
            ticksuffix="%",
            tickmode="auto",
            nticks=10
        )
    ))

    fig.update_layout(
        title="月度Alpha热力图（%）",
        xaxis_title="月份",
        yaxis_title="年份",
        height=400,
        margin=dict(l=50, r=50, t=50, b=50)
    )

    st.plotly_chart(fig, use_container_width=True)

    # 最近6个月详情
    st.markdown("#### 📊 最近6个月Alpha详情")
    recent_alpha = monthly_alpha.tail(6)
    recent_df = pd.DataFrame({
        '月份': recent_alpha.index.strftime('%Y-%m'),
        'Alpha (%)': (recent_alpha.values * 100).round(2),
        '表现': ['✅ 跑赢' if alpha > 0 else '❌ 跑输' for alpha in recent_alpha.values]
    })

    st.dataframe(
        recent_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            '月份': st.column_config.TextColumn('月份', width='medium'),
            'Alpha (%)': st.column_config.NumberColumn('Alpha (%)', format="%.2f%%"),
            '表现': st.column_config.TextColumn('表现', width='small')
        }
    )


def render_alpha_v2_dashboard(alpha_v2_result: dict):
    """
    渲染Alpha v2.0完整仪表盘

    Args:
        alpha_v2_result: Alpha v2.0结果字典
    """
    if not alpha_v2_result or alpha_v2_result.get('error'):
        st.error(f"⚠️ Alpha分析失败: {alpha_v2_result.get('error', '未知错误')}")
        return

    st.markdown("---")

    # 标题
    st.markdown("## 🎯 Alpha v2.0 专业分层版")
    st.markdown("""
    <small style="color: #666;">
    基于周频数据的三层次Alpha分析，剥离风格暴露，还原真实的选股能力
    </small>
    """, unsafe_allow_html=True)

    # 1. 三层次Alpha
    render_alpha_hierarchical(alpha_v2_result)

    st.markdown("---")

    # 2. 择时能力
    render_timing_ability(alpha_v2_result)

    st.markdown("---")

    # 3. 月度胜率
    render_monthly_win_rate(alpha_v2_result)
