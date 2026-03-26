"""
持仓穿透分析UI组件
展示行业权重、集中度、基本面、风格对比
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def render_industry_weights_panel(industry_weights: dict):
    """
    渲染行业权重面板

    Args:
        industry_weights: 行业权重计算结果
    """
    if 'note' in industry_weights and '数据不足' in industry_weights['note']:
        st.info(industry_weights['note'])
        return

    st.markdown("### 🏭 行业配置")

    # 1. 甜甜圈图
    top_industries = industry_weights.get('top_industries', [])

    if top_industries:
        fig = go.Figure(data=[go.Pie(
            labels=[i['industry'] for i in top_industries],
            values=[i['weight'] for i in top_industries],
            hole=0.4,
            textinfo='label+percent',
            textposition='outside',
            marker=dict(colors=px.colors.sequential.Blues_r),
        )])

        fig.update_layout(
            title='前十大行业配置',
            height=400,
            margin=dict(l=0, r=0, t=50, b=0),
        )

        st.plotly_chart(fig, use_container_width=True)

    # 2. 集中度说明
    note = industry_weights.get('note', '')
    concentration = industry_weights.get('concentration', '')

    col1, col2 = st.columns(2)
    with col1:
        st.metric("集中度", concentration)
    with col2:
        st.info(note)

    # 3. 完整表格(可折叠)
    full_weights = industry_weights.get('industry_weights', {})
    if full_weights:
        with st.expander("📋 查看完整行业分布"):
            df_industry = pd.DataFrame([
                {'行业': k, '权重(%)': v}
                for k, v in full_weights.items()
            ]).sort_values('权重(%)', ascending=False)

            st.dataframe(
                df_industry,
                use_container_width=True,
                hide_index=True,
            )


def render_concentration_panel(concentration: dict):
    """
    渲染持仓集中度面板

    Args:
        concentration: 持仓集中度计算结果
    """
    if 'note' in concentration and '数据不足' in concentration['note']:
        st.info(concentration['note'])
        return

    st.markdown("### 🎯 持仓集中度")

    # 1. 四列指标卡
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "HHI指数",
            f"{concentration.get('hhi', 0):.2f}",
            help="Herfindahl-Hirschman Index,越大表示持仓越集中"
        )

    with col2:
        st.metric(
            "前三大占比",
            f"{concentration.get('top3_ratio', 0):.1f}%",
            help="前三大重仓股占基金净值比例"
        )

    with col3:
        st.metric(
            "前五大占比",
            f"{concentration.get('top5_ratio', 0):.1f}%",
            help="前五大重仓股占基金净值比例"
        )

    with col4:
        st.metric(
            "前十大占比",
            f"{concentration.get('top10_ratio', 0):.1f}%",
            help="前十大重仓股占基金净值比例"
        )

    # 2. 分散度评分
    dispersion_score = concentration.get('dispersion_score', 0)
    st.metric("分散度评分", f"{dispersion_score:.1f}", help="0-100分,分数越高越分散")

    # 3. 集中度评级
    level = concentration.get('concentration_level', '')
    note = concentration.get('note', '')

    if level == '高度集中':
        st.warning(note)
    elif level == '中度集中':
        st.info(note)
    else:
        st.success(note)


def render_fundamentals_panel(fundamentals: dict):
    """
    渲染个股基本面面板(增强版)

    Args:
        fundamentals: 个股基本面数据
    """
    if not fundamentals or 'note' in fundamentals:
        st.info("个股基本面数据不足")
        return

    st.markdown("### 💰 前十大重仓股基本面")

    # 转换为DataFrame
    df_fund = pd.DataFrame(fundamentals).T.reset_index()
    df_fund.columns = ['代码', '名称', '价格', '市值(亿)', '规模', '风格', 'PE', 'PB', 'ROE']

    # 市值排序
    df_fund = df_fund.sort_values('市值(亿)', ascending=False)

    # 规模标签颜色
    def color_size(size):
        if size == '大盘':
            return '🟢'
        elif size == '中盘':
            return '🟡'
        else:
            return '🔵'

    df_fund['规模'] = df_fund['规模'].apply(color_size)

    # 投资风格标签颜色
    def color_style(style):
        if style == '价值':
            return '🔵'
        elif style == '成长':
            return '🔴'
        else:
            return '⚪'

    df_fund['风格'] = df_fund['风格'].apply(color_style)

    # PE颜色(高PE为红,低PE为绿)
    def color_pe(pe):
        if pe > 50:
            return '🔴'
        elif pe < 20:
            return '🟢'
        else:
            return '⚪'

    df_fund['PE'] = df_fund['PE'].apply(color_pe)

    # 展示表格
    st.dataframe(
        df_fund,
        use_container_width=True,
        hide_index=True,
        column_config={
            '代码': st.column_config.TextColumn('代码', width='small'),
            '名称': st.column_config.TextColumn('名称', width='medium'),
            '价格': st.column_config.NumberColumn('价格', format='¥%.2f', width='small'),
            '市值(亿)': st.column_config.NumberColumn('市值(亿)', format='%.2f', width='small'),
            '规模': st.column_config.TextColumn('规模', width='small', help='🟢大盘 🟡中盘 🔵小盘'),
            '风格': st.column_config.TextColumn('风格', width='small', help='🔵价值 🔴成长 ⚪均衡'),
            'PE': st.column_config.NumberColumn('PE', format='%.2f', width='small'),
            'PB': st.column_config.NumberColumn('PB', format='%.2f', width='small'),
            'ROE': st.column_config.NumberColumn('ROE', format='%.2f%', width='small'),
        }
    )

    # 统计摘要
    avg_market_cap = df_fund['市值(亿)'].mean()
    avg_pe = df_fund['PE'].str.replace('🔴', '').str.replace('🟢', '').str.replace('⚪', '').astype(float).mean()
    avg_roe = df_fund['ROE'].mean()

    # 投资风格分布
    style_dist = df_fund['风格'].apply(lambda x: x.replace('🔵', '价值').replace('🔴', '成长').replace('⚪', '均衡')).value_counts()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("平均市值", f"{avg_market_cap:.1f}亿")
    with col2:
        st.metric("平均PE", f"{avg_pe:.1f}")
    with col3:
        st.metric("平均ROE", f"{avg_roe:.1f}%")
    with col4:
        st.metric("投资风格", ', '.join([f"{k}:{v}" for k, v in style_dist.items()]))


def render_style_comparison_panel(style_comparison: dict):
    """
    渲染风格对比面板(增强版)

    Args:
        style_comparison: 风格对比结果
    """
    if 'note' in style_comparison and '数据不足' in style_comparison['note']:
        st.info(style_comparison['note'])
        return

    st.markdown("### 🏷️ 风格一致性检验")

    # 1. 市值风格对比
    st.markdown("#### 市值风格")

    holding_style = style_comparison.get('holding_style', {})
    ff_style = style_comparison.get('ff_style', {})

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("持仓市值风格", holding_style.get('size', '未知'))

    with col2:
        st.metric("FF模型市值风格", ff_style.get('size', '未知'))

    with col3:
        is_size_consistent = style_comparison.get('is_size_consistent', True)
        size_status = "✅ 一致" if is_size_consistent else "⚠️ 不一致"
        st.metric("市值一致性", size_status)

    # 2. 投资风格对比
    st.markdown("#### 投资风格")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("持仓投资风格", holding_style.get('style', '未知'))

    with col2:
        st.metric("FF模型投资风格", ff_style.get('style', '未知'))

    with col3:
        is_style_consistent = style_comparison.get('is_style_consistent', True)
        style_status = "✅ 一致" if is_style_consistent else "⚠️ 不一致"
        st.metric("投资风格一致性", style_status)

    # 3. 总体判定
    note = style_comparison.get('note', '')
    if '✅' in note:
        st.success(note)
    elif '⚠️' in note:
        st.warning(note)
    else:
        st.info(note)


def render_holdings_change_panel(holdings_change: dict):
    """
    渲染持仓变动面板

    Args:
        holdings_change: 持仓变动分析结果
    """
    if 'note' in holdings_change and '数据不足' in holdings_change['note']:
        st.info(holdings_change['note'])
        return

    st.markdown("### 📈 持仓变动追踪")

    # 1. 换手率和稳定性评分
    col1, col2 = st.columns(2)

    with col1:
        turnover = holdings_change.get('turnover_rate', 0)
        st.metric("换手率", f"{turnover:.1f}%", help="新进+退出的平均占比")

    with col2:
        stability = holdings_change.get('stability_score', 0)
        st.metric("持仓稳定性", f"{stability:.1f}", help="0-100分,分数越高越稳定")

    # 2. 解读
    note = holdings_change.get('note', '')
    if stability > 70:
        st.success(note)
    elif stability > 50:
        st.info(note)
    else:
        st.warning(note)

    # 3. 新进/退出/加仓/减仓
    new_stocks = holdings_change.get('new_stocks', [])
    exited_stocks = holdings_change.get('exited_stocks', [])
    increased_stocks = holdings_change.get('increased_stocks', [])
    decreased_stocks = holdings_change.get('decreased_stocks', [])

    if new_stocks:
        st.markdown("#### 🆕 新进股票")
        df_new = pd.DataFrame(new_stocks)
        st.dataframe(
            df_new[['code', 'name', 'current_ratio']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'code': '代码',
                'name': '名称',
                'current_ratio': st.column_config.NumberColumn('最新占比', format='%.2f%%'),
            }
        )

    if exited_stocks:
        st.markdown("#### 🚪 退出股票")
        df_exited = pd.DataFrame(exited_stocks)
        st.dataframe(
            df_exited[['code', 'name', 'previous_ratio']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'code': '代码',
                'name': '名称',
                'previous_ratio': st.column_config.NumberColumn('上期占比', format='%.2f%%'),
            }
        )

    if increased_stocks:
        st.markdown("#### 📈 加仓股票")
        df_inc = pd.DataFrame(increased_stocks)
        st.dataframe(
            df_inc[['code', 'name', 'previous_ratio', 'current_ratio', 'change']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'code': '代码',
                'name': '名称',
                'previous_ratio': st.column_config.NumberColumn('上期占比', format='%.2f%%'),
                'current_ratio': st.column_config.NumberColumn('最新占比', format='%.2f%%'),
                'change': st.column_config.NumberColumn('变动', format='%+.2f%%'),
            }
        )

    if decreased_stocks:
        st.markdown("#### 📉 减仓股票")
        df_dec = pd.DataFrame(decreased_stocks)
        st.dataframe(
            df_dec[['code', 'name', 'previous_ratio', 'current_ratio', 'change']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'code': '代码',
                'name': '名称',
                'previous_ratio': st.column_config.NumberColumn('上期占比', format='%.2f%%'),
                'current_ratio': st.column_config.NumberColumn('最新占比', format='%.2f%%'),
                'change': st.column_config.NumberColumn('变动', format='%+.2f%%'),
            }
        )


def render_holdings_penetration_dashboard(penetration: dict):
    """
    渲染完整的持仓穿透分析仪表盘(增强版)

    Args:
        penetration: 持仓穿透分析结果
    """
    if not penetration:
        st.info("持仓穿透数据不足")
        return

    st.markdown("## 🔍 权益持仓穿透分析")

    # 使用Tab切换不同模块(5个Tab)
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["行业配置", "持仓集中度", "个股基本面", "风格一致性", "持仓变动"])

    with tab1:
        industry_weights = penetration.get('industry_weights', {})
        render_industry_weights_panel(industry_weights)

    with tab2:
        concentration = penetration.get('concentration', {})
        render_concentration_panel(concentration)

    with tab3:
        fundamentals = penetration.get('fundamentals', {})
        render_fundamentals_panel(fundamentals)

    with tab4:
        style_comparison = penetration.get('style_comparison', {})
        render_style_comparison_panel(style_comparison)

    with tab5:
        holdings_change = penetration.get('holdings_change', {})
        render_holdings_change_panel(holdings_change)
