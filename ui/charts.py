"""
图表渲染模块
使用Plotly渲染各类图表
依赖：plotly
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

import config


def plot_radar_chart(
    scores: dict,
    weights: dict,
    model_type: str,
) -> go.Figure:
    """
    绘制五维雷达图

    Args:
        scores: 五维评分字典 {'超额能力': 80, '风险控制': 70, ...}
        weights: 权重字典 {'超额能力': 0.3, '风险控制': 0.15, ...}
        model_type: 模型类型

    Returns:
        Plotly Figure对象
    """
    # 维度顺序
    dimensions = ['超额能力', '风险控制', '性价比', '风格稳定', '业绩持续']
    values = [scores.get(d, 50) for d in dimensions]

    # 颜色
    color_map = {
        'equity': config.UI_CONFIG['radar']['equity'],
        'bond': config.UI_CONFIG['radar']['bond'],
        'mixed': config.UI_CONFIG['radar']['mixed'],
        'index': config.UI_CONFIG['radar']['index'],
        'sector': config.UI_CONFIG['radar']['sector'],
    }
    fill_color = color_map.get(model_type, config.UI_CONFIG['radar']['others'])

    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=dimensions,
        fill='toself',
        name='综合评分',
        line_color=fill_color,
        fillcolor=f'rgba({hex_to_rgb(fill_color)}, 0.2)',
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
            ),
        ),
        showlegend=True,
        height=400,
        margin=dict(l=20, r=20, t=20, b=20),
    )

    return fig


def plot_cumulative_return(
    fund_nav: pd.DataFrame,
    benchmark_df: pd.DataFrame = None,
    fund_name: str = '基金',
) -> go.Figure:
    """
    绘制累计收益曲线

    Args:
        fund_nav: 基金净值数据
        benchmark_df: 基准数据
        fund_name: 基金名称

    Returns:
        Plotly Figure对象
    """
    # 基金累计净值
    fund_df = fund_nav[['date', 'nav']].copy()
    fund_df['cum_nav'] = fund_df['nav'] / fund_df['nav'].iloc[0] * 100

    fig = go.Figure()

    # 基金曲线
    fig.add_trace(go.Scatter(
        x=fund_df['date'],
        y=fund_df['cum_nav'],
        name=fund_name,
        line=dict(color=config.UI_CONFIG['kpi_colors']['red'], width=2),
    ))

    # 基准曲线
    if benchmark_df is not None and not benchmark_df.empty:
        if 'ret' in benchmark_df.columns:
            bm_df = benchmark_df[['date', 'ret']].copy()
            bm_df['cum_nav'] = (1 + bm_df['ret']).cumprod() * 100
            fig.add_trace(go.Scatter(
                x=bm_df['date'],
                y=bm_df['cum_nav'],
                name='基准',
                line=dict(color='gray', width=1.5, dash='dash'),
            ))

    fig.update_layout(
        title='累计收益曲线',
        xaxis_title='日期',
        yaxis_title='累计净值（起点=100）',
        hovermode='x unified',
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
    )

    # 添加零线
    fig.add_hline(y=100, line_dash="dot", line_color="gray", opacity=0.5)

    return fig


def plot_holdings_pie(
    holdings: pd.DataFrame,
    value_col: str = '占净值比例',
    label_col: str = '股票名称',
    top_n: int = 10,
) -> go.Figure:
    """
    绘制持仓饼图

    Args:
        holdings: 持仓数据
        value_col: 值列名
        label_col: 标签列名
        top_n: 显示前N大

    Returns:
        Plotly Figure对象
    """
    if holdings.empty:
        fig = go.Figure()
        fig.update_layout(
            title='持仓数据暂无',
            height=400,
        )
        return fig

    # 取前N大
    df_top = holdings.head(top_n).copy()

    fig = go.Figure(data=[go.Pie(
        labels=df_top[label_col],
        values=df_top[value_col],
        hole=0.3,
    )])

    fig.update_layout(
        title=f'前{top_n}大持仓',
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=True,
    )

    return fig


def plot_bond_structure(
    bond_structure: dict,
) -> go.Figure:
    """
    绘制债券结构饼图

    Args:
        bond_structure: 债券结构字典

    Returns:
        Plotly Figure对象
    """
    type_dist = bond_structure.get('type_distribution', {})
    if not type_dist:
        fig = go.Figure()
        fig.update_layout(
            title='债券持仓数据暂无',
            height=400,
        )
        return fig

    fig = go.Figure(data=[go.Pie(
        labels=list(type_dist.keys()),
        values=list(type_dist.values()),
        hole=0.3,
    )])

    fig.update_layout(
        title='债券持仓结构',
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
    )

    return fig


def plot_style_analysis(
    style_analysis: dict,
) -> go.Figure:
    """
    绘制风格分析图

    Args:
        style_analysis: 风格分析结果

    Returns:
        Plotly Figure对象
    """
    beta = style_analysis.get('beta', 0)
    correlation = style_analysis.get('correlation', 0)
    style = style_analysis.get('style', '未知')

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=['Beta', '相关性'],
        y=[beta, correlation],
        marker_color=[config.UI_CONFIG['radar']['equity'], config.UI_CONFIG['radar']['bond']],
    ))

    fig.update_layout(
        title=f'风格分析：{style}',
        yaxis_title='数值',
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
    )

    # 添加参考线
    fig.add_hline(y=1.0, line_dash="dot", line_color="gray", opacity=0.5, annotation_text="Beta=1.0")
    fig.add_hline(y=0.8, line_dash="dot", line_color="green", opacity=0.5, annotation_text="高相关")

    return fig


def hex_to_rgb(hex_color: str) -> str:
    """将十六进制颜色转换为RGB格式"""
    hex_color = hex_color.lstrip('#')
    return ', '.join(str(int(hex_color[i:i+2], 16)) for i in (0, 2, 4))
