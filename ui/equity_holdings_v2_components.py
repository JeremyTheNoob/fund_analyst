"""
深度持仓穿透分析 UI 组件
负责：渲染四大分析模块的可视化图表
"""

from __future__ import annotations
import logging
from typing import Dict, Any

import streamlit as st
import plotly.graph_objects as go

logger = logging.getLogger(__name__)


# ============================================================
# 渲染函数：资产配置趋势图
# ============================================================

def render_asset_trend_chart(asset_trend_data: Dict[str, Any]):
    """
    渲染资产配置演变趋势图（百分比堆叠面积图）

    Args:
        asset_trend_data: 模块1的分析结果
    """
    if not asset_trend_data or not asset_trend_data.get("chart_data"):
        st.warning("资产配置趋势数据不足")
        return

    chart_data = asset_trend_data["chart_data"]

    st.markdown("### 📊 资产配置演变趋势")

    # 绘制堆叠面积图
    fig = go.Figure()

    for series in chart_data["series"]:
        fig.add_trace(go.Scatter(
            x=chart_data["x"],
            y=series["data"],
            mode="lines",
            name=series["name"],
            stackgroup="one",
            fill="tonexty",
            line=dict(color=series["color"], width=1.5),
            hovertemplate="%{fullData.name}: %{y:.2f}%<extra></extra>"
        ))

    fig.update_layout(
        xaxis_title="日期",
        yaxis_title="仓位占比 (%)",
        yaxis=dict(range=[0, 100]),
        height=350,
        margin=dict(t=40, b=30, l=50, r=30),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig, use_container_width=True)

    # 显示关键指标
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("股票仓位中枢", f"{asset_trend_data['stock_avg_ratio']}%")
    with col2:
        st.metric("股票仓位波动", f"±{asset_trend_data['stock_std_ratio']}%")
    with col3:
        st.metric("现金仓位分位", f"{asset_trend_data['cash_percentile']}%")
    with col4:
        st.metric("现金仓位状态", asset_trend_data["cash_percentile_desc"])

    # 风格漂移预警
    if asset_trend_data["style_drift_warning"]:
        st.warning(asset_trend_data["style_drift_warning"])


# ============================================================
# 渲染函数：持仓变化热力图
# ============================================================

def render_holdings_heatmap(holdings_evolution_data: Dict[str, Any]):
    """
    渲染前十大持仓股历史变化热力图

    Args:
        holdings_evolution_data: 模块2的分析结果
    """
    if not holdings_evolution_data or not holdings_evolution_data.get("heatmap_data"):
        st.warning("持仓演变数据不足")
        return

    heatmap_data = holdings_evolution_data["heatmap_data"]

    st.markdown("### 📈 前十大持仓股历史变化")

    # 绘制热力图
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data["z"],
        x=heatmap_data["x"],
        y=heatmap_data["y"],
        colorscale=heatmap_data["colors"],
        zmid=0,
        text=heatmap_data["annotations"],
        texttemplate="%{text}",
        textfont={"size": 11},
        hovertemplate="股票: %{y}<br>季度: %{x}<br>占比: %{z:.2f}%<extra></extra>"
    ))

    fig.update_layout(
        xaxis_title="季度",
        yaxis_title="股票名称",
        height=450,
        margin=dict(t=40, b=30, l=100, r=30)
    )

    st.plotly_chart(fig, use_container_width=True)

    # 显示关键指标
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("持仓留存率", f"{holdings_evolution_data['retention_rate']}%")
    with col2:
        st.metric("平均持仓周期", f"{holdings_evolution_data['avg_holding_periods']} 季度")
    with col3:
        st.metric("换手率", f"{holdings_evolution_data['turnover_rate']}%")
    with col4:
        st.metric("风格标签", holdings_evolution_data["style_tag"])


# ============================================================
# 渲染函数：交易能力评估
# ============================================================

def render_trading_ability(trading_ability_data: Dict[str, Any]):
    """
    渲染交易能力评估图表（条件渲染）

    Args:
        trading_ability_data: 模块3的分析结果
    """
    if not trading_ability_data:
        return

    # 检查数据是否充足
    if "数据不足" in trading_ability_data.get("ability_tag", ""):
        st.info("⚠️ 交易能力评估需要至少4个季度的持仓数据")
        return

    st.markdown("### 💹 交易能力评估（最近4季度）")

    chart_data = trading_ability_data.get("chart_data", {})

    if not chart_data:
        st.warning("交易能力数据不足")
        return

    # 绘制分组柱状图
    fig = go.Figure()

    # 正收益柱
    fig.add_trace(go.Bar(
        x=chart_data["x"],
        y=chart_data["positive_returns"],
        name="正收益比例",
        marker_color="#27ae60",
        text=chart_data["positive_returns"],
        texttemplate="%{y:.1f}%",
        textposition="outside"
    ))

    # 负收益柱
    fig.add_trace(go.Bar(
        x=chart_data["x"],
        y=chart_data["negative_returns"],
        name="负收益比例",
        marker_color="#e74c3c",
        text=chart_data["negative_returns"],
        texttemplate="%{y:.1f}%",
        textposition="outside"
    ))

    fig.update_layout(
        barmode="stack",
        xaxis_title="交易类型",
        yaxis_title="比例 (%)",
        yaxis=dict(range=[0, 100]),
        height=350,
        margin=dict(t=40, b=30, l=50, r=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig, use_container_width=True)

    # 显示关键指标
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("抄底成功率", f"{trading_ability_data['buy_success_rate']}%")
    with col2:
        st.metric("逃顶成功率", f"{trading_ability_data['sell_success_rate']}%")
    with col3:
        st.metric("能力评分", f"{trading_ability_data['ability_score']}/100")
    with col4:
        st.metric("能力标签", trading_ability_data["ability_tag"])

    # 交易详情（可折叠）
    with st.expander("📋 交易详情"):
        trades_detail = trading_ability_data.get("trades_detail", [])
        if trades_detail:
            trades_df = {
                "季度": [t["period"] for t in trades_detail],
                "类型": ["买入" if t["type"] == "buy" else "卖出" for t in trades_detail],
                "股票名称": [t["name"] for t in trades_detail],
                "占比 (%)": [t["ratio"] for t in trades_detail],
                "30天收益率 (%)": [t["return_30d"] for t in trades_detail]
            }
            st.dataframe(trades_df, use_container_width=True)
        else:
            st.write("无交易数据")


# ============================================================
# 渲染函数：估值分析与压力测试
# ============================================================

def render_valuation_stress(valuation_stress_data: Dict[str, Any]):
    """
    渲染估值分析与压力测试图表

    Args:
        valuation_stress_data: 模块4的分析结果
    """
    if not valuation_stress_data:
        return

    st.markdown("### 💎 估值分析与风险预警")

    # --- 估值散点图 ---
    valuation_chart = valuation_stress_data.get("valuation_chart", {})
    if valuation_chart:
        # 绘制散点图
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=valuation_chart["x"],
            y=valuation_chart["y"],
            mode="markers+text",
            marker=dict(
                size=valuation_chart["marker"]["size"],
                color=valuation_chart["marker"]["color"],
                line=dict(width=1, color="white")
            ),
            text=valuation_chart["text"],
            textposition="top center",
            textfont=dict(size=10),
            hovertemplate="股票: %{text}<br>PE: %{x:.2f}<br>占比: %{y:.2f}%<extra></extra>"
        ))

        fig.update_layout(
            xaxis_title="PE（市盈率）",
            yaxis_title="占净值比例 (%)",
            height=350,
            margin=dict(t=40, b=30, l=50, r=30),
            hovermode="closest"
        )

        st.plotly_chart(fig, use_container_width=True)

        # 颜色图例
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown('<span style="color:#e74c3c">●</span> 高估', unsafe_allow_html=True)
        with col2:
            st.markdown('<span style="color:#f39c12">●</span> 合理', unsafe_allow_html=True)
        with col3:
            st.markdown('<span style="color:#27ae60">●</span> 低估', unsafe_allow_html=True)

    # 风险预警
    risk_warning = valuation_stress_data.get("risk_warning", "")
    if risk_warning:
        st.warning(risk_warning)

    # --- 压力测试 ---
    st.markdown("#### 🧪 压力测试")

    # 行业压力测试
    industry_stress = valuation_stress_data.get("industry_stress", {})
    if industry_stress and industry_stress.get("industry_stress_chart"):
        industry_chart = industry_stress["industry_stress_chart"]

        st.markdown("**行业暴跌影响**")
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=industry_chart["x"],
            y=industry_chart["y"],
            marker_color=industry_chart["colors"],
            text=industry_chart["x"],
            texttemplate="%{text:.2f}%",
            textposition="outside",
            orientation="h"
        ))

        fig.update_layout(
            xaxis_title="对净值的影响 (%)",
            height=max(250, len(industry_chart["y"]) * 30),
            margin=dict(t=40, b=30, l=50, r=30)
        )

        st.plotly_chart(fig, use_container_width=True)

    # 全市场压力测试
    market_stress = valuation_stress_data.get("market_stress", {})
    if market_stress and market_stress.get("market_stress_chart"):
        market_chart = market_stress["market_stress_chart"]

        st.markdown("**全市场暴跌模拟**")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("预计净值跌幅", f"{market_chart['value']}%")
        with col2:
            st.metric("相对市场", f"{market_stress['vs_market']}%")
        with col3:
            st.metric("防御强度", market_stress['defensive_strength'])

    # 风险等级汇总
    risk_level = valuation_stress_data.get("industry_stress", {}).get("risk_level", "")
    if risk_level:
        if risk_level == "极高":
            st.error(f"🔴 风险等级：{risk_level}")
        elif risk_level == "高":
            st.warning(f"🟠 风险等级：{risk_level}")
        elif risk_level == "中等":
            st.info(f"🟡 风险等级：{risk_level}")
        else:
            st.success(f"🟢 风险等级：{risk_level}")


# ============================================================
# 主渲染函数
# ============================================================

def render_deep_holdings_ui(analysis_result: Dict[str, Any]):
    """
    渲染深度持仓分析主界面

    Args:
        analysis_result: 深度持仓分析结果，包含四大模块数据
    """
    if not analysis_result:
        st.warning("深度持仓分析数据不足")
        return

    st.divider()
    st.markdown("## 🔍 深度持仓穿透分析")
    st.markdown("*数据范围：最多5年（20个季度）*")

    # 模块1：资产配置趋势
    asset_trend = analysis_result.get("asset_trend", {})
    if asset_trend:
        render_asset_trend_chart(asset_trend)
        st.markdown("")

    # 模块2：持仓演变
    holdings_evolution = analysis_result.get("holdings_evolution", {})
    if holdings_evolution:
        render_holdings_heatmap(holdings_evolution)
        st.markdown("")

    # 模块3：交易能力评估（条件渲染）
    trading_ability = analysis_result.get("trading_ability", {})
    if trading_ability:
        render_trading_ability(trading_ability)
        st.markdown("")

    # 模块4：估值分析与压力测试
    valuation_stress = analysis_result.get("valuation_stress", {})
    if valuation_stress:
        render_valuation_stress(valuation_stress)
