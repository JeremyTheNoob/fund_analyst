"""
纯债基金 UI 展示组件
=====================

包含：
  1. 三重识别结果卡片
  2. 券种结构甜甜圈图
  3. 信用质量仪表盘（WACS + 评级分布）
  4. 集中度分析（静态HHI + 动态轨迹）
  5. 久期体系仪表盘（类型匹配 + 择时评分）
  6. 综合评分卡（3维底层 + 1维久期）
  7. 压力测试情景表
  8. 风险收益分布图（信用 × 久期 × 气泡=Carry）
  9. 宏观插件展示
 10. 四段式大白话结论

约束：本文件只能调用 Streamlit，不含数据获取逻辑
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


# ============================================================
# 🎨 颜色常量
# ============================================================
COLOR_RATE = '#3498db'       # 利率债：蓝色
COLOR_CREDIT = '#e74c3c'     # 信用债：红色
COLOR_NCD = '#2ecc71'        # 同业存单：绿色
COLOR_CONVERT = '#f39c12'    # 可转债：橙色
COLOR_OTHER = '#95a5a6'      # 其他：灰色

GRADE_COLOR = {
    'A+': '#27ae60', 'A': '#2ecc71', 'B': '#f39c12',
    'C': '#e67e22', 'D': '#e74c3c',
}


# ============================================================
# 1. 三重识别结果卡片
# ============================================================

def render_identity_card(identity: dict):
    """渲染纯债基金三重识别结果"""
    is_pure = identity.get('is_pure_bond', False)

    if is_pure:
        st.success("✅ **纯债基金识别通过** — 已启用纯债深度分析模式")
    else:
        st.warning("⚠️ **非标准纯债基金** — 以通用债券模型分析（部分指标可能偏差）")

    col1, col2, col3 = st.columns(3)

    with col1:
        f1_pass = identity.get('pass_filter1', False)
        f1_ratio = identity.get('filter1_bond_ratio', 0)
        status = "✅" if f1_pass else "❌"
        st.metric(
            label=f"{status} Filter1 债券占比",
            value=f"{f1_ratio*100:.1f}%",
            delta="≥90% ✓" if f1_pass else "需≥90%",
            delta_color="normal" if f1_pass else "inverse",
        )

    with col2:
        f2_pass = identity.get('pass_filter2', False)
        stock_r = identity.get('filter2_stock_ratio', 0)
        conv_r = identity.get('filter2_convert_ratio', 0)
        status = "✅" if f2_pass else "❌"
        st.metric(
            label=f"{status} Filter2 股票/可转债",
            value=f"股{stock_r*100:.1f}% / 转{conv_r*100:.1f}%",
            delta="通过" if f2_pass else "股需=0, 转需<5%",
            delta_color="normal" if f2_pass else "inverse",
        )

    with col3:
        f3_pass = identity.get('pass_filter3', True)
        bond_corr = identity.get('filter3_bond_corr')
        eq_corr = identity.get('filter3_equity_corr')
        status = "✅" if f3_pass else "❌"
        corr_text = (f"债券相关={bond_corr:.2f}" if bond_corr else "相关性计算中")
        st.metric(
            label=f"{status} Filter3 相关性",
            value=corr_text,
            delta="通过" if f3_pass else "与中债需>0.8",
            delta_color="normal" if f3_pass else "inverse",
        )

    # 备注
    notes = identity.get('notes', [])
    if notes:
        with st.expander("查看识别细节", expanded=False):
            for note in notes:
                st.caption(f"• {note}")


# ============================================================
# 2. 券种结构甜甜圈图
# ============================================================

def render_asset_structure(asset_structure: dict):
    """渲染券种结构分析"""
    st.markdown("### 📊 券种结构穿透")

    type_dist = asset_structure.get('type_distribution', [])
    if not type_dist:
        st.info("债券持仓数据不足，无法展示券种结构")
        return

    col_chart, col_metrics = st.columns([3, 2])

    with col_chart:
        # 甜甜圈图
        labels = [t['type'] for t in type_dist]
        values = [t['weight'] for t in type_dist]
        colors = [t['color'] for t in type_dist]

        fig = go.Figure(data=[go.Pie(
            labels=labels, values=values,
            hole=0.55, marker_colors=colors,
            textinfo='label+percent',
            hovertemplate='%{label}<br>占净值比例: %{value:.2f}%<extra></extra>',
        )])
        fig.update_layout(
            title='债券类型分布',
            showlegend=True,
            height=350,
            margin=dict(l=10, r=10, t=40, b=10),
        )
        # 中心文字：总占比
        total_w = asset_structure.get('total_weight', 0)
        fig.add_annotation(
            text=f"总占净值<br><b>{total_w:.1f}%</b>",
            x=0.5, y=0.5, font_size=14,
            showarrow=False, xref='paper', yref='paper',
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_metrics:
        # 三大核心比率
        rate_r = asset_structure.get('rate_ratio', 0)
        credit_r = asset_structure.get('credit_ratio', 0)
        ncd_r = asset_structure.get('ncd_ratio', 0)
        leverage = asset_structure.get('leverage_ratio', 1.0)

        st.markdown("**核心比率**")
        st.metric("🔵 利率敏感度", f"{rate_r*100:.1f}%",
                  help="利率债市值 / 总债券持仓市值")
        st.metric("🔴 信用暴露度", f"{credit_r*100:.1f}%",
                  help="信用债市值 / 总债券持仓市值")
        st.metric("🟢 现金替代率", f"{ncd_r*100:.1f}%",
                  help="同业存单 / 总资产（>50%为大号货币基金）")
        st.metric("⚖️ 杠杆率", f"{leverage*100:.0f}%",
                  delta="有杠杆" if leverage > 1.1 else "无杠杆",
                  delta_color="inverse" if leverage > 1.3 else "normal")

        # 存单警告
        if asset_structure.get('is_ncd_heavy'):
            st.warning("💰 同业存单占比>50%，本质上是大号货币基金，Alpha空间有限")

        # 结构得分
        q_score = asset_structure.get('quality_score', 70)
        st.metric("券种结构得分（S_struct）", f"{q_score:.0f}分")


# ============================================================
# 3. 信用质量仪表盘
# ============================================================

def render_credit_quality(credit_quality: dict):
    """渲染信用资质分析"""
    st.markdown("### 💳 信用资质分析")

    col_wacs, col_dist, col_score = st.columns([1, 2, 1])

    with col_wacs:
        wacs = credit_quality.get('wacs', 80)
        wacs_rating = credit_quality.get('wacs_rating', 'AA+')
        color = '#27ae60' if wacs >= 90 else ('#f39c12' if wacs >= 70 else '#e74c3c')

        st.markdown(f"""
        <div style="text-align:center; padding:20px; background:#f8f9fa;
                    border-radius:10px; border-left:4px solid {color};">
            <div style="font-size:2.5em; font-weight:bold; color:{color};">{wacs:.0f}</div>
            <div style="color:#666;">WACS综合评分</div>
            <div style="font-size:1.5em; font-weight:bold; color:{color}; margin-top:8px;">{wacs_rating}</div>
            <div style="color:#888; font-size:0.8em;">加权平均信用等级</div>
        </div>
        """, unsafe_allow_html=True)

        # 信用下沉系数
        sinking = credit_quality.get('sinking_ratio', 0)
        if credit_quality.get('is_credit_sinking'):
            st.warning(f"⚡ 信用下沉：AA+以下占比 {sinking*100:.0f}%")
        else:
            st.success(f"✅ 信用质量优良，AA+以下仅 {sinking*100:.0f}%")

    with col_dist:
        # 评级分布横向条形图
        rating_breakdown = credit_quality.get('rating_breakdown', {})
        if rating_breakdown:
            ratings = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', '未评级']
            present = {r: rating_breakdown.get(r, 0) * 100 for r in ratings
                       if r in rating_breakdown}

            if present:
                fig = go.Figure()
                colors_bar = {
                    'AAA': '#27ae60', 'AA+': '#2ecc71', 'AA': '#f39c12',
                    'AA-': '#e67e22', 'A+': '#e74c3c', 'A': '#c0392b',
                    '未评级': '#95a5a6',
                }
                for r, v in present.items():
                    fig.add_trace(go.Bar(
                        name=r, y=['评级分布'], x=[v],
                        orientation='h',
                        marker_color=colors_bar.get(r, '#95a5a6'),
                        text=f"{v:.1f}%", textposition='inside',
                    ))
                fig.update_layout(
                    barmode='stack', height=120,
                    showlegend=True, legend=dict(orientation='h', y=-0.5),
                    margin=dict(l=10, r=10, t=10, b=40),
                    xaxis=dict(range=[0, 100], ticksuffix='%'),
                )
                st.plotly_chart(fig, use_container_width=True)

    with col_score:
        s_credit = credit_quality.get('credit_score', 80)
        score_color = '#27ae60' if s_credit >= 90 else ('#f39c12' if s_credit >= 70 else '#e74c3c')
        st.markdown(f"""
        <div style="text-align:center; padding:20px; background:#f8f9fa;
                    border-radius:10px; border-top:4px solid {score_color};">
            <div style="font-size:2em; font-weight:bold; color:{score_color};">{s_credit:.0f}</div>
            <div style="color:#666;">信用资质得分</div>
            <div style="color:#888; font-size:0.8em;">S_credit (权重40%)</div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================
# 4. 集中度分析（静态+动态HHI）
# ============================================================

def render_concentration(concentration: dict):
    """渲染持仓集中度分析"""
    st.markdown("### 🎯 持仓集中度分析")

    col1, col2, col3 = st.columns(3)
    hhi = concentration.get('static_hhi', 500)
    top5 = concentration.get('top5_ratio', 0.2)
    top10 = concentration.get('top10_ratio', 0.35)
    hhi_level = concentration.get('hhi_level', 'low')
    hhi_drift = concentration.get('hhi_drift', 0)
    hhi_trend = concentration.get('hhi_trend', 'stable')

    hhi_color = {'low': '#27ae60', 'medium': '#f39c12', 'high': '#e74c3c'}.get(hhi_level, '#95a5a6')
    trend_emoji = {'rising': '📈', 'falling': '📉', 'stable': '➡️'}.get(hhi_trend, '➡️')

    with col1:
        st.markdown(f"""
        <div style="text-align:center; padding:15px; background:#f8f9fa;
                    border-radius:10px; border-left:4px solid {hhi_color};">
            <div style="font-size:2em; font-weight:bold; color:{hhi_color};">{hhi:.0f}</div>
            <div style="color:#666; font-size:0.85em;">静态HHI指数</div>
            <div style="color:#888; font-size:0.75em;">
                {'极度分散' if hhi<500 else '适度集中' if hhi<1500 else '高度集中'}
                {trend_emoji}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.metric("前五大重仓", f"{top5*100:.1f}%",
                  delta="分散" if top5 < 0.15 else ("适中" if top5 < 0.30 else "集中"),
                  delta_color="normal" if top5 < 0.30 else "inverse")
        st.metric("前十大重仓", f"{top10*100:.1f}%")

    with col3:
        conc_score = concentration.get('conc_score_final', 80)
        adj = concentration.get('dynamic_adjustment', 0)
        st.metric("集中度得分（S_conc）", f"{conc_score:.0f}分",
                  delta=f"动态调整{adj:+.0f}分" if adj != 0 else "无调整")

        if hhi_drift > 0.30:
            st.warning(f"⚠️ HHI偏离历史均值{hhi_drift*100:.0f}%，经理风格趋于激进")
        elif hhi_trend == 'falling':
            st.success("✅ HHI持续降低，持仓更加分散")

    # 动态HHI折线图
    dynamic_hhi = concentration.get('dynamic_hhi', [])
    if len(dynamic_hhi) >= 3:
        df_hhi = pd.DataFrame(dynamic_hhi)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_hhi['period'], y=df_hhi['hhi'],
            mode='lines+markers+text',
            text=[f"{h:.0f}" for h in df_hhi['hhi']],
            textposition='top center',
            line=dict(color=hhi_color, width=2),
            marker=dict(size=8, color=hhi_color),
            fill='tozeroy', fillcolor=f'rgba(52,152,219,0.1)',
        ))
        # 参考线
        avg_hhi = np.mean(df_hhi['hhi'])
        fig.add_hline(y=avg_hhi, line_dash='dash', line_color='gray',
                      annotation_text=f'均值={avg_hhi:.0f}', annotation_position='right')
        fig.add_hline(y=500, line_dash='dot', line_color='green',
                      annotation_text='低风险线500')
        fig.add_hline(y=1500, line_dash='dot', line_color='red',
                      annotation_text='高风险线1500')
        fig.update_layout(
            title='📈 动态HHI轨迹（历史集中度趋势）',
            height=280, margin=dict(l=40, r=80, t=40, b=20),
            xaxis_title='报告期', yaxis_title='HHI指数',
        )
        st.plotly_chart(fig, use_container_width=True)

    # 发行人集中度表格
    issuer_conc = concentration.get('issuer_concentration', [])
    if issuer_conc:
        with st.expander("🏢 发行人集中度（同一母公司合并）", expanded=False):
            df_issuer = pd.DataFrame(issuer_conc)
            df_issuer['ratio'] = (df_issuer['ratio'] * 100).round(2)
            df_issuer.columns = ['发行人', '占净值比例(%)', '占比']
            st.dataframe(df_issuer[['发行人', '占净值比例(%)', '占比']],
                        use_container_width=True, hide_index=True)


# ============================================================
# 5. 久期体系仪表盘
# ============================================================

def render_duration_system(duration_system: dict):
    """渲染久期体系分析"""
    st.markdown("### ⏱️ 久期管理分析")

    duration = duration_system.get('duration', 2.0)
    dur_grade = duration_system.get('duration_grade', 'B')
    dur_score = duration_system.get('duration_score', 80)
    timing_score = duration_system.get('timing_score', 0)
    drift_score = duration_system.get('drift_score', 80)
    std_range = duration_system.get('standard_range', (1.0, 5.0))
    is_in_std = duration_system.get('is_in_standard', True)
    r2 = duration_system.get('duration_r_squared', 0)
    stress_10bp = duration_system.get('stress_10bp', -0.2)
    stress_30bp = duration_system.get('stress_30bp', -0.6)

    grade_color = GRADE_COLOR.get(dur_grade, '#95a5a6')

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div style="text-align:center; padding:15px; background:#f8f9fa;
                    border-radius:10px; border-top:4px solid {grade_color};">
            <div style="font-size:2.5em; font-weight:bold; color:#2c3e50;">{duration:.2f}</div>
            <div style="color:#666;">有效久期（年）</div>
            <div style="font-size:1.5em; color:{grade_color}; font-weight:bold;">{dur_grade}级</div>
            <div style="color:#888; font-size:0.75em;">R²={r2:.2f}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        # 标准区间匹配进度条
        d_min, d_max = std_range
        if is_in_std:
            match_pct = 100
            bar_color = '#27ae60'
            match_text = '✅ 在标准区间内'
        else:
            # 偏离程度
            if duration < d_min:
                match_pct = max(0, int(duration / d_min * 100))
            else:
                match_pct = max(0, int(d_max / duration * 100))
            bar_color = '#e74c3c'
            match_text = '⚠️ 超出标准区间'

        st.markdown(f"""
        <div style="padding:15px; background:#f8f9fa; border-radius:10px;">
            <div style="color:#666; font-size:0.85em; margin-bottom:5px;">
                类型匹配（{duration_system.get('fund_subtype', '')}标准：{d_min}-{d_max}年）
            </div>
            <div style="background:#e0e0e0; border-radius:5px; height:20px;">
                <div style="background:{bar_color}; width:{match_pct}%; height:100%;
                            border-radius:5px; display:flex; align-items:center;
                            justify-content:center; color:white; font-size:0.8em;">
                    {match_pct}%
                </div>
            </div>
            <div style="color:#888; font-size:0.8em; margin-top:5px;">{match_text}</div>
            <div style="font-size:1.2em; font-weight:bold; color:#2c3e50; margin-top:5px;">
                基础分：{drift_score:.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        timing_color = '#27ae60' if timing_score > 0 else ('#e74c3c' if timing_score < 0 else '#95a5a6')
        timing_sign = '▲' if timing_score > 0 else ('▼' if timing_score < 0 else '→')
        st.markdown(f"""
        <div style="text-align:center; padding:15px; background:#f8f9fa; border-radius:10px;">
            <div style="font-size:2em; font-weight:bold; color:{timing_color};">
                {timing_sign} {timing_score:+.0f}
            </div>
            <div style="color:#666;">择时加分</div>
            <div style="color:#888; font-size:0.8em;">综合得分：{dur_score:.0f}分</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        # 压力测试快速看
        st.markdown(f"""
        <div style="padding:15px; background:#f8f9fa; border-radius:10px;">
            <div style="color:#666; font-weight:bold; font-size:0.9em;">压力测试</div>
            <div style="margin-top:8px;">
                <span style="color:#888;">+10BP</span>
                <span style="float:right; color:#e74c3c; font-weight:bold;">
                    {stress_10bp:.2f}%
                </span>
            </div>
            <div style="margin-top:5px;">
                <span style="color:#888;">+30BP</span>
                <span style="float:right; color:#e74c3c; font-weight:bold;">
                    {stress_30bp:.2f}%
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # 解读
    interp = duration_system.get('interpretation', '')
    if interp:
        st.caption(interp)


# ============================================================
# 6. 综合评分卡
# ============================================================

def render_pure_bond_score_card(scores: dict):
    """渲染纯债基金综合评分卡"""
    st.markdown("### 🏆 综合评分")

    one_vote_veto = scores.get('one_vote_veto', False)
    veto_reason = scores.get('veto_reason', '')
    grade = scores.get('grade', 'B')
    total_score = scores.get('total_score', 70)
    fund_label = scores.get('fund_label', '')
    risk_r = scores.get('composite_risk_r', 0)

    if one_vote_veto:
        st.error(f"❌ **一票否决** · 综合评级：D级 | {veto_reason}")

    grade_color = GRADE_COLOR.get(grade, '#95a5a6')

    col_grade, col_breakdown, col_risk = st.columns([1, 2, 1])

    with col_grade:
        st.markdown(f"""
        <div style="text-align:center; padding:25px; background:linear-gradient(135deg, {grade_color}20, {grade_color}05);
                    border-radius:15px; border:2px solid {grade_color};">
            <div style="font-size:3.5em; font-weight:bold; color:{grade_color};">{grade}</div>
            <div style="font-size:1.5em; color:#2c3e50; font-weight:bold;">{total_score:.0f}分</div>
            <div style="color:#666; font-size:0.85em; margin-top:5px;">{fund_label}</div>
        </div>
        """, unsafe_allow_html=True)

    with col_breakdown:
        # 三维底层资产 + 久期评分
        s_credit = scores.get('s_credit', 80)
        s_conc = scores.get('s_conc', 80)
        s_struct = scores.get('s_struct', 80)
        score_quality = scores.get('score_quality', 80)
        s_duration = scores.get('s_duration', 80)
        dur_grade = scores.get('duration_grade', 'B')

        # 评分进度条
        def score_bar(label, val, weight_pct, max_val=100):
            bar_width = min(100, int(val / max_val * 100))
            color = '#27ae60' if val >= 80 else ('#f39c12' if val >= 60 else '#e74c3c')
            return f"""
            <div style="margin-bottom:8px;">
                <div style="display:flex; justify-content:space-between; font-size:0.85em;">
                    <span style="color:#666;">{label}（权重{weight_pct}%）</span>
                    <span style="font-weight:bold; color:{color};">{val:.0f}分</span>
                </div>
                <div style="background:#e0e0e0; border-radius:4px; height:8px; margin-top:3px;">
                    <div style="background:{color}; width:{bar_width}%; height:100%; border-radius:4px;"></div>
                </div>
            </div>
            """

        bars = (
            score_bar('信用资质', s_credit, 40) +
            score_bar('持仓集中度', s_conc, 30) +
            score_bar('券种结构', s_struct, 30) +
            f'<div style="border-top:1px solid #eee; margin:8px 0; padding-top:5px; font-size:0.8em; color:#888;">底层资产质量：{score_quality:.0f}分</div>' +
            score_bar(f'久期管理（{dur_grade}级）', s_duration, 0, max_val=120)
        )
        st.markdown(bars, unsafe_allow_html=True)

    with col_risk:
        st.markdown(f"""
        <div style="padding:15px; background:#f8f9fa; border-radius:10px;">
            <div style="color:#666; font-size:0.85em; font-weight:bold;">综合风险指数 R</div>
            <div style="font-size:1.8em; font-weight:bold; color:{'#e74c3c' if risk_r>500000 else '#f39c12' if risk_r>100000 else '#27ae60'};">
                {risk_r:.0f}
            </div>
            <div style="color:#888; font-size:0.75em;">
                R = 久期 × HHI × (100-WACS)<br>
                {'❌ 极高风险' if risk_r>500000 else '⚠️ 中等风险' if risk_r>100000 else '✅ 风险可控'}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================
# 7. 压力测试情景表 + 风险收益分布图
# ============================================================

def render_stress_test_advanced(stress_results: dict, asset_structure: dict = None):
    """渲染多因子压力测试"""
    st.markdown("### 📐 多因子压力测试")

    scenarios = stress_results.get('scenarios', [])
    worst = stress_results.get('worst_case', {})
    interp = stress_results.get('risk_interpretation', '')

    if scenarios:
        # 横向柱状图
        fig = go.Figure()
        names = [s['name'] for s in scenarios]
        totals = [s['total_impact_pct'] for s in scenarios]
        rate_impacts = [s['rate_impact_pct'] for s in scenarios]
        credit_impacts = [s['credit_impact_pct'] for s in scenarios]
        liquidity_impacts = [s.get('liquidity_impact_pct', 0) for s in scenarios]

        fig.add_trace(go.Bar(
            name='利率冲击', y=names, x=rate_impacts,
            orientation='h', marker_color='#3498db',
        ))
        fig.add_trace(go.Bar(
            name='信用冲击', y=names, x=credit_impacts,
            orientation='h', marker_color='#e74c3c',
        ))
        fig.add_trace(go.Bar(
            name='流动性冲击', y=names, x=liquidity_impacts,
            orientation='h', marker_color='#f39c12',
        ))

        fig.update_layout(
            barmode='stack',
            title='情景压力测试：各分项冲击分解',
            xaxis=dict(title='预估净值变化（%）', ticksuffix='%'),
            height=320, margin=dict(l=180, r=20, t=40, b=20),
            legend=dict(orientation='h', y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)

        if interp:
            st.caption(interp)

    # 风险收益分布图（信用 × 久期 × Carry）
    if asset_structure:
        _render_risk_return_scatter(asset_structure)


def _render_risk_return_scatter(asset_structure: dict):
    """
    风险收益分布图（信用-久期-Carry气泡图）
    X轴: 信用质量（WACS）→ 越低信用越差
    Y轴: 用结构代理久期暴露
    气泡大小: 年化Carry估计
    """
    # 对于单只基金，展示债券类型的分布象限
    type_dist = asset_structure.get('type_distribution', [])
    if not type_dist:
        return

    st.markdown("#### 🗺️ 债券类型风险-收益象限")

    # 债券类型默认信用质量和收益率参考值（粗略估算）
    type_params = {
        '国债':         {'credit_score': 100, 'yield_approx': 2.3, 'duration_risk': 8},
        '政金债':       {'credit_score': 98,  'yield_approx': 2.5, 'duration_risk': 7},
        '同业存单':     {'credit_score': 95,  'yield_approx': 2.2, 'duration_risk': 1},
        '城投债':       {'credit_score': 75,  'yield_approx': 3.5, 'duration_risk': 3},
        '金融债':       {'credit_score': 85,  'yield_approx': 2.8, 'duration_risk': 3},
        '地产债':       {'credit_score': 50,  'yield_approx': 5.0, 'duration_risk': 3},
        '产业债':       {'credit_score': 70,  'yield_approx': 4.0, 'duration_risk': 4},
        '企业债':       {'credit_score': 65,  'yield_approx': 4.5, 'duration_risk': 3},
        '可转债':       {'credit_score': 80,  'yield_approx': 2.0, 'duration_risk': 5},
        '资产支持证券': {'credit_score': 70,  'yield_approx': 3.8, 'duration_risk': 2},
        '其他':         {'credit_score': 60,  'yield_approx': 3.0, 'duration_risk': 3},
    }

    fig = go.Figure()
    for t in type_dist:
        btype = t['type']
        params = type_params.get(btype, {'credit_score': 60, 'yield_approx': 3.0, 'duration_risk': 3})
        weight = t.get('weight', 1)

        fig.add_trace(go.Scatter(
            x=[params['duration_risk']],
            y=[params['credit_score']],
            mode='markers+text',
            text=[btype],
            textposition='top center',
            marker=dict(
                size=max(15, weight * 0.8),
                color=t.get('color', '#95a5a6'),
                opacity=0.8,
                line=dict(width=2, color='white'),
            ),
            name=f"{btype}({weight:.1f}%)",
            hovertemplate=(
                f"<b>{btype}</b><br>"
                f"持仓权重: {weight:.1f}%<br>"
                f"利率风险（久期代理）: {params['duration_risk']}年<br>"
                f"信用质量参考: {params['credit_score']}<br>"
                f"收益率参考: {params['yield_approx']:.1f}%<extra></extra>"
            )
        ))

    # 象限分割线
    fig.add_hline(y=85, line_dash='dash', line_color='rgba(0,0,0,0.2)')
    fig.add_vline(x=4, line_dash='dash', line_color='rgba(0,0,0,0.2)')

    # 象限标注
    for x, y, text in [
        (1.5, 97, '现金替代型'), (7, 97, '利率择时型'),
        (1.5, 55, '票息增强型'), (7, 55, '激进进攻型'),
    ]:
        fig.add_annotation(x=x, y=y, text=text, showarrow=False,
                          font=dict(size=10, color='rgba(0,0,0,0.3)'))

    fig.update_layout(
        title='债券类型风险-收益分布图<br><sup>X轴:利率风险 | Y轴:信用质量 | 气泡大小:持仓权重</sup>',
        xaxis=dict(title='利率敏感度（久期代理）', range=[0, 10]),
        yaxis=dict(title='信用质量（WACS参考）', range=[30, 110]),
        height=380, showlegend=True,
        legend=dict(orientation='h', y=-0.25, font_size=10),
        margin=dict(l=40, r=20, t=70, b=80),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("⚠️ 注：图中利率风险和信用质量均为典型值参考，非实测数据")


# ============================================================
# 8. 宏观环境插件展示
# ============================================================

def render_macro_plugin(macro_plugin: dict):
    """渲染宏观分析插件"""
    if not macro_plugin or not macro_plugin.get('has_macro_content'):
        return

    st.markdown("### 🌐 宏观环境穿透分析")

    macro_risk = macro_plugin.get('macro_risk_level', '🟢 低')
    st.markdown(f"**整体宏观风险：{macro_risk}**")

    # 利率宏观分析
    rate_text = macro_plugin.get('rate_macro_text', '')
    if rate_text:
        with st.expander("📊 宏观利率分析", expanded=True):
            for line in rate_text.split('\n'):
                if line.strip():
                    st.markdown(line)

    # 信用行业分析
    credit_text = macro_plugin.get('credit_industry_text', '')
    if credit_text:
        with st.expander("💼 行业信用分析", expanded=True):
            for line in credit_text.split('\n'):
                if line.strip():
                    st.markdown(line)

    # 发行人预警
    alerts = macro_plugin.get('issuer_alerts', [])
    if alerts:
        with st.expander(f"🔍 核心持仓发行人预警（{len(alerts)}条）", expanded=True):
            for alert in alerts:
                level = alert.get('alert_level', 'low')
                if level == 'high':
                    st.error(f"{alert['emoji']} {alert['message']}")
                elif level == 'medium':
                    st.warning(f"{alert['emoji']} {alert['message']}")
                else:
                    st.info(f"{alert['emoji']} {alert['message']}")


# ============================================================
# 9. 四段式大白话结论
# ============================================================

def render_pure_bond_conclusion(translation: dict):
    """渲染纯债基金四段式结论报告"""
    st.markdown("### 💬 综合诊断报告")

    one_vote = translation.get('one_vote_veto', False)
    grade = translation.get('overall_grade', 'B')
    grade_color = GRADE_COLOR.get(grade, '#95a5a6')

    # 顶部：标题 + 标签
    st.markdown(f"""
    <div style="padding:20px; background:linear-gradient(135deg, {grade_color}15, {grade_color}03);
                border-radius:12px; border-left:5px solid {grade_color}; margin-bottom:20px;">
        <div style="font-size:1.3em; font-weight:bold; color:#2c3e50;">
            {translation.get('headline', '')}
        </div>
        <div style="color:#666; font-size:0.9em; margin-top:8px;">
            {translation.get('scores_summary', '')}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 标签
    tags = translation.get('tags', [])
    if tags:
        tag_html = ' '.join([
            f'<span style="background:#f0f0f0; padding:4px 10px; border-radius:12px; '
            f'font-size:0.8em; margin:2px; display:inline-block;">{t}</span>'
            for t in tags
        ])
        st.markdown(tag_html, unsafe_allow_html=True)
        st.markdown("")

    # 四段内容
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 💰 钱从哪里来？")
        st.markdown(translation.get('income_source', ''), unsafe_allow_html=False)

        st.markdown("#### ⚠️ 坑在哪里？")
        st.markdown(translation.get('risk_check', ''), unsafe_allow_html=False)

    with col2:
        st.markdown("#### ⏱️ 久期专项")
        dur_highlight = translation.get('duration_highlight', '')
        dur_grade_note = translation.get('duration_grade_note', '')

        # A+级特殊展示
        if 'A+' in dur_highlight or '卓越' in dur_highlight:
            st.success(dur_highlight)
            if dur_grade_note:
                st.caption(dur_grade_note)
        elif '❌' in dur_highlight or 'D级' in dur_highlight:
            st.error(dur_highlight)
        else:
            st.info(dur_highlight)

        st.markdown("#### 📋 怎么买？")
        advice = translation.get('advice', '')
        if one_vote or grade == 'D':
            st.error(advice)
        elif grade == 'A':
            st.success(advice)
        else:
            st.markdown(advice)

    # 宏观备注（如有）
    macro_notes = translation.get('macro_notes', '')
    if macro_notes:
        with st.expander("🌐 宏观环境备注", expanded=False):
            st.markdown(macro_notes)


# ============================================================
# 🚀 主入口：纯债基金完整仪表盘
# ============================================================

def render_pure_bond_dashboard(model_results: dict, translation: dict = None, macro_plugin: dict = None):
    """
    纯债基金完整展示仪表盘

    Args:
        model_results: run_pure_bond_analysis()的结果
        translation: translate_pure_bond_results()的结果（可选）
        macro_plugin: get_macro_plugin()的结果（可选）
    """
    st.markdown("## 🔬 纯债基金深度诊断")
    st.markdown("""
    <small style="color:#666;">
    基于「3+2」分析体系：券种结构 / 信用资质 / 集中度 + 久期择时 / 三因子Alpha
    </small>
    """, unsafe_allow_html=True)

    # Tab布局
    tabs = st.tabs([
        "📋 基本识别",
        "🏗️ 券种结构",
        "💳 信用资质",
        "🎯 集中度",
        "⏱️ 久期管理",
        "📐 压力测试",
        "🏆 综合评分",
    ])

    with tabs[0]:
        identity = model_results.get('identity', {})
        render_identity_card(identity)

        data_quality = model_results.get('data_quality', {})
        if data_quality.get('warnings'):
            st.markdown("**数据质量提示：**")
            for w in data_quality['warnings']:
                st.caption(f"• {w}")

    with tabs[1]:
        render_asset_structure(model_results.get('asset_structure', {}))

    with tabs[2]:
        render_credit_quality(model_results.get('credit_quality', {}))

    with tabs[3]:
        render_concentration(model_results.get('concentration', {}))

    with tabs[4]:
        render_duration_system(model_results.get('duration_system', {}))

    with tabs[5]:
        stress_advanced = model_results.get('stress_test_advanced', {})
        asset_struct = model_results.get('asset_structure', {})
        if stress_advanced:
            render_stress_test_advanced(stress_advanced, asset_struct)
        else:
            # 回退到旧版压力测试
            stress_old = model_results.get('stress_test_results', {})
            if stress_old:
                st.info("使用基础压力测试（多因子版本数据不足）")
                scenarios = stress_old.get('scenarios', [])
                if scenarios:
                    df_s = pd.DataFrame(scenarios)
                    st.dataframe(df_s, use_container_width=True, hide_index=True)

    with tabs[6]:
        render_pure_bond_score_card(model_results.get('scores', {}))

    # 宏观插件（独立板块）
    if macro_plugin and macro_plugin.get('has_macro_content'):
        st.markdown("---")
        render_macro_plugin(macro_plugin)

    # 四段式结论
    if translation:
        st.markdown("---")
        render_pure_bond_conclusion(translation)
