"""
Streamlit 主界面入口 — fund_quant_v2
数据获取 → 标准化清洗 → 模块化计算 → 统一报告展示
"""

import streamlit as st
import plotly.graph_objects as go
import sys
import os
import logging

# 将项目根目录加入 PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志
from utils.common import setup_global_logging
setup_global_logging()
logger = logging.getLogger(__name__)

from pipeline import analyze_fund
from services.chart_interpretation import get_chart_interpretation

# ============================================================
# 页面配置
# ============================================================

# 预加载基金目录（提升首次校验速度）
@st.cache_resource(ttl=3600)  # 缓存1小时
def preload_fund_directory():
    """预加载基金目录"""
    try:
        from data_loader.fund_directory import get_fund_directory
        directory = get_fund_directory()
        stats = directory.get_statistics()
        logger.info(f"[preload_fund_directory] 预加载完成，共 {stats.get('total_funds', 0)} 只基金")
        return True
    except Exception as e:
        logger.warning(f"[preload_fund_directory] 预加载失败: {e}")
        return False

# 在Streamlit应用启动时预加载
preload_fund_directory()
st.set_page_config(
    page_title="净值风云 - 养基之前搜一搜",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================
# 主界面
# ============================================================

# 标题区域
st.markdown('<h1 style="text-align: center; color: #E84747;">净值风云</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; font-size: 18px; color: #666;">养基之前搜一搜</p>', unsafe_allow_html=True)

# 支持的基金类型（浅色小字）
st.markdown(
    '<p style="text-align: center; font-size: 12px; color: #999; margin-top: -10px; margin-bottom: 20px;">'
    '支持：权益类 · 纯债类 · 指数/ETF · 可转债/固收+</p>',
    unsafe_allow_html=True
)

# 输入区域（居中显示）
col1, col2, col3 = st.columns([2, 3, 2])
with col2:
    fund_code = st.text_input(
        "基金代码",
        placeholder="输入6位基金代码，如 000001",
        max_chars=6,
        label_visibility="collapsed"
    )

    col_a, col_b, col_c = st.columns([1, 1, 1])
    with col_a:
        st.empty()
    with col_b:
        analysis_range = st.radio(
            "分析周期",
            options=["自成立以来", "现任基金经理以来"],
            index=0,
            horizontal=True,
            label_visibility="collapsed"
        )
    with col_c:
        st.empty()

    # P1-配置建议细化：风险偏好选择器
    st.markdown("<div style='text-align: center; margin: 10px 0 5px 0; font-size: 14px; color: #666;'>您的风险偏好类型</div>", unsafe_allow_html=True)
    risk_preference = st.selectbox(
        "风险偏好",
        options=["保守型", "稳健型", "积极型"],
        index=1,
        label_visibility="collapsed",
        help="根据您的风险承受能力选择，系统将生成个性化配置建议",
        key="risk_preference"
    )

    st.markdown("<br>", unsafe_allow_html=True)  # 增加间距

    analyze_btn = st.button("🔍 开始分析", type="primary", use_container_width=True)

if not analyze_btn or not fund_code:
    st.stop()


# ============================================================
# 分析执行
# ============================================================
fund_code = fund_code.strip().zfill(6)

# 1. 基金代码格式校验（6位数字）
if not fund_code.isdigit() or len(fund_code) != 6:
    st.error("❌ 基金代码格式错误：请输入6位数字（如 000001）")
    st.stop()

# 2. 基金分析（使用 spinner 提示）
with st.spinner("🧠 净值大模型分析中，请稍候..."):
    try:
        # 根据用户选择确定分析参数
        if analysis_range == "自成立以来":
            years_to_load = 10
            since_inception = True
        else:
            years_to_load = 3
            since_inception = False

        # 执行分析
        report = analyze_fund(
            symbol=fund_code,
            years=years_to_load,
            since_inception=since_inception,
            verbose=True,
        )

    except Exception as e:
        st.error(f"❌ 基金分析失败：{str(e)}")
        st.stop()

# ============================================================
# 报告展示
# ============================================================

# 分隔线
st.divider()

if report is None:
    st.error("分析失败，请检查基金代码")
    st.stop()

text = report.text_report
basic = report.basic
fund_type = report.fund_type
charts = report.chart_data

# 移除旧的 Part 1（基础信息）和 Part 2（通用图表）
# 深度报告中已包含所需图表和解读

st.divider()

# ============================================================
# Part 3 + Part 4: 深度报告（权益类 vs 其他类型分路渲染）
# ============================================================

if report.equity_metrics:
    # ── 权益类：调用新深度报告生成器 ──────────────────────────
    from reporter.equity_report_writer import generate_equity_deep_report

    deep = generate_equity_deep_report(report)

    # 标题行
    st.markdown(deep["headline"])

    # ── 性格标签 ──
    if report.tags:
        tag_html = " ".join([
            f'<span style="background:#e8f4f8;border-radius:12px;padding:4px 10px;'
            f'margin:3px;display:inline-block;font-size:13px">{t}</span>'
            for t in report.tags
        ])
        st.markdown(tag_html, unsafe_allow_html=True)
        st.markdown("")  # 间距

    # ── 辅助：根据标记键取图表 Plotly figure ──────────────────
    def _get_chart_fig(chart_key: str):
        """根据 INSERT_CHART 标记 key 返回对应 Plotly figure，失败返回 None"""
        try:
            if chart_key == "CUM_RET":
                cum_data = charts.get("cumulative_return", {})
                if not (cum_data and "x" in cum_data and "series" in cum_data):
                    return None
                fig = go.Figure()
                for series in cum_data["series"]:
                    fig.add_trace(go.Scatter(
                        x=cum_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        line=dict(color=series.get("color", "#e74c3c"), width=2),
                    ))
                fig.update_layout(
                    title="累计收益率（%）",
                    yaxis_title="收益率 (%)",
                    hovermode="x unified", height=380,
                    margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",  # 横向图例
                        yanchor="bottom",
                        y=-0.25,  # 图例放在图表下方
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),  # 图例字体缩小
                    )
                )
                return fig

            elif chart_key == "EXCESS_ALPH":
                ex_data = charts.get("excess_return", {})
                if not (ex_data and "x" in ex_data and "series" in ex_data):
                    return None
                fig = go.Figure()
                for series in ex_data["series"]:
                    color = series.get("color", "#3498db")
                    if isinstance(color, list):
                        for i in range(len(series["data"]) - 1):
                            fig.add_trace(go.Scatter(
                                x=[ex_data["x"][i], ex_data["x"][i+1]],
                                y=[series["data"][i], series["data"][i+1]],
                                mode="lines",
                                name=series["name"] if i == 0 else "",
                                line=dict(color=color[i], width=2),
                                showlegend=(i == 0),
                            ))
                    else:
                        fig.add_trace(go.Scatter(
                            x=ex_data["x"], y=series["data"],
                            mode="lines", name=series["name"],
                            line=dict(color=color, width=2),
                        ))
                fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="超额=0")
                fig.update_layout(
                    title="超额收益曲线（%）",
                    yaxis_title="超额收益 (%)",
                    height=350, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "DRAWDOWN":
                dd_data = charts.get("drawdown", {})
                if not (dd_data and "x" in dd_data and "series" in dd_data):
                    return None
                fig = go.Figure()
                for series in dd_data["series"]:
                    fill_mode = series.get("fill", False)
                    if fill_mode == "below_zero":
                        fill_mode = "tozeroy"
                    elif fill_mode not in ("tozeroy", "tonexty", "tonextx"):
                        fill_mode = None
                    line_color = series.get("color", "#e74c3c")
                    if line_color == "#e74c3c":
                        fill_color = "rgba(231, 76, 60, 0.3)"
                        line_width = 1.5
                    elif line_color == "#95a5a6":
                        fill_color = None
                        line_width = 2.0
                    else:
                        fill_color = f"{line_color}4D"
                        line_width = 1
                    fig.add_trace(go.Scatter(
                        x=dd_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        fill=fill_mode,
                        line=dict(color=line_color, width=line_width),
                        fillcolor=fill_color,
                    ))
                fig.update_layout(
                    title="水下回撤（%）",
                    yaxis_title="回撤 (%)",
                    height=320, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "HEATMAP":
                hm_data = charts.get("monthly_heatmap", {})
                if not (hm_data and "data" in hm_data and "x" in hm_data and "y" in hm_data):
                    return None
                fig = go.Figure(data=go.Heatmap(
                    z=hm_data["data"],
                    x=hm_data["x"],
                    y=hm_data["y"],
                    colorscale=[[0, "#27ae60"], [0.5, "#ffffff"], [1, "#e74c3c"]],
                    zmid=0,
                    text=hm_data["data"],
                    texttemplate="%{text:.1f}%",
                    hovertemplate="年份: %{y}<br>月份: %{x}<br>收益率: %{z:.2f}%",
                ))
                fig.update_layout(title="月度收益热力图（%）", height=380, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig
        except Exception:
            return None

    # ── 辅助：渲染带图表插入点的章节 ──────────────────────────
    def _render_section_with_charts(section_text: str):
        """
        将章节文本按 [INSERT_CHART: XXX] 分割，
        分割前后的文字用 st.markdown 渲染，标记位置插入 Plotly 图表。
        """
        import re
        MARKER_RE = re.compile(r"\[INSERT_CHART:\s*(\w+)\]")
        parts = MARKER_RE.split(section_text)
        # split 结果：[text_before, chart_key, text_after, chart_key2, ...]
        i = 0
        while i < len(parts):
            chunk = parts[i].strip()
            if chunk:
                st.markdown(chunk)
            i += 1
            if i < len(parts):
                chart_key = parts[i].strip()
                fig = _get_chart_fig(chart_key)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                i += 1

    # ── 渲染5个章节（含持仓分析）──────────────────────────
    for sec_key in ("section1", "section2", "section3", "section4", "section5"):
        if sec_key in deep:  # 兼容旧版报告（只有4个章节）

            # 特殊处理：section5 包含深度持仓分析的占位符
            if sec_key == "section5" and "[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]" in deep[sec_key]:
                # 渲染标题（不包括占位符）
                section_lines = deep[sec_key].split('\n')
                for line in section_lines:
                    if line.strip() and "[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]" not in line:
                        st.markdown(line)
                    elif "[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]" in line:
                        break  # 遇到占位符停止渲染

                # 渲染深度持仓分析UI
                try:
                    # 获取基金Beta（用于市场压力测试）
                    fund_beta = report.equity_metrics.beta if report.equity_metrics and hasattr(report.equity_metrics, 'beta') else 1.0

                    from reporter.equity_holdings_v2 import generate_deep_holdings_analysis
                    from ui.equity_holdings_v2_components import render_deep_holdings_ui

                    # 确定分析周期：只有"自成立以来"才使用成立日期，其他都使用默认5年
                    if analysis_range == "自成立以来":
                        analysis_period_str = "成立以来"
                    else:
                        # "3年"、"1年"等使用默认加载逻辑
                        analysis_period_str = "默认"

                    # 生成深度持仓分析
                    with st.spinner("📊 加载持仓数据中（最多5年历史），请稍候..."):
                        holdings_analysis = generate_deep_holdings_analysis(
                            symbol=report.basic.symbol,  # 修复：使用 symbol 而不是 code
                            analysis_period=analysis_period_str,
                            establish_date=report.basic.establish_date if analysis_period_str == "成立以来" else None,
                            manager_start_date=None,
                            max_years=5,
                            fund_beta=fund_beta,
                            n_quarters_for_trading=4,
                            top_n_for_valuation=10
                        )

                    # 渲染UI
                    render_deep_holdings_ui(holdings_analysis)
                except Exception as e:
                    import traceback
                    error_detail = traceback.format_exc()
                    logger.error(f"[深度持仓分析] 加载失败: {e}\n{error_detail}")
                    st.info(f"💡 深度持仓分析加载失败：{str(e)[:200]}")
            else:
                # 普通章节渲染
                _render_section_with_charts(deep[sec_key])

            st.markdown("")  # 章节间空行

    # ── 结论章节（无图表，直接渲染）──────────────────────────
    st.divider()
    st.markdown(deep["conclusion"])

elif report.bond_metrics:
    # ── 债券类：调用新债基深度报告生成器 ──────────────────────
    from reporter.bond_report_writer import generate_bond_deep_report

    # 获取风险偏好参数
    risk_preference = st.session_state.get("risk_preference", "稳健型")
    deep_bond = generate_bond_deep_report(report, risk_preference=risk_preference)

    # 标题行
    st.markdown(deep_bond["headline"])

    # ── 性格标签 ──
    if report.tags:
        tag_html = " ".join([
            f'<span style="background:#eaf4fb;border-radius:12px;padding:4px 10px;'
            f'margin:3px;display:inline-block;font-size:13px">{t}</span>'
            for t in report.tags
        ])
        st.markdown(tag_html, unsafe_allow_html=True)
        st.markdown("")  # 间距

    # ── 渲染3个章节（复用权益类 _get_chart_fig / _render_section_with_charts）──
    # 此处直接内联实现，避免函数未定义问题
    def _get_bond_chart_fig(chart_key: str):
        """债券报告：根据标记 key 返回 Plotly figure"""
        try:
            if chart_key == "CUM_RET":
                cum_data = charts.get("cumulative_return", {})
                if not (cum_data and "x" in cum_data and "series" in cum_data):
                    return None
                fig = go.Figure()
                for series in cum_data["series"]:
                    fig.add_trace(go.Scatter(
                        x=cum_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        line=dict(color=series.get("color", "#2980b9"), width=2),
                    ))
                fig.update_layout(
                    title="累计收益率（%）",
                    yaxis_title="收益率 (%)",
                    hovermode="x unified", height=360,
                    margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "DRAWDOWN":
                dd_data = charts.get("drawdown", {})
                if not (dd_data and "x" in dd_data and "series" in dd_data):
                    return None
                fig = go.Figure()
                for series in dd_data["series"]:
                    fill_mode = series.get("fill", False)
                    if fill_mode == "below_zero":
                        fill_mode = "tozeroy"
                    elif fill_mode not in ("tozeroy", "tonexty", "tonextx"):
                        fill_mode = None
                    line_color = series.get("color", "#e74c3c")
                    fill_color = "rgba(231, 76, 60, 0.25)" if line_color == "#e74c3c" else None
                    line_width = 1.5 if line_color == "#e74c3c" else 2.0
                    fig.add_trace(go.Scatter(
                        x=dd_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        fill=fill_mode,
                        line=dict(color=line_color, width=line_width),
                        fillcolor=fill_color,
                    ))
                fig.update_layout(
                    title="水下回撤（%）",
                    yaxis_title="回撤 (%)",
                    height=300, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "HEATMAP":
                hm_data = charts.get("monthly_heatmap", {})
                if not (hm_data and "data" in hm_data and "x" in hm_data and "y" in hm_data):
                    return None
                fig = go.Figure(data=go.Heatmap(
                    z=hm_data["data"],
                    x=hm_data["x"],
                    y=hm_data["y"],
                    colorscale=[[0, "#27ae60"], [0.5, "#ffffff"], [1, "#e74c3c"]],
                    zmid=0,
                    text=hm_data["data"],
                    texttemplate="%{text:.2f}%",
                    hovertemplate="年份: %{y}<br>月份: %{x}<br>收益率: %{z:.2f}%",
                ))
                fig.update_layout(
                    title="月度收益热力图（%）",
                    height=380,
                    margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "BOND_HOLDINGS_PIE":
                # 债券持仓分类饼图
                pie_data = charts.get("bond_holdings_pie", {})
                if not (pie_data and "labels" in pie_data and "values" in pie_data):
                    return None
                fig = go.Figure(data=go.Pie(
                    labels=pie_data["labels"],
                    values=pie_data["values"],
                    hole=0.4,  # 甜甜圈效果
                    marker=dict(colors=pie_data.get("colors", ["#27ae60", "#3498db", "#e67e22", "#e74c3c"])),
                    textinfo="label+percent",
                    textposition="outside",
                    textfont=dict(size=12),
                ))
                fig.update_layout(
                    title="债券持仓分类占比",
                    height=380,
                    margin=dict(t=50, b=30, l=60, r=60),
                    showlegend=True,
                    legend=dict(
                        orientation="v",
                        yanchor="middle",
                        y=0.5,
                        xanchor="left",
                        x=1.05,
                        font=dict(size=11),
                    )
                )
                return fig

            elif chart_key == "RATE_PREDICTION":
                # 利率预测图表
                try:
                    from data_loader.rate_prediction import predict_rate_trend, generate_rate_prediction_chart

                    # 获取预测结果
                    prediction = predict_rate_trend(horizon="3m")

                    # 生成图表数据
                    chart_data = generate_rate_prediction_chart(prediction)

                    if not (chart_data and "x" in chart_data and "series" in chart_data):
                        return None

                    fig = go.Figure()

                    # 绘制各条曲线
                    for series in chart_data["series"]:
                        fig.add_trace(go.Scatter(
                            x=chart_data["x"],
                            y=series["data"],
                            mode="lines",
                            name=series["name"],
                            line=dict(
                                color=series.get("color", "#3498db"),
                                width=2,
                                dash=series.get("dash", "solid"),
                            ),
                        ))

                    # 添加预测信息注释
                    pred_info = chart_data.get("prediction_info", {})
                    direction = pred_info.get("direction", "")
                    confidence = pred_info.get("confidence", "")
                    current = pred_info.get("current", 0)
                    mid_term = pred_info.get("mid_term_forecast", 0)

                    annotation_text = (
                        f"方向: {direction}<br>"
                        f"置信度: {confidence}<br>"
                        f"当前: {current:.2f}%<br>"
                        f"3个月后预测: {mid_term:.2f}%"
                    )

                    fig.add_annotation(
                        x=chart_data["x"][-1],
                        y=chart_data["series"][0]["data"][-1] if chart_data["series"][0]["data"][-1] else mid_term,
                        text=annotation_text,
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=2,
                        arrowcolor="#e74c3c",
                        bgcolor="rgba(255, 255, 255, 0.9)",
                        bordercolor="#e74c3c",
                        borderwidth=1,
                        font=dict(size=11),
                        xshift=10,
                    )

                    fig.update_layout(
                        title="10Y国债收益率预测（技术指标模型）",
                        yaxis_title="收益率 (%)",
                        hovermode="x unified",
                        height=400,
                        margin=dict(t=50, b=30, l=60, r=60),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=-0.3,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=10),
                        )
                    )
                    return fig
                except Exception as e:
                    import traceback
                    logger.error(f"[利率预测图表] 渲染失败: {e}\n{traceback.format_exc()}")
                    return None
        except Exception as e:
            import traceback
            logger.error(f"[债券图表] 渲染失败: {e}\n{traceback.format_exc()}")
            return None

    def _render_bond_section(section_text: str):
        """渲染带图表插入点的债券章节"""
        import re
        MARKER_RE = re.compile(r"\[INSERT_CHART:\s*(\w+)\]")
        parts = MARKER_RE.split(section_text)
        i = 0
        while i < len(parts):
            chunk = parts[i].strip()
            if chunk:
                st.markdown(chunk)
            i += 1
            if i < len(parts):
                chart_key = parts[i].strip()
                fig = _get_bond_chart_fig(chart_key)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                i += 1

    # ── 渲染3个章节 ──────────────────────────────────────────
    for sec_key in ("section1", "section2", "section3"):
        _render_bond_section(deep_bond[sec_key])
        st.markdown("")  # 章节间空行

    # ── 结论章节（可能包含利率预测图表）────────────────────────
    st.divider()
    _render_bond_section(deep_bond["conclusion"])

elif report.index_metrics or report.fund_type == "index_enhanced":
    # ── 指数/ETF：调用新指数深度报告生成器 ─────────────────────
    # 支持：标准指数、指数增强型
    from reporter.index_report_writer import generate_index_deep_report

    deep_idx = generate_index_deep_report(report)

    # 标题行
    st.markdown(deep_idx["headline"])

    # ── 性格标签 ──
    if report.tags:
        tag_html = " ".join([
            f'<span style="background:#eaf0fb;border-radius:12px;padding:4px 10px;'
            f'margin:3px;display:inline-block;font-size:13px">{t}</span>'
            for t in report.tags
        ])
        st.markdown(tag_html, unsafe_allow_html=True)
        st.markdown("")  # 间距

    # ── 内联图表辅助函数 ───────────────────────────────────────
    def _get_idx_chart_fig(chart_key: str):
        """指数报告：根据标记 key 返回 Plotly figure"""
        try:
            if chart_key == "CUM_RET":
                cum_data = charts.get("cumulative_return", {})
                if not (cum_data and "x" in cum_data and "series" in cum_data):
                    return None
                fig = go.Figure()
                for series in cum_data["series"]:
                    fig.add_trace(go.Scatter(
                        x=cum_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        line=dict(color=series.get("color", "#2980b9"), width=2),
                    ))
                fig.update_layout(
                    title="累计收益率（%）— 基金 vs 指数",
                    yaxis_title="收益率 (%)",
                    hovermode="x unified", height=360,
                    margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "TRACKING_ERROR_SCATTER":
                # 优先使用 tracking_error_scatter 数据，否则使用 excess_return
                te_data = charts.get("tracking_error_scatter", {})
                ex_data = charts.get("excess_return", {})

                # 选择数据源：优先 tracking_error_scatter
                data_source = te_data if te_data and "x" in te_data and "series" in te_data else ex_data

                if data_source and "x" in data_source and "series" in data_source:
                    fig = go.Figure()
                    for series in ex_data["series"]:
                        color = series.get("color", "#8e44ad")
                        if isinstance(color, list):
                            for i in range(len(series["data"]) - 1):
                                fig.add_trace(go.Scatter(
                                    x=[ex_data["x"][i], ex_data["x"][i+1]],
                                    y=[series["data"][i], series["data"][i+1]],
                                    mode="lines",
                                    name=series["name"] if i == 0 else "",
                                    line=dict(color=color[i], width=1.5),
                                    showlegend=(i == 0),
                                ))
                        else:
                            fig.add_trace(go.Scatter(
                                x=ex_data["x"], y=series["data"],
                                mode="lines", name=series["name"],
                                line=dict(color=color, width=1.5),
                            ))
                    fig.add_hline(y=0, line_dash="dash", line_color="gray",
                                  annotation_text="偏离=0")
                    # 添加±2σ带（年化跟踪误差换算）
                    te_ann = report.index_metrics.tracking_error_annualized
                    te_daily_pct = te_ann / (252 ** 0.5) * 100  # 日化跟踪误差(%)
                    fig.update_layout(
                        title=f"跟踪偏离度（%）— 年化跟踪误差: {te_ann*100:.2f}%",
                        yaxis_title="偏离度 (%)",
                        height=340, margin=dict(t=40, b=30),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=-0.25,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=10),
                        ),
                        shapes=[
                            dict(type="rect", xref="paper", yref="y",
                                 x0=0, x1=1,
                                 y0=-te_daily_pct * 2, y1=te_daily_pct * 2,
                                 fillcolor="rgba(142,68,173,0.08)",
                                 line=dict(width=0)),
                        ]
                    )
                    return fig
                # fallback：回撤图
                return None

            elif chart_key == "PREMIUM_DISCOUNT":
                # 折溢价率：用月度热力图代替（若无折溢价专图）
                hm_data = charts.get("monthly_heatmap", {})
                if not (hm_data and "data" in hm_data and "x" in hm_data and "y" in hm_data):
                    return None
                pd_mean = report.index_metrics.premium_discount_mean * 100
                pd_std  = report.index_metrics.premium_discount_std * 100
                # 场外指数基金折溢价数据为0，显示为"无数据"
                pd_display = f"{pd_mean:+.3f}%，σ: {pd_std:.3f}%" if abs(pd_mean) > 0.001 or abs(pd_std) > 0.001 else "无数据（场外基金）"
                fig = go.Figure(data=go.Heatmap(
                    z=hm_data["data"],
                    x=hm_data["x"],
                    y=hm_data["y"],
                    colorscale=[[0, "#27ae60"], [0.5, "#ffffff"], [1, "#e74c3c"]],
                    zmid=0,
                    text=hm_data["data"],
                    texttemplate="%{text:.2f}%",
                    hovertemplate="年份: %{y}<br>月份: %{x}<br>收益率: %{z:.2f}%",
                ))
                fig.update_layout(
                    title=f"月度收益热力图（折溢价: {pd_display}）",
                    height=380, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "DRAWDOWN":
                dd_data = charts.get("drawdown", {})
                if not (dd_data and "x" in dd_data and "series" in dd_data):
                    return None
                fig = go.Figure()
                for series in dd_data["series"]:
                    fill_mode = series.get("fill", False)
                    if fill_mode == "below_zero":
                        fill_mode = "tozeroy"
                    elif fill_mode not in ("tozeroy", "tonexty", "tonextx"):
                        fill_mode = None
                    line_color = series.get("color", "#2980b9")
                    fill_color = "rgba(41,128,185,0.2)" if line_color == "#2980b9" else None
                    fig.add_trace(go.Scatter(
                        x=dd_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        fill=fill_mode,
                        line=dict(color=line_color, width=1.5),
                        fillcolor=fill_color,
                    ))
                fig.update_layout(
                    title="水下回撤（%）",
                    yaxis_title="回撤 (%)",
                    height=300, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

        except Exception:
            return None

    def _render_idx_section(section_text: str):
        """渲染带图表插入点的指数章节"""
        import re
        MARKER_RE = re.compile(r"\[INSERT_CHART:\s*(\w+)\]")
        parts = MARKER_RE.split(section_text)
        i = 0
        while i < len(parts):
            chunk = parts[i].strip()
            if chunk:
                st.markdown(chunk)
            i += 1
            if i < len(parts):
                chart_key = parts[i].strip()
                fig = _get_idx_chart_fig(chart_key)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                i += 1

    # ── 渲染3个章节 ──────────────────────────────────────────
    for sec_key in ("section1", "section2", "section3"):
        _render_idx_section(deep_idx[sec_key])
        st.markdown("")  # 章节间空行

    # ── 结论章节 ──────────────────────────────────────────────
    st.divider()
    st.markdown(deep_idx["conclusion"])

elif report.cb_metrics:
    # ── 可转债/固收+：调用新转债深度报告生成器 ─────────────────────
    from reporter.cb_report_writer import generate_cb_deep_report

    deep_cb = generate_cb_deep_report(report)

    # 标题行
    st.markdown(deep_cb["headline"])

    # ── 性格标签 ──
    if report.tags:
        tag_html = " ".join([
            f'<span style="background:#fef5e7;border-radius:12px;padding:4px 10px;'
            f'margin:3px;display:inline-block;font-size:13px;border:1px solid #f39c12">{t}</span>'
            for t in report.tags
        ])
        st.markdown(tag_html, unsafe_allow_html=True)
        st.markdown("")  # 间距

    # ── 内联图表辅助函数 ───────────────────────────────────────
    def _get_cb_chart_fig(chart_key: str):
        """转债报告：根据标记 key 返回 Plotly figure"""
        try:
            if chart_key == "CUM_RET":
                cum_data = charts.get("cumulative_return", {})
                if not (cum_data and "x" in cum_data and "series" in cum_data):
                    return None
                fig = go.Figure()
                for series in cum_data["series"]:
                    fig.add_trace(go.Scatter(
                        x=cum_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        line=dict(color=series.get("color", "#f39c12"), width=2),
                    ))
                fig.update_layout(
                    title="累计收益率（%）— 基金 vs 股债复合基准",
                    yaxis_title="收益率 (%)",
                    hovermode="x unified", height=360,
                    margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

            elif chart_key == "CAPTURE_RATIO":
                # 捕获率图：用超额收益曲线模拟上行/下行效率
                ex_data = charts.get("excess_return", {})
                if ex_data and "x" in ex_data and "series" in ex_data:
                    fig = go.Figure()
                    for series in ex_data["series"]:
                        color = series.get("color", "#e67e22")
                        fig.add_trace(go.Scatter(
                            x=ex_data["x"], y=series["data"],
                            mode="lines", name=series["name"],
                            line=dict(color=color, width=1.5),
                            fill="tozeroy" if "excess" in series["name"].lower() else None,
                            fillcolor="rgba(230,126,34,0.15)" if "excess" in series["name"].lower() else None,
                        ))
                    fig.add_hline(y=0, line_dash="dash", line_color="gray",
                                  annotation_text="超额=0")
                    fig.update_layout(
                        title="超额收益曲线（%）— 体现非对称捕获能力",
                        yaxis_title="超额收益率 (%)",
                        height=340, margin=dict(t=40, b=30),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=-0.25,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=10),
                        )
                    )
                    return fig
                # fallback：月度热力图
                hm_data = charts.get("monthly_heatmap", {})
                if hm_data and "data" in hm_data and "x" in hm_data and "y" in hm_data:
                    fig = go.Figure(data=go.Heatmap(
                        z=hm_data["data"],
                        x=hm_data["x"],
                        y=hm_data["y"],
                        colorscale=[[0, "#27ae60"], [0.5, "#ffffff"], [1, "#e74c3c"]],
                        zmid=0,
                        text=hm_data["data"],
                        texttemplate="%{text:.2f}%",
                        hovertemplate="年份: %{y}<br>月份: %{x}<br>收益率: %{z:.2f}%",
                    ))
                    fig.update_layout(title="月度收益热力图（%）", height=380, margin=dict(t=40, b=30),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=-0.25,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=10),
                        )
                    )
                    return fig
                return None

            elif chart_key == "DRAWDOWN":
                dd_data = charts.get("drawdown", {})
                if not (dd_data and "x" in dd_data and "series" in dd_data):
                    return None
                fig = go.Figure()
                for series in dd_data["series"]:
                    fill_mode = series.get("fill", False)
                    if fill_mode == "below_zero":
                        fill_mode = "tozeroy"
                    elif fill_mode not in ("tozeroy", "tonexty", "tonextx"):
                        fill_mode = None
                    line_color = series.get("color", "#f39c12")
                    fill_color = "rgba(243,156,18,0.2)" if line_color == "#f39c12" else None
                    fig.add_trace(go.Scatter(
                        x=dd_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        fill=fill_mode,
                        line=dict(color=line_color, width=1.5),
                        fillcolor=fill_color,
                    ))
                fig.update_layout(
                    title="水下回撤（%）",
                    yaxis_title="回撤 (%)",
                    height=300, margin=dict(t=40, b=30),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=-0.25,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=10),
                    )
                )
                return fig

        except Exception:
            return None

    def _render_cb_section(section_text: str):
        """渲染带图表插入点的转债章节"""
        import re
        MARKER_RE = re.compile(r"\[INSERT_CHART:\s*(\w+)\]")
        parts = MARKER_RE.split(section_text)
        i = 0
        while i < len(parts):
            chunk = parts[i].strip()
            if chunk:
                st.markdown(chunk)
            i += 1
            if i < len(parts):
                chart_key = parts[i].strip()
                fig = _get_cb_chart_fig(chart_key)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True)
                i += 1

    # ── 渲染4个章节（含大类资产穿透）────────────────────────
    for sec_key in ("section1", "section2", "section3", "section4"):
        if sec_key in deep_cb:  # 兼容旧版报告
            _render_cb_section(deep_cb[sec_key])
            st.markdown("")  # 章节间空行

    # ── 结论章节 ──────────────────────────────────────────────
    st.divider()
    st.markdown(deep_cb["conclusion"])

else:
    # ── 未知类型：兜底展示 ──────────────────────────────────────
    st.warning("⚠️ 暂不支持该基金类型的深度分析，仅显示基础指标。")
    st.markdown("📊 **关键指标**")

    # 尝试展示 common 指标
    for m_type in ["equity_metrics", "bond_metrics", "index_metrics", "cb_metrics"]:
        if hasattr(report, m_type):
            m = getattr(report, m_type)
            if m:
                cm = m.common
                metrics_to_show = [
                    ("年化收益", f"{cm.annualized_return*100:.2f}%"),
                    ("最大回撤", f"{cm.max_drawdown*100:.2f}%"),
                    ("夏普比率", f"{cm.sharpe_ratio:.2f}"),
                    ("累计收益", f"{cm.cumulative_return*100:.2f}%"),
                ]
                cols_m = st.columns(4)
                for idx, (label, value) in enumerate(metrics_to_show):
                    with cols_m[idx % 4]:
                        st.metric(label=label, value=value)
                break

    st.divider()

    # 深度诊断（兜底）
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown("📋 **深度诊断**")
        body_text = text.get("body", "")
        if body_text:
            sections = body_text.split("\n\n\n")
            for i, section in enumerate(sections):
                st.markdown(section)
                if i < len(sections) - 1:
                    st.markdown("---")

        if report.tags:
            st.markdown("🏷️ **性格标签**")
            tag_html = " ".join([
                f'<span style="background:#e8f4f8;border-radius:12px;padding:4px 10px;'
                f'margin:3px;display:inline-block">{t}</span>'
                for t in report.tags
            ])
            st.markdown(tag_html, unsafe_allow_html=True)

    with col_r:
        st.markdown("💡 **投资建议**")
        st.info(text.get("advice", ""))

        st.markdown("⚠️ **风险提示**")
        risk_text = text.get("risk_warning", "")
        if "🔴" in risk_text:
            st.error(risk_text)
        elif "⚠️" in risk_text:
            st.warning(risk_text)
        else:
            st.success(risk_text)

