"""
Streamlit 主界面入口 — fund_quant_v2
数据获取 → 标准化清洗 → 模块化计算 → 统一报告展示
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys
import os
import logging

# 配置日志
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# 将项目根目录加入 PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import analyze_fund
from models.schema import FundReport
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

# --- Part 1: 基金基础信息（紧凑展示）---
if basic:
    # 获取最新净值日期和净值
    latest_nav_date = ""
    latest_nav = ""
    if charts and "nav_latest" in charts:
        latest_nav_date = charts["nav_latest"].get("date", "")
        latest_nav = charts["nav_latest"].get("nav", "")

    # 综合费率 = 管理费 + 托管费
    total_fee = 0.0
    if basic.fee_manage > 0 or basic.fee_custody > 0:
        total_fee = basic.fee_manage + basic.fee_custody

    # 基金基础信息（紧凑展示，不显示标题）
    # 为每个参数固定留出2行空间，内容过多时自动缩小字体
    def _format_info_item(label: str, value: str, max_chars: int = 30) -> str:
        """格式化信息项，根据内容长度自动调整字体大小"""
        if not value:
            value = "—"

        content_length = len(value)
        if content_length <= max_chars:
            font_size = "14px"
        elif content_length <= max_chars * 2:
            font_size = "12px"
        else:
            font_size = "11px"

        return f'<div style="font-size:{font_size};line-height:1.6;min-height:3.2em;"><b>{label}</b><br>{value}</div>'

    col_info = st.columns(4)
    with col_info[0]:
        st.markdown(_format_info_item("基金名称", basic.name or "—", max_chars=20), unsafe_allow_html=True)
        st.markdown(_format_info_item("基金经理", basic.manager or "—"), unsafe_allow_html=True)
    with col_info[1]:
        st.markdown(_format_info_item("基金公司", basic.company or "—"), unsafe_allow_html=True)
        st.markdown(_format_info_item("基金类型", basic.type_raw or "—"), unsafe_allow_html=True)
    with col_info[2]:
        st.markdown(_format_info_item("成立日期", basic.establish_date or "—"), unsafe_allow_html=True)
        st.markdown(_format_info_item("基金规模", basic.scale or "—"), unsafe_allow_html=True)
    with col_info[3]:
        st.markdown(_format_info_item("单位净值", f"{latest_nav} ({latest_nav_date})" if latest_nav else "—"), unsafe_allow_html=True)
        # 显示业绩比较基准（从解析后的组件生成描述）
        benchmark_display = basic.benchmark_text or "—"
        # 如果有解析后的基准信息，生成更清晰的描述
        if basic.benchmark_parsed and basic.benchmark_parsed.get("components"):
            components = basic.benchmark_parsed["components"]
            comp_desc = " + ".join([f"{c['name']}×{int(c['weight']*100)}%" for c in components])
            benchmark_display = comp_desc
        st.markdown(_format_info_item("业绩比较基准", benchmark_display, max_chars=50), unsafe_allow_html=True)

    col_info2 = st.columns(4)
    with col_info2[0]:
        st.markdown(_format_info_item("申购状态", basic.purchase_status or "—"), unsafe_allow_html=True)
    with col_info2[1]:
        st.markdown(_format_info_item("赎回状态", basic.redeem_status or "—"), unsafe_allow_html=True)
    with col_info2[2]:
        min_purchase_text = f"{basic.min_purchase:.0f}元" if basic.min_purchase and basic.min_purchase > 0 else "—"
        st.markdown(_format_info_item("购买起点", min_purchase_text), unsafe_allow_html=True)
    with col_info2[3]:
        fee_text = f"{total_fee*100:.2f}%" if total_fee > 0 else "—"
        st.markdown(_format_info_item("综合费率", fee_text), unsafe_allow_html=True)


st.divider()

# --- Part 2: 图表 ---
if charts:
    # 只保留通用图表，移除特色图表
    tab_names = ["收益曲线", "回撤分析", "月度热力图", "超额收益曲线"]
    tabs = st.tabs(tab_names)

    # 收益曲线
    with tabs[0]:
        cum_data = charts.get("cumulative_return", {})
        if cum_data and "x" in cum_data and "series" in cum_data:
            fig = go.Figure()
            for series in cum_data["series"]:
                fig.add_trace(go.Scatter(
                    x=cum_data["x"], y=series["data"],
                    mode="lines", name=series["name"],
                    line=dict(color=series.get("color", "#e74c3c"), width=2),
                ))
            fig.update_layout(
                title="累计收益率（%）",
                xaxis_title="日期",
                yaxis_title="收益率 (%)",
                hovermode="x unified",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("收益曲线数据不足")

        # 图表解读
        interpretation = get_chart_interpretation("cumulative_return", report, charts)
        st.markdown(f"<div style='background-color:#f8f9fa; padding:15px; border-radius:10px; border-left:4px solid #3498db; margin-top:15px; font-size:14px; line-height:1.6'>{interpretation}</div>", unsafe_allow_html=True)

    # 回撤图
    with tabs[1]:
        dd_data = charts.get("drawdown", {})
        if dd_data and "x" in dd_data and "series" in dd_data:
            fig = go.Figure()
            for series in dd_data["series"]:
                # 根据 series 数据中的 fill 属性决定是否填充
                fill_mode = series.get("fill", False)
                # 只在 fill_mode 为 "below_zero" 时设置为 "tozeroy"，否则设为 None
                if fill_mode == "below_zero":
                    fill_mode = "tozeroy"
                elif fill_mode not in ("tozeroy", "tonexty", "tonextx"):
                    fill_mode = None

                # 根据 series 数据中的 color 属性使用对应颜色
                line_color = series.get("color", "#e74c3c")

                # 根据颜色生成对应的填充色（带透明度）
                if line_color == "#e74c3c":  # 红色（基金）
                    fill_color = "rgba(231, 76, 60, 0.3)"
                    line_width = 1.5
                elif line_color == "#95a5a6":  # 灰色（基准）
                    fill_color = None  # 基准不填充
                    line_width = 2.0  # 基准线条加粗，更明显
                else:
                    fill_color = f"{line_color}4D"  # 30% 透明度
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
                xaxis_title="日期",
                yaxis_title="回撤 (%)",
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("回撤数据不足")

        # 图表解读
        interpretation = get_chart_interpretation("drawdown", report, charts)
        st.markdown(f"<div style='background-color:#f8f9fa; padding:15px; border-radius:10px; border-left:4px solid #e74c3c; margin-top:15px; font-size:14px; line-height:1.6'>{interpretation}</div>", unsafe_allow_html=True)

    # 月度热力图
    with tabs[2]:
        hm_data = charts.get("monthly_heatmap", {})
        # 防御性检查：确保hm_data包含所有必要字段
        if hm_data and "data" in hm_data and "x" in hm_data and "y" in hm_data:
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
            fig.update_layout(title="月度收益热力图（%）", height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("月度数据不足（需要至少1年数据）")

        # 图表解读
        interpretation = get_chart_interpretation("monthly_heatmap", report, charts)
        st.markdown(f"<div style='background-color:#f8f9fa; padding:15px; border-radius:10px; border-left:4px solid #f39c12; margin-top:15px; font-size:14px; line-height:1.6'>{interpretation}</div>", unsafe_allow_html=True)

    # 超额收益曲线（第4个tab）
    with tabs[3]:
        ex_data = charts.get("excess_return", {})
        if ex_data and "x" in ex_data and "series" in ex_data:
            fig = go.Figure()
            for series in ex_data["series"]:
                # 特殊处理：如果颜色是列表，需要为每个数据点单独设置
                color = series.get("color", "#3498db")
                if isinstance(color, list):
                    # 为每个数据点单独设置颜色
                    for i in range(len(series["data"]) - 1):
                        fig.add_trace(go.Scatter(
                            x=[ex_data["x"][i], ex_data["x"][i+1]],
                            y=[series["data"][i], series["data"][i+1]],
                            mode="lines",
                            name=series["name"] if i == 0 else "",
                            line=dict(color=color[i], width=2),
                            showlegend=False if i > 0 else True
                        ))
                else:
                    fig.add_trace(go.Scatter(
                        x=ex_data["x"], y=series["data"],
                        mode="lines", name=series["name"],
                        line=dict(color=color, width=2),
                    ))
            
            # 添加零线
            fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="超额=0")
            
            fig.update_layout(
                title="超额收益曲线（%）",
                xaxis_title="日期",
                yaxis_title="超额收益 (%)",
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("超额收益数据不足")

        # 图表解读
        interpretation = get_chart_interpretation("excess_return", report, charts)
        st.markdown(f"<div style='background-color:#f8f9fa; padding:15px; border-radius:10px; border-left:4px solid #9b59b6; margin-top:15px; font-size:14px; line-height:1.6'>{interpretation}</div>", unsafe_allow_html=True)



st.divider()

# --- Part 3: 关键指标 ---
st.markdown("📊 关键指标")

metrics_to_show = []
if report.equity_metrics:
    m = report.equity_metrics
    cm = m.common
    metrics_to_show = [
        ("年化收益", f"{cm.annualized_return*100:.2f}%"),
        ("最大回撤", f"{cm.max_drawdown*100:.2f}%"),
        ("夏普比率", f"{cm.sharpe_ratio:.2f}"),
        ("年化Alpha", f"{m.alpha*100:.2f}%"),
        ("信息比率", f"{m.information_ratio:.2f}"),
        ("跟踪误差", f"{m.tracking_error*100:.2f}%"),
        ("Beta", f"{m.beta:.2f}"),
        ("R²", f"{m.r_squared:.2f}"),
    ]
elif report.bond_metrics:
    m = report.bond_metrics
    cm = m.common
    metrics_to_show = [
        ("年化收益", f"{cm.annualized_return*100:.2f}%"),
        ("最大回撤", f"{cm.max_drawdown*100:.2f}%"),
        ("夏普比率", f"{cm.sharpe_ratio:.2f}"),
        ("加权久期", f"{m.duration:.1f}年"),
        ("WACS评分", f"{m.wacs_score:.0f}"),
        ("HHI集中度", f"{m.hhi:.0f}"),
    ]
elif report.index_metrics:
    m = report.index_metrics
    cm = m.common
    metrics_to_show = [
        ("年化收益", f"{cm.annualized_return*100:.2f}%"),
        ("年化跟踪误差", f"{m.tracking_error_annualized*100:.2f}%"),
        ("与基准相关性", f"{m.correlation:.3f}"),
        ("信息比率", f"{m.information_ratio:.2f}"),
        ("折溢价均值", f"{m.premium_discount_mean*100:.3f}%"),
        ("工具评分", f"{m.tool_score:.0f}分 / {m.tool_grade}"),
    ]
elif report.cb_metrics:
    m = report.cb_metrics
    cm = m.common
    metrics_to_show = [
        ("年化收益", f"{cm.annualized_return*100:.2f}%"),
        ("最大回撤", f"{cm.max_drawdown*100:.2f}%"),
        ("夏普比率", f"{cm.sharpe_ratio:.2f}"),
        ("综合权益暴露", f"{m.equity_exposure*100:.1f}%"),
        ("平均Delta", f"{m.delta_avg:.2f}"),
        ("平均溢价率", f"{m.premium_avg:.1f}%"),
    ]

if metrics_to_show:
    for label, value in metrics_to_show:
        st.markdown(f"**{label}**：{value}")

st.divider()

# --- Part 4: 深度诊断 ---
col_l, col_r = st.columns([3, 2])

with col_l:
    st.markdown("📋 深度诊断")
    body_text = text.get("body", "")
    if body_text:
        # 解析 body_text 中的各个诊断部分（性格诊断/风险诊断/超额来源等）
        sections = body_text.split("\n\n\n")  # 三换行分隔大节
        for i, section in enumerate(sections):
            st.markdown(section)
            # 在每个大节（性格诊断/风险诊断/超额来源等）后添加指标解读预留位置
            if i < len(sections) - 1:  # 最后一个不加
                st.markdown("---")
                st.caption("📊 指标解读（待补充）")

    # 性格标签
    if report.tags:
        st.markdown("🏷️ 性格标签")
        tag_html = " ".join([
            f'<span style="background:#e8f4f8;border-radius:12px;padding:4px 10px;margin:3px;display:inline-block">{t}</span>'
            for t in report.tags
        ])
        st.markdown(tag_html, unsafe_allow_html=True)

with col_r:
    st.markdown("💡 投资建议")
    st.info(text.get("advice", ""))

    st.markdown("⚠️ 风险提示")
    risk_text = text.get("risk_warning", "")
    if "🔴" in risk_text:
        st.error(risk_text)
    elif "⚠️" in risk_text:
        st.warning(risk_text)
    else:
        st.success(risk_text)

