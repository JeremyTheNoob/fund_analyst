"""
UI组件模块
Streamlit UI组件展示
依赖：streamlit, config, utils
"""

import streamlit as st
import pandas as pd

import config
from utils.helpers import fmt_pct, fmt_f


def render_css():
    """渲染CSS样式"""
    st.markdown("""
    <style>
    .stApp { background-color: #f4f6f9; }

    .hero { background: linear-gradient(135deg,#1a1a2e,#0f3460);
            padding:28px 36px; border-radius:16px; color:white; margin-bottom:20px; }
    .hero h1 { font-size:1.9rem; margin:0 0 6px 0; }
    .hero p  { font-size:0.95rem; opacity:0.7; margin:0; }

    .kpi { background:white; border-radius:12px; padding:18px; text-align:center;
           box-shadow:0 2px 10px rgba(0,0,0,.06); border-top:3px solid #0f3460; }
    .kpi-val { font-size:1.55rem; font-weight:700; color:#0f3460; }
    .kpi-lbl { font-size:0.78rem; color:#888; margin-top:4px; }
    .kpi-sub { font-size:0.72rem; color:#999; margin-top:2px; }
    .kpi-red  .kpi-val { color:#e74c3c; }
    .kpi-green .kpi-val { color:#27ae60; }
    .kpi-orange .kpi-val { color:#e67e22; }

    .card { background:white; border-radius:12px; padding:20px 24px;
            box-shadow:0 2px 10px rgba(0,0,0,.06); margin:8px 0; }
    .card-warn   { border-left:4px solid #e67e22; background:#fffbf0; }
    .card-danger { border-left:4px solid #e74c3c; background:#fff5f5; }
    .card-good   { border-left:4px solid #27ae60; background:#f0fff4; }
    .card-info   { border-left:4px solid #3498db; background:#f0f7ff; }

    .section-title { font-size:1.05rem; font-weight:700; color:#1a1a2e;
                     margin:24px 0 16px 0; padding-bottom:8px; border-bottom:2px solid #f0f0f0; }
    </style>
    """, unsafe_allow_html=True)


def render_kpi_card(
    title: str,
    value: str,
    subtitle: str = '',
    color: str = 'default',
    col=None,
) -> None:
    """
    渲染KPI卡片

    Args:
        title: 标题
        value: 值
        subtitle: 副标题
        color: 颜色（default/red/green/orange）
        col: 列对象（用于布局）
    """
    color_class = f'kpi-{color}' if color != 'default' else ''

    html = f"""
    <div class="kpi {color_class}">
        <div class="kpi-lbl">{title}</div>
        <div class="kpi-val">{value}</div>
        <div class="kpi-sub">{subtitle}</div>
    </div>
    """

    if col:
        col.markdown(html, unsafe_allow_html=True)
    else:
        st.markdown(html, unsafe_allow_html=True)


def render_metric_card(
    title: str,
    metrics: dict,
    col=None,
) -> None:
    """
    渲染指标卡片

    Args:
        title: 标题
        metrics: 指标字典 {'指标名': '值', ...}
        col: 列对象
    """
    rows = ''.join([f'<tr><td>{k}</td><td style="text-align:right">{v}</td></tr>' for k, v in metrics.items()])

    html = f"""
    <div class="card">
        <h4 style="margin:0 0 10px 0;color:#1a1a2e;">{title}</h4>
        <table style="width:100%;border-collapse:collapse;">
            {rows}
        </table>
    </div>
    """

    if col:
        col.markdown(html, unsafe_allow_html=True)
    else:
        st.markdown(html, unsafe_allow_html=True)


def render_risk_card(
    title: str,
    message: str,
    level: str = 'info',
    col=None,
) -> None:
    """
    渲染风险提示卡片

    Args:
        title: 标题
        message: 消息内容
        level: 级别（info/warn/danger）
        col: 列对象
    """
    level_map = {
        'info': 'card-info',
        'warn': 'card-warn',
        'danger': 'card-danger',
    }
    card_class = level_map.get(level, 'card-info')

    html = f"""
    <div class="card {card_class}">
        <h4 style="margin:0 0 8px 0;">{title}</h4>
        <p style="margin:0;font-size:0.9rem;color:#666;">{message}</p>
    </div>
    """

    if col:
        col.markdown(html, unsafe_allow_html=True)
    else:
        st.markdown(html, unsafe_allow_html=True)


def render_basic_info(
    basic_info: dict,
    col=None,
) -> None:
    """
    渲染基金基本信息

    Args:
        basic_info: 基金基本信息字典
        col: 列对象
    """
    metrics = {
        '基金名称': basic_info.get('name', 'N/A'),
        '基金类型': basic_info.get('type_raw', 'N/A'),
        '成立日期': basic_info.get('establish_date', 'N/A'),
        '规模': basic_info.get('scale', 'N/A'),
        '基金公司': basic_info.get('company', 'N/A'),
        '基金经理': basic_info.get('manager', 'N/A'),
        '管理费率': fmt_pct(basic_info.get('fee_manage', 0)),
        '托管费率': fmt_pct(basic_info.get('fee_custody', 0)),
    }

    render_metric_card('基本信息', metrics, col)


def render_performance_metrics(
    performance: dict,
    col=None,
) -> None:
    """
    渲染业绩指标

    Args:
        performance: 业绩指标字典
        col: 列对象
    """
    metrics = {
        '累计收益': fmt_pct(performance.get('total_return', 0)),
        '年化收益': fmt_pct(performance.get('annual_return', 0)),
        '年化波动': fmt_pct(performance.get('annual_volatility', 0)),
        '夏普比率': fmt_f(performance.get('sharpe_ratio', 0)),
        '最大回撤': fmt_pct(performance.get('max_drawdown', 0)),
    }

    # 指标解读
    vol = performance.get('annual_volatility', 0)
    sharpe = performance.get('sharpe_ratio', 0)
    dd = performance.get('max_drawdown', 0)

    interpretations = []

    if vol > 0:
        if vol < 0.15:
            interpretations.append("🟢 年化波动低,收益稳定")
        elif vol < 0.25:
            interpretations.append("🟡 年化波动中等")
        else:
            interpretations.append("🔴 年化波动高,风险较大")

    if sharpe > 0:
        if sharpe > 1.5:
            interpretations.append("💎 夏普比率优秀,风险调整后收益高")
        elif sharpe > 1.0:
            interpretations.append("✅ 夏普比率良好")
        elif sharpe > 0.5:
            interpretations.append("🟡 夏普比率一般")
        else:
            interpretations.append("🔴 夏普比率偏低,承担的风险换回的收益不足")

    if dd < 0:
        if dd > -0.10:
            interpretations.append("🟢 最大回撤小,风控优秀")
        elif dd > -0.20:
            interpretations.append("🟡 最大回撤中等")
        else:
            interpretations.append("🔴 最大回撤大,历史亏损严重")

    # 最大回撤为负时标红
    if dd < config.RISK_THRESHOLDS['drawdown_warning']:
        card_type = 'warn'
        if dd < config.RISK_THRESHOLDS['drawdown_danger']:
            card_type = 'danger'
    else:
        card_type = 'info'

    # 渲染指标卡片
    render_metric_card('业绩指标', metrics, col)

    # 渲染解读
    if col:
        col.markdown("---")
        if interpretations:
            col.info("\n".join(interpretations))
    else:
        st.markdown("---")
        if interpretations:
            st.info("\n".join(interpretations))


def render_ff_results(
    ff_results: dict,
    col=None,
) -> None:
    """
    渲染FF因子模型结果

    Args:
        ff_results: FF模型结果字典
        col: 列对象
    """
    model_type = ff_results.get('model_type', 'ff3')
    alpha = ff_results.get('alpha', 0)
    alpha_pval = ff_results.get('alpha_pval', 1)
    r_squared = ff_results.get('r_squared', 0)
    interpretation = ff_results.get('interpretation', '')

    # Alpha颜色
    if alpha_pval < 0.05:
        alpha_color = 'green' if alpha > 0 else 'red'
    else:
        alpha_color = 'default'

    metrics = {
        '模型类型': model_type.upper(),
        'Alpha（年化）': fmt_pct(alpha),
        'Alpha显著性': f'{fmt_f((1-alpha_pval)*100, 1)}%' if alpha_pval < 1 else '不显著',
        'R²（解释力）': fmt_f(r_squared, 2),
    }

    render_metric_card(f'FF因子模型 ({model_type.upper()})', metrics, col)

    # 解读
    if interpretation:
        st.markdown(f"**模型解读：**\n{interpretation}")


def render_bond_results(
    model_results: dict,
    col=None,
) -> None:
    """
    渲染债券模型结果

    Args:
        model_results: 债券模型结果字典
        col: 列对象
    """
    duration_results = model_results.get('duration_results', {})
    three_factor_results = model_results.get('three_factor_results', {})

    # 久期归因
    duration = duration_results.get('duration', 0)
    convexity = duration_results.get('convexity', 0)
    r_squared = duration_results.get('r_squared', 0)

    metrics = {
        '久期（年）': fmt_f(duration, 2),
        '凸性': fmt_f(convexity, 2),
        'R²（解释力）': fmt_f(r_squared, 2),
    }

    render_metric_card('久期归因（T-Model）', metrics, col)

    # 三因子
    alpha = three_factor_results.get('alpha', 0)
    factor_betas = three_factor_results.get('factor_betas', {})

    metrics_3f = {
        'Alpha（年化）': fmt_pct(alpha),
    }
    if 'y2y' in factor_betas:
        metrics_3f['β_短端'] = fmt_f(factor_betas['y2y'], 2)
    if 'y10y' in factor_betas:
        metrics_3f['β_长端'] = fmt_f(factor_betas['y10y'], 2)
    if 'credit_spread' in factor_betas:
        metrics_3f['β_信用利差'] = fmt_f(factor_betas['credit_spread'], 2)

    render_metric_card('债券三因子模型', metrics_3f, col)


def render_model_results(
    model_type: str,
    model_results: dict,
    col=None,
) -> None:
    """
    渲染模型结果（根据类型选择）

    Args:
        model_type: 模型类型
        model_results: 模型结果字典
        col: 列对象
    """
    if model_type in ('equity', 'mixed', 'index', 'sector', 'qdii'):
        if 'ff_results' in model_results:
            render_ff_results(model_results['ff_results'], col)
    elif model_type == 'bond':
        render_bond_results(model_results, col)
    else:
        st.info(f"暂不支持 {model_type} 类型的深度分析")


def render_radar_scores(
    radar_scores: dict,
    col=None,
) -> None:
    """
    渲染雷达图评分

    Args:
        radar_scores: 雷达图评分字典
        col: 列对象
    """
    scores = radar_scores.get('scores', {})
    weights = radar_scores.get('weights', {})
    total_score = radar_scores.get('total_score', 0)

    # 颜色
    if total_score >= 80:
        score_color = 'green'
    elif total_score >= 60:
        score_color = 'orange'
    else:
        score_color = 'red'

    render_kpi_card(
        '综合评分',
        fmt_f(total_score, 1) + '分',
        '满分100分',
        color=score_color,
        col=col,
    )

    # 各维度得分
    metrics = {k: fmt_f(v, 1) for k, v in scores.items()}
    render_metric_card('五维评分', metrics, col)


def render_stress_test(
    stress_results: dict,
    col=None,
) -> None:
    """
    渲染压力测试结果

    Args:
        stress_results: 压力测试结果字典
        col: 列对象
    """
    worst = stress_results.get('worst_case', {})
    interpretation = stress_results.get('interpretation', '')

    metrics = {
        '最坏场景': worst.get('name', 'N/A'),
        '短端冲击': f"{worst.get('short_bp', 0)} BP",
        '长端冲击': f"{worst.get('long_bp', 0)} BP",
        '预估回撤': fmt_pct(worst.get('total_impact_pct', 0) / 100),
    }

    render_metric_card('压力测试', metrics, col)

    # 解读
    if interpretation:
        st.markdown(f"**风险提示：**\n{interpretation}")


def render_bond_holdings(
    bond_structure: dict,
    col=None,
) -> None:
    """
    渲染债券持仓结构

    Args:
        bond_structure: 债券结构字典
        col: 列对象
    """
    metrics = {
        '利率债占比': fmt_pct(bond_structure.get('rate_ratio', 0)),
        '信用债占比': fmt_pct(bond_structure.get('credit_ratio', 0)),
        '可转债占比': fmt_pct(bond_structure.get('convert_ratio', 0)),
    }

    render_metric_card('债券持仓结构', metrics, col)


def render_analysis_report(
    symbol: str,
    basic_info: dict,
    performance: dict,
    model_type: str,
    model_results: dict,
    radar_scores: dict,
) -> None:
    """
    渲染完整分析报告

    Args:
        symbol: 基金代码
        basic_info: 基本信息
        performance: 业绩指标
        model_type: 模型类型
        model_results: 模型结果
        radar_scores: 雷达图评分
    """
    # Part -1: 一句话点评(大白话解读)
    st.markdown("---")
    st.markdown("### 💬 一句话点评")

    # 从模型结果中提取解读
    one_liner = ""

    if model_type == 'equity':
        ff_results = model_results.get('ff_results', {})
        alpha = ff_results.get('alpha', 0)

        if alpha > 0.10:
            one_liner = "🏆 这是一只'明星基金',剔除市场因素后仍能创造显著超额收益,适合追求高回报的投资者。"
        elif alpha > 0.05:
            one_liner = "✅ 这是一只'优秀基金',具备持续的超额收益能力,值得长期持有。"
        elif alpha > 0:
            one_liner = "🟡 这是一只'尚可基金',小幅跑赢市场,但超额收益不够稳定。"
        else:
            one_liner = "🔴 这是一只'平庸基金',连市场平均收益都跑不赢,不如直接买指数基金。"

    elif model_type == 'bond':
        duration = model_results.get('duration', 0)
        stress = model_results.get('stress_test_results', {})
        worst_dd = stress.get('worst_case', {}).get('total_drawdown', 0)

        if duration > 3:
            one_liner = f"🟢 这是一只'长债基金',久期{duration:.2f}年,对利率变化敏感,极端情况下预计回撤{worst_dd*100:.1f}%。"
        elif duration > 1:
            one_liner = f"✅ 这是一只'中短债基金',久期{duration:.2f}年,攻守兼备,适合稳健配置。"
        else:
            one_liner = f"🟡 这是一只'短债基金',久期{duration:.2f}年,几乎不受利率影响,像货币基金一样稳定。"

    elif model_type == 'mixed':
        brinson = model_results.get('brinson_results', {})
        alloc = brinson.get('allocation', 0)

        if alloc > 0.05:
            one_liner = "🏆 这是一只'配置高手',基金经理擅长通过股债轮动创造超额收益,择时能力突出。"
        elif alloc < -0.05:
            one_liner = "🔴 这只基金的'择时能力较差',大类资产配置方向经常失误,拖累了整体表现。"
        else:
            one_liner = "✅ 这是一只'均衡配置基金',股债比例相对稳定,适合风险偏好中等的投资者。"

    elif model_type == 'index':
        one_liner = "🟢 这是一只'指数基金',紧密跟踪指数,费用低廉,适合被动投资策略。"

    else:
        one_liner = "📊 这只基金的分析数据不足,建议查看最新季报了解详情。"

    if one_liner:
        st.info(one_liner)

    # Part 0: 雷达图
    st.markdown("---")
    st.markdown("### 📊 综合评分")
    col_radar, col_scores = st.columns([1, 1])
    with col_radar:
        from ui.charts import plot_radar_chart
        fig = plot_radar_chart(
            radar_scores['scores'],
            radar_scores['weights'],
            model_type,
        )
        st.plotly_chart(fig, use_container_width=True)
    with col_scores:
        render_radar_scores(radar_scores, col=st)

    # Part 1: 基本信息
    st.markdown("---")
    st.markdown("### 📋 基金信息")
    render_basic_info(basic_info)

    # Part 2: 业绩指标
    st.markdown("---")
    st.markdown("### 📈 业绩指标")
    render_performance_metrics(performance)

    # Part 3: 模型分析
    st.markdown("---")
    st.markdown("### 🔬 模型分析")
    render_model_results(model_type, model_results)

    # Part 4: 风险提示（债券专属）
    if model_type == 'bond':
        stress_results = model_results.get('stress_test_results', {})
        if stress_results:
            st.markdown("---")
            st.markdown("### ⚠️ 风险提示")
            col_stress, col_holdings = st.columns([1, 1])
            with col_stress:
                render_stress_test(stress_results)
            with col_holdings:
                bond_structure = model_results.get('bond_structure', {})
                render_bond_holdings(bond_structure)


def render_disclaimer():
    """渲染免责声明"""
    st.markdown("""
    <div class="card card-info">
        <h4 style="margin:0 0 8px 0;">⚠️ 免责声明</h4>
        <p style="margin:0;font-size:0.85rem;color:#666;line-height:1.5;">
        本分析结果仅供参考，不构成任何投资建议。模型存在局限性，历史表现不代表未来收益。
        数据来源于公开信息，可能存在滞后或误差。投资有风险，入市需谨慎。
        </p>
    </div>
    """, unsafe_allow_html=True)
