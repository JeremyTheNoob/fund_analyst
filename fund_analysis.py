import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

# ===== 页面配置 =====
st.set_page_config(
    page_title="基金穿透式分析",
    page_icon="📊",
    layout="wide"
)

# ===== 数据获取函数（带缓存，10分钟内不重复请求） =====
@st.cache_data(ttl=600, show_spinner=False)
def get_nav_history(fund_code):
    """获取基金历史净值"""
    df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
    df['净值日期'] = pd.to_datetime(df['净值日期'])
    df['单位净值'] = pd.to_numeric(df['单位净值'], errors='coerce')
    df['日增长率'] = pd.to_numeric(df['日增长率'], errors='coerce')
    df = df.sort_values('净值日期').dropna(subset=['单位净值'])
    return df

@st.cache_data(ttl=600, show_spinner=False)
def get_fund_manager(fund_code):
    """获取基金经理信息"""
    df = ak.fund_manager_em()
    result = df[df['现任基金代码'].astype(str).str.contains(fund_code, na=False)]
    return result

@st.cache_data(ttl=600, show_spinner=False)
def get_portfolio(fund_code):
    """获取持仓数据"""
    try:
        df = ak.fund_portfolio_hold_em(symbol=fund_code, date="2024")
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_fund_basic_info(fund_code):
    """从基金日报中获取基金基本信息"""
    try:
        df = ak.fund_open_fund_daily_em()
        row = df[df['基金代码'] == fund_code]
        return row
    except:
        return pd.DataFrame()

# ===== 指标计算函数 =====
def calc_metrics(df):
    """计算夏普比率、最大回撤、年化收益率、年化波动率"""
    nav = df['单位净值'].values
    returns = pd.Series(nav).pct_change().dropna()

    # 年化收益率
    start_nav = nav[0]
    end_nav = nav[-1]
    days = (df['净值日期'].iloc[-1] - df['净值日期'].iloc[0]).days
    annual_return = (end_nav / start_nav) ** (365 / max(days, 1)) - 1

    # 年化波动率
    annual_vol = returns.std() * np.sqrt(252)

    # 最大回撤
    cummax = pd.Series(nav).cummax()
    drawdown = (pd.Series(nav) - cummax) / cummax
    max_drawdown = drawdown.min()

    # 夏普比率（无风险利率 2.5%）
    risk_free = 0.025
    sharpe = (returns.mean() * 252 - risk_free) / annual_vol if annual_vol > 0 else 0

    # 卡玛比率
    calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0

    return {
        'annual_return': annual_return,
        'annual_vol': annual_vol,
        'max_drawdown': max_drawdown,
        'sharpe': sharpe,
        'calmar': calmar,
        'total_return': (end_nav / start_nav - 1),
        'days': days
    }

# ===== 标题 =====
st.title("📊 基金穿透式分析工具")
st.markdown("输入基金代码，获取 4 个维度的深度分析报告")

# ===== 侧边栏 =====
with st.sidebar:
    st.header("🔍 基金查询")
    fund_code = st.text_input(
        "基金代码（6位数字）",
        value="014416",
        max_chars=6,
        help="例如：014416 泰康研究精选股票"
    )
    run = st.button("🚀 开始分析", type="primary", use_container_width=True)
    st.markdown("---")
    st.caption("📌 数据来源：AkShare")
    st.caption("⚠️ 仅供学习参考，不构成投资建议")

# ===== 欢迎页 =====
if not run:
    st.info("👈 请在左侧输入基金代码，点击「开始分析」")
    cols = st.columns(4)
    with cols[0]:
        st.markdown("#### 📈 维度一\n**风险与收益**\n\n夏普比率、最大回撤、年化收益、波动率")
    with cols[1]:
        st.markdown("#### 👤 维度二\n**基金经理**\n\n任职年限、历史回报、能力雷达图")
    with cols[2]:
        st.markdown("#### 📦 维度三\n**持仓底牌**\n\n前十大重仓股、集中度、持仓分布图")
    with cols[3]:
        st.markdown("#### 💰 维度四\n**成本与规则**\n\n费率结构、分红方式、申赎状态")
    st.stop()

# ===== 获取数据 =====
with st.spinner("📡 正在获取基金数据..."):
    try:
        nav_df = get_nav_history(fund_code)
        if nav_df.empty:
            st.error(f"未找到基金代码 {fund_code} 的数据，请确认代码是否正确")
            st.stop()
    except Exception as e:
        st.error(f"获取净值数据失败：{e}")
        st.stop()

    try:
        manager_df = get_fund_manager(fund_code)
    except:
        manager_df = pd.DataFrame()

    try:
        portfolio_df = get_portfolio(fund_code)
    except:
        portfolio_df = pd.DataFrame()

    try:
        basic_df = get_fund_basic_info(fund_code)
    except:
        basic_df = pd.DataFrame()

# ===== 计算指标 =====
metrics = calc_metrics(nav_df)

# ===== 基金概览 =====
st.success("✅ 数据获取成功")
st.markdown("---")

# 获取基金名称和状态
fund_name = "—"
buy_status = "—"
sell_status = "—"
fee = "—"

if not basic_df.empty:
    row = basic_df.iloc[0]
    fund_name = row.get('基金简称', '—')
    buy_status = row.get('申购状态', '—')
    sell_status = row.get('赎回状态', '—')
    fee = row.get('手续费', '—')

overview_cols = st.columns(5)
overview_cols[0].metric("基金名称", fund_name)
overview_cols[1].metric("基金代码", fund_code)
overview_cols[2].metric("最新净值", f"{nav_df['单位净值'].iloc[-1]:.4f}")
overview_cols[3].metric("申购状态", buy_status)
overview_cols[4].metric("赎回状态", sell_status)

st.markdown("---")

# ===================================================
# 维度 1：风险与收益平衡
# ===================================================
st.header("📈 维度一：风险与收益平衡")

left, right = st.columns([3, 1])

with left:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=nav_df['净值日期'],
        y=nav_df['单位净值'],
        mode='lines',
        name='单位净值',
        line=dict(color='#E84747', width=2),
        fill='tozeroy',
        fillcolor='rgba(232,71,71,0.08)'
    ))
    fig.update_layout(
        title="历史净值走势",
        xaxis_title="日期",
        yaxis_title="净值",
        hovermode='x unified',
        height=350,
        margin=dict(t=40, b=20)
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("#### 核心指标")

    ar = metrics['annual_return']
    av = metrics['annual_vol']
    md = metrics['max_drawdown']
    sr = metrics['sharpe']
    cr = metrics['calmar']
    tr = metrics['total_return']

    # 用颜色区分正负
    ar_delta = f"{'↑' if ar >= 0 else '↓'}"
    st.metric("年化收益率", f"{ar*100:.2f}%", delta=ar_delta, delta_color="normal" if ar >= 0 else "inverse")
    st.metric("年化波动率", f"{av*100:.2f}%")
    st.metric("最大回撤", f"{md*100:.2f}%")
    st.metric("夏普比率", f"{sr:.2f}")
    st.metric("卡玛比率", f"{cr:.2f}")
    st.metric("成立以来总收益", f"{tr*100:.2f}%")

    # 风险评级
    if av < 0.10:
        risk_label, risk_color = "低风险 🟢", "success"
    elif av < 0.20:
        risk_label, risk_color = "中低风险 🟡", "info"
    elif av < 0.30:
        risk_label, risk_color = "中高风险 🟠", "warning"
    else:
        risk_label, risk_color = "高风险 🔴", "error"

    getattr(st, risk_color)(f"风险等级：{risk_label}")

st.markdown("---")

# ===================================================
# 维度 2：基金经理及其风格
# ===================================================
st.header("👤 维度二：基金经理及其风格")

if not manager_df.empty:
    m = manager_df.iloc[0]
    manager_name = m.get('姓名', '—')
    company = m.get('所属公司', '—')
    tenure_days_str = str(m.get('累计从业时间', '0'))
    tenure_days = int(''.join(filter(str.isdigit, tenure_days_str))) if tenure_days_str else 0
    tenure_years = tenure_days / 365
    scale = m.get('现任基金资产总规模', '—')
    best_return = m.get('现任基金最佳回报', '—')

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("基金经理", manager_name)
    m2.metric("所属公司", company)
    m3.metric("从业年限", f"{tenure_years:.1f} 年")
    m4.metric("管理规模", f"{scale} 亿")

    # 能力雷达图
    st.markdown("#### 基金经理能力评估")

    tenure_score = min(tenure_years / 10 * 100, 100)
    scale_val = float(str(scale).replace('亿', '').strip()) if scale != '—' else 10
    scale_score = min(scale_val / 100 * 100, 100)
    return_score = min(max(metrics['annual_return'] * 100 + 50, 0), 100)
    drawdown_score = max(0, 100 + metrics['max_drawdown'] * 200)
    sharpe_score = min(max(metrics['sharpe'] * 30 + 50, 0), 100)

    categories = ['任职年限', '管理规模', '收益能力', '回撤控制', '夏普表现']
    values = [tenure_score, scale_score, return_score, drawdown_score, sharpe_score]

    fig2 = go.Figure(go.Scatterpolar(
        r=values + [values[0]],
        theta=categories + [categories[0]],
        fill='toself',
        fillcolor='rgba(232,71,71,0.15)',
        line=dict(color='#E84747', width=2)
    ))
    fig2.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        height=380,
        margin=dict(t=20, b=20)
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("⚠️ 未找到该基金的经理信息")

st.markdown("---")

# ===================================================
# 维度 3：持仓底牌
# ===================================================
st.header("📦 维度三：持仓底牌")

if not portfolio_df.empty:
    top10 = portfolio_df.head(10).copy()
    top10['占净值比例'] = pd.to_numeric(top10['占净值比例'], errors='coerce')

    p1, p2 = st.columns([1, 1])

    with p1:
        st.markdown("#### 前十大重仓股")
        display_cols = ['股票代码', '股票名称', '占净值比例', '持股数', '持仓市值']
        available_cols = [c for c in display_cols if c in top10.columns]
        st.dataframe(top10[available_cols], hide_index=True, use_container_width=True)

    with p2:
        st.markdown("#### 重仓股分布")
        fig3 = px.pie(
            top10,
            values='占净值比例',
            names='股票名称',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig3.update_traces(textposition='inside', textinfo='percent+label')
        fig3.update_layout(height=380, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    # 集中度分析
    top10_ratio = top10['占净值比例'].sum()
    top3_ratio = top10['占净值比例'].head(3).sum()
    top1_ratio = top10['占净值比例'].head(1).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("第一大持仓占比", f"{top1_ratio:.2f}%")
    c2.metric("前三大持仓占比", f"{top3_ratio:.2f}%")
    c3.metric("前十大持仓占比", f"{top10_ratio:.2f}%")

    if top10_ratio < 30:
        st.success("持仓分散 🟢：前十大集中度低，风险较分散")
    elif top10_ratio < 50:
        st.info("持仓适中 🟡：前十大集中度适中")
    elif top10_ratio < 70:
        st.warning("持仓集中 🟠：前十大集中度偏高，关注个股风险")
    else:
        st.error("持仓高度集中 🔴：前十大占比超 70%，个股风险暴露大")
else:
    st.warning("⚠️ 未找到该基金的持仓数据（部分基金不公开详细持仓）")

st.markdown("---")

# ===================================================
# 维度 4：成本与规则
# ===================================================
st.header("💰 维度四：成本与规则")

f1, f2, f3, f4 = st.columns(4)
f1.metric("手续费（折后）", fee if fee != '—' else '见基金合同')
f2.metric("申购状态", buy_status)
f3.metric("赎回状态", sell_status)
f4.metric("数据更新日期", str(nav_df['净值日期'].iloc[-1])[:10])

st.markdown("#### 费率说明")
st.info("""
| 费用项目 | 说明 |
|--------|------|
| **管理费** | 从基金资产中按日扣除，每年约 0.5%~1.5% |
| **托管费** | 从基金资产中按日扣除，每年约 0.1%~0.25% |
| **申购费** | 购买时一次性收取，网上通常打一折 |
| **赎回费** | 持有越久费率越低，一般持有 2 年以上免赎回费 |
""")

st.markdown("#### 分红方式建议")
col_div1, col_div2 = st.columns(2)
with col_div1:
    st.markdown("""
**💰 现金分红**
- 将收益以现金形式打入账户
- 适合需要定期现金流的投资者
- 注意：分红后净值会相应降低
""")
with col_div2:
    st.markdown("""
**📈 红利再投资**
- 将分红自动买入更多基金份额
- 享受复利增长效应
- 适合长期持有、不需要现金的投资者
""")

st.markdown("---")

# ===================================================
# 综合评分
# ===================================================
st.header("💡 综合评估")

score_r = min(max(metrics['annual_return'] * 100 + 30, 0), 40)   # 收益 最高40分
score_s = min(max(metrics['sharpe'] * 15 + 20, 0), 30)           # 夏普 最高30分
score_d = min(max((1 + metrics['max_drawdown']) * 30, 0), 30)    # 回撤 最高30分
total_score = score_r + score_s + score_d

if total_score >= 80:
    rating = "⭐⭐⭐⭐⭐ 优秀"
    rating_color = "success"
elif total_score >= 60:
    rating = "⭐⭐⭐⭐ 良好"
    rating_color = "info"
elif total_score >= 40:
    rating = "⭐⭐⭐ 一般"
    rating_color = "warning"
else:
    rating = "⭐⭐ 较差"
    rating_color = "error"

sc1, sc2 = st.columns([1, 2])
with sc1:
    st.metric("综合评分", f"{total_score:.0f} / 100")
    getattr(st, rating_color)(f"综合评级：{rating}")

with sc2:
    st.markdown("#### 🎯 投资关注点")
    tips = []
    if metrics['annual_vol'] > 0.25:
        tips.append("⚠️ 波动较大，建议做好仓位控制")
    else:
        tips.append("✅ 波动适中，适合稳健配置")
    if metrics['sharpe'] > 1:
        tips.append("✅ 夏普比率 > 1，风险收益性价比高")
    else:
        tips.append("⚠️ 夏普比率偏低，注意风险调整后收益")
    if metrics['max_drawdown'] < -0.30:
        tips.append("⚠️ 历史最大回撤超 30%，心理承受能力需较强")
    else:
        tips.append("✅ 回撤控制在合理范围内")
    if not manager_df.empty and tenure_years >= 5:
        tips.append("✅ 基金经理经验丰富（5年以上）")
    elif not manager_df.empty:
        tips.append("⚠️ 基金经理任职时间较短，稳定性待观察")
    for tip in tips:
        st.markdown(tip)

# ===== 底部 =====
st.markdown("---")
st.caption("📊 数据来源：AkShare | 本工具仅供学习参考，不构成任何投资建议 | 投资有风险，入市需谨慎")
