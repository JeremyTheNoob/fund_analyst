import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

# ===== 页面配置 =====
st.set_page_config(
    page_title="基金穿透式分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ===== 数据获取函数（带缓存，10分钟内不重复请求） =====
@st.cache_data(ttl=600, show_spinner=False)
def get_nav_history(fund_code):
    """获取基金历史净值"""
    try:
        df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        df['净值日期'] = pd.to_datetime(df['净值日期'])
        df['单位净值'] = pd.to_numeric(df['单位净值'], errors='coerce')
        df['日增长率'] = pd.to_numeric(df['日增长率'], errors='coerce')
        df = df.sort_values('净值日期').dropna(subset=['单位净值'])
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_fund_detail(fund_code):
    """获取基金详细信息（规模、费率、成立时间等）"""
    try:
        df = ak.fund_individual_detail_info_xq(symbol=fund_code)
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_fund_manager(fund_code):
    """获取基金经理信息"""
    try:
        df = ak.fund_manager_em()
        result = df[df['现任基金代码'].astype(str).str.contains(fund_code, na=False)]
        return result
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_portfolio(fund_code):
    """获取持仓数据"""
    try:
        # 尝试获取最近几个季度的数据
        quarters = []
        for year in [2024, 2023]:
            for quarter in ['0630', '0331', '1231', '0930']:
                try:
                    df = ak.fund_portfolio_hold_em(symbol=fund_code, date=f"{year}{quarter}")
                    if not df.empty:
                        df['报告期'] = f"{year}-{quarter[:2]}-{quarter[2:]}"
                        quarters.append(df)
                except:
                    continue
        if quarters:
            return pd.concat(quarters, ignore_index=True).sort_values('报告期', ascending=False)
        return pd.DataFrame()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_portfolio_industry(fund_code):
    """获取行业配置数据"""
    try:
        df = ak.fund_portfolio_industry_allocation_em(symbol=fund_code, date="2024")
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

@st.cache_data(ttl=3600, show_spinner=False)
def get_benchmark_index(index_code):
    """获取基准指数数据（用于计算Alpha）"""
    try:
        # 默认使用沪深300作为基准
        if index_code is None:
            index_code = "sh000300"
        df = ak.index_zh_a_hist(symbol=index_code, period="daily", start_date="20100101")
        df['日期'] = pd.to_datetime(df['日期'])
        df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
        df = df.sort_values('日期').dropna(subset=['收盘'])
        return df
    except:
        return pd.DataFrame()

# ===== 指标计算函数 =====
def calc_enhanced_metrics(nav_df, fund_code=None):
    """计算增强版指标：年化收益、波动率、最大回撤、夏普、卡玛、Alpha"""
    nav = nav_df['单位净值'].values
    returns = pd.Series(nav).pct_change().dropna()
    
    # 年化收益率
    start_nav = nav[0]
    end_nav = nav[-1]
    days = (nav_df['净值日期'].iloc[-1] - nav_df['净值日期'].iloc[0]).days
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
    
    # 卡玛比率（年化收益 / |最大回撤|）
    calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
    
    # Alpha计算（相对于基准）
    alpha = 0
    beta = 1
    try:
        benchmark_df = get_benchmark_index(None)
        if not benchmark_df.empty:
            # 合并基金净值和基准指数数据
            fund_nav_scaled = pd.DataFrame({
                'date': nav_df['净值日期'],
                'fund_return': returns
            }).set_index('date')
            
            benchmark_returns = benchmark_df.set_index('日期')['收盘'].pct_change().dropna()
            
            # 合并数据
            merged = pd.DataFrame({
                'fund': fund_nav_scaled['fund_return'],
                'benchmark': benchmark_returns
            }).dropna()
            
            if len(merged) > 30:
                # 计算Beta
                covariance = merged['fund'].cov(merged['benchmark'])
                benchmark_variance = merged['benchmark'].var()
                beta = covariance / benchmark_variance if benchmark_variance != 0 else 1
                
                # 计算Alpha (年化)
                alpha = (merged['fund'].mean() - beta * merged['benchmark'].mean()) * 252
    except Exception as e:
        pass
    
    return {
        'annual_return': annual_return,
        'annual_vol': annual_vol,
        'max_drawdown': max_drawdown,
        'sharpe': sharpe,
        'calmar': calmar,
        'alpha': alpha,
        'beta': beta,
        'total_return': (end_nav / start_nav - 1),
        'days': days
    }

def calc_turnover_rate(portfolio_df):
    """计算持仓换手率"""
    if len(portfolio_df) < 2:
        return 0
    
    # 按报告期分组
    quarters = portfolio_df['报告期'].unique()
    if len(quarters) < 2:
        return 0
    
    # 计算相邻季度持仓变化
    turnover_list = []
    for i in range(len(quarters) - 1):
        q1 = portfolio_df[portfolio_df['报告期'] == quarters[i]]
        q2 = portfolio_df[portfolio_df['报告期'] == quarters[i + 1]]
        
        if not q1.empty and not q2.empty:
            stocks_q1 = set(q1['股票名称'].unique())
            stocks_q2 = set(q2['股票名称'].unique())
            
            # 换手率 = (新增 + 清空) / 2 / 10
            new_stocks = len(stocks_q2 - stocks_q1)
            removed_stocks = len(stocks_q1 - stocks_q2)
            turnover = (new_stocks + removed_stocks) / 20
            turnover_list.append(turnover)
    
    return np.mean(turnover_list) * 100 if turnover_list else 0

def analyze_industry_concentration(industry_df):
    """分析行业集中度"""
    if industry_df.empty:
        return {'top1': 0, 'top3': 0, 'top5': 0, 'is_concentrated': False}
    
    industry_df['占净值比例'] = pd.to_numeric(industry_df['占净值比例'], errors='coerce')
    top_industries = industry_df.nlargest(5, '占净值比例')
    
    return {
        'top1': top_industries['占净值比例'].iloc[0] if len(top_industries) > 0 else 0,
        'top3': top_industries['占净值比例'].head(3).sum() if len(top_industries) >= 3 else 0,
        'top5': top_industries['占净值比例'].sum(),
        'top_industries': top_industries.head(5)
    }

# ===== 标题 =====
st.title("📊 基金深度分析工具")
st.markdown("基于 AkShare 数据源，5 维度穿透式分析")

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
    cols = st.columns(5)
    with cols[0]:
        st.markdown("#### 📋 身份扫描\n规模、费率、成立时间")
    with cols[1]:
        st.markdown("#### 📈 性价比分析\nAlpha、夏普、卡玛比率")
    with cols[2]:
        st.markdown("#### 👤 经理灵魂拷问\n风格稳定性、回撤控制")
    with cols[3]:
        st.markdown("#### 📦 持仓透视镜\n集中度、换手率")
    with cols[4]:
        st.markdown("#### 🎯 归因分析\nBeta、规模、风格因子")
    st.stop()

# ===== 获取数据 =====
progress_bar = st.progress(0, text="正在获取数据...")

try:
    nav_df = get_nav_history(fund_code)
    progress_bar.progress(20, text="获取净值数据...")
    
    if nav_df.empty:
        st.error(f"未找到基金代码 {fund_code} 的数据，请确认代码是否正确")
        st.stop()
except Exception as e:
    st.error(f"获取净值数据失败：{e}")
    st.stop()

try:
    detail_df = get_fund_detail(fund_code)
    progress_bar.progress(40, text="获取详细信息...")
except:
    detail_df = pd.DataFrame()

try:
    manager_df = get_fund_manager(fund_code)
    progress_bar.progress(60, text="获取经理信息...")
except:
    manager_df = pd.DataFrame()

try:
    portfolio_df = get_portfolio(fund_code)
    progress_bar.progress(80, text="获取持仓数据...")
except:
    portfolio_df = pd.DataFrame()

try:
    industry_df = get_portfolio_industry(fund_code)
    progress_bar.progress(90, text="获取行业配置...")
except:
    industry_df = pd.DataFrame()

try:
    basic_df = get_fund_basic_info(fund_code)
    progress_bar.progress(100, text="数据获取完成")
except:
    basic_df = pd.DataFrame()

progress_bar.empty()

# ===== 计算指标 =====
metrics = calc_enhanced_metrics(nav_df, fund_code)
turnover_rate = calc_turnover_rate(portfolio_df) if not portfolio_df.empty else 0
industry_concentration = analyze_industry_concentration(industry_df)

# ===== 基金概览 =====
st.success("✅ 数据获取成功")
st.markdown("---")

# 获取基金名称和状态
fund_name = "—"
fund_type = "—"
fund_size = "—"
establish_date = "—"
management_fee = "—"
custody_fee = "—"
buy_status = "—"
sell_status = "—"

if not detail_df.empty:
    row = detail_df.iloc[0] if len(detail_df) > 0 else None
    if row is not None:
        fund_name = row.get('基金简称', '—')
        fund_type = row.get('基金类型', '—')
        fund_size = row.get('基金规模', '—')
        establish_date = row.get('成立日期', '—')
        management_fee = row.get('管理费率', '—')
        custody_fee = row.get('托管费率', '—')

if not basic_df.empty:
    row = basic_df.iloc[0]
    if fund_name == "—":
        fund_name = row.get('基金简称', '—')
    buy_status = row.get('申购状态', '—')
    sell_status = row.get('赎回状态', '—')

overview_cols = st.columns(6)
overview_cols[0].metric("基金名称", fund_name)
overview_cols[1].metric("基金代码", fund_code)
overview_cols[2].metric("最新净值", f"{nav_df['单位净值'].iloc[-1]:.4f}")
overview_cols[3].metric("基金规模", fund_size)
overview_cols[4].metric("申购状态", buy_status)
overview_cols[5].metric("赎回状态", sell_status)

st.markdown("---")

# ===================================================
# 维度 1：基金身份扫描
# ===================================================
st.header("📋 维度一：基金身份扫描")

c1, c2, c3, c4 = st.columns(4)
c1.metric("基金类型", fund_type)
c2.metric("成立日期", establish_date)
c3.metric("管理费率", management_fee if management_fee != "—" else "见合同")
c4.metric("托管费率", custody_fee if custody_fee != "—" else "见合同")

if fund_size != "—":
    try:
        size_value = float(str(fund_size).replace('亿', '').replace('万', '').strip())
        if size_value > 500:
            st.warning(f"⚠️ 基金规模较大（{fund_size}），需警惕风格漂移风险")
        elif size_value < 2:
            st.warning(f"⚠️ 基金规模较小（{fund_size}），可能面临清盘风险")
        else:
            st.success(f"✅ 基金规模适中（{fund_size}），运作稳定")
    except:
        pass

st.markdown("---")

# ===================================================
# 维度 2：收益与风险的"性价比"
# ===================================================
st.header("📈 维度二：收益与风险的性价比")

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
    alpha = metrics['alpha']
    beta = metrics['beta']
    tr = metrics['total_return']
    
    ar_delta = f"{'↑' if ar >= 0 else '↓'}"
    st.metric("年化收益率", f"{ar*100:.2f}%", delta=ar_delta, delta_color="normal" if ar >= 0 else "inverse")
    st.metric("超额收益(Alpha)", f"{alpha*100:.2f}%")
    st.metric("年化波动率", f"{av*100:.2f}%")
    st.metric("最大回撤", f"{md*100:.2f}%")
    st.metric("夏普比率", f"{sr:.2f}")
    st.metric("卡玛比率", f"{cr:.2f}")
    st.metric("Beta系数", f"{beta:.2f}")
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
# 维度 3：持仓"透视镜"
# ===================================================
st.header("📦 维度三：持仓透视镜")

if not portfolio_df.empty:
    latest_portfolio = portfolio_df[portfolio_df['报告期'] == portfolio_df['报告期'].iloc[0]].head(10).copy()
    latest_portfolio['占净值比例'] = pd.to_numeric(latest_portfolio['占净值比例'], errors='coerce')
    
    p1, p2 = st.columns([1, 1])
    
    with p1:
        st.markdown("#### 前十大重仓股")
        display_cols = ['股票代码', '股票名称', '占净值比例']
        available_cols = [c for c in display_cols if c in latest_portfolio.columns]
        if '报告期' in latest_portfolio.columns:
            st.caption(f"报告期：{latest_portfolio['报告期'].iloc[0]}")
        st.dataframe(latest_portfolio[available_cols], hide_index=True, use_container_width=True)
    
    with p2:
        st.markdown("#### 重仓股分布")
        fig3 = px.pie(
            latest_portfolio,
            values='占净值比例',
            names='股票名称',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig3.update_traces(textposition='inside', textinfo='percent+label')
        fig3.update_layout(height=380, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)
    
    # 集中度分析
    top10_ratio = latest_portfolio['占净值比例'].sum()
    top3_ratio = latest_portfolio['占净值比例'].head(3).sum()
    top1_ratio = latest_portfolio['占净值比例'].head(1).sum()
    
    c1, c2, c3 = st.columns(3)
    c1.metric("第一大持仓占比", f"{top1_ratio:.2f}%")
    c2.metric("前三大持仓占比", f"{top3_ratio:.2f}%")
    c3.metric("前十大持仓占比", f"{top10_ratio:.2f}%")
    
    # 换手率分析
    st.markdown("#### 持仓风格分析")
    if turnover_rate > 70:
        st.warning(f"⚠️ 高换手率（{turnover_rate:.1f}%）：短线博弈风格，交易成本较高")
    elif turnover_rate > 40:
        st.info(f"🟡 中等换手率（{turnover_rate:.1f}%）：中等风格，适度调整持仓")
    else:
        st.success(f"✅ 低换手率（{turnover_rate:.1f}%）：长线持有风格，交易成本低")
    
    if not industry_df.empty:
        st.markdown("#### 行业配置分析")
        ind = industry_concentration
        ind_top = ind.get('top_industries', pd.DataFrame())
        
        if not ind_top.empty:
            i1, i2, i3 = st.columns(3)
            i1.metric("第一大行业占比", f"{ind['top1']:.2f}%")
            i2.metric("前三行业占比", f"{ind['top3']:.2f}%")
            i3.metric("前五行业占比", f"{ind['top5']:.2f}%")
            
            # 行业配置图
            fig4 = px.bar(
                ind_top.head(10),
                x='占净值比例',
                y='行业名称',
                orientation='h',
                color_discrete_sequence=px.colors.sequential.Reds
            )
            fig4.update_layout(
                title="行业配置 Top 10",
                xaxis_title="占净值比例 (%)",
                yaxis_title="行业",
                height=400,
                margin=dict(t=40, b=20)
            )
            st.plotly_chart(fig4, use_container_width=True)
            
            # 行业集中度提示
            if ind['top1'] > 30:
                st.warning(f"⚠️ 行业高度集中：第一大行业占比 {ind['top1']:.2f}%，存在赛道风险")
            elif ind['top5'] > 70:
                st.info(f"🟡 行业相对集中：前五大行业占比 {ind['top5']:.2f}%")
            else:
                st.success(f"✅ 行业配置分散：前五大行业占比 {ind['top5']:.2f}%")
else:
    st.warning("⚠️ 未找到该基金的持仓数据（部分基金不公开详细持仓）")

st.markdown("---")

# ===================================================
# 维度 4：基金经理"灵魂拷问"
# ===================================================
st.header("👤 维度四：基金经理灵魂拷问")

if not manager_df.empty:
    m = manager_df.iloc[0]
    manager_name = m.get('姓名', '—')
    company = m.get('所属公司', '—')
    tenure_days_str = str(m.get('累计从业时间', '0'))
    tenure_days = int(''.join(filter(str.isdigit, tenure_days_str))) if tenure_days_str else 0
    tenure_years = tenure_days / 365
    scale = m.get('现任基金资产总规模', '—')
    best_return = m.get('现任基金最佳回报', '—')
    funds_count = m.get('任职基金只数', '—')
    
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
    turnover_score = max(0, 100 - turnover_rate)
    
    categories = ['任职年限', '管理规模', '收益能力', '回撤控制', '夏普表现', '持仓稳定']
    values = [tenure_score, scale_score, return_score, drawdown_score, sharpe_score, turnover_score]
    
    fig2 = go.Figure(go.Scatterpolar(
        r=values + [values[0]],
        theta=categories + [categories[0]],
        fill='toself',
        fillcolor='rgba(232,71,71,0.15)',
        line=dict(color='#E84747', width=2)
    ))
    fig2.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        height=400,
        margin=dict(t=20, b=20)
    )
    st.plotly_chart(fig2, use_container_width=True)
    
    # 历史表现分析
    st.markdown("#### 历史表现分析")
    
    # 检查大熊市表现（2018、2022）
    crash_years = [2018, 2022]
    crash_performance = []
    
    for year in crash_years:
        year_nav = nav_df[nav_df['净值日期'].dt.year == year]
        if not year_nav.empty:
            year_return = (year_nav['单位净值'].iloc[-1] / year_nav['单位净值'].iloc[0] - 1) * 100
            crash_performance.append({
                'year': year,
                'return': year_return
            })
    
    if crash_performance:
        c1, c2 = st.columns(2)
        for cp in crash_performance:
            year = cp['year']
            ret = cp['return']
            if ret > -10:
                msg = f"✅ {year}年表现优异：{ret:.2f}%（同期市场大跌，经理风控能力突出）"
                c1 if year == 2018 else c2.success(msg)
            elif ret > -25:
                msg = f"🟡 {year}年表现中等：{ret:.2f}%（低于市场跌幅，有一定抗跌能力）"
                c1 if year == 2018 else c2.warning(msg)
            else:
                msg = f"⚠️ {year}年表现较差：{ret:.2f}%（回撤较大，需关注经理的回撤控制）"
                c1 if year == 2018 else c2.error(msg)
    
    # 从业年限评价
    if tenure_years >= 10:
        st.success(f"✅ 从业{tenure_years:.0f}年，经验丰富，经历过完整牛熊周期")
    elif tenure_years >= 5:
        st.info(f"🟡 从业{tenure_years:.1f}年，有一定经验，但需观察更长期表现")
    else:
        st.warning(f"⚠️ 从业{tenure_years:.1f}年，经验相对较短，稳定性待观察")
else:
    st.warning("⚠️ 未找到该基金的经理信息")

st.markdown("---")

# ===================================================
# 维度 5：归因分析 (Style & Attribution)
# ===================================================
st.header("🎯 维度五：归因分析")

beta = metrics['beta']
alpha = metrics['alpha']

b1, b2 = st.columns(2)
b1.metric("Beta系数", f"{beta:.2f}", 
         "Beta>1: 跟涨跟跌 | Beta<1: 相对稳健" if beta >= 1 else "Beta<1: 防御性强 | Beta>1: 攻击性强")
b2.metric("Alpha收益", f"{alpha*100:.2f}%", 
         "正Alpha: 跑赢基准 | 负Alpha: 跑输基准" if alpha >= 0 else "负Alpha: 跑输基准 | 正Alpha: 跑赢基准")

st.markdown("#### 收益来源解析")

# 基于Beta和Alpha分解收益
total_return = metrics['annual_return']
market_return = total_return * beta
excess_return = alpha

if beta > 1.1:
    st.warning(f"⚠️ 高Beta（{beta:.2f}）：基金走势受市场影响大，牛市时涨幅更高，熊市时回撤也更大")
elif beta < 0.9:
    st.success(f"✅ 低Beta（{beta:.2f}）：基金相对抗跌，适合风险厌恶型投资者")
else:
    st.info(f"🟡 中等Beta（{beta:.2f}）：基金走势与市场基本同步")

if alpha > 0.05:
    st.success(f"✅ 正Alpha（{alpha*100:.2f}%）：基金经理创造超额收益能力强")
elif alpha < -0.02:
    st.warning(f"⚠️ 负Alpha（{alpha*100:.2f}%）：基金经理持续跑输基准，需谨慎")
else:
    st.info(f"🟡 Alpha接近0（{alpha*100:.2f}%）：基金经理基本复制基准，未创造显著超额收益")

st.markdown("#### 因子暴露分析（基于持仓特征推断）")

# 基于行业配置推断风格因子
if not industry_df.empty and not industry_concentration.get('top_industries', pd.DataFrame()).empty:
    top_industry = industry_concentration['top_industries'].iloc[0]['行业名称'] if len(industry_concentration['top_industries']) > 0 else "—"
    
    # 简单的风格推断逻辑
    style_factors = []
    
    # 成长vs价值
    if '科技' in top_industry or '半导体' in top_industry or '新能源' in top_industry or '医药' in top_industry:
        style_factors.append("🎯 成长风格：偏好高成长赛道，波动较大但弹性强")
    elif '银行' in top_industry or '地产' in top_industry or '公用' in top_industry:
        style_factors.append("💎 价值风格：偏好低估值板块，防守性强但爆发力一般")
    else:
        style_factors.append("⚖️ 均衡风格：成长与价值并重")
    
    # 市值偏好
    if '大盘' in top_industry:
        style_factors.append("📊 大盘风格：偏好大盘蓝筹，流动性好但弹性一般")
    elif '中小' in top_industry or '成长' in top_industry:
        style_factors.append("🚀 小盘风格：偏好中小盘，弹性大但流动性风险高")
    
    for factor in style_factors:
        st.markdown(factor)
else:
    st.info("📝 风格因子分析需要完整的行业配置数据，当前数据不足")

st.markdown("---")

# ===================================================
# 综合评分
# ===================================================
st.header("💡 综合评估")

# 更新评分逻辑，加入更多维度
score_r = min(max(metrics['annual_return'] * 100 + 30, 0), 25)      # 收益 25分
score_s = min(max(metrics['sharpe'] * 15 + 20, 0), 20)               # 夏普 20分
score_d = min(max((1 + metrics['max_drawdown']) * 25, 0), 20)        # 回撤 20分
score_a = min(max(metrics['alpha'] * 100 + 50, 0), 15)               # Alpha 15分
score_t = max(0, 20 - turnover_rate / 5)                              # 换手率 20分

total_score = score_r + score_s + score_d + score_a + score_t

if total_score >= 85:
    rating = "⭐⭐⭐⭐⭐ 卓越"
    rating_color = "success"
elif total_score >= 70:
    rating = "⭐⭐⭐⭐ 优选"
    rating_color = "info"
elif total_score >= 55:
    rating = "⭐⭐⭐ 一般"
    rating_color = "warning"
elif total_score >= 40:
    rating = "⭐⭐ 较差"
    rating_color = "error"
else:
    rating = "⭐ 不推荐"
    rating_color = "error"

sc1, sc2 = st.columns([1, 2])
with sc1:
    st.metric("综合评分", f"{total_score:.0f} / 100")
    getattr(st, rating_color)(f"综合评级：{rating}")
    
    # 评分明细
    with st.expander("📊 评分明细"):
        st.write(f"- 收益能力：{score_r:.0f}/25（年化收益率{ar*100:.2f}%，Alpha{alpha*100:.2f}%）")
        st.write(f"- 风险调整收益：{score_s:.0f}/20（夏普比率{sr:.2f}）")
        st.write(f"- 回撤控制：{score_d:.0f}/20（最大回撤{md*100:.2f}%，卡玛比率{cr:.2f}）")
        st.write(f"- 超额收益：{score_a:.0f}/15（Alpha{alpha*100:.2f}%）")
        st.write(f"- 持仓稳定：{score_t:.0f}/20（换手率{turnover_rate:.1f}%）")

with sc2:
    st.markdown("#### 🎯 投资关注点")
    tips = []
    
    # 收益分析
    if metrics['annual_return'] < 0:
        tips.append("⚠️ 近期收益为负，需观察后续表现")
    elif metrics['annual_return'] > 0.15:
        tips.append("✅ 收益表现优异，需关注持续性")
    
    # 波动分析
    if metrics['annual_vol'] > 0.25:
        tips.append("⚠️ 波动较大，建议做好仓位控制")
    else:
        tips.append("✅ 波动适中，适合稳健配置")
    
    # 夏普分析
    if metrics['sharpe'] > 1.5:
        tips.append("✅ 夏普比率 > 1.5，风险收益性价比突出")
    elif metrics['sharpe'] > 1:
        tips.append("✅ 夏普比率 > 1，风险收益性价比高")
    else:
        tips.append("⚠️ 夏普比率偏低，注意风险调整后收益")
    
    # Alpha分析
    if metrics['alpha'] > 0.05:
        tips.append("✅ Alpha显著，基金经理选股能力强")
    elif metrics['alpha'] < 0:
        tips.append("⚠️ Alpha为负，持续跑输基准")
    
    # 回撤分析
    if metrics['max_drawdown'] < -0.40:
        tips.append("⚠️ 历史最大回撤超 40%，心理承受能力需较强")
    elif metrics['max_drawdown'] < -0.30:
        tips.append("⚠️ 历史最大回撤超 30%，需评估自身承受能力")
    else:
        tips.append("✅ 回撤控制在合理范围内")
    
    # 换手率分析
    if turnover_rate > 70:
        tips.append("⚠️ 高换手率，关注交易成本")
    
    # 规模分析
    try:
        size_value = float(str(fund_size).replace('亿', '').strip())
        if size_value > 500:
            tips.append("⚠️ 规模过大，需警惕风格漂移")
    except:
        pass
    
    # 经理分析
    if not manager_df.empty and tenure_years >= 5:
        tips.append("✅ 基金经理经验丰富（5年以上）")
    elif not manager_df.empty:
        tips.append("⚠️ 基金经理任职时间较短，稳定性待观察")
    
    # 集中度分析
    if top10_ratio > 70:
        tips.append("⚠️ 持仓高度集中，个股风险暴露大")
    elif top10_ratio < 30:
        tips.append("✅ 持仓分散，风险相对可控")
    
    for tip in tips:
        st.markdown(tip)

# ===== 底部 =====
st.markdown("---")
st.caption("📊 数据来源：AkShare | 本工具仅供学习参考，不构成任何投资建议 | 投资有风险，入市需谨慎")
