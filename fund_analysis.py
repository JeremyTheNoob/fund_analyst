import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import statsmodels.api as sm

# ===== 页面配置 =====
st.set_page_config(
    page_title="基金深度分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ===================================================
# 基础数据获取函数
# ===================================================

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
    """获取基金详细信息"""
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
    """获取持仓数据（多个季度）"""
    try:
        quarters = []
        for year in [2024, 2023]:
            for quarter in ['0630', '0331', '1231', '0930']:
                try:
                    df = ak.fund_portfolio_hold_em(symbol=fund_code, date=f"{year}{quarter}")
                    if not df.empty:
                        df['报告期'] = f"{year}-{quarter[:2]}-{quarter[2:]}"
                        df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce')
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
        df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce')
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_fund_basic_info(fund_code):
    """获取基金基本信息"""
    try:
        df = ak.fund_open_fund_daily_em()
        row = df[df['基金代码'] == fund_code]
        return row
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_index_data(index_code, start_date="20100101"):
    """获取指数数据（基准）"""
    try:
        if index_code == "000300":
            code = "sh000300"
        elif index_code == "000905":
            code = "sh000905"
        elif index_code == "000852":
            code = "sh000852"
        else:
            code = "sh000300"
        
        df = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start_date)
        df['日期'] = pd.to_datetime(df['日期'])
        df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
        df = df.sort_values('日期').dropna(subset=['收盘'])
        return df
    except:
        return pd.DataFrame()

# ===================================================
# 维度2: 收益与风险分析
# ===================================================

def calculate_performance_metrics(nav_df, benchmark_df=None):
    """计算收益与风险指标：年化收益、波动率、Sharpe、Calmar、Sortino、信息比率"""
    nav = nav_df['单位净值'].values
    daily_returns = pd.Series(nav).pct_change().dropna()
    
    start_nav = nav[0]
    end_nav = nav[-1]
    days = (nav_df['净值日期'].iloc[-1] - nav_df['净值日期'].iloc[0]).days
    annual_return = (end_nav / start_nav) ** (365 / max(days, 1)) - 1
    annual_volatility = daily_returns.std() * np.sqrt(252)
    
    risk_free_rate = 0.025
    sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility > 0 else 0
    
    cummax = pd.Series(nav).cummax()
    drawdown = (pd.Series(nav) - cummax) / cummax
    max_drawdown = drawdown.min()
    calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
    
    downside_returns = daily_returns[daily_returns < 0]
    downside_deviation = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0
    sortino_ratio = (annual_return - risk_free_rate) / downside_deviation if downside_deviation > 0 else 0
    
    information_ratio = 0
    tracking_error = 0
    alpha = 0
    beta = 1
    
    if benchmark_df is not None and not benchmark_df.empty:
        fund_ret = pd.DataFrame({
            'date': nav_df['净值日期'],
            'fund_return': daily_returns
        }).set_index('date')
        
        benchmark_nav = benchmark_df.set_index('日期')['收盘']
        benchmark_ret = benchmark_nav.pct_change().dropna()
        
        merged = pd.DataFrame({
            'fund': fund_ret['fund_return'],
            'benchmark': benchmark_ret
        }).dropna()
        
        if len(merged) > 30:
            excess_returns = merged['fund'] - merged['benchmark']
            X = sm.add_constant(merged['benchmark'])
            model = sm.OLS(merged['fund'], X).fit()
            alpha = model.params[0] * 252
            beta = model.params[1]
            tracking_error = excess_returns.std() * np.sqrt(252)
            information_ratio = alpha / tracking_error if tracking_error > 0 else 0
    
    return {
        'annual_return': annual_return,
        'annual_volatility': annual_volatility,
        'sharpe_ratio': sharpe_ratio,
        'calmar_ratio': calmar_ratio,
        'sortino_ratio': sortino_ratio,
        'max_drawdown': max_drawdown,
        'information_ratio': information_ratio,
        'alpha': alpha,
        'beta': beta,
        'tracking_error': tracking_error,
        'total_return': (end_nav / start_nav - 1),
        'days': days
    }

# ===================================================
# 维度3: 持仓结构分析 (HHI指数)
# ===================================================

def calculate_portfolio_structure(portfolio_df, industry_df):
    """计算持仓结构指标：HHI指数、行业集中度、换手率"""
    metrics = {}
    
    if not portfolio_df.empty:
        latest_quarter = portfolio_df['报告期'].iloc[0]
        latest_portfolio = portfolio_df[portfolio_df['报告期'] == latest_quarter]
        
        if not latest_portfolio.empty and '占净值比例' in latest_portfolio.columns:
            weights = latest_portfolio['占净值比例'].values / 100
            hhi = np.sum(weights ** 2)
            
            if hhi < 0.05:
                hhi_level = "高度分散"
            elif hhi < 0.10:
                hhi_level = "分散"
            elif hhi < 0.18:
                hhi_level = "适中"
            else:
                hhi_level = "集中"
            
            metrics['hhi'] = hhi
            metrics['hhi_level'] = hhi_level
            metrics['top10_ratio'] = weights.sum() * 100
            metrics['top3_ratio'] = weights[:3].sum() * 100 if len(weights) >= 3 else weights.sum() * 100
            metrics['top1_ratio'] = weights[0] * 100 if len(weights) > 0 else 0
    
    if not industry_df.empty:
        if '占净值比例' in industry_df.columns:
            industry_weights = industry_df['占净值比例'].values / 100
            industry_hhi = np.sum(industry_weights ** 2)
            metrics['industry_hhi'] = industry_hhi
            metrics['top_industry_ratio'] = industry_weights[0] * 100 if len(industry_weights) > 0 else 0
            metrics['top3_industry_ratio'] = industry_weights[:3].sum() * 100 if len(industry_weights) >= 3 else 0
    
    if len(portfolio_df) >= 2:
        quarters = sorted(portfolio_df['报告期'].unique(), reverse=True)
        turnover_rates = []
        
        for i in range(len(quarters) - 1):
            q1_stocks = set(portfolio_df[portfolio_df['报告期'] == quarters[i]]['股票名称'].values)
            q2_stocks = set(portfolio_df[portfolio_df['报告期'] == quarters[i+1]]['股票名称'].values)
            
            new_stocks = len(q2_stocks - q1_stocks)
            removed_stocks = len(q1_stocks - q2_stocks)
            quarterly_turnover = (new_stocks + removed_stocks) / 20
            turnover_rates.append(quarterly_turnover)
        
        metrics['turnover_rate'] = np.mean(turnover_rates) * 100 if turnover_rates else 0
    else:
        metrics['turnover_rate'] = 0
    
    return metrics

# ===================================================
# 维度4: 晨星九宫格风格判定
# ===================================================

def morningstar_style_box(nav_df, portfolio_df, industry_df):
    """晨星九宫格风格判定：X轴大盘vs小盘，Y轴价值vs成长"""
    style_metrics = {}
    
    if not portfolio_df.empty:
        latest_quarter = portfolio_df['报告期'].iloc[0]
        latest_portfolio = portfolio_df[portfolio_df['报告期'] == latest_quarter]
    else:
        return {'style': '数据不足', 'position': 'N/A'}
    
    large_cap_keywords = ['银行', '保险', '证券', '白酒', '家电', '地产', '公用', '交运']
    mid_cap_keywords = ['医药', '汽车', '建材', '化工', '机械']
    small_cap_keywords = ['科技', '电子', '新能源', '军工', '环保']
    
    large_cap_count = 0
    small_cap_count = 0
    
    if not latest_portfolio.empty and '股票名称' in latest_portfolio.columns:
        for stock in latest_portfolio['股票名称'].values:
            if any(kw in stock for kw in large_cap_keywords):
                large_cap_count += 1
            elif any(kw in stock for kw in small_cap_keywords):
                small_cap_count += 1
        
        total = len(latest_portfolio)
        if total > 0:
            large_cap_ratio = large_cap_count / total
            small_cap_ratio = small_cap_count / total
            
            if large_cap_ratio > 0.6:
                cap_style = "大盘"
            elif small_cap_ratio > 0.6:
                cap_style = "小盘"
            else:
                cap_style = "中盘"
        else:
            cap_style = "数据不足"
    else:
        cap_style = "数据不足"
    
    value_keywords = ['银行', '保险', '地产', '公用', '交运', '基建', '煤炭', '钢铁']
    growth_keywords = ['新能源', '电子', '半导体', '医药生物', '军工', '高端制造', '人工智能']
    
    value_count = 0
    growth_count = 0
    
    if not latest_portfolio.empty and '股票名称' in latest_portfolio.columns:
        for stock in latest_portfolio['股票名称'].values:
            if any(kw in stock for kw in value_keywords):
                value_count += 1
            elif any(kw in stock for kw in growth_keywords):
                growth_count += 1
        
        total = len(latest_portfolio)
        if total > 0:
            value_ratio = value_count / total
            growth_ratio = growth_count / total
            
            if value_ratio > 0.6:
                value_style = "价值"
            elif growth_ratio > 0.6:
                value_style = "成长"
            else:
                value_style = "平衡"
        else:
            value_style = "数据不足"
    else:
        value_style = "数据不足"
    
    style_metrics['cap_style'] = cap_style
    style_metrics['value_style'] = value_style
    style_metrics['style'] = f"{cap_style}{value_style}" if cap_style != "数据不足" and value_style != "数据不足" else "数据不足"
    
    return style_metrics

# ===================================================
# 维度5: Fama-French三因子模型
# ===================================================

def fama_french_three_factor(nav_df, benchmark_df=None):
    """Fama-French三因子模型回归（简化版：使用CAPM单因子）"""
    daily_returns = pd.Series(nav_df['单位净值'].values).pct_change().dropna()
    dates = nav_df['净值日期'].iloc[1:].values
    
    if benchmark_df is not None and not benchmark_df.empty:
        fund_ret = pd.DataFrame({
            'date': dates,
            'fund_return': daily_returns.values
        }).set_index('date')
        
        benchmark_nav = benchmark_df.set_index('日期')['收盘']
        benchmark_ret = benchmark_nav.pct_change().dropna()
        
        merged = pd.DataFrame({
            'fund': fund_ret['fund_return'],
            'market': benchmark_ret
        }).dropna()
        
        if len(merged) > 60:
            risk_free_daily = 0.025 / 252
            merged['fund_excess'] = merged['fund'] - risk_free_daily
            merged['market_excess'] = merged['market'] - risk_free_daily
            
            X = sm.add_constant(merged['market_excess'])
            model = sm.OLS(merged['fund_excess'], X).fit()
            
            return {
                'alpha': model.params[0] * 252,
                'beta_market': model.params[1],
                'r_squared': model.rsquared,
                'p_value': model.pvalues[1]
            }
    
    return {
        'alpha': 0,
        'beta_market': 1,
        'r_squared': 0,
        'p_value': 1
    }

# ===================================================
# 主界面
# ===================================================

st.title("📊 基金深度分析工具")
st.markdown("基于 AkShare 数据源，5 维度专业量化分析")

with st.sidebar:
    st.header("🔍 基金查询")
    fund_code = st.text_input(
        "基金代码（6位数字）",
        value="014416",
        max_chars=6,
        help="例如：014416 泰康研究精选股票"
    )
    
    st.markdown("#### 基准选择")
    benchmark_choice = st.selectbox(
        "比较基准",
        ["沪深300 (000300)", "中证500 (000905)", "中证1000 (000852)"],
        index=0
    )
    benchmark_code = benchmark_choice.split('(')[1].split(')')[0]
    
    run = st.button("🚀 开始分析", type="primary", use_container_width=True)
    st.markdown("---")
    st.caption("📌 数据来源：AkShare")
    st.caption("⚠️ 仅供学习参考，不构成投资建议")

if not run:
    st.info("👈 请在左侧输入基金代码，点击「开始分析」")
    cols = st.columns(5)
    with cols[0]:
        st.markdown("#### 📋 基本信息\n规模、费率、类型")
    with cols[1]:
        st.markdown("#### 📈 收益风险\nSharpe、Calmar、Sortino")
    with cols[2]:
        st.markdown("#### 👤 基金经理\n经验、能力雷达图")
    with cols[3]:
        st.markdown("#### 📦 持仓分析\nHHI指数、集中度、换手")
    with cols[4]:
        st.markdown("#### 🎯 风格归因\n晨星九宫格、FF三因子")
    st.stop()

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

benchmark_df = get_index_data(benchmark_code)

performance_metrics = calculate_performance_metrics(nav_df, benchmark_df)
portfolio_metrics = calculate_portfolio_structure(portfolio_df, industry_df)
style_metrics = morningstar_style_box(nav_df, portfolio_df, industry_df)
ff_metrics = fama_french_three_factor(nav_df, benchmark_df)

st.success("✅ 数据获取成功")
st.markdown("---")

fund_name = "—"
fund_type = "—"
fund_size = "—"
establish_date = "—"
buy_status = "—"
sell_status = "—"

if not detail_df.empty:
    row = detail_df.iloc[0] if len(detail_df) > 0 else None
    if row is not None:
        fund_name = row.get('基金简称', '—')
        fund_type = row.get('基金类型', '—')
        fund_size = row.get('基金规模', '—')
        establish_date = row.get('成立日期', '—')

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

st.header("📈 维度二：收益与风险分析")

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
    
    ar = performance_metrics['annual_return']
    av = performance_metrics['annual_volatility']
    md = performance_metrics['max_drawdown']
    sr = performance_metrics['sharpe_ratio']
    cr = performance_metrics['calmar_ratio']
    sotr = performance_metrics['sortino_ratio']
    ir = performance_metrics['information_ratio']
    alpha = performance_metrics['alpha']
    beta = performance_metrics['beta']
    
    ar_delta = f"{'↑' if ar >= 0 else '↓'}"
    st.metric("年化收益率", f"{ar*100:.2f}%", delta=ar_delta, delta_color="normal" if ar >= 0 else "inverse")
    st.metric("年化波动率", f"{av*100:.2f}%")
    st.metric("最大回撤", f"{md*100:.2f}%")
    st.metric("夏普比率", f"{sr:.2f}")
    st.metric("卡玛比率", f"{cr:.2f}")
    st.metric("Sortino比率", f"{sotr:.2f}")
    st.metric("Alpha", f"{alpha*100:.2f}%")
    st.metric("Beta", f"{beta:.2f}")
    if ir != 0:
        st.metric("信息比率", f"{ir:.2f}")
    
    if av < 0.10:
        risk_label, risk_color = "低风险 🟢", "success"
    elif av < 0.20:
        risk_label, risk_color = "中低风险 🟡", "info"
    elif av < 0.30:
        risk_label, risk_color = "中高风险 🟠", "warning"
    else:
        risk_label, risk_color = "高风险 🔴", "error"
    
    getattr(st, risk_color)(f"风险等级：{risk_label}")

with st.expander("📖 指标说明"):
    st.markdown("""
    - **夏普比率**: 每承担一单位风险带来的超额收益，>1为优秀
    - **卡玛比率**: 年化收益/|最大回撤|，反映回撤控制的性价比
    - **Sortino比率**: 只考虑下行风险，适合厌恶回撤的投资者
    - **Alpha**: 相对于基准的超额收益，>0表示跑赢基准
    - **Beta**: 市场敏感度，>1表示波动大于大盘
    - **信息比率**: Alpha/跟踪误差，衡量超额收益的稳定性
    """)

st.markdown("---")

st.header("📦 维度三：持仓结构分析 (HHI指数)")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("持仓HHI指数", f"{portfolio_metrics.get('hhi', 0):.3f}")
    st.caption(portfolio_metrics.get('hhi_level', '数据不足'))

with col2:
    st.metric("前10持仓占比", f"{portfolio_metrics.get('top10_ratio', 0):.2f}%")

with col3:
    st.metric("换手率", f"{portfolio_metrics.get('turnover_rate', 0):.1f}%")

hhi = portfolio_metrics.get('hhi', 0)
if hhi < 0.05:
    st.success(f"✅ HHI={hhi:.3f}: 持仓高度分散，个股风险低")
elif hhi < 0.10:
    st.info(f"🟡 HHI={hhi:.3f}: 持仓分散，风险适中")
elif hhi < 0.18:
    st.warning(f"🟠 HHI={hhi:.3f}: 持仓相对集中，关注集中度风险")
else:
    st.error(f"🔴 HHI={hhi:.3f}: 持仓高度集中，个股风险暴露大")

turnover = portfolio_metrics.get('turnover_rate', 0)
if turnover > 70:
    st.warning(f"⚠️ 高换手率（{turnover:.1f}%）：短线博弈风格，交易成本高")
elif turnover > 40:
    st.info(f"🟡 中等换手率（{turnover:.1f}%）：中等风格")
else:
    st.success(f"✅ 低换手率（{turnover:.1f}%）：长线持有风格，交易成本低")

if not portfolio_df.empty:
    st.markdown("#### 最新持仓")
    latest_quarter = portfolio_df['报告期'].iloc[0]
    latest_portfolio = portfolio_df[portfolio_df['报告期'] == latest_quarter].head(10)
    
    display_cols = ['股票代码', '股票名称', '占净值比例']
    available_cols = [c for c in display_cols if c in latest_portfolio.columns]
    st.dataframe(latest_portfolio[available_cols], hide_index=True, use_container_width=True)
    st.caption(f"报告期：{latest_quarter}")

if 'industry_hhi' in portfolio_metrics:
    st.markdown("#### 行业配置")
    
    i1, i2 = st.columns(2)
    i1.metric("行业HHI", f"{portfolio_metrics['industry_hhi']:.3f}")
    i2.metric("第一大行业占比", f"{portfolio_metrics.get('top_industry_ratio', 0):.2f}%")
    
    if not industry_df.empty:
        fig_industry = px.bar(
            industry_df.head(10),
            x='占净值比例',
            y='行业名称',
            orientation='h',
            color_discrete_sequence=px.colors.sequential.Reds
        )
        fig_industry.update_layout(
            title="行业配置 Top 10",
            height=400,
            margin=dict(t=40, b=20)
        )
        st.plotly_chart(fig_industry, use_container_width=True)

st.markdown("---")

st.header("👤 维度四：基金经理分析")

if not manager_df.empty:
    m = manager_df.iloc[0]
    manager_name = m.get('姓名', '—')
    company = m.get('所属公司', '—')
    tenure_days_str = str(m.get('累计从业时间', '0'))
    tenure_days = int(''.join(filter(str.isdigit, tenure_days_str))) if tenure_days_str else 0
    tenure_years = tenure_days / 365
    scale = m.get('现任基金资产总规模', '—')
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("基金经理", manager_name)
    m2.metric("所属公司", company)
    m3.metric("从业年限", f"{tenure_years:.1f} 年")
    m4.metric("管理规模", f"{scale} 亿")
    
    st.markdown("#### 能力评估")
    
    tenure_score = min(tenure_years / 10 * 100, 100)
    scale_val = float(str(scale).replace('亿', '').strip()) if scale != '—' else 10
    scale_score = min(scale_val / 100 * 100, 100)
    return_score = min(max(ar * 100 + 50, 0), 100)
    drawdown_score = max(0, 100 + md * 200)
    sharpe_score = min(max(sr * 30 + 50, 0), 100)
    
    categories = ['任职年限', '管理规模', '收益能力', '回撤控制', '夏普表现']
    values = [tenure_score, scale_score, return_score, drawdown_score, sharpe_score]
    
    fig_radar = go.Figure(go.Scatterpolar(
        r=values + [values[0]],
        theta=categories + [categories[0]],
        fill='toself',
        fillcolor='rgba(232,71,71,0.15)',
        line=dict(color='#E84747', width=2)
    ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        height=400,
        margin=dict(t=20, b=20)
    )
    st.plotly_chart(fig_radar, use_container_width=True)
else:
    st.warning("⚠️ 未找到该基金的经理信息")

st.markdown("---")

st.header("🎯 维度五：风格与归因分析")

st.markdown("#### 晨星九宫格风格判定")
style = style_metrics.get('style', '数据不足')
cap_style = style_metrics.get('cap_style', '—')
value_style = style_metrics.get('value_style', '—')

col_s1, col_s2, col_s3 = st.columns(3)
col_s1.metric("市值风格", cap_style)
col_s2.metric("价值/成长风格", value_style)
col_s3.metric("综合风格", style)

fig_style = go.Figure()
fig_style.add_shape(type="rect", x0=-1, y0=-1, x1=1, y1=1, line=dict(color="gray", width=2))
fig_style.add_shape(type="line", x0=0, y0=-1.2, x1=0, y1=1.2, line=dict(color="gray", width=1, dash="dash"))
fig_style.add_shape(type="line", x0=-1.2, y0=0, x1=1.2, y1=0, line=dict(color="gray", width=1, dash="dash"))

x_pos = 0.5 if cap_style == "大盘" else (-0.5 if cap_style == "小盘" else 0)
y_pos = 0.5 if value_style == "价值" else (-0.5 if value_style == "成长" else 0)

fig_style.add_trace(go.Scatter(
    x=[x_pos],
    y=[y_pos],
    mode='markers',
    marker=dict(size=30, color='#E84747'),
    name='当前风格'
))

fig_style.update_layout(
    title="晨星九宫格风格定位",
    xaxis=dict(title="大盘 ←→ 小盘", range=[-1.2, 1.2], zeroline=False),
    yaxis=dict(title="价值 ←→ 成长", range=[-1.2, 1.2], zeroline=False),
    showlegend=False,
    height=400,
    margin=dict(t=40, b=20)
)

fig_style.add_annotation(x=-0.9, y=0.9, text="大盘价值", showarrow=False, font=dict(size=12))
fig_style.add_annotation(x=0, y=0.9, text="大盘平衡", showarrow=False, font=dict(size=12))
fig_style.add_annotation(x=0.9, y=0.9, text="大盘成长", showarrow=False, font=dict(size=12))
fig_style.add_annotation(x=-0.9, y=-0.9, text="小盘价值", showarrow=False, font=dict(size=12))
fig_style.add_annotation(x=0, y=-0.9, text="小盘平衡", showarrow=False, font=dict(size=12))
fig_style.add_annotation(x=0.9, y=-0.9, text="小盘成长", showarrow=False, font=dict(size=12))

st.plotly_chart(fig_style, use_container_width=True)

st.markdown("#### Fama-French因子分析")
alpha_ff = ff_metrics.get('alpha', 0)
beta_market = ff_metrics.get('beta_market', 1)
r_squared = ff_metrics.get('r_squared', 0)

col_f1, col_f2, col_f3 = st.columns(3)
col_f1.metric("Alpha", f"{alpha_ff*100:.2f}%")
col_f2.metric("Market Beta", f"{beta_market:.2f}")
col_f3.metric("R²拟合度", f"{r_squared:.2f}")

st.markdown("""
**CAPM模型回归结果**:
Ri - Rf = α + β(Rm - Rf) + ε

- **Alpha > 0**: 基金经理创造超额收益
- **Beta > 1**: 基金波动大于市场
- **R²**: 模型解释度，越接近1说明收益主要来自市场因子
""")

st.markdown("---")

st.header("💡 综合评估")

score_return = min(max(ar * 100 + 50, 0), 25)
score_sharpe = min(max(sr * 15 + 10, 0), 20)
score_drawdown = min(max((1 + md) * 20, 0), 20)
score_alpha = min(max(alpha * 100 + 50, 0), 20)
score_concentration = max(0, 15 - portfolio_metrics.get('hhi', 0) * 100)

total_score = score_return + score_sharpe + score_drawdown + score_alpha + score_concentration

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
    
    with st.expander("📊 评分明细"):
        st.write(f"- 收益能力：{score_return:.0f}/25")
        st.write(f"- 风险调整收益：{score_sharpe:.0f}/20（夏普{sr:.2f}）")
        st.write(f"- 回撤控制：{score_drawdown:.0f}/20（最大回撤{md*100:.2f}%）")
        st.write(f"- 超额收益：{score_alpha:.0f}/20（Alpha{alpha*100:.2f}%）")
        st.write(f"- 分散度：{score_concentration:.0f}/15（HHI{portfolio_metrics.get('hhi', 0):.3f}）")

with sc2:
    st.markdown("#### 🎯 投资关注点")
    tips = []
    
    if ar > 0.15:
        tips.append("✅ 年化收益表现优异")
    elif ar < 0:
        tips.append("⚠️ 近期收益为负")
    
    if sr > 1.5:
        tips.append("✅ 夏普比率突出，风险收益性价比高")
    elif sr < 0.5:
        tips.append("⚠️ 夏普比率偏低")
    
    if cr > 0.8:
        tips.append("✅ 卡玛比率优秀，回撤控制好")
    
    if alpha > 0.05:
        tips.append("✅ Alpha显著，选股能力强")
    elif alpha < 0:
        tips.append("⚠️ Alpha为负，持续跑输基准")
    
    if hhi > 0.15:
        tips.append("⚠️ 持仓集中度高，个股风险大")
    
    if turnover > 70:
        tips.append("⚠️ 高换手率，交易成本高")
    
    if tenure_years < 3:
        tips.append("⚠️ 基金经理经验较短")
    
    for tip in tips:
        st.markdown(tip)

st.markdown("---")
st.caption("📊 数据来源：AkShare | 本工具仅供学习参考，不构成任何投资建议 | 投资有风险，入市需谨慎")
