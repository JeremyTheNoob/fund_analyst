import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

# ===== 页面配置 =====
st.set_page_config(
    page_title="基金深度分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ===================================================
# 数据获取函数
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
    """获取指数数据"""
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
# 计算函数
# ===================================================

def calculate_metrics(nav_df, benchmark_df=None):
    """计算核心指标"""
    nav = nav_df['单位净值'].values
    daily_returns = pd.Series(nav).pct_change().dropna()
    
    # 年化收益
    start_nav = nav[0]
    end_nav = nav[-1]
    days = (nav_df['净值日期'].iloc[-1] - nav_df['净值日期'].iloc[0]).days
    annual_return = (end_nav / start_nav) ** (365 / max(days, 1)) - 1
    
    # 波动率
    annual_vol = daily_returns.std() * np.sqrt(252)
    
    # 最大回撤
    cummax = pd.Series(nav).cummax()
    drawdown = (pd.Series(nav) - cummax) / cummax
    max_drawdown = drawdown.min()
    
    # 夏普比率
    risk_free = 0.025
    sharpe = (annual_return - risk_free) / annual_vol if annual_vol > 0 else 0
    
    # 卡玛比率
    calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
    
    # Sortino比率
    downside_returns = daily_returns[daily_returns < 0]
    downside_dev = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0
    sortino = (annual_return - risk_free) / downside_dev if downside_dev > 0 else 0
    
    # Alpha和Beta
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
            cov = merged['fund'].cov(merged['benchmark'])
            bench_var = merged['benchmark'].var()
            beta = cov / bench_var if bench_var != 0 else 1
            alpha = (merged['fund'].mean() - beta * merged['benchmark'].mean()) * 252
    
    return {
        'annual_return': annual_return,
        'annual_vol': annual_vol,
        'max_drawdown': max_drawdown,
        'sharpe': sharpe,
        'calmar': calmar,
        'sortino': sortino,
        'alpha': alpha,
        'beta': beta,
        'total_return': (end_nav / start_nav - 1),
        'days': days
    }

def calculate_hhi(portfolio_df):
    """计算HHI指数"""
    if portfolio_df.empty:
        return {'hhi': 0, 'top10': 0, 'top3': 0, 'top1': 0}
    
    latest = portfolio_df[portfolio_df['报告期'] == portfolio_df['报告期'].iloc[0]].head(10).copy()
    if latest.empty or '占净值比例' not in latest.columns:
        return {'hhi': 0, 'top10': 0, 'top3': 0, 'top1': 0}
    
    weights = latest['占净值比例'].values / 100
    hhi = np.sum(weights ** 2)
    
    return {
        'hhi': hhi,
        'top10': weights.sum() * 100,
        'top3': weights[:3].sum() * 100 if len(weights) >= 3 else weights.sum() * 100,
        'top1': weights[0] * 100 if len(weights) > 0 else 0
    }

def calculate_turnover(portfolio_df):
    """计算换手率"""
    if len(portfolio_df) < 2:
        return 0
    
    quarters = sorted(portfolio_df['报告期'].unique(), reverse=True)
    turnover_list = []
    
    for i in range(len(quarters) - 1):
        q1 = set(portfolio_df[portfolio_df['报告期'] == quarters[i]]['股票名称'].values)
        q2 = set(portfolio_df[portfolio_df['报告期'] == quarters[i+1]]['股票名称'].values)
        turnover_list.append((len(q2 - q1) + len(q1 - q2)) / 20 * 100)
    
    return np.mean(turnover_list) if turnover_list else 0

def infer_style(portfolio_df):
    """推断投资风格"""
    if portfolio_df.empty:
        return {'cap': '数据不足', 'style': '数据不足'}
    
    latest = portfolio_df[portfolio_df['报告期'] == portfolio_df['报告期'].iloc[0]]
    if latest.empty:
        return {'cap': '数据不足', 'style': '数据不足'}
    
    large = ['银行', '保险', '证券', '白酒', '家电', '地产', '公用', '交运']
    small = ['科技', '电子', '新能源', '军工', '环保']
    value = ['银行', '保险', '地产', '公用', '交运', '基建', '煤炭', '钢铁']
    growth = ['新能源', '电子', '半导体', '医药', '军工', '高端制造', '人工智能']
    
    large_cnt = sum(1 for s in latest['股票名称'].values if any(k in s for k in large))
    small_cnt = sum(1 for s in latest['股票名称'].values if any(k in s for k in small))
    value_cnt = sum(1 for s in latest['股票名称'].values if any(k in s for k in value))
    growth_cnt = sum(1 for s in latest['股票名称'].values if any(k in s for k in growth))
    
    total = len(latest)
    if total == 0:
        return {'cap': '数据不足', 'style': '数据不足'}
    
    cap = "大盘" if large_cnt / total > 0.6 else ("小盘" if small_cnt / total > 0.6 else "中盘")
    style = "价值" if value_cnt / total > 0.6 else ("成长" if growth_cnt / total > 0.6 else "平衡")
    
    return {'cap': cap, 'style': style}

# ===================================================
# 主界面
# ===================================================

st.title("📊 基金深度分析")

with st.sidebar:
    st.header("🔍 查询")
    fund_code = st.text_input("基金代码", value="014416", max_chars=6)
    benchmark = st.selectbox("基准", ["沪深300", "中证500", "中证1000"])
    run = st.button("开始分析", type="primary")
    st.caption("📌 数据来源：AkShare")

if not run:
    st.info("👈 输入代码开始分析")
    st.stop()

# ===== 获取数据 =====
with st.spinner("获取数据中..."):
    nav_df = get_nav_history(fund_code)
    if nav_df.empty:
        st.error("未找到该基金数据")
        st.stop()
    
    manager_df = get_fund_manager(fund_code)
    portfolio_df = get_portfolio(fund_code)
    basic_df = get_fund_basic_info(fund_code)
    
    benchmark_map = {"沪深300": "000300", "中证500": "000905", "中证1000": "000852"}
    benchmark_df = get_index_data(benchmark_map[benchmark])

# ===== 计算指标 =====
metrics = calculate_metrics(nav_df, benchmark_df)
hhi = calculate_hhi(portfolio_df)
turnover = calculate_turnover(portfolio_df)
style = infer_style(portfolio_df)

# ===== 基金信息 =====
fund_name = basic_df.iloc[0].get('基金简称', '—') if not basic_df.empty else "—"
fund_size = basic_df.iloc[0].get('基金规模', '—') if not basic_df.empty else "—"

col1, col2, col3 = st.columns(3)
col1.metric("基金名称", fund_name)
col2.metric("基金代码", fund_code)
col3.metric("最新净值", f"{nav_df['单位净值'].iloc[-1]:.4f}")

st.markdown("---")

# ===== 维度2：收益风险 =====
st.header("📈 收益与风险")

left, right = st.columns([2, 1])

with left:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=nav_df['净值日期'],
        y=nav_df['单位净值'],
        name='净值',
        line=dict(color='#E84747', width=2)
    ))
    fig.update_layout(height=300, margin=dict(t=30, b=10))
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("**核心指标**")
    col_r1, col_r2 = st.columns(2)
    col_r1.metric("年化收益", f"{metrics['annual_return']*100:.1f}%")
    col_r2.metric("夏普比率", f"{metrics['sharpe']:.2f}")
    
    col_r3, col_r4 = st.columns(2)
    col_r3.metric("最大回撤", f"{metrics['max_drawdown']*100:.1f}%")
    col_r4.metric("卡玛比率", f"{metrics['calmar']:.2f}")
    
    col_r5, col_r6 = st.columns(2)
    col_r5.metric("Sortino", f"{metrics['sortino']:.2f}")
    col_r6.metric("Alpha", f"{metrics['alpha']*100:.2f}%")

st.markdown("---")

# ===== 维度3：持仓分析 =====
st.header("📦 持仓分析")

c1, c2, c3 = st.columns(3)
c1.metric("HHI指数", f"{hhi['hhi']:.3f}")
c2.metric("前10占比", f"{hhi['top10']:.1f}%")
c3.metric("换手率", f"{turnover:.1f}%")

# 持仓数据
if not portfolio_df.empty:
    latest = portfolio_df[portfolio_df['报告期'] == portfolio_df['报告期'].iloc[0]].head(10)
    display_cols = ['股票名称', '占净值比例']
    available_cols = [c for c in display_cols if c in latest.columns]
    if available_cols:
        st.dataframe(latest[available_cols], hide_index=True, use_container_width=True)
else:
    st.warning("暂无持仓数据")

st.markdown("---")

# ===== 维度4：基金经理 =====
st.header("👤 基金经理")

if not manager_df.empty:
    m = manager_df.iloc[0]
    m1, m2, m3 = st.columns(3)
    m1.metric("姓名", m.get('姓名', '—'))
    m2.metric("公司", m.get('所属公司', '—'))
    tenure = int(''.join(filter(str.isdigit, str(m.get('累计从业时间', '0'))))) / 365
    m3.metric("从业", f"{tenure:.1f}年")
else:
    st.warning("暂无经理信息")

st.markdown("---")

# ===== 维度5：风格分析 =====
st.header("🎯 风格分析")

s1, s2 = st.columns(2)
s1.metric("市值风格", style['cap'])
s2.metric("成长/价值", style['style'])

# 九宫格
fig = go.Figure()
fig.add_shape(type="rect", x0=-1, y0=-1, x1=1, y1=1, line=dict(color="gray", width=2))
fig.add_shape(type="line", x0=0, y0=-1.2, x1=0, y1=1.2, line=dict(color="gray", width=1, dash="dash"))
fig.add_shape(type="line", x0=-1.2, y0=0, x1=1.2, y1=0, line=dict(color="gray", width=1, dash="dash"))

x = 0.5 if style['cap'] == "大盘" else (-0.5 if style['cap'] == "小盘" else 0)
y = 0.5 if style['style'] == "价值" else (-0.5 if style['style'] == "成长" else 0)

fig.add_trace(go.Scatter(x=[x], y=[y], mode='markers', marker=dict(size=30, color='#E84747')))
fig.update_layout(
    xaxis=dict(title="大盘←→小盘", range=[-1.2, 1.2], zeroline=False, showticklabels=False),
    yaxis=dict(title="价值←→成长", range=[-1.2, 1.2], zeroline=False, showticklabels=False),
    height=350, margin=dict(t=30, b=30)
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ===== 综合评分 =====
st.header("💡 综合评估")

score_r = min(max(metrics['annual_return'] * 100 + 50, 0), 30)
score_s = min(max(metrics['sharpe'] * 15 + 10, 0), 25)
score_d = min(max((1 + metrics['max_drawdown']) * 30, 0), 25)
score_a = min(max(metrics['alpha'] * 100 + 50, 0), 20)
total = score_r + score_s + score_d + score_a

rating = "⭐⭐⭐⭐⭐" if total >= 85 else ("⭐⭐⭐⭐" if total >= 70 else ("⭐⭐⭐" if total >= 55 else "⭐⭐"))

sc1, sc2 = st.columns([1, 2])
sc1.metric("综合评分", f"{total:.0f}/100")
sc1.markdown(f"**{rating}**")

with sc2:
    tips = []
    if metrics['sharpe'] > 1.5: tips.append("✅ 夏普比率优秀")
    if metrics['alpha'] > 0.03: tips.append("✅ Alpha显著")
    if metrics['calmar'] > 1: tips.append("✅ 回撤控制好")
    if hhi['hhi'] < 0.1: tips.append("✅ 持仓分散")
    if turnover < 50: tips.append("✅ 换手率低")
    for tip in tips:
        st.markdown(tip)

st.caption("⚠️ 仅供参考，不构成投资建议")
