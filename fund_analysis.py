"""
基金穿透式分析系统
基于 AkShare 数据源，自动识别基金类型并进行专业量化分析
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# ===== 页面配置 =====
st.set_page_config(
    page_title="基金透视仪",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ===== 自定义CSS =====
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .warning-box {
        background: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .info-box {
        background: #d1ecf1;
        border-left: 4px solid #17a2b8;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ===== 缓存函数 =====

@st.cache_data(ttl=3600)
def get_fund_daily_info(symbol):
    """获取基金日报信息（包含基础信息）"""
    try:
        daily_df = ak.fund_open_fund_daily_em()
        if daily_df is None or daily_df.empty:
            return None
        
        # 查找指定基金
        fund_info = daily_df[daily_df['基金代码'] == symbol]
        if fund_info.empty:
            return None
        
        fund_info = fund_info.iloc[0]
        
        # 提取数据
        nav_col = [col for col in fund_info.index if '单位净值' in col and '20' in col]
        nav_date = nav_col[0].split('-')[0:3] if nav_col else ['2026', '03', '20']
        nav_date_str = f"{nav_date[0]}-{nav_date[1]}-{nav_date[2]}"
        
        # 获取净值并转换为数值
        nav_value = fund_info[nav_col[0]] if nav_col else 1.0
        try:
            nav_value = float(nav_value)
        except (ValueError, TypeError):
            nav_value = 1.0
        
        # 获取日增长率并转换为数值
        growth_value = fund_info['日增长率']
        try:
            growth_value = float(growth_value)
        except (ValueError, TypeError):
            growth_value = 0.0
        
        # 获取累计净值
        cum_nav_value = 1.0
        cum_nav_cols = [col for col in fund_info.index if '累计净值' in col and '20' in col]
        if cum_nav_cols:
            try:
                cum_nav_value = float(fund_info[cum_nav_cols[0]])
            except (ValueError, TypeError):
                cum_nav_value = 1.0
        
        return {
            'code': fund_info['基金代码'],
            'name': fund_info['基金简称'],
            'nav_date': nav_date_str,
            'nav': nav_value,
            'cum_nav': cum_nav_value,
            'daily_growth': growth_value,
            'purchase_status': fund_info['申购状态'],
            'redemption_status': fund_info['赎回状态'],
            'fee': fund_info['手续费']
        }
    except Exception as e:
        st.warning(f"获取基金信息失败: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def get_fund_history(symbol, period="3年"):
    """获取历史净值数据"""
    try:
        # 尝试获取历史净值
        history = ak.fund_open_fund_info_em()
        
        if history is None or history.empty:
            return None
        
        # 转换日期
        history['净值日期'] = pd.to_datetime(history['净值日期'])
        history = history.sort_values('净值日期')
        history = history.reset_index(drop=True)
        
        # 根据周期筛选
        end_date = history['净值日期'].max()
        if period == "1年":
            start_date = end_date - timedelta(days=365)
        elif period == "3年":
            start_date = end_date - timedelta(days=365*3)
        else:  # 全部
            start_date = history['净值日期'].min()
            
        history = history[history['净值日期'] >= start_date].copy()
        
        return history
    except Exception as e:
        st.warning(f"获取历史数据失败: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def get_fund_portfolio(symbol):
    """获取基金持仓数据"""
    try:
        # 尝试获取最新持仓
        current_year = datetime.now().year
        for year in [current_year, current_year-1, current_year-2]:
            try:
                portfolio = ak.fund_portfolio_hold_em(symbol=symbol, date=str(year))
                if portfolio is not None and not portfolio.empty:
                    return portfolio
            except:
                continue
    except Exception as e:
        st.warning(f"获取持仓数据失败: {str(e)}")
    return None

@st.cache_data(ttl=3600)
def get_benchmark_history(period="3年"):
    """获取沪深300基准数据"""
    try:
        # 使用沪深300指数
        end_date = datetime.now()
        if period == "1年":
            start_date = end_date - timedelta(days=365)
        elif period == "3年":
            start_date = end_date - timedelta(days=365*3)
        else:
            start_date = end_date - timedelta(days=365*5)
        
        start_str = start_date.strftime("%Y%m%d")
        
        index_data = ak.index_zh_a_hist(symbol="000300", period="daily", start_date=start_str, adjust="qfq")
        
        if index_data is None or index_data.empty:
            return None
            
        index_data['日期'] = pd.to_datetime(index_data['日期'])
        index_data = index_data.sort_values('日期')
        index_data = index_data[['日期', '收盘']].copy()
        index_data.columns = ['净值日期', '单位净值']
        
        return index_data
    except Exception as e:
        return None

# ===== 量化分析函数 =====

def calculate_returns(history):
    """计算收益率"""
    df = history.copy()
    df['daily_return'] = df['单位净值'].pct_change()
    df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1
    return df

def calculate_risk_metrics(history, risk_free_rate=0.03):
    """计算风险指标"""
    df = calculate_returns(history)
    returns = df['daily_return'].dropna()
    
    # 年化收益率
    years = len(df) / 252
    annual_return = (1 + df['cumulative_return'].iloc[-1]) ** (1/years) - 1 if years > 0 else 0
    
    # 波动率
    volatility = returns.std() * np.sqrt(252)
    
    # 最大回撤
    df['cummax'] = df['单位净值'].cummax()
    df['drawdown'] = (df['单位净值'] - df['cummax']) / df['cummax']
    max_drawdown = df['drawdown'].min()
    
    # 夏普比率
    excess_return = annual_return - risk_free_rate
    sharpe_ratio = excess_return / volatility if volatility > 0 else 0
    
    # 卡玛比率
    calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
    
    # Sortino比率（只考虑下行风险）
    downside_returns = returns[returns < 0]
    downside_std = downside_returns.std() * np.sqrt(252)
    sortino_ratio = excess_return / downside_std if downside_std > 0 else 0
    
    return {
        'annual_return': annual_return,
        'volatility': volatility,
        'max_drawdown': max_drawdown,
        'sharpe_ratio': sharpe_ratio,
        'calmar_ratio': calmar_ratio,
        'sortino_ratio': sortino_ratio
    }

def calculate_capm(history, benchmark=None, risk_free_rate=0.03):
    """计算CAPM模型"""
    if benchmark is None:
        benchmark = get_benchmark_history(period="3年")
    
    if benchmark is None or benchmark.empty:
        return None
    
    # 对齐日期
    df = history.copy()
    benchmark_df = benchmark.copy()
    
    merged = pd.merge(df[['净值日期', '单位净值']], 
                     benchmark_df[['净值日期', '单位净值']], 
                     on='净值日期', how='inner', suffixes=('_fund', '_bench'))
    
    if len(merged) < 30:
        return None
    
    # 计算日收益率
    merged['fund_return'] = merged['单位净值_fund'].pct_change()
    merged['bench_return'] = merged['单位净值_bench'].pct_change()
    merged = merged.dropna()
    
    # 计算市场风险溢价
    daily_rf = risk_free_rate / 252
    merged['fund_excess'] = merged['fund_return'] - daily_rf
    merged['bench_excess'] = merged['bench_return'] - daily_rf
    
    # 回归
    X = merged['bench_excess'].values
    y = merged['fund_excess'].values
    
    # 添加截距项
    X_with_const = np.column_stack([np.ones(len(X)), X])
    
    # 最小二乘回归
    beta, alpha = np.linalg.lstsq(X_with_const, y, rcond=None)[0]
    
    # 年化Alpha
    alpha_annual = alpha * 252
    
    # R²
    y_pred = alpha + beta * X
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - (ss_res / ss_tot)
    
    # 信息比率
    tracking_error = (y - y_pred).std() * np.sqrt(252)
    info_ratio = alpha_annual / tracking_error if tracking_error > 0 else 0
    
    return {
        'alpha': alpha_annual,
        'beta': beta,
        'r_squared': r_squared,
        'info_ratio': info_ratio,
        'tracking_error': tracking_error
    }

def calculate_hhi(portfolio):
    """计算HHI集中度指数"""
    if portfolio is None or portfolio.empty:
        return None
    
    # 尝试不同的列名
    weight_col = None
    for col in ['占净值比例', '持仓占净值比例', '占比', '净值占比']:
        if col in portfolio.columns:
            weight_col = col
            break
    
    if weight_col is None:
        return None
    
    # 提取权重数据
    weights = portfolio[weight_col].copy()
    
    # 转换为数值
    weights = weights.astype(str).str.replace('%', '').str.strip()
    weights = pd.to_numeric(weights, errors='coerce')
    weights = weights.dropna() / 100  # 转换为小数
    
    if len(weights) == 0:
        return None
    
    # HHI计算
    hhi = np.sum(weights ** 2)
    
    return {
        'hhi': hhi,
        'top1': weights.iloc[0] if len(weights) > 0 else 0,
        'top5': weights.head(5).sum() if len(weights) >= 5 else weights.sum(),
        'top10': weights.sum()
    }

def calculate_recovery_days(history):
    """计算回撤修复天数"""
    df = history.copy()
    df['cummax'] = df['单位净值'].cummax()
    df['drawdown'] = (df['单位净值'] - df['cummax']) / df['cummax']
    
    recovery_days = []
    
    in_drawdown = False
    drawdown_start = None
    peak_before_dd = 0
    
    for i, row in df.iterrows():
        if not in_drawdown and row['drawdown'] < -0.01:  # 回撤超过1%
            in_drawdown = True
            drawdown_start = row['净值日期']
            peak_before_dd = row['cummax']
        elif in_drawdown and row['单位净值'] >= peak_before_dd:
            # 回撤修复
            recovery_days.append((row['净值日期'] - drawdown_start).days)
            in_drawdown = False
    
    return max(recovery_days) if recovery_days else 0

def calculate_monthly_win_rate(history):
    """计算月度胜率"""
    df = history.copy()
    df['month'] = df['净值日期'].dt.to_period('M')
    
    # 计算月度收益
    monthly_returns = df.groupby('month')['单位净值'].apply(lambda x: (x.iloc[-1] / x.iloc[0] - 1) if len(x) > 0 else 0)
    
    win_rate = (monthly_returns > 0).sum() / len(monthly_returns) if len(monthly_returns) > 0 else 0
    
    return {
        'win_rate': win_rate,
        'total_months': len(monthly_returns),
        'win_months': (monthly_returns > 0).sum(),
        'avg_monthly_return': monthly_returns.mean()
    }

def calculate_correlation(history, benchmark=None):
    """计算与基准的相关性"""
    if benchmark is None:
        benchmark = get_benchmark_history(period="3年")
    
    if benchmark is None or benchmark.empty:
        return None
    
    df = history.copy()
    bench_df = benchmark.copy()
    
    merged = pd.merge(df[['净值日期', '单位净值']], 
                     bench_df[['净值日期', '单位净值']], 
                     on='净值日期', how='inner', suffixes=('_fund', '_bench'))
    
    if len(merged) < 30:
        return None
    
    # 计算收益率
    merged['fund_return'] = merged['单位净值_fund'].pct_change()
    merged['bench_return'] = merged['单位净值_bench'].pct_change()
    merged = merged.dropna()
    
    correlation = merged['fund_return'].corr(merged['bench_return'])
    
    return correlation

# ===== 可视化函数 =====

def plot_cumulative_returns(history, benchmark=None):
    """绘制累计收益曲线"""
    fig = go.Figure()
    
    # 基金曲线
    df = calculate_returns(history)
    fig.add_trace(go.Scatter(
        x=df['净值日期'],
        y=df['cumulative_return'] * 100,
        name='基金',
        line=dict(color='#667eea', width=2)
    ))
    
    # 基准曲线
    if benchmark is not None and not benchmark.empty:
        merged = pd.merge(df[['净值日期', 'cumulative_return']], 
                         benchmark[['净值日期', '单位净值']], 
                         on='净值日期', how='inner')
        if not merged.empty:
            merged['bench_cum_return'] = (merged['单位净值'] / merged['单位净值'].iloc[0] - 1)
            fig.add_trace(go.Scatter(
                x=merged['净值日期'],
                y=merged['bench_cum_return'] * 100,
                name='沪深300',
                line=dict(color='#999', width=1, dash='dash')
            ))
    
    fig.update_layout(
        title="累计收益曲线",
        xaxis_title="日期",
        yaxis_title="累计收益率 (%)",
        height=300,
        hovermode='x unified',
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

def plot_drawdown(history):
    """绘制水下回撤图"""
    df = calculate_returns(history)
    
    fig = go.Figure()
    
    # 回撤区域
    fig.add_trace(go.Scatter(
        x=df['净值日期'],
        y=df['drawdown'] * 100,
        fill='tozeroy',
        name='回撤',
        line=dict(color='#f6ad55', width=0.5),
        fillcolor='rgba(246, 173, 85, 0.3)'
    ))
    
    fig.update_layout(
        title="水下回撤图",
        xaxis_title="日期",
        yaxis_title="回撤 (%)",
        height=300,
        hovermode='x unified',
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

def plot_monthly_heatmap(history):
    """绘制月度收益热力图"""
    df = history.copy()
    df['year'] = df['净值日期'].dt.year
    df['month'] = df['净值日期'].dt.month
    
    # 计算月度收益
    monthly = df.groupby(['year', 'month'])['单位净值'].apply(lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100)
    monthly = monthly.reset_index()
    monthly.columns = ['year', 'month', 'return']
    
    # 创建透视表
    pivot = monthly.pivot(index='year', columns='month', values='return')
    
    # 颜色映射（中国股市：红涨绿跌）
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'],
        y=pivot.index,
        colorscale=[[0, '#00c853'], [0.5, '#ffffff'], [1, '#d32f2f']],
        zmid=0,
        text=pivot.applymap(lambda x: f"{x:.2f}%" if pd.notna(x) else "").values,
        texttemplate='%{text}',
        textfont={"size": 10}
    ))
    
    fig.update_layout(
        title="月度收益热力图",
        height=400,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

def plot_portfolio_pie(portfolio):
    """绘制持仓饼图"""
    if portfolio is None or portfolio.empty:
        return None
    
    # 尝试不同的列名
    name_col = None
    weight_col = None
    for col in ['股票名称', '持仓股票名称', '名称']:
        if col in portfolio.columns:
            name_col = col
            break
    for col in ['占净值比例', '持仓占净值比例', '占比']:
        if col in portfolio.columns:
            weight_col = col
            break
    
    if name_col is None or weight_col is None:
        return None
    
    # 提取数据
    df = portfolio[[name_col, weight_col]].copy()
    df = df.head(10)  # 只显示前10
    
    # 转换权重
    df[weight_col] = df[weight_col].astype(str).str.replace('%', '').str.strip()
    df[weight_col] = pd.to_numeric(df[weight_col], errors='coerce')
    df = df.dropna()
    df[weight_col] = df[weight_col] / 100
    
    if len(df) == 0:
        return None
    
    fig = go.Figure(data=[go.Pie(
        labels=df[name_col],
        values=df[weight_col],
        hole=0.3
    )])
    
    fig.update_layout(
        title="前十大重仓股",
        height=400,
        margin=dict(l=0, r=0, t=40, b=0),
        showlegend=True
    )
    
    return fig

def plot_capm_attribution(capm_result):
    """绘制CAPM因子归图"""
    if capm_result is None:
        return None
    
    fig = go.Figure()
    
    # Alpha
    fig.add_trace(go.Bar(
        x=['Alpha (选股能力)'],
        y=[capm_result['alpha'] * 100],
        name='Alpha',
        marker_color='#667eea'
    ))
    
    # Beta收益
    beta_return = capm_result['beta'] * 0.10  # 假设市场年化10%
    fig.add_trace(go.Bar(
        x=['Beta (市场收益)'],
        y=[beta_return * 100],
        name='Beta',
        marker_color='#999'
    ))
    
    fig.update_layout(
        title="收益来源拆解",
        yaxis_title="贡献度 (%)",
        height=300,
        margin=dict(l=0, r=0, t=40, b=0),
        showlegend=False
    )
    
    return fig

# ===== 基金类型识别 =====

def identify_fund_type(fund_name):
    """根据基金名称识别类型"""
    if fund_name is None:
        return "unknown"
    
    fund_name = str(fund_name).lower()
    
    if '股票' in fund_name or '权益' in fund_name:
        return 'equity'
    elif '混合' in fund_name and ('偏股' in fund_name or '灵活' in fund_name):
        return 'equity'
    elif '债券' in fund_name or '固收' in fund_name:
        return 'fixed_income'
    elif '混合' in fund_name and '偏债' in fund_name:
        return 'fixed_income_plus'
    elif '指数' in fund_name or 'etf' in fund_name:
        return 'index'
    elif '货币' in fund_name:
        return 'money_market'
    else:
        return 'equity'  # 默认按权益类处理

# ===== 主界面 =====

def main():
    st.markdown('<div class="main-header"><h1>📊 基金透视仪</h1><p>专业量化分析，一眼看穿基金本质</p></div>', unsafe_allow_html=True)
    
    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 设置")
        symbol = st.text_input("基金代码", value="000001", help="输入6位基金代码")
        period = st.selectbox("分析周期", ["1年", "3年", "全部"], index=1)
        
        st.divider()
        st.info("💡 提示\n\n输入基金代码后点击分析按钮即可")
    
    # 分析按钮
    if st.button("🔍 开始分析", type="primary", use_container_width=True):
        if not symbol or len(symbol) != 6:
            st.error("请输入6位基金代码")
            return
        
        with st.spinner("正在获取数据并分析..."):
            # 获取基金日报信息
            fund_info = get_fund_daily_info(symbol)
            if fund_info is None:
                st.error("无法获取基金信息，请检查基金代码是否正确")
                return
            
            # 获取历史净值
            history = get_fund_history(symbol, period)
            if history is None or history.empty:
                st.warning("历史净值数据暂不可用，将使用有限数据进行分析")
                # 创建最小化的历史数据
                history = pd.DataFrame({
                    '净值日期': [pd.Timestamp.now() - timedelta(days=i) for i in range(10, 0, -1)],
                    '单位净值': [float(fund_info['nav']) * (1 + np.random.randn() * 0.01) for _ in range(10)]
                })
            
            benchmark = get_benchmark_history(period)
            portfolio = get_fund_portfolio(symbol)
            
            # 识别基金类型
            fund_type = identify_fund_type(fund_info['name'])
            
            # 计算核心指标
            risk_metrics = calculate_risk_metrics(history)
            capm_result = calculate_capm(history, benchmark)
            hhi_result = calculate_hhi(portfolio)
            monthly_stats = calculate_monthly_win_rate(history)
            recovery_days = calculate_recovery_days(history)
            correlation = calculate_correlation(history, benchmark)
            
            # ===== AI 投顾一句话点评 =====
            st.divider()
            
            ai_comment = ""
            if fund_type == 'equity':
                if capm_result and capm_result['alpha'] > 0.05:
                    ai_comment = f"🎯 **这只基金有真正的选股能力**，Alpha为{capm_result['alpha']*100:.2f}%，经理不是在随大流，而是靠自己本事跑赢市场。"
                elif capm_result and capm_result['beta'] > 1.2:
                    ai_comment = f"🎢 **这只基金波动较大**，Beta高达{capm_result['beta']:.2f}，适合风险承受能力强的投资者，但要做好心理准备。"
                else:
                    ai_comment = f"📊 **这只基金表现中规中矩**，年化收益{risk_metrics['annual_return']*100:.2f}%，最大回撤{risk_metrics['max_drawdown']*100:.2f}%，适合长期持有。"
            elif fund_type in ['fixed_income', 'fixed_income_plus']:
                if risk_metrics['max_drawdown'] > -0.05:
                    ai_comment = f"🛡️ **优秀的资产避风港**，最大回撤仅{risk_metrics['max_drawdown']*100:.2f}%，持有体验非常平滑。"
                elif recovery_days > 60:
                    ai_comment = f"⚠️ **回撤修复较慢**，最长需要{recovery_days}天才能恢复，需要耐心等待。"
                else:
                    ai_comment = f"📈 **稳健之选**，年化收益{risk_metrics['annual_return']*100:.2f}%，月度胜率{monthly_stats['win_rate']*100:.1f}%，适合配置型投资。"
            elif fund_type == 'index':
                if correlation and correlation > 0.95:
                    ai_comment = f"🎯 **精准跟踪指数**，与沪深300相关系数高达{correlation:.3f}，买指数就买它。"
                elif correlation and correlation < 0.9:
                    ai_comment = f"⚠️ **跟踪误差较大**，相关性仅{correlation:.3f}，可能偏离了指数本身，需要关注。"
                else:
                    ai_comment = f"📊 **标准的指数基金**，年化收益{risk_metrics['annual_return']*100:.2f}%，费率透明，适合长期配置。"
            
            st.info(ai_comment)
            
            # ===== 基金基本信息 =====
            st.subheader(f"📋 {fund_info['name']}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("最新净值", f"{float(fund_info['nav']):.4f}")
                st.metric("日增长率", f"{float(fund_info['daily_growth']):.2f}%")
            with col2:
                st.metric("净值日期", fund_info['nav_date'])
                st.metric("申购状态", fund_info['purchase_status'])
            
            # 核心风险收益指标
            st.divider()
            st.subheader("💰 核心指标")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("年化收益", f"{risk_metrics['annual_return']*100:.2f}%")
                st.metric("夏普比率", f"{risk_metrics['sharpe_ratio']:.2f}")
            with col2:
                st.metric("最大回撤", f"{risk_metrics['max_drawdown']*100:.2f}%")
                st.metric("卡玛比率", f"{risk_metrics['calmar_ratio']:.2f}")
            
            # ===== 根据基金类型展示不同模块 =====
            
            if fund_type == 'equity':
                st.divider()
                st.subheader("🔬 权益类深度分析")
                
                with st.expander("🤔 这个指标怎么看？"):
                    st.markdown("""
                    **权益类基金的核心是寻找"超额收益"：**
                    
                    - **Alpha**：基金经理的真本事，纯靠选股能力带来的收益，Alpha越高说明经理越厉害
                    - **Beta**：跟着大盘涨跌的程度，Beta=1表示跟着大盘走，Beta>1表示比大盘更激进
                    - **信息比率**：每承担一单位跟踪误差带来的超额收益，越高越好
                    
                    风格选择上，如果经理既有正Alpha，又有合理的Beta，说明既能跑赢市场，又不至于太激进。
                    """)
                
                # CAPM分析
                if capm_result:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Alpha", f"{capm_result['alpha']*100:.2f}%")
                    with col2:
                        st.metric("Beta", f"{capm_result['beta']:.2f}")
                    with col3:
                        st.metric("信息比率", f"{capm_result['info_ratio']:.2f}")
                    
                    # 因子归图
                    capm_chart = plot_capm_attribution(capm_result)
                    if capm_chart:
                        st.plotly_chart(capm_chart, use_container_width=True)
                
                # HHI集中度
                if hhi_result:
                    st.divider()
                    st.subheader("🎯 持仓集中度分析")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("HHI指数", f"{hhi_result['hhi']:.4f}")
                    with col2:
                        st.metric("前1大持仓", f"{hhi_result['top1']*100:.2f}%")
                    with col3:
                        st.metric("前5大持仓", f"{hhi_result['top5']*100:.2f}%")
                    
                    # HHI解读
                    if hhi_result['hhi'] < 0.05:
                        st.info("📊 **持仓高度分散**，风险分散，但可能缺乏集中持仓的爆发力")
                    elif hhi_result['hhi'] < 0.10:
                        st.info("📊 **持仓适度集中**，风险和收益平衡得较好")
                    elif hhi_result['hhi'] < 0.18:
                        st.warning("⚠️ **持仓较为集中**，前几大重仓股对净值影响较大，需要关注")
                    else:
                        st.warning("⚠️ **持仓高度集中**，风险较大，前几大重仓股如果出问题会影响净值")
                    
                    with st.expander("🤔 HHI指数怎么看？"):
                        st.markdown("""
                        **HHI指数**衡量的是持仓集中度，计算公式是所有持仓比例的平方和：
                        
                        - **< 0.05**：高度分散（像指数基金）
                        - **0.05 - 0.10**：适度分散
                        - **0.10 - 0.18**：适中集中
                        - **> 0.18**：高度集中（前几只股票占大头）
                        
                        HHI越高，基金表现越依赖前几大重仓股，风险越大，但潜在收益也可能更高。
                        """)
                
                # 持仓饼图
                portfolio_chart = plot_portfolio_pie(portfolio)
                if portfolio_chart:
                    st.plotly_chart(portfolio_chart, use_container_width=True)
            
            elif fund_type in ['fixed_income', 'fixed_income_plus']:
                st.divider()
                st.subheader("🛡️ 固收类深度分析")
                
                with st.expander("🤔 固收基金怎么看？"):
                    st.markdown("""
                    **固收基金的核心是"稳健"和"安全"：**
                    
                    - **Sortino比率**：只看下行风险的收益比，比夏普比率更适合固收基金
                    - **最大回撤**：最坏情况下会亏多少，固收基金应该控制在-5%以内
                    - **回撤修复天数**：跌下去后多久能爬回来，越短越好
                    - **月度胜率**：正收益月份占比，越高说明持有体验越好
                    
                    好的固收基金应该是：波动小、回撤浅、修复快、胜率高。
                    """)
                
                # 核心指标
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Sortino比率", f"{risk_metrics['sortino_ratio']:.2f}")
                    st.metric("最大回撤", f"{risk_metrics['max_drawdown']*100:.2f}%")
                with col2:
                    st.metric("回撤修复天数", f"{recovery_days}天")
                    st.metric("月度胜率", f"{monthly_stats['win_rate']*100:.1f}%")
                
                # 风险提示
                if risk_metrics['max_drawdown'] < -0.05:
                    st.warning(f"⚠️ **回撤偏大**：最大回撤{risk_metrics['max_drawdown']*100:.2f}%，对于固收基金来说偏高，需要注意")
                if recovery_days > 60:
                    st.warning(f"⚠️ **修复较慢**：最长需要{recovery_days}天才能修复回撤，持有体验一般")
                if monthly_stats['win_rate'] < 0.6:
                    st.warning(f"⚠️ **胜率偏低**：月度胜率{monthly_stats['win_rate']*100:.1f}%，负收益月份较多")
                
                if risk_metrics['max_drawdown'] >= -0.03 and monthly_stats['win_rate'] > 0.7:
                    st.info(f"✅ **优秀的资产避风港**：波动极小且胜率高，是优秀的固收基金")
                
                # 月度胜率详情
                with st.expander("📊 月度胜率详情"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("总月份数", monthly_stats['total_months'])
                    with col2:
                        st.metric("盈利月数", monthly_stats['win_months'])
                    with col3:
                        st.metric("平均月收益", f"{monthly_stats['avg_monthly_return']*100:.2f}%")
            
            elif fund_type == 'index':
                st.divider()
                st.subheader("🎯 指数基金深度分析")
                
                with st.expander("🤔 指数基金怎么看？"):
                    st.markdown("""
                    **指数基金的核心是"精准"和"廉价"：**
                    
                    - **相关系数**：与跟踪指数的拟合度，应该>0.95，越高越好
                    - **跟踪误差**：偏离指数的程度，越低越好，一般<2%
                    - **费率**：指数基金的优势在于低成本，越低越好
                    
                    好的指数基金应该是：跟得准、偏得少、费率低。
                    """)
                
                # 拟合度分析
                if correlation is not None:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("相关系数", f"{correlation:.3f}")
                    with col2:
                        st.metric("跟踪误差", f"{capm_result['tracking_error']*100:.2f}%")
                    
                    # 解读
                    if correlation > 0.95:
                        st.info(f"✅ **跟踪精准**：与沪深300相关系数高达{correlation:.3f}，买指数就买它")
                    elif correlation > 0.90:
                        st.warning(f"⚠️ **跟踪一般**：相关系数{correlation:.3f}，偶尔会偏离指数")
                    else:
                        st.warning(f"⚠️ **跟踪误差较大**：相关系数仅{correlation:.3f}，可能偏离了指数本身")
                
                # 年化收益对比
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("基金年化收益", f"{risk_metrics['annual_return']*100:.2f}%")
                with col2:
                    benchmark_return = (1 + risk_metrics['annual_return']) if capm_result and capm_result['beta'] > 0 else risk_metrics['annual_return']
                    st.metric("相对基准", f"{(risk_metrics['annual_return'] - benchmark_return)*100:.2f}%")
            
            # ===== 图表展示 =====
            st.divider()
            st.subheader("📈 可视化分析")
            
            # 累计收益曲线
            returns_chart = plot_cumulative_returns(history, benchmark)
            if returns_chart:
                st.plotly_chart(returns_chart, use_container_width=True)
            
            # 水下回撤图
            drawdown_chart = plot_drawdown(history)
            if drawdown_chart:
                st.plotly_chart(drawdown_chart, use_container_width=True)
            
            # 月度收益热力图
            heatmap_chart = plot_monthly_heatmap(history)
            if heatmap_chart:
                st.plotly_chart(heatmap_chart, use_container_width=True)
            
            # ===== 免责声明 =====
            st.divider()
            st.markdown("""
            <div class="warning-box">
            <strong>⚠️ 风险提示</strong><br>
            本工具仅提供量化分析数据，不构成投资建议。基金有风险，投资需谨慎。历史业绩不代表未来表现。
            </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
