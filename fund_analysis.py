"""
基金穿透式诊断系统 v7.0
FOF研究员级别的深度量化分析
全新架构，彻底解决v6系列的数据崩溃问题
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import statsmodels.api as sm
import warnings
warnings.filterwarnings('ignore')

# ==================== 页面配置 ====================
st.set_page_config(
    page_title="基金深度穿透诊断 | FOF研究员视角",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==================== 全局CSS ====================
st.markdown("""
<style>
/* 整体背景 */
.stApp { background-color: #f4f6f9; }

/* 顶部标题区 */
.hero-title {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    padding: 32px 40px;
    border-radius: 16px;
    color: white;
    margin-bottom: 24px;
}
.hero-title h1 { font-size: 2rem; margin: 0 0 8px 0; }
.hero-title p { font-size: 1rem; opacity: 0.7; margin: 0; }

/* 指标卡片 */
.kpi-card {
    background: white;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    text-align: center;
    border-top: 3px solid #0f3460;
    height: 100%;
}
.kpi-value { font-size: 1.6rem; font-weight: 700; color: #0f3460; }
.kpi-label { font-size: 0.8rem; color: #888; margin-top: 4px; }
.kpi-sub { font-size: 0.75rem; color: #aaa; margin-top: 4px; }

.kpi-red .kpi-value { color: #e74c3c; }
.kpi-green .kpi-value { color: #27ae60; }
.kpi-orange .kpi-value { color: #e67e22; }

/* 诊断卡片 */
.diag-card {
    background: white;
    border-radius: 12px;
    padding: 20px 24px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    margin: 10px 0;
    border-left: 4px solid #0f3460;
}
.diag-title { font-size: 1rem; font-weight: 600; color: #333; margin-bottom: 10px; }
.diag-body { font-size: 0.9rem; color: #555; line-height: 1.7; }

.diag-warn { border-left-color: #e67e22; background: #fffbf0; }
.diag-danger { border-left-color: #e74c3c; background: #fff5f5; }
.diag-good { border-left-color: #27ae60; background: #f0fff4; }

/* 标签 */
.tag {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
    margin: 2px;
}
.tag-blue { background: #e8f0fe; color: #1967d2; }
.tag-red { background: #fce8e6; color: #c5221f; }
.tag-green { background: #e6f4ea; color: #137333; }
.tag-orange { background: #fef3e2; color: #e37400; }
.tag-gray { background: #f1f3f4; color: #5f6368; }

/* 分隔线 */
.section-divider {
    border: none;
    border-top: 2px solid #e8eaf0;
    margin: 28px 0;
}
</style>
""", unsafe_allow_html=True)


# ==================== 工具函数 ====================

def safe_float(val, default=None):
    """安全转换为浮点数"""
    try:
        v = float(str(val).replace('%', '').replace(',', '').strip())
        return v if not np.isnan(v) else default
    except:
        return default

def format_pct(val, decimals=2):
    if val is None:
        return "N/A"
    return f"{val*100:.{decimals}f}%"

def format_num(val, decimals=2):
    if val is None:
        return "N/A"
    return f"{val:.{decimals}f}"

def pct_color(val):
    """正负数颜色"""
    if val is None:
        return "kpi-card"
    return "kpi-card kpi-red" if val < 0 else "kpi-card kpi-green"


# ==================== 第一阶段：基础信息获取 ====================

@st.cache_data(ttl=7200)
def fetch_basic_info(symbol: str) -> dict:
    """获取基金基础信息，多接口容错"""
    result = {
        'name': symbol, 'type_raw': '', 'establish_date': '',
        'scale': '', 'company': '', 'manager': '', 'manager_years': '',
        'benchmark': '', 'fee_manage': '', 'fee_sale': '', 'fee_redeem': '',
        'turnover': '', 'fund_type': 'equity'  # equity / bond / index
    }

    # 接口1：雪球
    try:
        df = ak.fund_individual_basic_info_xq(symbol=symbol)
        info = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        result['name'] = info.get('基金名称', info.get('fund_name', symbol))
        result['type_raw'] = info.get('基金类型', info.get('fund_type', ''))
        result['establish_date'] = info.get('成立时间', info.get('establish_date', ''))
        result['scale'] = info.get('最新规模', info.get('fund_size', ''))
        result['company'] = info.get('基金公司', info.get('company_name', ''))
        result['manager'] = info.get('基金经理', info.get('fund_manager', ''))
        result['benchmark'] = info.get('业绩比较基准', '')
    except:
        pass

    # 接口2：天天基金（作为补充）
    if not result['type_raw']:
        try:
            df2 = ak.fund_open_fund_info_em(symbol=symbol, indicator="基金概况")
            info2 = dict(zip(df2.iloc[:, 0], df2.iloc[:, 1]))
            if not result['name'] or result['name'] == symbol:
                result['name'] = info2.get('基金名称', symbol)
            result['type_raw'] = info2.get('基金类型', '')
        except:
            pass

    # 费率信息
    try:
        fee_df = ak.fund_open_fund_daily_em()
        row = fee_df[fee_df['基金代码'] == symbol]
        if not row.empty:
            r = row.iloc[0]
            result['fee_sale'] = str(r.get('手续费', ''))
            if not result['name'] or result['name'] == symbol:
                result['name'] = r.get('基金简称', symbol)
            if not result['type_raw']:
                result['type_raw'] = r.get('类型', '')
    except:
        pass

    # 基金经理任职年限
    try:
        mgr_df = ak.fund_manager_em()
        if result['manager']:
            mgr_match = mgr_df[mgr_df['姓名'] == result['manager']]
            if not mgr_match.empty:
                result['manager_years'] = mgr_match.iloc[0].get('累计从业时间', '')
    except:
        pass

    # 判断基金类型
    raw = str(result['type_raw']).lower() + str(result['name']).lower()
    if any(k in raw for k in ['指数', 'etf', 'qdii指数', 'lof', '被动']):
        result['fund_type'] = 'index'
    elif any(k in raw for k in ['债券', '债', 'bond', '固收', '货币', '现金']):
        result['fund_type'] = 'bond'
    else:
        result['fund_type'] = 'equity'

    return result


@st.cache_data(ttl=3600)
def fetch_nav_history(symbol: str) -> pd.DataFrame:
    """获取净值历史数据"""
    try:
        df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        df = df.rename(columns={df.columns[0]: 'date', df.columns[1]: 'nav'})
        df['date'] = pd.to_datetime(df['date'])
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)
        return df
    except:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def fetch_benchmark(index_code: str = "000300") -> pd.DataFrame:
    """获取基准指数数据（默认沪深300）"""
    try:
        end = datetime.now().strftime('%Y%m%d')
        start = (datetime.now() - timedelta(days=5*365)).strftime('%Y%m%d')
        df = ak.index_zh_a_hist(symbol=index_code, period="daily", start_date=start, end_date=end)
        df['date'] = pd.to_datetime(df['日期'])
        df['close'] = pd.to_numeric(df['收盘'], errors='coerce')
        return df[['date', 'close']].dropna().sort_values('date').reset_index(drop=True)
    except:
        return pd.DataFrame()


@st.cache_data(ttl=7200)
def fetch_holdings(symbol: str) -> pd.DataFrame:
    """获取基金持仓"""
    for year in [str(datetime.now().year), str(datetime.now().year - 1)]:
        try:
            df = ak.fund_portfolio_hold_em(symbol=symbol, date=year)
            if df is not None and not df.empty and len(df) >= 3:
                return df
        except:
            continue
    return pd.DataFrame()


@st.cache_data(ttl=3600)
def fetch_stock_snapshot() -> pd.DataFrame:
    """获取A股快照（含PE/PB），多接口容错"""
    try:
        df = ak.stock_zh_a_spot_em()
        # 统一列名
        cols = {}
        for c in df.columns:
            if '代码' in c: cols[c] = '代码'
            elif '名称' in c: cols[c] = '名称'
            elif '市盈' in c and '动态' in c: cols[c] = 'PE'
            elif '市盈' in c: cols[c] = 'PE'
            elif '市净' in c: cols[c] = 'PB'
            elif '总市值' in c: cols[c] = '总市值'
        df = df.rename(columns=cols)
        needed = [c for c in ['代码', '名称', 'PE', 'PB', '总市值'] if c in df.columns]
        df = df[needed].copy()
        for c in ['PE', 'PB', '总市值']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        if '代码' in df.columns and 'PE' in df.columns and len(df) > 100:
            return df
    except:
        pass
    return pd.DataFrame()


# ==================== 第二阶段：量化指标计算 ====================

def calc_return_series(nav_df: pd.DataFrame) -> pd.Series:
    """计算日收益率"""
    nav = nav_df.set_index('date')['nav']
    return nav.pct_change().dropna()

def calc_max_drawdown(nav_df: pd.DataFrame):
    """最大回撤 + 回血天数"""
    nav = nav_df['nav'].values
    peak = np.maximum.accumulate(nav)
    dd = (nav - peak) / peak
    max_dd = dd.min()

    # 回血天数
    recovery_days = None
    dd_idx = np.argmin(dd)
    peak_before_dd = np.argmax(nav[:dd_idx+1]) if dd_idx > 0 else 0
    recovery_level = nav[peak_before_dd]
    for i in range(dd_idx, len(nav)):
        if nav[i] >= recovery_level:
            recovery_days = i - dd_idx
            break

    return abs(max_dd), recovery_days

def calc_equity_metrics(nav_df: pd.DataFrame, bench_df: pd.DataFrame) -> dict:
    """计算权益类指标：alpha/beta/sharpe/IR等"""
    metrics = {}
    if nav_df.empty or len(nav_df) < 60:
        return metrics

    # 对齐数据
    nav_ret = calc_return_series(nav_df)
    if not bench_df.empty:
        bench_ret = bench_df.set_index('date')['close'].pct_change().dropna()
        common = nav_ret.index.intersection(bench_ret.index)
        if len(common) < 30:
            bench_ret = None
        else:
            nav_aligned = nav_ret.loc[common]
            bench_aligned = bench_ret.loc[common]
    else:
        bench_ret = None
        nav_aligned = nav_ret

    # 年化收益率
    n_years = len(nav_df) / 252
    total_ret = nav_df['nav'].iloc[-1] / nav_df['nav'].iloc[0] - 1
    annual_ret = (1 + total_ret) ** (1 / max(n_years, 0.1)) - 1
    metrics['annual_ret'] = annual_ret
    metrics['total_ret'] = total_ret
    metrics['n_years'] = n_years

    # 年化波动率
    annual_vol = nav_ret.std() * np.sqrt(252)
    metrics['annual_vol'] = annual_vol

    # 夏普比率（无风险利率2.5%）
    rf = 0.025
    metrics['sharpe'] = (annual_ret - rf) / annual_vol if annual_vol > 0 else None

    # 最大回撤 + 回血天数
    metrics['max_dd'], metrics['recovery_days'] = calc_max_drawdown(nav_df)

    # 卡玛比率
    metrics['calmar'] = annual_ret / metrics['max_dd'] if metrics['max_dd'] > 0 else None

    # CAPM：Alpha/Beta
    if bench_ret is not None:
        X = sm.add_constant(bench_aligned.values)
        y = nav_aligned.values
        try:
            model = sm.OLS(y, X).fit()
            metrics['beta'] = model.params[1]
            daily_alpha = model.params[0]
            metrics['alpha'] = daily_alpha * 252
            metrics['alpha_pvalue'] = model.pvalues[1] if len(model.pvalues) > 1 else 1.0
            metrics['r_squared'] = model.rsquared
        except:
            pass

        # 信息比率
        excess = nav_aligned.values - bench_aligned.values
        te = excess.std() * np.sqrt(252)
        metrics['tracking_error'] = te
        metrics['info_ratio'] = (excess.mean() * 252) / te if te > 0 else None

    # 月度胜率
    nav_monthly = nav_df.set_index('date')['nav'].resample('ME').last().pct_change().dropna()
    metrics['win_rate'] = (nav_monthly > 0).mean() if len(nav_monthly) > 3 else None

    # 下行偏差
    neg_rets = nav_ret[nav_ret < 0]
    metrics['downside_vol'] = neg_rets.std() * np.sqrt(252) if len(neg_rets) > 5 else None

    return metrics


def calc_bond_metrics(nav_df: pd.DataFrame) -> dict:
    """计算债券类指标"""
    metrics = {}
    if nav_df.empty or len(nav_df) < 20:
        return metrics

    nav_ret = calc_return_series(nav_df)

    # 年化收益率
    n_years = len(nav_df) / 252
    total_ret = nav_df['nav'].iloc[-1] / nav_df['nav'].iloc[0] - 1
    annual_ret = (1 + total_ret) ** (1 / max(n_years, 0.1)) - 1
    metrics['annual_ret'] = annual_ret
    metrics['total_ret'] = total_ret
    metrics['n_years'] = n_years

    # 波动率
    annual_vol = nav_ret.std() * np.sqrt(252)
    metrics['annual_vol'] = annual_vol

    # 下行偏差（债券重点关注）
    neg_rets = nav_ret[nav_ret < 0]
    downside_vol = neg_rets.std() * np.sqrt(252) if len(neg_rets) > 5 else 0
    metrics['downside_vol'] = downside_vol

    # Sortino比率
    rf = 0.025
    metrics['sortino'] = (annual_ret - rf) / downside_vol if downside_vol > 0 else None

    # 最大回撤
    metrics['max_dd'], metrics['recovery_days'] = calc_max_drawdown(nav_df)

    # 月度胜率
    nav_monthly = nav_df.set_index('date')['nav'].resample('ME').last().pct_change().dropna()
    metrics['win_rate'] = (nav_monthly > 0).mean() if len(nav_monthly) > 3 else None

    # 收益构成分析（近12个月 vs 近3年总收益）
    if len(nav_df) >= 250:
        recent_1y = nav_df.tail(252)['nav']
        ret_1y = recent_1y.iloc[-1] / recent_1y.iloc[0] - 1
        metrics['ret_1y'] = ret_1y

        # 用净值变动估算票息贡献（债券基金净值缓慢上升部分≈利息）
        daily_rets = calc_return_series(nav_df.tail(252))
        positive_days = daily_rets[daily_rets > 0].sum()
        negative_days = daily_rets[daily_rets < 0].sum()
        metrics['ret_positive_contrib'] = positive_days  # 票息+资本利得
        metrics['ret_negative_contrib'] = negative_days  # 资本损失

    return metrics


# ==================== 第三阶段：持仓深度穿透 ====================

def analyze_holdings_equity(holdings: pd.DataFrame, snapshot_df: pd.DataFrame) -> dict:
    """权益类持仓穿透分析"""
    result = {'stocks': [], 'industries': {}, 'weighted_pe': None, 'weighted_pb': None,
              'weighted_roe': None, 'concentration': None}

    if holdings is None or holdings.empty:
        return result

    # 统一列名
    col_map = {}
    for c in holdings.columns:
        if '代码' in c: col_map[c] = '股票代码'
        elif '名称' in c or '股票名' in c: col_map[c] = '股票名称'
        elif '比例' in c or '占' in c or '比重' in c: col_map[c] = '占净值比例'
        elif '行业' in c: col_map[c] = '所属行业'
    holdings = holdings.rename(columns=col_map)

    if '占净值比例' in holdings.columns:
        holdings['占净值比例'] = pd.to_numeric(holdings['占净值比例'], errors='coerce')

    # 行业分布
    if '所属行业' in holdings.columns:
        industry_dist = holdings.groupby('所属行业')['占净值比例'].sum().dropna()
        result['industries'] = industry_dist.sort_values(ascending=False).head(8).to_dict()

    # 与估值快照合并
    if not snapshot_df.empty and '股票代码' in holdings.columns:
        merged = holdings.merge(snapshot_df, left_on='股票代码', right_on='代码', how='left')

        stocks_list = []
        for _, row in merged.iterrows():
            pe = safe_float(row.get('PE'))
            pb = safe_float(row.get('PB'))
            weight = safe_float(row.get('占净值比例', 0), 0)
            stocks_list.append({
                '代码': row.get('股票代码', ''),
                '名称': row.get('股票名称', row.get('名称', '')),
                '权重%': round(weight, 2) if weight else 0,
                'PE': round(pe, 1) if pe and 0 < pe < 500 else None,
                'PB': round(pb, 2) if pb and 0 < pb < 50 else None,
            })
        result['stocks'] = stocks_list

        # 加权PE/PB
        valid = [(s['权重%'], s['PE']) for s in stocks_list if s['PE'] and s['权重%']]
        if valid:
            total_w = sum(w for w, _ in valid)
            result['weighted_pe'] = sum(w * pe for w, pe in valid) / total_w if total_w > 0 else None

        valid_pb = [(s['权重%'], s['PB']) for s in stocks_list if s['PB'] and s['权重%']]
        if valid_pb:
            total_w = sum(w for w, _ in valid_pb)
            result['weighted_pb'] = sum(w * pb for w, pb in valid_pb) / total_w if total_w > 0 else None

        # HHI集中度
        weights = [s['权重%'] for s in stocks_list if s['权重%']]
        if weights:
            total = sum(weights)
            hhi = sum((w/total*100)**2 for w in weights)
            result['concentration'] = hhi

    return result


def analyze_holdings_bond(holdings: pd.DataFrame) -> dict:
    """债券类持仓穿透"""
    result = {'bonds': [], 'credit_aaa_pct': None, 'credit_below_aa_pct': None,
              'has_convertible': False}

    if holdings is None or holdings.empty:
        return result

    # 统一列名
    col_map = {}
    for c in holdings.columns:
        if '代码' in c: col_map[c] = '债券代码'
        elif '名称' in c: col_map[c] = '债券名称'
        elif '比例' in c or '占' in c: col_map[c] = '占净值比例'
        elif '评级' in c or '信用' in c: col_map[c] = '信用评级'
    holdings = holdings.rename(columns=col_map)

    if '占净值比例' in holdings.columns:
        holdings['占净值比例'] = pd.to_numeric(holdings['占净值比例'], errors='coerce')

    bonds_list = []
    for _, row in holdings.iterrows():
        name = str(row.get('债券名称', ''))
        rating = str(row.get('信用评级', ''))
        weight = safe_float(row.get('占净值比例', 0), 0)

        # 识别可转债
        if '转债' in name or '可转' in name:
            result['has_convertible'] = True

        bonds_list.append({'名称': name, '权重%': round(weight, 2), '评级': rating})

    result['bonds'] = bonds_list

    # 信用分层（按持仓名称和评级推断）
    aaa_weight = sum(b['权重%'] for b in bonds_list if 'AAA' in b['评级'].upper())
    below_aa_weight = sum(b['权重%'] for b in bonds_list
                         if any(r in b['评级'] for r in ['AA+', 'AA', 'A+', 'A-', 'BBB']))

    total = sum(b['权重%'] for b in bonds_list)
    if total > 0:
        result['credit_aaa_pct'] = aaa_weight / total * 100
        result['credit_below_aa_pct'] = below_aa_weight / total * 100

    return result


# ==================== 晨星风格箱 ====================

def calc_style_box(holdings_result: dict) -> str:
    """根据加权PE/PB估算晨星风格箱"""
    pe = holdings_result.get('weighted_pe')
    pb = holdings_result.get('weighted_pb')

    if pe is None and pb is None:
        return "数据不足"

    # 估值维度（价值/混合/成长）
    if pb and pe:
        score = 0
        if pb < 1.5: score -= 1
        elif pb > 3.5: score += 1
        if pe < 15: score -= 1
        elif pe > 35: score += 1
        val_label = "价值" if score <= -1 else ("成长" if score >= 1 else "混合")
    else:
        val_label = "混合"

    return val_label


# ==================== 第四阶段：可视化 ====================

def plot_nav_chart(nav_df: pd.DataFrame, bench_df: pd.DataFrame, fund_name: str):
    """累计收益率对比图"""
    fig = go.Figure()

    # 基金净值
    nav_norm = nav_df['nav'] / nav_df['nav'].iloc[0] * 100
    fig.add_trace(go.Scatter(
        x=nav_df['date'], y=nav_norm,
        name=fund_name, line=dict(color='#0f3460', width=2),
        hovertemplate='%{x|%Y-%m-%d}<br>净值: %{y:.1f}<extra></extra>'
    ))

    # 基准对比
    if not bench_df.empty:
        common_start = max(nav_df['date'].iloc[0], bench_df['date'].iloc[0])
        bench_cut = bench_df[bench_df['date'] >= common_start]
        nav_cut = nav_df[nav_df['date'] >= common_start]
        if not bench_cut.empty and not nav_cut.empty:
            bench_norm = bench_cut['close'] / bench_cut['close'].iloc[0] * 100
            fig.add_trace(go.Scatter(
                x=bench_cut['date'], y=bench_norm,
                name='沪深300', line=dict(color='#95a5a6', width=1.5, dash='dot'),
                hovertemplate='%{x|%Y-%m-%d}<br>基准: %{y:.1f}<extra></extra>'
            ))

    fig.update_layout(
        title=dict(text='累计净值表现（基准=100）', font=dict(size=14)),
        height=350, margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(orientation='h', y=-0.15),
        xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
        yaxis=dict(showgrid=True, gridcolor='#f0f0f0')
    )
    return fig


def plot_drawdown_chart(nav_df: pd.DataFrame):
    """水下回撤图"""
    nav = nav_df['nav'].values
    peak = np.maximum.accumulate(nav)
    drawdown = (nav - peak) / peak * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=nav_df['date'], y=drawdown,
        fill='tozeroy', fillcolor='rgba(231,76,60,0.15)',
        line=dict(color='#e74c3c', width=1.5),
        name='回撤',
        hovertemplate='%{x|%Y-%m-%d}<br>回撤: %{y:.2f}%<extra></extra>'
    ))

    fig.update_layout(
        title=dict(text='历史回撤曲线', font=dict(size=14)),
        height=250, margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor='white', paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
        yaxis=dict(showgrid=True, gridcolor='#f0f0f0', ticksuffix='%')
    )
    return fig


def plot_monthly_heatmap(nav_df: pd.DataFrame):
    """月度收益热力图"""
    nav_monthly = nav_df.set_index('date')['nav'].resample('ME').last().pct_change().dropna() * 100
    if len(nav_monthly) < 6:
        return None

    df_m = nav_monthly.reset_index()
    df_m.columns = ['date', 'ret']
    df_m['year'] = df_m['date'].dt.year
    df_m['month'] = df_m['date'].dt.month

    pivot = df_m.pivot(index='year', columns='month', values='ret')
    pivot.columns = ['1月','2月','3月','4月','5月','6月','7月','8月','9月','10月','11月','12月'][
        :len(pivot.columns)] if len(pivot.columns) <= 12 else pivot.columns

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index.astype(str),
        colorscale=[[0, '#27ae60'], [0.5, 'white'], [1, '#e74c3c']],
        zmid=0,
        text=[[f"{v:.1f}%" if not np.isnan(v) else "" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        hovertemplate='%{y}年 %{x}<br>收益: %{z:.2f}%<extra></extra>'
    ))

    fig.update_layout(
        title=dict(text='月度收益热力图（红涨绿跌）', font=dict(size=14)),
        height=max(200, len(pivot) * 35 + 80),
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor='white', paper_bgcolor='white'
    )
    return fig


def plot_industry_pie(industries: dict):
    """行业分布饼图"""
    if not industries:
        return None
    labels = list(industries.keys())
    values = list(industries.values())

    colors = px.colors.qualitative.Set3
    fig = go.Figure(data=go.Pie(
        labels=labels, values=values,
        hole=0.4,
        marker_colors=colors[:len(labels)],
        textinfo='label+percent',
        hovertemplate='%{label}<br>占比: %{value:.1f}%<extra></extra>'
    ))
    fig.update_layout(
        title=dict(text='重仓行业分布', font=dict(size=14)),
        height=320, margin=dict(l=10, r=10, t=40, b=10),
        showlegend=False
    )
    return fig


def plot_holdings_bar(stocks: list):
    """持仓权重条形图"""
    if not stocks:
        return None

    df = pd.DataFrame(stocks).head(10)
    df = df[df['权重%'] > 0].sort_values('权重%', ascending=True)

    fig = go.Figure(go.Bar(
        x=df['权重%'], y=df['名称'],
        orientation='h',
        marker_color='#0f3460',
        text=[f"{v:.1f}%" for v in df['权重%']],
        textposition='outside',
        hovertemplate='%{y}<br>权重: %{x:.1f}%<extra></extra>'
    ))

    fig.update_layout(
        title=dict(text='前十大重仓（%）', font=dict(size=14)),
        height=320, margin=dict(l=20, r=60, t=40, b=20),
        plot_bgcolor='white', paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
    )
    return fig


# ==================== 第五阶段：大白话诊断 ====================

def generate_diagnosis(basic: dict, metrics: dict, holdings_result: dict,
                        fund_type: str, n_years: float) -> dict:
    """生成四维诊断报告"""
    diag = {'character': '', 'skill': '', 'risk': '', 'avoid': '', 'summary': ''}

    name = basic.get('name', '该基金')
    annual_ret = metrics.get('annual_ret')
    annual_vol = metrics.get('annual_vol')
    sharpe = metrics.get('sharpe')
    max_dd = metrics.get('max_dd')
    beta = metrics.get('beta')
    alpha = metrics.get('alpha')
    alpha_pvalue = metrics.get('alpha_pvalue', 1.0)
    win_rate = metrics.get('win_rate')
    recovery_days = metrics.get('recovery_days')
    sortino = metrics.get('sortino')
    w_pe = holdings_result.get('weighted_pe')
    w_pb = holdings_result.get('weighted_pb')
    concentration = holdings_result.get('concentration')

    ret_str = format_pct(annual_ret) if annual_ret else "N/A"
    dd_str = format_pct(max_dd) if max_dd else "N/A"

    # ===== 性格诊断 =====
    if fund_type == 'equity':
        style = calc_style_box(holdings_result) if holdings_result else "N/A"
        if beta and beta > 1.2:
            char = f"**激进型选手**。{name}的Beta值达{beta:.2f}，大幅高于市场，牛市加速涨、熊市加速跌，风格{style}偏向，属于'放大市场'型选手。"
        elif beta and beta < 0.7:
            char = f"**防守型选手**。{name}的Beta仅{beta:.2f}，擅长控制市场风险，但也会在强牛市中跑输大盘，风格偏{style}防御。"
        else:
            char = f"**均衡型选手**。{name}的Beta约{beta:.2f if beta else 'N/A'}，随市场波动适中，风格偏{style}，稳健为主。"
    elif fund_type == 'bond':
        sortino_v = sortino if sortino else 0
        if sortino_v > 2:
            char = f"**高效债券猎手**。Sortino比率{sortino_v:.2f}，控制下行风险能力出色，每承担1单位下行风险能获取的超额收益高于同类。"
        elif sortino_v > 1:
            char = f"**稳健固收专家**。Sortino比率{sortino_v:.2f}，整体风险收益匹配良好，适合稳健投资者。"
        else:
            char = f"**保守型固收产品**。Sortino比率偏低，说明下行波动对收益的侵蚀较明显，需关注信用风险。"
    else:  # index
        char = f"**被动跟踪型**。{name}为指数/ETF产品，策略透明，费率低，与所跟踪指数高度联动。"

    diag['character'] = char

    # ===== 实力诊断（运气 vs 本事）=====
    if fund_type == 'equity':
        if alpha is not None:
            if alpha > 0.05 and alpha_pvalue < 0.05:
                skill = f"年化Alpha达 **{alpha*100:.1f}%**，统计显著（p={alpha_pvalue:.3f}），**这是真本事**，超额收益可靠，并非市场Beta的搭便车。"
            elif alpha > 0 and alpha_pvalue >= 0.05:
                skill = f"年化Alpha为正（{alpha*100:.1f}%），但统计不显著（p={alpha_pvalue:.3f}），**超额收益存疑**，可能有一定运气成分，需更长时间验证。"
            else:
                skill = f"年化Alpha为负（{alpha*100:.1f}%），**跑输基准调整后的风险收益**，需警惕是否只是Beta收益的包装产品。"
        else:
            skill = f"历史数据不足，无法有效评估Alpha。年化收益约{ret_str}。"
    else:
        wr_str = f"{win_rate*100:.1f}%" if win_rate else "N/A"
        skill = f"年化收益{ret_str}，月度胜率{wr_str}，从历史表现看，{'收益稳定性较强，是实力的体现。' if win_rate and win_rate > 0.65 else '胜率有限，市场择时能力有待验证。'}"

    diag['skill'] = skill

    # ===== 风险警告 =====
    risks = []
    if max_dd and max_dd > 0.30:
        risks.append(f"⚠️ **历史最大回撤高达{dd_str}**，极端行情下可能出现大幅亏损。")
    if recovery_days and recovery_days > 365:
        risks.append(f"⚠️ **回血耗时约{recovery_days}天（超过1年）**，长期资金才适合参与。")
    if annual_vol and annual_vol > 0.25:
        risks.append(f"⚠️ **年化波动率{format_pct(annual_vol)}较高**，心理承受能力弱的投资者慎入。")
    if w_pe and w_pe > 50:
        risks.append(f"⚠️ **持仓加权PE约{w_pe:.0f}倍**，估值偏高，需关注业绩增速能否匹配。")
    if concentration and concentration > 2500:
        risks.append(f"⚠️ **持仓高度集中（HHI={concentration:.0f}）**，单只个股踩雷影响较大。")
    if fund_type == 'bond' and holdings_result.get('has_convertible'):
        risks.append("⚠️ **持有可转债**，存在一定股性波动风险，非纯债投资者需知晓。")
    if not risks:
        risks.append("✅ 主要风险指标在合理范围内，未发现明显异常风险。")

    diag['risk'] = '\n\n'.join(risks)

    # ===== 避坑指南 =====
    avoid_list = []
    if max_dd and max_dd > 0.25:
        avoid_list.append("❌ **保守型投资者**：历史回撤超25%，无法承受短期大幅亏损者不适合")
    if n_years < 1:
        avoid_list.append("❌ **追求确定性的投资者**：成立不足1年，量化指标可靠性有限")
    if beta and beta > 1.3:
        avoid_list.append("❌ **波动恐惧者**：Beta>1.3，市场下跌时损失将被放大")
    if sharpe and sharpe < 0.3:
        avoid_list.append("❌ **期望稳健收益者**：夏普比率偏低，风险收益比不佳")
    if w_pe and w_pe > 60:
        avoid_list.append("❌ **价值投资者**：当前估值偏高，安全边际有限")
    if not avoid_list:
        avoid_list.append("✅ 该产品适合人群较广，但仍需结合个人风险承受能力判断。")

    diag['avoid'] = '\n\n'.join(avoid_list)

    # ===== 总结 =====
    rating_score = 50
    if annual_ret and annual_ret > 0.10: rating_score += 10
    if annual_ret and annual_ret > 0.15: rating_score += 5
    if sharpe and sharpe > 1.0: rating_score += 10
    if alpha and alpha > 0.03: rating_score += 10
    if max_dd and max_dd < 0.15: rating_score += 10
    if max_dd and max_dd > 0.35: rating_score -= 15
    if win_rate and win_rate > 0.60: rating_score += 5
    if w_pe and w_pe > 50: rating_score -= 5
    rating_score = max(10, min(95, rating_score))

    if rating_score >= 75:
        rating = "⭐⭐⭐⭐⭐ 优质产品"
        color = "#27ae60"
    elif rating_score >= 60:
        rating = "⭐⭐⭐⭐ 良好产品"
        color = "#2980b9"
    elif rating_score >= 45:
        rating = "⭐⭐⭐ 中规中矩"
        color = "#e67e22"
    else:
        rating = "⭐⭐ 需要谨慎"
        color = "#e74c3c"

    diag['rating'] = rating
    diag['rating_score'] = rating_score
    diag['rating_color'] = color

    return diag


# ==================== 主界面 ====================

def render_kpi(label: str, value: str, sub: str = "", color_class: str = "kpi-card"):
    return f"""<div class="{color_class}">
    <div class="kpi-value">{value}</div>
    <div class="kpi-label">{label}</div>
    {'<div class="kpi-sub">' + sub + '</div>' if sub else ''}
    </div>"""


def main():
    # 标题
    st.markdown("""
    <div class="hero-title">
        <h1>🔬 基金深度穿透诊断</h1>
        <p>FOF研究员视角 · 五阶段量化分析 · 大白话诊断报告</p>
    </div>
    """, unsafe_allow_html=True)

    # 输入区
    col_input, col_btn = st.columns([3, 1])
    with col_input:
        symbol = st.text_input(
            "基金代码",
            value="011040",
            placeholder="输入6位基金代码，例如：011040",
            label_visibility="collapsed"
        )
    with col_btn:
        run = st.button("🔍 开始诊断", use_container_width=True, type="primary")

    if not run:
        # 使用说明
        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.info("**📊 权益类基金**\nAlpha/Beta · 夏普 · 回撤\n晨星风格箱 · 持仓估值穿透")
        with c2:
            st.info("**🏦 债券类基金**\nSortino · 久期分析\n信用分层 · 收益构成")
        with c3:
            st.info("**📈 指数/ETF**\n跟踪误差 · 相关性\n估值分位 · 费率对比")
        return

    symbol = symbol.strip()
    if not symbol or len(symbol) != 6:
        st.error("请输入正确的6位基金代码")
        return

    # ===================== 数据加载 =====================
    with st.spinner(""):
        basic = fetch_basic_info(symbol)
        nav_df = fetch_nav_history(symbol)
        bench_df = fetch_benchmark()
        holdings_raw = fetch_holdings(symbol)
        fund_type = basic.get('fund_type', 'equity')

        # 获取估值快照（权益类才需要）
        snapshot_df = pd.DataFrame()
        if fund_type in ('equity', 'index'):
            snapshot_df = fetch_stock_snapshot()

    if nav_df.empty:
        st.error(f"无法获取基金 {symbol} 的净值数据，请检查基金代码是否正确。")
        return

    # ===================== 计算指标 =====================
    n_years = len(nav_df) / 252
    is_new_fund = n_years < 1

    if fund_type in ('equity', 'index'):
        metrics = calc_equity_metrics(nav_df, bench_df)
        holdings_result = analyze_holdings_equity(holdings_raw, snapshot_df)
    else:
        metrics = calc_bond_metrics(nav_df)
        holdings_result = analyze_holdings_bond(holdings_raw)

    diag = generate_diagnosis(basic, metrics, holdings_result, fund_type, n_years)

    # ===================== 展示区 =====================

    # 基本信息条
    fund_name = basic.get('name', symbol)
    type_label = {"equity": "权益类（股票/混合）", "bond": "债券类（固收）", "index": "指数/ETF"}
    type_tags = {"equity": "tag-blue", "bond": "tag-orange", "index": "tag-green"}

    st.markdown(f"""
    <div style="background:white;border-radius:12px;padding:20px 24px;
    box-shadow:0 2px 12px rgba(0,0,0,0.06);margin:16px 0 8px 0;">
        <div style="display:flex;align-items:center;flex-wrap:wrap;gap:12px;">
            <span style="font-size:1.4rem;font-weight:700;color:#1a1a2e;">{fund_name}</span>
            <span style="color:#888;font-size:0.9rem;">{symbol}</span>
            <span class="tag {type_tags.get(fund_type,'tag-gray')}">{type_label.get(fund_type,'')}</span>
            {'<span class="tag tag-red">⚠️ 成立不足1年，指标仅供参考</span>' if is_new_fund else ''}
        </div>
        <div style="margin-top:12px;display:flex;flex-wrap:wrap;gap:16px;font-size:0.85rem;color:#555;">
            {'<span>📅 成立：' + str(basic.get('establish_date','N/A')) + '</span>' if basic.get('establish_date') else ''}
            {'<span>👤 经理：' + str(basic.get('manager','N/A')) + ('（' + str(basic.get('manager_years')) + '）' if basic.get('manager_years') else '') + '</span>' if basic.get('manager') else ''}
            {'<span>🏢 公司：' + str(basic.get('company','N/A')) + '</span>' if basic.get('company') else ''}
            {'<span>💰 规模：' + str(basic.get('scale','N/A')) + '</span>' if basic.get('scale') else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ===================== Tab布局 =====================
    tab1, tab2, tab3, tab4 = st.tabs(["📊 量化看板", "🔬 持仓穿透", "📈 图表", "🩺 诊断报告"])

    # ---- Tab1: 量化看板 ----
    with tab1:
        if fund_type in ('equity', 'index'):
            st.markdown("#### 核心业绩指标")
            r1c1, r1c2, r1c3, r1c4 = st.columns(4)
            annual_ret = metrics.get('annual_ret')
            with r1c1:
                st.markdown(render_kpi(
                    "年化收益率",
                    format_pct(annual_ret) if annual_ret else "N/A",
                    f"成立以来{format_pct(metrics.get('total_ret'))}",
                    "kpi-card kpi-red" if (annual_ret and annual_ret < 0) else "kpi-card kpi-green"
                ), unsafe_allow_html=True)
            with r1c2:
                st.markdown(render_kpi(
                    "年化波动率",
                    format_pct(metrics.get('annual_vol')),
                    "年化标准差",
                    "kpi-card"
                ), unsafe_allow_html=True)
            with r1c3:
                sharpe = metrics.get('sharpe')
                st.markdown(render_kpi(
                    "夏普比率",
                    format_num(sharpe),
                    ">1.0 优秀 | >0.5 良好",
                    "kpi-card kpi-green" if (sharpe and sharpe > 1) else (
                        "kpi-card kpi-orange" if (sharpe and sharpe > 0.5) else "kpi-card")
                ), unsafe_allow_html=True)
            with r1c4:
                st.markdown(render_kpi(
                    "最大回撤",
                    format_pct(metrics.get('max_dd')),
                    f"回血约{metrics.get('recovery_days', '—')}天" if metrics.get('recovery_days') else "未完全回血",
                    "kpi-card kpi-red" if (metrics.get('max_dd') and metrics['max_dd'] > 0.25) else "kpi-card"
                ), unsafe_allow_html=True)

            st.markdown("#### CAPM 归因分析")
            r2c1, r2c2, r2c3, r2c4 = st.columns(4)
            alpha = metrics.get('alpha')
            beta = metrics.get('beta')
            alpha_pvalue = metrics.get('alpha_pvalue')
            with r2c1:
                st.markdown(render_kpi(
                    "Alpha（年化）",
                    format_pct(alpha) if alpha else "N/A",
                    f"p值={alpha_pvalue:.3f}" if alpha_pvalue else "",
                    "kpi-card kpi-green" if (alpha and alpha > 0) else "kpi-card kpi-red"
                ), unsafe_allow_html=True)
            with r2c2:
                st.markdown(render_kpi(
                    "Beta",
                    format_num(beta),
                    "<1 防守 | >1 进攻",
                    "kpi-card"
                ), unsafe_allow_html=True)
            with r2c3:
                st.markdown(render_kpi(
                    "信息比率",
                    format_num(metrics.get('info_ratio')),
                    ">0.5 较好",
                    "kpi-card"
                ), unsafe_allow_html=True)
            with r2c4:
                st.markdown(render_kpi(
                    "月度胜率",
                    format_pct(metrics.get('win_rate'), 1) if metrics.get('win_rate') else "N/A",
                    ">60% 优秀",
                    "kpi-card kpi-green" if (metrics.get('win_rate') and metrics['win_rate'] > 0.6)
                    else "kpi-card"
                ), unsafe_allow_html=True)

        else:  # bond
            st.markdown("#### 债券基金核心指标")
            r1c1, r1c2, r1c3, r1c4 = st.columns(4)
            with r1c1:
                annual_ret = metrics.get('annual_ret')
                st.markdown(render_kpi(
                    "年化收益率",
                    format_pct(annual_ret),
                    f"总收益{format_pct(metrics.get('total_ret'))}",
                    "kpi-card kpi-green" if (annual_ret and annual_ret > 0) else "kpi-card kpi-red"
                ), unsafe_allow_html=True)
            with r1c2:
                sortino = metrics.get('sortino')
                st.markdown(render_kpi(
                    "Sortino比率",
                    format_num(sortino),
                    "只看下行风险的夏普",
                    "kpi-card kpi-green" if (sortino and sortino > 2) else "kpi-card"
                ), unsafe_allow_html=True)
            with r1c3:
                st.markdown(render_kpi(
                    "最大回撤",
                    format_pct(metrics.get('max_dd')),
                    f"回血约{metrics.get('recovery_days', '—')}天" if metrics.get('recovery_days') else "",
                    "kpi-card kpi-red" if (metrics.get('max_dd') and metrics['max_dd'] > 0.05) else "kpi-card"
                ), unsafe_allow_html=True)
            with r1c4:
                st.markdown(render_kpi(
                    "月度胜率",
                    format_pct(metrics.get('win_rate'), 1) if metrics.get('win_rate') else "N/A",
                    ">65% 优秀",
                    "kpi-card kpi-green" if (metrics.get('win_rate') and metrics['win_rate'] > 0.65)
                    else "kpi-card"
                ), unsafe_allow_html=True)

            if metrics.get('ret_1y') is not None:
                st.markdown("#### 近1年收益构成分析")
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown(render_kpi("近1年收益",
                        format_pct(metrics.get('ret_1y')), "", "kpi-card"), unsafe_allow_html=True)
                with c2:
                    st.markdown(render_kpi("下行偏差（年化）",
                        format_pct(metrics.get('downside_vol')), "只计算亏损日的波动", "kpi-card"),
                        unsafe_allow_html=True)
                with c3:
                    st.markdown(render_kpi("年化波动率",
                        format_pct(metrics.get('annual_vol')), "", "kpi-card"), unsafe_allow_html=True)

    # ---- Tab2: 持仓穿透 ----
    with tab2:
        if fund_type in ('equity', 'index'):
            stocks = holdings_result.get('stocks', [])
            w_pe = holdings_result.get('weighted_pe')
            w_pb = holdings_result.get('weighted_pb')
            style = calc_style_box(holdings_result)
            hhi = holdings_result.get('concentration')

            st.markdown("#### 持仓估值画像")
            pc1, pc2, pc3, pc4 = st.columns(4)
            with pc1:
                st.markdown(render_kpi("加权平均PE",
                    f"{w_pe:.1f}x" if w_pe else "N/A",
                    "前十大重仓加权",
                    "kpi-card kpi-red" if (w_pe and w_pe > 50) else "kpi-card"
                ), unsafe_allow_html=True)
            with pc2:
                st.markdown(render_kpi("加权平均PB",
                    f"{w_pb:.2f}x" if w_pb else "N/A",
                    "前十大重仓加权",
                    "kpi-card"
                ), unsafe_allow_html=True)
            with pc3:
                st.markdown(render_kpi("晨星风格",
                    style, "基于估值推断", "kpi-card"
                ), unsafe_allow_html=True)
            with pc4:
                st.markdown(render_kpi("持仓集中度(HHI)",
                    f"{hhi:.0f}" if hhi else "N/A",
                    "<1000分散 | >2500集中",
                    "kpi-card kpi-orange" if (hhi and hhi > 2500) else "kpi-card"
                ), unsafe_allow_html=True)

            if stocks:
                st.markdown("#### 前十大重仓个股估值")
                display_stocks = []
                for s in stocks[:10]:
                    display_stocks.append({
                        '股票名称': s['名称'],
                        '代码': s['代码'],
                        '持仓权重': f"{s['权重%']}%",
                        'PE': f"{s['PE']:.1f}x" if s['PE'] else "N/A",
                        'PB': f"{s['PB']:.2f}x" if s['PB'] else "N/A",
                        '估值判断': (
                            "🟢 低估" if (s['PE'] and s['PE'] < 20) else
                            ("🔴 较高" if (s['PE'] and s['PE'] > 50) else "🟡 合理")
                        ) if s['PE'] else "—"
                    })
                st.dataframe(pd.DataFrame(display_stocks), use_container_width=True, hide_index=True)

                if not snapshot_df.empty:
                    st.caption("✅ 已从实时市场获取估值数据")
                else:
                    st.caption("⚠️ 未能获取实时估值数据，PE/PB显示N/A")

        else:  # bond
            bonds = holdings_result.get('bonds', [])
            st.markdown("#### 债券持仓信用分析")
            bc1, bc2, bc3 = st.columns(3)
            with bc1:
                aaa_pct = holdings_result.get('credit_aaa_pct')
                st.markdown(render_kpi("AAA级占比",
                    f"{aaa_pct:.1f}%" if aaa_pct else "N/A",
                    "越高信用质量越好",
                    "kpi-card kpi-green" if (aaa_pct and aaa_pct > 60) else "kpi-card"
                ), unsafe_allow_html=True)
            with bc2:
                below_aa = holdings_result.get('credit_below_aa_pct')
                st.markdown(render_kpi("AA及以下占比",
                    f"{below_aa:.1f}%" if below_aa else "N/A",
                    "占比越高信用风险越大",
                    "kpi-card kpi-orange" if (below_aa and below_aa > 20) else "kpi-card"
                ), unsafe_allow_html=True)
            with bc3:
                has_conv = holdings_result.get('has_convertible', False)
                st.markdown(render_kpi("含权债（可转债）",
                    "✅ 持有" if has_conv else "❌ 不含",
                    "含可转债则有股性波动",
                    "kpi-card kpi-orange" if has_conv else "kpi-card"
                ), unsafe_allow_html=True)

            if bonds:
                st.markdown("#### 持仓明细")
                st.dataframe(pd.DataFrame(bonds[:20]), use_container_width=True, hide_index=True)

    # ---- Tab3: 图表 ----
    with tab3:
        # 净值图
        fig1 = plot_nav_chart(nav_df, bench_df, fund_name)
        st.plotly_chart(fig1, use_container_width=True)

        # 回撤图
        fig2 = plot_drawdown_chart(nav_df)
        st.plotly_chart(fig2, use_container_width=True)

        col_left, col_right = st.columns(2)

        # 月度热力图
        with col_left:
            fig3 = plot_monthly_heatmap(nav_df)
            if fig3:
                st.plotly_chart(fig3, use_container_width=True)

        # 行业/持仓分布
        with col_right:
            if fund_type in ('equity', 'index'):
                industries = holdings_result.get('industries', {})
                stocks = holdings_result.get('stocks', [])
                if industries:
                    fig4 = plot_industry_pie(industries)
                    if fig4:
                        st.plotly_chart(fig4, use_container_width=True)
                elif stocks:
                    fig5 = plot_holdings_bar(stocks)
                    if fig5:
                        st.plotly_chart(fig5, use_container_width=True)
            else:
                bonds = holdings_result.get('bonds', [])
                if bonds:
                    bond_data = pd.DataFrame(bonds[:10])
                    st.markdown("**持仓债券明细**")
                    st.dataframe(bond_data, use_container_width=True, hide_index=True)

    # ---- Tab4: 诊断报告 ----
    with tab4:
        # 综合评分
        score = diag.get('rating_score', 50)
        rating = diag.get('rating', '⭐⭐⭐')
        color = diag.get('rating_color', '#e67e22')

        st.markdown(f"""
        <div style="background:white;border-radius:12px;padding:24px;
        box-shadow:0 2px 12px rgba(0,0,0,0.06);margin:0 0 20px 0;text-align:center;">
            <div style="font-size:2rem;color:{color};font-weight:700;">{rating}</div>
            <div style="font-size:3rem;font-weight:900;color:{color};margin:8px 0;">{score}</div>
            <div style="color:#888;font-size:0.85rem;">综合诊断评分（满分100）</div>
        </div>
        """, unsafe_allow_html=True)

        # 四维诊断
        st.markdown("### 🎭 性格诊断：这是个什么类型的选手？")
        st.markdown(f"""<div class="diag-card diag-good">
        <div class="diag-body">{diag['character']}</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("### 🧪 实力诊断：这是运气还是本事？")
        st.markdown(f"""<div class="diag-card">
        <div class="diag-body">{diag['skill']}</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("### ⚠️ 风险警告：这里有什么坑？")
        st.markdown(f"""<div class="diag-card diag-warn">
        <div class="diag-body">{diag['risk']}</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("### 🚫 避坑指南：哪类人千万不能买？")
        st.markdown(f"""<div class="diag-card diag-danger">
        <div class="diag-body">{diag['avoid']}</div>
        </div>""", unsafe_allow_html=True)

        # 数据表尾
        with st.expander("📋 查看完整指标数据"):
            all_metrics = {}
            for k, v in metrics.items():
                if isinstance(v, float):
                    all_metrics[k] = round(v, 6)
                else:
                    all_metrics[k] = v
            st.json(all_metrics)

    # 免责声明
    st.markdown("---")
    st.caption("⚠️ 本报告基于公开数据和量化模型自动生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。")


if __name__ == "__main__":
    main()
