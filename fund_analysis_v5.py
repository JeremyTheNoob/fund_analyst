"""
基金全维度体检报告 v5.0
角色：顶级量化基金研究员 + 理财顾问大白话解读

三大维度：
1. 业绩归因 (Alpha/Beta/Sharpe/MDD)
2. 持仓风格穿透 (行业分布/集中度/风格判定)
3. 风险与集中度分析 (CR10/赛道赌徒判定)

输出：专业数据看板 + 大白话体检总结
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import statsmodels.api as sm
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import warnings
warnings.filterwarnings("ignore")

# ===================== 页面配置 =====================
st.set_page_config(
    page_title="基金全维度体检",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    .banner {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 50%, #3d7ab5 100%);
        padding: 2rem 1rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .banner h1 { font-size: 2rem; margin: 0; letter-spacing: 3px; }
    .banner p { font-size: 1rem; margin: 0.5rem 0 0; opacity: 0.85; }

    .section-title {
        font-size: 1.3rem;
        font-weight: bold;
        color: #1e3a5f;
        border-left: 5px solid #3d7ab5;
        padding-left: 12px;
        margin: 1.5rem 0 1rem;
    }

    .metric-card {
        background: #f8fafc;
        border-radius: 10px;
        padding: 16px;
        margin: 8px 0;
        border: 1px solid #e2e8f0;
    }
    .metric-value {
        font-size: 1.6rem;
        font-weight: bold;
        color: #1e293b;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #64748b;
        margin-bottom: 4px;
    }
    .metric-interpret {
        font-size: 0.8rem;
        margin-top: 6px;
        padding: 4px 8px;
        border-radius: 4px;
        display: inline-block;
    }

    .diagnosis-box {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        border-radius: 12px;
        padding: 20px;
        margin: 16px 0;
        border-left: 5px solid #0ea5e9;
    }
    .diagnosis-title {
        font-size: 1.1rem;
        font-weight: bold;
        color: #0369a1;
        margin-bottom: 12px;
    }
    .diagnosis-content {
        font-size: 1rem;
        line-height: 1.8;
        color: #334155;
    }

    .warning-box {
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
        border-radius: 12px;
        padding: 16px 20px;
        margin: 16px 0;
        border-left: 5px solid #f59e0b;
    }
    .danger-box {
        background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%);
        border-radius: 12px;
        padding: 16px 20px;
        margin: 16px 0;
        border-left: 5px solid #ef4444;
    }
    .success-box {
        background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
        border-radius: 12px;
        padding: 16px 20px;
        margin: 16px 0;
        border-left: 5px solid #10b981;
    }

    .personality-tag {
        display: inline-block;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 0.9rem;
        font-weight: bold;
        margin: 4px;
    }
    .tag-aggressive { background: #fecaca; color: #991b1b; }
    .tag-steady { background: #bfdbfe; color: #1e40af; }
    .tag-balanced { background: #bbf7d0; color: #166534; }
    .tag-gambler { background: #fde68a; color: #92400e; }

    #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ===================== 数据获取层 =====================

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fund_nav(symbol: str, years: int = 3) -> pd.DataFrame | None:
    """获取基金历史净值"""
    try:
        df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        df.columns = ["date", "nav", "daily_ret"]
        df["date"] = pd.to_datetime(df["date"])
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.sort_values("date").dropna(subset=["nav"]).reset_index(drop=True)
        cutoff = df["date"].max() - timedelta(days=365 * years)
        return df[df["date"] >= cutoff].reset_index(drop=True)
    except Exception as e:
        st.error(f"净值数据获取失败：{e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_benchmark(symbol: str = "sh000300") -> pd.DataFrame | None:
    """获取沪深300基准数据"""
    try:
        df = ak.stock_zh_index_daily(symbol=symbol)
        df = df[["date", "close"]].copy()
        df.columns = ["date", "bench"]
        df["date"] = pd.to_datetime(df["date"])
        df["bench"] = pd.to_numeric(df["bench"], errors="coerce")
        return df.sort_values("date").dropna().reset_index(drop=True)
    except Exception:
        try:
            df = ak.stock_zh_index_daily_em(symbol=symbol)
            df = df[["date", "close"]].copy()
            df.columns = ["date", "bench"]
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date").dropna().reset_index(drop=True)
        except Exception as e:
            st.warning(f"基准数据获取失败：{e}")
            return None


@st.cache_data(ttl=7200, show_spinner=False)
def fetch_portfolio(symbol: str) -> pd.DataFrame | None:
    """获取基金持仓数据"""
    for year in [datetime.now().year, datetime.now().year - 1]:
        try:
            df = ak.fund_portfolio_hold_em(symbol=symbol, date=str(year))
            if df is not None and not df.empty:
                # 标准化列名
                df = df.rename(columns={
                    "股票代码": "code",
                    "股票名称": "name",
                    "占净值比例": "weight",
                    "持仓市值": "mv",
                    "所处行业": "sector"
                })
                # 提取数字
                df["weight"] = pd.to_numeric(df["weight"].astype(str).str.replace("%", ""), errors="coerce")
                return df
        except Exception:
            continue
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fund_basic(symbol: str) -> dict:
    """获取基金基本信息"""
    try:
        df = ak.fund_open_fund_daily_em()
        row = df[df["基金代码"] == symbol]
        if row.empty:
            return {}
        r = row.iloc[0].to_dict()
        nav_cols = [c for c in r if "单位净值" in c and "2" in c]
        nav = 0.0
        for c in nav_cols:
            try:
                nav = float(r[c])
                if nav > 0:
                    break
            except Exception:
                pass
        try:
            growth = float(r.get("日增长率", 0))
        except Exception:
            growth = 0.0
        return {
            "name": r.get("基金简称", symbol),
            "nav": nav,
            "growth": growth,
            "buy": r.get("申购状态", "--"),
            "sell": r.get("赎回状态", "--"),
            "fee": r.get("手续费", "--"),
        }
    except Exception:
        return {"name": symbol}


# ===================== 核心计算引擎 =====================

def calc_performance_attribution(df_nav: pd.DataFrame, df_bench: pd.DataFrame) -> dict:
    """
    维度一：业绩归因
    计算 Alpha, Beta, Sharpe, 最大回撤
    """
    # 合并数据
    merged = pd.merge(df_nav[["date", "nav"]], df_bench[["date", "bench"]], on="date", how="inner")
    merged["fund_ret"] = merged["nav"].pct_change()
    merged["bench_ret"] = merged["bench"].pct_change()
    merged = merged.dropna()

    if len(merged) < 60:
        return {"error": "数据不足，需要至少60个交易日"}

    # 基础指标
    nav = df_nav["nav"]
    total_ret = nav.iloc[-1] / nav.iloc[0] - 1
    total_days = (df_nav["date"].iloc[-1] - df_nav["date"].iloc[0]).days
    annual_ret = (1 + total_ret) ** (365 / max(total_days, 1)) - 1

    # 最大回撤
    cummax = nav.cummax()
    drawdown = (nav - cummax) / cummax
    max_dd = drawdown.min()

    # 夏普比率
    rets = merged["fund_ret"]
    rf_daily = 0.02 / 252
    sharpe = (rets.mean() - rf_daily) / rets.std() * np.sqrt(252) if rets.std() > 0 else 0

    # Alpha/Beta 回归
    y = rets - rf_daily
    X = sm.add_constant(merged["bench_ret"] - rf_daily)
    model = sm.OLS(y, X).fit()

    alpha = model.params["const"] * 252  # 年化
    beta = model.params["bench_ret"]
    r2 = model.rsquared
    p_alpha = model.pvalues["const"]

    # 信息比率
    tracking_err = model.resid.std() * np.sqrt(252)
    info_ratio = alpha / tracking_err if tracking_err > 0 else 0

    return {
        "annual_ret": annual_ret,
        "total_ret": total_ret,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "alpha": alpha,
        "beta": beta,
        "r2": r2,
        "p_alpha": p_alpha,
        "info_ratio": info_ratio,
        "tracking_err": tracking_err,
        "merged": merged
    }


def calc_portfolio_style(portfolio: pd.DataFrame) -> dict:
    """
    维度二：持仓风格穿透
    分析行业分布、集中度、风格判定
    """
    if portfolio is None or portfolio.empty:
        return {"error": "无持仓数据"}

    # CR10: 前十大占比
    top10 = portfolio.head(10)
    cr10 = top10["weight"].sum() if "weight" in top10.columns else 0

    # 行业分布
    sector_dist = {}
    if "sector" in portfolio.columns:
        sector_dist = portfolio.groupby("sector")["weight"].sum().sort_values(ascending=False).to_dict()

    # 风格判定
    style = "未知"
    style_desc = ""

    if cr10 > 60:
        style = "高度集中型"
        style_desc = "前十大重仓占比超60%，经理偏好集中押注"
    elif cr10 > 40:
        style = "适度集中型"
        style_desc = "前十大占比40-60%，在集中与分散间平衡"
    else:
        style = "分散配置型"
        style_desc = "前十大占比低于40%，偏好广泛分散"

    # 赛道赌徒判定
    sector_gambler = False
    top_sector = ""
    top_sector_pct = 0

    if sector_dist:
        top_sector = list(sector_dist.keys())[0]
        top_sector_pct = list(sector_dist.values())[0]
        if top_sector_pct > 40:
            sector_gambler = True

    return {
        "cr10": cr10,
        "sector_dist": sector_dist,
        "style": style,
        "style_desc": style_desc,
        "sector_gambler": sector_gambler,
        "top_sector": top_sector,
        "top_sector_pct": top_sector_pct,
        "top10": top10
    }


def calc_risk_metrics(df_nav: pd.DataFrame, perf: dict) -> dict:
    """
    维度三：风险与集中度分析
    """
    nav = df_nav["nav"]
    rets = nav.pct_change().dropna()

    # 波动率
    volatility = rets.std() * np.sqrt(252)

    # 月度胜率
    df2 = df_nav.copy()
    df2["ym"] = df2["date"].dt.to_period("M")
    monthly = df2.groupby("ym")["nav"].apply(lambda x: x.iloc[-1] / x.iloc[0] - 1 if len(x) > 1 else 0)
    win_rate = (monthly > 0).mean()

    # 回撤修复天数
    peak = nav.iloc[0]
    dd_start = None
    max_recovery = 0
    for i, v in enumerate(nav):
        if v >= peak:
            if dd_start is not None:
                max_recovery = max(max_recovery, i - dd_start)
                dd_start = None
            peak = v
        else:
            if dd_start is None:
                dd_start = i

    # 下行风险 (Sortino)
    rf_daily = 0.02 / 252
    down_rets = rets[rets < rf_daily]
    sortino = (rets.mean() - rf_daily) / down_rets.std() * np.sqrt(252) \
              if len(down_rets) > 1 and down_rets.std() > 0 else 0

    return {
        "volatility": volatility,
        "win_rate": win_rate,
        "max_recovery": max_recovery,
        "sortino": sortino
    }


# ===================== 大白话诊断生成 =====================

def generate_personality_diagnosis(perf: dict, style: dict) -> tuple[str, str]:
    """
    性格诊断：一句话形容经理
    返回 (性格标签, 颜色)
    """
    alpha = perf.get("alpha", 0)
    beta = perf.get("beta", 0)
    r2 = perf.get("r2", 0)

    # 高 Alpha + 低 Beta = 独立选股型
    if alpha > 0.05 and beta < 1.0:
        return "🏹 独立狙击手", "steady"

    # 高 Alpha + 高 Beta = 激进进攻型
    if alpha > 0.03 and beta > 1.1:
        return "⚡ 激进进攻手", "aggressive"

    # 低 Alpha + 高 R2 = 跟随大盘型
    if abs(alpha) < 0.02 and r2 > 0.8:
        return "🎭 大盘跟随者", "balanced"

    # 负 Alpha = 能力存疑
    if alpha < -0.02:
        return "⚠️ 能力待验证", "gambler"

    # 赛道赌徒
    if style.get("sector_gambler"):
        return "🎰 赛道押注者", "gambler"

    return "⚖️ 均衡配置者", "balanced"


def generate_ability_diagnosis(perf: dict) -> str:
    """
    实力诊断：解释经理赚的是"运气的钱"还是"本事的钱"
    """
    alpha = perf.get("alpha", 0)
    p_alpha = perf.get("p_alpha", 1)
    info_ratio = perf.get("info_ratio", 0)
    r2 = perf.get("r2", 0)

    lines = []

    # Alpha 显著性
    if p_alpha < 0.05 and alpha > 0.03:
        lines.append(f"✅ **统计显著的超额收益**：Alpha={alpha*100:.1f}%（p<0.05），"
                     f"这不是运气，是经理真有选股本事。")
    elif p_alpha < 0.05 and alpha < 0:
        lines.append(f"⚠️ **显著负超额**：Alpha={alpha*100:.1f}%（p<0.05），"
                     f"经理选股能力明显跑输市场，建议回避。")
    elif alpha > 0.03:
        lines.append(f"⏳ **超额收益待验证**：Alpha={alpha*100:.1f}% 但 p={p_alpha:.2f}，"
                     f"看起来不错，但需要更长时间确认不是运气。")
    else:
        lines.append(f"📊 **无显著超额**：Alpha≈0，经理主要靠大盘吃饭。")

    # 信息比率
    if info_ratio > 0.5:
        lines.append(f"✅ **超额收益稳定**：信息比率={info_ratio:.2f}，Alpha 不是偶然波动带来的。")
    elif info_ratio < 0.3 and alpha > 0:
        lines.append(f"⚠️ **超额收益不稳定**：信息比率={info_ratio:.2f}，Alpha 可能是运气成分。")

    # R2 解释
    if r2 > 0.8:
        lines.append(f"📈 **紧跟大盘**：R²={r2:.2f}，{r2*100:.0f}%收益来自大盘，独立选股贡献有限。")
    elif r2 < 0.5:
        lines.append(f"🎯 **独立性强**：R²={r2:.2f}，与大盘关联度低，经理走自己的路。")

    return "\n\n".join(lines)


def generate_avoidance_guide(perf: dict, style: dict, risk: dict) -> str:
    """
    避坑指南：告诉小白什么样的人千万别买
    """
    warnings = []

    # 高风险警告
    if perf.get("max_dd", 0) < -0.35:
        warnings.append("🚨 **心脏不好别买**：历史最大回撤超35%，跌起来像过山车，"
                        "承受不了-30%浮亏的人请远离。")

    # 赛道赌徒警告
    if style.get("sector_gambler"):
        warnings.append(f"🎰 **不懂行业别买**：单一行业（{style.get('top_sector')}）"
                        f"占比超40%，这是赛道押注型。"
                        f"如果你不懂这个行业，别碰。")

    # 负 Alpha 警告
    if perf.get("alpha", 0) < -0.02 and perf.get("p_alpha", 1) < 0.1:
        warnings.append("⚠️ **求稳别买**：经理选股能力显著为负，长期跑输大盘，"
                        "不如买指数基金。")

    # 高 Beta 警告
    if perf.get("beta", 1) > 1.3:
        warnings.append("📉 **熊市别买**：Beta>1.3，市场跌10%它跌13%，"
                        "如果看空大盘，这只基会让你亏更多。")

    # 修复慢警告
    if risk.get("max_recovery", 0) > 90:
        warnings.append("⏳ **急用钱别买**：历史最长回撤修复超90天，"
                        "跌下去可能要等3个月才能回本，短期资金别进。")

    if not warnings:
        return "✅ **暂无重大风险点**，但请确保你能承受该基金的历史最大回撤。"

    return "\n\n".join(warnings)


# ===================== 可视化 =====================

def fig_cumulative(merged: pd.DataFrame) -> go.Figure:
    df = merged.copy()
    df["fund_cum"] = (1 + df["fund_ret"]).cumprod() - 1
    df["bench_cum"] = (1 + df["bench_ret"]).cumprod() - 1

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["fund_cum"] * 100,
        name="基金", line=dict(color="#dc2626", width=2.5)
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bench_cum"] * 100,
        name="沪深300", line=dict(color="#94a3b8", width=2, dash="dash")
    ))
    fig.update_layout(
        title="累计收益走势对比",
        yaxis_title="累计收益率 (%)",
        height=400,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right")
    )
    return fig


def fig_drawdown(df_nav: pd.DataFrame) -> go.Figure:
    nav = df_nav["nav"]
    dd = (nav - nav.cummax()) / nav.cummax() * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_nav["date"], y=dd,
        fill="tozeroy", name="回撤",
        line=dict(color="#ef4444", width=0.5),
        fillcolor="rgba(239,68,68,0.2)"
    ))
    fig.add_hline(y=-20, line_dash="dash", line_color="#f59e0b",
                  annotation_text="-20%警戒线")
    fig.update_layout(
        title="历史回撤图",
        yaxis_title="回撤 (%)",
        height=300,
        showlegend=False
    )
    return fig


def fig_sector_dist(sector_dist: dict) -> go.Figure:
    if not sector_dist:
        return go.Figure()

    sectors = list(sector_dist.keys())[:10]
    values = [sector_dist[s] for s in sectors]

    fig = px.pie(
        names=sectors, values=values,
        hole=0.4,
        title="行业分布（前10大）"
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(height=400, showlegend=False)
    return fig


def fig_top10_holding(top10: pd.DataFrame) -> go.Figure:
    if top10 is None or top10.empty:
        return go.Figure()

    fig = go.Figure(go.Bar(
        x=top10["weight"],
        y=top10["name"],
        orientation="h",
        marker_color="#3b82f6"
    ))
    fig.update_layout(
        title="前十大重仓股",
        xaxis_title="占净值比例 (%)",
        height=350,
        yaxis=dict(autorange="reversed")
    )
    return fig


# ===================== UI 组件 =====================

def render_metric_card(label: str, value: str, interpret: str, color: str):
    color_map = {
        "green": "#10b981", "blue": "#3b82f6",
        "yellow": "#f59e0b", "red": "#ef4444", "gray": "#6b7280"
    }
    hex_color = color_map.get(color, "#6b7280")

    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-interpret" style="background:{hex_color}20;color:{hex_color};">
            {interpret}
        </div>
    </div>
    """, unsafe_allow_html=True)


def interpret_alpha(alpha: float, p: float) -> tuple[str, str]:
    if p < 0.05 and alpha > 0.05: return "显著正超额，经理有本事", "green"
    if p < 0.05 and alpha < 0: return "显著负超额，建议回避", "red"
    if alpha > 0.03: return "正超额，但需时间验证", "blue"
    if alpha < -0.02: return "负超额，跑输市场", "yellow"
    return "无显著超额，靠大盘吃饭", "gray"


def interpret_beta(beta: float) -> tuple[str, str]:
    if 0.9 <= beta <= 1.1: return "与大盘同步", "blue"
    if beta < 0.9: return "比大盘稳健", "green"
    if beta < 1.3: return "波动大于大盘", "yellow"
    return "高波动，激进型", "red"


def interpret_sharpe(sharpe: float) -> tuple[str, str]:
    if sharpe >= 1.0: return "优秀，风险收益比好", "green"
    if sharpe >= 0.5: return "合格，尚可接受", "blue"
    return "偏低，波动大回报少", "yellow"


def interpret_maxdd(mdd: float) -> tuple[str, str]:
    if mdd >= -0.15: return "回撤控制优秀", "green"
    if mdd >= -0.25: return "回撤适中", "blue"
    if mdd >= -0.35: return "回撤偏大，需谨慎", "yellow"
    return "回撤剧烈，心脏不好别买", "red"


# ===================== 主程序 =====================

def main():
    # Banner
    st.markdown("""
    <div class="banner">
        <h1>🏥 基金全维度体检中心</h1>
        <p>业绩归因 · 持仓穿透 · 风险分析 · 大白话诊断</p>
    </div>
    """, unsafe_allow_html=True)

    # 输入区
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        symbol = st.text_input("基金代码（6位）", value="011040",
                               placeholder="例如 011040", label_visibility="collapsed")
    with col2:
        period = st.selectbox("分析周期", ["近1年", "近3年"], index=1,
                              label_visibility="collapsed")
    with col3:
        analyze = st.button("🔍 开始体检", type="primary", use_container_width=True)

    if not analyze:
        st.markdown("""
        <div style="text-align:center;padding:3rem;color:#94a3b8;">
            <div style="font-size:4rem">🏥</div>
            <p style="font-size:1.2rem;">输入基金代码，开始全维度体检</p>
            <p>示例：011040（中庚价值品质）· 004812（中欧先进制造）</p>
        </div>
        """, unsafe_allow_html=True)
        return

    if len(symbol) != 6 or not symbol.isdigit():
        st.error("请输入6位数字基金代码")
        return

    # 数据获取
    with st.spinner("正在获取基金数据，请稍候..."):
        basic = fetch_fund_basic(symbol)
        df_nav = fetch_fund_nav(symbol, years=3 if period == "近3年" else 1)
        df_bench = fetch_benchmark()
        portfolio = fetch_portfolio(symbol)

    if df_nav is None or df_nav.empty:
        st.error("无法获取净值数据，请确认基金代码")
        return

    # 计算三大维度
    with st.spinner("正在进行量化分析..."):
        perf = calc_performance_attribution(df_nav, df_bench) if df_bench is not None else {"error": "基准数据缺失"}
        style = calc_portfolio_style(portfolio)
        risk = calc_risk_metrics(df_nav, perf)

    fund_name = basic.get("name", symbol)

    # ===================== 头部信息 =====================
    st.subheader(f"📋 {fund_name}")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("最新净值", f"{basic.get('nav', 0):.4f}" if basic.get('nav') else "--")
    with c2:
        g = basic.get("growth", 0)
        st.metric("日增长率", f"{g:.2f}%", delta=f"{g:.2f}%",
                  delta_color="normal" if g >= 0 else "inverse")
    with c3:
        st.metric("申购状态", basic.get("buy", "--"))
    with c4:
        st.metric("手续费", basic.get("fee", "--"))

    st.divider()

    # ===================== 第一部分：大白话体检总结 =====================
    st.markdown('<div class="section-title">📊 大白话体检总结</div>', unsafe_allow_html=True)

    # 性格诊断
    personality, p_color = generate_personality_diagnosis(perf, style)
    st.markdown(f'<span class="personality-tag tag-{p_color}">{personality}</span>', unsafe_allow_html=True)

    # 实力诊断
    ability_text = generate_ability_diagnosis(perf)
    st.markdown(f"""
    <div class="diagnosis-box">
        <div class="diagnosis-title">💪 实力诊断：经理赚的是「运气的钱」还是「本事的钱」？</div>
        <div class="diagnosis-content">{ability_text}</div>
    </div>
    """, unsafe_allow_html=True)

    # 避坑指南
    avoid_text = generate_avoidance_guide(perf, style, risk)
    if "🚨" in avoid_text or "⚠️" in avoid_text:
        box_class = "danger-box"
    elif "🎰" in avoid_text:
        box_class = "warning-box"
    else:
        box_class = "success-box"

    st.markdown(f"""
    <div class="{box_class}">
        <div class="diagnosis-title">⚠️ 避坑指南：什么样的人千万别买这只基？</div>
        <div class="diagnosis-content">{avoid_text}</div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ===================== 第二部分：专业数据看板 =====================
    st.markdown('<div class="section-title">🔬 专业数据看板</div>', unsafe_allow_html=True)

    # 维度一：业绩归因
    st.subheader("维度一：业绩归因 (Alpha/Beta 模型)")

    if "error" not in perf:
        col1, col2, col3 = st.columns(3)
        with col1:
            interp, color = interpret_alpha(perf["alpha"], perf["p_alpha"])
            render_metric_card("Alpha (年化)", f"{perf['alpha']*100:.2f}%", interp, color)
        with col2:
            interp, color = interpret_beta(perf["beta"])
            render_metric_card("Beta", f"{perf['beta']:.2f}", interp, color)
        with col3:
            render_metric_card("R² (拟合度)", f"{perf['r2']:.2f}",
                               f"{perf['r2']*100:.0f}%收益来自大盘", "blue")

        col4, col5, col6 = st.columns(3)
        with col4:
            interp, color = interpret_sharpe(perf["sharpe"])
            render_metric_card("夏普比率", f"{perf['sharpe']:.2f}", interp, color)
        with col5:
            interp, color = interpret_maxdd(perf["max_dd"])
            render_metric_card("最大回撤", f"{perf['max_dd']*100:.2f}%", interp, color)
        with col6:
            render_metric_card("信息比率", f"{perf['info_ratio']:.2f}",
                               "超额收益稳定性" if perf["info_ratio"] > 0.5 else "超额收益不稳定",
                               "green" if perf["info_ratio"] > 0.5 else "yellow")

        # 累计收益图
        st.plotly_chart(fig_cumulative(perf["merged"]), use_container_width=True)
        st.plotly_chart(fig_drawdown(df_nav), use_container_width=True)
    else:
        st.warning(perf["error"])

    # 维度二：持仓风格穿透
    st.subheader("维度二：持仓风格穿透")

    if "error" not in style:
        col1, col2 = st.columns(2)
        with col1:
            cr10 = style["cr10"]
            if cr10 > 60:
                cr_interp, cr_color = "高度集中，押注重仓", "yellow"
            elif cr10 > 40:
                cr_interp, cr_color = "适度集中", "blue"
            else:
                cr_interp, cr_color = "分散配置", "green"
            render_metric_card("CR10 (前十大占比)", f"{cr10:.1f}%", cr_interp, cr_color)

        with col2:
            if style["sector_gambler"]:
                sec_interp, sec_color = f"赛道押注型，{style['top_sector']}占{style['top_sector_pct']:.1f}%", "yellow"
            else:
                sec_interp, sec_color = "行业配置均衡", "green"
            render_metric_card("行业集中度", style.get("top_sector", "未知"), sec_interp, sec_color)

        # 持仓图表
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(fig_top10_holding(style.get("top10")), use_container_width=True)
        with c2:
            st.plotly_chart(fig_sector_dist(style.get("sector_dist")), use_container_width=True)

        # 风格描述
        st.info(f"**持仓风格判定**：{style['style']} — {style['style_desc']}")
    else:
        st.warning(style["error"])

    # 维度三：风险与集中度
    st.subheader("维度三：风险与集中度分析")

    col1, col2, col3 = st.columns(3)
    with col1:
        vol = risk["volatility"]
        if vol < 0.15:
            vol_interp, vol_color = "波动率低，稳健", "green"
        elif vol < 0.25:
            vol_interp, vol_color = "波动适中", "blue"
        else:
            vol_interp, vol_color = "波动较大", "yellow"
        render_metric_card("年化波动率", f"{vol*100:.2f}%", vol_interp, vol_color)

    with col2:
        wr = risk["win_rate"]
        if wr >= 0.7:
            wr_interp, wr_color = "月度胜率高", "green"
        elif wr >= 0.55:
            wr_interp, wr_color = "月度胜率正常", "blue"
        else:
            wr_interp, wr_color = "月度胜率偏低", "yellow"
        render_metric_card("月度胜率", f"{wr*100:.1f}%", wr_interp, wr_color)

    with col3:
        rec = risk["max_recovery"]
        if rec <= 30:
            rec_interp, rec_color = "修复快", "green"
        elif rec <= 60:
            rec_interp, rec_color = "修复速度正常", "blue"
        else:
            rec_interp, rec_color = f"修复慢，最长{rec}天", "yellow"
        render_metric_card("回撤修复", f"{rec} 天", rec_interp, rec_color)

    # 免责声明
    st.divider()
    st.caption("⚠️ 本工具仅提供量化数据分析，不构成投资建议。基金有风险，投资须谨慎。")


if __name__ == "__main__":
    main()
