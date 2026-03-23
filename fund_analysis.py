"""
基金穿透式分析系统 v3.0
核心分析引擎：Alpha/Beta回归 + 三层归因 + 白话诊断报告
数据来源：AkShare
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import statsmodels.api as sm
from datetime import datetime, timedelta
import plotly.graph_objects as go
import warnings
warnings.filterwarnings("ignore")

# ===================== 页面配置 =====================
st.set_page_config(
    page_title="基金透视仪",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    /* 顶部Banner */
    .banner {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 1.5rem 1rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .banner h1 { font-size: 1.8rem; margin: 0; letter-spacing: 2px; }
    .banner p  { font-size: 0.9rem; margin: 0.3rem 0 0; opacity: 0.7; }

    /* 诊断报告卡片 */
    .report-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1.2rem 1.4rem;
        margin: 0.8rem 0;
        border-left: 5px solid #0f3460;
    }
    .report-card.green  { border-left-color: #28a745; background: #f0fff4; }
    .report-card.red    { border-left-color: #dc3545; background: #fff5f5; }
    .report-card.yellow { border-left-color: #ffc107; background: #fffbf0; }
    .report-card.blue   { border-left-color: #17a2b8; background: #f0faff; }

    /* 大结论文字 */
    .verdict { font-size: 1.1rem; font-weight: bold; margin-bottom: 0.4rem; }
    .sub-text { font-size: 0.9rem; opacity: 0.8; line-height: 1.6; }

    /* 场景标签 */
    .scenario-tag {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .tag-gold   { background: #fff3cd; color: #856404; }
    .tag-danger { background: #f8d7da; color: #721c24; }
    .tag-avg    { background: #e2e3e5; color: #383d41; }

    /* 隐藏 Streamlit 默认元素 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ===================== 数据层 =====================

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fund_nav(symbol: str) -> pd.DataFrame | None:
    """获取基金历史净值（3年）"""
    try:
        df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        df.columns = ["date", "nav", "daily_ret_pct"]
        df["date"] = pd.to_datetime(df["date"])
        df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
        df = df.sort_values("date").dropna(subset=["nav"]).reset_index(drop=True)

        # 截取近3年
        cutoff = df["date"].max() - timedelta(days=365 * 3)
        return df[df["date"] >= cutoff].reset_index(drop=True)
    except Exception as e:
        st.error(f"净值数据获取失败：{e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_benchmark() -> pd.DataFrame | None:
    """获取沪深300日线数据"""
    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000300")
        df = df[["date", "close"]].copy()
        df.columns = ["date", "bench"]
        df["date"]  = pd.to_datetime(df["date"])
        df["bench"] = pd.to_numeric(df["bench"], errors="coerce")
        return df.sort_values("date").dropna().reset_index(drop=True)
    except Exception as e:
        st.warning(f"基准数据获取失败（指标计算将跳过对比）：{e}")
        return None


@st.cache_data(ttl=7200, show_spinner=False)
def fetch_portfolio(symbol: str) -> pd.DataFrame | None:
    """获取最新持仓"""
    for year in [datetime.now().year, datetime.now().year - 1]:
        try:
            df = ak.fund_portfolio_hold_em(symbol=symbol, date=str(year))
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_basic_info(symbol: str) -> dict:
    """从日报获取基金基础信息"""
    try:
        daily = ak.fund_open_fund_daily_em()
        row = daily[daily["基金代码"] == symbol]
        if row.empty:
            return {}
        row = row.iloc[0].to_dict()

        # 提取最新净值列
        nav_cols = [c for c in row if "单位净值" in c and "2" in c]
        nav = float(row[nav_cols[0]]) if nav_cols else 0.0
        cum_cols = [c for c in row if "累计净值" in c and "2" in c]
        cum = float(row[cum_cols[0]]) if cum_cols else 0.0

        return {
            "name":    row.get("基金简称", "未知"),
            "nav":     nav,
            "cum_nav": cum,
            "growth":  float(row.get("日增长率", 0)),
            "buy":     row.get("申购状态", "--"),
            "sell":    row.get("赎回状态", "--"),
            "fee":     row.get("手续费", "--"),
        }
    except Exception as e:
        st.warning(f"基础信息获取失败：{e}")
        return {}


# ===================== 分析引擎 =====================

def build_merged(df_nav: pd.DataFrame, df_bench: pd.DataFrame) -> pd.DataFrame:
    """合并基金净值与基准，计算日收益率"""
    df = pd.merge(df_nav[["date", "nav"]], df_bench[["date", "bench"]], on="date", how="inner")
    df["fund_ret"]  = df["nav"].pct_change()
    df["bench_ret"] = df["bench"].pct_change()
    return df.dropna().reset_index(drop=True)


def calc_risk_metrics(df_nav: pd.DataFrame, merged: pd.DataFrame, rf=0.02) -> dict:
    """计算全套风险/收益指标"""
    nav = df_nav["nav"]
    rets = merged["fund_ret"]

    # 年化收益率
    total_days = (df_nav["date"].iloc[-1] - df_nav["date"].iloc[0]).days
    total_ret   = nav.iloc[-1] / nav.iloc[0] - 1
    annual_ret  = (1 + total_ret) ** (365 / max(total_days, 1)) - 1

    # 最大回撤
    cummax = nav.cummax()
    drawdown = (nav - cummax) / cummax
    max_dd = drawdown.min()

    # 回撤修复天数（最长一次）
    max_recovery = _calc_max_recovery(nav)

    # Sharpe（日无风险利率=rf/252）
    daily_rf = rf / 252
    excess   = rets - daily_rf
    sharpe   = excess.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0

    # Sortino（只用下行波动）
    down_rets = rets[rets < daily_rf]
    sortino   = excess.mean() / down_rets.std() * np.sqrt(252) if len(down_rets) > 0 and down_rets.std() > 0 else 0

    # Calmar
    calmar = annual_ret / abs(max_dd) if max_dd != 0 else 0

    # 月度胜率
    df_nav2 = df_nav.copy()
    df_nav2["ym"] = df_nav2["date"].dt.to_period("M")
    monthly = df_nav2.groupby("ym")["nav"].apply(lambda x: x.iloc[-1] / x.iloc[0] - 1)
    win_rate = (monthly > 0).mean()

    return {
        "annual_ret":    annual_ret,
        "total_ret":     total_ret,
        "max_dd":        max_dd,
        "max_recovery":  max_recovery,
        "sharpe":        sharpe,
        "sortino":       sortino,
        "calmar":        calmar,
        "win_rate":      win_rate,
        "volatility":    rets.std() * np.sqrt(252),
    }


def _calc_max_recovery(nav: pd.Series) -> int:
    """计算历史最长回撤修复天数"""
    peak = nav.iloc[0]
    dd_start = None
    max_recovery = 0
    for i, v in nav.items():
        if v >= peak:
            if dd_start is not None:
                days = (nav.index.get_loc(i) - nav.index.get_loc(dd_start))
                max_recovery = max(max_recovery, days)
                dd_start = None
            peak = v
        else:
            if dd_start is None:
                dd_start = i
    return max_recovery


def calc_alpha_beta(merged: pd.DataFrame, rf=0.02) -> dict:
    """OLS回归：计算Alpha、Beta、R²、p值"""
    daily_rf = rf / 252
    y = merged["fund_ret"] - daily_rf
    X = sm.add_constant(merged["bench_ret"] - daily_rf)
    model = sm.OLS(y, X).fit()

    alpha_daily = model.params["const"]
    beta        = model.params["bench_ret"]
    r2          = model.rsquared
    p_alpha     = model.pvalues["const"]
    p_beta      = model.pvalues["bench_ret"]

    # 信息比率
    resid        = model.resid
    tracking_err = resid.std() * np.sqrt(252)
    info_ratio   = (alpha_daily * 252) / tracking_err if tracking_err > 0 else 0

    return {
        "alpha":        alpha_daily * 252,   # 年化
        "beta":         beta,
        "r2":           r2,
        "p_alpha":      p_alpha,
        "p_beta":       p_beta,
        "tracking_err": tracking_err,
        "info_ratio":   info_ratio,
    }


def calc_hhi(portfolio: pd.DataFrame) -> float | None:
    """HHI集中度"""
    for col in ["占净值比例", "持仓占净值比例", "占比"]:
        if col in portfolio.columns:
            w = pd.to_numeric(portfolio[col].astype(str).str.replace("%",""), errors="coerce").dropna() / 100
            return float((w**2).sum()) if len(w) > 0 else None
    return None


# ===================== 诊断文字生成 =====================

def diagnose_alpha_beta(alpha: float, beta: float, r2: float, p_alpha: float) -> tuple[str, str, str, str]:
    """
    返回 (场景标签, 场景类别, 诊断文本, 建议文本)
    场景类别: gold | danger | avg
    """
    alpha_sig = p_alpha < 0.05   # 统计显著

    if alpha > 0.05 and alpha_sig and 0.7 <= beta <= 1.2:
        return (
            "黄金组合 ★",
            "gold",
            f"经理选股能力 **极强**！Alpha {alpha*100:.1f}% 且统计显著，"
            f"Beta={beta:.2f} 说明跟大盘节奏合拍，不是靠加杠杆蒙的。",
            "这种基金是长跑冠军，适合作为**底仓长期持有**。你花的管理费，值！"
        )
    elif alpha < 0 and beta > 1.2:
        return (
            "跟风盘 ⚠",
            "danger",
            f"Alpha 为负（{alpha*100:.1f}%），Beta 高达 {beta:.2f}。"
            f"诊断：经理在牛市靠高Beta赢了大盘，选股能力本身很差。",
            "牛市可短期持有博反弹；一旦震荡或熊市，**亏得会比大盘更惨**。如果不看好大盘，请远离。"
        )
    elif -0.02 < alpha < 0.02 and 0.9 < beta < 1.1 and r2 > 0.75:
        return (
            "平庸之辈",
            "avg",
            f"Alpha约为0（{alpha*100:.1f}%），Beta约为1，R²={r2:.2f}——"
            f"收益 {r2*100:.0f}% 都靠大盘贡献，经理基本无为而治。",
            "花着高管理费买了个廉价指数。**不如去买同类指数ETF**，费率更低，拿到的收益一模一样。"
        )
    elif alpha > 0.02:
        tag_str = "选股能力不错" if alpha_sig else "Alpha 待验证"
        return (
            tag_str,
            "gold" if alpha_sig else "avg",
            f"Alpha {alpha*100:.1f}%（{'统计显著' if alpha_sig else 'p={:.2f}，尚不显著'.format(p_alpha)}），"
            f"Beta={beta:.2f}，R²={r2:.2f}。有一定超额能力，但还需更长时间验证。",
            "可纳入观察名单，**建议继续跟踪 1-2 个季度**后再决定是否重仓。"
        )
    else:
        return (
            "中等水平",
            "avg",
            f"Alpha {alpha*100:.1f}%，Beta {beta:.2f}，R²={r2:.2f}。"
            f"表现中规中矩，没有明显优势也没有明显劣势。",
            "适合作为**辅助仓位**，不建议重仓，若有更好选择可替换。"
        )


def diagnose_drawdown(max_dd: float, recovery: int) -> tuple[str, str]:
    """持有体感诊断"""
    mdd_pct = abs(max_dd) * 100
    if mdd_pct > 35:
        feel   = "像坐过山车，心脏不好别买"
        color  = "red"
    elif mdd_pct > 20:
        feel   = "波动明显，标准的成长股基风险"
        color  = "yellow"
    elif mdd_pct > 10:
        feel   = "标准家用车感，波动在正常公募范围内"
        color  = "blue"
    else:
        feel   = "像开老头乐，非常平稳"
        color  = "green"

    recovery_str = ""
    if recovery > 60:
        recovery_str = f"⚠ 最长修复回撤需 **{recovery}天**，需要耐心。"
    elif recovery > 0:
        recovery_str = f"回撤修复较快，最长 {recovery}天。"

    return (f"**最大回撤 {mdd_pct:.1f}%**，{feel}。{recovery_str}", color)


def score_fund(sharpe: float, alpha: float, max_dd: float, win_rate: float) -> tuple[str, str]:
    """综合评分"""
    score = 0
    score += min(sharpe * 20, 40)           # Sharpe 贡献 0-40分
    score += min(max(alpha * 200, 0), 30)   # Alpha 贡献 0-30分
    score += max(30 - abs(max_dd) * 100, 0) # 回撤扣分
    score += win_rate * 20 - 10             # 胜率贡献

    if score >= 70:
        return "⭐⭐⭐⭐⭐  极优", "green"
    elif score >= 55:
        return "⭐⭐⭐⭐  优秀", "green"
    elif score >= 40:
        return "⭐⭐⭐  合格", "blue"
    elif score >= 25:
        return "⭐⭐  一般，谨慎持有", "yellow"
    else:
        return "⭐  建议回避", "red"


def identify_type(name: str) -> str:
    n = name.lower()
    if any(k in n for k in ["债", "固收", "增强收益"]):   return "fixed"
    if any(k in n for k in ["指数", "etf", "联接"]):       return "index"
    if "货币" in n:                                          return "money"
    return "equity"   # 默认权益


# ===================== 图表层 =====================

def fig_cumulative(merged: pd.DataFrame) -> go.Figure:
    df = merged.copy()
    df["fund_cum"]  = (1 + df["fund_ret"]).cumprod() - 1
    df["bench_cum"] = (1 + df["bench_ret"]).cumprod() - 1

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["fund_cum"]*100,
                             name="基金", line=dict(color="#0f3460", width=2)))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bench_cum"]*100,
                             name="沪深300", line=dict(color="#aaa", width=1.5, dash="dash")))
    fig.update_layout(
        title="累计收益 vs 沪深300",
        yaxis_title="累计收益率 (%)",
        height=320, hovermode="x unified",
        margin=dict(l=0,r=0,t=40,b=0),
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right")
    )
    return fig


def fig_drawdown(df_nav: pd.DataFrame) -> go.Figure:
    nav = df_nav["nav"]
    dd  = (nav - nav.cummax()) / nav.cummax() * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_nav["date"], y=dd,
        fill="tozeroy", name="回撤",
        line=dict(color="#e05252", width=0.5),
        fillcolor="rgba(224,82,82,0.25)"
    ))
    fig.update_layout(
        title="水下回撤图",
        yaxis_title="回撤 (%)",
        height=250, hovermode="x unified",
        margin=dict(l=0,r=0,t=40,b=0)
    )
    return fig


def fig_monthly_heatmap(df_nav: pd.DataFrame) -> go.Figure:
    df = df_nav.copy()
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    mo = df.groupby(["year","month"])["nav"].apply(
        lambda x: (x.iloc[-1]/x.iloc[0]-1)*100 if len(x)>1 else 0
    ).reset_index()
    mo.columns = ["year","month","ret"]
    pivot = mo.pivot(index="year", columns="month", values="ret")
    pivot.columns = [f"{c}月" for c in pivot.columns]

    fig = go.Figure(go.Heatmap(
        z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
        colorscale=[[0,"#00a86b"],[0.5,"#ffffff"],[1,"#d32f2f"]],
        zmid=0,
        text=[[f"{v:.1f}%" if not np.isnan(v) else "" for v in row] for row in pivot.values],
        texttemplate="%{text}", textfont={"size":10}
    ))
    fig.update_layout(title="月度收益热力图（红涨绿跌）",
                      height=350, margin=dict(l=0,r=0,t=40,b=0))
    return fig


def fig_portfolio_pie(portfolio: pd.DataFrame) -> go.Figure | None:
    name_col = next((c for c in ["股票名称","持仓股票名称"] if c in portfolio.columns), None)
    wt_col   = next((c for c in ["占净值比例","持仓占净值比例"] if c in portfolio.columns), None)
    if not name_col or not wt_col:
        return None

    df = portfolio[[name_col, wt_col]].head(10).copy()
    df[wt_col] = pd.to_numeric(df[wt_col].astype(str).str.replace("%",""), errors="coerce")
    df = df.dropna()
    if df.empty:
        return None

    fig = go.Figure(go.Pie(labels=df[name_col], values=df[wt_col],
                           hole=0.35, textinfo="label+percent"))
    fig.update_layout(title="前十大重仓股", height=380,
                      margin=dict(l=0,r=0,t=40,b=0), showlegend=False)
    return fig


def fig_attribution_bar(alpha: float, beta: float) -> go.Figure:
    """收益来源归因柱状图（Beta假设贡献市场年化10%）"""
    beta_contrib  = beta * 0.10 * 100
    alpha_contrib = alpha * 100

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["Beta贡献<br>(市场收益)","Alpha贡献<br>(选股能力)"],
        y=[beta_contrib, alpha_contrib],
        marker_color=["#aabbcc", "#d32f2f" if alpha_contrib > 0 else "#00a86b"],
        text=[f"{beta_contrib:.1f}%", f"{alpha_contrib:.1f}%"],
        textposition="outside"
    ))
    fig.update_layout(
        title="收益归因拆解（假设市场年化10%）",
        yaxis_title="贡献（%）",
        height=280, margin=dict(l=0,r=0,t=40,b=0),
        showlegend=False
    )
    return fig


# ===================== Streamlit 主界面 =====================

def main():
    # ---- Banner ----
    st.markdown("""
    <div class="banner">
        <h1>📊 基金透视仪</h1>
        <p>三层归因 · Alpha/Beta拆解 · 白话诊断报告</p>
    </div>
    """, unsafe_allow_html=True)

    # ---- 输入区 ----
    col_inp1, col_inp2, col_inp3 = st.columns([2, 1, 1])
    with col_inp1:
        symbol = st.text_input("基金代码（6位）", value="004812",
                               placeholder="例如 000001", label_visibility="collapsed")
    with col_inp2:
        period_choice = st.selectbox("分析周期", ["近1年","近3年"], index=1,
                                     label_visibility="collapsed")
    with col_inp3:
        run = st.button("🔍 开始分析", type="primary", use_container_width=True)

    if not run:
        st.markdown("""
        <div style="text-align:center;padding:3rem;color:#aaa;">
            <div style="font-size:3rem">📊</div>
            <p>输入基金代码，点击"开始分析"</p>
            <p style="font-size:0.85rem">支持股票型、债券型、指数型公募基金</p>
        </div>
        """, unsafe_allow_html=True)
        return

    if len(symbol) != 6 or not symbol.isdigit():
        st.error("请输入6位数字基金代码")
        return

    # ---- 数据获取 ----
    with st.spinner("正在获取数据，请稍候..."):
        basic   = fetch_basic_info(symbol)
        df_nav  = fetch_fund_nav(symbol)
        df_bench= fetch_benchmark()
        portfolio = fetch_portfolio(symbol)

    if df_nav is None or df_nav.empty:
        st.error("无法获取净值数据，请确认基金代码是否正确")
        return

    # 截取周期
    if period_choice == "近1年":
        cutoff = df_nav["date"].max() - timedelta(days=365)
        df_nav = df_nav[df_nav["date"] >= cutoff].reset_index(drop=True)

    # 合并 & 计算
    merged   = build_merged(df_nav, df_bench) if df_bench is not None else None
    metrics  = calc_risk_metrics(df_nav, merged if merged is not None else
                                 pd.DataFrame({"fund_ret": df_nav["nav"].pct_change().dropna()}))
    ab       = calc_alpha_beta(merged) if (merged is not None and len(merged) >= 60) else None
    hhi      = calc_hhi(portfolio) if portfolio is not None else None

    fund_name = basic.get("name", symbol)
    fund_type = identify_type(fund_name)

    # ===================== 展示区 =====================

    # ---- 0. 基金头部信息 ----
    st.subheader(f"{'📈' if fund_type=='equity' else '🛡' if fund_type=='fixed' else '🎯'} {fund_name}")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("最新净值", f"{basic.get('nav', '--'):.4f}" if basic.get('nav') else "--")
    with c2:
        growth = basic.get("growth", 0)
        st.metric("日增长率", f"{growth:.2f}%",
                  delta=f"{growth:.2f}%" ,
                  delta_color="normal" if growth >= 0 else "inverse")
    with c3:
        st.metric("申购状态", basic.get("buy", "--"))
    with c4:
        st.metric("手续费", basic.get("fee", "--"))

    st.divider()

    # ---- 1. 一句话 AI 点评 ----
    if ab:
        tag, tag_color, diagnosis, suggestion = diagnose_alpha_beta(
            ab["alpha"], ab["beta"], ab["r2"], ab["p_alpha"]
        )
        tag_cls = f"tag-{tag_color}"
        card_cls = "report-card green" if tag_color=="gold" else \
                   "report-card red" if tag_color=="danger" else "report-card"
        st.markdown(f"""
        <div class="{card_cls}">
            <span class="scenario-tag {tag_cls}">{tag}</span>
            <div class="verdict">📋 AI 投顾一句话点评</div>
            <div class="sub-text">
                {diagnosis}<br><br>
                💡 建议：{suggestion}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("📋 基准数据不足，无法生成 Alpha/Beta 点评（可能是网络问题，请稍后重试）")

    st.divider()

    # ---- 2. 核心指标看板 ----
    st.subheader("📊 核心风险收益指标")

    m = metrics
    r1c1, r1c2 = st.columns(2)
    with r1c1:
        st.metric("年化收益率", f"{m['annual_ret']*100:.2f}%")
    with r1c2:
        st.metric("最大回撤", f"{m['max_dd']*100:.2f}%")

    r2c1, r2c2 = st.columns(2)
    with r2c1:
        st.metric("夏普比率", f"{m['sharpe']:.2f}", help="≥1.0 为优秀，越高越好")
    with r2c2:
        st.metric("卡玛比率", f"{m['calmar']:.2f}", help="年化收益/最大回撤，越高越好")

    r3c1, r3c2 = st.columns(2)
    with r3c1:
        st.metric("Sortino 比率", f"{m['sortino']:.2f}", help="只看下行风险，比夏普更严格")
    with r3c2:
        st.metric("月度胜率", f"{m['win_rate']*100:.1f}%", help="正收益月份占比")

    # 持有体感
    dd_text, dd_color = diagnose_drawdown(m["max_dd"], m["max_recovery"])
    card_cls = f"report-card {dd_color}"
    st.markdown(f"""
    <div class="{card_cls}">
        <div class="verdict">🎢 持有体感预测</div>
        <div class="sub-text">{dd_text}</div>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("🤔 这些指标怎么看？"):
        st.markdown("""
        | 指标 | 白话解释 | 参考标准 |
        |------|---------|---------|
        | **夏普比率** | 每承担1份波动，赚了多少超额收益 | ≥1.0 优秀，≥0.5 合格 |
        | **卡玛比率** | 每亏1%的历史最大回撤，带来了多少年化收益 | ≥1.0 优秀 |
        | **Sortino** | 和夏普类似，但只惩罚下行风险，对固收类更公平 | ≥1.5 优秀 |
        | **月度胜率** | 一年12个月里，有多少个月是正收益 | ≥60% 合格，≥75% 优秀 |
        | **最大回撤** | 从历史最高点到最低点，最多亏了多少 | 股基<35% 可接受 |
        """)

    st.divider()

    # ---- 3. Alpha/Beta 深度拆解 ----
    if ab:
        st.subheader("🔬 Alpha/Beta 因子拆解")

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Alpha（年化）",
                      f"{ab['alpha']*100:.2f}%",
                      help="扣除市场贡献后，经理纯靠选股赚的钱")
        with c2:
            st.metric("Beta",
                      f"{ab['beta']:.2f}",
                      help="大盘涨1%，这只基金涨多少%")
        with c3:
            st.metric("R²（拟合度）",
                      f"{ab['r2']:.2f}",
                      help="收益中有多少比例可以被大盘解释")

        c4, c5 = st.columns(2)
        with c4:
            st.metric("信息比率",
                      f"{ab['info_ratio']:.2f}",
                      help="Alpha / 跟踪误差，衡量超额收益的稳定性")
        with c5:
            sig = "✅ 统计显著" if ab["p_alpha"] < 0.05 else f"⚠ 不显著 (p={ab['p_alpha']:.2f})"
            st.metric("Alpha显著性", sig)

        with st.expander("🤔 Alpha / Beta / R² 怎么理解？"):
            st.markdown("""
            把收益拆成三层：

            **第一层 — Beta（市场收益）**：大盘涨了，大家都涨。
            - Beta=1.0：跟大盘同步
            - Beta=1.3：大盘涨10%，它涨13%（也亏得多）
            - Beta=0.6：跟大盘弱相关

            **第二层 — Alpha（纯选股收益）**：扣掉大盘那份之后，经理自己贡献的部分。
            - Alpha>5% 且显著 → 经理有真本事
            - Alpha≈0 → 经理纯靠大盘吃饭
            - Alpha<0 → 选股能力弱，跑输大盘

            **第三层 — R²（可解释度）**：大盘能解释多少收益
            - R²=0.9 → 90% 的收益来自大盘，"紧跟大盘型"
            - R²=0.3 → 30% 来自大盘，"特立独行型"，风险更独立
            """)

        # 归因柱图
        st.plotly_chart(fig_attribution_bar(ab["alpha"], ab["beta"]), use_container_width=True)

    st.divider()

    # ---- 4. 综合评分 ----
    score_text, score_color = score_fund(
        m["sharpe"], ab["alpha"] if ab else 0, m["max_dd"], m["win_rate"]
    )
    card_cls = f"report-card {score_color}"
    st.markdown(f"""
    <div class="{card_cls}">
        <div class="verdict">🏆 量化综合评分</div>
        <div class="sub-text" style="font-size:1.2rem;">{score_text}</div>
        <div class="sub-text">综合考量：夏普比率 · Alpha能力 · 最大回撤 · 月度胜率</div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ---- 5. 图表区（可折叠） ----
    with st.expander("📈 展开图表详情", expanded=False):

        # 累计收益
        if merged is not None and not merged.empty:
            st.plotly_chart(fig_cumulative(merged), use_container_width=True)

        # 水下回撤
        st.plotly_chart(fig_drawdown(df_nav), use_container_width=True)

        # 月度热力图
        if len(df_nav) > 60:
            st.plotly_chart(fig_monthly_heatmap(df_nav), use_container_width=True)

    # ---- 6. 持仓分析（可折叠） ----
    if portfolio is not None and not portfolio.empty:
        with st.expander("🗂 展开持仓分析", expanded=False):
            # HHI
            if hhi is not None:
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("HHI 集中度", f"{hhi:.4f}",
                              help="HHI<0.05 高度分散 | 0.05-0.18 适中 | >0.18 高度集中")
                with c2:
                    if hhi < 0.05:
                        st.info("持仓高度分散")
                    elif hhi < 0.18:
                        st.info("持仓适度集中")
                    else:
                        st.warning("持仓高度集中，前几大重仓主导净值走势")

            # 持仓饼图
            pie = fig_portfolio_pie(portfolio)
            if pie:
                st.plotly_chart(pie, use_container_width=True)

            # 原始持仓表格
            st.caption("👆 左右滑动查看完整数据")
            st.dataframe(portfolio.head(15), use_container_width=True)

    # ---- 免责声明 ----
    st.divider()
    st.caption("⚠ 本工具仅提供量化数据分析，不构成投资建议。基金有风险，投资须谨慎。历史业绩不代表未来表现。")


if __name__ == "__main__":
    main()
