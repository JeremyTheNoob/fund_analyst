"""
基金穿透式分析系统 v4.0
全自动类型识别 → 三套独立分析引擎
- 权益类：Alpha/Beta 归因 + HHI + 信息比率
- 债券类：Sortino + 回撤修复天数 + 月度胜率
- 指数ETF：跟踪误差 + 相关系数 + 费率对比
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

    .type-badge {
        display: inline-block;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: bold;
        margin-bottom: 0.6rem;
    }
    .badge-equity { background: #ffecd2; color: #c05621; }
    .badge-fixed  { background: #e6f4ea; color: #276221; }
    .badge-index  { background: #e8f0fe; color: #1a56db; }
    .badge-money  { background: #f3e8ff; color: #6b21a8; }
    .badge-unknown{ background: #f0f0f0; color: #555; }

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

    .verdict  { font-size: 1.1rem; font-weight: bold; margin-bottom: 0.4rem; }
    .sub-text { font-size: 0.9rem; opacity: 0.85; line-height: 1.6; }

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
    .tag-blue   { background: #cce5ff; color: #004085; }

    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ===================== 数据层 =====================

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_fund_type(symbol: str) -> dict:
    """
    用 fund_name_em() 精确获取基金类型
    返回 {'raw_type': '股票型', 'category': 'equity', 'name': '中欧先进制造股票A'}
    """
    try:
        df = ak.fund_name_em()
        row = df[df["基金代码"] == symbol]
        if row.empty:
            return {"raw_type": "未知", "category": "unknown", "name": symbol}
        r = row.iloc[0]
        raw_type = r["基金类型"]
        name     = r["基金简称"]
        category = _map_category(raw_type)
        return {"raw_type": raw_type, "category": category, "name": name}
    except Exception as e:
        return {"raw_type": "未知", "category": "unknown", "name": symbol}


def _map_category(raw_type: str) -> str:
    """
    将 AkShare 的细分类型映射到四大类
    股票型 / 混合型偏股 → equity
    债券型 / 混合型偏债 / QDII债 → fixed
    指数型 → index
    货币型 → money
    """
    t = raw_type.lower()
    if "货币" in t:
        return "money"
    if "指数" in t:
        return "index"
    # 债券类（含固收+）
    if "债" in t:
        return "fixed"
    # 混合型-灵活 / 混合型-偏债 归 fixed，偏股/平衡 归 equity
    if "混合" in t:
        if "偏债" in t or "固收" in t:
            return "fixed"
        return "equity"   # 偏股/灵活/平衡 默认权益
    if "股票" in t or "权益" in t:
        return "equity"
    return "unknown"


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fund_nav(symbol: str, years: int = 3) -> pd.DataFrame | None:
    """获取基金历史净值"""
    try:
        df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        df.columns = ["date", "nav", "daily_ret_pct"]
        df["date"] = pd.to_datetime(df["date"])
        df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
        df = df.sort_values("date").dropna(subset=["nav"]).reset_index(drop=True)
        cutoff = df["date"].max() - timedelta(days=365 * years)
        return df[df["date"] >= cutoff].reset_index(drop=True)
    except Exception as e:
        st.error(f"净值数据获取失败：{e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_benchmark(symbol: str = "sh000300") -> pd.DataFrame | None:
    """获取沪深300日线数据，多接口容错"""
    # 接口1：stock_zh_index_daily（新浪，稳定）
    try:
        df = ak.stock_zh_index_daily(symbol=symbol)
        df = df[["date", "close"]].copy()
        df.columns = ["date", "bench"]
        df["date"]  = pd.to_datetime(df["date"])
        df["bench"] = pd.to_numeric(df["bench"], errors="coerce")
        return df.sort_values("date").dropna().reset_index(drop=True)
    except Exception:
        pass

    # 接口2：stock_zh_index_daily_em（东财，备用）
    try:
        df = ak.stock_zh_index_daily_em(symbol=symbol)
        df = df[["date", "close"]].copy()
        df.columns = ["date", "bench"]
        df["date"]  = pd.to_datetime(df["date"])
        df["bench"] = pd.to_numeric(df["bench"], errors="coerce")
        return df.sort_values("date").dropna().reset_index(drop=True)
    except Exception:
        pass

    # 接口3：index_zh_a_hist（备用）
    try:
        code = symbol.replace("sh", "").replace("sz", "")
        df = ak.index_zh_a_hist(symbol=code, period="daily",
                                start_date="20200101",
                                end_date=datetime.now().strftime("%Y%m%d"))
        df = df[["日期", "收盘"]].copy()
        df.columns = ["date", "bench"]
        df["date"]  = pd.to_datetime(df["date"])
        df["bench"] = pd.to_numeric(df["bench"], errors="coerce")
        return df.sort_values("date").dropna().reset_index(drop=True)
    except Exception:
        pass

    st.warning("⚠ 基准数据（沪深300）获取失败，Alpha/Beta 相关指标将跳过。")
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
            "nav":    nav,
            "growth": growth,
            "buy":    r.get("申购状态", "--"),
            "sell":   r.get("赎回状态", "--"),
            "fee":    r.get("手续费", "--"),
        }
    except Exception:
        return {}


# ===================== 通用计算工具 =====================

def build_merged(df_nav: pd.DataFrame, df_bench: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(df_nav[["date", "nav"]], df_bench[["date", "bench"]], on="date", how="inner")
    df["fund_ret"]  = df["nav"].pct_change()
    df["bench_ret"] = df["bench"].pct_change()
    return df.dropna().reset_index(drop=True)


def calc_base_metrics(df_nav: pd.DataFrame, rets: pd.Series, rf: float = 0.02) -> dict:
    """所有类型通用的基础指标"""
    nav = df_nav["nav"]
    total_days = max((df_nav["date"].iloc[-1] - df_nav["date"].iloc[0]).days, 1)
    total_ret  = nav.iloc[-1] / nav.iloc[0] - 1
    annual_ret = (1 + total_ret) ** (365 / total_days) - 1

    cummax  = nav.cummax()
    dd      = (nav - cummax) / cummax
    max_dd  = dd.min()
    max_rec = _calc_max_recovery(nav)

    daily_rf = rf / 252
    excess   = rets - daily_rf
    vol      = rets.std() * np.sqrt(252)
    sharpe   = excess.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0

    down_rets = rets[rets < daily_rf]
    sortino   = excess.mean() / down_rets.std() * np.sqrt(252) \
                if len(down_rets) > 1 and down_rets.std() > 0 else 0

    calmar = annual_ret / abs(max_dd) if max_dd != 0 else 0

    df2 = df_nav.copy()
    df2["ym"] = df2["date"].dt.to_period("M")
    monthly  = df2.groupby("ym")["nav"].apply(
        lambda x: x.iloc[-1] / x.iloc[0] - 1 if len(x) > 1 else 0
    )
    win_rate = (monthly > 0).mean()

    return {
        "annual_ret": annual_ret,
        "total_ret":  total_ret,
        "max_dd":     max_dd,
        "max_rec":    max_rec,
        "sharpe":     sharpe,
        "sortino":    sortino,
        "calmar":     calmar,
        "win_rate":   win_rate,
        "volatility": vol,
    }


def _calc_max_recovery(nav: pd.Series) -> int:
    """历史最长回撤修复天数"""
    peak = nav.iloc[0]
    dd_start_idx = None
    max_rec = 0
    arr = nav.reset_index(drop=True)
    for i, v in enumerate(arr):
        if v >= peak:
            if dd_start_idx is not None:
                max_rec = max(max_rec, i - dd_start_idx)
                dd_start_idx = None
            peak = v
        else:
            if dd_start_idx is None:
                dd_start_idx = i
    return max_rec


def calc_alpha_beta(merged: pd.DataFrame, rf: float = 0.02) -> dict:
    """OLS: Alpha / Beta / R² / 信息比率"""
    daily_rf = rf / 252
    y = merged["fund_ret"] - daily_rf
    X = sm.add_constant(merged["bench_ret"] - daily_rf)
    model = sm.OLS(y, X).fit()

    alpha_d = model.params["const"]
    beta    = model.params["bench_ret"]
    r2      = model.rsquared
    p_alpha = model.pvalues["const"]

    resid        = model.resid
    tracking_err = resid.std() * np.sqrt(252)
    info_ratio   = (alpha_d * 252) / tracking_err if tracking_err > 0 else 0

    return {
        "alpha":        alpha_d * 252,
        "beta":         beta,
        "r2":           r2,
        "p_alpha":      p_alpha,
        "tracking_err": tracking_err,
        "info_ratio":   info_ratio,
    }


def calc_index_tracking(merged: pd.DataFrame) -> dict:
    """指数基金专用：跟踪误差 / 相关系数"""
    corr = merged["fund_ret"].corr(merged["bench_ret"])
    diff = merged["fund_ret"] - merged["bench_ret"]
    te   = diff.std() * np.sqrt(252)   # 年化跟踪误差
    return {"corr": corr, "tracking_error": te}


def calc_hhi(portfolio: pd.DataFrame) -> float | None:
    for col in ["占净值比例", "持仓占净值比例", "占比"]:
        if col in portfolio.columns:
            w = pd.to_numeric(
                portfolio[col].astype(str).str.replace("%", ""), errors="coerce"
            ).dropna() / 100
            return float((w ** 2).sum()) if len(w) > 0 else None
    return None


# ===================== 诊断文字生成 =====================

def diagnose_equity(ab: dict) -> tuple[str, str, str, str]:
    """权益类场景判断，返回(场景标签, 颜色, 诊断, 建议)"""
    alpha, beta, r2, p = ab["alpha"], ab["beta"], ab["r2"], ab["p_alpha"]
    sig = p < 0.05

    if alpha > 0.05 and sig and 0.7 <= beta <= 1.2:
        return ("黄金组合 ★", "gold",
                f"Alpha {alpha*100:.1f}%（统计显著），Beta={beta:.2f}，选股能力极强，不是靠加杠杆蒙的。",
                "这种基金是长跑冠军，适合作为**底仓长期持有**。管理费花得值！")
    elif alpha < 0 and beta > 1.2:
        return ("跟风盘 ⚠", "danger",
                f"Alpha 为负（{alpha*100:.1f}%），Beta 高达 {beta:.2f}。"
                "牛市靠加杠杆赢了大盘，但选股能力本身很弱。",
                "牛市可短期持有；**一旦震荡或熊市，亏得比大盘更惨**。")
    elif -0.02 < alpha < 0.02 and 0.9 < beta < 1.1 and r2 > 0.75:
        return ("平庸之辈", "avg",
                f"Alpha≈0（{alpha*100:.1f}%），Beta≈1，R²={r2:.2f}。"
                f"收益 {r2*100:.0f}% 都靠大盘贡献，经理基本无为而治。",
                "花着高管理费买了个廉价指数。**不如换同类ETF**，费率更低，收益一样。")
    elif alpha > 0.02:
        tag = "选股能力不错" if sig else "Alpha 待验证"
        color = "gold" if sig else "avg"
        return (tag, color,
                f"Alpha {alpha*100:.1f}%（{'显著' if sig else f'p={p:.2f}，尚不显著'}），"
                f"Beta={beta:.2f}，有一定超额能力。",
                "可纳入观察名单，**建议继续跟踪 1-2 季度**后再决定是否重仓。")
    else:
        return ("中等水平", "avg",
                f"Alpha {alpha*100:.1f}%，Beta {beta:.2f}，R²={r2:.2f}。表现中规中矩。",
                "适合作为**辅助仓位**，若有更好选择可考虑替换。")


def diagnose_fixed(metrics: dict) -> tuple[str, str]:
    """固收类专项诊断，返回(诊断文本, 颜色)"""
    sortino = metrics["sortino"]
    rec     = metrics["max_rec"]
    win     = metrics["win_rate"]
    mdd     = abs(metrics["max_dd"]) * 100

    lines = []
    color = "green"

    # Sortino
    if sortino >= 2.0:
        lines.append(f"Sortino={sortino:.2f}——下行风险控制**极优**，真正的资产避风港。")
    elif sortino >= 1.0:
        lines.append(f"Sortino={sortino:.2f}——下行风险控制良好，持有体感稳健。")
    else:
        lines.append(f"Sortino={sortino:.2f}——下行风险控制偏弱，债基中的跌将。")
        color = "yellow"

    # 回撤修复
    if rec > 60:
        lines.append(f"⚠ 最长回撤修复需 **{rec}天**（超 60 天警戒线），资金流动性较差。")
        color = "red" if color != "red" else "red"
    elif rec > 30:
        lines.append(f"最长回撤修复 {rec} 天，需要一定耐心。")
        color = "yellow" if color == "green" else color
    else:
        lines.append(f"回撤修复较快，最长 {rec} 天，流动性良好。")

    # 月度胜率
    if win >= 0.75:
        lines.append(f"月度胜率 {win*100:.0f}%——**75%+ 的月份是正收益**，持有体感非常舒适。")
    elif win >= 0.60:
        lines.append(f"月度胜率 {win*100:.0f}%，合格。")
    else:
        lines.append(f"月度胜率仅 {win*100:.0f}%，**亏损月份偏多**，持有体感较差。")
        color = "yellow" if color == "green" else color

    return ("  ".join(lines), color)


def diagnose_index(index_metrics: dict, ab: dict | None, fee: str) -> tuple[str, str]:
    """指数ETF专项诊断，返回(诊断文本, 颜色)"""
    corr = index_metrics["corr"]
    te   = index_metrics["tracking_error"]
    lines = []
    color = "green"

    # 相关系数
    if corr >= 0.98:
        lines.append(f"与基准相关系数 {corr:.4f}——**跟踪精度极高**，名副其实的指数基金。")
    elif corr >= 0.95:
        lines.append(f"与基准相关系数 {corr:.4f}——跟踪精度良好。")
    else:
        lines.append(f"⚠ 与基准相关系数仅 {corr:.4f}——**跟踪偏差较大**，选择时需谨慎。")
        color = "yellow"

    # 跟踪误差
    if te <= 0.02:
        lines.append(f"年化跟踪误差 {te*100:.2f}%——**非常精准**，买到的就是指数。")
    elif te <= 0.05:
        lines.append(f"年化跟踪误差 {te*100:.2f}%——在可接受范围内。")
    else:
        lines.append(f"⚠ 年化跟踪误差 {te*100:.2f}%——**误差偏大**，这只指数基金买得不够准。")
        color = "yellow" if color == "green" else color

    # 费率
    try:
        fee_pct = float(fee.replace("%", ""))
        if fee_pct <= 0.1:
            lines.append(f"管理费率 {fee}——**非常低廉**，是优质低费率产品。")
        elif fee_pct <= 0.5:
            lines.append(f"管理费率 {fee}，处于正常水平。")
        else:
            lines.append(f"⚠ 管理费率 {fee}——**偏高**，建议与同类费率更低产品对比。")
            color = "yellow" if color == "green" else color
    except Exception:
        pass

    return ("  ".join(lines), color)


def score_fund_equity(sharpe: float, alpha: float, max_dd: float, win_rate: float) -> tuple[str, str]:
    score = 0
    score += min(sharpe * 20, 40)
    score += min(max(alpha * 200, 0), 30)
    score += max(30 - abs(max_dd) * 100, 0)
    score += win_rate * 20 - 10
    if score >= 70: return "⭐⭐⭐⭐⭐  极优", "green"
    if score >= 55: return "⭐⭐⭐⭐  优秀", "green"
    if score >= 40: return "⭐⭐⭐  合格", "blue"
    if score >= 25: return "⭐⭐  一般，谨慎持有", "yellow"
    return "⭐  建议回避", "red"


def score_fund_fixed(sortino: float, max_dd: float, win_rate: float, max_rec: int) -> tuple[str, str]:
    score = 0
    score += min(sortino * 20, 40)
    score += max(40 - abs(max_dd) * 200, 0)
    score += win_rate * 20 - 10
    score -= max(max_rec - 30, 0) * 0.2   # 修复天数超30惩罚
    if score >= 70: return "⭐⭐⭐⭐⭐  极优稳健", "green"
    if score >= 55: return "⭐⭐⭐⭐  优质债基", "green"
    if score >= 40: return "⭐⭐⭐  合格", "blue"
    if score >= 25: return "⭐⭐  一般，可关注替代品", "yellow"
    return "⭐  建议回避", "red"


def score_fund_index(corr: float, te: float, fee: str) -> tuple[str, str]:
    score = 0
    score += corr * 50
    score += max(30 - te * 200, 0)
    try:
        fee_pct = float(fee.replace("%", ""))
        score += max(20 - fee_pct * 20, 0)
    except Exception:
        pass
    if score >= 70: return "⭐⭐⭐⭐⭐  极优低费精准", "green"
    if score >= 55: return "⭐⭐⭐⭐  优秀", "green"
    if score >= 40: return "⭐⭐⭐  合格", "blue"
    return "⭐⭐  跟踪精度或费率存在问题", "yellow"


# ===================== 图表层 =====================

def fig_cumulative(merged: pd.DataFrame, bench_label: str = "沪深300") -> go.Figure:
    df = merged.copy()
    df["fund_cum"]  = (1 + df["fund_ret"]).cumprod() - 1
    df["bench_cum"] = (1 + df["bench_ret"]).cumprod() - 1
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["fund_cum"]*100,
                             name="基金", line=dict(color="#d32f2f", width=2)))
    fig.add_trace(go.Scatter(x=df["date"], y=df["bench_cum"]*100,
                             name=bench_label, line=dict(color="#aaa", width=1.5, dash="dash")))
    fig.update_layout(
        title=f"累计收益 vs {bench_label}",
        yaxis_title="累计收益率 (%)",
        height=320, hovermode="x unified",
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right")
    )
    return fig


def fig_drawdown(df_nav: pd.DataFrame) -> go.Figure:
    nav = df_nav["nav"]
    dd  = (nav - nav.cummax()) / nav.cummax() * 100
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_nav["date"], y=dd, fill="tozeroy", name="回撤",
        line=dict(color="#e05252", width=0.5),
        fillcolor="rgba(224,82,82,0.25)"
    ))
    fig.update_layout(title="水下回撤图", yaxis_title="回撤 (%)",
                      height=250, hovermode="x unified",
                      margin=dict(l=0, r=0, t=40, b=0))
    return fig


def fig_monthly_heatmap(df_nav: pd.DataFrame) -> go.Figure:
    df = df_nav.copy()
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    mo = df.groupby(["year", "month"])["nav"].apply(
        lambda x: (x.iloc[-1]/x.iloc[0]-1)*100 if len(x) > 1 else 0
    ).reset_index()
    mo.columns = ["year", "month", "ret"]
    pivot = mo.pivot(index="year", columns="month", values="ret")
    pivot.columns = [f"{c}月" for c in pivot.columns]
    fig = go.Figure(go.Heatmap(
        z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
        colorscale=[[0, "#00a86b"], [0.5, "#ffffff"], [1, "#d32f2f"]],
        zmid=0,
        text=[[f"{v:.1f}%" if not np.isnan(v) else "" for v in row] for row in pivot.values],
        texttemplate="%{text}", textfont={"size": 10}
    ))
    fig.update_layout(title="月度收益热力图（红涨绿跌）",
                      height=350, margin=dict(l=0, r=0, t=40, b=0))
    return fig


def fig_portfolio_pie(portfolio: pd.DataFrame) -> go.Figure | None:
    name_col = next((c for c in ["股票名称", "持仓股票名称"] if c in portfolio.columns), None)
    wt_col   = next((c for c in ["占净值比例", "持仓占净值比例"] if c in portfolio.columns), None)
    if not name_col or not wt_col:
        return None
    df = portfolio[[name_col, wt_col]].head(10).copy()
    df[wt_col] = pd.to_numeric(df[wt_col].astype(str).str.replace("%", ""), errors="coerce")
    df = df.dropna()
    if df.empty:
        return None
    fig = go.Figure(go.Pie(labels=df[name_col], values=df[wt_col],
                           hole=0.35, textinfo="label+percent"))
    fig.update_layout(title="前十大重仓股", height=380,
                      margin=dict(l=0, r=0, t=40, b=0), showlegend=False)
    return fig


def fig_attribution_bar(alpha: float, beta: float) -> go.Figure:
    beta_c  = beta * 0.10 * 100
    alpha_c = alpha * 100
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["Beta 贡献\n(市场收益)", "Alpha 贡献\n(选股能力)"],
        y=[beta_c, alpha_c],
        marker_color=["#aabbcc", "#d32f2f" if alpha_c > 0 else "#00a86b"],
        text=[f"{beta_c:.1f}%", f"{alpha_c:.1f}%"],
        textposition="outside"
    ))
    fig.update_layout(
        title="收益归因拆解（假设市场年化10%）",
        yaxis_title="贡献（%）",
        height=280, margin=dict(l=0, r=0, t=40, b=0), showlegend=False
    )
    return fig


def fig_tracking_bar(fund_ret: pd.Series, bench_ret: pd.Series) -> go.Figure:
    """指数ETF：跟踪偏差分布图"""
    diff = (fund_ret - bench_ret) * 100
    fig = go.Figure(go.Histogram(
        x=diff, nbinsx=40,
        marker_color="#1a56db", opacity=0.75, name="跟踪偏差"
    ))
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    fig.update_layout(
        title="日跟踪偏差分布（越集中在0附近越精准）",
        xaxis_title="日跟踪偏差 (%)", yaxis_title="频次",
        height=280, margin=dict(l=0, r=0, t=40, b=0)
    )
    return fig


# ===================== 公共展示块 =====================

def _show_type_badge(category: str, raw_type: str):
    badge_map = {
        "equity":  ("badge-equity", "权益类"),
        "fixed":   ("badge-fixed",  "债券/固收类"),
        "index":   ("badge-index",  "指数/ETF"),
        "money":   ("badge-money",  "货币型"),
        "unknown": ("badge-unknown","未知类型"),
    }
    cls, label = badge_map.get(category, ("badge-unknown", "未知"))
    st.markdown(
        f'<span class="type-badge {cls}">🏷 {label} · {raw_type}</span>',
        unsafe_allow_html=True
    )


def _show_base_header(basic: dict, type_info: dict):
    name     = type_info["name"]
    category = type_info["category"]
    icon_map = {"equity": "📈", "fixed": "🛡", "index": "🎯", "money": "💰", "unknown": "❓"}
    st.subheader(f"{icon_map.get(category,'📊')} {name}")
    _show_type_badge(category, type_info["raw_type"])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        nav = basic.get("nav", 0)
        st.metric("最新净值", f"{nav:.4f}" if nav else "--")
    with c2:
        g = basic.get("growth", 0)
        st.metric("日增长率", f"{g:.2f}%",
                  delta=f"{g:.2f}%", delta_color="normal" if g >= 0 else "inverse")
    with c3:
        st.metric("申购状态", basic.get("buy", "--"))
    with c4:
        st.metric("手续费", basic.get("fee", "--"))
    st.divider()


def _interpret_sharpe(val: float) -> tuple[str, str]:
    """返回(解读文字, 颜色)"""
    if val >= 1.0: return "优秀，每承担1份波动能赚到超额收益", "green"
    if val >= 0.5: return "合格，风险收益比尚可", "blue"
    return "偏低，承担波动但回报不足", "yellow"


def _interpret_sortino(val: float) -> tuple[str, str]:
    if val >= 1.5: return "优秀，下行风险控制极好", "green"
    if val >= 1.0: return "良好，持有体感稳健", "blue"
    return "偏弱，下跌时波动较大", "yellow"


def _interpret_calmar(val: float) -> tuple[str, str]:
    if val >= 1.0: return "优秀，回撤补偿充足", "green"
    if val >= 0.5: return "合格，回撤补偿尚可", "blue"
    return "偏低，回撤大但收益跟不上", "yellow"


def _interpret_maxdd(val: float) -> tuple[str, str]:
    """val 是原始小数，如 -0.25"""
    pct = abs(val) * 100
    if pct <= 10: return "非常平稳，像开老头乐", "green"
    if pct <= 20: return "波动正常，标准家用车感", "blue"
    if pct <= 35: return "波动明显，成长股基风险", "yellow"
    return "像坐过山车，心脏不好别买", "red"


def _interpret_winrate(val: float) -> tuple[str, str]:
    if val >= 0.75: return "优秀，75%+月份赚钱", "green"
    if val >= 0.60: return "合格，六成月份赚钱", "blue"
    return "偏低，亏损月份偏多", "yellow"


def _interpret_recovery(days: int) -> tuple[str, str]:
    if days <= 30: return "修复及时，流动性好", "green"
    if days <= 60: return "需要一定耐心", "blue"
    return f"⚠ 超60天警戒线，持有体感差", "red"


def _metric_with_interpret(label: str, value: str, interp: str, color: str):
    """带实时解读的指标卡片"""
    color_map = {"green": "#28a745", "blue": "#17a2b8", "yellow": "#ffc107", "red": "#dc3545"}
    hex_color = color_map.get(color, "#666")
    st.markdown(f"""
    <div style="background:#f8f9fa;border-radius:8px;padding:12px;margin:4px 0;">
        <div style="font-size:0.85rem;color:#666;">{label}</div>
        <div style="font-size:1.4rem;font-weight:bold;color:#333;">{value}</div>
        <div style="font-size:0.8rem;color:{hex_color};margin-top:4px;">💡 {interp}</div>
    </div>
    """, unsafe_allow_html=True)


def _show_base_risk_panel(m: dict, show_sortino_primary: bool = False):
    """通用风险指标面板（带实时数据解读）"""
    # 第一行：收益 + 回撤
    r1, r2 = st.columns(2)
    with r1:
        _metric_with_interpret(
            "年化收益率", f"{m['annual_ret']*100:.2f}%",
            "过去一年的收益表现", "blue"
        )
    with r2:
        interp, color = _interpret_maxdd(m['max_dd'])
        _metric_with_interpret(
            "最大回撤", f"{m['max_dd']*100:.2f}%",
            interp, color
        )

    # 第二行：根据类型显示不同指标
    r3, r4 = st.columns(2)
    if show_sortino_primary:
        with r3:
            interp, color = _interpret_sortino(m['sortino'])
            _metric_with_interpret("Sortino 比率", f"{m['sortino']:.2f}", interp, color)
        with r4:
            interp, color = _interpret_winrate(m['win_rate'])
            _metric_with_interpret("月度胜率", f"{m['win_rate']*100:.1f}%", interp, color)
    else:
        with r3:
            interp, color = _interpret_sharpe(m['sharpe'])
            _metric_with_interpret("夏普比率", f"{m['sharpe']:.2f}", interp, color)
        with r4:
            interp, color = _interpret_calmar(m['calmar'])
            _metric_with_interpret("卡玛比率", f"{m['calmar']:.2f}", interp, color)


def _card(text: str, color: str):
    st.markdown(
        f'<div class="report-card {color}"><div class="sub-text">{text}</div></div>',
        unsafe_allow_html=True
    )


def _verdict_card(title: str, body: str, color: str):
    st.markdown(f"""
    <div class="report-card {color}">
        <div class="verdict">{title}</div>
        <div class="sub-text">{body}</div>
    </div>
    """, unsafe_allow_html=True)


# ===================== 三大分析模块 =====================

def module_equity(df_nav, merged, portfolio, basic, type_info):
    """权益类专项分析模块"""
    st.subheader("📊 核心风险收益指标")

    rets = merged["fund_ret"] if merged is not None else df_nav["nav"].pct_change().dropna()
    m = calc_base_metrics(df_nav, rets)
    ab = calc_alpha_beta(merged) if (merged is not None and len(merged) >= 60) else None
    hhi = calc_hhi(portfolio) if portfolio is not None else None

    # 核心指标（带实时解读）
    _show_base_risk_panel(m)

    # 额外指标
    r5, r6 = st.columns(2)
    with r5:
        interp, color = _interpret_sortino(m['sortino'])
        _metric_with_interpret("Sortino 比率", f"{m['sortino']:.2f}", interp, color)
    with r6:
        interp, color = _interpret_winrate(m['win_rate'])
        _metric_with_interpret("月度胜率", f"{m['win_rate']*100:.1f}%", interp, color)

    # 回撤修复天数（权益类也显示）
    r7, _ = st.columns(2)
    with r7:
        interp, color = _interpret_recovery(m['max_rec'])
        _metric_with_interpret("回撤修复天数", f"{m['max_rec']} 天", interp, color)

    st.divider()

    # Alpha/Beta 深度拆解
    if ab:
        st.subheader("🔬 Alpha/Beta 因子拆解（核心）")
        tag, tag_color, diagnosis, suggestion = diagnose_equity(ab)

        # AI 点评卡片
        card_cls = "report-card green" if tag_color == "gold" else \
                   "report-card red" if tag_color == "danger" else "report-card"
        tag_cls = f"tag-{tag_color}"
        st.markdown(f"""
        <div class="{card_cls}">
            <span class="scenario-tag {tag_cls}">{tag}</span>
            <div class="verdict">📋 AI 投顾点评</div>
            <div class="sub-text">{diagnosis}<br><br>💡 建议：{suggestion}</div>
        </div>
        """, unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Alpha（年化）", f"{ab['alpha']*100:.2f}%",
                      help="扣除市场贡献后，经理纯靠选股赚的钱")
        with c2:
            st.metric("Beta", f"{ab['beta']:.2f}",
                      help="大盘涨1%，这只基金涨多少%")
        with c3:
            st.metric("R²", f"{ab['r2']:.2f}",
                      help="收益中有多少比例可以被大盘解释")

        c4, c5 = st.columns(2)
        with c4:
            st.metric("信息比率", f"{ab['info_ratio']:.2f}",
                      help="Alpha / 跟踪误差，衡量超额收益的稳定性。≥0.5 良好")
        with c5:
            sig_text = "✅ 统计显著" if ab["p_alpha"] < 0.05 else f"⚠ 不显著 (p={ab['p_alpha']:.2f})"
            st.metric("Alpha 显著性", sig_text)

        with st.expander("🤔 Alpha / Beta / R² 怎么理解？"):
            st.markdown("""
            **三层收益拆解**：

            **第一层 — Beta（市场收益）**：大盘涨了，大家都涨。
            - Beta=1.0：跟大盘同步
            - Beta=1.3：大盘涨10%，它涨13%（亏时也亏更多）
            - Beta=0.6：与大盘弱相关，走自己的路

            **第二层 — Alpha（纯选股收益）**：扣掉大盘那份之后，经理自己贡献的超额。
            - Alpha>5% 且显著 → 经理有真本事，值得长期持有
            - Alpha≈0 → 经理靠大盘吃饭，考虑换ETF
            - Alpha<0 → 选股拖了后腿，跑输大盘

            **第三层 — R²（可解释度）**：大盘能解释多少收益
            - R²=0.9 → 90% 收益来自大盘，"紧跟大盘型"
            - R²=0.3 → 只有30%来自大盘，独立性强，风险分散

            **信息比率（IR）**：衡量 Alpha 是否稳定而非运气。
            - IR > 0.5：Alpha 具有持续性，经理实力真实
            - IR < 0.3：Alpha 可能是运气，需要更多时间验证
            """)

        # 归因图
        st.plotly_chart(fig_attribution_bar(ab["alpha"], ab["beta"]), use_container_width=True)

    st.divider()

    # 持仓分析（折叠）
    if portfolio is not None and not portfolio.empty:
        with st.expander("🗂 展开持仓穿透分析", expanded=False):
            if hhi is not None:
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("HHI 集中度", f"{hhi:.4f}",
                              help="<0.05 高度分散 | 0.05-0.18 适中 | >0.18 高度集中")
                with c2:
                    if hhi < 0.05:
                        st.info("持仓高度分散，风险分布均匀")
                    elif hhi < 0.18:
                        st.info("持仓适度集中")
                    else:
                        st.warning("持仓高度集中，前几大重仓主导净值走势")

            pie = fig_portfolio_pie(portfolio)
            if pie:
                st.plotly_chart(pie, use_container_width=True)

            st.caption("👆 左右滑动查看完整数据")
            st.dataframe(portfolio.head(15), use_container_width=True)

    st.divider()

    # 综合评分
    score_text, score_color = score_fund_equity(
        m["sharpe"], ab["alpha"] if ab else 0, m["max_dd"], m["win_rate"]
    )
    _verdict_card(
        "🏆 量化综合评分（权益类标准）",
        f'<span style="font-size:1.2rem;">{score_text}</span><br>'
        "综合考量：夏普比率 · Alpha能力 · 最大回撤 · 月度胜率",
        score_color
    )

    # 图表区（折叠）
    with st.expander("📈 展开图表详情", expanded=False):
        if merged is not None and not merged.empty:
            st.plotly_chart(fig_cumulative(merged), use_container_width=True)
        st.plotly_chart(fig_drawdown(df_nav), use_container_width=True)
        if len(df_nav) > 60:
            st.plotly_chart(fig_monthly_heatmap(df_nav), use_container_width=True)


def module_fixed(df_nav, merged, basic, type_info):
    """债券/固收类专项分析模块"""
    st.subheader("📊 固收类核心指标")

    rets = merged["fund_ret"] if merged is not None else df_nav["nav"].pct_change().dropna()
    m = calc_base_metrics(df_nav, rets)

    # 核心指标：固收优先展示 Sortino + 回撤修复
    _show_base_risk_panel(m, show_sortino_primary=True)

    # 额外指标（带实时解读）
    r5, r6 = st.columns(2)
    with r5:
        interp, color = _interpret_recovery(m['max_rec'])
        _metric_with_interpret("回撤修复天数", f"{m['max_rec']} 天", interp, color)
    with r6:
        vol = m['volatility'] * 100
        if vol <= 3:
            interp, color = "极稳，纯债水准", "green"
        elif vol <= 5:
            interp, color = "稳健，固收+水准", "blue"
        elif vol <= 8:
            interp, color = "波动偏大", "yellow"
        else:
            interp, color = "波动剧烈，不像债基", "red"
        _metric_with_interpret("年化波动率", f"{vol:.2f}%", interp, color)

    # 专项诊断
    diag_text, diag_color = diagnose_fixed(m)
    _verdict_card("📋 AI 投顾点评（固收专项）", diag_text, diag_color)

    st.divider()

    # 综合评分
    score_text, score_color = score_fund_fixed(
        m["sortino"], m["max_dd"], m["win_rate"], m["max_rec"]
    )
    _verdict_card(
        "🏆 量化综合评分（固收类标准）",
        f'<span style="font-size:1.2rem;">{score_text}</span><br>'
        "综合考量：Sortino · 最大回撤 · 月度胜率 · 回撤修复天数",
        score_color
    )

    # 图表
    with st.expander("📈 展开图表详情", expanded=False):
        if merged is not None and not merged.empty:
            st.plotly_chart(fig_cumulative(merged, "沪深300参考"), use_container_width=True)
        st.plotly_chart(fig_drawdown(df_nav), use_container_width=True)
        if len(df_nav) > 60:
            st.plotly_chart(fig_monthly_heatmap(df_nav), use_container_width=True)


def module_index(df_nav, merged, basic, type_info):
    """指数/ETF专项分析模块"""
    st.subheader("📊 指数基金核心指标")

    rets = merged["fund_ret"] if merged is not None else df_nav["nav"].pct_change().dropna()
    m = calc_base_metrics(df_nav, rets)

    # 基础收益风险
    _show_base_risk_panel(m)

    # 指数专项指标（带实时解读）
    if merged is not None and len(merged) >= 30:
        idx_m = calc_index_tracking(merged)

        def _interpret_corr(c: float) -> tuple[str, str]:
            if c >= 0.98: return "极优，跟踪精准", "green"
            if c >= 0.95: return "良好，跟踪稳定", "blue"
            if c >= 0.90: return "一般，有一定偏差", "yellow"
            return "偏差较大，建议换产品", "red"

        def _interpret_te(te: float) -> tuple[str, str]:
            if te <= 0.02: return "极优，买得准", "green"
            if te <= 0.05: return "合格，可接受", "blue"
            return "误差偏大，不够精准", "yellow"

        r5, r6 = st.columns(2)
        with r5:
            interp, color = _interpret_corr(idx_m['corr'])
            _metric_with_interpret("与基准相关系数", f"{idx_m['corr']:.4f}", interp, color)
        with r6:
            interp, color = _interpret_te(idx_m['tracking_error'])
            _metric_with_interpret("年化跟踪误差", f"{idx_m['tracking_error']*100:.2f}%", interp, color)

        # 费率解读
        fee_str = basic.get("fee", "--")
        try:
            fee_pct = float(fee_str.replace("%", ""))
            if fee_pct <= 0.15:
                fee_interp, fee_color = "极低费率，性价比极高", "green"
            elif fee_pct <= 0.5:
                fee_interp, fee_color = "费率正常", "blue"
            else:
                fee_interp, fee_color = "费率偏高，建议对比同类", "yellow"
        except Exception:
            fee_interp, fee_color = "费率数据不可用", "blue"

        r7, _ = st.columns(2)
        with r7:
            _metric_with_interpret("管理费率", fee_str, fee_interp, fee_color)

        # 专项诊断
        diag_text, diag_color = diagnose_index(idx_m, None, basic.get("fee", "--"))
        _verdict_card("📋 AI 投顾点评（指数ETF专项）", diag_text, diag_color)

        st.divider()

        # 综合评分
        score_text, score_color = score_fund_index(
            idx_m["corr"], idx_m["tracking_error"], basic.get("fee", "--")
        )
        _verdict_card(
            "🏆 量化综合评分（指数ETF标准）",
            f'<span style="font-size:1.2rem;">{score_text}</span><br>'
            "综合考量：跟踪精度 · 费率 · 收益表现",
            score_color
        )

        # 图表
        with st.expander("📈 展开图表详情", expanded=False):
            st.plotly_chart(fig_cumulative(merged, "基准指数"), use_container_width=True)
            st.plotly_chart(fig_tracking_bar(merged["fund_ret"], merged["bench_ret"]),
                            use_container_width=True)
            st.plotly_chart(fig_drawdown(df_nav), use_container_width=True)
            if len(df_nav) > 60:
                st.plotly_chart(fig_monthly_heatmap(df_nav), use_container_width=True)
    else:
        st.warning("数据不足，无法计算跟踪误差和相关系数（至少需要30个交易日）")
        with st.expander("📈 展开图表", expanded=False):
            st.plotly_chart(fig_drawdown(df_nav), use_container_width=True)


# ===================== 主界面 =====================

def main():
    # Banner
    st.markdown("""
    <div class="banner">
        <h1>📊 基金透视仪</h1>
        <p>自动识别类型 · 三套专项引擎 · 白话诊断报告</p>
    </div>
    """, unsafe_allow_html=True)

    # 输入区
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        symbol = st.text_input("基金代码（6位）", value="004812",
                               placeholder="例如 000001", label_visibility="collapsed")
    with c2:
        period_choice = st.selectbox("分析周期", ["近1年", "近3年"], index=1,
                                     label_visibility="collapsed")
    with c3:
        run = st.button("🔍 开始分析", type="primary", use_container_width=True)

    if not run:
        st.markdown("""
        <div style="text-align:center;padding:3rem;color:#aaa;">
            <div style="font-size:3rem">📊</div>
            <p>输入基金代码，点击开始分析</p>
            <p style="font-size:0.85rem">自动识别基金类型，切换对应专项分析模型</p>
            <p style="font-size:0.82rem">支持：权益类 · 债券/固收类 · 指数ETF</p>
        </div>
        """, unsafe_allow_html=True)
        return

    if len(symbol) != 6 or not symbol.isdigit():
        st.error("请输入6位数字基金代码")
        return

    # 数据获取
    with st.spinner("正在识别基金类型并获取数据..."):
        type_info = fetch_fund_type(symbol)
        basic     = fetch_basic_info(symbol)
        df_nav    = fetch_fund_nav(symbol, years=3 if period_choice == "近3年" else 1)
        df_bench  = fetch_benchmark()
        portfolio = fetch_portfolio(symbol)

    if df_nav is None or df_nav.empty:
        st.error("无法获取净值数据，请确认基金代码是否正确")
        return

    # 截取周期
    if period_choice == "近1年":
        cutoff = df_nav["date"].max() - timedelta(days=365)
        df_nav = df_nav[df_nav["date"] >= cutoff].reset_index(drop=True)

    merged = build_merged(df_nav, df_bench) if df_bench is not None else None

    category = type_info["category"]

    # 基金头部信息
    _show_base_header(basic, type_info)

    # 货币型：不适合本工具
    if category == "money":
        st.warning(
            "⚠ **货币型基金**的净值始终维持在1.0000附近，Alpha/Beta 等指标对它没有意义。\n\n"
            "建议直接比较：**7日年化收益率** 和 **万份收益**，选最高的那个即可。"
        )
        return

    # 未知类型：提示但继续用权益类分析
    if category == "unknown":
        st.info(f"⚠ 未能识别基金类型（{type_info['raw_type']}），将使用权益类分析框架。结果仅供参考。")
        category = "equity"

    # 路由到对应模块
    if category == "equity":
        module_equity(df_nav, merged, portfolio, basic, type_info)
    elif category == "fixed":
        module_fixed(df_nav, merged, basic, type_info)
    elif category == "index":
        module_index(df_nav, merged, basic, type_info)

    # 免责声明
    st.divider()
    st.caption("⚠ 本工具仅提供量化数据分析，不构成投资建议。基金有风险，投资须谨慎。历史业绩不代表未来表现。")


if __name__ == "__main__":
    main()
