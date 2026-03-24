"""
基金全维度体检中心 v6.0
特点：数据更完善、分析更深入、等待时间更长
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import statsmodels.api as sm
import time
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(page_title="基金全维度体检中心", layout="wide")

# ==================== CSS 样式 ====================
st.markdown("""
<style>
.metric-card {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-radius: 12px;
    padding: 16px;
    color: white;
    margin: 8px 0;
}
.metric-value {
    font-size: 1.8rem;
    font-weight: bold;
}
.metric-label {
    font-size: 0.85rem;
    opacity: 0.9;
}
.metric-desc {
    font-size: 0.8rem;
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid rgba(255,255,255,0.3);
}
.diagnosis-card {
    background: #f8f9fa;
    border-left: 4px solid #28a745;
    padding: 16px;
    border-radius: 0 8px 8px 0;
    margin: 12px 0;
}
.warning-card {
    border-left-color: #ffc107;
    background: #fffbeb;
}
.danger-card {
    border-left-color: #dc3545;
    background: #fef2f2;
}
</style>
""", unsafe_allow_html=True)


# ==================== 数据获取层 ====================

@st.cache_data(ttl=3600)
def fetch_fund_basic_info(symbol: str) -> dict:
    """获取基金基础信息（雪球接口，含规模/成立时间/业绩基准）"""
    try:
        df = ak.fund_individual_basic_info_xq(symbol=symbol)
        info = dict(zip(df['item'], df['value']))
        return {
            'name': info.get('基金名称', 'N/A'),
            'full_name': info.get('基金全称', 'N/A'),
            'establish_date': info.get('成立时间', 'N/A'),
            'scale': info.get('最新规模', 'N/A'),
            'company': info.get('基金公司', 'N/A'),
            'manager': info.get('基金经理', 'N/A'),
            'benchmark': info.get('业绩比较基准', 'N/A'),
            'invest_strategy': info.get('投资策略', 'N/A'),
            'fund_type': info.get('基金类型', 'N/A')
        }
    except Exception as e:
        st.warning(f"基金详情获取失败: {e}")
        return {}


@st.cache_data(ttl=3600)
def fetch_nav_data(symbol: str, years: int = 3) -> pd.DataFrame | None:
    """获取基金净值数据"""
    try:
        df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        df.columns = ["date", "nav", "pct"]
        df["date"] = pd.to_datetime(df["date"])
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.sort_values("date").dropna()
        
        cutoff = df["date"].max() - timedelta(days=365*years)
        return df[df["date"] >= cutoff].reset_index(drop=True)
    except Exception as e:
        st.error(f"净值数据获取失败: {e}")
        return None


@st.cache_data(ttl=3600)
def fetch_benchmark_data(symbol: str = "sh000300") -> pd.DataFrame | None:
    """获取基准指数数据"""
    try:
        df = ak.stock_zh_index_daily(symbol=symbol)
        df = df[["date", "close"]].copy()
        df.columns = ["date", "bench"]
        df["date"] = pd.to_datetime(df["date"])
        df["bench"] = pd.to_numeric(df["bench"], errors="coerce")
        return df.sort_values("date").dropna().reset_index(drop=True)
    except Exception:
        return None


@st.cache_data(ttl=3600)
def fetch_holdings(symbol: str) -> pd.DataFrame | None:
    """获取基金持仓数据"""
    try:
        # 尝试最近两年的数据
        for year in ["2024", "2023"]:
            try:
                df = ak.fund_portfolio_hold_em(symbol=symbol, date=year)
                if not df.empty:
                    return df
            except:
                continue
        return None
    except Exception as e:
        st.warning(f"持仓数据获取失败: {e}")
        return None


@st.cache_data(ttl=3600)
def fetch_stock_valuation_batch() -> pd.DataFrame:
    """批量获取全市场股票估值数据（缓存避免重复请求）"""
    errors = []
    
    # 尝试方法1: 东财接口
    try:
        df = ak.stock_zh_a_spot_em()
        
        # 动态查找列名（容错不同版本）
        col_mapping = {}
        for target in ["代码", "名称", "市盈", "市净", "市值", "行业"]:
            for col in df.columns:
                if target in col:
                    col_mapping[target] = col
                    break
        
        # 检查关键列是否存在
        if "代码" in col_mapping and ("市盈" in col_mapping or "市净" in col_mapping):
            selected_cols = []
            rename_map = {}
            if "代码" in col_mapping:
                selected_cols.append(col_mapping["代码"])
                rename_map[col_mapping["代码"]] = "代码"
            if "名称" in col_mapping:
                selected_cols.append(col_mapping["名称"])
                rename_map[col_mapping["名称"]] = "名称"
            if "市盈" in col_mapping:
                selected_cols.append(col_mapping["市盈"])
                rename_map[col_mapping["市盈"]] = "市盈率-动态"
            if "市净" in col_mapping:
                selected_cols.append(col_mapping["市净"])
                rename_map[col_mapping["市净"]] = "市净率"
            if "市值" in col_mapping:
                selected_cols.append(col_mapping["市值"])
                rename_map[col_mapping["市值"]] = "总市值"
            if "行业" in col_mapping:
                selected_cols.append(col_mapping["行业"])
                rename_map[col_mapping["行业"]] = "所属行业"
            
            result = df[selected_cols].copy()
            result = result.rename(columns=rename_map)
            
            # 验证数据有效性
            if len(result) > 100 and ('市盈率-动态' in result.columns or '市净率' in result.columns):
                return result
            else:
                errors.append(f"东财接口返回数据不足: {len(result)}行")
        else:
            errors.append(f"东财接口缺少关键列，可用列: {list(df.columns)[:10]}...")
    except Exception as e:
        errors.append(f"东财接口异常: {str(e)[:50]}")
    
    # 尝试方法2: 新浪接口
    try:
        df = ak.stock_zh_a_spot()
        # 新浪接口列名不同，需要映射
        col_mapping = {}
        for target in ["代码", "名称", "市盈", "市净", "市值", "行业"]:
            for col in df.columns:
                if target in col:
                    col_mapping[target] = col
                    break
        
        if "代码" in col_mapping:
            selected_cols = [col_mapping[c] for c in col_mapping]
            result = df[selected_cols].copy()
            rename_map = {v: k for k, v in col_mapping.items()}
            rename_map[col_mapping.get("市盈", "")] = "市盈率-动态"
            rename_map[col_mapping.get("市净", "")] = "市净率"
            rename_map[col_mapping.get("市值", "")] = "总市值"
            rename_map[col_mapping.get("行业", "")] = "所属行业"
            result = result.rename(columns=rename_map)
            if len(result) > 100:
                return result
    except Exception as e:
        errors.append(f"新浪接口异常: {str(e)[:50]}")
    
    # 返回空DataFrame，但带上错误信息
    empty_df = pd.DataFrame()
    empty_df._errors = errors  # 附加错误信息用于调试
    return empty_df


def enrich_holdings_with_valuation(holdings: pd.DataFrame) -> pd.DataFrame:
    """为持仓数据添加估值信息"""
    if holdings is None or holdings.empty:
        return holdings
    
    # 批量获取全市场估值数据（静默加载，只显示齿轮动画）
    valuation_df = fetch_stock_valuation_batch()
    
    if valuation_df.empty:
        return holdings
    
    # 合并持仓和估值数据
    merged = holdings.merge(
        valuation_df,
        left_on="股票代码",
        right_on="代码",
        how="left"
    )
    
    # 清理数据
    merged["市盈率-动态"] = pd.to_numeric(merged.get("市盈率-动态"), errors="coerce")
    merged["市净率"] = pd.to_numeric(merged.get("市净率"), errors="coerce")
    merged["总市值"] = pd.to_numeric(merged.get("总市值"), errors="coerce")
    merged["占净值比例"] = pd.to_numeric(merged.get("占净值比例"), errors="coerce")
    
    return merged


# ==================== 计算层 ====================

def calc_performance_attribution(nav_df: pd.DataFrame, bench_df: pd.DataFrame) -> dict:
    """业绩归因：Alpha/Beta/Sharpe/MDD"""
    merged = pd.merge(nav_df[["date", "nav"]], bench_df[["date", "bench"]], on="date", how="inner")
    if len(merged) < 60:
        return {}
    
    merged["fund_ret"] = merged["nav"].pct_change()
    merged["bench_ret"] = merged["bench"].pct_change()
    merged = merged.dropna()
    
    if len(merged) < 30:
        return {}
    
    # 基础指标
    fund_rets = merged["fund_ret"]
    bench_rets = merged["bench_ret"]
    
    annual_ret = fund_rets.mean() * 252
    annual_vol = fund_rets.std() * np.sqrt(252)
    rf = 0.02
    sharpe = (annual_ret - rf) / annual_vol if annual_vol > 0 else 0
    
    # 最大回撤
    cumulative = (1 + fund_rets).cumprod()
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    max_dd = drawdown.min()
    calmar = annual_ret / abs(max_dd) if max_dd != 0 else 0
    
    # Alpha/Beta回归
    y = fund_rets - rf/252
    X = sm.add_constant(bench_rets - rf/252)
    model = sm.OLS(y, X).fit()
    
    alpha_daily = model.params["const"]
    alpha_annual = alpha_daily * 252
    beta = model.params.iloc[1] if len(model.params) > 1 else 0
    r_squared = model.rsquared
    alpha_pvalue = model.pvalues["const"]
    
    # 信息比率
    tracking_error = (fund_rets - bench_rets).std() * np.sqrt(252)
    info_ratio = (annual_ret - bench_rets.mean() * 252) / tracking_error if tracking_error > 0 else 0
    
    # 月度胜率
    merged["year_month"] = merged["date"].dt.to_period("M")
    monthly = merged.groupby("year_month").agg({
        "fund_ret": lambda x: (1 + x).prod() - 1,
        "bench_ret": lambda x: (1 + x).prod() - 1
    })
    win_rate = (monthly["fund_ret"] > 0).mean()
    
    return {
        "annual_ret": annual_ret,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "calmar": calmar,
        "alpha": alpha_annual,
        "alpha_pvalue": alpha_pvalue,
        "beta": beta,
        "r_squared": r_squared,
        "info_ratio": info_ratio,
        "win_rate": win_rate,
        "tracking_error": tracking_error,
        "merged_df": merged
    }


def calc_portfolio_style(holdings_enriched: pd.DataFrame) -> dict:
    """持仓风格分析：集中度/估值/行业"""
    if holdings_enriched is None or holdings_enriched.empty:
        return {}
    
    df = holdings_enriched.copy()
    
    # 确保占净值比例是数值
    df["占净值比例"] = pd.to_numeric(df["占净值比例"], errors="coerce")
    
    # CR10（前十大占比）
    cr10 = df["占净值比例"].sum()
    
    # 查找估值列（容错不同列名）
    pe_col = None
    for col in df.columns:
        if "市盈" in col or col in ["PE", "pe", "市盈率"]:
            pe_col = col
            break
    
    pb_col = None
    for col in df.columns:
        if "市净" in col or col in ["PB", "pb", "市净率"]:
            pb_col = col
            break
    
    mv_col = None
    for col in df.columns:
        if "市值" in col and "总" in col:
            mv_col = col
            break
    
    # 加权平均估值
    weighted_pe = np.nan
    if pe_col and pe_col in df.columns:
        df[pe_col] = pd.to_numeric(df[pe_col], errors="coerce")
        df_valid_pe = df[df[pe_col] > 0]
        if df_valid_pe["占净值比例"].sum() > 0:
            weighted_pe = (df_valid_pe[pe_col] * df_valid_pe["占净值比例"]).sum() / df_valid_pe["占净值比例"].sum()
    
    weighted_pb = np.nan
    if pb_col and pb_col in df.columns:
        df[pb_col] = pd.to_numeric(df[pb_col], errors="coerce")
        df_valid_pb = df[df[pb_col] > 0]
        if df_valid_pb["占净值比例"].sum() > 0:
            weighted_pb = (df_valid_pb[pb_col] * df_valid_pb["占净值比例"]).sum() / df_valid_pb["占净值比例"].sum()
    
    # 平均市值（亿元）
    avg_mv = np.nan
    if mv_col and mv_col in df.columns:
        df[mv_col] = pd.to_numeric(df[mv_col], errors="coerce")
        df_valid_mv = df[df[mv_col] > 0]
        if df_valid_mv["占净值比例"].sum() > 0:
            avg_mv = (df_valid_mv[mv_col] * df_valid_mv["占净值比例"]).sum() / df_valid_mv["占净值比例"].sum() / 1e8
    
    # 行业分布
    industry_col = None
    for col in df.columns:
        if "行业" in col or "industry" in col.lower():
            industry_col = col
            break
    
    industry_dist = pd.Series()
    if industry_col and industry_col in df.columns:
        industry_dist = df.groupby(industry_col)["占净值比例"].sum().sort_values(ascending=False)
    
    top_industry = industry_dist.index[0] if len(industry_dist) > 0 else "N/A"
    top_industry_pct = industry_dist.iloc[0] if len(industry_dist) > 0 else 0
    
    # 风格判定
    if weighted_pe > 30 and weighted_pb > 3:
        style = "高质量成长型"
        style_desc = "高PE高PB，押注高成长赛道"
    elif weighted_pe < 15 and weighted_pb < 1.5:
        style = "深度价值型"
        style_desc = "低PE低PB，捡便宜等修复"
    else:
        style = "均衡配置型"
        style_desc = "估值适中，不极端"
    
    # 集中度判定
    if cr10 > 70:
        concentration = "高度集中"
        conc_desc = "前十大占比超70%，押注重仓股"
    elif cr10 > 50:
        concentration = "适度集中"
        conc_desc = "前十大占比50-70%，有重点但不过度"
    else:
        concentration = "分散配置"
        conc_desc = "前十大占比低于50%，分散风险"
    
    # 赛道赌徒判定
    is_sector_bet = top_industry_pct > 40
    
    return {
        "cr10": cr10,
        "weighted_pe": weighted_pe,
        "weighted_pb": weighted_pb,
        "avg_mv": avg_mv,
        "industry_dist": industry_dist,
        "top_industry": top_industry,
        "top_industry_pct": top_industry_pct,
        "style": style,
        "style_desc": style_desc,
        "concentration": concentration,
        "conc_desc": conc_desc,
        "is_sector_bet": is_sector_bet,
        "holdings_df": df
    }


# ==================== 诊断层 ====================

def generate_diagnosis(perf: dict, style: dict, basic: dict) -> dict:
    """生成大白话诊断报告"""
    
    # 1. 性格诊断
    alpha = perf.get("alpha", 0)
    beta = perf.get("beta", 0)
    r2 = perf.get("r_squared", 0)
    alpha_sig = perf.get("alpha_pvalue", 1) < 0.05
    is_sector = style.get("is_sector_bet", False)
    
    if alpha_sig and alpha > 0.03 and beta < 1.0:
        personality = "🏹 独立狙击手"
        person_desc = "选股能力强且不太依赖大盘，有自己独立的投资逻辑"
    elif alpha_sig and alpha > 0.03 and beta >= 1.0:
        personality = "⚡ 激进进攻手"
        person_desc = "敢冲敢打，市场好时涨得猛，跌起来也不含糊"
    elif abs(alpha) < 0.02 and r2 > 0.7:
        personality = "🎭 大盘跟随者"
        person_desc = "主要靠天吃饭，大盘涨它涨，大盘跌它跌"
    elif alpha_sig and alpha < -0.02:
        personality = "⚠️ 能力待验证"
        person_desc = "跑输基准且统计显著，需要观察是否只是阶段性表现"
    elif is_sector:
        personality = "🎰 赛道押注者"
        person_desc = f"重仓{style.get('top_industry', '某一行业')}，赌对了起飞，赌错了趴窝"
    else:
        personality = "⚖️ 均衡配置者"
        person_desc = "不押注单一赛道，追求稳健收益"
    
    # 2. 实力诊断
    sharpe = perf.get("sharpe", 0)
    info_ratio = perf.get("info_ratio", 0)
    win_rate = perf.get("win_rate", 0)
    
    if alpha_sig and alpha > 0:
        ability = "✅ 赚的是「本事的钱」"
        ability_desc = f"Alpha={alpha*100:.1f}% 统计显著，基金经理确实有超额收益能力"
    elif alpha_sig and alpha < 0:
        ability = "❌ 跑输基准"
        ability_desc = "不仅没创造超额收益，还稳定跑输，需要警惕"
    else:
        ability = "🎲 赚的是「运气的钱」"
        ability_desc = "Alpha不显著，无法区分是能力还是运气"
    
    if info_ratio > 0.5:
        stability = "信息比率良好，超额收益较稳定"
    elif info_ratio > 0:
        stability = "信息比率一般，超额收益波动较大"
    else:
        stability = "信息比率为负，跑输基准"
    
    # 3. 避坑指南
    warnings = []
    
    if perf.get("max_dd", 0) < -0.35:
        warnings.append({
            "icon": "💔",
            "title": "心脏不好别买",
            "desc": f"最大回撤{perf['max_dd']*100:.1f}%，跌起来像过山车"
        })
    
    if is_sector:
        warnings.append({
            "icon": "🎰",
            "title": "不懂行业别买",
            "desc": f"{style.get('top_industry_pct', 0):.1f}%仓位押注{style.get('top_industry', '某行业')}，你需要对这个行业有判断"
        })
    
    if beta > 1.3:
        warnings.append({
            "icon": "📉",
            "title": "熊市别买",
            "desc": f"Beta={beta:.2f}，大盘跌1%它跌{beta:.2f}%，熊市里会很难受"
        })
    
    if sharpe < 0.3:
        warnings.append({
            "icon": "⚠️",
            "title": "性价比不高",
            "desc": f"夏普比率{sharpe:.2f}，承担的风险和收益不成正比"
        })
    
    if not warnings:
        warnings.append({
            "icon": "✅",
            "title": "暂无重大风险",
            "desc": "各项指标在可接受范围内，但仍需关注市场变化"
        })
    
    return {
        "personality": personality,
        "person_desc": person_desc,
        "ability": ability,
        "ability_desc": ability_desc,
        "stability": stability,
        "warnings": warnings
    }


# ==================== 展示层 ====================

def show_metric_card(label: str, value: str, desc: str, color: str = "blue"):
    """展示指标卡片"""
    colors = {
        "blue": "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        "green": "linear-gradient(135deg, #11998e 0%, #38ef7d 100%)",
        "orange": "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)",
        "red": "linear-gradient(135deg, #eb3349 0%, #f45c43 100%)"
    }
    bg = colors.get(color, colors["blue"])
    
    st.markdown(f"""
    <div style="background: {bg}; border-radius: 12px; padding: 16px; color: white; margin: 8px 0;">
        <div style="font-size: 0.85rem; opacity: 0.9;">{label}</div>
        <div style="font-size: 1.8rem; font-weight: bold;">{value}</div>
        <div style="font-size: 0.8rem; margin-top: 8px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.3);">
            💡 {desc}
        </div>
    </div>
    """, unsafe_allow_html=True)


def show_diagnosis_card(title: str, content: str, level: str = "info"):
    """展示诊断卡片"""
    colors = {"info": "#28a745", "warning": "#ffc107", "danger": "#dc3545"}
    bg_colors = {"info": "#f8f9fa", "warning": "#fffbeb", "danger": "#fef2f2"}
    
    color = colors.get(level, colors["info"])
    bg = bg_colors.get(level, bg_colors["info"])
    
    st.markdown(f"""
    <div style="background: {bg}; border-left: 4px solid {color}; padding: 16px; border-radius: 0 8px 8px 0; margin: 12px 0;">
        <div style="font-weight: bold; color: {color}; margin-bottom: 8px;">{title}</div>
        <div style="color: #333;">{content}</div>
    </div>
    """, unsafe_allow_html=True)


def plot_cumulative_return(merged_df: pd.DataFrame):
    """绘制累计收益对比图"""
    merged_df["fund_cum"] = (1 + merged_df["fund_ret"]).cumprod()
    merged_df["bench_cum"] = (1 + merged_df["bench_ret"]).cumprod()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=merged_df["date"], y=merged_df["fund_cum"],
        name="基金", line=dict(color="#e74c3c", width=2)
    ))
    fig.add_trace(go.Scatter(
        x=merged_df["date"], y=merged_df["bench_cum"],
        name="沪深300", line=dict(color="#3498db", width=2)
    ))
    fig.update_layout(
        title="累计收益走势对比",
        xaxis_title="日期",
        yaxis_title="累计收益",
        hovermode="x unified",
        height=400
    )
    return fig


def plot_drawdown(merged_df: pd.DataFrame):
    """绘制回撤图"""
    cumulative = (1 + merged_df["fund_ret"]).cumprod()
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max * 100
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=merged_df["date"], y=drawdown,
        fill="tozeroy", fillcolor="rgba(231, 76, 60, 0.3)",
        line=dict(color="#e74c3c"), name="回撤"
    ))
    fig.update_layout(
        title="历史回撤",
        xaxis_title="日期",
        yaxis_title="回撤幅度 (%)",
        height=300
    )
    return fig


def plot_industry_pie(industry_dist: pd.Series):
    """绘制行业分布饼图"""
    if industry_dist.empty:
        return None
    
    # 只显示前5，其他归为"其他"
    top5 = industry_dist.head(5)
    other = industry_dist.iloc[5:].sum() if len(industry_dist) > 5 else 0
    
    labels = list(top5.index) + (["其他"] if other > 0 else [])
    values = list(top5.values) + ([other] if other > 0 else [])
    
    fig = px.pie(names=labels, values=values, title="行业分布")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return fig


def plot_holdings_bar(holdings_df: pd.DataFrame):
    """绘制持仓条形图"""
    if holdings_df is None or holdings_df.empty:
        return None
    
    df = holdings_df.head(10).copy()
    df = df.sort_values("占净值比例", ascending=True)
    
    fig = go.Figure(go.Bar(
        x=df["占净值比例"],
        y=df["股票名称"],
        orientation="h",
        marker_color="#3498db"
    ))
    fig.update_layout(
        title="前十大重仓股",
        xaxis_title="占净值比例 (%)",
        height=400
    )
    return fig


# ==================== 主界面 ====================

def main():
    st.title("🔬 基金全维度体检中心 v6.0")
    st.markdown("*数据更完善，分析更深入，等待时间更长*")
    
    # 输入区域
    col1, col2 = st.columns([2, 1])
    with col1:
        symbol = st.text_input("输入基金代码", value="004812", placeholder="如: 004812")
    with col2:
        analyze_btn = st.button("🔍 开始深度体检", type="primary", use_container_width=True)
    
    if not analyze_btn:
        st.info("👆 输入基金代码后点击按钮开始分析")
        return
    
    # 数据获取阶段（使用简洁的加载动画）
    with st.spinner(""):
        basic = fetch_fund_basic_info(symbol)
        nav_df = fetch_nav_data(symbol)
        bench_df = fetch_benchmark_data()
        holdings_raw = fetch_holdings(symbol)
        holdings_enriched = enrich_holdings_with_valuation(holdings_raw)
    
    # 检查数据完整性
    if nav_df is None or nav_df.empty:
        st.error("❌ 净值数据获取失败，无法继续分析")
        return
    
    # 展示基础信息
    st.divider()
    st.subheader(f"📋 {basic.get('name', symbol)} 基础档案")
    
    info_cols = st.columns(4)
    with info_cols[0]:
        st.metric("基金类型", basic.get('fund_type', 'N/A'))
    with info_cols[1]:
        st.metric("成立时间", basic.get('establish_date', 'N/A'))
    with info_cols[2]:
        st.metric("最新规模", basic.get('scale', 'N/A'))
    with info_cols[3]:
        st.metric("基金经理", basic.get('manager', 'N/A'))
    
    with st.expander("查看投资策略与业绩基准"):
        st.write(f"**业绩比较基准**: {basic.get('benchmark', 'N/A')}")
        st.write(f"**投资策略**: {basic.get('invest_strategy', 'N/A')[:200]}...")
    
    # 计算分析
    perf = calc_performance_attribution(nav_df, bench_df) if bench_df is not None else {}
    style = calc_portfolio_style(holdings_enriched)
    diagnosis = generate_diagnosis(perf, style, basic)
    
    # 大白话诊断报告（置顶）
    st.divider()
    st.subheader("🩺 大白话体检报告")
    
    diag_col1, diag_col2 = st.columns(2)
    with diag_col1:
        show_diagnosis_card(
            f"性格诊断: {diagnosis['personality']}",
            diagnosis['person_desc'],
            "info"
        )
        show_diagnosis_card(
            diagnosis['ability'],
            f"{diagnosis['ability_desc']}<br><br>📊 {diagnosis['stability']}",
            "warning" if "❌" in diagnosis['ability'] else "info"
        )
    
    with diag_col2:
        st.markdown("**⚠️ 避坑指南**")
        for w in diagnosis['warnings']:
            show_diagnosis_card(
                f"{w['icon']} {w['title']}",
                w['desc'],
                "danger" if w['title'] in ["心脏不好别买", "不懂行业别买"] else "warning"
            )
    
    # 专业数据看板
    st.divider()
    st.subheader("📊 专业数据看板")
    
    # 业绩归因
    st.markdown("**维度一：业绩归因 (Alpha/Beta 模型)**")
    if perf:
        perf_cols = st.columns(4)
        with perf_cols[0]:
            alpha_color = "green" if perf['alpha'] > 0.03 else "blue" if perf['alpha'] > 0 else "orange"
            show_metric_card(
                "Alpha (年化)",
                f"{perf['alpha']*100:.2f}%",
                "统计显著" if perf['alpha_pvalue'] < 0.05 else "不显著",
                alpha_color
            )
        with perf_cols[1]:
            beta_color = "green" if 0.8 <= perf['beta'] <= 1.2 else "orange"
            show_metric_card(
                "Beta",
                f"{perf['beta']:.2f}",
                "与大盘同步" if 0.8 <= perf['beta'] <= 1.2 else ("激进型" if perf['beta'] > 1.2 else "防御型"),
                beta_color
            )
        with perf_cols[2]:
            sharpe_color = "green" if perf['sharpe'] >= 1 else "blue" if perf['sharpe'] >= 0.5 else "orange"
            show_metric_card(
                "夏普比率",
                f"{perf['sharpe']:.2f}",
                "优秀" if perf['sharpe'] >= 1 else ("合格" if perf['sharpe'] >= 0.5 else "偏低"),
                sharpe_color
            )
        with perf_cols[3]:
            dd_color = "green" if perf['max_dd'] > -0.15 else "blue" if perf['max_dd'] > -0.25 else "orange" if perf['max_dd'] > -0.35 else "red"
            show_metric_card(
                "最大回撤",
                f"{perf['max_dd']*100:.2f}%",
                "很稳" if perf['max_dd'] > -0.15 else ("正常" if perf['max_dd'] > -0.25 else ("波动大" if perf['max_dd'] > -0.35 else "过山车")),
                dd_color
            )
        
        # 图表
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(plot_cumulative_return(perf['merged_df']), use_container_width=True)
        with chart_col2:
            st.plotly_chart(plot_drawdown(perf['merged_df']), use_container_width=True)
    else:
        st.warning("基准数据不足，无法计算业绩归因指标")
    
    # 持仓穿透
    st.markdown("**维度二：持仓风格穿透**")
    if style:
        style_cols = st.columns(4)
        with style_cols[0]:
            cr10_color = "orange" if style['cr10'] > 70 else "blue" if style['cr10'] > 50 else "green"
            show_metric_card(
                "CR10 (前十大占比)",
                f"{style['cr10']:.1f}%",
                style['conc_desc'],
                cr10_color
            )
        with style_cols[1]:
            pe_color = "orange" if style['weighted_pe'] > 30 else "blue" if style['weighted_pe'] > 15 else "green"
            show_metric_card(
                "加权平均 PE",
                f"{style['weighted_pe']:.1f}" if not np.isnan(style['weighted_pe']) else "N/A",
                "偏成长" if style['weighted_pe'] > 30 else ("适中" if style['weighted_pe'] > 15 else "偏价值"),
                pe_color
            )
        with style_cols[2]:
            show_metric_card(
                "加权平均 PB",
                f"{style['weighted_pb']:.2f}" if not np.isnan(style['weighted_pb']) else "N/A",
                style['style_desc'],
                "blue"
            )
        with style_cols[3]:
            mv_color = "green" if style['avg_mv'] > 1000 else "blue"
            show_metric_card(
                "平均市值",
                f"{style['avg_mv']:.0f}亿" if not np.isnan(style['avg_mv']) else "N/A",
                "大盘蓝筹" if style['avg_mv'] > 1000 else ("中盘" if style['avg_mv'] > 200 else "小盘成长"),
                mv_color
            )
        
        # 持仓图表
        hold_col1, hold_col2 = st.columns(2)
        with hold_col1:
            fig = plot_industry_pie(style['industry_dist'])
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("行业数据暂不可用")
        with hold_col2:
            fig = plot_holdings_bar(style['holdings_df'])
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("持仓数据暂不可用")
    else:
        st.warning("持仓数据不足，无法进行风格分析")
    
    # 详细数据表
    with st.expander("查看详细指标数据"):
        if perf:
            st.markdown("**业绩归因指标**")
            perf_df = pd.DataFrame({
                "指标": ["年化收益率", "年化波动率", "夏普比率", "最大回撤", "卡玛比率",
                        "Alpha(年化)", "Alpha显著性", "Beta", "R²", "信息比率", "月度胜率", "跟踪误差"],
                "数值": [
                    f"{perf['annual_ret']*100:.2f}%",
                    f"{perf['annual_vol']*100:.2f}%",
                    f"{perf['sharpe']:.2f}",
                    f"{perf['max_dd']*100:.2f}%",
                    f"{perf['calmar']:.2f}",
                    f"{perf['alpha']*100:.2f}%",
                    "显著" if perf['alpha_pvalue'] < 0.05 else "不显著",
                    f"{perf['beta']:.2f}",
                    f"{perf['r_squared']:.2f}",
                    f"{perf['info_ratio']:.2f}",
                    f"{perf['win_rate']*100:.1f}%",
                    f"{perf['tracking_error']*100:.2f}%"
                ]
            })
            st.dataframe(perf_df, use_container_width=True)
        
        if style and style['holdings_df'] is not None:
            st.markdown("**前十大重仓股详情**")
            display_df = style['holdings_df'].head(10)[[
                '股票代码', '股票名称', '占净值比例', '市盈率-动态', '市净率', '所属行业'
            ]].copy()
            display_df.columns = ['代码', '名称', '占比(%)', 'PE', 'PB', '行业']
            st.dataframe(display_df, use_container_width=True)


if __name__ == "__main__":
    main()
