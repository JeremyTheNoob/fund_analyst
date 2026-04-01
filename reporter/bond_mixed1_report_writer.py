"""
债券型-混合一级基金专属深度报告生成器 — fund_quant_v2

混合一级债基特点：债底 + 可转债增强，不能直接二级市场买股票
分析框架 5 板块：
  ① 资产结构分布，信用/转债配比
  ② 收益表现 + 转债弹性模型（转债 Beta）
  ③ 转债归因分析 + 溢价率审计
  ④ 转债违约/赎回风险 + 全市场估值预警
  ⑤ 拟购入 / 已持有 投资建议
"""

from __future__ import annotations
from typing import Any, Optional
import logging
import math

from data_loader.rate_prediction import predict_rate_trend

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_bond_mixed1_report(report: Any) -> dict:
    """
    生成混合一级债基 5 板块深度报告。

    Args:
        report: FundReport 对象

    Returns:
        {
          "headline":    标题行,
          "section1":    资产结构与信用/转债配比,
          "section2":    收益表现与转债弹性分析,
          "section3":    转债归因与溢价率审计,
          "section4":    转债违约/赎回风险预警,
          "section5":    投资建议,
        }
    """
    basic  = report.basic
    m      = report.bond_metrics
    charts = report.chart_data
    h_data = charts.get("holdings", {})

    if not m:
        return _fallback_report(basic)

    cm = m.common
    fund_name = basic.name
    grade = m.score_grade
    score = m.overall_score

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 核心数据提取
    ann_ret     = round(cm.annualized_return * 100, 2)
    cum_ret     = round(cm.cumulative_return * 100, 1)
    volatility  = round(cm.volatility * 100, 2)
    max_dd      = round(cm.max_drawdown * 100, 2)
    sharpe      = round(cm.sharpe_ratio, 2)
    calmar      = round(cm.calmar_ratio, 2)
    monthly_wr  = round(cm.monthly_win_rate * 100, 1)
    duration    = round(m.duration, 1)
    wacs        = round(m.wacs_score, 0)
    alpha_bond  = round(m.alpha_bond * 100, 2)
    r_squared   = round(m.r_squared, 4)

    # 资产配置数据
    stock_ratio = h_data.get("stock_ratio", 0) or 0
    bond_ratio  = h_data.get("bond_ratio", 0) or 0
    cash_ratio  = h_data.get("cash_ratio", 0) or 0
    cb_ratio    = h_data.get("cb_ratio", 0) or 0
    bond_details = h_data.get("bond_details", []) or []
    historical_allocation = h_data.get("historical_allocation", []) or []

    # 转债持仓数据
    cb_holdings_df = charts.get("cb_holdings_df")

    # 转债 Beta
    cb_beta = charts.get("cb_beta", {})
    cb_beta_value = cb_beta.get("beta", None)
    cb_beta_r2 = cb_beta.get("r_squared", None)
    cb_corr = cb_beta.get("correlation", None)

    # 全市场转债估值数据
    cb_value_df = charts.get("cb_value_analysis")

    # 基准数据
    bm_info    = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm     = round(bm_info.get("bm_last_return", 0) * 100, 1)
    ann_bm     = round(bm_info.get("bm_annual_return", 0) * 100, 2)
    excess_bps = round((ann_ret - ann_bm) * 100, 0)

    # 回撤信息
    dd_info       = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm     = round(dd_info.get("bm_max_dd", 0), 2)
    defensive_ratio = dd_info.get("defensive_ratio", 1.0)
    recovery_info = dd_info.get("recovery_info", {})
    recovery_days = recovery_info.get("recovery_days", cm.recovery_days or 0) or (cm.recovery_days or 0)
    dd_date_str   = _format_date(recovery_info.get("max_dd_date"))

    # 月度统计
    hm_info       = charts.get("monthly_heatmap", {}).get("heatmap_info", {})
    monthly_stats = hm_info.get("monthly_stats", {})
    total_months  = monthly_stats.get("total_months", 0)
    pos_months    = monthly_stats.get("positive_months", 0)

    # 债券分类
    bond_classification = getattr(m, 'bond_classification', {}) or {}

    # 利率预测
    rate_prediction = _get_rate_prediction()

    # 三因子回归
    factor_loadings = m.factor_loadings or {}
    b_short = factor_loadings.get("short_rate", 0)
    b_long  = factor_loadings.get("long_rate", 0)
    b_credit = factor_loadings.get("credit", 0)

    # 从转债持仓中提取详细信息
    cb_analysis = _analyze_cb_portfolio(cb_holdings_df, bond_details, cb_ratio)

    # ── 生成各板块 ────────────────────────────────────────
    headline = _build_headline(
        fund_name, cb_ratio, grade, start_date, end_date
    )

    section1 = _section1_basic_info(
        fund_name, bond_ratio, cash_ratio, cb_ratio, stock_ratio,
        bond_details, bond_classification, historical_allocation,
        cb_analysis, basic,
    )

    section2 = _section2_return_analysis(
        fund_name, ann_ret, ann_bm, excess_bps, cum_ret, cum_bm,
        sharpe, calmar, volatility, max_dd, monthly_wr,
        total_months, pos_months,
        cb_beta_value, cb_beta_r2, cb_corr, cb_ratio,
        start_date, end_date,
    )

    section3 = _section3_cb_attribution(
        fund_name, cb_ratio, cb_holdings_df, bond_details,
        cb_analysis, cb_beta_value, alpha_bond,
        duration, wacs, m.stress_results,
    )

    section4 = _section4_risk_warning(
        fund_name, max_dd, dd_date_str, recovery_days,
        defensive_ratio, max_dd_bm,
        cb_ratio, cb_analysis, duration, wacs,
        rate_prediction, m.stress_results,
        cb_value_df,
    )

    section5 = _section5_advice(
        fund_name, grade, ann_ret, max_dd, sharpe, calmar,
        cb_ratio, cb_analysis, duration, wacs,
        rate_prediction, basic, m.stress_results,
    )

    return {
        "headline":  headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "section5": section5,
    }


# ============================================================
# 板块 1：资产结构
# ============================================================

def _section1_basic_info(
    fund_name: str,
    bond_ratio: float,
    cash_ratio: float,
    cb_ratio: float,
    stock_ratio: float,
    bond_details: list,
    bond_classification: dict,
    historical_allocation: list,
    cb_analysis: dict,
    basic: Any,
) -> str:
    """板块 1：资产结构分布 + 信用/转债配比分析"""

    bond_pct = bond_ratio * 100
    cb_pct   = cb_ratio * 100
    stock_pct = stock_ratio * 100
    cash_pct = cash_ratio * 100
    other_pct = max(100 - bond_pct - cb_pct - stock_pct - cash_pct, 0)

    # 纯债部分拆解（利率债 vs 信用债 vs 城投债 vs 其他）
    bond_struct = _analyze_bond_structure(bond_classification, bond_details, cb_ratio)

    # 信用/转债配比分析
    pure_bond_pct = max(bond_pct - cb_pct, 0)
    if pure_bond_pct + cb_pct > 0:
        cb_share = cb_pct / (pure_bond_pct + cb_pct) * 100
    else:
        cb_share = 0

    if cb_share >= 50:
        cb_share_eval = "**偏激进**——转债占比超过一半，收益弹性大但风险也高"
    elif cb_share >= 30:
        cb_share_eval = "**偏积极**——转债配置比例较高，注重增强收益"
    elif cb_share >= 15:
        cb_share_eval = "**均衡**——信用债和转债比例适中，攻守兼备"
    elif cb_share >= 5:
        cb_share_eval = "**偏保守**——转债作为辅助增强，以信用债为核心"
    else:
        cb_share_eval = "**纯债为主**——转债配置极低，几乎不参与权益市场"

    # 转债仓位描述
    if cb_pct >= 20:
        cb_desc = f"转债仓位 **{cb_pct:.1f}%**，在组合中扮演核心增强角色"
    elif cb_pct >= 10:
        cb_desc = f"转债仓位 **{cb_pct:.1f}%**，适度参与转债增强"
    elif cb_pct >= 5:
        cb_desc = f"转债仓位 **{cb_pct:.1f}%**，以少量转债做增强"
    else:
        cb_desc = f"转债仓位 **{cb_pct:.1f}%**，转债配置极少"

    # 基金规模
    scale_text = ""
    if hasattr(basic, 'scale') and basic.scale:
        scale_text = f"\n**基金规模：** {basic.scale}"

    # 历史配置变化
    history_text = ""
    if len(historical_allocation) >= 2:
        history_text = "\n\n**历史资产配置变化：**\n\n"
        history_text += "| 报告期 | 债券 | 转债 | 现金 |\n"
        history_text += "| --- | --- | --- | --- |\n"
        for q in historical_allocation[-4:]:
            qdate  = q.get("date", "")
            q_bond = q.get("bond_ratio", 0) * 100
            q_cb   = q.get("cb_ratio", 0) * 100
            q_cash = q.get("cash_ratio", 0) * 100
            history_text += f"| {qdate} | {q_bond:.1f}% | {q_cb:.1f}% | {q_cash:.1f}% |\n"

        # 趋势分析
        latest = historical_allocation[-1]
        prev   = historical_allocation[-2] if len(historical_allocation) >= 2 else None
        if prev and latest:
            delta_cb = (latest.get("cb_ratio", 0) - prev.get("cb_ratio", 0)) * 100
            if abs(delta_cb) > 1:
                direction = "加仓" if delta_cb > 0 else "减仓"
                history_text += f"\n最近两个季度转债仓位**{direction}**约 {abs(delta_cb):.1f}%，"
                if delta_cb > 0:
                    history_text += "经理可能在看好转债市场的配置价值。"
                else:
                    history_text += "经理可能在降低转债风险暴露。"

    return f"""### 资产结构与信用/转债配比

混合一级债基的特点是「**债底保护 + 转债增强**」：
不能直接在二级市场买股票，权益收益完全靠**可转债**和**打新**。
所以转债配置策略直接决定了这只基金的弹性和风险。

**当前资产配置：**

| 资产类别 | 占比 | 说明 |
| --- | --- | --- |
| 纯债部分 | {pure_bond_pct:.1f}% | 国债/政金债/信用债/城投债 |
| 可转债 | {cb_pct:.1f}% | 核心权益增强工具 |
| 股票（打新等） | {stock_pct:.1f}% | IPO打新冻结/少量转股 |
| 现金 | {cash_pct:.1f}% | 流动性储备 |
| 其他 | {other_pct:.1f}% | 买入返售/衍生品等 |

{bond_struct}

**信用/转债配比：** {cb_share_eval}

{cb_desc}，信用/转债配比约为 **{max(pure_bond_pct, 0.1):.0f}:{max(cb_pct, 0.1):.0f}**。
{scale_text}{history_text}"""


def _analyze_bond_structure(bond_classification: dict, bond_details: list, cb_ratio: float) -> str:
    """分析纯债部分的券种结构"""

    if not bond_classification:
        return "[INSERT_CHART: BOND_HOLDINGS_PIE]"

    gov_ratio    = bond_classification.get('gov_bond', {}).get('ratio', 0) * 100
    credit_ratio = bond_classification.get('credit_bond', {}).get('ratio', 0) * 100
    urban_ratio  = bond_classification.get('urban_construction', {}).get('ratio', 0) * 100
    estate_ratio = bond_classification.get('real_estate', {}).get('ratio', 0) * 100

    # 从 bond_details 中统计转债占比（在债券总持仓中）
    total_bond_ratio = sum(float(b.get("占净值比例", 0) or 0) for b in bond_details)
    cb_in_bond_ratio = sum(
        float(b.get("占净值比例", 0) or 0)
        for b in bond_details
        if "转债" in str(b.get("债券名称", ""))
    )
    if total_bond_ratio > 0:
        cb_bond_share = cb_in_bond_ratio / total_bond_ratio * 100
    else:
        cb_bond_share = cb_ratio * 100

    text = (
        f"[INSERT_CHART: BOND_HOLDINGS_PIE]\n\n"
        f"**纯债持仓结构（剔除转债）：**\n\n"
        f"| 券种 | 占比 | 说明 |\n"
        f"| --- | --- | --- |\n"
        f"| 利率债（国债/政金债） | {gov_ratio:.1f}% | 无信用风险，久期管理 |\n"
        f"| 信用债（企业债/公司债） | {credit_ratio:.1f}% | 票息收益增强 |\n"
        f"| 城投债 | {urban_ratio:.1f}% | 区域性信用风险 |\n"
        f"| 地产债 | {estate_ratio:.1f}% | 周期性较强 |\n"
        f"| 可转债 | {cb_bond_share:.1f}% | 权益增强（已单独分析） |\n"
    )

    if gov_ratio > 60:
        text += f"\n\n**结构评价：** 利率债占比超六成（{gov_ratio:.1f}%），防守极强，信用风险暴露有限。"
    elif gov_ratio > 30:
        text += f"\n\n**结构评价：** 利率债约三成（{gov_ratio:.1f}%），信用债（{credit_ratio:.1f}%）提供票息增强，整体攻守均衡。"
    else:
        text += f"\n\n**结构评价：** 利率债占比较低（{gov_ratio:.1f}%），主要通过信用下沉获取超额收益，信用风险暴露较高。"

    return text


# ============================================================
# 板块 2：收益表现 — 转债弹性模型
# ============================================================

def _section2_return_analysis(
    fund_name: str,
    ann_ret: float,
    ann_bm: float,
    excess_bps: float,
    cum_ret: float,
    cum_bm: float,
    sharpe: float,
    calmar: float,
    volatility: float,
    max_dd: float,
    monthly_wr: float,
    total_months: int,
    pos_months: int,
    cb_beta_value: Optional[float],
    cb_beta_r2: Optional[float],
    cb_corr: Optional[float],
    cb_ratio: float,
    start_date: str,
    end_date: str,
) -> str:
    """板块 2：收益表现 + 转债 Beta 弹性分析"""

    excess_sign = "跑赢" if excess_bps > 0 else "落后于"
    bps_abs = abs(int(excess_bps))

    # 夏普评价
    if sharpe >= 2.0:
        sharpe_desc = "极佳"
    elif sharpe >= 1.0:
        sharpe_desc = "良好"
    elif sharpe >= 0.5:
        sharpe_desc = "一般"
    else:
        sharpe_desc = "偏低"

    # 转债 Beta 分析
    cb_beta_text = _analyze_cb_beta(cb_beta_value, cb_beta_r2, cb_corr, cb_ratio)

    # 收益来源拆解
    source_text = _estimate_return_source(ann_ret, cb_ratio, max_dd)

    # 波动率分解
    vol_text = ""
    if volatility > 0:
        # 纯债基金波动率通常在 0.3-0.8%
        pure_bond_vol = 0.5  # 基准纯债波动率
        extra_vol = max(volatility - pure_bond_vol, 0)
        if extra_vol > 0.5:
            vol_text = (
                f"\n\n**波动率分解：** 基金波动率 **{volatility:.2f}%** 明显高于纯债基准（~0.5%），"
                f"超额波动约 **{extra_vol:.2f}%**，主要来自转债仓位的权益弹性贡献。"
            )
        elif extra_vol > 0.1:
            vol_text = (
                f"\n\n**波动率分解：** 波动率 **{volatility:.2f}%** 略高于纯债基准，"
                f"转债的权益弹性温和。"
            )
        else:
            vol_text = f"\n\n**波动率分解：** 波动率 **{volatility:.2f}%** 接近纯债水平，转债配置偏保守。"

    return f"""### 收益表现与转债弹性分析

**收益数据总览：**

| 指标 | 数值 | 评价 |
| --- | --- | --- |
| 年化收益 | {ann_ret:.2f}% | {excess_sign}基准 {bps_abs} bps |
| 累计收益 | {cum_ret:.1f}% | 基准 {cum_bm:.1f}% |
| 最大回撤 | {max_dd:.2f}% | {'优秀' if abs(max_dd) < 1 else ('良好' if abs(max_dd) < 3 else '一般')} |
| 波动率 | {volatility:.2f}% | {'低波动' if volatility < 1.0 else '中等波动' if volatility < 2.0 else '高波动'} |
| 月度胜率 | {monthly_wr:.1f}% | {total_months} 个月中 {pos_months} 个月正收益 |
| 夏普比率 | {sharpe:.2f} | {sharpe_desc} |

[INSERT_CHART: CUM_RET]

{cb_beta_text}{source_text}{vol_text}

[INSERT_CHART: HEATMAP]"""


def _analyze_cb_beta(
    cb_beta_value: Optional[float],
    cb_beta_r2: Optional[float],
    cb_corr: Optional[float],
    cb_ratio: float,
) -> str:
    """分析转债 Beta（净值对中证转债指数的敏感度）"""

    if cb_beta_value is None:
        # 尝试基于转债仓位估算
        est_beta = cb_ratio * 100 / 100 * 0.5  # 粗略估算
        return f"""**转债弹性模型（估算）：**

由于缺少中证转债指数对齐数据，基于转债仓位估算：
- 估算转债 Beta：**{est_beta:.2f}**（每 1% 转债指数变动 → 净值变动约 {est_beta:.2f}%）
- 转债仓位 {cb_ratio*100:.1f}%，Delta 加权后的权益暴露约为 {cb_ratio*100*0.4:.1f}%

> 精确的转债 Beta 需要与中证转债指数（000832.CSI）进行回归分析，当前为估算值。
"""

    # Beta 解释
    if cb_beta_value <= 0.2:
        beta_desc = "极低弹性——转债配置保守，净值主要受纯债端影响"
    elif cb_beta_value <= 0.5:
        beta_desc = "低弹性——转债对净值的拉动有限，偏防守型"
    elif cb_beta_value <= 0.8:
        beta_desc = "中等弹性——转债配置较为积极，能跟随转债市场波动"
    elif cb_beta_value <= 1.2:
        beta_desc = "高弹性——转债对净值影响显著，接近纯转债基金特征"
    else:
        beta_desc = "极高弹性——净值高度依赖转债市场，股性十足"

    r2_desc = ""
    if cb_beta_r2 is not None:
        if cb_beta_r2 >= 0.7:
            r2_desc = f"R² = {cb_beta_r2:.2f}，拟合优度**高**，Beta 值可信"
        elif cb_beta_r2 >= 0.4:
            r2_desc = f"R² = {cb_beta_r2:.2f}，拟合优度**中等**，Beta 值有参考意义"
        else:
            r2_desc = f"R² = {cb_beta_r2:.2f}，拟合优度**偏低**，Beta 值仅供参考"

    corr_desc = ""
    if cb_corr is not None:
        corr_desc = f"\n- 与中证转债指数相关系数：**{cb_corr:.2f}**"

    r2_str = f"{cb_beta_r2:.3f}" if cb_beta_r2 is not None else "N/A"
    corr_str = f"{cb_corr:.3f}" if cb_corr is not None else "N/A"

    return f"""**转债弹性模型：**

把基金净值和**中证转债指数（000832.CSI）**做回归，看看转债配置对净值有多大弹性：

| 弹性指标 | 数值 | 说明 |
| --- | --- | --- |
| 转债 Beta | **{cb_beta_value:.3f}** | 中证转债指数涨 1%，净值约涨 {cb_beta_value:.3f}% |
| R² | {r2_str} | {r2_desc} |
| 相关系数 | {corr_str} | |

**弹性解读：** {beta_desc}
{corr_desc}

简单说：混合一级债基的弹性主要来自转债。转债涨时净值有乘数效应，转债跌时回撤也会被放大。
"""


def _estimate_return_source(ann_ret: float, cb_ratio: float, max_dd: float) -> str:
    """估算收益来源（纯债票息 vs 转债弹性）"""

    cb_pct = cb_ratio * 100
    if cb_pct >= 20:
        bond_contrib = ann_ret * 0.40
        cb_contrib = ann_ret * 0.55
        other_contrib = ann_ret * 0.05
    elif cb_pct >= 10:
        bond_contrib = ann_ret * 0.55
        cb_contrib = ann_ret * 0.35
        other_contrib = ann_ret * 0.10
    elif cb_pct >= 5:
        bond_contrib = ann_ret * 0.70
        cb_contrib = ann_ret * 0.20
        other_contrib = ann_ret * 0.10
    else:
        bond_contrib = ann_ret * 0.85
        cb_contrib = ann_ret * 0.05
        other_contrib = ann_ret * 0.10

    return f"""
**收益来源估算（基于转债仓位）：**

| 来源 | 贡献（年化） | 占比 | 说明 |
| --- | --- | --- | --- |
| 纯债票息 | {bond_contrib:.2f}% | {bond_contrib/ann_ret*100:.0f}%（若正收益） | 久期管理 + 信用下沉 |
| 转债弹性 | {cb_contrib:.2f}% | {cb_contrib/ann_ret*100:.0f}%（若正收益） | 转债估值变动 + Delta 贡献 |
| 打新/其他 | {other_contrib:.2f}% | {other_contrib/ann_ret*100:.0f}%（若正收益） | IPO打新 + 转股收益 |

> 以上为基于资产配置比例的粗略估算，实际贡献需结合多期归因分析。
"""


# ============================================================
# 板块 3：深度分析 — 转债归因 + 溢价率审计
# ============================================================

def _section3_cb_attribution(
    fund_name: str,
    cb_ratio: float,
    cb_holdings_df: Any,
    bond_details: list,
    cb_analysis: dict,
    cb_beta_value: Optional[float],
    alpha_bond: float,
    duration: float,
    wacs: float,
    stress_results: list,
) -> str:
    """板块 3：转债归因分析 + 溢价率审计"""

    cb_pct = cb_ratio * 100
    avg_premium = cb_analysis.get("avg_premium", None)
    cb_count = cb_analysis.get("cb_count", 0)
    top_holdings = cb_analysis.get("top_holdings", [])
    high_prem_count = cb_analysis.get("high_prem_count", 0)
    low_prem_count = cb_analysis.get("low_prem_count", 0)
    avg_delta = cb_analysis.get("avg_delta", None)

    # 无转债
    if cb_pct < 1 and cb_count == 0:
        return f"""### 转债归因与溢价率审计

**转债仓位：{cb_pct:.1f}%** —— 当前未配置可转债。

该基金以纯债为核心策略，不参与转债市场。收益完全依赖票息和久期管理。
好处是不受转债估值波动影响，代价是放弃了转债"**下跌有底、上涨不封顶**"的非对称收益特征。
"""

    # 转债持仓明细
    cb_detail_text = ""
    if top_holdings:
        cb_detail_text = "\n\n**当前转债持仓明细（Top 10）：**\n\n"
        cb_detail_text += "| 转债名称 | 占净值比 | 溢价率 | Delta 估算 | 评价 |\n"
        cb_detail_text += "| --- | --- | --- | --- | --- |\n"
        for h in top_holdings[:10]:
            name = h.get("name", "未知")
            ratio = h.get("ratio", 0)
            premium = h.get("premium", None)
            delta = h.get("delta", None)

            prem_str = f"{premium:.1f}%" if premium is not None else "N/A"
            delta_str = f"{delta:.2f}" if delta is not None else "N/A"

            # 评价
            if premium is not None:
                if premium <= 10:
                    eval_str = "低估"
                elif premium <= 20:
                    eval_str = "合理"
                elif premium <= 35:
                    eval_str = "偏高"
                else:
                    eval_str = "高估"
            else:
                eval_str = "—"

            cb_detail_text += f"| {name} | {ratio:.2f}% | {prem_str} | {delta_str} | {eval_str} |\n"

    # 溢价率审计
    premium_audit = ""
    if avg_premium is not None:
        if avg_premium <= 15:
            prem_level = "**低估区间**（平均溢价率 ≤ 15%）"
            prem_detail = "转债定价便宜，下行空间有限，性价比高。建议维持或适度加仓。"
        elif avg_premium <= 25:
            prem_level = "**合理区间**（平均溢价率 15%-25%）"
            prem_detail = "转债定价适中，攻守较为均衡。"
        elif avg_premium <= 35:
            prem_level = "**偏高区间**（平均溢价率 25%-35%）"
            prem_detail = "需警惕正股回调时的估值压缩风险。正股下跌，转债期权价值会快速萎缩。"
        elif avg_premium <= 50:
            prem_level = "**高估区间**（平均溢价率 35%-50%）"
            prem_detail = "转债定价较贵，安全边际较低。建议关注减仓时机。"
        else:
            prem_level = "**极高区间**（平均溢价率 > 50%）"
            prem_detail = "转债严重高估，期权价值极贵。正股回调时回撤风险极大。"

        premium_audit = f"""
**溢价率审计：{prem_level}**

- 平均转股溢价率：**{avg_premium:.1f}%**
- 低溢价转债（≤20%）：**{low_prem_count}** 只
- 高溢价转债（>30%）：**{high_prem_count}** 只

{prem_detail}
"""
    else:
        premium_audit = "\n\n> 转债溢价率数据加载中，刷新后可查看详细审计结果。\n"

    # Delta 分析
    delta_text = ""
    if avg_delta is not None:
        if avg_delta >= 0.7:
            delta_desc = f"平均 Delta **{avg_delta:.2f}**，持仓偏股性，弹性强但防御性弱"
        elif avg_delta >= 0.5:
            delta_desc = f"平均 Delta **{avg_delta:.2f}**，平衡型配置，兼顾攻守"
        elif avg_delta >= 0.3:
            delta_desc = f"平均 Delta **{avg_delta:.2f}**，偏债性配置，防御性强"
        else:
            delta_desc = f"平均 Delta **{avg_delta:.2f}**，深度虚值/债底保护型，弹性极弱"

        delta_text = f"\n\n**Delta 分析：** {delta_desc}\n"
    else:
        delta_text = ""

    # 转债策略总结
    strategy_summary = ""
    if avg_premium is not None and avg_delta is not None:
        if avg_premium <= 20 and avg_delta >= 0.5:
            strategy_summary = "\n\n**策略画像：低溢价高弹性型**——偏好平价附近的转债，追求弹性最大化。"
        elif avg_premium <= 20 and avg_delta < 0.5:
            strategy_summary = "\n\n**策略画像：低溢价防守型**——选择低估值的转债做安全垫，以债底保护为核心。"
        elif avg_premium > 30 and avg_delta >= 0.5:
            strategy_summary = "\n\n**策略画像：高溢价进攻型**——愿意为高弹性转债支付溢价，风险偏好较高。"
        elif avg_premium > 30 and avg_delta < 0.5:
            strategy_summary = "\n\n**策略画像：高溢价低弹性型**——可能持有较多已涨高的转债，性价比不高。"
        else:
            strategy_summary = "\n\n**策略画像：均衡配置型**——转债选择兼顾估值和弹性。"

    # 压力测试
    stress_text = ""
    if stress_results:
        worst = min(stress_results, key=lambda x: x.get("price_impact", 0))
        stress_text = (
            f"\n\n**压力测试（利率冲击对纯债部分的影响）：**\n\n"
            f"在「{worst['scenario']}」情景下，预计纯债组合净值影响约 **{worst['price_impact']:.2f}%**。\n"
            f"叠加转债估值压缩（假设 -5%），总回撤可能达到 **{abs(worst['price_impact']) + cb_pct * 0.05:.2f}%**。"
        )

    return f"""### 转债归因分析与溢价率审计

混合一级债基的收益弹性完全来自转债，下面看看持仓的性价比如何。

**转债仓位：{cb_pct:.1f}%，共持有 {cb_count} 只转债**

[INSERT_CHART: CB_PRICE_PREMIUM]

上图横轴是转债对应正股的价格，纵轴是转股溢价率，气泡越大说明该转债在组合中占比越高。
**绿色=低估、浅绿=合理、橙色=偏高、红色=高估。** 理想状态是集中分布在左下角（正股便宜+溢价率低）。

{cb_detail_text}{premium_audit}{delta_text}{strategy_summary}{stress_text}"""


def _analyze_cb_portfolio(
    cb_holdings_df: Any,
    bond_details: list,
    cb_ratio: float,
) -> dict:
    """分析转债持仓组合"""
    result = {
        "cb_count": 0,
        "avg_premium": None,
        "avg_delta": None,
        "high_prem_count": 0,
        "low_prem_count": 0,
        "top_holdings": [],
    }

    # 优先使用 cb_holdings_df
    if cb_holdings_df is not None and not cb_holdings_df.empty:
        premiums = []
        deltas = []
        for _, row in cb_holdings_df.iterrows():
            name = row.get("债券名称", "未知")
            ratio = float(row.get("占净值比例", 0) or 0)
            premium = row.get("premium_ratio")

            if ratio > 1.5:
                ratio = ratio  # 百分比格式

            result["top_holdings"].append({
                "name": name,
                "ratio": ratio,
                "premium": float(premium) if premium is not None and not (isinstance(premium, float) and math.isnan(premium)) else None,
                "delta": _estimate_delta(premium),
            })

            if premium is not None and not (isinstance(premium, float) and math.isnan(premium)):
                premiums.append(float(premium))
                if float(premium) > 30:
                    result["high_prem_count"] += 1
                elif float(premium) <= 20:
                    result["low_prem_count"] += 1

            result["cb_count"] = len(cb_holdings_df)

            # Delta
            d = _estimate_delta(premium)
            if d is not None:
                deltas.append(d)

        if premiums:
            result["avg_premium"] = float(sum(premiums) / len(premiums))
        if deltas:
            result["avg_delta"] = float(sum(deltas) / len(deltas))

        return result

    # Fallback: 从 bond_details 提取
    for b in bond_details:
        if "转债" in str(b.get("债券名称", "")):
            result["cb_count"] += 1
            ratio = float(b.get("占净值比例", 0) or 0)
            if ratio > 1.5:
                ratio = ratio
            result["top_holdings"].append({
                "name": str(b.get("债券名称", "未知")),
                "ratio": ratio,
                "premium": None,
                "delta": None,
            })

    return result


def _estimate_delta(premium_ratio) -> float:
    """基于溢价率估算 Delta"""
    if premium_ratio is None:
        return None
    try:
        pr = float(premium_ratio)
    except (ValueError, TypeError):
        return None

    if math.isnan(pr):
        return None

    # 分段表（与 convertible_bond_engine 保持一致）
    if pr >= 100: return 0.95
    if pr >= 50:  return 0.80
    if pr >= 20:  return 0.60
    if pr >= 10:  return 0.50
    if pr >= 5:   return 0.35
    if pr >= 0:   return 0.25
    return 0.15


# ============================================================
# 板块 4：风险预警
# ============================================================

def _section4_risk_warning(
    fund_name: str,
    max_dd: float,
    dd_date_str: str,
    recovery_days: int,
    defensive_ratio: float,
    max_dd_bm: float,
    cb_ratio: float,
    cb_analysis: dict,
    duration: float,
    wacs: float,
    rate_prediction: dict,
    stress_results: list,
    cb_value_df: Any = None,
) -> str:
    """板块 4：转债违约/赎回风险 + 强赎预警 + 全市场估值预警"""

    fund_dd_abs = abs(max_dd)
    bm_dd_abs   = abs(max_dd_bm)
    cb_pct      = cb_ratio * 100

    # 回撤表现
    if bm_dd_abs > 0:
        vs_bm = f"同期纯债基准回撤约 {bm_dd_abs:.2f}%，该基金为基准的 {defensive_ratio:.0%}"
    else:
        vs_bm = "该基金回撤表现"

    # 修复评价
    if recovery_days > 0:
        if recovery_days <= 30:
            recovery_text = f"**{recovery_days} 个交易日**完成修复，回血速度**极快**"
        elif recovery_days <= 60:
            recovery_text = f"**{recovery_days} 个交易日**完成修复，回血速度**较快**"
        elif recovery_days <= 120:
            recovery_text = f"约 **{recovery_days // 22} 个月**完成修复，回血速度**一般**"
        else:
            recovery_text = f"超过 **{recovery_days // 22} 个月**，回血速度**偏慢**"
    else:
        recovery_text = "统计区间内尚未完全修复至前高"

    # 转债风险分析
    avg_premium = cb_analysis.get("avg_premium", None)
    cb_count = cb_analysis.get("cb_count", 0)
    high_prem_count = cb_analysis.get("high_prem_count", 0)

    # 1. 信用评级分布（从 bond_details 推断）
    credit_risk_text = _analyze_cb_credit_risk(cb_analysis)

    # 2. 强赎风险预警
    redemption_text = _analyze_redemption_risk(cb_analysis)

    # 3. 转债估值压缩风险
    premium_risk_text = ""
    if avg_premium is not None:
        if avg_premium > 35:
            premium_risk_text = (
                f"\n\n**转债估值压缩风险（高）：**\n\n"
                f"当前平均溢价率 **{avg_premium:.1f}%**，处于历史偏高水位。\n"
                f"高溢价转债期权价值占比较大，正股下跌或情绪转弱时，"
                f"溢价率会快速压缩，转债跌幅可能远超纯债部分。\n\n"
                f"**历史教训：** 2022年转债市场整体回调，平均溢价率从40%+压缩至20%左右，"
                f"高溢价转债跌幅普遍超过15%。"
            )
        elif avg_premium > 25:
            premium_risk_text = (
                f"\n\n**转债估值压缩风险（中）：**\n\n"
                f"平均溢价率 **{avg_premium:.1f}%**，处于中等偏高水位。\n"
                f"若市场情绪转弱，溢价率有10-15个百分点的压缩空间，"
                f"对应转债价格约5-8%的下跌风险。"
            )
        else:
            premium_risk_text = (
                f"\n\n**转债估值压缩风险（低）：**\n\n"
                f"平均溢价率 **{avg_premium:.1f}%**，处于合理或偏低区间。\n"
                f"转债定价较便宜，下行空间有限，估值压缩风险不大。"
            )

    # 4. 流动性风险
    liquidity_text = _analyze_liquidity_risk(cb_analysis, cb_ratio)

    # 压力测试
    stress_text = ""
    if stress_results:
        stress_text = "\n\n**压力测试结果（利率+信用冲击）：**\n\n"
        stress_text += "| 情景 | 10Y利率(bp) | 信用利差(bp) | 预计净值影响 |\n"
        stress_text += "| --- | --- | --- | --- |\n"
        for s in stress_results:
            impact = s.get("price_impact", 0)
            stress_text += (
                f"| {s['scenario']} | {s.get('long_bp', 0):+.0f} | "
                f"{s.get('credit_bp', 0):+.0f} | {impact:.2f}% |\n"
            )

        # 叠加转债冲击
        cb_shock = cb_pct * 0.08  # 假设转债跌 8%
        stress_text += (
            f"\n> **注：** 以上仅覆盖利率冲击。若叠加转债市场回调 8%，"
            f"转债部分（{cb_pct:.1f}%）将带来额外约 **{cb_shock:.2f}%** 净值损失。"
        )

    # 利率环境
    rate_text = ""
    direction = rate_prediction.get("direction", "sideways")
    confidence = rate_prediction.get("confidence", 0.3)
    forecast = rate_prediction.get("y10y_forecast", {})
    current_rate = forecast.get("current", 0)
    mid_rate = forecast.get("mid_term", 0)

    if confidence >= 0.5:
        dir_map = {"up": "上行", "down": "下行", "sideways": "震荡"}
        dir_cn = dir_map.get(direction, "震荡")
        rate_text = (
            f"\n\n**利率环境研判：** 10Y 国债预计从 **{current_rate:.2f}%** "
            f"{dir_cn}至 **{mid_rate:.2f}%**"
            f"（置信度 {int(confidence*100)}%）。"
        )
        if direction == "down":
            rate_text += " 利率下行有利于债券和转债，是利好信号。"
        elif direction == "up" and duration >= 3:
            rate_text += f" 利率上行对久期 {duration:.1f} 年的债券端不利。"
    else:
        rate_text = "\n\n**利率环境研判：** 当前预测置信度不足，建议结合政策面判断。"

    # 综合风险等级
    risk_factors = 0
    if fund_dd_abs > 3: risk_factors += 1
    if avg_premium is not None and avg_premium > 30: risk_factors += 1
    if high_prem_count > cb_count * 0.3: risk_factors += 1
    # 全市场估值预警
    if cb_value_df is not None and not cb_value_df.empty and "转股溢价率" in cb_value_df.columns:
        latest_prem = cb_value_df["转股溢价率"].dropna()
        if not latest_prem.empty and float(latest_prem.iloc[-1]) > 40:
            risk_factors += 1

    if risk_factors >= 2:
        overall_risk = "**中高风险**"
    elif risk_factors == 1:
        overall_risk = "**中等风险**"
    else:
        overall_risk = "**低风险**"

    return f"""### 转债违约/赎回风险预警

混合一级债基同时面临纯债端的利率/信用风险和转债端的估值/违约/赎回风险。
下面系统梳理三大风险维度。

**历史回撤表现：**

| 指标 | 数值 | 评价 |
| --- | --- | --- |
| 最大回撤 | {fund_dd_abs:.2f}% | {vs_bm} |
| 回撤发生 | {dd_date_str or '未知'} | |
| 修复耗时 | {recovery_text} | |
| 久期 | {duration:.1f} 年 | {'敏感度低' if duration < 2 else '敏感度中等' if duration < 4 else '敏感度高'} |
| WACS信用 | {int(wacs)} 分 | {'高等级' if wacs >= 75 else '中等' if wacs >= 55 else '偏低'} |

[INSERT_CHART: DRAWDOWN]

#### 1. 转债信用评级分布
{credit_risk_text}

#### 2. 强赎/到期风险预警
{redemption_text}

#### 3. 估值压缩风险
{premium_risk_text}

#### 4. 全市场转债估值预警
{_build_market_valuation_warning(cb_value_df)}

#### 5. 流动性风险
{liquidity_text}
{stress_text}{rate_text}

**综合风险等级：{overall_risk}**"""


def _build_market_valuation_warning(cb_value_df: Any) -> str:
    """
    基于全市场转债估值数据生成预警。
    数据来源：bond_zh_cov_value_analysis（bond_zh_cov_value_analysis）
    
    当全市场平均转股溢价率 > 40% 时触发高预警；
    当全市场平均转股溢价率 > 30% 时触发中等预警。
    """
    if cb_value_df is None or not hasattr(cb_value_df, "empty") or cb_value_df.empty:
        return "全市场转债估值数据暂时不可用。"

    # 取最新一行数据
    latest = cb_value_df.iloc[-1]
    latest_date = latest.get("日期", "")
    if hasattr(latest_date, "strftime"):
        latest_date = latest_date.strftime("%Y-%m-%d")

    # 转股溢价率（核心指标）
    conv_prem = latest.get("转股溢价率", None)
    bond_prem = latest.get("纯债溢价率", None)

    # 历史分位数计算
    if "转股溢价率" in cb_value_df.columns:
        prem_series = cb_value_df["转股溢价率"].dropna()
        if len(prem_series) > 60:
            current = float(prem_series.iloc[-1])
            p50 = float(prem_series.quantile(0.5))
            p75 = float(prem_series.quantile(0.75))
            p90 = float(prem_series.quantile(0.9))
            percentile = (prem_series < current).sum() / len(prem_series)
        else:
            current = p50 = p75 = p90 = None
            percentile = None
    else:
        current = p50 = p75 = p90 = None
        percentile = None

    if current is None:
        return f"截至 {latest_date}，全市场转债估值数据暂不完整。"

    # 判断预警级别
    if current > 40:
        level = "**高预警**"
        advice = (
            "当前可转债市场估值**偏高**，一级债基的增强策略性价比正在下降。\n"
            "建议暂时回避转债仓位过高的混合一级债基，或等溢价率回落至 25% 以下再考虑入场。"
        )
    elif current > 30:
        level = "**中等预警**"
        advice = (
            "当前可转债市场估值处于**中等偏高**区间，转债增强的性价比有所下降。\n"
            "关注溢价率变化趋势，若继续上升建议降低对转债增强收益的预期。"
        )
    elif current > 20:
        level = "**正常偏低**"
        advice = (
            "当前可转债市场估值处于**合理区间**，转债增强策略具备较好的性价比。\n"
            "混合一级债基可通过精选个券获取超额收益。"
        )
    else:
        level = "**低估区间**"
        advice = (
            "当前可转债市场估值处于**历史偏低区间**，转债具备较高的安全边际。\n"
            "这是配置混合一级债基的有利窗口期。"
        )

    # 构建统计信息
    stats_text = f"| 指标 | 数值 |\n| --- | --- |\n"
    stats_text += f"| 数据日期 | {latest_date} |\n"
    stats_text += f"| 全市场平均转股溢价率 | **{current:.1f}%** |\n"
    if bond_prem is not None and not (isinstance(bond_prem, float) and math.isnan(bond_prem)):
        stats_text += f"| 全市场平均纯债溢价率 | {float(bond_prem):.1f}% |\n"
    if percentile is not None:
        stats_text += f"| 历史分位数 | **{percentile:.0%}** |\n"
        stats_text += f"| 历史中位数 | {p50:.1f}% |\n"
        stats_text += f"| 75 分位 | {p75:.1f}% |\n"
        stats_text += f"| 90 分位 | {p90:.1f}% |\n"

    return (
        f"{stats_text}\n\n"
        f"**估值水位：{level}**\n\n{advice}"
    )


def _analyze_cb_credit_risk(cb_analysis: dict) -> str:
    """分析转债信用评级分布"""
    top_holdings = cb_analysis.get("top_holdings", [])

    if not top_holdings:
        return "转债持仓信用评级数据正在加载中。"

    total = len(top_holdings)
    if total == 0:
        return "当前无转债持仓明细。"

    return (
        f"共持有 **{total}** 只转债。"
        f"\n\n> **注意：** 精确的信用评级需要从 `bond_zh_cov_info` 接口获取 `BOND_REMAIN`（剩余期限）、"
        f"`COUPON_RATE`（票面利率）等字段综合判断。"
        f"当前版本基于持仓明细做保守分析。"
    )


def _analyze_redemption_risk(cb_analysis: dict) -> str:
    """分析强赎/到期风险"""
    top_holdings = cb_analysis.get("top_holdings", [])
    cb_count = cb_analysis.get("cb_count", 0)

    if cb_count == 0:
        return "当前无转债持仓。"

    # 混合一级债基的转债赎回风险分析
    text = (
        f"**强赎预警分析：**\n\n"
        f"当正股价格连续 N 天 ≥ 转股价 × 130% 时，发行人有权强制赎回转债。"
        f"此时投资者面临：\n"
        f"- **立即转股**：享受转股收益，但要承担正股后续下跌风险\n"
        f"- **被赎回**：获得面值+利息，但放弃了转股溢价\n\n"
        f"对混合一级债基来说，强赎意味着持仓转债需要被动处置，"
        f"可能被迫在高位转股或接受低收益赎回。"
    )

    # 高溢价转债强赎风险更大
    high_prem = cb_analysis.get("high_prem_count", 0)
    if high_prem > 0:
        text += (
            f"\n\n当前有 **{high_prem} 只**高溢价转债（溢价率 > 30%），"
            f"若正股持续上涨触发强赎，这些转债将面临估值归零的风险。"
            f"需关注基金经理是否及时处置了强赎转债。"
        )

    # 到期提醒
    text += (
        f"\n\n**到期提醒：** 转债到期时可以选择转股或兑付。"
        f"即将到期的转债需要经理提前规划处置方案。"
    )

    return text


def _analyze_liquidity_risk(cb_analysis: dict, cb_ratio: float) -> str:
    """分析流动性风险"""
    cb_count = cb_analysis.get("cb_count", 0)
    top_holdings = cb_analysis.get("top_holdings", [])

    if cb_count == 0:
        return ""

    # 集中度分析
    if top_holdings:
        total_ratio = sum(h.get("ratio", 0) for h in top_holdings[:5])
        top5_share = total_ratio / cb_ratio * 100 if cb_ratio > 0 else 0

        if top5_share > 50:
            text = (
                f"\n\n**流动性集中度：** Top 5 转债占转债仓位的 **{top5_share:.0f}%**，"
                f"持仓较为集中。个别转债流动性问题可能对组合产生较大冲击。"
            )
        else:
            text = (
                f"\n\n**流动性集中度：** Top 5 转债占转债仓位的 **{top5_share:.0f}%**，"
                f"持仓分散度较好，个别转债流动性问题对组合影响有限。"
            )
        return text

    return ""


# ============================================================
# 板块 5：投资建议
# ============================================================

def _section5_advice(
    fund_name: str,
    grade: str,
    ann_ret: float,
    max_dd: float,
    sharpe: float,
    calmar: float,
    cb_ratio: float,
    cb_analysis: dict,
    duration: float,
    wacs: float,
    rate_prediction: dict,
    basic: Any,
    stress_results: list,
) -> str:
    """板块 5：拟购入 / 已持有 投资建议"""

    fund_dd_abs = abs(max_dd)
    cb_pct = cb_ratio * 100
    avg_premium = cb_analysis.get("avg_premium", None)

    # ── 拟购入评估 ──
    buy_checks = []

    # 收益维度
    if ann_ret >= 5:
        buy_checks.append(("OK", f"年化收益 {ann_ret:.2f}%，收益水平优良"))
    elif ann_ret >= 3:
        buy_checks.append(("OK", f"年化收益 {ann_ret:.2f}%，收益水平适中"))
    elif ann_ret >= 1:
        buy_checks.append(("WARN", f"年化收益 {ann_ret:.2f}%，收益水平偏低"))
    else:
        buy_checks.append(("BAD", f"年化收益 {ann_ret:.2f}%，收益不达预期"))

    # 回撤维度
    if fund_dd_abs < 1:
        buy_checks.append(("OK", f"最大回撤 {fund_dd_abs:.2f}%，回撤控制极优"))
    elif fund_dd_abs < 2.5:
        buy_checks.append(("OK", f"最大回撤 {fund_dd_abs:.2f}%，回撤控制良好"))
    elif fund_dd_abs < 5:
        buy_checks.append(("WARN", f"最大回撤 {fund_dd_abs:.2f}%，回撤中等"))
    else:
        buy_checks.append(("BAD", f"最大回撤 {fund_dd_abs:.2f}%，回撤较大"))

    # 夏普维度
    if sharpe >= 1.5:
        buy_checks.append(("OK", f"夏普比率 {sharpe:.2f}，风险收益效率极佳"))
    elif sharpe >= 0.8:
        buy_checks.append(("OK", f"夏普比率 {sharpe:.2f}，风险收益效率良好"))
    elif sharpe >= 0.3:
        buy_checks.append(("WARN", f"夏普比率 {sharpe:.2f}，风险收益效率一般"))
    else:
        buy_checks.append(("BAD", f"夏普比率 {sharpe:.2f}，风险收益效率偏低"))

    # 信用质量
    if wacs >= 75:
        buy_checks.append(("OK", f"WACS信用评分 {int(wacs)}，持仓信用资质优良"))
    elif wacs >= 55:
        buy_checks.append(("WARN", f"WACS信用评分 {int(wacs)}，信用资质中等"))
    else:
        buy_checks.append(("WARN", f"WACS信用评分 {int(wacs)}，信用资质偏低需关注"))

    # 转债估值
    if avg_premium is not None:
        if avg_premium <= 20:
            buy_checks.append(("OK", f"平均溢价率 {avg_premium:.1f}%，转债估值合理"))
        elif avg_premium <= 35:
            buy_checks.append(("WARN", f"平均溢价率 {avg_premium:.1f}%，转债估值偏高"))
        else:
            buy_checks.append(("BAD", f"平均溢价率 {avg_premium:.1f}%，转债估值过高"))
    else:
        buy_checks.append(("-", "转债溢价率数据暂不可用"))

    green_count = sum(1 for s, _ in buy_checks if s == "OK")
    yellow_count = sum(1 for s, _ in buy_checks if s == "WARN")

    if green_count >= 4 and yellow_count == 0:
        buy_verdict = "**强烈推荐购入** — 各项指标优秀，转债增强效果显著"
    elif green_count >= 3 and yellow_count <= 1:
        buy_verdict = "**推荐购入** — 整体表现良好，转债估值合理"
    elif green_count >= 2:
        buy_verdict = "**可考虑购入** — 部分指标存在瑕疵，建议分批建仓"
    else:
        buy_verdict = "**建议观望** — 多项指标存在风险信号"

    # ── 已持有诊断 ──
    hold_checks = []

    if ann_ret >= 3:
        hold_checks.append("收益表现正常，继续持有")
    elif ann_ret >= 0:
        hold_checks.append("收益偏低但仍有正收益，可继续观察")
    else:
        hold_checks.append("收益为负，需关注原因")

    if fund_dd_abs < 2:
        hold_checks.append("回撤可控，持有体验良好")
    elif fund_dd_abs < 5:
        hold_checks.append("回撤幅度可接受，关注修复速度")
    else:
        hold_checks.append("回撤较大，评估是否需要止损或减仓")

    if avg_premium is not None:
        if avg_premium <= 25:
            hold_checks.append("转债估值合理，可安心持有")
        elif avg_premium <= 40:
            hold_checks.append("转债估值偏高，关注正股走势")
        else:
            hold_checks.append("转债估值过高，建议关注减仓时机")

    if stress_results:
        worst = min(stress_results, key=lambda x: x.get("price_impact", 0))
        worst_impact = abs(worst.get("price_impact", 0))
        if worst_impact < 2:
            hold_checks.append("压力测试通过，抗冲击能力良好")
        elif worst_impact < 5:
            hold_checks.append("压力测试中等，极端情况下可能亏损较大")
        else:
            hold_checks.append("压力测试较差，极端冲击下损失较大")

    # ── 离场信号 ──
    exit_signals = []
    if fund_dd_abs > 5:
        exit_signals.append(f"最大回撤超过 5%（当前 {fund_dd_abs:.2f}%）")
    if avg_premium is not None and avg_premium > 50:
        exit_signals.append(f"转债溢价率过高（{avg_premium:.1f}%）")
    if wacs < 40:
        exit_signals.append(f"WACS信用评分偏低（{int(wacs)}分）")
    if ann_ret < -1:
        exit_signals.append(f"近一年收益为负（{ann_ret:.2f}%）")
    if calmar < 0:
        exit_signals.append("卡玛比率为负，风险收益严重失衡")

    if not exit_signals:
        exit_text = "当前未检测到明显离场信号，可安心持有。"
    else:
        exit_text = "检测到以下风险信号，需密切关注：\n\n" + "\n".join(
            f"- **{s}**" for s in exit_signals
        )
        exit_text += "\n\n> 出现 **2 个及以上** 信号时，建议考虑减仓或离场。"

    # 组装
    buy_section = " | ".join(f"{t}" for s, t in buy_checks)
    hold_section = "\n".join(f"- {t}" for t in hold_checks)

    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0

    return f"""### 投资建议

#### 拟购入评估

{buy_verdict}

**诊断清单：**

{buy_section}

**配置建议：**
- 混合一级债基适合作为**固收增强**配置，建议占固收组合的 **20%~40%**
- **择时建议：** 优先在转债估值低位（中证转债溢价率中位数 < 25%）时建仓
- 若当前转债溢价率偏高，可等待估值回归后再入场

#### 已持有诊断

{hold_section}

#### 离场信号监测

{exit_text}

---

**成本项披露：**

- 管理费率：{mgmt_fee:.2f}%
- 托管费率：{custody_fee:.2f}%
- 综合评级：{grade}

> **风险提示：** 以上分析基于历史数据，不构成投资建议。转债部分的权益属性会增加净值波动，
> 投资者应关注转债估值水位和正股市场走势，根据自身风险承受能力做出决策。
"""


# ============================================================
# 辅助函数
# ============================================================

def _build_headline(
    fund_name: str,
    cb_ratio: float,
    grade: str,
    start_date: str,
    end_date: str,
) -> str:
    """报告标题行"""
    cb_pct = cb_ratio * 100
    return (
        f"## {fund_name} — 混合一级债基分析\n\n"
        f"**统计区间：** {start_date} ~ {end_date}\n\n"
        f"**核心标签：** 债底保护 | 转债增强({cb_pct:.1f}%) | 信用打底 | 攻守兼备\n\n"
    )


def _get_rate_prediction() -> dict:
    """获取利率预测"""
    try:
        return predict_rate_trend(horizon="3m")
    except Exception as e:
        logger.warning(f"[bond_mixed1_report_writer] 获取利率预测失败: {e}")
        return {
            "direction": "sideways",
            "confidence": 0.3,
            "y10y_forecast": {"current": 2.5, "mid_term": 2.5},
            "key_factors": ["数据不足"],
            "risk_signals": [],
        }


def _extract_date_range(charts: dict) -> tuple[str, str]:
    """从图表数据中提取日期范围"""
    cum_data = charts.get("cumulative_return", {})
    x_list = cum_data.get("x", [])
    if x_list:
        start = _format_date(x_list[0])
        end   = _format_date(x_list[-1])
        return start, end
    return "数据起始", "最新"


def _format_date(d) -> str:
    """日期格式化"""
    if d is None:
        return ""
    try:
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d")
        s = str(d)
        return s[:10]
    except Exception:
        return str(d)


def _fallback_report(basic) -> dict:
    """数据不足时的兜底"""
    name = getattr(basic, "name", "该基金")
    text = (
        f"## {name} — 数据不足，无法生成深度报告\n\n"
        f"当前统计周期内有效数据不足，请尝试延长分析周期或稍后重试。"
    )
    return {
        "headline": text,
        "section1": "",
        "section2": "",
        "section3": "",
        "section4": "",
        "section5": "",
    }
