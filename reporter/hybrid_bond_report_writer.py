"""
混合型-偏债基金深度评价报告生成器 — fund_quant_v2
报告结构：5板块 + 图表插入点标记 + 投资建议

板块设计：
1. 收益情况展示 + 稳健性表现解读（夏普比率）
2. 收益分析 + 收益归因模型（股/债/转债饼图 + 各资产类别收益贡献）
3. 深度分析 + 债券持仓分析（券种分布、久期、利率敏感度）
4. 风险预警 + 股票仓位穿透（合同红线监测）+ 回撤情况与修复天数
5. 投资建议（拟买入 / 持有中 / 离场信号）
"""

from __future__ import annotations
from typing import Any
import logging

from data_loader.rate_prediction import predict_rate_trend

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_hybrid_bond_report(report: Any) -> dict:
    """
    生成混合型-偏债基金深度评价报告（5板块结构）

    Args:
        report: 基金报告数据（FundReport，含 bond_metrics + chart_data）

    Returns:
        {
          "meta":     {fund_name, fund_type, start_date, end_date, grade, score},
          "headline": 标题行,
          "section1": 收益情况展示（含 [INSERT_CHART: CUM_RET]）,
          "section2": 收益归因分析（含 [INSERT_CHART: ASSET_ALLOCATION_PIE]）,
          "section3": 债券持仓深度分析（含 [INSERT_CHART: BOND_HOLDINGS_PIE]）,
          "section4": 风险预警（含 [INSERT_CHART: DRAWDOWN]）,
          "section5": 投资建议（拟买入/持有中/离场信号）,
          "full_text": 完整纯文本
        }
    """
    basic = report.basic
    m = report.bond_metrics
    charts = report.chart_data

    if not m:
        return _fallback_report(basic)

    # ── 提取核心数据 ──────────────────────────────────────
    cm = m.common
    fund_name = basic.name
    grade = m.score_grade
    score = m.overall_score

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 收益数据
    ann_ret    = round(cm.annualized_return * 100, 2)
    cum_ret    = round(cm.cumulative_return * 100, 1)
    volatility = round(cm.volatility * 100, 2)
    sharpe     = round(cm.sharpe_ratio, 2)
    sortino    = round(cm.sortino_ratio, 2)
    calmar     = round(cm.calmar_ratio, 2)

    # 基准数据
    bm_info    = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm     = round(bm_info.get("bm_last_return", 0) * 100, 1)
    ann_bm     = round(bm_info.get("bm_annual_return", 0) * 100, 2)
    excess_bps = round((ann_ret - ann_bm) * 100, 0)

    # 风险数据
    max_dd        = round(cm.max_drawdown * 100, 2)
    recovery_days = cm.recovery_days or 0

    # 月度胜率
    hm_info       = charts.get("monthly_heatmap", {}).get("heatmap_info", {})
    monthly_stats = hm_info.get("monthly_stats", {})
    total_months  = monthly_stats.get("total_months", 0)
    pos_months    = monthly_stats.get("positive_months", 0)
    pos_rate      = round(pos_months / total_months * 100, 1) if total_months > 0 else 50.0

    # 回撤信息
    dd_info       = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm     = round(dd_info.get("bm_max_dd", 0), 2)
    defensive_ratio = dd_info.get("defensive_ratio", 1.0)
    recovery_info = dd_info.get("recovery_info", {})
    recovery_days_chart = recovery_info.get("recovery_days", recovery_days) or recovery_days
    dd_date_str   = _format_date(recovery_info.get("max_dd_date"))

    # 固收专项数据
    duration   = round(m.duration, 1)
    wacs_score = round(m.wacs_score, 0)
    alpha_bond = round(m.alpha_bond * 100, 2) if hasattr(m, 'alpha_bond') else 0.0

    # 资产配置数据（偏债混合型核心）
    holdings = charts.get("holdings", {})
    stock_ratio = holdings.get("stock_ratio", 0.0)
    bond_ratio  = holdings.get("bond_ratio", 0.0)
    cash_ratio  = holdings.get("cash_ratio", 0.0)
    cb_ratio    = holdings.get("cb_ratio", 0.0)
    other_ratio = max(0, 1.0 - stock_ratio - bond_ratio - cash_ratio - cb_ratio)

    # 债券持仓分类
    bond_classification = getattr(m, 'bond_classification', {}) or {}

    # 压力测试
    stress_results = m.stress_results or []

    # 利率预测
    rate_prediction = _get_rate_prediction()

    # 综合评级描述
    grade_desc = {
        "A+": "顶尖配置级偏债资产",
        "A":  "优质稳健偏债品种",
        "B":  "中等水平偏债品种",
        "C":  "偏弱，需关注风险控制",
        "D":  "弱势，不建议持有",
    }.get(grade, "偏债基金品种")

    # ── 生成各章节 ─────────────────────────────────────
    headline = _build_headline(fund_name, start_date, end_date)

    section1 = _section1_performance(
        fund_name, ann_ret, ann_bm, excess_bps, cum_ret, cum_bm,
        sharpe, sortino, volatility, pos_rate, total_months, pos_months,
        start_date, end_date
    )

    section2 = _section2_attribution(
        fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
        ann_ret, duration
    )

    section3 = _section3_bond_deep(
        fund_name, bond_classification, duration, wacs_score,
        alpha_bond, stress_results
    )

    section4 = _section4_risk_warning(
        fund_name, stock_ratio, max_dd, max_dd_bm, dd_date_str,
        recovery_days_chart, defensive_ratio, duration,
        rate_prediction, stress_results
    )

    section5 = _section5_investment_advice(
        fund_name, grade, grade_desc, sharpe, calmar, max_dd,
        duration, stock_ratio, ann_ret, rate_prediction, basic
    )

    full_text = "\n\n".join([
        headline, section1, section2, section3, section4, section5
    ])

    return {
        "meta": {
            "fund_name": fund_name,
            "fund_type": "混合型-偏债",
            "start_date": start_date,
            "end_date": end_date,
            "grade": grade,
            "score": score,
        },
        "headline":   headline,
        "section1":   section1,
        "section2":   section2,
        "section3":   section3,
        "section4":   section4,
        "section5":   section5,
        "full_text":  full_text,
    }


# ============================================================
# 板块 1：收益情况展示 + 稳健性解读
# ============================================================

def _section1_performance(
    fund_name, ann_ret, ann_bm, excess_bps, cum_ret, cum_bm,
    sharpe, sortino, volatility, pos_rate, total_months, pos_months,
    start_date, end_date
) -> str:
    """第一章：收益情况展示 + 稳健性表现解读"""

    bm_label = "中债-综合财富指数"
    bm_excess_sign = "跑赢" if excess_bps > 0 else "落后于"
    bps_abs = abs(int(excess_bps))

    # 夏普比率解读
    if sharpe >= 2.0:
        sharpe_comment = f"夏普比率 **{sharpe:.2f}**，承担1单位风险能换回来超过2单位收益，这水平相当顶尖。"
    elif sharpe >= 1.0:
        sharpe_comment = f"夏普比率 **{sharpe:.2f}**，风险收益性价比不错，适合做组合的底仓。"
    elif sharpe >= 0.5:
        sharpe_comment = f"夏普比率 **{sharpe:.2f}**，中等水平，作为「固收+」策略来说，超额收益的稳定性还有提升空间。"
    else:
        sharpe_comment = f"夏普比率只有 **{sharpe:.2f}**，承担了波动但没赚到多少超额收益，性价比不高。"

    # 稳健性解读（波动率 + 月度胜率）
    if volatility <= 1.5 and pos_rate >= 80:
        stability_desc = (
            f"年化波动率只有 **{volatility:.2f}%**，"
            f"过去 {total_months} 个月里 {pos_rate:.0f}% 都是正收益，"
            f"持有体验非常好。这种「低波动+高胜率」就是偏债混合基金作为「理财替代品」的底气。"
        )
    elif volatility <= 3.0 and pos_rate >= 70:
        stability_desc = (
            f"年化波动率 **{volatility:.2f}%**，月度胜率 **{pos_rate:.0f}%**，"
            f"整体走势稳健，偶尔有阶段性回撤但修复快，持有体验不错。"
        )
    else:
        stability_desc = (
            f"年化波动率达 **{volatility:.2f}%**，月度胜率 **{pos_rate:.0f}%**，"
            f"波动有点偏权益化了。买偏债基金图的就是稳，这个波动水平需要掂量一下。"
        )

    # 索提诺比率补充
    sortino_comment = ""
    if sortino >= 2.0:
        sortino_comment = f"\n\n索提诺比率 **{sortino:.2f}**，下行风险控制非常出色，经理有很强的「守门员」意识。"

    return (
        f"### 一、收益表现\n\n"
        f"偏债混合基金的定位是「**比纯债多一层弹性，比偏股少一层恐惧**」。"
        f"我们用 {bm_label} 做基准，看看 {fund_name} 表现如何。\n\n"
        f"**核心数据：**\n"
        f"- 年化收益率：**{ann_ret:.2f}%**（基准 {ann_bm:.2f}%，{bm_excess_sign} **{bps_abs} bps**）\n"
        f"- 区间累计收益：**{cum_ret:.1f}%**（基准 {cum_bm:.1f}%）\n"
        f"- 年化波动率：**{volatility:.2f}%**\n"
        f"- 月度胜率：**{pos_rate:.0f}%**（{total_months} 个月中 {pos_months} 个月正收益）\n\n"
        f"[INSERT_CHART: CUM_RET]\n\n"
        f"**稳健性解读：**\n\n"
        f"{sharpe_comment}\n\n"
        f"{stability_desc}"
        f"{sortino_comment}"
    )


# ============================================================
# 板块 2：收益归因分析（股/债/转债饼图 + 收益贡献拆解）
# ============================================================

def _section2_attribution(
    fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    ann_ret, duration
) -> str:
    """第二章：收益归因模型"""

    sr = stock_ratio * 100
    br = bond_ratio * 100
    cr = cash_ratio * 100
    cb_r = cb_ratio * 100
    or_ = max(0, 100 - sr - br - cr - cb_r)

    # 资产配置结构评价
    if sr <= 10:
        allocation_comment = (
            f"股票仓位只有 {sr:.1f}%，基本是「纯债增强型」，"
            f"收益弹性有限，但回撤控制非常强。适合追求绝对收益的底仓配置。"
        )
    elif sr <= 20:
        allocation_comment = (
            f"股票仓位 {sr:.1f}% 是偏债混合的典型水平，"
            f"债券打底 + 适度权益增强，稳健和收益兼顾。"
        )
    elif sr <= 30:
        allocation_comment = (
            f"股票仓位 {sr:.1f}% 偏高了，快赶上混合平衡型了，"
            f"净值波动会比同类偏债基金大不少，要关注经理的择时和风控能力。"
        )
    else:
        allocation_comment = (
            f"股票仓位高达 {sr:.1f}%，这已经不像「偏债」了。"
            f"实际风格可能更接近平衡混合型，买之前要想清楚自己能不能承受这个波动。"
        )

    # 转债评价
    cb_comment = ""
    if cb_ratio > 0.05:
        if cb_ratio <= 0.15:
            cb_comment = (
                f"\n\n可转债配了 {cb_r:.1f}%，是个不错的弹性工具——"
                f"股市涨的时候能跟着涨，跌的时候有债底保护。"
            )
        else:
            cb_comment = (
                f"\n\n可转债配了 {cb_r:.1f}%，挺高的。"
                f"转债估值偏高的时候，所谓的「债底保护」可能靠不住。"
            )

    # 收益贡献估算（简化模型）
    bond_contribution = bond_ratio * 3.5
    stock_contribution = stock_ratio * 8.0
    cash_contribution = cash_ratio * 1.5
    other_contribution = ann_ret - bond_contribution - stock_contribution - cash_contribution

    contribution_text = (
        f"**收益贡献估算：**\n\n"
        f"| 资产类别 | 仓位占比 | 估算贡献 | 说明 |\n"
        f"|---------|---------|---------|------|\n"
        f"| 债券 | {br:.1f}% | ≈ {bond_contribution:.2f}% | 票息 + 久期管理 |\n"
        f"| 股票 | {sr:.1f}% | ≈ {stock_contribution:.2f}% | 资本利得 + 分红 |\n"
    )
    if cb_ratio > 0.01:
        contribution_text += f"| 可转债 | {cb_r:.1f}% | 含于债券/股票 | 混合收益来源 |\n"
    contribution_text += (
        f"| 现金 | {cr:.1f}% | ≈ {cash_contribution:.2f}% | 货币市场收益 |\n"
        f"| **其他/Alpha** | — | ≈ {other_contribution:+.2f}% | **选券/择时超额** |\n"
    )

    if other_contribution > 0.5:
        contribution_text += (
            f"\n\n经理通过主动管理多赚了约 **{other_contribution:.2f}%**，"
            f"选券和择时能力还可以。"
        )
    elif other_contribution < -0.5:
        contribution_text += (
            f"\n\n经理的主动管理反而拖累了约 **{abs(other_contribution):.2f}%**，"
            f"超额收益为负，要想想是不是策略出了问题。"
        )

    return (
        f"### 二、收益归因\n\n"
        f"偏债混合的收益来源可以拆成「**债券底仓 + 股票增强 + 现金管理**」三块。"
        f"下面来看看每块贡献了多少。\n\n"
        f"[INSERT_CHART: ASSET_ALLOCATION_PIE]\n\n"
        f"**资产配置结构：**\n\n"
        f"{allocation_comment}"
        f"{cb_comment}\n\n"
        f"{contribution_text}"
    )


# ============================================================
# 板块 3：债券持仓深度分析（券种、久期、利率敏感度）
# ============================================================

def _section3_bond_deep(
    fund_name, bond_classification, duration, wacs_score,
    alpha_bond, stress_results
) -> str:
    """第三章：债券持仓深度分析"""

    # 券种分布
    if bond_classification:
        gov_ratio  = bond_classification.get('gov_bond', {}).get('ratio', 0) * 100
        credit_ratio = bond_classification.get('credit_bond', {}).get('ratio', 0) * 100
        urban_ratio = bond_classification.get('urban_construction', {}).get('ratio', 0) * 100
        estate_ratio = bond_classification.get('real_estate', {}).get('ratio', 0) * 100

        bond_pie_text = (
            f"[INSERT_CHART: BOND_HOLDINGS_PIE]\n\n"
            f"**券种分布：**\n\n"
            f"- **利率债**（国债、政金债）：{gov_ratio:.1f}% — 最安全，票息确定\n"
            f"- **信用债**（企业债、公司债）：{credit_ratio:.1f}% — 收益增强来源\n"
            f"- **城投债**：{urban_ratio:.1f}% — 区域性债务\n"
            f"- **地产债**：{estate_ratio:.1f}% — 跟房地产周期走\n"
        )

        # 信用结构分析
        credit_analysis = []
        if gov_ratio > 60:
            credit_analysis.append("利率债占比超六成，信用风险极低，组合很安全")
        elif gov_ratio > 30:
            credit_analysis.append("利率债占比三成以上，有一定防御性，同时用信用债增厚收益")
        else:
            credit_analysis.append("利率债占比较低，主要靠信用下沉赚超额，要关注信用风险")
        if urban_ratio > 10:
            credit_analysis.append(f"城投债占比 {urban_ratio:.1f}%，关注地方债务化解进展")
        if estate_ratio > 5:
            credit_analysis.append(f"地产债占比 {estate_ratio:.1f}%，盯紧房地产政策")
    else:
        bond_pie_text = "债券持仓数据暂不可用，建议稍后刷新查看。\n\n"
        credit_analysis = []

    # 久期分析
    if duration <= 2.0:
        dur_type = "短债型"
        dur_comment = f"组合久期约 **{duration:.1f} 年**，短债配置，对利率波动不敏感。加息100BP大概影响净值 {duration:.1f}%，防御力很强。"
    elif duration <= 4.0:
        dur_type = "中短债型"
        dur_comment = f"组合久期约 **{duration:.1f} 年**，中短端布局，利率下行时能吃到资本利得，加息100BP大概影响 {duration:.1f}%。"
    else:
        dur_type = "中长债型"
        dur_comment = f"组合久期约 **{duration:.1f} 年**，中长端配置，利率敏感度较高。加息100BP大概影响 {duration:.1f}%，要注意利率上行风险。"

    # WACS 信用评分
    if wacs_score >= 80:
        wacs_comment = f"持仓加权平均信用评分（WACS）**{int(wacs_score)} 分**，信用资质优良，违约风险极低。"
    elif wacs_score >= 60:
        wacs_comment = f"持仓加权平均信用评分（WACS）**{int(wacs_score)} 分**，信用资质中等，以AA级信用债为主。"
    else:
        wacs_comment = f"持仓加权平均信用评分（WACS）**{int(wacs_score)} 分**，信用资质偏低，存在信用下沉博弈。"

    # Alpha 解读
    alpha_comment = ""
    if alpha_bond > 0.3:
        alpha_comment = f"\n\n债券三因子回归显示，经理贡献了约 **{alpha_bond:.2f}%** 的年化纯Alpha，选券和择时有真功夫。"
    elif alpha_bond < -0.1:
        alpha_comment = f"\n\n债券三因子回归显示，年化Alpha为 **{alpha_bond:.2f}%**，超额收益主要来自系统性因子，主动管理没加分。"

    # 信用分析文本
    credit_text = ""
    if credit_analysis:
        credit_text = "\n\n**信用结构分析：**\n\n" + "\n".join(f"- {c}" for c in credit_analysis)

    return (
        f"### 三、债券持仓深度分析\n\n"
        f"对于偏债混合基金，债券底仓的质量直接决定了安全垫有多厚。\n\n"
        f"{bond_pie_text}"
        f"**久期分析：**\n\n"
        f"- 久期类型：{dur_type}（{duration:.1f} 年）\n"
        f"- {dur_comment}\n\n"
        f"**信用质量：**\n\n"
        f"- {wacs_comment}"
        f"{alpha_comment}"
        f"{credit_text}"
    )


# ============================================================
# 板块 4：风险预警（股票仓位穿透 + 回撤 + 修复天数）
# ============================================================

def _section4_risk_warning(
    fund_name, stock_ratio, max_dd, max_dd_bm, dd_date_str,
    recovery_days, defensive_ratio, duration,
    rate_prediction, stress_results
) -> str:
    """第四章：风险预警"""

    fund_dd_abs = abs(max_dd)
    sr = stock_ratio * 100

    # ---- 股票仓位穿透 ----
    redline_warnings = []
    if sr > 30:
        redline_warnings.append(
            f"股票仓位 {sr:.1f}% 已超过偏债混合型常规上限（30%），风格漂移风险较高"
        )
    elif sr > 20:
        redline_warnings.append(
            f"股票仓位 {sr:.1f}% 处于偏债混合的上限区间，市场急跌时回撤压力不小"
        )

    # 仓位趋势（如果有历史数据）
    stock_comment = ""
    if redline_warnings:
        stock_comment = "\n\n" + "\n".join(f"- {w}" for w in redline_warnings)
    else:
        stock_comment = (
            f"\n\n当前股票仓位 **{sr:.1f}%** 处于合理区间，暂无仓位越限风险。"
        )

    # ---- 回撤分析 ----
    dd_date_text = f"（发生于 {dd_date_str}）" if dd_date_str else ""

    if fund_dd_abs <= 1.0:
        dd_quality = "回撤控制极其优秀"
    elif fund_dd_abs <= 3.0:
        dd_quality = "回撤控制良好，正常水平"
    elif fund_dd_abs <= 5.0:
        dd_quality = "回撤控制一般，可能因为权益仓位偏高"
    else:
        dd_quality = "回撤控制偏弱，已超过偏债混合的常见区间"

    # 基准对比
    bm_dd_abs = abs(max_dd_bm)
    vs_bm_text = ""
    if bm_dd_abs > 0:
        vs_bm_text = (
            f"同期基准最大回撤 {bm_dd_abs:.2f}%，"
            f"该基金为基准回撤的 **{defensive_ratio:.0%}**，{dd_quality}。"
        )

    # ---- 修复天数 ----
    if recovery_days > 0:
        if recovery_days <= 15:
            recovery_comment = f"**{recovery_days} 个交易日**就修复了，修复速度很快。"
        elif recovery_days <= 45:
            recovery_comment = f"用了 **{recovery_days} 个交易日**修复，速度还可以。"
        elif recovery_days <= 90:
            recovery_comment = f"用了 **{recovery_days} 个交易日**修复，磨了一阵子。"
        else:
            recovery_comment = f"用了 **{recovery_days} 个交易日**才修复，修复比较慢，风控能力存疑。"
    else:
        recovery_comment = "统计区间内尚未完全修复至前高，或者回撤持续时间很短。"

    # ---- 利率环境影响 ----
    rate_comment = ""
    direction = rate_prediction.get("direction", "sideways")
    confidence = rate_prediction.get("confidence", 0.3)
    forecast = rate_prediction.get("y10y_forecast", {})
    current_rate = forecast.get("current", 0)
    mid_term_rate = forecast.get("mid_term", 0)

    if confidence >= 0.5:
        dir_map = {"up": "上行", "down": "下行", "sideways": "震荡"}
        dir_cn = dir_map.get(direction, "震荡")
        rate_comment = (
            f"\n\n**利率环境研判：**\n\n"
            f"技术指标预测未来3个月10Y国债收益率从 **{current_rate:.2f}%** {dir_cn}至 **{mid_term_rate:.2f}%**"
            f"（置信度 {int(confidence * 100)}%）。"
        )
        if direction == "up" and duration >= 3.0:
            rate_comment += (
                f"\n\n**利率上行风险**：该基金久期较长（{duration:.1f}年），"
                f"利率上行阶段可能面临较大亏损。"
            )
        elif direction == "down":
            rate_comment += (
                f"\n\n利率下行周期中，基金有望通过久期管理多吃到一些资本利得。"
            )

    # ---- 压力测试 ----
    stress_comment = ""
    if stress_results:
        worst = max(stress_results, key=lambda x: abs(x.get("price_impact", 0)))
        worst_impact = abs(worst.get("price_impact", 0))
        worst_scenario = worst.get("scenario", "")
        stress_comment = (
            f"\n\n**压力测试（四情景）：**\n\n"
            f"| 情景 | 预估净值影响 |\n"
            f"|------|------------|\n"
        )
        for s in stress_results:
            impact = s.get("price_impact", 0)
            stress_comment += f"| {s['scenario']} | {impact:+.2f}% |\n"
        stress_comment += (
            f"\n最不利情景「**{worst_scenario}**」下，预估净值下跌 **{worst_impact:.2f}%**。"
        )

    return (
        f"### 四、风险预警\n\n"
        f"**1. 股票仓位穿透**\n\n"
        f"偏债混合基金的合同通常对股票仓位有上限（常见30%或40%），仓位逼近红线意味着风格可能漂移了。"
        f"{stock_comment}\n\n"
        f"**2. 回撤与修复**\n\n"
        f"统计区间内，{fund_name} 最大回撤 **{fund_dd_abs:.2f}%**{dd_date_text}。"
        f"{vs_bm_text}\n\n"
        f"[INSERT_CHART: DRAWDOWN]\n\n"
        f"{recovery_comment}"
        f"{rate_comment}"
        f"{stress_comment}"
    )


# ============================================================
# 板块 5：投资建议（拟买入 / 持有中 / 离场信号）
# ============================================================

def _section5_investment_advice(
    fund_name, grade, grade_desc, sharpe, calmar, max_dd,
    duration, stock_ratio, ann_ret, rate_prediction, basic
) -> str:
    """第五章：投资建议"""

    # 成本披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0

    # ---- 拟买入 ----
    if grade in ("A+", "A"):
        buy_advice = (
            f"该基金综合评级 {grade}（{grade_desc}），夏普 {sharpe:.2f}，最大回撤 {abs(max_dd):.2f}%，"
            f"各项指标都在偏债混合的第一梯队。\n\n"
            f"建议作为固收组合的**核心底仓**，追求稳健增值。"
            f"市场震荡时能当「稳定器」，牛市中也能通过权益仓位吃到弹性收益。"
        )
    elif grade == "B":
        buy_advice = (
            f"该基金综合评级 {grade}，表现中等。夏普 {sharpe:.2f}，最大回撤 {abs(max_dd):.2f}%。\n\n"
            f"可以小仓位观察，等业绩企稳再考虑加仓。"
            f"重点关注基金经理的风格稳定性和信用风险管理能力。"
        )
    else:
        buy_advice = (
            f"该基金综合评级 {grade}，多项指标处于偏债混合的后段。\n\n"
            f"建议暂缓配置，等基金经理调整策略或市场环境改善后再评估。"
        )

    # ---- 持有中 ----
    hold_signals = []
    if sharpe >= 1.0:
        hold_signals.append("夏普比率健康，风险收益性价比还行")
    else:
        hold_signals.append(f"夏普比率降到 {sharpe:.2f}，性价比在下降")

    if abs(max_dd) <= 3.0:
        hold_signals.append("最大回撤可控，防御能力稳定")
    elif abs(max_dd) > 5.0:
        hold_signals.append(f"最大回撤达 {abs(max_dd):.2f}%，超出偏债混合的正常范围")

    if stock_ratio <= 0.25:
        hold_signals.append("权益仓位合理，没有风格漂移")
    elif stock_ratio > 0.35:
        hold_signals.append(f"权益仓位 {stock_ratio:.0%} 偏高，风格偏离了偏债定位")

    if calmar >= 2.0:
        hold_signals.append(f"卡玛比率 {calmar:.1f}，回撤修复效率高")

    hold_advice = (
        f"**持有诊断：**\n\n"
        + "\n".join(f"- {s}" for s in hold_signals)
        + "\n\n> 综合评估："
    )
    negative_signals = sum(1 for s in hold_signals if "超出" in s or "偏离" in s or "下降" in s)
    if negative_signals >= 2:
        hold_advice += "多项预警触发，建议**降低仓位或转入观察模式**。"
    elif negative_signals == 1:
        hold_advice += "有个别风险信号，**密切跟踪下一季报**。"
    else:
        hold_advice += "各项指标健康，**继续持有**。"

    # ---- 离场信号 ----
    exit_signals = []
    exit_signals.append(f"夏普比率连续两个季度低于 0.5（当前 {sharpe:.2f}）")
    exit_signals.append(f"最大回撤超过 8%（当前 {abs(max_dd):.2f}%）")
    exit_signals.append(f"股票仓位持续超过 35%（当前 {stock_ratio:.0%}），风格严重偏离偏债定位")
    exit_signals.append(f"信用评级分布中 AA- 及以下占比超过 40%")

    exit_advice = (
        f"**离场信号监测：**\n\n以下任一条件触发时，建议启动离场评估：\n\n"
        + "\n".join(f"- {s}" for s in exit_signals)
    )

    return (
        f"### 五、投资建议\n\n"
        f"**拟买入评估**\n\n"
        f"{buy_advice}\n\n"
        f"---\n\n"
        f"**持有中诊断**\n\n"
        f"{hold_advice}\n\n"
        f"---\n\n"
        f"{exit_advice}\n\n"
        f"---\n\n"
        f"**成本项披露：**\n\n"
        f"- 管理费率：{mgmt_fee:.2f}%　托管费率：{custody_fee:.2f}%\n\n"
        f"> **风险提示：** 以上分析基于历史数据，不构成投资建议。过往业绩不代表未来表现。"
    )


# ============================================================
# 辅助函数
# ============================================================

def _get_rate_prediction() -> dict:
    """获取利率预测结果"""
    try:
        return predict_rate_trend(horizon="3m")
    except Exception as e:
        logger.warning(f"[hybrid_bond_report_writer] 获取利率预测失败: {e}")
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
        end = _format_date(x_list[-1])
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


def _build_headline(fund_name, start_date, end_date) -> str:
    """报告标题行（无评分/评级）"""
    return (
        f"## {fund_name} — 混合型·偏债 深度分析\n"
        f"分析区间：{start_date} 至 {end_date}\n\n"
        f"偏债混合基金的卖点是「**比纯债多赚一点，比偏股少亏一点**」。"
        f"下面从收益表现、资产归因、债券持仓、风险预警、投资建议五个方面来拆解。"
    )


def _fallback_report(basic) -> dict:
    """数据不足时的兜底报告"""
    name = getattr(basic, "name", "该基金")
    text = (
        f"## {name} — 数据不足，无法生成偏债混合深度报告\n\n"
        f"当前统计周期内有效数据不足，无法生成完整的偏债混合分析报告。"
    )
    return {
        "meta": {"fund_name": name},
        "headline": text,
        "section1": "", "section2": "", "section3": "",
        "section4": "", "section5": "",
        "full_text": text,
    }
