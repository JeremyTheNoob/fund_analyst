"""
中短债基金深度评价报告生成器 — fund_quant_v2
报告结构：5板块 + 图表插入点标记 + 投资建议

板块设计：
1. 信用垫层（券种分布 + WACS + 政金债/信用债穿透）
2. 收益表现与滚动持有体验（累计收益 + 滚动持有X天不亏钱概率）
3. 深度分析与杠杆率（杠杆利用率 + 杠杆风险评估）
4. 风险预警与规模变动（规模变动趋势 + 流动性预警 + 申赎监测）
5. 投资建议（拟买入 / 已持有 分别给出）
"""

from __future__ import annotations
from typing import Any
import logging
import re

import numpy as np
import pandas as pd

from data_loader.rate_prediction import predict_rate_trend

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_bond_short_report(report: Any) -> dict:
    """
    生成中短债基金深度评价报告（5板块结构）

    Args:
        report: FundReport（含 bond_metrics + chart_data）

    Returns:
        {
          "meta":     {fund_name, fund_type, start_date, end_date, grade, score},
          "headline": 标题行,
          "section1": 基本信息+信用垫层（含 [INSERT_CHART: BOND_HOLDINGS_PIE]）,
          "section2": 收益表现+滚动持有体验（含 [INSERT_CHART: CUM_RET]）,
          "section3": 深度分析+杠杆率（无图表标记，纯文字）,
          "section4": 风险预警+规模变动（含 [INSERT_CHART: DRAWDOWN]）,
          "section5": 投资建议（拟买入/已持有）,
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

    # 固收专项数据
    duration   = round(m.duration, 1)
    wacs_score = round(m.wacs_score, 0)
    alpha_bond = round(m.alpha_bond * 100, 2) if hasattr(m, 'alpha_bond') else 0.0
    hhi        = round(m.hhi, 1)

    # 资产配置数据
    holdings = charts.get("holdings", {})
    bond_ratio  = holdings.get("bond_ratio", 0.0)
    cash_ratio  = holdings.get("cash_ratio", 0.0)
    stock_ratio = holdings.get("stock_ratio", 0.0)
    bond_details = holdings.get("bond_details", [])

    # 债券持仓分类
    bond_classification = getattr(m, 'bond_classification', {}) or {}

    # 压力测试
    stress_results = m.stress_results or []

    # 利率预测
    rate_prediction = _get_rate_prediction()

    # ── 中短债专属计算 ──────────────────────────────────────

    # 1. 滚动持有胜率（从日收益率计算）
    nav_df = charts.get("nav_df")
    holding_win_rates = _compute_rolling_holding_win_rate(nav_df, windows=[7, 15, 30, 60, 90])

    # 2. 杠杆率估算
    leverage_ratio, leverage_grade, leverage_detail = _estimate_leverage_ratio(
        bond_ratio, cash_ratio, bond_details
    )

    # 3. 期限分布（从 bond_details 解析）
    maturity_distribution = _compute_maturity_distribution(bond_details)

    # 4. 政金债/信用债穿透
    gov_credit_breakdown = _analyze_gov_credit_breakdown(bond_details, bond_classification)

    # 5. 规模信息
    fund_scale = basic.scale or ""

    # ── 生成各章节 ─────────────────────────────────────
    headline = _build_headline(fund_name, duration, wacs_score, start_date, end_date, grade)

    section1 = _section1_credit_cushion(
        fund_name, bond_classification, duration, wacs_score,
        gov_credit_breakdown, maturity_distribution
    )

    section2 = _section2_return_holding_experience(
        fund_name, ann_ret, ann_bm, excess_bps, cum_ret, cum_bm,
        sharpe, sortino, volatility, pos_rate, total_months, pos_months,
        holding_win_rates, start_date, end_date
    )

    section3 = _section3_leverage_analysis(
        fund_name, leverage_ratio, leverage_grade, leverage_detail,
        bond_ratio, cash_ratio, stress_results, duration
    )

    section4 = _section4_risk_warning(
        fund_name, max_dd, max_dd_bm, recovery_days_chart,
        defensive_ratio, duration, fund_scale, rate_prediction
    )

    section5 = _section5_investment_advice(
        fund_name, grade, sharpe, calmar, max_dd, duration,
        wacs_score, leverage_ratio, ann_ret, rate_prediction, basic
    )

    full_text = "\n\n".join([
        headline, section1, section2, section3, section4, section5
    ])

    return {
        "meta": {
            "fund_name": fund_name,
            "fund_type": "债券型-中短债",
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
# 板块 1：信用垫层
# ============================================================

def _section1_credit_cushion(
    fund_name, bond_classification, duration, wacs_score,
    gov_credit_breakdown, maturity_distribution
) -> str:
    """一、信用垫层分析：券种配置与信用结构"""

    if not bond_classification:
        return """### 信用垫层分析

**数据说明：** 当前基金持仓数据正在加载中，请稍候刷新页面查看详细分析。

[INSERT_CHART: BOND_HOLDINGS_PIE]
"""

    gov_ratio  = bond_classification.get('gov_bond', {}).get('ratio', 0) * 100
    credit_ratio = bond_classification.get('credit_bond', {}).get('ratio', 0) * 100
    urban_ratio = bond_classification.get('urban_construction', {}).get('ratio', 0) * 100
    estate_ratio = bond_classification.get('real_estate', {}).get('ratio', 0) * 100

    # 政金债 / 信用债穿透分析
    gov_detail = gov_credit_breakdown.get("gov_detail", "")
    credit_detail = gov_credit_breakdown.get("credit_detail", "")

    # 期限分布分析
    maturity_text = ""
    if maturity_distribution:
        short_ratio = maturity_distribution.get("short", 0) * 100
        mid_ratio = maturity_distribution.get("medium", 0) * 100
        long_ratio = maturity_distribution.get("long", 0) * 100
        maturity_text = (
            f"\n\n**期限分布：**\n\n"
            f"- **短期（≤1年）**：{short_ratio:.1f}% — 利率敏感度极低\n"
            f"- **中期（1-3年）**：{mid_ratio:.1f}% — 兼顾票息与防御\n"
            f"- **长期（>3年）**：{long_ratio:.1f}% — 资本利得弹性\n"
        )
        # 期限匹配评价
        if short_ratio > 50:
            maturity_text += "\n\n> 该基金**集中配置短期限债券**，和中短债「低波动、稳健收益」的定位高度一致。"
        elif short_ratio + mid_ratio > 80:
            maturity_text += "\n\n> 期限以中短期为主，符合中短债基金的典型特征。"
        else:
            maturity_text += "\n\n> 长期债券占比较高，可能增加利率敏感度，需关注久期管理。"

    # WACS 信用评分
    if wacs_score >= 80:
        wacs_comment = f"WACS信用评分 **{int(wacs_score)}分**，持仓整体信用资质优良"
    elif wacs_score >= 60:
        wacs_comment = f"WACS信用评分 **{int(wacs_score)}分**，持仓信用资质中等"
    else:
        wacs_comment = f"WACS信用评分 **{int(wacs_score)}分**，需警惕信用下沉风险"

    # 久期评价
    if duration <= 1.0:
        dur_comment = f"加权久期仅 **{duration:.1f} 年**，几乎不受利率波动影响"
    elif duration <= 2.0:
        dur_comment = f"加权久期 **{duration:.1f} 年**，处于短端安全区"
    elif duration <= 3.0:
        dur_comment = f"加权久期 **{duration:.1f} 年**，处于中短端，加息1%预计影响净值约 {duration:.1f}%"
    else:
        dur_comment = f"加权久期 **{duration:.1f} 年**，偏长，需关注利率上行风险"

    return (
        f"### 信用垫层分析\n\n"
        f"买中短债基金，图的就是比货币基金多赚一点、波动比长债少一大截。"
        f"底层资产买的是什么债券，直接决定了这个目标能不能实现。\n\n"
        f"**券种配置结构：**\n\n"
        f"[INSERT_CHART: BOND_HOLDINGS_PIE]\n\n"
        f"**数据解读：**\n\n"
        f"- **利率债（国债、政金债）**：{gov_ratio:.1f}% — 风险最低，票息确定性高\n"
        f"- **信用债（企业债、公司债）**：{credit_ratio:.1f}% — 收益增强的主要来源\n"
        f"- **城投债**：{urban_ratio:.1f}% — 区域性债务\n"
        f"- **地产债**：{estate_ratio:.1f}% — 周期性敏感\n\n"
        f"{gov_detail}"
        f"{credit_detail}"
        f"**久期与信用：**\n\n"
        f"- {dur_comment}\n"
        f"- {wacs_comment}\n"
        f"{maturity_text}"
    )


# ============================================================
# 板块 2：收益表现与滚动持有体验
# ============================================================

def _section2_return_holding_experience(
    fund_name, ann_ret, ann_bm, excess_bps, cum_ret, cum_bm,
    sharpe, sortino, volatility, pos_rate, total_months, pos_months,
    holding_win_rates, start_date, end_date
) -> str:
    """二、收益表现与滚动持有体验"""

    bm_label = "中债-综合财富指数"
    bm_excess_sign = "跑赢" if excess_bps > 0 else "落后于"
    bps_abs = abs(int(excess_bps))

    # 滚动持有胜率表格
    holding_table = ""
    if holding_win_rates:
        holding_table = (
            f"\n\n**滚动持有胜率（持有X天不亏钱的概率）：**\n\n"
            f"| 持有期 | 不亏钱概率 | 体验评价 |\n"
            f"|--------|-----------|----------|\n"
        )
        for window, win_rate in holding_win_rates.items():
            prob = win_rate * 100
            if prob >= 95:
                experience = "极佳，几乎稳赚"
            elif prob >= 85:
                experience = "很好，大概率盈利"
            elif prob >= 75:
                experience = "良好，需要耐心"
            elif prob >= 60:
                experience = "一般，偶有浮亏"
            else:
                experience = "较差，波动较大"
            holding_table += f"| {window}天 | {prob:.1f}% | {experience} |\n"

        holding_table += "\n"
        # 滚动持有体验总结
        win_30 = holding_win_rates.get(30, 0) * 100
        win_90 = holding_win_rates.get(90, 0) * 100
        if win_90 >= 90:
            holding_summary = (
                f"> 持有 90 天不亏钱的概率高达 **{win_90:.1f}%**，"
                f"持有 30 天不亏钱概率 **{win_30:.1f}%**。"
                f"只要拿得住，大概率不会亏。持有体验**极佳**。"
            )
        elif win_90 >= 75:
            holding_summary = (
                f"> 持有 90 天不亏钱概率 **{win_90:.1f}%**，"
                f"持有 30 天不亏钱概率 **{win_30:.1f}%**。"
                f"建议至少持有 **3 个月**，体验会好很多。"
            )
        else:
            holding_summary = (
                f"> 持有 90 天不亏钱概率 **{win_90:.1f}%**，波动相对明显。"
                f"建议做好 **6 个月以上** 的持有准备，不要因为短期浮亏就跑。"
            )
        holding_table += holding_summary

    # 收益评价
    if ann_ret >= 4.0:
        ret_comment = f"年化收益率 **{ann_ret:.2f}%**，远超货币基金（约1.5-2.0%），管理能力优秀"
    elif ann_ret >= 3.0:
        ret_comment = f"年化收益率 **{ann_ret:.2f}%**，跑赢货币基金，体现了中短债基金该有的水平"
    elif ann_ret >= 2.0:
        ret_comment = f"年化收益率 **{ann_ret:.2f}%**，略优于货币基金，但和其他优秀中短债比还有差距"
    else:
        ret_comment = f"年化收益率仅 **{ann_ret:.2f}%**，接近货币基金水平，收益增强能力不足"

    return (
        f"### 收益表现与持有体验\n\n"
        f"中短债基金的核心卖点不是高收益，而是**拿着不操心**。"
        f"下面看看这只基金赚得怎么样，以及持有多少天大概率不亏钱。\n\n"
        f"**核心数据：**\n\n"
        f"- 年化收益率：**{ann_ret:.2f}%**（基准 {ann_bm:.2f}%，{bm_excess_sign} **{bps_abs} bps**）\n"
        f"- 区间累计收益：**{cum_ret:.1f}%**（基准 {cum_bm:.1f}%）\n"
        f"- 年化波动率：**{volatility:.2f}%**\n"
        f"- 夏普比率：**{sharpe:.2f}**\n"
        f"- 月度胜率：**{pos_rate:.0f}%**（{total_months} 个月中 {pos_months} 个月正收益）\n\n"
        f"[INSERT_CHART: CUM_RET]\n\n"
        f"**收益评价：** {ret_comment}。\n\n"
        f"{holding_table}"
    )


# ============================================================
# 板块 3：深度分析与杠杆率
# ============================================================

def _section3_leverage_analysis(
    fund_name, leverage_ratio, leverage_grade, leverage_detail,
    bond_ratio, cash_ratio, stress_results, duration
) -> str:
    """三、深度分析：杠杆利用率"""

    # 杠杆率评价
    if leverage_ratio <= 1.05:
        leverage_comment = (
            f"当前杠杆率 **{leverage_ratio:.2f}倍**（即杠杆贡献约 {(leverage_ratio-1)*100:.1f}%），"
            f"处于**保守水平**，基本没有使用杠杆。"
        )
    elif leverage_ratio <= 1.10:
        leverage_comment = (
            f"当前杠杆率 **{leverage_ratio:.2f}倍**（即杠杆贡献约 {(leverage_ratio-1)*100:.1f}%），"
            f"处于**温和水平**，适度使用杠杆增强收益。"
        )
    elif leverage_ratio <= 1.20:
        leverage_comment = (
            f"当前杠杆率 **{leverage_ratio:.2f}倍**（即杠杆贡献约 {(leverage_ratio-1)*100:.1f}%），"
            f"处于**中等水平**，杠杆对收益的增厚效果明显，但同时也放大了风险。"
        )
    else:
        leverage_comment = (
            f"当前杠杆率 **{leverage_ratio:.2f}倍**（即杠杆贡献约 {(leverage_ratio-1)*100:.1f}%），"
            f"处于**激进水平**，大幅使用杠杆放大收益，但在资金面收紧时可能面临较大的去杠杆压力。"
        )

    # 杠杆评级
    leverage_grade_text = f"杠杆评级：**{leverage_grade}**"

    # 杠杆来源说明
    leverage_source = ""
    if bond_ratio > 1.0:
        leverage_source = (
            f"\n\n**杠杆来源分析：**\n\n"
            f"债券仓位占净值比 **{bond_ratio:.1%}**（超过100%），"
            f"说明基金通过**正回购**借入资金加仓债券。"
        )
        if cash_ratio < 0.02:
            leverage_source += (
                f"\n现金仓位仅 **{cash_ratio:.1%}**，流动性缓冲极薄。"
            )
        elif cash_ratio < 0.05:
            leverage_source += (
                f"\n现金仓位 **{cash_ratio:.1%}**，流动性缓冲较薄。"
            )
        else:
            leverage_source += (
                f"\n现金仓位 **{cash_ratio:.1%}**，具备一定的流动性缓冲。"
            )
    else:
        leverage_source = (
            f"\n\n**杠杆来源分析：**\n\n"
            f"债券仓位占净值比 **{bond_ratio:.1%}**，未超过100%，"
            f"说明基金**未使用明显杠杆**。收益主要来自票息和久期管理。"
        )

    # 杠杆与收益的关系
    leverage_return_text = ""
    if leverage_ratio > 1.05:
        lever_return = (leverage_ratio - 1) * 100
        leverage_return_text = (
            f"\n\n**杠杆收益估算：**\n\n"
            f"按当前杠杆率 {leverage_ratio:.2f}倍、假设债券综合收益率为 3.5% 估算，"
            f"杠杆贡献约 **{lever_return:.1f}%** 的额外收益。"
            f"但杠杆是双刃剑——赚钱时放大收益，亏钱时同样放大损失。"
        )

    # 压力测试
    stress_text = ""
    if stress_results:
        worst = max(stress_results, key=lambda x: abs(x.get("price_impact", 0)))
        worst_impact = abs(worst.get("price_impact", 0))
        worst_scenario = worst.get("scenario", "")
        stress_text = (
            f"\n\n**压力测试（四情景）：**\n\n"
            f"| 情景 | 预估净值影响 |\n"
            f"|------|------------|\n"
        )
        for s in stress_results:
            impact = s.get("price_impact", 0)
            stress_text += f"| {s['scenario']} | {impact:+.2f}% |\n"
        stress_text += (
            f"\n> 中短债基金久期短，对利率冲击天然有防御性。"
            f"最不利情景「**{worst_scenario}**」下，预估净值仅下跌 **{worst_impact:.2f}%**。"
        )

    return (
        f"### 杠杆率分析\n\n"
        f"中短债基金可以通过正回购加杠杆买更多债券。"
        f"牛市时多赚，资金面收紧时可能被迫卖债还钱，引发净值波动。\n\n"
        f"**杠杆监测：**\n\n"
        f"{leverage_comment}\n\n"
        f"{leverage_grade_text}"
        f"{leverage_source}"
        f"{leverage_return_text}"
        f"{stress_text}"
    )


# ============================================================
# 板块 4：风险预警与规模变动
# ============================================================

def _section4_risk_warning(
    fund_name, max_dd, max_dd_bm, recovery_days_chart,
    defensive_ratio, duration, fund_scale, rate_prediction
) -> str:
    """四、风险预警与规模变动"""

    fund_dd_abs = abs(max_dd)

    # 回撤评价
    if fund_dd_abs <= 0.3:
        dd_quality = "回撤控制**极其优秀**，属于顶尖防御水平"
    elif fund_dd_abs <= 0.8:
        dd_quality = "回撤控制**优秀**，符合中短债基金定位"
    elif fund_dd_abs <= 2.0:
        dd_quality = "回撤控制**良好**，处于中短债基金的合理范围"
    else:
        dd_quality = "回撤控制**偏弱**，超出中短债基金常见区间"

    # 基准对比
    bm_dd_abs = abs(max_dd_bm)
    vs_bm_text = ""
    if bm_dd_abs > 0:
        vs_bm_text = (
            f"同期基准最大回撤 {bm_dd_abs:.2f}%，"
            f"该基金为基准回撤深度的 **{defensive_ratio:.0%}**，{dd_quality}。"
        )

    # 修复天数
    if recovery_days_chart > 0:
        if recovery_days_chart <= 15:
            recovery_comment = f"净值在 **{recovery_days_chart} 个交易日**内完成修复，修复速度极快"
        elif recovery_days_chart <= 45:
            recovery_comment = f"净值在 **{recovery_days_chart} 个交易日**内完成修复，修复速度较快"
        else:
            recovery_comment = f"净值用了 **{recovery_days_chart} 个交易日**才修复，速度偏慢"
    else:
        recovery_comment = "统计区间内尚未完全修复至前高"

    # 规模变动与流动性分析
    scale_text = ""
    if fund_scale:
        # 解析规模字符串（如 "45.32亿元"）
        scale_match = re.search(r"([\d.]+)\s*(亿元|万亿)", fund_scale)
        if scale_match:
            scale_num = float(scale_match.group(1))
            scale_unit = scale_match.group(2)
            if scale_unit == "万亿":
                scale_num *= 10000

            if scale_num >= 100:
                scale_level = "大规模"
                scale_comment = f"基金规模 **{fund_scale}**，属于**大规模**产品。大规模基金流动性好，但管理难度较高，收益弹性可能受限"
            elif scale_num >= 30:
                scale_level = "中大规模"
                scale_comment = f"基金规模 **{fund_scale}**，处于**中大规模**，兼顾流动性与灵活性"
            elif scale_num >= 5:
                scale_level = "中小规模"
                scale_comment = f"基金规模 **{fund_scale}**，属于**中小规模**产品，策略灵活性较高"
            else:
                scale_level = "小规模"
                scale_comment = f"基金规模 **{fund_scale}**，属于**小规模**产品。需关注流动性风险：小规模基金在市场波动时可能面临赎回冲击"

            scale_text = (
                f"\n\n**规模与流动性：**\n\n"
                f"{scale_comment}。\n\n"
            )

            # 流动性预警信号
            liquidity_warnings = []
            if scale_num < 5:
                liquidity_warnings.append(f"小规模风险：当前规模 {fund_scale}，低于 5 亿元警戒线，存在清盘风险")
            if scale_num < 2:
                liquidity_warnings.append(f"清盘预警：当前规模不足 2 亿元，需高度关注基金连续 60 日净值低于 5000 万的清盘风险")
            if scale_num > 200:
                liquidity_warnings.append(f"大额赎回敏感性：大规模基金在市场恐慌时可能遭遇集中赎回，导致被迫卖出资产引发净值波动")

            if liquidity_warnings:
                scale_text += "**流动性预警信号：**\n\n" + "\n".join(f"- {w}" for w in liquidity_warnings)
    else:
        scale_text = (
            f"\n\n**规模与流动性：**\n\n"
            f"规模数据暂不可用，建议通过基金公司官网或天天基金查看最新规模。\n\n"
            f"> 对于中短债基金，建议优先选择规模 **5亿元以上** 的产品，"
            f"以规避小规模基金的流动性风险和清盘风险。"
        )

    # 利率环境影响
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
            f"技术指标模型预测未来3个月10Y国债收益率将从 **{current_rate:.2f}%** {dir_cn}至 **{mid_term_rate:.2f}%**"
            f"（置信度 {int(confidence * 100)}%）。"
        )
        if direction == "up":
            rate_comment += (
                f"\n\n中短债基金久期短，对利率上行**天然免疫**。"
                f"即便 10Y 国债上行 50BP，预计净值影响仅约 {duration * 0.5:.2f}%。"
            )
        elif direction == "down":
            rate_comment += (
                f"\n\n利率下行周期中，中短债基金的收益弹性**低于长债基金**。"
                f"但净值的下行保护能力更强，适合风险厌恶型投资者。"
            )
    else:
        rate_comment = (
            f"\n\n**利率环境研判：**\n\n"
            f"当前利率预测置信度较低，无法给出明确方向判断。"
            f"中短债基金由于其低久期特性，对利率波动天然具有防御性。"
        )

    return (
        f"### 风险预警与规模变动\n\n"
        f"中短债基金最大的风险不是信用违约，"
        f"而是**大规模赎回**迫使经理在不利价位卖债，加剧净值波动。\n\n"
        f"**回撤与修复：**\n\n"
        f"统计区间内，{fund_name} 的最大回撤为 **{fund_dd_abs:.2f}%**。{vs_bm_text}\n"
        f"{recovery_comment}。\n\n"
        f"[INSERT_CHART: DRAWDOWN]\n\n"
        f"{scale_text}"
        f"{rate_comment}"
    )


# ============================================================
# 板块 5：投资建议
# ============================================================

def _section5_investment_advice(
    fund_name, grade, sharpe, calmar, max_dd, duration,
    wacs_score, leverage_ratio, ann_ret, rate_prediction, basic
) -> str:
    """五、投资建议（拟买入 / 已持有）"""

    # 成本披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0

    # ---- 拟买入评估 ----
    if grade in ("A+", "A"):
        buy_advice = (
            f"**推荐评级：适合配置**\n\n"
            f"该中短债基金综合评级 {grade}，夏普比率 {sharpe:.2f}，最大回撤 {abs(max_dd):.2f}%，"
            f"各项指标均处于同类**第一梯队**。\n\n"
            f"**配置建议：** 可作为「**货币基金增强替代品**」配置 **30%~50%** 的闲置资金，"
            f"尤其适合以下场景：\n"
            f"- 从货币基金升级，追求略高收益\n"
            f"- 短期资金停放（3~12个月）\n"
            f"- 组合中的「压舱石」配置\n\n"
            f"**操作建议：** 建议在利率震荡或下行周期时逐步建仓，"
            f"避免在资金面大幅收紧时一次性大额买入。"
        )
    elif grade == "B":
        buy_advice = (
            f"**推荐评级：可以配置**\n\n"
            f"该中短债基金综合评级 {grade}，表现处于同类**中等水平**。\n\n"
            f"**配置建议：** 可作为「**过渡性配置**」小仓位持有，建议 **10%~20%**。"
            f"需持续关注基金经理的投资风格稳定性和信用风险管理能力。"
        )
    else:
        buy_advice = (
            f"**推荐评级：谨慎观察**\n\n"
            f"该中短债基金综合评级 {grade}，多项指标处于同类**后段**。\n\n"
            f"**配置建议：** 建议暂缓配置，等待基金经理调整策略或市场环境改善后再做评估。"
        )

    # ---- 已持有诊断 ----
    hold_signals = []
    if sharpe >= 1.5:
        hold_signals.append(("夏普比率优秀，风险收益性价比突出", "OK"))
    elif sharpe >= 0.8:
        hold_signals.append(("夏普比率健康", "OK"))
    else:
        hold_signals.append((f"夏普比率降至 {sharpe:.2f}，性价比下降", "WARN"))

    if abs(max_dd) <= 1.0:
        hold_signals.append(("回撤极浅，防御能力出色", "OK"))
    elif abs(max_dd) > 2.0:
        hold_signals.append((f"最大回撤达 {abs(max_dd):.2f}%，超出中短债常见区间", "BAD"))

    if leverage_ratio <= 1.15:
        hold_signals.append(("杠杆率温和，无激进操作风险", "OK"))
    elif leverage_ratio > 1.30:
        hold_signals.append((f"杠杆率 {leverage_ratio:.2f} 倍偏高，放大风险敞口", "BAD"))

    if wacs_score >= 60:
        hold_signals.append(("信用资质良好", "OK"))
    elif wacs_score < 50:
        hold_signals.append((f"信用评分 {int(wacs_score)} 分偏低，信用下沉风险", "WARN"))

    hold_advice = "**持有诊断：**\n\n" + "\n".join(
        f"- {text}" for text, emoji in hold_signals
    )

    negative_count = sum(1 for _, emoji in hold_signals if emoji == "BAD")
    if negative_count >= 2:
        hold_advice += "\n\n> **多项预警触发**，建议降低仓位或转入观察模式"
    elif negative_count == 1:
        hold_advice += "\n\n> 存在个别风险信号，建议密切跟踪下一季度报告"
    else:
        hold_advice += "\n\n> 各项指标健康，**继续持有**"

    # ---- 离场信号 ----
    exit_signals = [
        f"夏普比率连续两个季度低于 0.5（当前 {sharpe:.2f}）",
        f"最大回撤超过 2%（当前 {abs(max_dd):.2f}%）—— 对中短债而言已属异常",
        f"杠杆率持续超过 1.30 倍（当前 {leverage_ratio:.2f}）—— 激进杠杆放大风险",
        f"基金规模跌破 2 亿元—— 清盘风险",
        f"WACS 信用评分降至 50 分以下（当前 {int(wacs_score)}）—— 信用质量恶化",
    ]
    exit_advice = (
        f"**离场信号监测：**\n\n以下任一条件触发时，建议启动离场评估：\n\n"
        + "\n".join(f"- {s}" for s in exit_signals)
    )

    return (
        f"### 投资建议\n\n"
        f"**拟买入评估**\n\n"
        f"{buy_advice}\n\n"
        f"---\n\n"
        f"**已持有诊断**\n\n"
        f"{hold_advice}\n\n"
        f"---\n\n"
        f"{exit_advice}\n\n"
        f"---\n\n"
        f"**成本项披露：**\n\n"
        f"- 管理费率：{mgmt_fee:.2f}%　托管费率：{custody_fee:.2f}%"
        f"　最大申购费率：{purchase_fee:.2f}%\n\n"
        f"> **风险提示：** 以上分析基于历史数据，不构成任何投资建议。"
        f"过往业绩不代表未来表现。基金有风险，投资需谨慎。"
    )


# ============================================================
# 辅助计算函数
# ============================================================

def _compute_rolling_holding_win_rate(
    nav_df: pd.DataFrame,
    windows: list = None,
) -> dict:
    """
    计算滚动持有胜率：持有 X 天后不亏钱的概率。

    算法：对于每个历史日期 t，计算 t 到 t+N 天的累计收益率，
    如果 > 0 则记为一次"胜"。

    Args:
        nav_df: 净值 DataFrame（列：date, nav, ret）
        windows: 持有期列表（天），默认 [7, 15, 30, 60, 90]

    Returns:
        {window_days: win_rate}，如 {30: 0.85}
    """
    if windows is None:
        windows = [7, 15, 30, 60, 90]

    if nav_df is None or nav_df.empty or "ret" not in nav_df.columns:
        return {}

    try:
        rets = nav_df["ret"].values
        n = len(rets)
        result = {}

        for w in windows:
            if w >= n:
                result[w] = 0.5  # 数据不足
                continue

            wins = 0
            total = 0
            # 滚动窗口计算（跳过最后 w 天，因为没有足够的未来数据）
            for i in range(n - w):
                # 累计收益率 = (1+r1)*(1+r2)*...*(1+rw) - 1
                cum_ret = np.prod(1 + rets[i:i + w]) - 1
                if cum_ret > 0:
                    wins += 1
                total += 1

            if total > 0:
                result[w] = round(wins / total, 4)
            else:
                result[w] = 0.5

        return result
    except Exception as e:
        logger.warning(f"[_compute_rolling_holding_win_rate] 计算失败: {e}")
        return {}


def _estimate_leverage_ratio(
    bond_ratio: float,
    cash_ratio: float,
    bond_details: list,
) -> tuple:
    """
    估算基金杠杆率。

    简化公式：杠杆率 ≈ 债券占比 / (1 - 现金占比)
    当债券占比 > 100% 时，说明使用了正回购加杠杆。

    Args:
        bond_ratio: 债券占净值比（小数，如 1.12 表示 112%）
        cash_ratio: 现金占净值比（小数）
        bond_details: 债券持仓明细列表

    Returns:
        (leverage_ratio, grade, detail_text)
    """
    # 杠杆率估算逻辑：
    # 1. 主要依据 bond_ratio（占净值比，小数格式如 1.12 表示 112%）
    # 2. 当 bond_ratio > 1.0 时，说明使用了正回购加杠杆
    # 3. 持仓明细通常只披露前 N 大，合计远小于 100%，不能用于杠杆估算
    if bond_ratio > 1.0:
        estimated_leverage = bond_ratio  # 债券仓位本身就是杠杆率的近似
    else:
        estimated_leverage = 1.0  # 未使用杠杆

    # 杠杆评级
    if estimated_leverage <= 1.05:
        grade = "保守"
    elif estimated_leverage <= 1.15:
        grade = "温和"
    elif estimated_leverage <= 1.25:
        grade = "中等"
    else:
        grade = "激进"

    detail = f"债券仓位 {bond_ratio:.1%}，现金 {cash_ratio:.1%}，估算杠杆率 {estimated_leverage:.2f} 倍"

    return round(estimated_leverage, 2), grade, detail


def _compute_maturity_distribution(bond_details: list) -> dict:
    """
    从债券持仓明细中解析期限分布。

    分类规则（基于债券名称关键词）：
    - 短期（≤1年）：含 "1年"/"6M"/"3M"/"同业存单"/"NCD"/"超短" 等
    - 中期（1-3年）：含 "2年"/"3年"/"1年" 等
    - 长期（>3年）：含 "5年"/"7年"/"10年"/"30年" 等

    Args:
        bond_details: 债券持仓明细列表

    Returns:
        {"short": ratio, "medium": ratio, "long": ratio}（占比，加和为1）
    """
    if not bond_details:
        return {}

    import re

    short_weight = 0.0
    medium_weight = 0.0
    long_weight = 0.0
    total_weight = 0.0

    for bond in bond_details:
        name = str(bond.get("债券名称", "") or "")
        name_lower = name.lower()
        ratio = float(bond.get("占净值比例", 0) or 0)

        if ratio <= 0:
            continue

        # 兼容百分比格式：占净值比例通常在 0.1~10 范围（表示百分比）
        # 如果 > 1.5 且 < 1.0（不可能），或通过上下文判断
        # 中短债单只债券占净值比通常 0.5~10%，所以 > 50 才可能是小数*100的百分数
        if ratio > 50:
            ratio = ratio / 100.0
        is_short = False
        if any(kw in name for kw in ["同业存单", "NCD", "超短", "超短期"]):
            is_short = True
        else:
            # 匹配数字+年/Y
            match = re.search(r"(\d+)(年|Y|y)", name)
            if match:
                years = int(match.group(1))
                if years <= 1:
                    is_short = True

        if is_short:
            short_weight += ratio
        else:
            # 中长期区分
            match = re.search(r"(\d+)(年|Y|y)", name)
            if match:
                years = int(match.group(1))
                if years <= 3:
                    medium_weight += ratio
                else:
                    long_weight += ratio
            else:
                # 未识别期限，按中期处理（中短债基金大部分是中短期）
                medium_weight += ratio

        total_weight += ratio

    if total_weight <= 0:
        return {}

    return {
        "short": round(short_weight / total_weight, 4),
        "medium": round(medium_weight / total_weight, 4),
        "long": round(long_weight / total_weight, 4),
    }


def _analyze_gov_credit_breakdown(
    bond_details: list,
    bond_classification: dict,
) -> dict:
    """
    分析政金债 / 信用债的穿透结构。

    Returns:
        {"gov_detail": str, "credit_detail": str}
    """
    if not bond_details or not bond_classification:
        return {"gov_detail": "", "credit_detail": ""}

    gov_details = bond_classification.get('gov_bond', {}).get('details', [])
    credit_details = bond_classification.get('credit_bond', {}).get('details', [])

    gov_text = ""
    if gov_details:
        # 统计政金债细分类
        policy_bonds = []
        local_bonds = []
        for b in gov_details:
            name = str(b.get("债券名称", ""))
            if any(kw in name for kw in ["国开", "进出口", "农发"]):
                policy_bonds.append(b)
            elif "国债" in name:
                policy_bonds.append(b)
            else:
                policy_bonds.append(b)  # 其他利率债

        gov_ratio = bond_classification.get('gov_bond', {}).get('ratio', 0) * 100
        if gov_ratio > 0:
            gov_text = (
                f"\n\n**利率债穿透（占持仓 {gov_ratio:.1f}%）：**\n\n"
                f"利率债是中短债基金的「**安全垫**」。"
            )
            if len(policy_bonds) >= 3:
                # 展示前3大利率债持仓
                top3 = sorted(policy_bonds, key=lambda x: float(x.get("占净值比例", 0) or 0), reverse=True)[:3]
                gov_text += "主要持仓：\n\n"
                for b in top3:
                    bname = b.get("债券名称", "")
                    bratio = float(b.get("占净值比例", 0) or 0)
                    # 占净值比例通常是百分比格式（如 3.16 表示 3.16%）
                    if bratio > 50:
                        bratio = bratio / 100
                    gov_text += f"- {bname}（{bratio:.2f}%）\n"
            elif len(policy_bonds) > 0:
                gov_text += f"共持有 **{len(policy_bonds)} 只** 利率债品种。"
            else:
                gov_text += "具体持仓明细待加载。"

    credit_text = ""
    credit_ratio = bond_classification.get('credit_bond', {}).get('ratio', 0) * 100
    if credit_ratio > 0 and credit_details:
        # 统计信用债中是否有城投/地产
        urban_count = sum(1 for d in bond_classification.get('urban_construction', {}).get('details', []))
        estate_count = sum(1 for d in bond_classification.get('real_estate', {}).get('details', []))
        urban_r = bond_classification.get('urban_construction', {}).get('ratio', 0) * 100
        estate_r = bond_classification.get('real_estate', {}).get('ratio', 0) * 100

        credit_text = (
            f"\n\n**信用债穿透（占持仓 {credit_ratio:.1f}%）：**\n\n"
            f"信用债是中短债基金的「**收益增强层**」。"
        )

        if len(credit_details) >= 3:
            top3 = sorted(credit_details, key=lambda x: float(x.get("占净值比例", 0) or 0), reverse=True)[:3]
            credit_text += "主要持仓：\n\n"
            for b in top3:
                bname = b.get("债券名称", "")
                bratio = float(b.get("占净值比例", 0) or 0)
                # 占净值比例通常是百分比格式（如 3.16 表示 3.16%）
                if bratio > 50:
                    bratio = bratio / 100
                credit_text += f"- {bname}（{bratio:.2f}%）\n"

        if urban_count > 0:
            credit_text += (
                f"\n\n其中城投债 {urban_count} 只（占比 {urban_r:.1f}%），"
                f"需关注区域信用风险分化。"
            )
        if estate_count > 0:
            credit_text += (
                f"\n\n含地产债 {estate_count} 只（占比 {estate_r:.1f}%），"
                f"受房地产周期影响较大，需密切关注。"
            )

    return {"gov_detail": gov_text, "credit_detail": credit_text}


# ============================================================
# 通用辅助函数
# ============================================================

def _get_rate_prediction() -> dict:
    """获取利率预测结果"""
    try:
        return predict_rate_trend(horizon="3m")
    except Exception as e:
        logger.warning(f"[bond_short_report_writer] 获取利率预测失败: {e}")
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


def _build_headline(fund_name, duration, wacs_score, start_date, end_date, grade) -> str:
    """报告标题行"""
    return (
        f"## {fund_name} — 中短债基金分析\n\n"
        f"**统计区间：** {start_date} ~ {end_date}　｜　"
        f"**久期：** {duration:.1f}年　｜　**WACS：** {int(wacs_score)}分\n\n"
        f"从信用垫层、收益体验、杠杆率、风险预警和投资建议五个维度拆开看。"
    )


def _fallback_report(basic) -> dict:
    """数据不足时的兜底报告"""
    name = getattr(basic, "name", "该基金")
    text = (
        f"## {name} — 数据不足，无法生成中短债深度报告\n\n"
        f"当前统计周期内有效数据不足，无法生成完整的中短债深度分析报告。"
    )
    return {
        "meta": {"fund_name": name},
        "headline": text,
        "section1": "", "section2": "", "section3": "",
        "section4": "", "section5": "",
        "full_text": text,
    }
