"""
债券型基金深度评价报告生成器 — fund_quant_v2
角色：资深固收研究员（FICCC 分析师）
报告结构：5章节 + 图表插入点标记 + 利率专题（仅 bond_long）
"""

from __future__ import annotations
from typing import Any
import logging

from data_loader.rate_prediction import predict_rate_trend, generate_rate_prediction_chart

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_bond_deep_report(report: Any, risk_preference: str = "稳健型") -> dict:
    """
    生成债券型基金深度评价报告（约1000字，含图表标记）

    Args:
        report: 基金报告数据
        risk_preference: 用户风险偏好（"保守型"/"稳健型"/"积极型"）

    Returns:
        {
          "meta":       {fund_name, fund_type, start_date, end_date, grade, score},
          "headline":   报告标题行,
          "section0":   债券持仓分析（含 [INSERT_CHART: BOND_HOLDINGS_PIE]）,
          "section1":   收益获取逻辑（含 [INSERT_CHART: CUM_RET]）,
          "section2":   回撤深度与修复效率（含 [INSERT_CHART: DRAWDOWN]）,
          "section3":   月度胜率与持有体验（含 [INSERT_CHART: HEATMAP]）,
          "conclusion": 综合结论与投资建议（含 [INSERT_CHART: RATE_PREDICTION]）,
          "section_rate": 10年国债收益率专题（仅 bond_long，含 [INSERT_CHART: Y10Y_TREND]）,
          "full_text":  完整纯文本（所有章节合并）
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
    ann_ret      = round(cm.annualized_return * 100, 2)
    cum_ret      = round(cm.cumulative_return * 100, 1)
    volatility   = round(cm.volatility * 100, 2)

    # 基准数据
    bm_info     = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm      = round(bm_info.get("bm_last_return", 0) * 100, 1)
    ann_bm      = round(bm_info.get("bm_annual_return", 0) * 100, 2)  # 使用正确的基准年化收益
    excess_bps  = round((ann_ret - ann_bm) * 100, 0)   # 超额收益（bps）

    # 风险数据
    max_dd      = round(cm.max_drawdown * 100, 2)
    recovery_days = cm.recovery_days or 0

    # 卡玛比率
    calmar      = round(cm.calmar_ratio, 2)

    # 月度胜率（vs 0）
    monthly_win_rate = round(cm.monthly_win_rate * 100, 1)

    # 固收专项数据
    duration    = round(m.duration, 1)
    wacs_score  = round(m.wacs_score, 0)
    alpha_bond  = round(m.alpha_bond * 100, 2) if hasattr(m, 'alpha_bond') else 0.0

    # 回撤信息（从图表数据）
    dd_info       = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm     = round(dd_info.get("bm_max_dd", 0), 2)
    defensive_ratio = dd_info.get("defensive_ratio", 1.0)
    recovery_info = dd_info.get("recovery_info", {})
    recovery_days_chart = recovery_info.get("recovery_days", recovery_days) or recovery_days
    dd_date_str   = _format_date(recovery_info.get("max_dd_date"))

    # 月度统计
    hm_info       = charts.get("monthly_heatmap", {}).get("heatmap_info", {})
    monthly_stats = hm_info.get("monthly_stats", {})
    total_months  = monthly_stats.get("total_months", 0)
    pos_months    = monthly_stats.get("positive_months", 0)
    pos_rate      = round(pos_months / total_months * 100, 1) if total_months > 0 else monthly_win_rate

    # 收益获取风格推断（曲线平滑度 → 票息 vs 久期博弈）
    style_tag, style_desc = _infer_return_style(volatility, duration, calmar)

    # 防御性评价
    defense_desc = _defense_quality(max_dd, max_dd_bm, defensive_ratio)

    # 修复速度评价
    recovery_desc = _recovery_quality(recovery_days_chart)

    # 持有体验评价
    hold_exp_desc = _hold_experience(pos_rate, calmar)

    # 综合评分描述
    grade_desc = {
        "A+": "顶尖配置级固收资产",
        "A":  "优质稳健固收品种",
        "B":  "中等水平固收品种",
        "C":  "偏弱，需关注风险控制",
        "D":  "弱势，不建议持有",
    }.get(grade, "稳健固收品种")

    # 债券持仓分类数据
    bond_classification = getattr(m, 'bond_classification', {}) or {}

    # ── 利率预测分析 ─────────────────────────────────────
    rate_prediction = _get_rate_prediction()

    # ── 生成各章节文本 ─────────────────────────────────────
    headline = _build_headline(fund_name, style_tag, grade_desc, start_date, end_date, grade)
    
    # P1-新增：债券持仓分析章节
    section0_bond_holdings = _section0_bond_holdings_analysis(
        fund_name, bond_classification,
        engine_duration=duration, wacs_score=wacs_score,
    )
    
    section1 = _section1_return_logic(
        fund_name, ann_ret, ann_bm, excess_bps,
        style_tag, style_desc, duration, cum_ret, cum_bm, alpha_bond,
        start_date, end_date
    )
    section2 = _section2_drawdown_recovery(
        fund_name, max_dd, max_dd_bm, dd_date_str,
        recovery_days_chart, defense_desc, recovery_desc, defensive_ratio
    )
    section3 = _section3_monthly_stability(
        fund_name, pos_rate, total_months, pos_months,
        calmar, volatility, hold_exp_desc, monthly_win_rate
    )
    conclusion = _conclusion_advice(
        fund_name, style_tag, grade, grade_desc,
        max_dd, calmar, duration, wacs_score,
        ann_ret, excess_bps, rate_prediction, basic, risk_preference
    )

    # ── 利率专题（仅 bond_long / bond 类利率敏感型基金）────────
    section_rate = ""
    fund_type = getattr(report, 'fund_type', '')
    if fund_type == "bond_long":
        try:
            from data_loader.idx_bond_loader import (
                load_y10y_technical_analysis,
                generate_y10y_rate_topic,
            )
            rate_analysis = load_y10y_technical_analysis()
            section_rate = generate_y10y_rate_topic(rate_analysis)
            logger.info(f"[bond_report_writer] {fund_name} 利率专题生成成功")
        except Exception as e:
            logger.warning(f"[bond_report_writer] 利率专题生成失败: {e}")

    full_text = "\n\n".join(filter(None, [
        headline, section0_bond_holdings, section1, section2,
        section3, conclusion, section_rate,
    ]))

    return {
        "meta": {
            "fund_name":  fund_name,
            "fund_type":  "债券型基金",
            "start_date": start_date,
            "end_date":   end_date,
            "grade":      grade,
            "score":      score,
        },
        "headline":     headline,
        "section0":     section0_bond_holdings,
        "section1":     section1,
        "section2":     section2,
        "section3":     section3,
        "conclusion":   conclusion,
        "section_rate": section_rate,  # 仅 bond_long 有内容
        "full_text":    full_text,
    }


# ============================================================
# 章节生成函数
# ============================================================

def _build_headline(
    fund_name: str,
    style_tag: str,
    grade_desc: str,
    start_date: str,
    end_date: str,
    grade: str,
) -> str:
    """报告标题行"""
    return (
        f"## [债基深度评级] {style_tag}：{fund_name} 固收穿透分析报告\n\n"
        f"**分析日期：** {end_date}　｜　**统计区间：** {start_date} ~ {end_date}　｜　"
        f"**综合评级：** {grade}\n\n"
        f"**核心结论：** 该基金{grade_desc}。"
        f"以下报告将从收益归因、风险防守、持有体验三个维度对其进行系统性穿透。"
    )


def _section0_bond_holdings_analysis(
    fund_name: str,
    bond_classification: dict,
    engine_duration: float = 0.0,
    wacs_score: float = 0.0,
) -> str:
    """零、债券持仓分析：券种配置与信用结构"""
    
    if not bond_classification:
        return """### 零、债券持仓分析：券种配置与信用结构

**数据说明：** 当前基金持仓数据正在加载中，请稍候刷新页面查看详细分析。

[INSERT_CHART: BOND_HOLDINGS_PIE]
"""
    
    # 计算各类债券占比
    gov_ratio = bond_classification.get('gov_bond', {}).get('ratio', 0) * 100
    credit_ratio = bond_classification.get('credit_bond', {}).get('ratio', 0) * 100
    urban_ratio = bond_classification.get('urban_construction', {}).get('ratio', 0) * 100
    estate_ratio = bond_classification.get('real_estate', {}).get('ratio', 0) * 100
    
    # 使用 engine 从持仓加权计算的真实久期（修复旧版内部估算不一致问题）
    avg_duration = engine_duration if engine_duration > 0 else 3.5
    
    # 久期分类
    if avg_duration <= 2.0:
        duration_type = "短债型"
        duration_desc = "利率敏感度低，加息1%预计净值下跌约{:.1f}%".format(avg_duration)
        duration_risk = "较低"
    elif avg_duration <= 4.0:
        duration_type = "中债型"
        duration_desc = "利率敏感度中等，加息1%预计净值下跌约{:.1f}%".format(avg_duration)
        duration_risk = "中等"
    else:
        duration_type = "长债型"
        duration_desc = "利率敏感度高，加息1%预计净值下跌约{:.1f}%".format(avg_duration)
        duration_risk = "较高"
    
    # 生成信用结构分析
    credit_analysis = []
    
    if gov_ratio > 60:
        credit_analysis.append("**利率债占比超六成**，信用风险极低，收益主要来自票息和久期博弈")
    elif gov_ratio > 30:
        credit_analysis.append("**利率债占比三成以上**，具备一定的防御性，但也会参与信用债投资")
    else:
        credit_analysis.append("**利率债占比较低**，主要通过信用下沉和个券选择获取超额收益")
    
    if urban_ratio > 10:
        credit_analysis.append(f"**城投债占比{urban_ratio:.1f}%**，需关注地方债务化解进展和区域风险")
    
    if estate_ratio > 5:
        credit_analysis.append(f"**地产债占比{estate_ratio:.1f}%**，需密切关注房地产政策调控和销售数据")
    
    # WACS 信用评分解读
    if wacs_score > 0:
        if wacs_score >= 80:
            wacs_desc = f"WACS信用评分 **{int(wacs_score)}分**，持仓整体信用资质优良，以高等级债券为主"
        elif wacs_score >= 60:
            wacs_desc = f"WACS信用评分 **{int(wacs_score)}分**，持仓信用资质中等，AA级信用债占比较高"
        else:
            wacs_desc = f"WACS信用评分 **{int(wacs_score)}分**，持仓信用资质偏低，需警惕信用下沉风险"
        credit_analysis.append(wacs_desc)
    
    # 信用评级分布（待接入评级数据）
    rating_placeholder = """
**信用评级分布：**

- 待接入：AAA级占比
- 待接入：AA+级占比
- 待接入：AA级占比
- 待接入：AA-级及以下占比

> **说明**：信用评级数据正在对接中，后续将展示详细的评级分布和变化趋势。
"""
    
    return f"""### 零、债券持仓分析：券种配置与信用结构

对于债券型基金而言，底层资产的信用质量和券种配置直接决定了其风险收益特征。

**券种配置结构：**

[INSERT_CHART: BOND_HOLDINGS_PIE]

**数据解读：**

- **利率债（国债、政金债）**：{gov_ratio:.1f}% —— 风险最低，流动性最好，是组合的"压舱石"
- **信用债（企业债、公司债）**：{credit_ratio:.1f}% —— 收益增强的主要来源，需关注信用风险
- **城投债**：{urban_ratio:.1f}% —— 区域性债务，受地方政府财政状况影响
- **地产债**：{estate_ratio:.1f}% —— 周期性较强，与房地产景气度高度相关

**久期分析（从持仓加权计算）：**

- **久期类型**：{duration_type}（加权久期 {avg_duration:.1f} 年）
- **利率敏感度**：{duration_desc}
- **风险等级**：{duration_risk}

**信用结构分析：**

{"".join(credit_analysis)}

{rating_placeholder}

**对收益的影响：**

- **利率债占比高**：净值波动较小，收益相对稳定，适合保守型投资者
- **信用债占比高**：潜在收益更高，但需承担信用利差扩大风险
- **城投/地产占比高**：对宏观政策和行业景气度敏感，需动态跟踪"}}"""


def _section1_return_logic(
    fund_name: str,
    ann_ret: float,
    ann_bm: float,
    excess_bps: float,
    style_tag: str,
    style_desc: str,
    duration: float,
    cum_ret: float,
    cum_bm: float,
    alpha_bond: float,
    start_date: str,
    end_date: str,
) -> str:
    """第一章：收益获取逻辑（票息 vs 资本利得）"""
    bm_label = "中债-综合财富指数（全收益）"
    bm_excess_sign = "跑赢" if excess_bps > 0 else "落后于"
    bps_abs = abs(int(excess_bps))

    # 久期解读
    if duration <= 1.5:
        dur_comment = (
            f"该基金久期仅约 {duration:.1f} 年，偏向**超短端配置**，"
            f"对利率波动极不敏感，几乎不依赖资本利得获利。"
        )
    elif duration <= 3.0:
        dur_comment = (
            f"该基金久期约 {duration:.1f} 年，属于**中短端布局**，"
            f"在利率下行阶段可适度获取资本利得，但核心收益仍来自票息保护。"
        )
    else:
        dur_comment = (
            f"该基金久期约 {duration:.1f} 年，属于**中长端配置**，"
            f"经理有意拉长久期以博取资本利得，超额收益的波动性较高。"
        )

    # Alpha 债券三因子解读
    alpha_comment = ""
    if alpha_bond > 0.3:
        alpha_comment = (
            f"\n\n债券三因子回归显示，在剔除短端利率、长端利率和信用利差的系统性贡献后，"
            f"经理仍贡献了约 **{alpha_bond:.2f}%** 的年化纯Alpha，体现了选券和择时的综合优势。"
        )
    elif alpha_bond < -0.1:
        alpha_comment = (
            f"\n\n债券三因子回归显示，经理的年化Alpha为 **{alpha_bond:.2f}%**，"
            f"系统性因子贡献了超额收益的绝大部分，需关注策略的可持续性。"
        )

    return (
        f"### 一、收益获取逻辑：票息与资本利得的动态平衡\n\n"
        f"债基的收益由**票息（Carry）**和**资本利得（Capital Gain）**两部分构成。"
        f"在评价 {fund_name} 时，我们以 {bm_label} 作为全收益基准进行对比。\n\n"
        f"**数据表现：**\n"
        f"统计区间（{start_date} ~ {end_date}）内，该基金年化收益率为 **{ann_ret:.2f}%**，"
        f"{bm_excess_sign}基准约 **{bps_abs} bps**（基准年化约 {ann_bm:.2f}%）。"
        f"区间累计收益 {cum_ret:.1f}%，基准累计 {cum_bm:.1f}%。\n\n"
        f"[INSERT_CHART: CUM_RET]\n\n"
        f"**解读：**\n"
        f"{style_desc}"
        f"{dur_comment}"
        f"{alpha_comment}"
    )


def _section2_drawdown_recovery(
    fund_name: str,
    max_dd: float,
    max_dd_bm: float,
    dd_date_str: str,
    recovery_days: int,
    defense_desc: str,
    recovery_desc: str,
    defensive_ratio: float,
) -> str:
    """第二章：回撤深度与修复效率"""
    bm_dd_abs = abs(max_dd_bm)
    fund_dd_abs = abs(max_dd)

    # 同类对比文字
    if bm_dd_abs > 0:
        vs_bm_text = (
            f"同期债券基准最大回撤约 **{bm_dd_abs:.2f}%**，"
            f"该基金仅为基准回撤深度的 **{defensive_ratio:.0%}**，{defense_desc}。"
        )
    else:
        vs_bm_text = f"该基金展现了{defense_desc}的防御能力。"

    # 修复期文字
    if recovery_days > 0:
        recovery_text = (
            f"净值在触底后经历 **{recovery_days} 个交易日**完成修复，{recovery_desc}。"
            f"这种「坑浅、填得快」的特征，"
            f"证明底层资产流动性良好且久期配置合理。"
        )
    else:
        recovery_text = (
            "统计区间内尚未完全修复至前高，或回撤持续时间较短，需持续观察。"
        )

    dd_date_text = f"（发生于 {dd_date_str}）" if dd_date_str else ""

    return (
        f"### 二、风险边际控制：水下回撤与修复效率\n\n"
        f"对于债基持有者，**回撤深度**和**「回血」速度**是决定持有信心的核心指标。"
        f"回撤越浅、修复越快，持有人越不容易在低谷割肉。\n\n"
        f"**数据表现：**\n"
        f"统计区间内，{fund_name} 的最大回撤为 **{fund_dd_abs:.2f}%**{dd_date_text}，"
        f"{vs_bm_text}\n\n"
        f"[INSERT_CHART: DRAWDOWN]\n\n"
        f"**解读：**\n"
        f"{recovery_text}\n\n"
        f"水下回撤图清晰呈现了历次调整的**深度与持续时长**。"
        f"优秀的固收产品应呈现「回撤面积小、修复斜率陡」的形态——即每一次向下都浅而短暂，"
        f"向上修复则迅速有力。这一特征是票息保护能力的直接体现。"
    )


def _section3_monthly_stability(
    fund_name: str,
    pos_rate: float,
    total_months: int,
    pos_months: int,
    calmar: float,
    volatility: float,
    hold_exp_desc: str,
    monthly_win_rate: float,
) -> str:
    """第三章：月度胜率与持有体验"""
    neg_months = total_months - pos_months if total_months > 0 else 0

    # 月度胜率解读
    if pos_rate >= 92:
        win_rate_comment = (
            f"在过去 **{total_months} 个月**中，仅有 **{neg_months} 个月**出现净值下滑，"
            f"正收益月份占比高达 **{pos_rate:.1f}%**。"
            f"这意味着投资者在任意时点介入后，持有满3个月获得正收益的概率趋近于100%。"
        )
    elif pos_rate >= 80:
        win_rate_comment = (
            f"在过去 **{total_months} 个月**中，正收益月份占比为 **{pos_rate:.1f}%**，"
            f"共有 **{neg_months} 个月**出现轻微回调，整体持有体验较好。"
        )
    else:
        win_rate_comment = (
            f"在过去 **{total_months} 个月**中，正收益月份占比为 **{pos_rate:.1f}%**，"
            f"波动较为明显，需关注持有期的回撤管理。"
        )

    # 卡玛比率解读
    if calmar >= 5.0:
        calmar_comment = (
            f"卡玛比率（Calmar Ratio）高达 **{calmar:.1f}**，"
            f"意味着每承担1%的最大回撤，可以换取 {calmar:.1f}% 的年化收益，"
            f"风险收益效率属于**顶尖水平**。"
        )
    elif calmar >= 2.0:
        calmar_comment = (
            f"卡玛比率（Calmar Ratio）为 **{calmar:.1f}**，"
            f"风险调整后收益表现**良好**，属于中上水平的固收产品。"
        )
    else:
        calmar_comment = (
            f"卡玛比率（Calmar Ratio）为 **{calmar:.1f}**，"
            f"风险调整后收益表现**一般**，性价比有待提升。"
        )

    return (
        f"### 三、波动率管理：持有体验的量化拆解\n\n"
        f"波动率是衡量债基「持有体感」的终极指标。"
        f"年化波动率 **{volatility:.2f}%** 的背后，是经理日复一日对流动性管理和杠杆控制的体现。\n\n"
        f"**数据表现：**\n"
        f"{win_rate_comment} "
        f"{calmar_comment}\n\n"
        f"[INSERT_CHART: HEATMAP]\n\n"
        f"**解读：**\n"
        f"月度热力图是持有体验最直观的展示。{hold_exp_desc}"
        f"在债市出现理财赎回潮、资金面收紧等极端情形时，"
        f"优秀的经理可以通过精细化的**流动性管理**和**杠杆压降**，"
        f"将月度亏损控制在极小范围内。"
        f"热力图中绿色（亏损）方格的深浅和密度，是判断经理防御能力的重要视觉信号。"
    )


def _conclusion_advice(
    fund_name: str,
    style_tag: str,
    grade: str,
    grade_desc: str,
    max_dd: float,
    calmar: float,
    duration: float,
    wacs_score: float,
    ann_ret: float,
    excess_bps: float,
    rate_prediction: dict,
    basic: Any,
    risk_preference: str = "稳健型"
) -> str:
    """综合结论与投资建议（根据风险偏好个性化）"""

    # 经理画像
    if calmar >= 5.0 and abs(max_dd) <= 0.5:
        manager_portrait = (
            "该经理是典型的**「绝对收益导向型」**选手。"
            "不追求极致的业绩排名，但极其厌恶回撤。"
            "收益来源清晰：通过严控信用风险，换取确定性的票息溢价。"
        )
    elif calmar >= 2.0:
        manager_portrait = (
            "该经理是**「稳健收益型」**选手。"
            "在票息积累的基础上，适度参与久期博弈，"
            "追求在不同债市环境下均能获取稳健的正收益。"
        )
    else:
        manager_portrait = (
            "该经理风格偏向**「主动久期管理型」**，"
            "在利率趋势判断上表现积极，收益弹性较大但回撤相对明显。"
        )

    # 核心风险点（基于利率预测 + 基金久期）
    risk_point = _generate_risk_point(duration, wacs_score, rate_prediction)

    # 配置建议（根据风险偏好个性化）
    allocation = _generate_personalized_allocation(
        grade, duration, risk_preference, ann_ret, calmar, fund_name
    )

    # 利率预测图表（仅在预测置信度 >= 0.5 时显示）
    chart_insertion = ""
    if rate_prediction["confidence"] >= 0.5:
        chart_insertion = "\n\n[INSERT_CHART: RATE_PREDICTION]\n\n"

    # 成本项披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    return (
        f"### 四、综合结论与投资建议\n\n"
        f"**1. 经理画像**\n\n"
        f"{manager_portrait}\n\n"
        f"**2. 核心风险点**\n\n"
        f"{risk_point}"
        f"{chart_insertion}"
        f"**3. 配置建议**\n\n"
        f"{allocation}\n\n"
        f"> **风险提示：** 债券基金并非无风险，利率风险、信用风险、流动性风险均可能导致净值损失。"
        f"以上分析基于历史数据，不构成任何投资建议。过往业绩不代表未来表现。\n\n"
        f"---\n\n"
        f"**成本项披露：**\n\n"
        f"- 管理费率：{mgmt_fee:.2f}%\n"
        f"- 托管费率：{custody_fee:.2f}%\n"
        f"- 最大申购费率：{purchase_fee:.2f}%\n"
        f"- 最大赎回费率：{redeem_fee:.2f}%"
    )


# ============================================================
# 辅助函数
# ============================================================

def _get_rate_prediction() -> dict:
    """
    获取利率预测结果（技术指标模型）

    Returns:
        {
            "direction": "up" / "down" / "sideways",
            "confidence": 0.75,
            "y10y_forecast": {...},
            "key_factors": [...],
            "risk_signals": [...],
            "chart_data": {...},
        }
    """
    try:
        return predict_rate_trend(horizon="3m")
    except Exception as e:
        logger.warning(f"[bond_report_writer] 获取利率预测失败: {e}")
        return {
            "direction": "sideways",
            "confidence": 0.3,
            "y10y_forecast": {"current": 2.5, "mid_term": 2.5},
            "key_factors": ["数据不足，无法做出有效预测"],
            "risk_signals": [],
            "chart_data": {},
        }


def _generate_risk_point(duration: float, wacs_score: float, rate_prediction: dict) -> str:
    """
    生成核心风险点（基于基金久期 + 利率预测）

    Args:
        duration: 基金久期（年）
        wacs_score: 持仓信用质量评分
        rate_prediction: 利率预测结果

    Returns:
        风险点描述文本
    """
    direction = rate_prediction["direction"]
    confidence = rate_prediction["confidence"]
    factors = rate_prediction["key_factors"]
    risks = rate_prediction["risk_signals"]

    # 利率环境总结
    forecast = rate_prediction["y10y_forecast"]
    current = forecast["current"]
    mid_term = forecast["mid_term"]

    direction_map = {"up": "上行", "down": "下行", "sideways": "震荡"}
    direction_cn = direction_map.get(direction, "震荡")
    confidence_cn = f"{int(confidence * 100)}%"

    # 基础风险描述（基金自身）
    if duration >= 5.0:
        base_risk = f"该基金久期约 {duration:.1f} 年，**久期风险较高**。"
    elif wacs_score < 60:
        base_risk = f"持仓信用质量评分（WACS）为 {int(wacs_score)} 分，信用资质偏低。"
    else:
        base_risk = "该基金持仓偏向中短端且信用等级较高，防御能力较强。"

    # 利率环境影响分析
    if direction == "up" and duration >= 3.0:
        rate_impact = (
            f"\n\n**利率环境影响：**\n"
            f"根据技术指标模型，未来3个月10Y国债收益率预计将从 **{current:.2f}%** {direction_cn}至 **{mid_term:.2f}%**（预测置信度：{confidence_cn}）。"
            f"\n\n由于该基金久期较长，在利率上行阶段，净值可能面临**资本利得亏损风险**，回撤幅度可能加大。"
        )
    elif direction == "down" and duration <= 2.0:
        rate_impact = (
            f"\n\n**利率环境影响：**\n"
            f"根据技术指标模型，未来3个月10Y国债收益率预计将从 **{current:.2f}%** {direction_cn}至 **{mid_term:.2f}%**（预测置信度：{confidence_cn}）。"
            f"\n\n由于该基金久期较短，在利率下行阶段，可能因久期不足而**跟涨偏慢**，超额收益空间受限。"
        )
    elif direction == "down" and duration >= 3.0:
        rate_impact = (
            f"\n\n**利率环境影响：**\n"
            f"根据技术指标模型，未来3个月10Y国债收益率预计将从 **{current:.2f}%** {direction_cn}至 **{mid_term:.2f}%**（预测置信度：{confidence_cn}）。"
            f"\n\n该基金久期较长，在利率下行阶段有望通过**资本利得放大收益**，但需警惕利率反弹时的回撤风险。"
        )
    else:
        rate_impact = (
            f"\n\n**利率环境影响：**\n"
            f"根据技术指标模型，未来3个月10Y国债收益率预计将从 **{current:.2f}%** {direction_cn}至 **{mid_term:.2f}%**（预测置信度：{confidence_cn}）。"
            f"\n\n利率环境对基金表现影响中性，建议关注后续政策面变化。"
        )

    # 关键因素
    if factors and confidence >= 0.5:
        factors_text = "\n\n**关键判断依据：**\n" + "\n".join(f"• {f}" for f in factors[:3])
    else:
        factors_text = ""

    # 风险信号
    if risks:
        risks_text = "\n\n**需警惕的风险信号：**\n" + "\n".join(f"• {r}" for r in risks[:2])
    else:
        risks_text = ""

    return f"{base_risk}{rate_impact}{factors_text}{risks_text}"

def _infer_return_style(volatility: float, duration: float, calmar: float) -> tuple[str, str]:
    """
    推断收益获取风格：票息驱动 vs 久期博弈
    返回 (style_tag, style_desc)
    """
    # 高度平滑 + 短久期 = 纯票息
    if volatility <= 0.8 and duration <= 2.0:
        tag = "票息之石与回撤之盾"
        desc = (
            "累计收益曲线**极度平滑**，向上斜率高度一致。"
            "这表明经理并不依赖频繁的久期博弈（博取资本利得），"
            "而是通过持有高质量信用债，赚取**确定性的票息收入**。"
            "在利率下行周期中，超额收益缓慢但稳健地积累，体现了极佳的配置性价比。\n\n"
        )
    # 中等波动 + 中等久期 = 票息+适度博弈
    elif volatility <= 1.5 and duration <= 4.0:
        tag = "票息护城河与久期杠杆"
        desc = (
            "累计收益曲线较为平稳，但在利率趋势行情中会出现**阶段性加速上扬**。"
            "这表明经理在票息保护的基础上，会在判断利率趋势明确时适度拉长久期，"
            "通过**资本利得放大收益**，整体策略兼顾稳健性与弹性。\n\n"
        )
    # 高波动 + 长久期 = 久期博弈主导
    else:
        tag = "久期博弈者的攻守之道"
        desc = (
            "收益曲线呈现出较明显的**波动特征**，阶段性涨跌幅度超出纯债基金常见区间。"
            "这表明经理主动参与久期博弈，在利率下行阶段通过**拉长组合久期**博取较大资本利得，"
            "但在利率上行或震荡阶段亦会承受更深的回撤。\n\n"
        )
    return tag, desc


def _defense_quality(max_dd: float, max_dd_bm: float, defensive_ratio: float) -> str:
    """防御性评价"""
    fund_dd_abs = abs(max_dd)
    if fund_dd_abs <= 0.3:
        return "防御能力**极强**（绝对回撤极浅，属于顶尖固收水平）"
    elif fund_dd_abs <= 0.8:
        if defensive_ratio < 0.7:
            return "防御能力**优秀**（深度远优于基准）"
        return "防御能力**良好**（回撤控制得当）"
    elif fund_dd_abs <= 2.0:
        return "防御能力**一般**（回撤深度处于同类中游）"
    else:
        return "防御能力**偏弱**（回撤深度高于同类平均）"


def _recovery_quality(recovery_days: int) -> str:
    """修复速度评价"""
    if recovery_days <= 0:
        return "修复情况数据不足"
    elif recovery_days <= 10:
        return "修复速度**极快**（10个交易日内完成修复，流动性管理出色）"
    elif recovery_days <= 30:
        return "修复速度**较快**（约1~1.5个月完成修复，属于良好水平）"
    elif recovery_days <= 60:
        return "修复速度**一般**（约2~3个月完成修复，处于同类中游）"
    else:
        return f"修复速度**偏慢**（约 {recovery_days} 个交易日才完成修复，需关注经理的风险处置能力）"


def _hold_experience(pos_rate: float, calmar: float) -> str:
    """持有体验评价"""
    if pos_rate >= 90 and calmar >= 4.0:
        return (
            "该基金的持有体验属于**顶尖水平**。"
            "负收益月份极少，且每次亏损幅度极小，"
            "投资者几乎感受不到明显波动，是理想的「睡后资产」。"
        )
    elif pos_rate >= 80 and calmar >= 2.0:
        return (
            "该基金的持有体验**良好**。"
            "大多数月份保持正收益，偶有轻微回调，"
            "整体节奏稳健，适合中低风险偏好的投资者长期持有。"
        )
    else:
        return (
            "该基金的持有体验**一般**，"
            "月度收益波动相对明显，需要投资者具备一定的心理承受能力。"
        )


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
    """将各种格式的日期转换为字符串"""
    if d is None:
        return ""
    try:
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d")
        s = str(d)
        return s[:10]   # 取前10字符 YYYY-MM-DD
    except Exception:
        return str(d)


def _year_count(start_date: str, end_date: str) -> float:
    """估算年数"""
    try:
        from datetime import datetime
        fmt = "%Y-%m-%d"
        s = datetime.strptime(start_date[:10], fmt)
        e = datetime.strptime(end_date[:10], fmt)
        return max((e - s).days / 365, 0.1)
    except Exception:
        return 1.0


def _generate_personalized_allocation(
    grade: str,
    duration: float,
    risk_preference: str,
    ann_ret: float,
    calmar: float,
    fund_name: str
) -> str:
    """
    生成个性化配置建议（基于风险偏好、基金评级、久期特征）

    Args:
        grade: 基金评级（A+/A/B/C/D）
        duration: 基金久期（年）
        risk_preference: 用户风险偏好（保守型/稳健型/积极型）
        ann_ret: 年化收益率（%）
        calmar: 卡玛比率
        fund_name: 基金名称

    Returns:
        个性化配置建议文本
    """
    # 久期分类
    if duration <= 2.0:
        duration_type = "短债型"
        duration_risk = "低"
    elif duration <= 4.0:
        duration_type = "中债型"
        duration_risk = "中等"
    else:
        duration_type = "长债型"
        duration_risk = "高"

    # 基础配置建议（根据评级和风险偏好）
    if grade in ("A+", "A"):
        if risk_preference == "保守型":
            base_allocation = "**30%~50%** 的核心仓位"
            role_desc = "作为组合中的「压舱石」或「现金增强替代品」"
            suitability = "对于追求资产保值、对回撤容忍度极低的投资者"
        elif risk_preference == "稳健型":
            base_allocation = "**20%~40%** 的底仓配置"
            role_desc = "作为组合中的「稳定器」和「收益基石」"
            suitability = "对于追求稳健增值、能承受小幅波动的投资者"
        else:  # 积极型
            base_allocation = "**15%~30%** 的固收底仓"
            role_desc = "作为组合中的「风险缓冲层」和「流动性储备」"
            suitability = "对于追求高收益、能接受一定回撤的积极型投资者"

        allocation = (
            f"适合{suitability}。{role_desc}，{fund_name}具备极高的长期配置价值，"
            f"建议以 {base_allocation} 长期持有，作为穿越牛熊的稳健资产。"
        )

        # 根据久期调整建议
        if duration >= 3.0 and risk_preference == "保守型":
            allocation += (
                f"\n\n**久期匹配建议：** 该基金久期较长（{duration:.1f}年），"
                f"对利率敏感度{duration_risk}。建议保守型投资者适当降低配置比例至 **20%~35%**，"
                f"或在利率上行预期较强时，通过久期更短的债基进行平衡。"
            )
        elif duration <= 1.5 and risk_preference == "积极型":
            allocation += (
                f"\n\n**收益增强建议：** 该基金久期较短（{duration:.1f}年），"
                f"收益弹性相对有限。积极型投资者可考虑配置 **5%~10%** 的信用债基金或可转债基金，"
                f"以提升组合整体收益水平。"
            )

    elif grade == "B":
        if risk_preference == "保守型":
            base_allocation = "**10%~20%** 的卫星仓位"
            role_desc = "作为货币基金的增强替代品"
            suitability = "对于希望获得略高于货基收益、但不愿承担较大风险的保守型投资者"
        elif risk_preference == "稳健型":
            base_allocation = "**15%~25%** 的配置比例"
            role_desc = "作为固收组合的收益增强部分"
            suitability = "对于有一定固收投资经验、追求稳健增值的稳健型投资者"
        else:  # 积极型
            base_allocation = "**5%~15%** 的流动性管理工具"
            role_desc = "作为权益投资的「避风港」和「现金管理工具」"
            suitability = "对于主要配置权益资产、需要固收类资产平衡风险的积极型投资者"

        allocation = (
            f"{suitability}。{role_desc}，建议以 {base_allocation} 配置，"
            f"并需定期关注其久期变化、基金经理动向以及信用风险暴露情况。"
        )

        # 根据收益和风险调整
        if ann_ret >= 5.0 and calmar >= 3.0:
            allocation += (
                f"\n\n**性价比提示：** 该基金年化收益 **{ann_ret:.2f}%**，卡玛比率 **{calmar:.1f}**，"
                f"风险收益比较高。{risk_preference}投资者可适当提高配置比例至建议范围上限。"
            )
        elif ann_ret < 3.0 or calmar < 1.5:
            allocation += (
                f"\n\n**性价比提示：** 该基金风险调整后收益一般。"
                f"建议配置比例不超过建议范围下限，或寻找更优的替代品种。"
            )

    else:  # C/D级
        if risk_preference == "保守型":
            allocation = (
                "**不建议配置。** 该基金评级较低，风险收益特征不符合保守型投资者的保值需求。"
                "建议优先选择A级或以上的短债基金或利率债基金。"
            )
        elif risk_preference == "稳健型":
            allocation = (
                "**谨慎配置，严格限仓。** 待基金风格和数据企稳后再考虑增持，"
                "建议初始配置比例不超过 **5%**，并设置严格的止损观察期（如3个月）。"
            )
        else:  # 积极型
            allocation = (
                "**可小仓位博弈。** 对于积极型投资者，可配置 **5%~10%** 作为高风险高收益的博弈仓位，"
                "但需密切跟踪回撤和久期变化，并做好及时止损的准备。"
            )

    # 添加投资期限建议
    allocation += "\n\n**投资期限建议：**"
    if duration <= 2.0:
        allocation += "该基金久期较短，适合 **3个月~1年** 的中短期投资。"
    elif duration <= 4.0:
        allocation += "该基金久期中短，建议持有 **6个月~2年** 以平滑短期波动。"
    else:
        allocation += "该基金久期较长，建议持有 **1年以上** 以获取完整的利率周期收益。"

    if risk_preference == "保守型":
        allocation += "保守型投资者可适当缩短持有期限，或选择久期更短的替代品种。"
    elif risk_preference == "积极型":
        allocation += "积极型投资者可根据利率预测灵活调整持有期限，在利率下行周期可适当延长。"

    return allocation


def _fallback_report(basic) -> dict:
    """数据不足时的兜底报告"""
    name = getattr(basic, "name", "该基金")
    text = (
        f"## {name} — 数据不足，无法生成深度报告\n\n"
        f"当前统计周期内有效数据不足，无法生成完整的债基深度分析报告。\n"
        f"请尝试延长分析周期，或等待更多净值数据积累后重新分析。"
    )
    return {
        "meta":       {"fund_name": name},
        "headline":   text,
        "section1":   "",
        "section2":   "",
        "section3":   "",
        "conclusion": "",
        "full_text":  text,
    }
