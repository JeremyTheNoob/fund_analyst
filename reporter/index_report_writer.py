"""
指数/ETF 深度评级报告生成器 — fund_quant_v2
角色：资深指数研究员（ETF 策略分析师）
报告结构：4章节（净值拟合 / 跟踪精度 / 流动性 / 配置建议）+ 图表插入点标记
"""

from __future__ import annotations
from typing import Any


# ============================================================
# 主入口
# ============================================================

def generate_index_deep_report(report: Any) -> dict:
    """
    生成指数/ETF 深度评级报告（约1000字，含图表标记）

    Returns:
        {
          "meta":       {fund_name, index_name, start_date, end_date, grade, tool_score, tags},
          "headline":   标题行（markdown）,
          "section1":   净值拟合与贝塔回归（含 [INSERT_CHART: CUM_RET]）,
          "section2":   跟踪偏离度与精细化管理（含 [INSERT_CHART: TRACKING_ERROR_SCATTER]）,
          "section3":   场内流动性与交易成本（含 [INSERT_CHART: PREMIUM_DISCOUNT]）,
          "conclusion": 配置建议与核心风险,
          "full_text":  完整纯文本（所有章节合并）
        }
    """
    basic  = report.basic
    m      = report.index_metrics
    charts = report.chart_data

    if not m:
        return _fallback_report(basic)

    cm = m.common

    # ── 基础数据提取 ─────────────────────────────────────────
    fund_name    = basic.name
    index_name   = basic.benchmark_text or "目标指数"
    grade        = m.tool_grade
    tool_score   = round(m.tool_score, 1)
    tags         = report.tags or []

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 收益数据
    cum_fund     = round(cm.cumulative_return * 100, 2)
    ann_ret      = round(cm.annualized_return * 100, 2)

    # 基准累计收益（从 chart_data 取）
    bm_info      = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm       = round(bm_info.get("bm_last_return", 0) * 100, 2)
    is_tr        = bm_info.get("is_total_return", False)

    # 跟踪误差
    te_daily     = round(m.tracking_error * 100, 4)          # 日化（%）
    te_annual    = round(m.tracking_error_annualized * 100, 2)  # 年化（%）
    correlation  = round(m.correlation, 4)
    ir_value     = round(m.information_ratio, 2)

    # 超额 / 成本
    enhanced_ret = round(m.enhanced_return * 100, 2)         # 增强收益（打新+替代）
    ter          = round(m.total_expense_ratio * 100, 3)      # 总费率
    cash_drag    = round(m.cash_drag * 100, 3)                # 现金拖累
    rebalance    = round(m.rebalance_impact * 100, 3)         # 调仓冲击

    # 折溢价
    pd_mean      = round(m.premium_discount_mean * 100, 3)    # %
    pd_std       = round(m.premium_discount_std * 100, 3)     # %
    pd_grade     = m.premium_discount_grade                    # 优秀/良好/一般/较差

    # 累计偏离
    cum_deviation = round(cum_fund - cum_bm, 2)

    # ── 统计年数 ─────────────────────────────────────────────
    year_count = _year_count(start_date, end_date)

    # ── 各章节生成 ───────────────────────────────────────────
    meta = {
        "fund_name":  fund_name,
        "index_name": index_name,
        "start_date": start_date,
        "end_date":   end_date,
        "grade":      grade,
        "tool_score": tool_score,
        "tags":       tags,
    }

    headline = _build_headline(grade, fund_name, index_name, tool_score, start_date, end_date)

    section1 = _section1_nav_fitting(
        fund_name, index_name, start_date, end_date,
        cum_fund, cum_bm, cum_deviation, ann_ret, is_tr,
        correlation, te_annual
    )

    section2 = _section2_tracking_error(
        fund_name, te_daily, te_annual,
        enhanced_ret, ter, cash_drag, rebalance,
        correlation, ir_value, cum_deviation
    )

    section3 = _section3_liquidity_cost(
        fund_name, index_name,
        pd_mean, pd_std, pd_grade,
        basic
    )

    conclusion = _section4_conclusion(
        fund_name, index_name, grade, tool_score, tags,
        te_annual, pd_grade, enhanced_ret, ann_ret, year_count,
        m
    )

    full_text = "\n\n".join([headline, section1, section2, section3, conclusion])

    return {
        "meta":       meta,
        "headline":   headline,
        "section1":   section1,
        "section2":   section2,
        "section3":   section3,
        "conclusion": conclusion,
        "full_text":  full_text,
    }


# ============================================================
# 各章节生成函数
# ============================================================

def _build_headline(grade: str, fund_name: str, index_name: str,
                    tool_score: float, start_date: str, end_date: str) -> str:
    """构建标题行（markdown）"""
    grade_emoji = {"A+": "🏆", "A": "⭐", "B": "📊", "C": "⚠️", "D": "❌"}.get(grade, "📊")
    grade_label = {
        "A+": "极高精度工具",
        "A":  "高效指数工具",
        "B":  "标准跟踪工具",
        "C":  "跟踪偏差偏大",
        "D":  "工具属性较弱",
    }.get(grade, "指数工具")

    return (
        f"## {grade_emoji} [指数/ETF深度评级] 精准贝塔与效率工具：{fund_name}跟踪精度穿透报告\n\n"
        f"> **分析对象：** {fund_name}  \n"
        f"> **跟踪标的：** {index_name}  \n"
        f"> **统计区间：** {start_date} ~ {end_date}  \n"
        f"> **工具评级：** {grade}（{grade_label}）  工具评分：{tool_score:.1f}分\n\n"
        f"---"
    )


def _section1_nav_fitting(
    fund_name, index_name, start_date, end_date,
    cum_fund, cum_bm, cum_deviation, ann_ret, is_tr,
    correlation, te_annual
) -> str:
    """一、净值拟合分析：贝塔收益的"影子"效应"""

    bm_type = "全收益（含分红再投资）" if is_tr else "价格指数"

    # 偏离程度定性
    if abs(cum_deviation) <= 0.5:
        fit_desc   = f"高度重合，累计偏离仅 **{cum_deviation:+.2f}个百分点**"
        fit_quality = "净值与指数如同影子与实体，几乎完全重叠"
    elif abs(cum_deviation) <= 2.0:
        fit_desc   = f"拟合良好，累计偏离 **{cum_deviation:+.2f}个百分点**"
        fit_quality = "净值对指数的复刻精度优秀，仅存在少量成本摩擦"
    elif abs(cum_deviation) <= 5.0:
        fit_desc   = f"存在一定偏离，累计偏差 **{cum_deviation:+.2f}个百分点**"
        fit_quality = "净值偏离指数较明显，调仓损益控制有待优化"
    else:
        fit_desc   = f"偏离较大，累计差距 **{cum_deviation:+.2f}个百分点**"
        fit_quality = "基金净值与指数表现出现显著背离，工具属性受损"

    # 相关性描述
    if correlation >= 0.999:
        corr_desc = "相关系数高达 **{:.4f}**，呈现近乎完美的贝塔跟随".format(correlation)
    elif correlation >= 0.995:
        corr_desc = "相关系数 **{:.4f}**，拟合质量优秀".format(correlation)
    elif correlation >= 0.98:
        corr_desc = "相关系数 **{:.4f}**，拟合质量良好".format(correlation)
    else:
        corr_desc = "相关系数 **{:.4f}**，与指数存在一定的结构性差异".format(correlation)

    # 年化跟踪描述
    if te_annual <= 0.5:
        te_desc = "年化跟踪误差极低（**{:.2f}%**），处于业内顶级水平".format(te_annual)
    elif te_annual <= 1.0:
        te_desc = "年化跟踪误差较低（**{:.2f}%**），复刻精度优秀".format(te_annual)
    elif te_annual <= 2.0:
        te_desc = "年化跟踪误差适中（**{:.2f}%**），处于行业中等水平".format(te_annual)
    else:
        te_desc = "年化跟踪误差偏高（**{:.2f}%**），值得关注".format(te_annual)

    return f"""### 一、净值拟合分析：贝塔收益的「影子」效应

指数基金的首要使命是**复刻**——让投资者在看好某个市场方向时，能够不折不扣地获得指数应有的涨幅，而不是被经理的主动判断稀释或偏移。

**数据表现：**

统计期（{start_date} ~ {end_date}）内，**{fund_name}**的累计收益率为 **{cum_fund:.2f}%**，对应目标指数（{bm_type}基准）累计收益率为 **{cum_bm:.2f}%**，二者{fit_desc}。

[INSERT_CHART: CUM_RET]

**解读：**

累计收益曲线显示，{fit_quality}。{corr_desc}。{te_desc}。

这种「影子式」的跟随能力，意味着当市场上涨时，基金净值能够同步兑现指数收益；当市场下跌时，基金同样如实传导风险——这正是纯粹工具属性的核心体现。"""


def _section2_tracking_error(
    fund_name, te_daily, te_annual,
    enhanced_ret, ter, cash_drag, rebalance,
    correlation, ir_value, cum_deviation
) -> str:
    """二、跟踪偏离度与精细化管理"""

    # 日均偏离幅度（±范围）
    daily_range = round(te_daily * 2, 4)  # ±2σ 覆盖95%

    # 超额收益归因
    if enhanced_ret > 0.3:
        alpha_src    = "打新收益或成分股替代策略"
        alpha_detail = (
            f"值得注意的是，该基金年化超额收益为 **+{enhanced_ret:.2f}%**，"
            f"这一正超额并非来自经理的选股眼光，"
            f"而通常源于**打新收益**（新股申购回报）或**成分股替代策略**（在指数成份股调整期间，"
            f"提前布局替代标的获取调仓收益）。这是被动管理框架内合法且可持续的效率来源。"
        )
    elif enhanced_ret > 0:
        alpha_src    = "费率节约或现金管理优化"
        alpha_detail = (
            f"基金年化超额收益为 **+{enhanced_ret:.2f}%**，"
            f"主要来源于对冲总费率（**{ter:.3f}%**）后仍保持的微量正偏离，"
            f"体现了经理在现金管理和调仓损益控制方面的精细化运营能力。"
        )
    elif enhanced_ret > -0.2:
        alpha_src    = "费率摩擦"
        alpha_detail = (
            f"基金年化超额收益为 **{enhanced_ret:+.2f}%**，"
            f"与总费率（**{ter:.3f}%**）基本持平，"
            f"属于被动基金正常的摩擦成本范围。现金拖累（**{cash_drag:.3f}%**）"
            f"和调仓冲击（**{rebalance:.3f}%**）均在可控区间内。"
        )
    else:
        alpha_src    = "成本超支或调仓冲击过大"
        alpha_detail = (
            f"基金年化超额收益为 **{enhanced_ret:+.2f}%**，"
            f"明显低于正常摩擦成本预期，表明存在**成本管理问题**。"
            f"现金拖累（**{cash_drag:.3f}%**）或调仓冲击损益（**{rebalance:.3f}%**）偏高，"
            f"需关注指数成份股调整期间的执行效率。"
        )

    # IR 描述
    if ir_value > 0.5:
        ir_desc = f"信息比率（IR = **{ir_value:.2f}**）表现优秀，超额回报的稳定性较高"
    elif ir_value > 0:
        ir_desc = f"信息比率（IR = **{ir_value:.2f}**）为正，超额稳定性一般"
    else:
        ir_desc = f"信息比率（IR = **{ir_value:.2f}**）为负，提示跟踪损耗相对偏大"

    return f"""### 二、跟踪偏离度与精细化管理：极致的精度控制

跟踪误差（Tracking Error）是衡量指数基金运营水平的核心指标——它反映了经理在成份股调仓、申赎应对、分红处理等环节的操作精细化程度。

**数据表现：**

日均跟踪偏离度分布在 [**±{daily_range:.4f}%**] 区间内（±2σ覆盖），年化跟踪误差为 **{te_annual:.2f}%**。

[INSERT_CHART: TRACKING_ERROR_SCATTER]

**解读：**

在跟踪偏离度散点图中，数据点密集分布于零轴附近，说明经理的日常操作对指数的干扰极小。这背后的驱动力包括：（1）成份股权重的精确复刻；（2）指数调仓期间的最小化冲击执行；（3）分红收益的及时再投入。

**超额收益归因（{alpha_src}）：**

{alpha_detail}

{ir_desc}。从总费率（**{ter:.3f}%**）来看，该基金的持有成本处于{"行业领先" if ter <= 0.15 else "合理" if ter <= 0.5 else "偏高"}水平。"""


def _section3_liquidity_cost(
    fund_name, index_name,
    pd_mean, pd_std, pd_grade,
    basic
) -> str:
    """三、场内流动性与交易成本"""

    # 是否 ETF（有场内交易数据）
    is_etf = "ETF" in basic.type_raw.upper() or "交易型" in basic.type_raw

    # 对于场外基金（无折溢价数据），直接返回场外基金的说明
    if not is_etf:
        etf_remark = (
            "该品种以场外申赎为主要交易方式，T+1/T+2 到账，"
            "适合长期持有的配置型投资者，流动性限制相对较低。"
        )
        return f"""### 三、场内流动性与交易成本：隐形门槛的穿透分析

对于开放式基金而言，主要以场外申赎为主，无场内买卖折溢价。

**产品特征：**

该品种属于 **场外开放式基金**，投资者可通过场外渠道（银行、基金公司、第三方销售平台）进行申购和赎回，T+1/T+2 到账结算。

{etf_remark}

**实操建议：** 建议通过定期定投或大额一次性购入的方式参与，成本相对透明，适合长期持有。"""
    
    # 以下是 ETF（场内交易品种）的折溢价分析
    # 折溢价描述
    if pd_grade == "优秀":
        pd_verdict  = "折溢价控制极为精准"
        pd_detail   = (
            "套利机制运转高效，二级市场价格能够实时、准确地反映底层资产净值，"
            "投资者几乎不需要承担额外的「情绪溢价」或「流动性折价」。"
        )
        pd_advice   = "大额投资者可放心使用场内买入策略，摩擦成本极低。"
    elif pd_grade == "良好":
        pd_verdict  = "折溢价控制良好"
        pd_detail   = (
            "套利机制基本有效，偶发性折溢价在正常交易日内能够快速收敛。"
            "极端市场情绪下可能出现短暂的价格偏离。"
        )
        pd_advice   = "建议采用限价单成交，避免在流动性较差时段的市价买入。"
    elif pd_grade == "一般":
        pd_verdict  = "折溢价偏离有所放大"
        pd_detail   = (
            "套利机制存在一定时滞，价格对净值的追踪不够及时。"
            "在市场波动较大时段，可能出现较明显的溢价或折价。"
        )
        pd_advice   = "建议场外申购赎回替代场内买卖，以规避二级市场摩擦。"
    else:
        pd_verdict  = "折溢价偏离较大"
        pd_detail   = (
            "套利效率较低，二级市场价格与净值之间存在系统性偏差，"
            "这通常与该品种的市场活跃度较低有关。"
        )
        pd_advice   = "强烈建议通过场外渠道申购，避免场内交易的额外损耗。"

    # 折溢价均值方向
    if pd_mean > 0.05:
        pd_mean_desc = f"均值溢价 **+{pd_mean:.3f}%**，市场对该 ETF 存在一定需求溢价"
    elif pd_mean < -0.05:
        pd_mean_desc = f"均值折价 **{pd_mean:.3f}%**，存在场内价格低于净值的系统性现象"
    else:
        pd_mean_desc = f"折溢价均值接近零（**{pd_mean:+.3f}%**），价格与净值高度一致"

    etf_remark = (
        "作为场内交易品种，该 ETF 支持 T+0 日内交易，"
        "具备灵活调仓的天然优势，适合日内套利和网格策略操作者。"
    )

    return f"""### 三、场内流动性与交易成本：隐形门槛的穿透分析

对于场内基金而言，折溢价率与成交流动性直接决定了投资者的「进出效率」——这是衡量工具属性优劣的关键维度。

**数据表现：**

折溢价率均值为 **{pd_mean:+.3f}%**，标准差为 **±{pd_std:.3f}%**，综合评级：**{pd_grade}**。

[INSERT_CHART: PREMIUM_DISCOUNT]

**解读：**

{pd_mean_desc}，波动标准差 ±{pd_std:.3f}%，{pd_verdict}。{pd_detail}

{etf_remark}

**实操建议：** {pd_advice}"""


def _section4_conclusion(
    fund_name, index_name, grade, tool_score, tags,
    te_annual, pd_grade, enhanced_ret, ann_ret, year_count,
    m
) -> str:
    """四、综合结论与配置建议"""

    # 工具等级定性
    if grade == "A+":
        tool_label     = "**「高精度、高流动性、低摩擦」**的顶级配置工具"
        tool_summary   = "完美对标核心资产的整体表现，剔除了人为选股的随机性。"
        alloc_ratio    = "20%~50%"
        alloc_scenario = "定投、网格交易、核心卫星策略的核心持仓"
    elif grade == "A":
        tool_label     = "**「高效、稳健、低成本」**的优质指数工具"
        tool_summary   = "跟踪精度优秀，适合作为核心配置仓位。"
        alloc_ratio    = "15%~40%"
        alloc_scenario = "核心仓位配置、定期定额投资策略"
    elif grade == "B":
        tool_label     = "**「标准化、可用性强」**的普通指数工具"
        tool_summary   = "跟踪质量符合标准，满足大多数配置需求。"
        alloc_ratio    = "10%~30%"
        alloc_scenario = "大类资产配置中的beta暴露工具"
    else:
        tool_label     = "**「需审慎选择」**的指数工具"
        tool_summary   = "跟踪误差偏大或折溢价偏高，使用前需评估替代品。"
        alloc_ratio    = "5%~15%"
        alloc_scenario = "暂不建议大比例配置，建议对比同类竞品"

    # 核心风险点
    risk_points = []
    risk_points.append(
        "**被动β风险**：由于纯粹的被动属性，当目标指数系统性下跌时，"
        "基金将承担100%的指数损失，不提供任何主动防御缓冲。"
    )
    if te_annual > 1.0:
        risk_points.append(
            f"**跟踪误差风险**：年化跟踪误差 {te_annual:.2f}% 相对偏高，"
            "在指数大幅上涨阶段可能出现「指数涨而基金跑输」的现象。"
        )
    if pd_grade in ("一般", "较差"):
        risk_points.append(
            "**折溢价风险**：场内折溢价控制欠佳，极端行情下存在以高价买入或低价卖出的风险。"
        )
    if enhanced_ret < -0.3:
        risk_points.append(
            f"**成本拖累风险**：年化净超额为 {enhanced_ret:+.2f}%，"
            "长期持有将持续承受成本侵蚀，建议关注费率更低的同类竞品。"
        )

    risk_text = "\n".join([f"- {r}" for r in risk_points])

    # 标签展示
    tag_line = "、".join(tags) if tags else "工具型、指数基金"

    return f"""---

### 四、综合结论与配置建议

#### 工具评价

{fund_name}是一个{tool_label}。{tool_summary}综合工具评分 **{tool_score:.1f}分**（满分100），工具评级 **{grade}**。

基金核心标签：{tag_line}

#### 核心风险点

{risk_text}

#### 配置建议

**适合人群：** 希望通过定投、网格交易或大类资产配置捕捉 [{index_name}] 收益的投资者。

**建议仓位：** 在投资组合中配置 **{alloc_ratio}** 作为{alloc_scenario}，不建议集中押注单一指数。

**使用场景：** 
- 📌 **定期定额（定投）**：用于摊平持仓成本，长期捕捉指数上行贝塔
- 📌 **网格交易**：借助折溢价波动或净值波动区间，低买高卖积累收益  
- 📌 **核心卫星配置**：作为"核心仓位"锁定市场整体回报，卫星仓位配置主动管理基金

> ⚠️ **重要提示**：指数基金不提供保本保障，当目标指数下跌时，基金将同步承受亏损。请在充分了解所跟踪指数的行业/风格特征后，结合自身风险偏好合理配置。"""


# ============================================================
# 辅助函数
# ============================================================

def _extract_date_range(charts: dict) -> tuple:
    """从 chart_data 中提取起止日期"""
    try:
        x_list = charts.get("cumulative_return", {}).get("x", [])
        if x_list and len(x_list) >= 2:
            return str(x_list[0])[:10], str(x_list[-1])[:10]
    except Exception:
        pass
    return "N/A", "N/A"


def _year_count(start_date: str, end_date: str) -> float:
    """估算统计年数"""
    try:
        from datetime import datetime
        fmt = "%Y-%m-%d"
        d1 = datetime.strptime(start_date[:10], fmt)
        d2 = datetime.strptime(end_date[:10], fmt)
        return max((d2 - d1).days / 365.25, 0.1)
    except Exception:
        return 1.0


def _fallback_report(basic: Any) -> dict:
    """数据不足时的降级报告"""
    name = getattr(basic, "name", "该基金")
    msg  = (
        f"## ⚠️ {name} — 数据不足，无法生成完整报告\n\n"
        f"> 当前缺少指数/ETF 效率分析数据（IndexMetrics），"
        f"请检查 pipeline 是否正确识别基金类型并完成分析。"
    )
    return {
        "meta":       {},
        "headline":   msg,
        "section1":   "",
        "section2":   "",
        "section3":   "",
        "conclusion": "",
        "full_text":  msg,
    }
