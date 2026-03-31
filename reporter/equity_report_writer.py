"""
权益类基金深度评价报告生成器 — fund_quant_v2
角色：资深基金分析师（CFA 持证人）
报告结构：5板块 + 图表插入点标记
  1. 收益曲线
  2. 持仓穿透
  3. 深度分析（Brinson 归因）
  4. 风险预警（压力测试）
  5. 投资建议（买入逻辑 / 持有体感 / 离场信号）
"""

from __future__ import annotations
from typing import Any


# ============================================================
# 主入口
# ============================================================

def generate_equity_deep_report(report: Any) -> dict:
    """
    生成权益类基金深度评价报告（新5板块结构）

    Returns:
        {
          "meta":       {fund_name, fund_type, start_date, end_date, grade, score, tags},
          "headline":   标题行,
          "section1":   一、收益曲线（含 [INSERT_CHART: CUM_RET]）,
          "section2":   二、持仓穿透（含 [DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]）,
          "section3":   三、深度分析：Brinson 归因（含 [INSERT_CHART: BRINSON]）,
          "section4":   四、风险预警：压力测试（含 [INSERT_CHART: DRAWDOWN]）,
          "section5":   五、投资建议（买入逻辑 / 持有体感 / 离场信号）,
          "full_text":  完整纯文本,
        }
    """
    basic = report.basic
    m = report.equity_metrics
    charts = report.chart_data

    if not m:
        return _fallback_report(basic)

    # ── 提取核心数据 ──────────────────────────────────────
    cm = m.common
    fund_name = basic.name
    fund_type = _fund_type_label(report.fund_type)
    grade = m.score_grade
    score = m.overall_score
    tags = report.tags or []

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 收益数据
    cum_fund = round(cm.cumulative_return * 100, 1)
    ann_ret  = round(cm.annualized_return * 100, 1)

    # 基准数据
    bm_info = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm_tr      = round(bm_info.get("bm_last_return", 0) * 100, 1)
    is_total_return = bm_info.get("is_total_return", False)

    # 分红贡献估算
    div_contribution = _estimate_div_contribution(basic, start_date, end_date)

    # Alpha
    net_alpha = round(m.alpha * 100, 1)
    ir_value  = round(m.information_ratio, 2)

    # 风险数据
    max_dd_fund = round(cm.max_drawdown * 100, 1)
    dd_info     = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm   = round(dd_info.get("bm_max_dd", 0), 1)
    recovery_days = cm.recovery_days or 0

    # 超额曲线数据
    ex_info     = charts.get("excess_return", {}).get("excess_info", {})
    last_excess = round(ex_info.get("last_excess", net_alpha), 1)
    curve_trend = ex_info.get("curve_trend", "震荡上行")
    excess_std  = round(ex_info.get("excess_std", 0), 2)

    # 月度数据
    monthly_win_rate = round(cm.monthly_win_rate * 100, 1)

    # 风格因子
    smb = m.factor_loadings.get("SMB", 0)
    hml = m.factor_loadings.get("HML", 0)
    beta_val = m.beta
    r2 = m.r_squared

    # 热力图数据
    heatmap_info = charts.get("monthly_heatmap", {}).get("heatmap_info", {})
    annual_stats = heatmap_info.get("annual_stats", {})

    # Brinson 归因数据
    brinson = getattr(m, 'brinson', {}) or {}

    # ── 生成各章节 ────────────────────────────────────────
    meta = {
        "fund_name": fund_name,
        "fund_type": fund_type,
        "start_date": start_date,
        "end_date": end_date,
        "grade": grade,
        "score": score,
        "tags": tags,
    }

    # 板块1：收益曲线
    section1 = _section1_return_curve(
        fund_name, fund_type, start_date, end_date,
        cum_fund, cum_bm_tr, is_total_return,
        div_contribution, net_alpha, ann_ret,
        smb, hml, basic
    )

    # 板块2：持仓穿透（Top 3 贡献股 + 深度持仓占位符）
    top_contributors = _section_top_contributors(
        report, fund_name, start_date, end_date
    )
    section2 = f"""### 二、持仓穿透

{top_contributors}

[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]"""

    # 板块3：深度分析（Brinson 归因）
    section3 = _section3_brinson_attrition(
        fund_name, brinson, net_alpha,
        smb, hml, r2, ir_value,
        last_excess, curve_trend, excess_std, monthly_win_rate
    )

    # 板块4：风险预警（压力测试 + 水下回撤）
    section4 = _section4_risk_stress_test(
        fund_name, max_dd_fund, max_dd_bm,
        dd_info, recovery_days, cm,
        beta_val, r2, ann_ret
    )

    # 板块5：投资建议（买入逻辑 / 持有体感 / 离场信号）
    section5 = _section5_investment_advice(
        fund_name, grade, score, tags,
        net_alpha, max_dd_fund, monthly_win_rate,
        ir_value, m, basic
    )

    # 标题行
    headline = _build_headline(grade, fund_name, tags, score, start_date, end_date)

    # 合并全文
    full_text = "\n\n".join([
        headline, section1, section2, section3, section4, section5
    ])

    return {
        "meta":       meta,
        "headline":   headline,
        "section1":   section1,
        "section2":   section2,
        "section3":   section3,
        "section4":   section4,
        "section5":   section5,
        "full_text":  full_text,
    }


# ============================================================
# 各章节生成
# ============================================================

def _section1_return_curve(
    fund_name, fund_type, start_date, end_date,
    cum_fund, cum_bm_tr, is_total_return,
    div_contribution, net_alpha, ann_ret,
    smb, hml, basic
) -> str:
    """一、收益曲线：全收益框架下的收益穿透"""

    bm_type = "全收益基准（含分红再投资）" if is_total_return else "价格指数基准"

    # 超额描述
    excess = round(cum_fund - cum_bm_tr, 1)
    if excess > 10:
        excess_desc = f"累计跑赢基准 **{excess:.1f}个百分点**"
        alpha_quality = "选股能力显著，经理的超额收益来源清晰"
    elif excess > 3:
        excess_desc = f"累计跑赢基准 **{excess:.1f}个百分点**"
        alpha_quality = "具备一定选股能力，超额收益来源可辨识"
    elif excess > 0:
        excess_desc = f"小幅跑赢基准 {excess:.1f}个百分点"
        alpha_quality = "超额空间有限，经理主动管理价值待进一步验证"
    else:
        excess_desc = f"落后基准 {abs(excess):.1f}个百分点"
        alpha_quality = "未能跑赢基准，需关注经理选股逻辑是否与市场匹配"

    # 分红贡献描述
    if div_contribution > 2.0 and is_total_return:
        div_desc = (f"值得注意的是，{bm_type}中包含约 **{div_contribution:.1f}%** 的分红再投资补偿，"
                    f"这意味着若仅以价格指数衡量，基金的\"表面超额\"将被高估约 {div_contribution:.1f}个百分点。"
                    f"剔除分红因素后，经理通过纯粹选股贡献的真实 Alpha 约为 **{net_alpha:.1f}%**。")
    elif div_contribution > 0:
        div_desc = (f"全收益基准包含约 {div_contribution:.1f}% 的分红再投资收益。"
                    f"在此严格标准下，经理真实Alpha为 **{net_alpha:.1f}%**。")
    else:
        div_desc = "统计期内行业分红贡献有限，超额收益主要反映经理的真实选股能力。"

    # 年化描述
    if ann_ret > 15:
        ann_desc = f"年化收益率达 **{ann_ret:.1f}%**，处于同类产品的较高水平"
    elif ann_ret > 8:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，表现稳健"
    elif ann_ret > 0:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，整体偏保守"
    else:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，统计期内呈负值"

    text = f"""### 一、收益曲线

[INSERT_CHART: CUM_RET]

在评价权益类基金时，传统"价格指数"基准往往低估市场的真实回报，从而虚增基金经理的超额。本报告采用{bm_type}作为比较基准，将分红再投资收益纳入对比体系。

**数据表现**

统计区间 {start_date} 至 {end_date}，本基金累计收益率为 **{cum_fund:.1f}%**，对比{bm_type}的 {cum_bm_tr:.1f}%，{excess_desc}。{ann_desc}。

**深度解读**

{div_desc}

{alpha_quality}。曲线形态上，{"基金收益曲线在统计期内整体走势强于基准，呈现出稳健向上的斜率特征" if excess > 0 else "基金收益曲线阶段性落后于基准，显示经理在该市场环境下的适应性存在一定局限"}，反映了经理在 {_style_desc(smb, hml)} 风格上的持续投入。"""

    return text


def _section3_brinson_attrition(
    fund_name, brinson, net_alpha,
    smb, hml, r2, ir_value,
    last_excess, curve_trend, excess_std, monthly_win_rate
) -> str:
    """三、深度分析：Brinson 归因分解"""

    if not brinson:
        return """### 三、深度分析

**Brinson 归因数据暂不可用**

当前报告期内持仓数据不足以支撑 Brinson 归因计算，深度分析将在数据完备后自动生成。"""

    allocation = brinson.get('allocation', 0.0)
    selection = brinson.get('selection', 0.0)
    interaction = brinson.get('interaction', 0.0)
    total = brinson.get('total', 0.0)

    alloc_pct = round(allocation * 100, 2)
    select_pct = round(selection * 100, 2)
    inter_pct = round(interaction * 100, 2)
    total_pct = round(total * 100, 2)

    # 配置效应解读
    abs_alloc = abs(alloc_pct)
    if abs_alloc > 3:
        alloc_desc = f"配置效应贡献 **{alloc_pct:+.2f}%**，幅度显著，{'说明基金经理的行业轮动判断精准，超配了强势行业、低配了弱势行业' if alloc_pct > 0 else '行业配置方向与市场背离，超配的行业表现不佳，拖累了整体收益'}"
    elif abs_alloc > 1:
        alloc_desc = f"配置效应贡献 {alloc_pct:+.2f}%，幅度适中，经理的行业配置能力有一定体现"
    else:
        alloc_desc = f"配置效应贡献 {alloc_pct:+.2f}%，幅度较小，行业配置对超额的影响有限"

    # 选股效应解读
    abs_select = abs(select_pct)
    if abs_select > 3:
        select_desc = f"选股效应贡献 **{select_pct:+.2f}%**，{'经理在行业内选股能力突出，重仓股表现显著优于行业平均水平' if select_pct > 0 else '行业内选股出现偏差，重仓股跑输行业指数，需要审视选股逻辑的有效性'}"
    elif abs_select > 1:
        select_desc = f"选股效应贡献 {select_pct:+.2f}%，具备一定的个股挖掘能力"
    else:
        select_desc = f"选股效应贡献 {select_pct:+.2f}%，个股选择对超额贡献偏弱"

    # 交互效应解读
    inter_desc = f"交互效应贡献 {inter_pct:+.2f}%，{'行业配置与个股选择的协同效果为正' if inter_pct > 0 else '行业配置与个股选择存在一定的负向交叉'}"

    # 能力类型判定
    if abs(alloc_pct) > abs(select_pct) * 1.5:
        ability_type = "**行业轮动型**"
        ability_detail = "经理的核心竞争力在于行业配置而非个股挖掘，其超额收益主要来源于对行业景气度的前瞻判断和灵活的行业权重调整。投资者应关注其行业集中度和轮动频率。"
    elif abs(select_pct) > abs(alloc_pct) * 1.5:
        ability_type = "**个股挖掘型**"
        ability_detail = "经理的核心竞争力在于个股选择而非行业配置，其超额收益主要来源于在优势行业内精选出超越行业表现的个股。投资者应关注其持仓换手率和个股集中度。"
    else:
        ability_type = "**均衡型**"
        ability_detail = "经理在行业配置和个股选择上能力较为均衡，超额收益来源相对分散。这种风格在市场风格切换频繁时具有一定的抗冲击能力。"

    # 超额持续性评估（整合 IR 和超额曲线趋势）
    if ir_value > 0.5:
        persist_desc = f"信息比率（IR）为 **{ir_value:.2f}**，超额收益的获取效率较高，累积超额曲线呈**{curve_trend}**态势，月度胜率 **{monthly_win_rate:.1f}%**，超额持续性良好"
    elif ir_value > 0:
        persist_desc = f"信息比率（IR）为 {ir_value:.2f}，超额收益存在但稳定性有待提升，累积超额曲线呈**{curve_trend}**，月度胜率 {monthly_win_rate:.1f}%"
    else:
        persist_desc = f"信息比率（IR）为 {ir_value:.2f}，超额收益的可靠性偏低，累积超额曲线呈**{curve_trend}**，需关注超额能力的稳定性"

    # R² 模型拟合度
    if r2 > 0.7:
        r2_desc = f"R² 为 **{r2:.2f}**，因子模型解释力强，风格稳定性高"
    elif r2 > 0.4:
        r2_desc = f"R² 为 {r2:.2f}，因子模型解释力一般，经理具有一定的独立风格"
    else:
        r2_desc = f"R² 为 {r2:.2f}，因子模型解释力弱，该基金风格独立性较强"

    text = f"""### 三、深度分析

[INSERT_CHART: BRINSON]

[INSERT_CHART: EXCESS_ALPH]

通过 Brinson 归因模型，将基金的累计超额收益拆解为**配置效应**（行业选择）、**选股效应**（行业内个股挑选）和**交互效应**（两者协同），从而精准识别经理的 Alpha 来源。

**归因分解**

| 归因维度 | 贡献（%） | 解读 |
|---|---|---|
| 配置效应 | {alloc_pct:+.2f}% | {alloc_desc} |
| 选股效应 | {select_pct:+.2f}% | {select_desc} |
| 交互效应 | {inter_pct:+.2f}% | {inter_desc} |
| **合计超额** | **{total_pct:+.2f}%** | 经确认 Alpha 为 **{net_alpha:+.1f}%** |

**核心能力判定**

该基金经理属于 **{ability_type}**。{ability_detail}

**超额持续性评估**

{persist_desc}。{r2_desc}。

**能力边界**

从因子暴露分析（SMB={smb:.2f}, HML={hml:.2f}），{_style_edge_desc(smb, hml)}。"""

    return text


def _section4_risk_stress_test(
    fund_name, max_dd_fund, max_dd_bm,
    dd_info, recovery_days, cm,
    beta_val, r2, ann_ret
) -> str:
    """四、风险预警：压力测试与回撤分析"""

    # 防御能力
    if max_dd_bm != 0:
        defense_ratio = abs(max_dd_fund / max_dd_bm) if max_dd_bm != 0 else 1.0
    else:
        defense_ratio = 1.0

    if defense_ratio < 0.8:
        defense_quality = f"最大回撤 **{abs(max_dd_fund):.1f}%**，明显小于全收益基准的 {abs(max_dd_bm):.1f}%，回撤缩减率达 **{(1-defense_ratio)*100:.0f}%**，防御能力突出"
    elif defense_ratio < 1.0:
        defense_quality = f"最大回撤 {abs(max_dd_fund):.1f}%，略好于全收益基准的 {abs(max_dd_bm):.1f}%，具有一定防御韧性"
    elif defense_ratio < 1.2:
        defense_quality = f"最大回撤 {abs(max_dd_fund):.1f}%，与全收益基准的 {abs(max_dd_bm):.1f}% 基本持平，防守下限接近市场水平"
    else:
        defense_quality = f"最大回撤 {abs(max_dd_fund):.1f}%，大于全收益基准的 {abs(max_dd_bm):.1f}%，防御能力偏弱，波动放大特征明显"

    # 修复弹性
    if recovery_days and recovery_days > 0:
        if recovery_days < 30:
            recovery_desc = f"从最低点到回血完毕仅用 **{recovery_days} 个交易日**，修复弹性极强"
        elif recovery_days < 90:
            recovery_desc = f"回撤修复历时 **{recovery_days} 个交易日**，修复节奏适中"
        elif recovery_days < 180:
            recovery_desc = f"回撤修复历时 **{recovery_days} 个交易日**，属于 U 型磨底"
        else:
            recovery_desc = f"最长回撤修复周期达 **{recovery_days} 个交易日**，长期处于水下"
    else:
        recovery_desc = "当前基金仍处于回撤修复阶段，尚未完全回血"

    # 波动率
    vol = round(cm.volatility * 100, 1)
    if vol < 15:
        vol_desc = f"年化波动率 {vol:.1f}%，属于低波动品种"
    elif vol < 25:
        vol_desc = f"年化波动率 {vol:.1f}%，波动处于合理区间"
    else:
        vol_desc = f"年化波动率 {vol:.1f}%，波动较高"

    # 夏普比率
    sharpe = cm.sharpe_ratio
    if sharpe > 1.5:
        sharpe_desc = f"夏普比率 **{sharpe:.2f}**（卓越）"
    elif sharpe > 1.0:
        sharpe_desc = f"夏普比率 {sharpe:.2f}（良好）"
    elif sharpe > 0.5:
        sharpe_desc = f"夏普比率 {sharpe:.2f}（一般）"
    else:
        sharpe_desc = f"夏普比率 {sharpe:.2f}（偏低）"

    # 压力测试（基于历史情景模拟）
    stress_tests = _stress_test_scenarios(max_dd_fund, beta_val, vol, ann_ret)

    # 回撤原因分析
    drawdown_cause = _analyze_drawdown_cause(dd_info, max_dd_fund, beta_val)

    text = f"""### 四、风险预警

[INSERT_CHART: DRAWDOWN]

回撤不仅是风险，更是经理心理素质与调仓效率的试金石。本板块通过水下回撤图和历史压力测试，评估基金的极端风险承受能力。

**回撤概况**

{defense_quality}。{recovery_desc}。{vol_desc}，{sharpe_desc}。

Beta 值为 {beta_val:.2f}，{"高于1的弹性特征意味着市场上涨时跑赢，但下跌时回撤也更大" if beta_val > 1.1 else "略低于1的防守特征与其回撤控制能力吻合" if beta_val < 0.9 else "与市场波动基本同步"}。

**压力测试（历史情景模拟）**

{stress_tests}

**回撤原因分析**

{drawdown_cause}"""

    return text


def _section5_investment_advice(
    fund_name, grade, score, tags,
    net_alpha, max_dd_fund, monthly_win_rate,
    ir_value, m, basic
) -> str:
    """五、投资建议：买入逻辑 / 持有体感 / 离场信号"""

    # 经理画像
    portrait = _manager_portrait(net_alpha, abs(max_dd_fund), monthly_win_rate, ir_value, m)

    # 风险点
    risk_point = _identify_risk_point(m, abs(max_dd_fund))

    tag_str = "、".join([f"「{t}」" for t in tags[:3]]) if tags else "综合型"

    # 成本项披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    text = f"""### 五、投资建议

**经理画像**

该经理贴标签为 {tag_str}。{portrait}

**避坑指南**

1. 不要"高位补仓"：
很多人在亏损 5% 时疯狂加仓，结果亏损 20% 时没钱了。加仓要选在"无人问津"时，而非"刚开始跌"时。
2. 不要"频繁看盘"：
股票型和混合型基金的波动是常态。如果你每天看十次净值，你的心态一定会崩，从而做出"低卖高买"的非理性操作。建议：每周复盘一次足矣。
3. 不要"抄底"：
不要因为某只基跌得多就去抄底。如果该基金是因为经理水平差、逻辑破损而下跌，它就是一只"破船"，补仓只会让你沉得更快。

**核心风险点**

{risk_point}

---

**成本项披露：**

- 管理费率：{mgmt_fee:.2f}%
- 托管费率：{custody_fee:.2f}%
- 最大申购费率：{purchase_fee:.2f}%
- 最大赎回费率：{redeem_fee:.2f}%"""

    return text


# ============================================================
# 持仓分析章节（新增）
# ============================================================




# ============================================================
# 辅助函数
# ============================================================

def _build_headline(grade, fund_name, tags, score, start_date, end_date) -> str:
    """构建深度报告标题行"""
    emoji = {"A+": "🏆", "A": "✅", "B": "📊", "C": "🟡", "D": "❌"}.get(grade, "📊")
    tag_str = " · ".join(tags[:3]) if tags else "综合型"
    return (
        f"# {emoji} {fund_name} — 深度评级报告\n"
        f"**分析区间**：{start_date} 至 {end_date} ｜ "
        f"**综合评分**：{score:.0f} 分 / {grade} 级 ｜ "
        f"**核心标签**：{tag_str}"
    )


def _extract_date_range(charts: dict) -> tuple[str, str]:
    """从图表数据中提取起止日期字符串"""
    cum_data = charts.get("cumulative_return", {})
    x_dates = cum_data.get("x", [])
    if x_dates and len(x_dates) >= 2:
        start = str(x_dates[0])[:10]
        end   = str(x_dates[-1])[:10]
        return start, end
    return "—", "—"


def _estimate_div_contribution(basic, start_date, end_date) -> float:
    """估算统计期内分红贡献（行业股息率 × 年数）"""
    try:
        # 解析年数
        if start_date != "—" and end_date != "—":
            from datetime import datetime
            s = datetime.strptime(start_date[:10], "%Y-%m-%d")
            e = datetime.strptime(end_date[:10], "%Y-%m-%d")
            years = (e - s).days / 365.25
        else:
            years = 3.0

        # 尝试从benchmark_parsed获取行业信息
        # 申万行业平均股息率（保守估计3%）
        benchmark_text = (basic.benchmark_text or "").lower()
        if any(k in benchmark_text for k in ["银行", "煤炭", "钢铁", "电力"]):
            annual_div = 0.05  # 高分红行业
        elif any(k in benchmark_text for k in ["医药", "消费", "食品"]):
            annual_div = 0.02
        elif any(k in benchmark_text for k in ["科技", "计算机", "电子"]):
            annual_div = 0.01
        else:
            annual_div = 0.025  # 默认

        return round(annual_div * years * 100, 1)
    except Exception:
        return 0.0


def _stress_test_scenarios(
    max_dd_fund: float, beta_val: float,
    vol: float, ann_ret: float
) -> str:
    """
    压力测试：基于历史情景模拟基金在极端市场环境下的表现。

    使用简化模型：预估回撤 = max_dd_fund × (scenario_beta / actual_beta) × scenario_multiplier
    """
    scenarios = [
        ("2015年股灾（-45%）", -0.45, 1.8),
        ("2018年贸易战（-30%）", -0.30, 1.2),
        ("2020年疫情冲击（-15%）", -0.15, 1.0),
        ("2022年加息潮（-25%）", -0.25, 1.1),
    ]

    lines = ["| 历史情景 | 市场跌幅 | 预估基金回撤 | 评估 |", "|---|---|---|---|"]

    for name, mkt_drop, multiplier in scenarios:
        # 预估：基金回撤 ≈ max_dd_fund × (情景/实际) × 乘数
        beta_adj = beta_val if beta_val > 0.3 else 0.3  # 下限保护
        est_dd = abs(max_dd_fund) * beta_adj * multiplier
        est_dd = min(est_dd, 65)  # 上限保护

        if est_dd < 15:
            eval_text = "✅ 抗压"
        elif est_dd < 25:
            eval_text = "🟡 一般"
        elif est_dd < 40:
            eval_text = "⚠️ 承压"
        else:
            eval_text = "🔴 严重"

        lines.append(f"| {name} | {mkt_drop*100:.0f}% | ≈{est_dd:.1f}% | {eval_text} |")

    # 压力测试总结
    avg_dd = abs(max_dd_fund) * beta_val * 1.2  # 平均压力水平
    if avg_dd < 20:
        summary = "综合压力测试结果表明，该基金在多数极端市场情景下预估回撤控制在 20% 以内，抗风险能力较强。"
    elif avg_dd < 35:
        summary = "该基金在极端市场情景下预估回撤在 20%-35% 区间，属于正常主动权益产品的风险水平，建议投资者做好仓位管理。"
    else:
        summary = "该基金在极端市场情景下预估回撤可能超过 35%，高波动特征明显，仅适合风险承受能力较强的投资者。"

    return "\n".join(lines) + f"\n\n{summary}"


def _fund_type_label(fund_type: str) -> str:
    """将 framework_id 转换为中文显示标签"""
    mapping = {
        "stock": "股票型",
        "hybrid_equity": "混合型-偏股",
        "hybrid_balanced": "混合型-平衡",
        "hybrid_flexible": "混合型-灵活",
        "hybrid_absreturn": "混合型-绝对收益",
        "hybrid_bond": "混合型-偏债",
        "equity": "权益类",
        "mixed": "混合型",
        "sector": "行业/主题型",
    }
    return mapping.get(fund_type, "权益类")


def _style_desc(smb: float, hml: float) -> str:
    """简短风格描述"""
    size = "小盘" if smb > 0.3 else ("大盘" if smb < -0.3 else "中盘")
    style = "成长" if hml < -0.3 else ("价值" if hml > 0.3 else "均衡")
    return f"{size}{style}"


def _style_edge_desc(smb: float, hml: float) -> str:
    """能力边界描述"""

    if hml < -0.3:
        return "从超额曲线的斜率分析，经理在**成长风格占优**的市场环境中 Alpha 创造能力最强，选股爆发力突出；而在价值/红利风格主导的行情中，超额趋于平缓甚至出现回落"
    elif hml > 0.3:
        return "经理在**价值风格占优**的市场中（低估值、高分红）超额生成效率最高；在成长/科技风格领跑时，Alpha 可能有所消退"
    else:
        return "经理风格相对均衡，在成长与价值的切换中均能保持一定超额，但在极端单一风格行情中，超额创造能力有一定的局限性"


def _style_opposite(smb: float, hml: float) -> str:
    """风格对立面"""
    if hml < -0.3:
        return "价值/低估值红利"
    elif hml > 0.3:
        return "成长/科技"
    else:
        return "极端风格"


def _detailed_style_label(smb, hml, factor_loadings, r2) -> tuple[str, str]:
    """详细风格标签与描述"""
    mom = factor_loadings.get("Short_MOM", 0)
    rmw = factor_loadings.get("RMW", 0)

    # 确定标签
    size = "小盘" if smb > 0.4 else ("大盘" if smb < -0.4 else "中盘")
    style = "成长" if hml < -0.4 else ("价值" if hml > 0.4 else "均衡")
    label = f"{size}{style}型"

    # 补充描述
    extra = []
    if mom > 0.2:
        extra.append("兼具动量追随特征")
    if rmw > 0.3:
        extra.append("偏好高盈利质量个股")
    if r2 < 0.5:
        extra.append("具有强烈的独立风格，超越传统因子框架")

    desc = (
        f"SMB因子暴露为 {smb:.2f}（{'偏好小市值' if smb > 0.3 else '偏好大市值' if smb < -0.3 else '市值中性'}），"
        f"HML因子暴露为 {hml:.2f}（{'偏好低估值价值股' if hml > 0.3 else '偏好高估值成长股' if hml < -0.3 else '成长价值均衡'}）"
    )
    if extra:
        desc += "，" + "，".join(extra)

    return label, desc


def _seasonal_analysis(heatmap_info: dict) -> str:
    """分析季节性规律"""
    if not heatmap_info:
        return "月度热力图数据有限，季节性规律暂无法准确识别。"

    monthly_details = heatmap_info.get("monthly_details", {})
    monthly_returns = monthly_details.get("monthly_returns", {})

    if not monthly_returns:
        return "建议结合月度热力图识别经理在不同季节的表现规律，尤其关注春季（1-3月）和四季度（10-12月）的盈亏特征。"

    # 按月份汇总平均收益
    month_avg = {}
    for (year, month), ret in monthly_returns.items():
        if ret is not None and not (isinstance(ret, float) and (ret != ret)):  # 排除NaN
            if month not in month_avg:
                month_avg[month] = []
            month_avg[month].append(ret)

    if not month_avg:
        return "月度数据不足，季节性分析暂略。"

    month_avg_vals = {m: sum(vs)/len(vs)*100 for m, vs in month_avg.items()}

    # 找最强月和最弱月
    best_month = max(month_avg_vals.items(), key=lambda x: x[1])
    worst_month = min(month_avg_vals.items(), key=lambda x: x[1])

    month_names = {1:"1月",2:"2月",3:"3月",4:"4月",5:"5月",6:"6月",
                   7:"7月",8:"8月",9:"9月",10:"10月",11:"11月",12:"12月"}

    return (
        f"热力图显示，该基金在 **{month_names.get(best_month[0],'—')}** 前后往往表现较强（历史均值约 {best_month[1]:.1f}%），"
        f"而在 **{month_names.get(worst_month[0],'—')}** 前后表现相对偏弱（历史均值约 {worst_month[1]:.1f}%）。"
        f"建议投资者在弱势月份前适当控制仓位，在强势窗口期考虑增仓介入。"
    )


def _manager_portrait(net_alpha, max_dd_fund, monthly_win_rate, ir_value, m) -> str:
    """生成经理画像"""
    alpha_type = "强Alpha型" if net_alpha > 8 else ("稳健Alpha型" if net_alpha > 3 else ("基准跟随型" if net_alpha > -3 else "跑输基准型"))
    defense_type = "强防御" if max_dd_fund < 15 else ("中等防御" if max_dd_fund < 25 else "弱防御")

    return (
        f'该经理是一位**"{alpha_type}、{defense_type}"**的主动管理者。'
        f"其 Alpha 来源{'并非依赖择时（仓位变动），而是通过在擅长的行业内挖掘具备超额弹性的个股' if ir_value > 0.3 else '显示出对市场节奏的一定敏感性，但选股能力的稳定性需进一步观察'}。"
        f"月度胜率 {monthly_win_rate:.1f}% {'表明其对市场节奏的把握较为精准' if monthly_win_rate > 55 else '显示其表现受市场风格影响较大，存在一定的风格依赖性'}。"
    )


def _investment_advice(net_alpha, max_dd_fund, ir_value, m) -> tuple[str, str]:
    """生成投资建议和适合投资者类型"""
    if net_alpha > 5 and max_dd_fund < 20:
        advice = "✅ 该基金具备持续超额能力且回撤可控，适合长期持有或逢低分批买入"
        risk_pref = "中风险偏好投资者，适合将其作为核心权益底仓，建议持有周期不低于2年"
    elif net_alpha > 3 and max_dd_fund < 30:
        advice = "✅ 具备正超额且回撤在可接受范围，适合作为核心配置搭配定投降低成本"
        risk_pref = "中高风险承受能力投资者，建议以定期定额方式分散时间成本"
    elif net_alpha > 0 and max_dd_fund >= 25:
        advice = "🟡 超额能力尚可，但波动偏大。建议控制仓位在组合的20%以内，采用定投方式进入"
        risk_pref = "高风险承受能力投资者，不建议重仓，适合作为进攻型卫星仓位"
    elif net_alpha < -3:
        advice = "🔴 近期 Alpha 为负，持续跑输全收益基准。建议观察至少一个季度，确认是否进入转折点后再介入"
        risk_pref = "目前不推荐新增配置，现有持仓者可结合赎回成本评估是否止盈/止损"
    else:
        advice = "ℹ️ 综合来看，表现中性偏弱。如行情明朗且经理出现明显风格加强信号，可考虑小仓位介入"
        risk_pref = "稳健型投资者，需结合基金公司平台资源和经理历史业绩综合判断"

    return advice, risk_pref


def _identify_risk_point(m, max_dd_fund) -> str:
    """识别核心风险点"""
    risks = []

    if m.style_drift_flag:
        risks.append("⚠️ **风格漂移风险**：近期 Beta 偏离历史均值，经理可能在进行仓位结构调整，需关注后续风格的稳定性")

    if max_dd_fund > 25:
        risks.append(f"⚠️ **高波动风险**：最大回撤 {max_dd_fund:.1f}%，在市场系统性下跌时净值损失可观，需做好仓位管理")

    smb = m.factor_loadings.get("SMB", 0)
    if smb > 0.5:
        risks.append("⚠️ **小盘流动性风险**：基金重仓小市值股票，在市场极端情况下可能面临流动性折价")

    hml = m.factor_loadings.get("HML", 0)
    if hml < -0.5:
        risks.append("⚠️ **成长风格集中风险**：基金高度暴露于成长因子，若市场全面切换至价值/低估值风格，Alpha 可能阶段性消退")

    ir = m.information_ratio
    if ir < 0:
        risks.append("⚠️ **超额不稳定风险**：信息比率为负，说明超额的获取并不具有一致性，存在不稳定性")

    if not risks:
        risks.append("ℹ️ 当前未发现明显的结构性风险，主要需关注市场系统性风险带来的净值波动")

    return "\n\n".join(risks[:2])  # 最多展示2个核心风险点


def _section_top_contributors(
    report: Any,
    fund_name: str,
    start_date: str,
    end_date: str
) -> str:
    """
    P2-新增：Top 3贡献股/拖累股分析
    
    计算逻辑：
    1. 从持仓数据中获取top10_stocks（包含持仓比例）
    2. 获取每只股票在分析区间内的涨跌幅
    3. 计算贡献度 = 持仓比例 × 个股涨跌幅
    4. 排序找出Top 3贡献和Top 3拖累
    
    Args:
        report: 基金报告数据
        fund_name: 基金名称
        start_date: 分析开始日期
        end_date: 分析结束日期
        
    Returns:
        贡献度分析文字描述
    """
    # 检查是否有持仓数据
    if not hasattr(report, 'holdings') or not report.holdings or not report.holdings.top10_stocks:
        return """**持仓数据暂不可用**

当前暂无法获取该基金的持仓数据，贡献度分析将在数据接入后自动显示。
"""
    
    top10_stocks = report.holdings.top10_stocks
    
    # 如果持仓数据为空或数据格式不正确
    if not top10_stocks or len(top10_stocks) == 0:
        return """**持仓数据暂不可用**

当前暂无法获取该基金的持仓数据，贡献度分析将在数据接入后自动显示。
"""
    
    # 简化版贡献度分析（不接入实时股票数据，基于持仓比例推断）
    # 实际应用中，应该接入股票历史数据计算真实涨跌幅
    
    # 按持仓比例排序（假设比例高的股票对收益影响更大）
    sorted_stocks = sorted(top10_stocks, key=lambda x: x.get('占净值比例', 0), reverse=True)
    
    # 取Top 5持仓（用于分析）
    top5_holdings = sorted_stocks[:5]
    
    # 生成分析文本
    analysis_parts = []
    
    # 整体持仓集中度分析
    total_ratio = sum(stock.get('占净值比例', 0) for stock in top5_holdings)
    analysis_parts.append(f"**持仓集中度分析：**")
    analysis_parts.append(f"前五大重仓股合计占比 **{total_ratio:.1f}%**，{'持仓较为集中' if total_ratio > 30 else '持仓相对分散'}")
    
    # Top 3贡献股（基于持仓比例推断）
    analysis_parts.append(f"\n**Top 3 潜在贡献股（按持仓比例推断）：**")
    for i, stock in enumerate(top5_holdings[:3], 1):
        name = stock.get('股票名称', '未知')
        ratio = stock.get('占净值比例', 0)
        code = stock.get('股票代码', '')
        analysis_parts.append(f"{i}. **{name}** ({code}) — 持仓占比 **{ratio:.1f}%**")
    
    # 风险提示：集中度风险
    if total_ratio > 40:
        analysis_parts.append(f"\n**⚠️ 集中度风险提醒：**")
        analysis_parts.append(f"前五大重仓股占比超过40%，基金表现与这些个股的涨跌高度相关。建议投资者关注这些核心持仓的基本面变化。")
    
    analysis_parts.append(f"\n**注：** 以上为基于持仓比例的简化分析。实际贡献度需结合个股在分析区间内的真实涨跌幅计算，将在后续版本中接入实时股票数据。")
    
    return "\n".join(analysis_parts)


def _analyze_drawdown_cause(
    dd_info: dict,
    max_dd_fund: float,
    beta_val: float
) -> str:
    """
    P2-新增：分析回撤原因（系统性风险 vs 主动性失误）
    
    Args:
        dd_info: 回撤信息字典（包含max_dd_date等）
        max_dd_fund: 基金最大回撤（%）
        beta_val: Beta值
        
    Returns:
        回撤原因分析文字
    """
    # 获取最大回撤发生时间
    max_dd_date = dd_info.get('max_dd_date') if dd_info else None
    
    if not max_dd_date:
        return """**回撤原因分析：**

由于数据限制，暂无法精确分析回撤发生的市场背景。建议结合同期市场指数表现进行判断。"""
    
    # 将日期转换为可比较的格式
    try:
        from datetime import datetime
        if hasattr(max_dd_date, 'strftime'):
            dd_date = max_dd_date
        else:
            dd_date = datetime.strptime(str(max_dd_date)[:10], '%Y-%m-%d')
        
        year = dd_date.year
        month = dd_date.month
    except:
        return """**回撤原因分析：**

由于数据限制，暂无法精确分析回撤发生的市场背景。建议结合同期市场指数表现进行判断。"""
    
    # 系统性风险时间段识别
    systemic_risk_periods = []
    
    # 2022年：全球加息潮、俄乌冲突、疫情反复
    if year == 2022:
        systemic_risk_periods.append("2022年全球央行加息潮、地缘政治冲突与疫情反复导致的全球资产价格重估")
    
    # 2021年：教培行业整顿、互联网反垄断
    elif year == 2021 and month >= 7:
        systemic_risk_periods.append("2021年教培行业整顿与互联网反垄断政策引发的核心资产估值重构")
    
    # 2020年：疫情冲击
    elif year == 2020 and month <= 6:
        systemic_risk_periods.append("2020年新冠疫情爆发导致的全球流动性危机与资产价格暴跌")
    
    # 2018年：中美贸易摩擦
    elif year == 2018:
        systemic_risk_periods.append("2018年中美贸易摩擦升级引发的出口链与科技板块估值下修")
    
    # 2015年：A股异常波动
    elif year == 2015 and 6 <= month <= 9:
        systemic_risk_periods.append("2015年A股异常波动期间的流动性枯竭与千股跌停")
    
    # 根据Beta值和回撤幅度判断原因
    analysis_parts = []
    analysis_parts.append(f"**回撤原因分析（基于时间序列推断）：**")
    analysis_parts.append(f"最大回撤发生于 **{dd_date.strftime('%Y年%m月')}**，回撤幅度 **{abs(max_dd_fund):.1f}%**")
    
    if systemic_risk_periods:
        analysis_parts.append(f"\n**系统性风险识别：**")
        analysis_parts.append(f"该时间段处于{'、'.join(systemic_risk_periods)}。")
        
        # 根据Beta判断基金相对于市场的表现
        if beta_val > 1.2:
            analysis_parts.append(f"该基金Beta值为 **{beta_val:.2f}**（高于1.2），在市场下跌期间跌幅超过市场平均水平，主要受系统性风险影响，但也可能包含一定的主动性失误（如仓位控制不当）。")
        elif beta_val < 0.8:
            analysis_parts.append(f"该基金Beta值为 **{beta_val:.2f}**（低于0.8），在市场下跌期间展现了较强的防御能力，回撤主要受系统性风险影响，但基金经理通过选股或仓位管理有效控制了下跌幅度。")
        else:
            analysis_parts.append(f"该基金Beta值为 **{beta_val:.2f}**（接近1），回撤主要由系统性风险驱动，与整体市场走势基本一致。")
        
        analysis_parts.append(f"\n**结论：** 本次回撤**主要是系统性风险**所致，{'基金经理在风险控制方面表现' + ('优秀' if beta_val < 0.8 else '良好' if beta_val < 1.0 else '一般') + '，' + ('有效降低了回撤幅度' if beta_val < 1.0 else '回撤幅度与市场相当')}" )
    else:
        # 非典型系统性风险时期，更可能是主动性失误或个股风险
        analysis_parts.append(f"\n**主动性失误识别：**")
        analysis_parts.append(f"该时间段不属于典型的系统性风险集中爆发期（如2020年疫情、2022年全球加息等）。")
        
        if abs(max_dd_fund) > 30:
            analysis_parts.append(f"回撤幅度达到 **{abs(max_dd_fund):.1f}%**，显著超过市场 typical 波动范围，**可能存在主动性失误**，如：")
            analysis_parts.append(f"- **重仓股暴雷**：核心持仓个股出现业绩大幅下滑或负面事件")
            analysis_parts.append(f"- **行业配置错误**：重仓行业遭遇政策利空或景气度逆转")
            analysis_parts.append(f"- **择时失误**：在市场高位加仓、低位减仓的错误操作")
        else:
            analysis_parts.append(f"回撤幅度 **{abs(max_dd_fund):.1f}%**，处于正常波动范围内，可能是正常的市场波动或个股短期调整所致。")
        
        analysis_parts.append(f"\n**建议：** 建议结合持仓分析，重点观察同期重仓股表现，判断是否存在个股或行业层面的配置失误。")
    
    return "\n".join(analysis_parts)


def _fallback_report(basic) -> dict:
    """数据不足时的回退报告"""
    return {
        "meta": {"fund_name": basic.name, "fund_type": "权益类", "grade": "—", "score": 0, "tags": []},
        "headline": f"# 【{basic.name}】数据不足，无法生成深度报告",
        "section1": "数据获取不完整，无法进行收益曲线分析。",
        "section2": "持仓数据不足，无法进行持仓穿透分析。",
        "section3": "深度分析数据不足，无法进行 Brinson 归因分解。",
        "section4": "风险数据不足，无法进行压力测试分析。",
        "section5": "投资建议数据不足。",
        "full_text": "数据不足，无法生成完整的深度评价报告。",
    }
