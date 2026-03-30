"""
权益类基金深度评价报告生成器 — fund_quant_v2
角色：资深基金分析师（CFA 持证人）
报告结构：4章节 + 图表插入点标记 + 结论建议
"""

from __future__ import annotations
from typing import Any


# ============================================================
# 主入口
# ============================================================

def generate_equity_deep_report(report: Any) -> dict:
    """
    生成权益类基金深度评价报告（约1000字，含图表标记）

    Returns:
        {
          "meta":     {fund_name, fund_type, start_date, end_date, grade, score, tags},
          "section1": 全收益框架下的收益穿透（含 [INSERT_CHART: CUM_RET]）,
          "section2": Alpha的持续性与能力边界（含 [INSERT_CHART: EXCESS_ALPH]）,
          "section3": 风险控制与修复弹性（含 [INSERT_CHART: DRAWDOWN]）,
          "section4": 盈利稳定性与风格定性（含 [INSERT_CHART: HEATMAP]）,
          "conclusion": 综合结论与投资建议,
          "full_text": 完整纯文本（所有章节合并）
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

    # 日期范围（从chart_data取）
    start_date, end_date = _extract_date_range(charts)

    # 收益数据
    cum_fund = round(cm.cumulative_return * 100, 1)
    ann_ret  = round(cm.annualized_return * 100, 1)

    # 基准数据
    bm_info = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm_tr      = round(bm_info.get("bm_last_return", 0) * 100, 1)
    is_total_return = bm_info.get("is_total_return", False)

    # 分红贡献估算（行业年化股息率 × 统计年数）
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

    section1 = _section1_cumulative_return(
        fund_name, fund_type, start_date, end_date,
        cum_fund, cum_bm_tr, is_total_return,
        div_contribution, net_alpha, ann_ret,
        smb, hml, basic
    )

    section2 = _section2_alpha_persistence(
        fund_name, last_excess, curve_trend,
        ir_value, excess_std, monthly_win_rate,
        net_alpha, smb, hml,
        ex_info, start_date, end_date
    )

    section3 = _section3_risk_defense(
        fund_name, max_dd_fund, max_dd_bm,
        dd_info, recovery_days, cm,
        beta_val, r2
    )

    section4 = _section4_stability_style(
        fund_name, monthly_win_rate, annual_stats,
        heatmap_info, smb, hml, m.factor_loadings,
        beta_val, r2, tags
    )

    # ── 持仓分析章节（新的深度持仓穿透分析）────────────────
    # 不在报告生成阶段调用深度持仓分析，而是返回占位符
    # 由 main.py 在渲染阶段按需调用（避免重复加载）
    section5_holdings = """### 五、持仓穿透分析：微观归因与风格定性

[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]"""

    conclusion = _section5_conclusion(
        fund_name, grade, score, tags,
        net_alpha, max_dd_fund, monthly_win_rate,
        ir_value, m
    )

    # 标题行
    headline = _build_headline(grade, fund_name, tags, score, start_date, end_date)

    # 合并全文（新增 section5）
    full_text = "\n\n".join([
        headline, section1, section2, section3, section4,
        section5_holdings, conclusion
    ])

    return {
        "meta":       meta,
        "headline":   headline,
        "section1":   section1,
        "section2":   section2,
        "section3":   section3,
        "section4":   section4,
        "section5":   section5_holdings,  # 新增
        "conclusion": conclusion,
        "full_text":  full_text,
    }


# ============================================================
# 各章节生成
# ============================================================

def _section1_cumulative_return(
    fund_name, fund_type, start_date, end_date,
    cum_fund, cum_bm_tr, is_total_return,
    div_contribution, net_alpha, ann_ret,
    smb, hml, basic
) -> str:
    """一、全收益框架下的收益穿透"""

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

    text = f"""### 一、全收益框架下的收益穿透

[INSERT_CHART: CUM_RET]

在评价权益类基金时，传统"价格指数"基准往往低估市场的真实回报，从而虚增基金经理的超额。本报告采用{bm_type}作为比较基准，将分红再投资收益纳入对比体系。

**数据表现**

统计区间 {start_date} 至 {end_date}，本基金累计收益率为 **{cum_fund:.1f}%**，对比{bm_type}的 {cum_bm_tr:.1f}%，{excess_desc}。{ann_desc}。

**深度解读**

{div_desc}

{alpha_quality}。曲线形态上，{"基金收益曲线在统计期内整体走势强于基准，呈现出稳健向上的斜率特征" if excess > 0 else "基金收益曲线阶段性落后于基准，显示经理在该市场环境下的适应性存在一定局限"}，反映了经理在 {_style_desc(smb, hml)} 风格上的持续投入。"""

    return text


def _section2_alpha_persistence(
    fund_name, last_excess, curve_trend,
    ir_value, excess_std, monthly_win_rate,
    net_alpha, smb, hml,
    ex_info, start_date, end_date
) -> str:
    """二、Alpha的持续性与能力边界"""

    # IR 解读
    if ir_value > 1.0:
        ir_desc = f"信息比率（IR）高达 **{ir_value:.2f}**，属于卓越水平，意味着每承担一单位偏离风险，能换取超过一单位的超额回报"
    elif ir_value > 0.5:
        ir_desc = f"信息比率（IR）为 **{ir_value:.2f}**，属于优秀水平，超额收益的获取效率较高"
    elif ir_value > 0:
        ir_desc = f"信息比率（IR）为 {ir_value:.2f}，处于合格区间，超额收益存在但稳定性有待提升"
    else:
        ir_desc = f"信息比率（IR）为 {ir_value:.2f}，为负值，超额收益的可靠性偏低"

    # 超额曲线趋势
    if curve_trend == "加速突破":
        trend_desc = "超额曲线近期呈**加速突破**态势，说明经理在当前市场风格下进入强势通道"
        trend_outlook = "需关注风格切换风险，当前优势可能随市场风格变化而收敛"
    elif curve_trend == "高位盘整":
        trend_desc = "超额曲线处于**高位盘整**阶段，说明经理维持超额但创新高能力边际减弱"
        trend_outlook = "建议观察能否突破平台期，进入新的上升通道"
    else:
        trend_desc = "超额曲线呈**震荡上行**走势，短期内有所波动但中期趋势向上"
        trend_outlook = "波动来源于市场风格的周期性切换，经理长期选股能力仍值得肯定"

    # 能力边界
    style_edge = _style_edge_desc(smb, hml)

    # 稳定性
    if excess_std < 0.5:
        stability = f"超额收益波动极低（日均波动 {excess_std:.2f}%），如阶梯般稳健上行，体现了极强的选股一致性"
    elif excess_std < 1.0:
        stability = f"超额收益波动适中（日均波动 {excess_std:.2f}%），节奏感明显，阶段性爆发与修复交替出现"
    else:
        stability = f"超额收益波动较大（日均波动 {excess_std:.2f}%），经理博弈属性较强，适合能接受短期波动的投资者"

    text = f"""### 二、Alpha 的持续性与能力边界

[INSERT_CHART: EXCESS_ALPH]

超额收益曲线（几何法计算）揭示了经理主动管理能力的"含金量"。

**数据表现**

统计期内累计超额收益为 **{last_excess:.1f}%**，{trend_desc}。{ir_desc}。月度超额胜率为 **{monthly_win_rate:.1f}%**（即有{monthly_win_rate:.0f}%的月份跑赢全收益基准）。

**深度解读**

{stability}。

{trend_outlook}

**能力边界识别**

{style_edge}。这意味着当{_style_opposite(smb, hml)}风格主导市场时，经理可能面临超额收敛的压力。建议投资者在配置时，结合当前市场风格环境动态评估。"""

    return text


def _section3_risk_defense(
    fund_name, max_dd_fund, max_dd_bm,
    dd_info, recovery_days, cm,
    beta_val, r2
) -> str:
    """三、风险控制与修复弹性"""

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
            recovery_desc = f"从最低点到回血完毕仅用 **{recovery_days} 个交易日**，修复弹性极强，呈现标准的 V 型反转形态"
        elif recovery_days < 90:
            recovery_desc = f"回撤修复历时 **{recovery_days} 个交易日**，修复节奏适中，市场回暖后反弹有力"
        elif recovery_days < 180:
            recovery_desc = f"回撤修复历时 **{recovery_days} 个交易日**，属于 U 型磨底，修复速度偏慢"
        else:
            recovery_desc = f"最长回撤修复周期达 **{recovery_days} 个交易日**，长期处于水下，需关注经理的调仓应对能力"
    else:
        recovery_desc = "当前基金仍处于回撤修复阶段，尚未完全回血"

    # 波动率描述
    vol = round(cm.volatility * 100, 1)
    if vol < 15:
        vol_desc = f"年化波动率 {vol:.1f}%，属于低波动品种"
    elif vol < 25:
        vol_desc = f"年化波动率 {vol:.1f}%，波动处于合理区间"
    else:
        vol_desc = f"年化波动率 {vol:.1f}%，波动较高，适合风险承受能力较强的投资者"

    # 夏普和卡玛
    sharpe = cm.sharpe_ratio
    if sharpe > 1.5:
        risk_reward = f"夏普比率 **{sharpe:.2f}**（卓越），风险调整后收益极强"
    elif sharpe > 1.0:
        risk_reward = f"夏普比率 {sharpe:.2f}（良好），每单位风险获取了充分回报"
    elif sharpe > 0.5:
        risk_reward = f"夏普比率 {sharpe:.2f}（一般），风险与收益的匹配尚可"
    else:
        risk_reward = f"夏普比率 {sharpe:.2f}（偏低），风险调整后回报不足"

    text = f"""### 三、风险控制与修复弹性

[INSERT_CHART: DRAWDOWN]

回撤不仅是风险，更是经理心理素质与调仓效率的试金石。

**数据表现**

{defense_quality}。{recovery_desc}。{vol_desc}，{risk_reward}。

**深度解读**

{"更关键的是修复速度：" if recovery_days and recovery_days < 90 else ""}{"相对快速的回血节奏，结合持仓分析，暗示经理在下跌过程中进行了前瞻性的防御性换仓，将头寸切换至更具弹性的品种，从而实现了跌得少、回血快的非对称优势。" if recovery_days and recovery_days < 60 else f"经理在回撤应对中展现了{('较强的仓位管理能力' if defense_ratio < 1.0 else '与市场基本同步的操作节奏')}，建议持续关注后续回撤修复进展。"}

{"Beta 值为 " + str(round(beta_val, 2)) + ("，高于1的弹性特征意味着市场上涨时跑赢，但下跌时回撤也更大，这与上述数据吻合。" if beta_val > 1.1 else "，略低于1的防守特征与其较强的回撤控制能力高度吻合。" if beta_val < 0.9 else "，与市场波动基本同步，属于标准主动权益产品的Beta水平。")}"""

    return text


def _section4_stability_style(
    fund_name, monthly_win_rate, annual_stats,
    heatmap_info, smb, hml, factor_loadings,
    beta_val, r2, tags
) -> str:
    """四、盈利稳定性与风格定性"""

    # 月度胜率描述
    if monthly_win_rate >= 65:
        wr_desc = f"月度胜率高达 **{monthly_win_rate:.1f}%**，属于高胜率选手，在超过3/5的月份中能够跑赢基准"
    elif monthly_win_rate >= 55:
        wr_desc = f"月度胜率 **{monthly_win_rate:.1f}%**，超过半数月份能够跑赢基准，稳定性良好"
    elif monthly_win_rate >= 45:
        wr_desc = f"月度胜率 {monthly_win_rate:.1f}%，与基准互有胜负，表现较为均衡"
    else:
        wr_desc = f"月度胜率 {monthly_win_rate:.1f}%，偏低于50%，说明经理在多数月份未能跑赢基准"

    # 年度表现
    annual_desc = ""
    if annual_stats:
        positive_years = sum(1 for v in annual_stats.values() if v > 0)
        total_years = len(annual_stats)
        best_year = max(annual_stats.items(), key=lambda x: x[1])
        worst_year = min(annual_stats.items(), key=lambda x: x[1])
        annual_desc = (
            f"年度维度来看，统计期内共 {total_years} 个自然年，其中 {positive_years} 年实现正收益。"
            f"最佳年份为 {best_year[0]} 年（{best_year[1]:.1f}%），"
            f"最差年份为 {worst_year[0]} 年（{worst_year[1]:.1f}%），"
            f"年度收益的波动性{'较低，具有良好的收益稳定性' if abs(best_year[1] - worst_year[1]) < 30 else '较高，呈现较大的年际差异，受市场风格影响明显'}。"
        )

    # 风格定性
    style_label, style_desc = _detailed_style_label(smb, hml, factor_loadings, r2)

    # 季节性规律
    seasonal_desc = _seasonal_analysis(heatmap_info)

    text = f"""### 四、盈利稳定性与风格定性

[INSERT_CHART: HEATMAP]

通过月度盈亏矩阵，我们可以量化经理的盈利稳定度，并识别其风格特征与季节性规律。

**数据表现**

{wr_desc}。{annual_desc}

**风格定性**

根据因子暴露分析，该基金经理属于典型的**{style_label}**：{style_desc}。{"R² 值为 " + str(round(r2, 2)) + ("，因子模型拟合度较高，风格持续稳定" if r2 > 0.7 else "，拟合度一般，经理具有一定独立风格，不完全跟随单一风格因子" if r2 > 0.4 else "，拟合度较低，该基金风格独立性强，难以用传统因子模型完全解释")}。

**季节性规律**

{seasonal_desc}"""

    return text


def _section5_conclusion(
    fund_name, grade, score, tags,
    net_alpha, max_dd_fund, monthly_win_rate,
    ir_value, m
) -> str:
    """六、综合结论与投资建议"""

    # 经理画像
    portrait = _manager_portrait(net_alpha, abs(max_dd_fund), monthly_win_rate, ir_value, m)

    # 投资建议
    advice, risk_pref = _investment_advice(net_alpha, abs(max_dd_fund), ir_value, m)

    # 风险点
    risk_point = _identify_risk_point(m, abs(max_dd_fund))

    # 配置建议
    if grade in ("A+", "A") and net_alpha > 3:
        config_advice = f"建议作为权益组合中的**核心持仓**（Core Holding）。当前评级 **{grade} 级（{score:.0f}分）**，具备长期持有价值。考虑到其{'优秀的回撤控制' if abs(max_dd_fund) < 20 else '较高的弹性特征'}，{'在市场震荡磨底期是理想的介入点，可适当提高组合权重' if abs(max_dd_fund) < 20 else '建议在市场趋势明确时分批建仓，避免高位追入'}。"
    elif grade == "B":
        config_advice = f"建议作为权益组合中的**卫星配置**（Satellite Holding），综合评级 **{grade} 级（{score:.0f}分）**，可在组合中占据适度比例，结合市场环境灵活调整仓位。"
    else:
        config_advice = f"当前综合评级 **{grade} 级（{score:.0f}分）**，建议以**观察仓**形式参与，等待经理能力进一步验证后再加大配置。"

    tag_str = "、".join([f"「{t}」" for t in tags[:3]]) if tags else "综合型"

    text = f"""### 六、综合结论与投资建议

**一、经理画像**

该经理贴标签为 {tag_str}。{portrait}

**二、核心风险点**

{risk_point}

**三、配置建议**

{advice}。{config_advice}

*适合投资者类型：{risk_pref}*"""

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


def _fund_type_label(fund_type: str) -> str:
    mapping = {
        "equity": "股票型",
        "mixed":  "混合型",
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


def _fallback_report(basic) -> dict:
    """数据不足时的回退报告"""
    return {
        "meta": {"fund_name": basic.name, "fund_type": "权益类", "grade": "—", "score": 0, "tags": []},
        "headline": f"# 【{basic.name}】数据不足，无法生成深度报告",
        "section1": "数据获取不完整，无法进行全收益分析。",
        "section2": "Alpha 数据不足。",
        "section3": "风险数据不足。",
        "section4": "月度数据不足。",
        "conclusion": "建议获取更多历史数据后重新分析。",
        "full_text": "数据不足，无法生成完整的深度评价报告。",
    }
