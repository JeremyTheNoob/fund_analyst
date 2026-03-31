"""
混合型-偏股基金深度评价报告生成器 — fund_quant_v2
角色：资深基金分析师（CFA 持证人）
报告结构：6板块 + 图表插入点标记
  1. 资产结构模型（股债动态比例 + 资产配置面积图）
  2. 收益表现（历史业绩 + 同类分位数）
  3. 深度分析（前十大重仓股穿透 + 重仓股留存图）
  4. 非股票部分分析（风险对冲/转债灰色地带/现金择时信号）
  5. 风险预警（RBSA 动态回归风格漂移）
  6. 投资建议（拟买入/持有中/离场信号）
"""

from __future__ import annotations
from typing import Any


# ============================================================
# 主入口
# ============================================================

def generate_hybrid_equity_report(report: Any) -> dict:
    """
    生成混合型-偏股基金深度评价报告（6板块结构）

    Returns:
        {
          "meta":       {fund_name, fund_type, start_date, end_date, grade, score, tags},
          "headline":   标题行,
          "section1":   一、资产结构模型（[INSERT_CHART: ASSET_ALLOCATION]）,
          "section2":   二、收益表现（[INSERT_CHART: CUM_RET] + [INSERT_CHART: HEATMAP]）,
          "section3":   三、深度分析（[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]）,
          "section4":   四、非股票部分分析（风险对冲/转债灰色地带/现金择时）,
          "section5":   五、风险预警（[INSERT_CHART: RBSA_DRIFT] + [INSERT_CHART: DRAWDOWN]）,
          "section6":   六、投资建议（拟买入/持有中/离场信号）,
          "full_text":  完整纯文本,
        }
    """
    basic = report.basic
    m = report.equity_metrics
    charts = report.chart_data
    holdings = report.chart_data.get("holdings", {})

    if not m:
        return _fallback_report(basic)

    # ── 提取核心数据 ──────────────────────────────────────
    cm = m.common
    fund_name = basic.name
    grade = m.score_grade
    score = m.overall_score
    tags = report.tags or []

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 资产配置数据
    stock_ratio = holdings.get("stock_ratio", 0.0)
    bond_ratio = holdings.get("bond_ratio", 0.0)
    cash_ratio = holdings.get("cash_ratio", 0.0)
    cb_ratio = holdings.get("cb_ratio", 0.0)
    historical_allocation = holdings.get("historical_allocation", [])

    # 收益数据
    cum_fund = round(cm.cumulative_return * 100, 1)
    ann_ret  = round(cm.annualized_return * 100, 1)

    # 基准数据
    bm_info = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm_tr      = round(bm_info.get("bm_last_return", 0) * 100, 1)

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

    # 月度数据
    monthly_win_rate = round(cm.monthly_win_rate * 100, 1)

    # 风格因子
    smb = m.factor_loadings.get("SMB", 0)
    hml = m.factor_loadings.get("HML", 0)
    beta_val = m.beta
    r2 = m.r_squared

    # Brinson 归因
    brinson = getattr(m, 'brinson', {}) or {}

    # ── 生成各章节 ────────────────────────────────────────
    meta = {
        "fund_name": fund_name,
        "fund_type": "混合型-偏股",
        "start_date": start_date,
        "end_date": end_date,
        "grade": grade,
        "score": score,
        "tags": tags,
    }

    # 板块1：资产结构模型
    section1 = _section1_asset_structure(
        fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
        historical_allocation, start_date, end_date
    )

    # 板块2：收益表现
    section2 = _section2_return_performance(
        fund_name, start_date, end_date,
        cum_fund, cum_bm_tr, ann_ret,
        net_alpha, beta_val, smb, hml, basic
    )

    # 板块3：深度分析（前十大重仓股穿透 + 留存图）
    top_contributors = _section_top_contributors(
        report, fund_name, start_date, end_date
    )
    section3 = f"""### 三、深度分析

{top_contributors}

[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]

[INSERT_CHART: HOLDINGS_RETENTION]"""

    # 板块4：非股票部分分析
    section4 = _section4_non_equity_analysis(
        fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
        beta_val, max_dd_fund, historical_allocation
    )

    # 板块5：风险预警（RBSA 风格漂移）
    section5 = _section5_risk_warning_rbsa(
        fund_name, max_dd_fund, max_dd_bm,
        dd_info, recovery_days, cm,
        beta_val, r2, ann_ret, smb, hml
    )

    # 板块6：投资建议
    section6 = _section6_investment_advice(
        fund_name, grade, score, tags,
        net_alpha, max_dd_fund, monthly_win_rate,
        ir_value, m, basic
    )

    # 标题行
    headline = _build_headline(grade, fund_name, tags, score, start_date, end_date)

    # 合并全文
    full_text = "\n\n".join([
        headline, section1, section2, section3, section4, section5, section6
    ])

    return {
        "meta":       meta,
        "headline":   headline,
        "section1":   section1,
        "section2":   section2,
        "section3":   section3,
        "section4":   section4,
        "section5":   section5,
        "section6":   section6,
        "full_text":  full_text,
    }


# ============================================================
# 板块1：资产结构模型
# ============================================================

def _section1_asset_structure(
    fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    historical_allocation, start_date, end_date
) -> str:
    """一、资产结构模型：股债动态比例"""

    # 股票仓位分析
    if stock_ratio > 0.90:
        position_desc = f"最新季报股票仓位高达 **{stock_ratio:.1%}**，已逼近偏股混合型上限（95%），属于高仓位运作"
    elif stock_ratio > 0.80:
        position_desc = f"最新季报股票仓位为 **{stock_ratio:.1%}**，处于偏股混合型中枢位置，属于标准偏股运作"
    elif stock_ratio > 0.70:
        position_desc = f"最新季报股票仓位为 **{stock_ratio:.1%}**，相对同类偏股基金偏低，经理可能倾向防守"
    else:
        position_desc = f"最新季报股票仓位仅为 **{stock_ratio:.1%}**，明显低于偏股混合型标准（≥60%），需关注仓位策略"

    # 资产配置总结
    non_stock = 1 - stock_ratio
    asset_parts = []
    if bond_ratio > 0.01:
        asset_parts.append(f"债券 **{bond_ratio:.1%}**")
    if cb_ratio > 0.01:
        asset_parts.append(f"可转债 **{cb_ratio:.1%}**")
    if cash_ratio > 0.01:
        asset_parts.append(f"现金 **{cash_ratio:.1%}**")
    if not asset_parts:
        asset_parts.append(f"非股票资产 **{non_stock:.1%}**")

    non_stock_desc = "、".join(asset_parts)
    allocation_summary = f"非股票部分占比 **{non_stock:.1%}**（{non_stock_desc}），{'构成了重要的风险缓冲垫' if non_stock > 0.15 else '安全垫较薄，抗风险空间有限'}"

    # 历史仓位趋势分析（如果有历史数据）
    history_desc = ""
    if historical_allocation and len(historical_allocation) >= 2:
        history_desc = _analyze_position_trend(historical_allocation)

    # 转债灰色地带预警
    cb_warning = ""
    total_equity_like = stock_ratio + cb_ratio
    if cb_ratio > 0.10:
        cb_warning = (
            f"\n\n**⚠️ 可转债灰色地带监测：**\n\n"
            f"可转债仓位占比 **{cb_ratio:.1%}**，加上股票仓位后，"
            f"**股性资产合计达 {total_equity_like:.1%}**。"
        )
        if total_equity_like > 0.95:
            cb_warning += f"已**超过95%**，经理实质上在满仓运作股票，名义上是混合型，实际上等同于股票型基金，波动会极大。"
        elif total_equity_like > 0.85:
            cb_warning += f"接近90%，风格偏激进，需关注转债部分的权益暴露风险。"
        else:
            cb_warning += f"占比适中，属于合理的转债增强策略。"

    text = f"""### 一、资产结构模型

[INSERT_CHART: ASSET_ALLOCATION]

混合型偏股基金的核心特征在于"股债动态平衡"——经理通过调节股票仓位来控制风险敞口。本板块追踪基金的资产结构演变，揭示经理的仓位管理风格。

**当前资产结构**

{position_desc}。{allocation_summary}。

**资产配置明细**

| 资产类别 | 占比 | 说明 |
|---|---|---|
| 股票 | {stock_ratio:.1%} | 直接权益敞口 |
| 债券 | {bond_ratio:.1%} | 固收底仓（纯债/信用债） |
| 可转债 | {cb_ratio:.1%} | 股债混合工具，含隐性权益敞口 |
| 现金及其他 | {cash_ratio:.1%} | 流动性储备 |
{history_desc}{cb_warning}"""

    return text


def _analyze_position_trend(historical_allocation: list) -> str:
    """分析历史仓位趋势"""
    if not historical_allocation:
        return ""

    # 提取股票仓位序列
    stock_series = [(q.get("date", ""), q.get("stock_ratio", 0)) for q in historical_allocation]
    stock_values = [v for _, v in stock_series if v > 0]

    if len(stock_values) < 2:
        return ""

    avg_stock = sum(stock_values) / len(stock_values)
    max_stock = max(stock_values)
    min_stock = min(stock_values)
    latest_stock = stock_values[-1]

    # 仓位波动幅度
    swing = max_stock - min_stock

    if swing > 0.20:
        swing_desc = f"**大幅择时型**：仓位波动范围 {min_stock:.1%} ~ {max_stock:.1%}，振幅达 {swing:.1%}，经理擅长通过仓位调节进行择时"
    elif swing > 0.10:
        swing_desc = f"**适度调节型**：仓位波动范围 {min_stock:.1%} ~ {max_stock:.1%}，振幅 {swing:.1%}，经理会根据市场环境微调仓位"
    else:
        swing_desc = f"**稳定仓位型**：仓位基本稳定在 {min_stock:.1%} ~ {max_stock:.1%}，振幅仅 {swing:.1%}，经理不依赖择时，主要靠选股获取超额"

    # 当前仓位相对历史均值
    if latest_stock > avg_stock + 0.05:
        position_signal = f"当前仓位（{latest_stock:.1%}）**高于历史均值**（{avg_stock:.1%}），经理当前偏乐观"
    elif latest_stock < avg_stock - 0.05:
        position_signal = f"当前仓位（{latest_stock:.1%}）**低于历史均值**（{avg_stock:.1%}），经理当前偏谨慎"
    else:
        position_signal = f"当前仓位（{latest_stock:.1%}）**接近历史均值**（{avg_stock:.1%}），仓位管理平稳"

    return f"\n\n**历史仓位趋势**\n\n{swing_desc}。{position_signal}。"


# ============================================================
# 板块2：收益表现
# ============================================================

def _section2_return_performance(
    fund_name, start_date, end_date,
    cum_fund, cum_bm_tr, ann_ret,
    net_alpha, beta_val, smb, hml, basic
) -> str:
    """二、收益表现：历史业绩 + 同类分位数"""

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
        alpha_quality = "超额空间有限，主动管理价值待进一步验证"
    else:
        excess_desc = f"落后基准 {abs(excess):.1f}个百分点"
        alpha_quality = "未能跑赢基准，需关注经理选股逻辑是否与市场匹配"

    # 年化描述
    if ann_ret > 15:
        ann_desc = f"年化收益率达 **{ann_ret:.1f}%**，处于同类产品的较高水平"
    elif ann_ret > 8:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，表现稳健"
    elif ann_ret > 0:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，整体偏保守"
    else:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，统计期内呈负值"

    # Beta 解读
    if beta_val > 1.1:
        beta_desc = f"Beta 值 {beta_val:.2f}，高于市场基准，上涨时弹性更足但下跌时跌幅也更大"
    elif beta_val < 0.9:
        beta_desc = f"Beta 值 {beta_val:.2f}，低于市场基准，展现了较好的防守特征"
    else:
        beta_desc = f"Beta 值 {beta_val:.2f}，与市场同步，风险暴露中性"

    # 风格定位
    style = _style_desc(smb, hml)

    # 同类分位提示（实际分位数需外部数据，这里用 Alpha 估算）
    if net_alpha > 5:
        percentile_desc = "基于超额收益水平，预估计处于同类基金 **前20%**（优秀分位）"
    elif net_alpha > 0:
        percentile_desc = "基于超额收益水平，预估计处于同类基金 **前50%**（中等偏上分位）"
    elif net_alpha > -3:
        percentile_desc = "基于超额收益水平，预估计处于同类基金 **中位区间**"
    else:
        percentile_desc = "基于超额收益水平，预估计处于同类基金 **后50%**（需关注改善空间）"

    text = f"""### 二、收益表现

[INSERT_CHART: CUM_RET]

[INSERT_CHART: HEATMAP]

统计区间 {start_date} 至 {end_date}，本基金累计收益率为 **{cum_fund:.1f}%**，对比基准的 {cum_bm_tr:.1f}%，{excess_desc}。{ann_desc}。

{beta_desc}。从风格暴露分析（SMB={smb:.2f}, HML={hml:.2f}），经理在{style}风格上持续投入，{alpha_quality}。

**同类分位数估算**

{percentile_desc}（注：同类分位数基于超额收益水平估算，精确排名需接入晨星/银河评级数据）。"""

    return text


# ============================================================
# 板块3：深度分析（前十大重仓股）
# ============================================================

def _section_top_contributors(
    report: Any,
    fund_name: str,
    start_date: str,
    end_date: str
) -> str:
    """Top 10 贡献股分析"""
    # 从 chart_data.holdings 获取持仓数据（FundReport 没有 holdings 属性）
    holdings_data = report.chart_data.get("holdings", {})
    top10_stocks = holdings_data.get("top10_stocks", [])

    if not top10_stocks or len(top10_stocks) == 0:
        return "**持仓数据暂不可用**\n\n当前暂无法获取该基金的持仓数据，贡献度分析将在数据接入后自动显示。"

    sorted_stocks = sorted(top10_stocks, key=lambda x: x.get('占净值比例', 0), reverse=True)
    top5_holdings = sorted_stocks[:5]

    analysis_parts = []

    # 整体持仓集中度
    total_ratio = sum(stock.get('占净值比例', 0) for stock in top5_holdings)
    analysis_parts.append(f"**持仓集中度分析：**\n前五大重仓股合计占比 **{total_ratio:.1f}%**，{'持仓较为集中，基金表现与核心持仓高度相关' if total_ratio > 30 else '持仓相对分散'}")

    # Top 5 持仓明细
    analysis_parts.append(f"\n**Top 5 重仓股：**")
    for i, stock in enumerate(top5_holdings, 1):
        name = stock.get('股票名称', '未知')
        ratio = stock.get('占净值比例', 0)
        code = stock.get('股票代码', '')
        analysis_parts.append(f"{i}. **{name}** ({code}) — 持仓占比 **{ratio:.1f}%**")

    # 集中度风险
    if total_ratio > 40:
        analysis_parts.append(f"\n**⚠️ 集中度风险提醒：**前五大重仓股占比超过40%，基金表现与这些个股的涨跌高度相关。")

    return "\n".join(analysis_parts)


# ============================================================
# 板块4：非股票部分分析
# ============================================================

def _section4_non_equity_analysis(
    fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    beta_val, max_dd_fund, historical_allocation
) -> str:
    """四、非股票部分分析：风险对冲 / 转债灰色地带 / 现金择时"""

    non_stock_ratio = 1 - stock_ratio

    # ── 维度 A：风险对冲能力 ──
    dimension_a = _analyze_hedge_ability(
        fund_name, non_stock_ratio, beta_val, max_dd_fund
    )

    # ── 维度 B：转债灰色地带 ──
    dimension_b = _analyze_cb_grey_zone(
        fund_name, stock_ratio, bond_ratio, cb_ratio
    )

    # ── 维度 C：现金择时信号 ──
    dimension_c = _analyze_cash_timing_signal(
        fund_name, cash_ratio, historical_allocation
    )

    text = f"""### 四、非股票部分分析

混合型偏股基金中，非股票部分（债券 + 可转债 + 现金）是**风险缓冲层**，也是经理投资哲学的"隐形标签"。本板块从三个维度拆解这层资产的真实性质。

**维度 A：风险对冲能力（防御性测试）**

{dimension_a}

**维度 B：可转债的"灰色地带"监测**

{dimension_b}

**维度 C：现金及逆回购的"择时"信号**

{dimension_c}"""

    return text


def _analyze_hedge_ability(fund_name, non_stock_ratio, beta_val, max_dd_fund) -> str:
    """维度 A：风险对冲能力评估"""
    if non_stock_ratio < 0.10:
        hedge_score = "极差"
        hedge_desc = (
            f"非股票资产仅占 **{non_stock_ratio:.1%}**，几乎没有风险缓冲空间。"
            f"在市场系统性下跌时，基金净值将与市场基本同步下跌，无法提供有效防御。"
        )
        hedge_verdict = "该基金本质上是一个'加了少量债券的股票基金'，风险特征更接近股票型而非混合型。"
    elif non_stock_ratio < 0.20:
        hedge_score = "较弱"
        hedge_desc = (
            f"非股票资产占 **{non_stock_ratio:.1%}**，缓冲空间有限。"
            f"在股票市场大跌（如 -3%）的交易日，债券部分可能提供微弱的缓冲效果，但不足以完全对冲权益损失。"
        )
        hedge_verdict = "非股票部分的防御性属于'聊胜于无'水平，投资者不应将其视为安全垫。"
    elif non_stock_ratio < 0.35:
        hedge_score = "合格"
        hedge_desc = (
            f"非股票资产占 **{non_stock_ratio:.1%}**，构成了基本的防御层。"
            f"Beta 值 {beta_val:.2f}，最大回撤 {abs(max_dd_fund):.1f}%。"
            f"在股票市场大跌时，债券部分能提供一定的缓冲作用。"
        )
        hedge_verdict = "非股票部分的配置是合理的，符合混合型基金'攻守兼备'的定位。"
    else:
        hedge_score = "优秀"
        hedge_desc = (
            f"非股票资产占 **{non_stock_ratio:.1%}**，安全垫较厚。"
            f"在股票市场大幅下跌时，这部分资产能有效缓冲净值波动。"
        )
        hedge_verdict = "经理对非股票部分有系统性的配置思路，防御性管理能力突出。"

    return f"""非股票资产占比 **{non_stock_ratio:.1%}**，防御能力评级：**{hedge_score}**。

{hedge_desc}

{hedge_verdict}"""


def _analyze_cb_grey_zone(fund_name, stock_ratio, bond_ratio, cb_ratio) -> str:
    """维度 B：转债灰色地带分析"""
    if cb_ratio < 0.03:
        return f"""股票仓位 **{stock_ratio:.1%}**，基本不使用转债工具。经理倾向于通过纯股+纯债的方式进行资产配置，风格透明度较高。"""

    total_equity_like = stock_ratio + cb_ratio

    if total_equity_like > 0.95:
        severity = "🔴 **严重**"
        implication = (
            f"股票（{stock_ratio:.1%}）+ 可转债（{cb_ratio:.1%}）合计达 **{total_equity_like:.1%}**，"
            f"已超过偏股混合型95%的仓位上限（含隐性权益敞口）。"
            f"经理名义上是混合型，实际上在满仓干股票，波动将极大。"
        )
        advice = "投资者应将其视为高波动股票型产品，不适合风险承受能力较低的投资者。"
    elif total_equity_like > 0.85:
        severity = "🟡 **关注**"
        implication = (
            f"股票（{stock_ratio:.1%}）+ 可转债（{cb_ratio:.1%}）合计达 **{total_equity_like:.1%}**，"
            f"风格偏激进。可转债中隐含的 Delta 敞口在牛市中会放大收益，但在熊市中同样会放大亏损。"
        )
        advice = "建议关注可转债的平均转股溢价率和纯债价值，评估下行保护是否充分。"
    else:
        severity = "🟢 **正常**"
        implication = (
            f"股票（{stock_ratio:.1%}）+ 可转债（{cb_ratio:.1%}）合计 **{total_equity_like:.1%}**，"
            f"处于合理区间。经理通过可转债实现'进可攻、退可守'的策略，属于标准的混合型运作。"
        )
        advice = ""

    return f"""可转债仓位 **{cb_ratio:.1%}**，灰色地带评级：{severity}。

{implication}

{advice}"""


def _analyze_cash_timing_signal(fund_name, cash_ratio, historical_allocation) -> str:
    """维度 C：现金择时信号"""
    # 当前现金分析
    if cash_ratio > 0.15:
        current_signal = f"当前现金占比 **{cash_ratio:.1%}**，处于较高水平，经理可能在**持币观望**，对后市偏谨慎"
    elif cash_ratio > 0.08:
        current_signal = f"当前现金占比 **{cash_ratio:.1%}**，处于正常偏高水平，保留了一定的灵活度"
    elif cash_ratio > 0.03:
        current_signal = f"当前现金占比 **{cash_ratio:.1%}**，处于正常水平"
    else:
        current_signal = f"当前现金占比 **{cash_ratio:.1%}**，仓位打满，经理看好后市或不愿意错失机会"

    # 历史趋势分析
    history_signal = ""
    if historical_allocation and len(historical_allocation) >= 3:
        cash_series = [(q.get("date", ""), q.get("cash_ratio", 0)) for q in historical_allocation]
        cash_values = [v for _, v in cash_series if v > 0]

        if len(cash_values) >= 3:
            avg_cash = sum(cash_values) / len(cash_values)
            latest_cash = cash_values[-1]

            # 检测异常变动（最近一次 vs 历史均值）
            change = latest_cash - avg_cash
            if change > 0.08:
                history_signal = (
                    f"\n\n**📊 择时信号检测：**"
                    f"\n近期现金占比（{latest_cash:.1%}）相比历史均值（{avg_cash:.1%}）上升了 **{change:.1%}**，"
                    f"经理可能正在降低仓位，释放出**看空信号**。"
                    f"对于想买入的投资者，这是一个重要的择时参考——建议等待现金占比回落后再介入。"
                )
            elif change < -0.08:
                history_signal = (
                    f"\n\n**📊 择时信号检测：**"
                    f"\n近期现金占比（{latest_cash:.1%}）相比历史均值（{avg_cash:.1%}）下降了 **{abs(change):.1%}**，"
                    f"经理可能正在加仓，释放出**看多信号**。"
                )
            else:
                history_signal = (
                    f"\n\n**📊 择时信号检测：**"
                    f"\n现金占比变化不大（均值{avg_cash:.1%}），未检测到明显的择时信号。"
                    f"经理的现金管理较为稳定。"
                )

    return f"""{current_signal}。{history_signal}"""


# ============================================================
# 板块5：风险预警（RBSA 风格漂移）
# ============================================================

def _section5_risk_warning_rbsa(
    fund_name, max_dd_fund, max_dd_bm,
    dd_info, recovery_days, cm,
    beta_val, r2, ann_ret, smb, hml
) -> str:
    """五、风险预警：RBSA 动态回归风格漂移 + 压力测试"""

    # ── RBSA 风格漂移分析 ──
    rbsa_desc = _rbsa_analysis(smb, hml, r2, beta_val)

    # ── 回撤概况 ──
    defense_ratio = abs(max_dd_fund / max_dd_bm) if max_dd_bm != 0 else 1.0

    if defense_ratio < 0.8:
        defense_quality = f"最大回撤 **{abs(max_dd_fund):.1f}%**，明显小于基准的 {abs(max_dd_bm):.1f}%，防御能力突出"
    elif defense_ratio < 1.0:
        defense_quality = f"最大回撤 {abs(max_dd_fund):.1f}%，略好于基准的 {abs(max_dd_bm):.1f}%"
    elif defense_ratio < 1.2:
        defense_quality = f"最大回撤 {abs(max_dd_fund):.1f}%，与基准的 {abs(max_dd_bm):.1f}% 基本持平"
    else:
        defense_quality = f"最大回撤 {abs(max_dd_fund):.1f}%，大于基准的 {abs(max_dd_bm):.1f}%，防御能力偏弱"

    # 修复弹性
    if recovery_days and recovery_days > 0:
        if recovery_days < 30:
            recovery_desc = f"从最低点到回血完毕仅用 **{recovery_days}** 个交易日，修复弹性极强"
        elif recovery_days < 90:
            recovery_desc = f"回撤修复历时 **{recovery_days}** 个交易日，节奏适中"
        elif recovery_days < 180:
            recovery_desc = f"回撤修复历时 **{recovery_days}** 个交易日，属于 U 型磨底"
        else:
            recovery_desc = f"最长回撤修复周期达 **{recovery_days}** 个交易日，长期处于水下"
    else:
        recovery_desc = "当前基金仍处于回撤修复阶段"

    # 波动率 & 夏普
    vol = round(cm.volatility * 100, 1)
    sharpe = cm.sharpe_ratio

    # 压力测试
    stress_tests = _stress_test_scenarios(max_dd_fund, beta_val, vol, ann_ret)

    # FF 五因子暴露表
    factor_table = _build_factor_exposure_table(smb, hml, beta_val, r2)

    text = f"""### 五、风险预警

[INSERT_CHART: RBSA_DRIFT]

[INSERT_CHART: DRAWDOWN]

**RBSA 风格漂移分析**

{rbsa_desc}

**回撤概况**

{defense_quality}。{recovery_desc}。年化波动率 {vol:.1f}%，夏普比率 {sharpe:.2f}。

**Fama-French 五因子暴露**

{factor_table}

**压力测试（历史情景模拟）**

{stress_tests}"""

    return text


def _rbsa_analysis(smb, hml, r2, beta_val) -> str:
    """RBSA 基于净值的动态风格分析"""
    # R² 评估
    if r2 > 0.7:
        r2_quality = f"R² = **{r2:.2f}**，因子模型解释力强，基金风格稳定性高"
    elif r2 > 0.4:
        r2_quality = f"R² = {r2:.2f}，因子模型解释力一般，经理具有一定的独立风格"
    else:
        r2_quality = f"R² = {r2:.2f}，因子模型解释力弱，该基金风格独立性较强，可能有部分资产（如转债、定增）未被 FF 因子充分解释"

    # 风格定位
    style = _style_desc(smb, hml)

    # Beta 偏离分析
    if beta_val > 1.3:
        beta_warning = f"⚠️ Beta 值 {beta_val:.2f}，远超1.0，基金在上涨行情中弹性十足，但下跌时的跌幅也会被放大。对于混合型基金而言，Beta 超过1.3意味着经理可能在用更高的波动换取收益"
    elif beta_val < 0.7:
        beta_warning = f"ℹ️ Beta 值 {beta_val:.2f}，远低于1.0，基金呈现显著的低波动特征，但可能在牛市中跑输市场"
    else:
        beta_warning = ""

    # SMB/HML 边界分析
    factor_edge = _style_edge_desc(smb, hml)

    return f"""基于净值对 Fama-French 五因子的动态回归分析，{r2_quality}。

当前风格定位为**{style}型**。{factor_edge}。

{beta_warning}"""


def _build_factor_exposure_table(smb, hml, beta_val, r2) -> str:
    """构建 FF 五因子暴露表"""
    # SMB
    if abs(smb) > 0.3:
        smb_desc = "显著暴露" if smb > 0 else "显著规避"
    else:
        smb_desc = "中性"

    # HML
    if abs(hml) > 0.3:
        hml_desc = "偏好价值" if hml > 0 else "偏好成长"
    else:
        hml_desc = "均衡"

    # Beta
    if beta_val > 1.2:
        beta_desc = "高弹性"
    elif beta_val < 0.8:
        beta_desc = "低弹性"
    else:
        beta_desc = "市场同步"

    return f"""| 因子 | 暴露值 | 解读 |
|---|---|---|
| Market Beta | {beta_val:.2f} | {beta_desc} |
| SMB（规模） | {smb:.2f} | {smb_desc} |
| HML（价值） | {hml:.2f} | {hml_desc} |
| R²（解释力） | {r2:.2f} | {'风格稳定' if r2 > 0.6 else '风格漂移风险'} |"""


def _stress_test_scenarios(
    max_dd_fund: float, beta_val: float,
    vol: float, ann_ret: float
) -> str:
    """压力测试"""
    scenarios = [
        ("2015年股灾（-45%）", -0.45, 1.8),
        ("2018年贸易战（-30%）", -0.30, 1.2),
        ("2020年疫情冲击（-15%）", -0.15, 1.0),
        ("2022年加息潮（-25%）", -0.25, 1.1),
    ]

    lines = ["| 历史情景 | 市场跌幅 | 预估基金回撤 | 评估 |", "|---|---|---|---|"]

    for name, mkt_drop, multiplier in scenarios:
        beta_adj = beta_val if beta_val > 0.3 else 0.3
        est_dd = abs(max_dd_fund) * beta_adj * multiplier
        est_dd = min(est_dd, 65)

        if est_dd < 15:
            eval_text = "✅ 抗压"
        elif est_dd < 25:
            eval_text = "🟡 一般"
        elif est_dd < 40:
            eval_text = "⚠️ 承压"
        else:
            eval_text = "🔴 严重"

        lines.append(f"| {name} | {mkt_drop*100:.0f}% | ≈{est_dd:.1f}% | {eval_text} |")

    avg_dd = abs(max_dd_fund) * beta_val * 1.2
    if avg_dd < 20:
        summary = "综合压力测试结果表明，该基金在多数极端市场情景下预估回撤控制在 20% 以内，抗风险能力较强。"
    elif avg_dd < 35:
        summary = "该基金在极端市场情景下预估回撤在 20%-35% 区间，属于正常主动权益产品的风险水平。"
    else:
        summary = "该基金在极端市场情景下预估回撤可能超过 35%，高波动特征明显，仅适合风险承受能力较强的投资者。"

    return "\n".join(lines) + f"\n\n{summary}"


# ============================================================
# 板块6：投资建议
# ============================================================

def _section6_investment_advice(
    fund_name, grade, score, tags,
    net_alpha, max_dd_fund, monthly_win_rate,
    ir_value, m, basic
) -> str:
    """六、投资建议：拟买入 / 持有中 / 离场信号"""

    portrait = _manager_portrait(net_alpha, abs(max_dd_fund), monthly_win_rate, ir_value, m)
    risk_point = _identify_risk_point(m, abs(max_dd_fund))

    tag_str = "、".join([f"「{t}」" for t in tags[:3]]) if tags else "综合型"

    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    text = f"""### 六、投资建议

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
# 辅助函数
# ============================================================

def _build_headline(grade, fund_name, tags, score, start_date, end_date) -> str:
    emoji = {"A+": "🏆", "A": "✅", "B": "📊", "C": "🟡", "D": "❌"}.get(grade, "📊")
    tag_str = " · ".join(tags[:3]) if tags else "综合型"
    return (
        f"# {emoji} {fund_name} — 混合偏股深度评级报告\n"
        f"**分析区间**：{start_date} 至 {end_date} ｜ "
        f"**综合评分**：{score:.0f} 分 / {grade} 级 ｜ "
        f"**核心标签**：{tag_str}"
    )


def _extract_date_range(charts: dict) -> tuple[str, str]:
    cum_data = charts.get("cumulative_return", {})
    x_dates = cum_data.get("x", [])
    if x_dates and len(x_dates) >= 2:
        start = str(x_dates[0])[:10]
        end   = str(x_dates[-1])[:10]
        return start, end
    return "—", "—"


def _style_desc(smb: float, hml: float) -> str:
    size = "小盘" if smb > 0.3 else ("大盘" if smb < -0.3 else "中盘")
    style = "成长" if hml < -0.3 else ("价值" if hml > 0.3 else "均衡")
    return f"{size}{style}"


def _style_edge_desc(smb: float, hml: float) -> str:
    if hml < -0.3:
        return "经理在**成长风格占优**的市场环境中 Alpha 创造能力最强；而在价值/红利风格主导的行情中，超额趋于平缓甚至出现回落"
    elif hml > 0.3:
        return "经理在**价值风格占优**的市场中超额生成效率最高；在成长/科技风格领跑时，Alpha 可能有所消退"
    else:
        return "经理风格相对均衡，在成长与价值的切换中均能保持一定超额，但在极端单一风格行情中，超额创造能力有一定的局限性"


def _manager_portrait(net_alpha, max_dd_fund, monthly_win_rate, ir_value, m) -> str:
    alpha_type = "强Alpha型" if net_alpha > 8 else ("稳健Alpha型" if net_alpha > 3 else ("基准跟随型" if net_alpha > -3 else "跑输基准型"))
    defense_type = "强防御" if max_dd_fund < 15 else ("中等防御" if max_dd_fund < 25 else "弱防御")

    return (
        f'该经理是一位**"{alpha_type}、{defense_type}"**的主动管理者。'
        f"其 Alpha 来源{'并非依赖择时，而是通过在擅长的行业内挖掘具备超额弹性的个股' if ir_value > 0.3 else '显示出对市场节奏的一定敏感性，但选股能力的稳定性需进一步观察'}。"
        f"月度胜率 {monthly_win_rate:.1f}% {'表明其对市场节奏的把握较为精准' if monthly_win_rate > 55 else '显示其表现受市场风格影响较大'}。"
    )


def _identify_risk_point(m, max_dd_fund) -> str:
    risks = []

    if m.style_drift_flag:
        risks.append("⚠️ **风格漂移风险**：近期 Beta 偏离历史均值，经理可能在进行仓位结构调整")

    if max_dd_fund > 25:
        risks.append(f"⚠️ **高波动风险**：最大回撤 {max_dd_fund:.1f}%，在市场系统性下跌时净值损失可观")

    smb = m.factor_loadings.get("SMB", 0)
    if smb > 0.5:
        risks.append("⚠️ **小盘流动性风险**：基金重仓小市值股票，在市场极端情况下可能面临流动性折价")

    hml = m.factor_loadings.get("HML", 0)
    if hml < -0.5:
        risks.append("⚠️ **成长风格集中风险**：基金高度暴露于成长因子，若市场切换至价值风格，Alpha 可能阶段性消退")

    ir = m.information_ratio
    if ir < 0:
        risks.append("⚠️ **超额不稳定风险**：信息比率为负，超额的获取并不具有一致性")

    if not risks:
        risks.append("ℹ️ 当前未发现明显的结构性风险")

    return "\n\n".join(risks[:2])


def _fallback_report(basic) -> dict:
    return {
        "meta": {"fund_name": basic.name, "fund_type": "混合型-偏股", "grade": "—", "score": 0, "tags": []},
        "headline": f"# 【{basic.name}】数据不足，无法生成深度报告",
        "section1": "数据获取不完整，无法进行资产结构分析。",
        "section2": "数据不足，无法进行收益表现分析。",
        "section3": "持仓数据不足，无法进行深度分析。",
        "section4": "数据不足，无法进行非股票部分分析。",
        "section5": "风险数据不足，无法进行风格漂移分析。",
        "section6": "投资建议数据不足。",
        "full_text": "数据不足，无法生成完整的混合偏股深度评价报告。",
    }
