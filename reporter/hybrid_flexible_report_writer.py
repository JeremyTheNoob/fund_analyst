"""
混合型-灵活配置基金深度评价报告生成器 — fund_quant_v2
角色：资深基金分析师（CFA 持证人）
报告结构：5板块 + 图表插入点标记
  1. 收益情况展示（累计收益 + 收益归因 + 月度胜率）
  2. 择时风格（历史仓位区间 + 仓位/市场对照图 + 暴跌减仓复盘）
  3. 深度分析（股债持仓穿透 + 关键决策复盘 + 宏观事件时间线）
  4. 风险预警（水下回撤 + 实时Beta黑盒探测 + 择时失败预警 + 风格极端切换）
  5. 投资建议（拟买入/持有中/离场信号 + 仓位黑盒/择时/风格三重监控）
"""

from __future__ import annotations
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_hybrid_flexible_report(report: Any) -> dict:
    """
    生成混合型-灵活配置基金深度评价报告（5板块结构）

    Returns:
        {
          "meta":       {fund_name, fund_type, start_date, end_date, grade, score, tags},
          "headline":   标题行,
          "section1":   一、收益情况展示（[INSERT_CHART: CUM_RET] + [INSERT_CHART: HEATMAP]）,
          "section2":   二、择时风格（[INSERT_CHART: TIMING_COMPARISON]）,
          "section3":   三、深度分析（[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]）,
          "section4":   四、风险预警（[INSERT_CHART: DRAWDOWN] + [INSERT_CHART: ROLLING_BETA]）,
          "section5":   五、投资建议（Beta黑盒/择时失败/风格切换 三重监控）,
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
    top10_stocks = holdings.get("top10_stocks", [])

    # 评级描述
    grade_desc = _grade_description(grade, score)

    # 仓位统计
    pos_stats = _compute_position_stats(historical_allocation)

    # Beta 分析
    beta_data = _analyze_beta_for_report(report)

    # 择时贡献度分析
    timing_data = _analyze_timing_contribution(report)

    # 持仓断层分析
    drift_data = _analyze_holdings_drift(report)

    # ── 暴跌减仓复盘 ──────────────────────────────────────
    crash_review = _crash_avoidance_review(report, historical_allocation)

    # ── 构建报告 ──────────────────────────────────────────
    meta = {
        "fund_name": fund_name,
        "fund_type": "混合型-灵活配置",
        "start_date": start_date,
        "end_date": end_date,
        "grade": grade,
        "score": score,
        "tags": tags,
    }

    headline = _build_headline(fund_name, grade_desc, start_date, end_date, grade)

    section1 = _section1_performance(
        fund_name, cm, m, start_date, end_date,
        stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    )
    section2 = _section2_timing_style(
        fund_name, pos_stats, historical_allocation, crash_review, m,
    )
    section3 = _section3_deep_analysis(
        fund_name, top10_stocks, stock_ratio, bond_ratio, cb_ratio,
        historical_allocation, report,
    )
    section4 = _section4_risk_warning(
        fund_name, cm, m, beta_data, timing_data, drift_data,
    )
    section5 = _section5_investment_advice(
        fund_name, grade, score, tags, cm, m, beta_data, timing_data, drift_data, basic,
    )

    # 存储分析数据到 chart_data 供 main.py 使用
    report.chart_data["flexible_timing"] = timing_data
    report.chart_data["flexible_beta"] = beta_data
    report.chart_data["flexible_drift"] = drift_data
    report.chart_data["flexible_crash_review"] = crash_review
    report.chart_data["flexible_pos_stats"] = pos_stats

    full_text = "\n\n".join([headline, section1, section2, section3, section4, section5])

    return {
        "meta": meta,
        "headline": headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "section5": section5,
        "full_text": full_text,
    }


# ============================================================
# Section 1: 收益情况展示
# ============================================================

def _section1_performance(
    fund_name, cm, m, start_date, end_date,
    stock_ratio, bond_ratio, cash_ratio, cb_ratio,
) -> str:
    """一、收益情况展示：历史业绩 + 收益归因 + 月度胜率"""

    ann_ret = cm.annualized_return * 100
    cum_ret = cm.cumulative_return * 100
    max_dd = cm.max_drawdown * 100
    sharpe = cm.sharpe_ratio
    sortino = cm.sortino_ratio
    calmar = cm.calmar_ratio
    monthly_wr = cm.monthly_win_rate * 100
    volatility = cm.volatility * 100

    # Alpha
    net_alpha = m.alpha * 100 if hasattr(m, "alpha") else 0.0
    beta = m.beta if hasattr(m, "beta") else 1.0
    r2 = m.r_squared if hasattr(m, "r_squared") else 0.0
    ir = m.information_ratio if hasattr(m, "information_ratio") else 0.0
    te = m.tracking_error if hasattr(m, "tracking_error") else 0.0

    # 收益评价
    if ann_ret > 15:
        ret_eval = "🌟 表现优异"
    elif ann_ret > 8:
        ret_eval = "✅ 表现良好"
    elif ann_ret > 0:
        ret_eval = "⚠️ 收益平平"
    else:
        ret_eval = "❌ 收益为负"

    # 夏普评价
    if sharpe > 1.5:
        sharpe_eval = "风险调整后收益优秀"
    elif sharpe > 1.0:
        sharpe_eval = "风险调整后收益良好"
    elif sharpe > 0.5:
        sharpe_eval = "承担的波动与收益基本匹配"
    else:
        sharpe_eval = "风险调整后收益不理想"

    # 月度胜率评价
    if monthly_wr > 60:
        wr_eval = "月度胜率较高，大部分月份都能跑赢基准"
    elif monthly_wr > 50:
        wr_eval = "月度胜率略高于50%，具有一定的稳定性"
    else:
        wr_eval = "月度胜率偏低，胜负各半甚至更多月份跑输"

    # 资产配置归因
    asset_desc_parts = []
    if stock_ratio > 0.01:
        asset_desc_parts.append(f"股票 **{stock_ratio:.1%}**")
    if bond_ratio > 0.01:
        asset_desc_parts.append(f"债券 **{bond_ratio:.1%}**")
    if cb_ratio > 0.01:
        asset_desc_parts.append(f"可转债 **{cb_ratio:.1%}**")
    if cash_ratio > 0.01:
        asset_desc_parts.append(f"现金 **{cash_ratio:.1%}**")
    asset_desc = "、".join(asset_desc_parts) if asset_desc_parts else "数据缺失"

    # 波动评价
    if volatility < 8:
        vol_eval = "低波动，运作稳健"
    elif volatility < 15:
        vol_eval = "中等波动，符合混合型特征"
    else:
        vol_eval = "高波动，注意回撤风险"

    text = f"""### 一、收益情况展示

**{fund_name}** 分析区间：{start_date} ~ {end_date}

| 指标 | 数值 | 评价 |
|------|------|------|
| 年化收益 | {ann_ret:+.2f}% | {ret_eval} |
| 累计收益 | {cum_ret:+.2f}% | — |
| 最大回撤 | {max_dd:.2f}% | {'可控' if abs(max_dd) < 15 else '需警惕'} |
| 夏普比率 | {sharpe:.2f} | {sharpe_eval} |
| Sortino | {sortino:.2f} | — |
| 月度胜率 | {monthly_wr:.0f}% | {wr_eval} |
| 年化波动 | {volatility:.2f}% | {vol_eval} |
| Beta（全期）| {beta:.3f} | {'进攻型' if beta > 0.9 else '平衡型' if beta > 0.5 else '防守型'} |
| 年化 Alpha | {net_alpha:+.2f}% | {'显著' if net_alpha > 2 else '微弱' if net_alpha > 0 else '无'} |
| 信息比率 | {ir:.2f} | {'优秀' if ir > 0.5 else '良好' if ir > 0 else '不足'} |

**收益归因简析：**
- 最新资产配置为 {asset_desc}
- R² = {r2:.2%}（{'模型拟合良好' if r2 > 0.7 else '市场解释力较弱，选股贡献占主导'}）
- {'有效控制波动的同时获取了超额收益' if sharpe > 1 and volatility < 12 else '收益与波动基本匹配' if sharpe > 0.5 else '承担了较高波动但收益不理想'}

[INSERT_CHART: CUM_RET]

[INSERT_CHART: HEATMAP]"""

    return text


# ============================================================
# Section 2: 择时风格
# ============================================================

def _section2_timing_style(
    fund_name, pos_stats, historical_allocation, crash_review, m,
) -> str:
    """二、择时风格：仓位区间 + 暴跌减仓复盘"""

    # 仓位区间描述
    if pos_stats and pos_stats.get("n_quarters", 0) > 0:
        n_q = pos_stats["n_quarters"]
        hi = pos_stats["max_stock"]
        lo = pos_stats["min_stock"]
        avg = pos_stats["avg_stock"]
        hi_q = pos_stats["max_date"]
        lo_q = pos_stats["min_date"]
        swing = pos_stats["swing"]

        # 仓位风格判断
        if swing > 40:
            timing_style = "🔴 **激进择时型** — 仓位波动极大（振幅{:.0f}%），经理频繁大幅调仓，择时意图明显".format(swing * 100)
        elif swing > 20:
            timing_style = "🟡 **灵活调整型** — 仓位波动适中（振幅{:.0f}%），经理会根据市场环境适度调整仓位".format(swing * 100)
        else:
            timing_style = "🟢 **仓位稳定型** — 仓位波动较小（振幅{:.0f}%），经理倾向于通过选股而非择时获取收益".format(swing * 100)

        pos_table_rows = "\n".join([
            f"| {q.get('date', '—')} | {q.get('stock_ratio', 0)*100:.1f}% | {q.get('bond_ratio', 0)*100:.1f}% | {q.get('cash_ratio', 0)*100:.1f}% |"
            for q in historical_allocation
        ])

        text = f"""### 二、择时风格分析

**{timing_style}**

**历史仓位统计（近 {n_q} 个季度）：**
- 股票仓位范围：**{lo:.1%} ~ {hi:.1%}**，平均 **{avg:.1%}**
- 最高仓位：{hi:.1%}（{hi_q}）
- 最低仓位：{lo:.1%}（{lo_q}）
- 仓位振幅：{swing:.1%}

**逐季度资产配置变化：**

| 季度 | 股票仓位 | 债券仓位 | 现金仓位 |
|------|----------|----------|----------|
{pos_table_rows}

[INSERT_CHART: TIMING_COMPARISON]

"""
        # 暴跌减仓复盘
        if crash_review and crash_review.get("events"):
            events = crash_review["events"]
            text += f"""**🔥 暴跌减仓复盘：经理在几次大跌前成功避险了吗？**

"""
            for ev in events:
                market_drop = ev.get("market_drop", 0) * 100
                fund_drop = ev.get("fund_drop", 0) * 100
                pos_before = ev.get("pos_before", 0) * 100
                pos_after = ev.get("pos_after", 0) * 100
                judgement = ev.get("judgement", "")

                if judgement == "提前减仓":
                    icon = "🟢"
                elif judgement == "维持低位":
                    icon = "🟢"
                elif judgement == "未能预判":
                    icon = "🟡"
                elif judgement == "逆势加仓":
                    icon = "🔴"
                else:
                    icon = "⚪"

                text += f"""{icon} **{ev.get('period', '—')}**：市场跌幅 {market_drop:.1f}%，基金跌幅 {fund_drop:.1f}%
  - 暴跌前仓位：{pos_before:.1f}% → 暴跌后仓位：{pos_after:.1f}%
  - 评价：{judgement}

"""

            # 总结
            summary = crash_review.get("summary", "")
            if summary:
                text += f"**择时总评：** {summary}\n"

        return text
    else:
        return f"""### 二、择时风格分析

⚠️ 历史资产配置数据不足，无法进行择时风格分析。"""


# ============================================================
# Section 3: 深度分析
# ============================================================

def _section3_deep_analysis(
    fund_name, top10_stocks, stock_ratio, bond_ratio, cb_ratio,
    historical_allocation, report,
) -> str:
    """三、深度分析：股债持仓穿透 + 关键决策复盘"""

    cash_ratio = 0.0
    holdings = report.chart_data.get("holdings", {})
    if holdings:
        cash_ratio = holdings.get("cash_ratio", 0.0)

    # 持仓集中度
    n_stocks = len(top10_stocks)
    top1_ratio = 0.0
    top10_total = 0.0
    if top10_stocks:
        top1_ratio = top10_stocks[0].get("ratio", 0)
        top10_total = sum(s.get("ratio", 0) for s in top10_stocks)

    # 重仓股列表
    stock_lines = []
    for i, s in enumerate(top10_stocks[:10], 1):
        name = s.get("name", "未知")
        ratio = s.get("ratio", 0) * 100
        stock_lines.append(f"{i}. {name}（{ratio:.2f}%）")
    stock_table = "\n".join(stock_lines) if stock_lines else "数据暂缺"

    # 仓位变化趋势描述
    trend_desc = ""
    if len(historical_allocation) >= 2:
        recent = historical_allocation[-1]
        prev = historical_allocation[-2]
        delta_stock = recent.get("stock_ratio", 0) - prev.get("stock_ratio", 0)
        if delta_stock > 0.05:
            trend_desc = f"最新季度较上季度**加仓** {delta_stock:.1%}，经理看好后市"
        elif delta_stock < -0.05:
            trend_desc = f"最新季度较上季度**减仓** {abs(delta_stock):.1%}，经理趋于保守"
        else:
            trend_desc = f"最新季度仓位基本持平（变化 {delta_stock:+.1%}），维持现有策略"

    # 持仓集中度评价
    if top10_total > 0.6:
        conc_eval = "⚠️ 前10大重仓股集中度偏高，组合分散性不足"
    elif top10_total > 0.4:
        conc_eval = "✅ 前10大重仓股集中度适中"
    else:
        conc_eval = "🟢 前10大重仓股分散度较高，组合分散性良好"

    # 仓位弹性
    if historical_allocation and len(historical_allocation) >= 2:
        pos_range = max(q.get("stock_ratio", 0) for q in historical_allocation) - min(q.get("stock_ratio", 0) for q in historical_allocation)
        if pos_range > 0.3:
            pos_eval = f"仓位弹性极大（波动 {pos_range:.1%}），灵活配置能力突出但需关注择时胜率"
        elif pos_range > 0.15:
            pos_eval = f"仓位弹性适中（波动 {pos_range:.1%}），具备一定灵活配置能力"
        else:
            pos_eval = f"仓位弹性较小（波动 {pos_range:.1%}），更偏向稳定配置"
    else:
        pos_eval = "仓位弹性数据暂缺"

    text = f"""### 三、深度分析

**持仓结构（最新季报）：**
- 股票占比：**{stock_ratio:.1%}** | 债券占比：**{bond_ratio:.1%}** | 现金占比：**{cash_ratio:.1%}**
- 前10大重仓股占比：**{top10_total:.1%}**（{conc_eval}）
- 第一大重仓：{top10_stocks[0].get("name", "—") if top10_stocks else "—"}（{top1_ratio:.2f}%）
- {pos_eval}
- {trend_desc}

**前10大重仓股明细：**
{stock_table}

[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]"""

    return text


# ============================================================
# Section 4: 风险预警
# ============================================================

def _section4_risk_warning(
    fund_name, cm, m, beta_data, timing_data, drift_data,
) -> str:
    """四、风险预警：Beta黑盒 + 择时失败 + 风格切换"""

    max_dd = cm.max_drawdown * 100
    volatility = cm.volatility * 100
    sharpe = cm.sharpe_ratio
    monthly_wr = cm.monthly_win_rate * 100

    # --- Beta 黑盒探测 ---
    current_beta = beta_data.get("current_beta", 0.5) if beta_data else 0.5
    trend_beta = beta_data.get("beta_trend", "stable") if beta_data else "stable"
    beta_20d_avg = beta_data.get("beta_20d_avg", 0.5) if beta_data else 0.5
    beta_60d_avg = beta_data.get("beta_60d_avg", 0.5) if beta_data else 0.5

    if current_beta < 0.3:
        beta_alert = f"🟢 **低风险** — 当前 Beta = {current_beta:.2f}（{trend_beta}），经理正在「猫着」，即便股市崩盘，用户也无需过度惊慌"
    elif current_beta > 1.0:
        beta_alert = f"🔴 **高风险预警** — 当前 Beta = {current_beta:.2f}（{trend_beta}），经理在加杠杆或重仓高弹性股，一旦行情转头回撤将不可控"
    elif current_beta > 0.8:
        beta_alert = f"🟡 **中高风险** — 当前 Beta = {current_beta:.2f}（{trend_beta}），仓位偏进攻，注意市场风险"
    else:
        beta_alert = f"🟢 **中低风险** — 当前 Beta = {current_beta:.2f}（{trend_beta}），仓位较为谨慎"

    # --- 择时贡献度 ---
    timer_corr = timing_data.get("timer_correlation", 0.5) if timing_data else 0.5
    timer_alert = ""
    if timing_data:
        if timer_corr < 0.2:
            timer_alert = "🔴 **择时失败预警** — 滚动相关性极低，基金净值与市场走势严重脱钩，可能存在择时失败"
        elif timer_corr < 0.4:
            timer_alert = "🟡 **择时能力存疑** — 滚动相关性偏低，经理的择时操作效果有限"
        else:
            timer_alert = f"🟢 择时贡献度正常（滚动相关性 {timer_corr:.2f}）"
    else:
        timer_alert = "⚪ 择时数据暂缺"

    # --- 风格极端切换 ---
    drift_alert = ""
    if drift_data:
        drift_score = drift_data.get("drift_score", 0)
        if drift_score > 70:
            drift_alert = f"🔴 **风格极端切换预警** — 行业偏离度得分 {drift_score:.0f}/100，连续两季重仓股出现剧烈断层，经理可能在进行风格漂移"
        elif drift_score > 40:
            drift_alert = f"🟡 **风格调整提醒** — 行业偏离度得分 {drift_score:.0f}/100，持仓有一定调整，需持续关注"
        else:
            drift_alert = f"🟢 风格稳定（偏离度 {drift_score:.0f}/100），持仓连续性好"

        # 显示具体变动
        added = drift_data.get("added_stocks", [])
        removed = drift_data.get("removed_stocks", [])
        if added or removed:
            drift_alert += "\n  - 新进入前十大：" + "、".join(added[:5]) if added else "\n  - 前十大无新增"
            drift_alert += "\n  - 退出前十大：" + "、".join(removed[:5]) if removed else "\n  - 前十大无退出"
    else:
        drift_alert = "⚪ 持仓断层数据暂缺"

    # 风格漂移标记
    style_drift_flag = m.style_drift_flag if hasattr(m, "style_drift_flag") else False
    drift_warning = ""
    if style_drift_flag:
        drift_warning = "\n\n⚠️ **RBSA 模型检测到风格漂移信号**：近期 Beta 与全期 Beta 偏差超过阈值，经理可能改变了投资风格"

    text = f"""### 四、风险预警

[INSERT_CHART: DRAWDOWN]

**📊 实时仓位「黑盒」探测：**
- 当前 Beta：{current_beta:.3f}（20日均 {beta_20d_avg:.3f}，60日均 {beta_60d_avg:.3f}）
- {beta_alert}

**⏱ 择时贡献度监控（Timer Beta）：**
- {timer_alert}

**🔄 风格极端切换监控：**
- {drift_alert}{drift_warning}

[INSERT_CHART: ROLLING_BETA]

**综合风险指标：**
- 最大回撤：{max_dd:.2f}%（{'可控' if abs(max_dd) < 15 else '⚠️ 偏高' if abs(max_dd) < 25 else '🔴 高风险'}）
- 年化波动：{volatility:.2f}%
- 月度胜率：{monthly_wr:.0f}%"""

    return text


# ============================================================
# Section 5: 投资建议
# ============================================================

def _section5_investment_advice(
    fund_name, grade, score, tags, cm, m, beta_data, timing_data, drift_data, basic,
) -> str:
    """五、投资建议：Beta黑盒/择时失败/风格切换 三重监控"""

    ann_ret = cm.annualized_return * 100
    max_dd = cm.max_drawdown * 100
    monthly_wr = cm.monthly_win_rate * 100
    sharpe = cm.sharpe_ratio
    net_alpha = m.alpha * 100 if hasattr(m, "alpha") else 0.0
    ir = m.information_ratio if hasattr(m, "information_ratio") else 0.0

    # ── A. 买入建议 ──
    buy_signals = []
    buy_warnings = []
    if ann_ret > 10 and sharpe > 1.0:
        buy_signals.append(f"年化收益 {ann_ret:+.1f}%、夏普 {sharpe:.2f}，历史表现优异")
    if net_alpha > 2:
        buy_signals.append(f"年化 Alpha {net_alpha:+.1f}%，具备真实超额收益能力")
    if abs(max_dd) < 15:
        buy_signals.append(f"最大回撤仅 {max_dd:.1f}%，风控优秀")
    if monthly_wr > 55:
        buy_signals.append(f"月度胜率 {monthly_wr:.0f}%，大部分时间跑赢基准")

    if ann_ret < 0:
        buy_warnings.append(f"年化收益为负（{ann_ret:.1f}%），历史表现不佳")
    if abs(max_dd) > 25:
        buy_warnings.append(f"最大回撤达 {max_dd:.1f}%，回撤风险极大")
    if sharpe < 0.5:
        buy_warnings.append(f"夏普比率仅 {sharpe:.2f}，风险调整后收益不理想")

    if buy_signals:
        buy_text = "✅ " + "；".join(buy_signals)
    else:
        buy_text = "⚠️ 核心指标无突出亮点"
    if buy_warnings:
        buy_text += "\n  ⚠️ 注意：" + "；".join(buy_warnings)

    # ── B. 持有建议 ──
    hold_signals = []
    hold_warnings = []

    # Beta 黑盒
    current_beta = beta_data.get("current_beta", 0.5) if beta_data else 0.5
    if current_beta > 1.0:
        hold_warnings.append(f"当前 Beta = {current_beta:.2f} > 1.0，经理处于进攻模式，建议密切关注市场走势")
    elif current_beta < 0.3:
        hold_signals.append(f"当前 Beta = {current_beta:.2f} < 0.3，经理已防守，短期风险可控")

    # 择时监控
    if timing_data:
        timer_corr = timing_data.get("timer_correlation", 0.5)
        if timer_corr < 0.3:
            hold_warnings.append("择时贡献度偏低，基金与市场脱钩，建议审视经理策略是否失效")

    # 风格切换
    if drift_data:
        drift_score = drift_data.get("drift_score", 0)
        if drift_score > 50:
            hold_warnings.append(f"行业偏离度 {drift_score:.0f}，持仓剧烈调整中，建议观察下一季报确认方向")

    hold_text = "✅ " + "；".join(hold_signals) if hold_signals else ""
    if hold_warnings:
        hold_text += ("\n  " if hold_text else "⚠️ ") + "；".join(hold_warnings)
    if not hold_text:
        hold_text = "ℹ️ 当前无特别信号，建议定期检视"

    # ── C. 离场信号 ──
    exit_signals = []
    if abs(max_dd) > 30 and sharpe < 0.3:
        exit_signals.append(f"最大回撤 {max_dd:.1f}% + 夏普 {sharpe:.2f}，风险收益严重不匹配")
    if net_alpha < -3:
        exit_signals.append(f"年化 Alpha {net_alpha:.1f}%，持续跑输市场")
    if timing_data and timing_data.get("timer_correlation", 0.5) < 0.2:
        exit_signals.append("择时贡献度极低，净值与市场完全脱钩")
    if drift_data and drift_data.get("drift_score", 0) > 80:
        exit_signals.append("风格极端切换，偏离度得分过高，投资逻辑可能已改变")

    if exit_signals:
        exit_text = "🔴 " + "；\n🔴 ".join(exit_signals)
    else:
        exit_text = "🟢 当前无强烈离场信号"

    # 费用信息
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, "fee_manage") and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, "fee_custody") and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, "fee_sale") and basic.fee_sale else 0.0

    # 评级文字
    grade_map = {
        "S": "稀缺优质基金，适合长期重仓持有",
        "A": "优质基金，值得配置",
        "B": "中等水平基金，建议与其他基金搭配使用",
        "C": "表现一般，需仔细审视是否适合",
        "D": "建议回避或尽快赎回",
    }
    grade_advice = grade_map.get(grade, "数据不足以评估")

    text = f"""### 五、投资建议

**综合评级：{grade}（{score:.0f}/100）— {grade_advice}**

**A. 拟买入评估：**
{buy_text}

**B. 持有中监控（三重预警体系）：**
{hold_text}

> 💡 灵活配置型基金的核心看点是**择时能力**。建议重点关注：
> - **仓位黑盒**：通过 Beta 实时感知经理仓位状态（低Beta=猫着，高Beta=进攻中）
> - **择时贡献度**：市场涨基金不涨=择时失败，需警惕
> - **风格切换**：连续两季重仓股剧烈变动=风格漂移，可能改变投资逻辑

**C. 离场信号：**
{exit_text}

**费率信息：**
- 管理费 {mgmt_fee:.2f}% / 托管费 {custody_fee:.2f}% / 申购费 {purchase_fee:.2f}%"""

    return text


# ============================================================
# 辅助分析函数
# ============================================================

def _compute_position_stats(historical_allocation: list) -> dict:
    """
    计算历史仓位统计（8个季度最高/最低/平均股票占比）
    """
    if not historical_allocation or len(historical_allocation) == 0:
        return {"n_quarters": 0}

    stocks = [q.get("stock_ratio", 0) for q in historical_allocation]

    max_stock = max(stocks)
    min_stock = min(stocks)
    avg_stock = sum(stocks) / len(stocks)

    # 找到最高和最低仓位的日期
    max_date = historical_allocation[stocks.index(max_stock)].get("date", "—")
    min_date = historical_allocation[stocks.index(min_stock)].get("date", "—")

    return {
        "n_quarters": len(historical_allocation),
        "max_stock": max_stock,
        "min_stock": min_stock,
        "avg_stock": avg_stock,
        "max_date": max_date,
        "min_date": min_date,
        "swing": max_stock - min_stock,
    }


def _analyze_beta_for_report(report: Any) -> dict:
    """
    分析当前 Beta 状态（用于报告文本）。
    使用 equity_engine 已计算的滚动 Beta 数据。
    """
    m = report.equity_metrics
    if not m:
        return {"current_beta": 0.5, "beta_trend": "unknown", "beta_20d_avg": 0.5, "beta_60d_avg": 0.5}

    rolling_20 = m.rolling_beta_20d or []
    rolling_60 = m.rolling_beta_60d or []

    if rolling_20:
        current_beta = rolling_20[-1]
        beta_20d_avg = sum(rolling_20[-20:]) / min(len(rolling_20), 20) if rolling_20 else current_beta
    else:
        current_beta = m.beta if hasattr(m, "beta") else 0.5
        beta_20d_avg = current_beta

    if rolling_60:
        beta_60d_avg = sum(rolling_60[-60:]) / min(len(rolling_60), 60) if rolling_60 else current_beta
    else:
        beta_60d_avg = current_beta

    # Beta 趋势
    if len(rolling_20) >= 10:
        recent_avg = sum(rolling_20[-10:]) / 10
        older_avg = sum(rolling_20[-20:-10]) / 10 if len(rolling_20) >= 20 else sum(rolling_20[:-10]) / max(len(rolling_20) - 10, 1)
        if recent_avg - older_avg > 0.15:
            trend = "上升趋势（经理加仓中）"
        elif older_avg - recent_avg > 0.15:
            trend = "下降趋势（经理减仓中）"
        else:
            trend = "基本稳定"
    else:
        trend = "数据不足"

    return {
        "current_beta": current_beta,
        "beta_trend": trend,
        "beta_20d_avg": beta_20d_avg,
        "beta_60d_avg": beta_60d_avg,
        "rolling_20d": rolling_20,
        "rolling_60d": rolling_60,
    }


def _analyze_timing_contribution(report: Any) -> dict:
    """
    择时贡献度分析：通过滚动相关性评估择时效果。
    """
    charts = report.chart_data
    nav_df = charts.get("nav_df")
    benchmark_df = charts.get("benchmark_df")

    if nav_df is None or benchmark_df is None:
        return {"timer_correlation": 0.5}

    try:
        import numpy as np
        fund = nav_df[["date", "ret"]].copy()
        fund["date"] = pd.to_datetime(fund["date"])
        bm = benchmark_df[["date", "bm_ret"]].copy() if "bm_ret" in benchmark_df.columns else None

        if bm is None:
            return {"timer_correlation": 0.5}

        bm["date"] = pd.to_datetime(bm["date"])
        merged = fund.merge(bm, on="date", how="inner").dropna()

        if len(merged) < 60:
            return {"timer_correlation": 0.5}

        fund_rets = merged["ret"].values
        bm_rets = merged["bm_ret"].values

        # 滚动相关性（60日窗口）
        window = 60
        correlations = []
        for i in range(window, len(merged)):
            corr = np.corrcoef(fund_rets[i - window:i], bm_rets[i - window:i])[0, 1]
            if not np.isnan(corr):
                correlations.append(round(corr, 3))

        timer_corr = np.mean(correlations[-20:]) if correlations else 0.5

        return {
            "timer_correlation": round(timer_corr, 3),
            "recent_correlations": correlations[-20:] if correlations else [],
        }
    except Exception as e:
        logger.warning(f"[hybrid_flexible] 择时贡献度分析失败: {e}")
        return {"timer_correlation": 0.5}


def _analyze_holdings_drift(report: Any) -> dict:
    """
    风格极端切换分析：对比连续两个季度的前十大重仓。
    通过 Jaccard 相似度计算持仓断层。
    """
    try:
        from data_loader.equity_loader import load_stock_holdings
        symbol = report.basic.symbol

        # 加载两个季度的持仓数据
        from data_loader.equity_loader import _generate_quarter_dates
        quarter_dates = _generate_quarter_dates(max_quarters=3)

        holdings_by_quarter = {}
        for qd in quarter_dates[:2]:  # 最新两个季度
            try:
                df = load_stock_holdings(symbol, date=qd)
                if df is not None and not df.empty:
                    names = set()
                    for _, row in df.iterrows():
                        name = row.get("stock_name", row.get("名称", ""))
                        if name:
                            names.add(str(name))
                    holdings_by_quarter[qd] = names
            except Exception:
                continue

        quarters = sorted(holdings_by_quarter.keys())
        if len(quarters) < 2:
            return {"drift_score": 0, "added_stocks": [], "removed_stocks": []}

        current = holdings_by_quarter[quarters[-1]]
        previous = holdings_by_quarter[quarters[-2]]

        # Jaccard 相似度
        intersection = current & previous
        union = current | previous
        jaccard = len(intersection) / len(union) if union else 1.0

        # 偏离度得分（0=无变化, 100=完全不同）
        drift_score = round((1 - jaccard) * 100, 1)

        added = list(current - previous)
        removed = list(previous - current)

        return {
            "drift_score": drift_score,
            "jaccard": round(jaccard, 3),
            "added_stocks": added,
            "removed_stocks": removed,
            "current_quarter": quarters[-1],
            "previous_quarter": quarters[-2],
        }
    except Exception as e:
        logger.warning(f"[hybrid_flexible] 持仓断层分析失败: {e}")
        return {"drift_score": 0, "added_stocks": [], "removed_stocks": []}


def _crash_avoidance_review(report: Any, historical_allocation: list) -> dict:
    """
    暴跌减仓复盘：检查历史几次大跌前经理是否减仓。

    算法：识别市场大跌区间，检查大跌前后的仓位变化。
    """
    if not historical_allocation or len(historical_allocation) < 3:
        return {"events": [], "summary": "历史数据不足"}

    try:
        import pandas as pd
        import numpy as np

        charts = report.chart_data
        benchmark_df = charts.get("benchmark_df")
        nav_df = charts.get("nav_df")

        if benchmark_df is None or nav_df is None:
            return {"events": [], "summary": "数据不足"}

        # 构建合并数据
        fund = nav_df[["date", "ret"]].copy()
        fund["date"] = pd.to_datetime(fund["date"])
        bm = benchmark_df[["date", "bm_ret"]].copy() if "bm_ret" in benchmark_df.columns else None
        if bm is None:
            return {"events": [], "summary": "基准数据不足"}

        bm["date"] = pd.to_datetime(bm["date"])
        merged = fund.merge(bm, on="date", how="inner").sort_values("date").reset_index(drop=True)

        if len(merged) < 120:
            return {"events": [], "summary": "数据时长不足"}

        # 识别市场大跌区间（60日累计跌幅 > 10%）
        bm_cumret = (1 + merged["bm_ret"]).cumprod()
        events = []

        i = 0
        while i < len(merged) - 60:
            window = merged.iloc[i:i + 60]
            window_drop = (window["bm_ret"] + 1).prod() - 1

            if window_drop < -0.10:  # 市场大跌 10%+
                period_start = str(window["date"].iloc[0].date())
                period_end = str(window["date"].iloc[-1].date())
                market_drop = window_drop
                fund_drop = (window["ret"] + 1).prod() - 1

                # 查找大跌前后的仓位
                crash_start = window["date"].iloc[0]
                pos_before = None
                pos_after = None

                for q in historical_allocation:
                    q_date = pd.to_datetime(q.get("date", ""))
                    if q_date <= crash_start:
                        pos_before = q.get("stock_ratio", 0)
                    elif pos_after is None:
                        pos_after = q.get("stock_ratio", 0)

                # 判断
                judgement = "数据不足"
                if pos_before is not None and pos_after is not None:
                    delta = pos_after - pos_before
                    if delta < -0.10:
                        judgement = "提前减仓"
                    elif pos_before < 0.4:
                        judgement = "维持低位"
                    elif fund_drop > market_drop:
                        judgement = "未能预判"
                    elif delta > 0.05:
                        judgement = "逆势加仓"
                    else:
                        judgement = "小幅调整"

                events.append({
                    "period": f"{period_start} ~ {period_end}",
                    "market_drop": market_drop,
                    "fund_drop": fund_drop,
                    "pos_before": pos_before if pos_before is not None else 0,
                    "pos_after": pos_after if pos_after is not None else 0,
                    "judgement": judgement,
                })

                i += 60  # 跳过已分析区间
            else:
                i += 5  # 步进

        # 最多保留最近5次
        events = events[-5:]

        # 总结
        if events:
            success_count = sum(1 for e in events if e["judgement"] in ("提前减仓", "维持低位"))
            total = len(events)
            if success_count == total:
                summary = f"经理在 {total} 次大跌中全部成功避险/保持低仓位，择时能力突出"
            elif success_count >= total * 0.6:
                summary = f"经理在 {success_count}/{total} 次大跌中成功应对，择时能力尚可"
            else:
                summary = f"经理在 {success_count}/{total} 次大跌中成功应对，择时能力有待提升"
        else:
            summary = "分析区间内未检测到显著市场大跌（60日跌幅>10%）"

        return {"events": events, "summary": summary}

    except Exception as e:
        logger.warning(f"[hybrid_flexible] 暴跌减仓复盘失败: {e}")
        return {"events": [], "summary": "分析失败"}


# ============================================================
# 通用辅助函数
# ============================================================

def _extract_date_range(charts: dict) -> tuple[str, str]:
    cum_data = charts.get("cumulative_return", {})
    x_dates = cum_data.get("x", [])
    if x_dates and len(x_dates) >= 2:
        start = str(x_dates[0])[:10]
        end = str(x_dates[-1])[:10]
        return start, end
    return "—", "—"


def _grade_description(grade: str, score: float) -> str:
    """评级描述"""
    grade_map = {
        "S": "稀缺优质 ⭐⭐⭐⭐⭐",
        "A": "优秀 ⭐⭐⭐⭐",
        "B": "良好 ⭐⭐⭐",
        "C": "一般 ⭐⭐",
        "D": "较差 ⭐",
    }
    return f"{grade} 级（{score:.0f} 分）" + grade_map.get(grade, "")


def _build_headline(fund_name, grade_desc, start_date, end_date, grade) -> str:
    """报告标题行"""
    return (
        f"## 📊 {fund_name} — 混合型·灵活配置深度评价\n\n"
        f"**综合评级：{grade_desc}** | 分析区间：{start_date} ~ {end_date}\n\n"
        f"> 💡 灵活配置型基金的核心价值在于**择时能力**——经理能否在牛熊切换时精准调仓。"
        f"本报告将从收益、择时、持仓、风险四个维度进行穿透式分析。"
    )


def _fallback_report(basic) -> dict:
    """数据不足时的兜底报告"""
    fund_name = basic.name if hasattr(basic, "name") else "未知基金"
    text = f"### {fund_name}\n\n⚠️ 数据不足，无法生成完整分析报告。请确认基金代码正确且数据源可用。"
    return {
        "meta": {"fund_name": fund_name, "fund_type": "混合型-灵活配置"},
        "headline": f"## 📊 {fund_name}",
        "section1": text,
        "section2": "",
        "section3": "",
        "section4": "",
        "section5": "",
        "full_text": text,
    }


# 顶部导入 pandas（供 _analyze_timing_contribution 使用）
import pandas as pd
