"""
混合型-偏股基金深度评价报告生成器 — fund_quant_v2
报告结构：6板块 + 图表插入点标记
  1. 资产结构
  2. 收益表现
  3. 深度分析
  4. 非股票部分
  5. 风险预警
  6. 投资建议
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
          "meta":       {fund_name, fund_type, start_date, end_date, tags},
          "headline":   标题行,
          "section1":   一、资产结构（[INSERT_CHART: ASSET_ALLOCATION]）,
          "section2":   二、收益表现（[INSERT_CHART: CUM_RET] + [INSERT_CHART: HEATMAP]）,
          "section3":   三、深度分析（[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]）,
          "section4":   四、非股票部分,
          "section5":   五、风险预警（[INSERT_CHART: RBSA_DRIFT] + [INSERT_CHART: DRAWDOWN]）,
          "section6":   六、投资建议,
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
    cum_bm_tr = round(bm_info.get("bm_last_return", 0) * 100, 1)

    # Alpha
    net_alpha = round(m.alpha * 100, 1)
    ir_value  = round(m.information_ratio, 2)

    # 风险数据
    max_dd_fund = round(cm.max_drawdown * 100, 1)
    dd_info     = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm   = round(dd_info.get("bm_max_dd", 0), 1)
    recovery_days = cm.recovery_days or 0

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
        "tags": tags,
    }

    # 板块1：资产结构
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

    # 板块3：深度分析
    top_contributors = _section_top_contributors(report, fund_name)
    section3 = f"""### 三、深度分析

{top_contributors}

[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]

[INSERT_CHART: HOLDINGS_RETENTION]"""

    # 板块4：非股票部分
    section4 = _section4_non_equity(
        stock_ratio, bond_ratio, cash_ratio, cb_ratio,
        beta_val, max_dd_fund, historical_allocation
    )

    # 板块5：风险预警
    section5 = _section5_risk_warning(
        max_dd_fund, max_dd_bm, dd_info, recovery_days,
        cm, beta_val, r2, ann_ret, smb, hml
    )

    # 板块6：投资建议
    section6 = _section6_investment_advice(
        fund_name, tags, net_alpha, max_dd_fund,
        monthly_win_rate, ir_value, m, basic
    )

    # 标题行
    headline = _build_headline(fund_name, tags, start_date, end_date)

    full_text = "\n\n".join([
        headline, section1, section2, section3, section4, section5, section6
    ])

    return {
        "meta": meta,
        "headline": headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "section5": section5,
        "section6": section6,
        "full_text": full_text,
    }


# ============================================================
# 板块1：资产结构
# ============================================================

def _section1_asset_structure(
    fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    historical_allocation, start_date, end_date
) -> str:
    """一、资产结构"""

    # 股票仓位
    if stock_ratio > 0.90:
        pos_desc = f"股票仓位 **{stock_ratio:.1%}**，逼近偏股混合型上限（95%），基本满仓干"
    elif stock_ratio > 0.80:
        pos_desc = f"股票仓位 **{stock_ratio:.1%}**，标准偏股水平"
    elif stock_ratio > 0.70:
        pos_desc = f"股票仓位 **{stock_ratio:.1%}**，比同类偏股基金低一些，经理偏防守"
    else:
        pos_desc = f"股票仓位只有 **{stock_ratio:.1%}**，明显低于标准，需关注"

    non_stock = 1 - stock_ratio
    parts = []
    if bond_ratio > 0.01:
        parts.append(f"债券 **{bond_ratio:.1%}**")
    if cb_ratio > 0.01:
        parts.append(f"可转债 **{cb_ratio:.1%}**")
    if cash_ratio > 0.01:
        parts.append(f"现金 **{cash_ratio:.1%}**")
    non_stock_desc = "、".join(parts) if parts else f"非股票资产 **{non_stock:.1%}**"
    buffer_desc = f"非股票部分占 **{non_stock:.1%}**（{non_stock_desc}），{'有一定的安全垫' if non_stock > 0.15 else '安全垫很薄'}"

    # 历史仓位趋势
    history_desc = ""
    if historical_allocation and len(historical_allocation) >= 2:
        history_desc = _analyze_position_trend(historical_allocation)

    # 转债预警
    cb_warning = ""
    total_equity_like = stock_ratio + cb_ratio
    if cb_ratio > 0.10:
        cb_warning = f"\n\n**可转债灰色地带**：转债仓位 **{cb_ratio:.1%}**，加上股票后股性资产达 **{total_equity_like:.1%}**。"
        if total_equity_like > 0.95:
            cb_warning += "已经超过 95%，名义上是混合型，实际上等于股票型，波动会很大。"
        elif total_equity_like > 0.85:
            cb_warning += "接近 90%，风格偏激进。"
        else:
            cb_warning += "属于合理的转债增强策略。"

    text = f"""### 一、资产结构

{fund_name}是混合偏股基金，核心看点就是股债怎么配。我们来看经理的仓位管理。

[INSERT_CHART: ASSET_ALLOCATION]

{pos_desc}。{buffer_desc}。

| 资产类别 | 占比 |
|---|---|
| 股票 | {stock_ratio:.1%} |
| 债券 | {bond_ratio:.1%} |
| 可转债 | {cb_ratio:.1%} |
| 现金及其他 | {cash_ratio:.1%} |
{history_desc}{cb_warning}"""

    return text


def _analyze_position_trend(historical_allocation: list) -> str:
    """分析历史仓位趋势"""
    stock_values = [q.get("stock_ratio", 0) for q in historical_allocation if q.get("stock_ratio", 0) > 0]
    if len(stock_values) < 2:
        return ""

    avg_stock = sum(stock_values) / len(stock_values)
    max_stock = max(stock_values)
    min_stock = min(stock_values)
    latest = stock_values[-1]
    swing = max_stock - min_stock

    if swing > 0.20:
        style = "大幅择时型"
        detail = f"仓位波动 {min_stock:.1%} ~ {max_stock:.1%}，振幅 {swing:.1%}，经理会通过仓位调来做择时"
    elif swing > 0.10:
        style = "适度调节型"
        detail = f"仓位波动 {min_stock:.1%} ~ {max_stock:.1%}，振幅 {swing:.1%}，偶尔微调"
    else:
        style = "稳定仓位型"
        detail = f"仓位稳定在 {min_stock:.1%} ~ {max_stock:.1%}，不靠择时，靠选股"

    if latest > avg_stock + 0.05:
        signal = f"当前仓位（{latest:.1%}）**高于均值**（{avg_stock:.1%}），经理偏乐观"
    elif latest < avg_stock - 0.05:
        signal = f"当前仓位（{latest:.1%}）**低于均值**（{avg_stock:.1%}），经理偏谨慎"
    else:
        signal = f"当前仓位（{latest:.1%}）接近均值，没太大变化"

    return f"\n\n历史看，经理是**{style}**的选手——{detail}。{signal}。"


# ============================================================
# 板块2：收益表现
# ============================================================

def _section2_return_performance(
    fund_name, start_date, end_date,
    cum_fund, cum_bm_tr, ann_ret,
    net_alpha, beta_val, smb, hml, basic
) -> str:
    """二、收益表现"""

    excess = round(cum_fund - cum_bm_tr, 1)
    if excess > 10:
        alpha_desc = f"累计跑赢基准 **{excess:.1f} 个百分点**，选股能力很强"
    elif excess > 3:
        alpha_desc = f"累计跑赢基准 **{excess:.1f} 个百分点**"
    elif excess > 0:
        alpha_desc = f"小幅跑赢 {excess:.1f} 个百分点"
    else:
        alpha_desc = f"落后基准 {abs(excess):.1f} 个百分点，选股能力存疑"

    if ann_ret > 15:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，很高了"
    elif ann_ret > 8:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，还不错"
    elif ann_ret > 0:
        ann_desc = f"年化收益率 {ann_ret:.1f}%"
    else:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，亏钱了"

    if beta_val > 1.1:
        beta_desc = f"Beta **{beta_val:.2f}**，涨的时候猛，跌的时候也猛"
    elif beta_val < 0.9:
        beta_desc = f"Beta **{beta_val:.2f}**，比大盘稳"
    else:
        beta_desc = f"Beta **{beta_val:.2f}**，跟大盘同步"

    text = f"""### 二、收益表现

[INSERT_CHART: CUM_RET]

[INSERT_CHART: HEATMAP]

从 {start_date} 到 {end_date}，累计收益率 **{cum_fund:.1f}%**，年化 **{ann_ret:.1f}%**。{ann_desc}。

{alpha_desc}。{beta_desc}。风格上偏向{_style_desc(smb, hml)}。"""

    return text


# ============================================================
# 板块3：深度分析（持仓穿透）
# ============================================================

def _section_top_contributors(report: Any, fund_name: str) -> str:
    """Top 持仓分析"""
    holdings_data = report.chart_data.get("holdings", {})
    top10_stocks = holdings_data.get("top10_stocks", [])

    if not top10_stocks:
        return "当前暂无法获取持仓数据。"

    sorted_stocks = sorted(top10_stocks, key=lambda x: x.get('占净值比例', 0), reverse=True)
    top5 = sorted_stocks[:5]

    parts = []
    total_ratio = sum(s.get('占净值比例', 0) for s in top5)
    parts.append(f"前五大重仓股合计 **{total_ratio:.1f}%**，{'比较集中' if total_ratio > 30 else '相对分散'}。")

    for i, stock in enumerate(top5[:3], 1):
        name = stock.get('股票名称', '?')
        ratio = stock.get('占净值比例', 0)
        parts.append(f"{i}. **{name}** — 占比 **{ratio:.1f}%**")

    if total_ratio > 40:
        parts.append(f"\n**注意：** 前五大持仓超过 40%，基金表现和这几只股票深度绑定。")

    return "\n".join(parts)


# ============================================================
# 板块4：非股票部分
# ============================================================

def _section4_non_equity(
    stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    beta_val, max_dd_fund, historical_allocation
) -> str:
    """四、非股票部分分析"""

    non_stock = 1 - stock_ratio
    total_equity_like = stock_ratio + cb_ratio

    # 风险对冲能力
    if non_stock < 0.10:
        hedge = f"非股票资产只有 **{non_stock:.1%}**，几乎没有安全垫，本质上是'加了点债券的股票基金'"
    elif non_stock < 0.20:
        hedge = f"非股票资产 **{non_stock:.1%}**，安全垫有限，聊胜于无"
    elif non_stock < 0.35:
        hedge = f"非股票资产 **{non_stock:.1%}**，有基本的防御层，还算合理"
    else:
        hedge = f"非股票资产 **{non_stock:.1%}**，安全垫较厚，防守能力不错"

    # 转债灰色地带
    if cb_ratio < 0.03:
        cb = "基本不用转债，风格比较透明。"
    elif total_equity_like > 0.95:
        cb = f"转债 **{cb_ratio:.1%}** + 股票 **{stock_ratio:.1%}** = **{total_equity_like:.1%}**，超过 95%，满仓干股票，波动会很大。"
    elif total_equity_like > 0.85:
        cb = f"转债 **{cb_ratio:.1%}** + 股票 **{stock_ratio:.1%}** = **{total_equity_like:.1%}**，风格偏激进，注意转债的权益敞口。"
    else:
        cb = f"转债 **{cb_ratio:.1%}**，加上股票后 **{total_equity_like:.1%}**，属于合理的转债增强策略。"

    # 现金择时信号
    if cash_ratio > 0.15:
        cash = f"现金占 **{cash_ratio:.1%}**，经理在**持币观望**，对后市偏谨慎"
    elif cash_ratio < 0.03:
        cash = f"现金几乎为零，仓位打满，经理看好后市"
    else:
        cash = f"现金 **{cash_ratio:.1%}**，正常水平"

    # 历史择时信号
    timing_signal = ""
    if historical_allocation and len(historical_allocation) >= 3:
        cash_vals = [q.get("cash_ratio", 0) for q in historical_allocation if q.get("cash_ratio", 0) > 0]
        if len(cash_vals) >= 3:
            avg_cash = sum(cash_vals) / len(cash_vals)
            latest_cash = cash_vals[-1]
            change = latest_cash - avg_cash
            if change > 0.08:
                timing_signal = f"\n近期现金占比相比历史均值上升了 **{change:.1%}**，经理可能在**降低仓位**，释放看空信号。"
            elif change < -0.08:
                timing_signal = f"\n近期现金占比相比历史均值下降了 **{abs(change):.1%}**，经理可能在**加仓**，释放看多信号。"

    text = f"""### 四、非股票部分

混合偏股基金里，非股票部分是"安全垫"，也能反映经理的投资思路。

**防御能力**：{hedge}。

**转债灰色地带**：{cb}

**现金信号**：{cash}。{timing_signal}"""

    return text


# ============================================================
# 板块5：风险预警
# ============================================================

def _section5_risk_warning(
    max_dd_fund, max_dd_bm, dd_info, recovery_days,
    cm, beta_val, r2, ann_ret, smb, hml
) -> str:
    """五、风险预警"""

    defense_ratio = abs(max_dd_fund / max_dd_bm) if max_dd_bm != 0 else 1.0

    if defense_ratio < 0.8:
        dd_desc = f"回撤 **{abs(max_dd_fund):.1f}%**，比基准的 {abs(max_dd_bm):.1f}% 小不少，防守不错"
    elif defense_ratio < 1.0:
        dd_desc = f"回撤 {abs(max_dd_fund):.1f}%，略好于基准的 {abs(max_dd_bm):.1f}%"
    elif defense_ratio < 1.2:
        dd_desc = f"回撤 {abs(max_dd_fund):.1f}%，和基准差不多"
    else:
        dd_desc = f"回撤 {abs(max_dd_fund):.1f}%，比基准的 {abs(max_dd_bm):.1f}% 还大，防守偏弱"

    if recovery_days and recovery_days > 0:
        if recovery_days < 30:
            rec_desc = f"回血只用了 **{recovery_days} 个交易日**"
        elif recovery_days < 90:
            rec_desc = f"回血用了 {recovery_days} 个交易日"
        elif recovery_days < 180:
            rec_desc = f"回血磨了 {recovery_days} 个交易日"
        else:
            rec_desc = f"最长一次回血用了 {recovery_days} 个交易日，泡了很久"
    else:
        rec_desc = "目前还在回血路上"

    vol = round(cm.volatility * 100, 1)
    sharpe = cm.sharpe_ratio

    # 风格漂移
    if r2 > 0.7:
        r2_desc = f"R² {r2:.2f}，风格稳定"
    elif r2 > 0.4:
        r2_desc = f"R² {r2:.2f}，有一定独立风格"
    else:
        r2_desc = f"R² {r2:.2f}，风格很独立，不太随大盘走"

    style = _style_desc(smb, hml)
    if beta_val > 1.3:
        beta_warn = f"Beta {beta_val:.2f} 太高了，涨的时候爽，跌的时候也惨"
    elif beta_val < 0.7:
        beta_warn = f"Beta {beta_val:.2f} 比较低，可能在牛市里跑不赢大盘"
    else:
        beta_warn = ""

    stress = _stress_test_scenarios(max_dd_fund, beta_val, vol, ann_ret)

    text = f"""### 五、风险预警

[INSERT_CHART: RBSA_DRIFT]

[INSERT_CHART: DRAWDOWN]

{dd_desc}。{rec_desc}。波动率 {vol:.1f}%，夏普比率 {sharpe:.2f}。

风格定位为**{style}型**，{r2_desc}。{beta_warn}

**压力测试**

{stress}"""

    return text


def _stress_test_scenarios(
    max_dd_fund: float, beta_val: float,
    vol: float, ann_ret: float
) -> str:
    """压力测试"""
    scenarios = [
        ("2015年股灾", -0.45, 1.8),
        ("2018年贸易战", -0.30, 1.2),
        ("2020年疫情", -0.15, 1.0),
        ("2022年加息潮", -0.25, 1.1),
    ]

    lines = ["| 历史情景 | 市场跌幅 | 预估基金回撤 |", "|---|---|---|"]

    for name, mkt_drop, multiplier in scenarios:
        beta_adj = beta_val if beta_val > 0.3 else 0.3
        est_dd = abs(max_dd_fund) * beta_adj * multiplier
        est_dd = min(est_dd, 65)

        if est_dd < 15:
            eval_text = "扛得住"
        elif est_dd < 25:
            eval_text = "一般"
        elif est_dd < 40:
            eval_text = "比较难受"
        else:
            eval_text = "很受伤"

        lines.append(f"| {name} | {mkt_drop*100:.0f}% | ≈{est_dd:.1f}% | {eval_text} |")

    avg_dd = abs(max_dd_fund) * beta_val * 1.2
    if avg_dd < 20:
        summary = "综合看，极端行情下回撤在 20% 以内，风控还可以。"
    elif avg_dd < 35:
        summary = "极端行情下预估回撤 20%-35%，正常水平，注意控制仓位。"
    else:
        summary = "极端行情下预估回撤可能超过 35%，只适合风险承受力强的人。"

    return "\n".join(lines) + f"\n\n{summary}"


# ============================================================
# 板块6：投资建议
# ============================================================

def _section6_investment_advice(
    fund_name, tags, net_alpha, max_dd_fund,
    monthly_win_rate, ir_value, m, basic
) -> str:
    """六、投资建议"""

    portrait = _manager_portrait(net_alpha, abs(max_dd_fund), monthly_win_rate, ir_value, m)
    risk_point = _identify_risk_point(m, abs(max_dd_fund))

    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    text = f"""### 六、投资建议

{portrait}

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

**费率：** 管理费 {mgmt_fee:.2f}% · 托管费 {custody_fee:.2f}% · 申购费 {purchase_fee:.2f}% · 赎回费 {redeem_fee:.2f}%"""

    return text


# ============================================================
# 辅助函数
# ============================================================

def _build_headline(fund_name, tags, start_date, end_date) -> str:
    tag_str = " · ".join(tags[:3]) if tags else "综合型"
    return (
        f"## {fund_name} — 混合偏股深度分析\n"
        f"分析区间：{start_date} 至 {end_date} ｜ 风格标签：{tag_str}"
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


def _manager_portrait(net_alpha, max_dd_fund, monthly_win_rate, ir_value, m) -> str:
    if net_alpha > 8:
        alpha_type = "Alpha 很强"
    elif net_alpha > 3:
        alpha_type = "Alpha 还行"
    elif net_alpha > -3:
        alpha_type = "Alpha 一般"
    else:
        alpha_type = "跑不赢基准"

    if max_dd_fund < 15:
        defense_type = "回撤控制得好"
    elif max_dd_fund < 25:
        defense_type = "回撤中等"
    else:
        defense_type = "回撤偏大"

    win_desc = f"月度胜率 {monthly_win_rate:.1f}%，" + (
        "对市场节奏把握得不错" if monthly_win_rate > 55
        else "受市场风格影响较大"
    )

    return f"这位经理是**{alpha_type}、{defense_type}**的选手。{win_desc}。"


def _identify_risk_point(m, max_dd_fund) -> str:
    risks = []

    if m.style_drift_flag:
        risks.append("**风格漂移**：最近 Beta 偏离历史均值，经理可能在调仓位")
    if max_dd_fund > 25:
        risks.append(f"**高波动**：最大回撤 {max_dd_fund:.1f}%，市场暴跌时净值会很难看")
    smb = m.factor_loadings.get("SMB", 0)
    if smb > 0.5:
        risks.append("**小盘风险**：重仓小市值股票，极端行情下可能卖不出去")
    hml = m.factor_loadings.get("HML", 0)
    if hml < -0.5:
        risks.append("**成长集中**：高度押注成长股，市场切到价值风格时可能吃土")
    ir = m.information_ratio
    if ir < 0:
        risks.append("**超额不稳**：信息比率为负，超额时有时无")

    if not risks:
        risks.append("目前没发现明显的结构性风险")

    return "\n\n".join(risks[:2])


def _fallback_report(basic) -> dict:
    return {
        "meta": {"fund_name": basic.name, "fund_type": "混合型-偏股", "tags": []},
        "headline": f"## 【{basic.name}】数据不足，无法生成深度报告",
        "section1": "数据不足。",
        "section2": "数据不足。",
        "section3": "数据不足。",
        "section4": "数据不足。",
        "section5": "数据不足。",
        "section6": "数据不足。",
        "full_text": "数据不足，无法生成完整的深度评价报告。",
    }
