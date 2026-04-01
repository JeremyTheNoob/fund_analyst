"""
权益类基金深度评价报告生成器 — fund_quant_v2
报告结构：5板块 + 图表插入点标记
  1. 收益情况
  2. 持仓穿透
  3. 深度分析（Brinson 归因）
  4. 风险预警
  5. 投资建议
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
          "meta":       {fund_name, fund_type, start_date, end_date, tags},
          "headline":   标题行,
          "section1":   一、收益情况（含 [INSERT_CHART: CUM_RET]）,
          "section2":   二、持仓穿透（含 [DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]）,
          "section3":   三、深度分析（含 [INSERT_CHART: BRINSON], [INSERT_CHART: EXCESS_ALPH]）,
          "section4":   四、风险预警（含 [INSERT_CHART: DRAWDOWN]）,
          "section5":   五、投资建议,
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
        "tags": tags,
    }

    # 板块1：收益情况
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

    # 板块4：风险预警
    section4 = _section4_risk_warning(
        fund_name, max_dd_fund, max_dd_bm,
        dd_info, recovery_days, cm,
        beta_val, r2, ann_ret
    )

    # 板块5：投资建议
    section5 = _section5_investment_advice(
        fund_name, tags,
        net_alpha, max_dd_fund, monthly_win_rate,
        ir_value, m, basic
    )

    # 标题行
    headline = _build_headline(fund_name, tags, start_date, end_date)

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
    """一、收益情况"""

    bm_label = "全收益基准" if is_total_return else "基准"
    excess = round(cum_fund - cum_bm_tr, 1)

    # 超额描述
    if excess > 10:
        alpha_desc = f"基金经理的选股能力非常优秀，累计跑赢基准 **{excess:.1f} 个百分点**"
    elif excess > 3:
        alpha_desc = f"基金经理有一定的选股能力，累计跑赢基准 **{excess:.1f} 个百分点**"
    elif excess > 0:
        alpha_desc = f"小幅跑赢基准 {excess:.1f} 个百分点"
    else:
        alpha_desc = f"落后基准 {abs(excess):.1f} 个百分点，选股能力存疑"

    # 分红描述
    if div_contribution > 2.0 and is_total_return:
        div_desc = f"公平起见，我们用全收益基准做对比，把分红再投资也算进去。基准里分红贡献了约 {div_contribution:.1f}%，给基金业绩\"洗了个澡\"。"
    elif is_total_return:
        div_desc = "为了公平比较，我们用全收益基准（含分红再投资）来做对比。"
    else:
        div_desc = ""

    # 年化描述
    if ann_ret > 15:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，算是很高了"
    elif ann_ret > 8:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，还不错"
    elif ann_ret > 0:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，不算高"
    else:
        ann_desc = f"年化收益率 {ann_ret:.1f}%，亏钱了"

    # 风格一句话
    style = _style_desc(smb, hml)

    text = f"""### 一、收益情况

{fund_name}是一只{fund_type}基金，买这种基金就是为了追求更高的收益。我们先来看收益情况。

从 {start_date} 到 {end_date}，累计收益率为 **{cum_fund:.1f}%**，折算成年化收益率为 **{ann_ret:.1f}%**。{ann_desc}。

[INSERT_CHART: CUM_RET]

{div_desc}

在此标准下，基金的真实 Alpha 为 **{net_alpha:.1f}%**，{alpha_desc}。

从风格上看，这只基金偏向{_style_desc(smb, hml)}风格，经理在这方面有持续的投入。"""

    return text


def _section3_brinson_attrition(
    fund_name, brinson, net_alpha,
    smb, hml, r2, ir_value,
    last_excess, curve_trend, excess_std, monthly_win_rate
) -> str:
    """三、深度分析"""

    if not brinson:
        return """### 三、深度分析

Brinson 归因数据暂不可用，当前持仓数据不足以支撑深度分析。"""

    allocation = brinson.get('allocation', 0.0)
    selection = brinson.get('selection', 0.0)
    interaction = brinson.get('interaction', 0.0)
    total = brinson.get('total', 0.0)

    alloc_pct = round(allocation * 100, 2)
    select_pct = round(selection * 100, 2)
    inter_pct = round(interaction * 100, 2)
    total_pct = round(total * 100, 2)

    # 配置效应
    if abs(alloc_pct) > 3:
        alloc_desc = "经理的行业轮动判断{'精准' if alloc_pct > 0 else '不准'}，贡献 **{:+.2f}%**".format(alloc_pct)
    elif abs(alloc_pct) > 1:
        alloc_desc = "行业配置贡献 {:+.2f}%，有一定行业判断力".format(alloc_pct)
    else:
        alloc_desc = "行业配置贡献 {:+.2f}%，基本靠选股吃饭".format(alloc_pct)

    # 选股效应
    if abs(select_pct) > 3:
        select_desc = "选股能力{'突出' if select_pct > 0 else '出现偏差'}，贡献 **{:+.2f}%**".format(select_pct)
    elif abs(select_pct) > 1:
        select_desc = "选股贡献 {:+.2f}%，能挑出好股票".format(select_pct)
    else:
        select_desc = "选股贡献 {:+.2f}%，不太明显".format(select_pct)

    # 能力类型
    if abs(alloc_pct) > abs(select_pct) * 1.5:
        ability = "行业轮动型"
        ability_detail = "超额主要靠行业配置，胜在看得准行业方向。建议关注行业集中度和轮动频率。"
    elif abs(select_pct) > abs(alloc_pct) * 1.5:
        ability = "个股挖掘型"
        ability_detail = "超额主要靠在好行业里挑好公司，选股是核心武器。建议关注换手率和个股集中度。"
    else:
        ability = "均衡型"
        ability_detail = "行业配置和选股能力都不错，超额来源比较分散，抗风格切换的能力更强。"

    # 超额持续性
    if ir_value > 0.5:
        persist = f"信息比率 **{ir_value:.2f}**，超额赚得稳，月度胜率 {monthly_win_rate:.1f}%"
    elif ir_value > 0:
        persist = f"信息比率 {ir_value:.2f}，有超额但不够稳，月度胜率 {monthly_win_rate:.1f}%"
    else:
        persist = f"信息比率 {ir_value:.2f}，超额不太靠谱"

    # R²
    if r2 > 0.7:
        r2_desc = f"R² 为 {r2:.2f}，风格稳定"
    elif r2 > 0.4:
        r2_desc = f"R² 为 {r2:.2f}，有一定独立风格"
    else:
        r2_desc = f"R² 为 {r2:.2f}，风格很独立，不太随大盘走"

    text = f"""### 三、深度分析

下面把超额收益拆开来看，到底靠的是什么本事。

[INSERT_CHART: BRINSON]

| 能力维度 | 贡献 | 说明 |
|---|---|---|
| 行业配置 | {alloc_pct:+.2f}% | {alloc_desc} |
| 个股选择 | {select_pct:+.2f}% | {select_desc} |
| 交互效应 | {inter_pct:+.2f}% | {'配合默契' if inter_pct > 0 else '有点互相拖后腿'} |
| **合计** | **{total_pct:+.2f}%** | |

这只基金经理是 **{ability}** 的选手。{ability_detail}

[INSERT_CHART: EXCESS_ALPH]

从超额收益曲线看，{persist}。{r2_desc}。"""

    return text


def _section4_risk_warning(
    fund_name, max_dd_fund, max_dd_bm,
    dd_info, recovery_days, cm,
    beta_val, r2, ann_ret
) -> str:
    """四、风险预警"""

    # 防御能力
    if max_dd_bm != 0:
        defense_ratio = abs(max_dd_fund / max_dd_bm)
    else:
        defense_ratio = 1.0

    if defense_ratio < 0.8:
        defense_desc = f"回撤 **{abs(max_dd_fund):.1f}%**，比基准的 {abs(max_dd_bm):.1f}% 小不少，防守能力不错"
    elif defense_ratio < 1.0:
        defense_desc = f"回撤 {abs(max_dd_fund):.1f}%，略好于基准的 {abs(max_dd_bm):.1f}%，还行"
    elif defense_ratio < 1.2:
        defense_desc = f"回撤 {abs(max_dd_fund):.1f}%，和基准差不多"
    else:
        defense_desc = f"回撤 {abs(max_dd_fund):.1f}%，比基准的 {abs(max_dd_bm):.1f}% 还大，防守偏弱"

    # 修复弹性
    if recovery_days and recovery_days > 0:
        if recovery_days < 30:
            recovery_desc = f"从最低点回血只用了 **{recovery_days} 个交易日**，恢复力很强"
        elif recovery_days < 90:
            recovery_desc = f"回血用了 {recovery_days} 个交易日，中等节奏"
        elif recovery_days < 180:
            recovery_desc = f"回血用了 {recovery_days} 个交易日，磨了比较久"
        else:
            recovery_desc = f"最长一次回血用了 {recovery_days} 个交易日，一直在水下泡着"
    else:
        recovery_desc = "目前还在回血的路上"

    # 波动率
    vol = round(cm.volatility * 100, 1)
    if vol < 15:
        vol_desc = f"年化波动率 {vol:.1f}%，波动不大"
    elif vol < 25:
        vol_desc = f"年化波动率 {vol:.1f}%，正常水平"
    else:
        vol_desc = f"年化波动率 {vol:.1f}%，波动比较大"

    # 夏普
    sharpe = cm.sharpe_ratio
    sharpe_desc = f"夏普比率 {sharpe:.2f}"

    # Beta
    if beta_val > 1.1:
        beta_desc = f"Beta 为 **{beta_val:.2f}**，涨的时候跑得快，跌的时候也摔得狠"
    elif beta_val < 0.9:
        beta_desc = f"Beta 为 **{beta_val:.2f}**，比大盘稳，防守姿态"
    else:
        beta_desc = f"Beta 为 **{beta_val:.2f}**，跟大盘基本同步"

    # 压力测试
    stress_tests = _stress_test_scenarios(max_dd_fund, beta_val, vol, ann_ret)

    # 回撤原因
    drawdown_cause = _analyze_drawdown_cause(dd_info, max_dd_fund, beta_val)

    text = f"""### 四、风险预警

[INSERT_CHART: DRAWDOWN]

{defense_desc}。{recovery_desc}。{vol_desc}，{sharpe_desc}。{beta_desc}。

**如果遇到极端行情会怎样？**

{stress_tests}

{drawdown_cause}"""

    return text


def _section5_investment_advice(
    fund_name, tags,
    net_alpha, max_dd_fund, monthly_win_rate,
    ir_value, m, basic
) -> str:
    """五、投资建议"""

    tag_str = "、".join([f"「{t}」" for t in tags[:3]]) if tags else "综合型"

    # 经理画像
    portrait = _manager_portrait(net_alpha, abs(max_dd_fund), monthly_win_rate, ir_value, m)

    # 风险点
    risk_point = _identify_risk_point(m, abs(max_dd_fund))

    # 费率
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    text = f"""### 五、投资建议

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
    """构建深度报告标题行（无评分/评级）"""
    tag_str = " · ".join(tags[:3]) if tags else "综合型"
    return (
        f"## {fund_name} — 深度分析报告\n"
        f"分析区间：{start_date} 至 {end_date} ｜ 风格标签：{tag_str}"
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
        if start_date != "—" and end_date != "—":
            from datetime import datetime
            s = datetime.strptime(start_date[:10], "%Y-%m-%d")
            e = datetime.strptime(end_date[:10], "%Y-%m-%d")
            years = (e - s).days / 365.25
        else:
            years = 3.0

        benchmark_text = (basic.benchmark_text or "").lower()
        if any(k in benchmark_text for k in ["银行", "煤炭", "钢铁", "电力"]):
            annual_div = 0.05
        elif any(k in benchmark_text for k in ["医药", "消费", "食品"]):
            annual_div = 0.02
        elif any(k in benchmark_text for k in ["科技", "计算机", "电子"]):
            annual_div = 0.01
        else:
            annual_div = 0.025

        return round(annual_div * years * 100, 1)
    except Exception:
        return 0.0


def _stress_test_scenarios(
    max_dd_fund: float, beta_val: float,
    vol: float, ann_ret: float
) -> str:
    """压力测试：基于历史情景模拟"""
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
        summary = "综合来看，极端行情下预估回撤在 20% 以内，风控能力还可以。"
    elif avg_dd < 35:
        summary = "极端行情下预估回撤在 20%-35%，属于正常水平，注意控制仓位。"
    else:
        summary = "极端行情下预估回撤可能超过 35%，波动很大，只适合风险承受力强的人。"

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
        return "经理在**成长风格**行情里 Alpha 最强，选股爆发力突出；但在价值/红利行情中可能熄火"
    elif hml > 0.3:
        return "经理在**价值风格**行情里最吃香；成长/科技领跑时可能掉队"
    else:
        return "经理风格比较均衡，但遇到极端单一风格行情时，超额空间有限"


def _style_opposite(smb: float, hml: float) -> str:
    """风格对立面"""
    if hml < -0.3:
        return "价值/低估值红利"
    elif hml > 0.3:
        return "成长/科技"
    else:
        return "极端风格"


def _seasonal_analysis(heatmap_info: dict) -> str:
    """分析季节性规律"""
    if not heatmap_info:
        return ""

    monthly_details = heatmap_info.get("monthly_details", {})
    monthly_returns = monthly_details.get("monthly_returns", {})
    if not monthly_returns:
        return ""

    month_avg = {}
    for (year, month), ret in monthly_returns.items():
        if ret is not None and not (isinstance(ret, float) and (ret != ret)):
            if month not in month_avg:
                month_avg[month] = []
            month_avg[month].append(ret)

    if not month_avg:
        return ""

    month_avg_vals = {m: sum(vs)/len(vs)*100 for m, vs in month_avg.items()}
    best_month = max(month_avg_vals.items(), key=lambda x: x[1])
    worst_month = min(month_avg_vals.items(), key=lambda x: x[1])

    month_names = {1:"1月",2:"2月",3:"3月",4:"4月",5:"5月",6:"6月",
                   7:"7月",8:"8月",9:"9月",10:"10月",11:"11月",12:"12月"}

    return (
        f"从月度热力图看，这只基金在{month_names.get(best_month[0], '')}前后比较强"
        f"（均值约 {best_month[1]:.1f}%），"
        f"{month_names.get(worst_month[0], '')}前后偏弱"
        f"（均值约 {worst_month[1]:.1f}%）。"
    )


def _manager_portrait(net_alpha, max_dd_fund, monthly_win_rate, ir_value, m) -> str:
    """生成经理画像"""
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
    """识别核心风险点"""
    risks = []

    if m.style_drift_flag:
        risks.append("**风格漂移**：最近 Beta 偏离了历史均值，经理可能在调仓位，后续风格要盯紧")

    if max_dd_fund > 25:
        risks.append(f"**高波动**：最大回撤 {max_dd_fund:.1f}%，市场暴跌时净值会很难看，注意仓位管理")

    smb = m.factor_loadings.get("SMB", 0)
    if smb > 0.5:
        risks.append("**小盘风险**：重仓小市值股票，极端行情下可能卖不出去")

    hml = m.factor_loadings.get("HML", 0)
    if hml < -0.5:
        risks.append("**成长集中**：高度押注成长股，市场切到价值风格时可能吃土")

    ir = m.information_ratio
    if ir < 0:
        risks.append("**超额不稳**：信息比率为负，超额收益时有时无")

    if not risks:
        risks.append("目前没发现明显的结构性风险，主要注意大盘系统性风险就行")

    return "\n\n".join(risks[:2])


def _section_top_contributors(
    report: Any,
    fund_name: str,
    start_date: str,
    end_date: str
) -> str:
    """Top 持仓分析"""
    if not hasattr(report, 'holdings') or not report.holdings or not report.holdings.top10_stocks:
        return "当前暂无法获取该基金的持仓数据。"

    top10_stocks = report.holdings.top10_stocks
    if not top10_stocks or len(top10_stocks) == 0:
        return "当前暂无法获取该基金的持仓数据。"

    sorted_stocks = sorted(top10_stocks, key=lambda x: x.get('占净值比例', 0), reverse=True)
    top5 = sorted_stocks[:5]

    parts = []
    total_ratio = sum(stock.get('占净值比例', 0) for stock in top5)
    parts.append(f"前五大重仓股合计占 **{total_ratio:.1f}%**，{'比较集中' if total_ratio > 30 else '相对分散'}。")

    for i, stock in enumerate(top5[:3], 1):
        name = stock.get('股票名称', '未知')
        ratio = stock.get('占净值比例', 0)
        code = stock.get('股票代码', '')
        parts.append(f"{i}. **{name}**（{code}）— 占比 **{ratio:.1f}%**")

    if total_ratio > 40:
        parts.append(f"\n**注意：** 前五大持仓超过 40%，基金表现和这几只股票高度绑定，要重点盯它们的基本面。")

    return "\n".join(parts)


def _analyze_drawdown_cause(
    dd_info: dict,
    max_dd_fund: float,
    beta_val: float
) -> str:
    """分析回撤原因"""
    max_dd_date = dd_info.get('max_dd_date') if dd_info else None

    if not max_dd_date:
        return ""

    try:
        from datetime import datetime
        if hasattr(max_dd_date, 'strftime'):
            dd_date = max_dd_date
        else:
            dd_date = datetime.strptime(str(max_dd_date)[:10], '%Y-%m-%d')
        year = dd_date.year
        month = dd_date.month
    except Exception:
        return ""

    # 历史背景
    periods = {
        (2022, 1): "2022年全球央行疯狂加息",
        (2021, 7): "2021年互联网反垄断和教培整顿",
        (2020, 1): "2020年疫情冲击",
        (2018, 1): "2018年中美贸易摩擦",
    }

    bg = ""
    for (y, m), desc in periods.items():
        if year == y and month >= m:
            bg = desc
            break

    if bg:
        cause = f"最大回撤发生在 **{dd_date.strftime('%Y年%m月')}**，幅度 **{abs(max_dd_fund):.1f}%**。当时正值{bg}，属于系统性风险。"
        if beta_val > 1.2:
            cause += "Beta 超过 1.2，跌得比大盘还狠，仓位没及时收缩。"
        elif beta_val < 0.8:
            cause += "Beta 低于 0.8，跌得比大盘少，经理做了减仓操作，不错。"
        else:
            cause += "和大盘跌幅差不多，正常水平。"
    else:
        cause = f"最大回撤发生在 **{dd_date.strftime('%Y年%m月')}**，幅度 **{abs(max_dd_fund):.1f}%**。这段时间没有明显的系统性事件，"
        if abs(max_dd_fund) > 30:
            cause += "可能是重仓股暴雷或行业判断失误。"
        else:
            cause += "属于正常波动范围。"

    return cause


def _fallback_report(basic) -> dict:
    """数据不足时的回退报告"""
    return {
        "meta": {"fund_name": basic.name, "fund_type": "权益类", "tags": []},
        "headline": f"## 【{basic.name}】数据不足，无法生成深度报告",
        "section1": "数据获取不完整，无法进行收益分析。",
        "section2": "持仓数据不足。",
        "section3": "深度分析数据不足。",
        "section4": "风险数据不足。",
        "section5": "投资建议数据不足。",
        "full_text": "数据不足，无法生成完整的深度评价报告。",
    }
