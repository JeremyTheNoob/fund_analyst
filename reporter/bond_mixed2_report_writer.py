"""
债券型-混合二级基金专属深度报告生成器 — fund_quant_v2

混合二级债基特点：债底 + 不超过20%股票 + 可转债增强
分析框架 5 板块：
  ① 资产结构概览 + 股票仓位红线监控
  ② 收益表现 + 债券类型分布 + 股票增强质量分析
  ③ 转债配置策略 + 溢价率/仓位监控
  ④ 股债双杀风险复盘 + 压力测试
  ⑤ 拟购入 / 已持有 投资建议
"""

from __future__ import annotations
from typing import Any, Optional
import logging

from data_loader.rate_prediction import predict_rate_trend

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_bond_mixed2_report(report: Any) -> dict:
    """
    生成混合二级债基 5 板块深度报告。

    Args:
        report: FundReport 对象

    Returns:
        {
          "headline":    标题行,
          "section1":    基本信息与股票仓位监控,
          "section2":    收益表现与股票增强分析,
          "section3":    转债配置策略深度分析,
          "section4":    股债双杀风险复盘,
          "section5":    投资建议,
        }
    """
    basic  = report.basic
    m      = report.bond_metrics
    charts = report.chart_data
    h_data = charts.get("holdings", {})

    if not m:
        return _fallback_report(basic)

    cm = m.common
    fund_name = basic.name
    grade = m.score_grade
    score = m.overall_score

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 核心数据提取
    ann_ret     = round(cm.annualized_return * 100, 2)
    cum_ret     = round(cm.cumulative_return * 100, 1)
    volatility  = round(cm.volatility * 100, 2)
    max_dd      = round(cm.max_drawdown * 100, 2)
    sharpe      = round(cm.sharpe_ratio, 2)
    calmar      = round(cm.calmar_ratio, 2)
    monthly_wr  = round(cm.monthly_win_rate * 100, 1)
    duration    = round(m.duration, 1)
    wacs        = round(m.wacs_score, 0)
    alpha_bond  = round(m.alpha_bond * 100, 2)

    # 资产配置数据
    stock_ratio = h_data.get("stock_ratio", 0) or 0
    bond_ratio  = h_data.get("bond_ratio", 0) or 0
    cash_ratio  = h_data.get("cash_ratio", 0) or 0
    cb_ratio    = h_data.get("cb_ratio", 0) or 0
    top10_stocks = h_data.get("top10_stocks", []) or []
    bond_details = h_data.get("bond_details", []) or []
    historical_allocation = h_data.get("historical_allocation", []) or []

    # 转债持仓数据
    cb_holdings_df = charts.get("cb_holdings_df")

    # 基准数据
    bm_info    = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm     = round(bm_info.get("bm_last_return", 0) * 100, 1)
    ann_bm     = round(bm_info.get("bm_annual_return", 0) * 100, 2)
    excess_bps = round((ann_ret - ann_bm) * 100, 0)

    # 回撤信息
    dd_info       = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm     = round(dd_info.get("bm_max_dd", 0), 2)
    defensive_ratio = dd_info.get("defensive_ratio", 1.0)
    recovery_info = dd_info.get("recovery_info", {})
    recovery_days = recovery_info.get("recovery_days", cm.recovery_days or 0) or (cm.recovery_days or 0)
    dd_date_str   = _format_date(recovery_info.get("max_dd_date"))

    # 月度统计
    hm_info       = charts.get("monthly_heatmap", {}).get("heatmap_info", {})
    monthly_stats = hm_info.get("monthly_stats", {})
    total_months  = monthly_stats.get("total_months", 0)
    pos_months    = monthly_stats.get("positive_months", 0)

    # 债券分类
    bond_classification = getattr(m, 'bond_classification', {}) or {}

    # 利率预测
    rate_prediction = _get_rate_prediction()

    # ── 生成各板块 ────────────────────────────────────────
    headline = _build_headline(
        fund_name, stock_ratio, cb_ratio, grade, start_date, end_date
    )

    section1 = _section1_basic_info(
        fund_name, stock_ratio, bond_ratio, cash_ratio, cb_ratio,
        top10_stocks, historical_allocation, basic,
    )

    section2 = _section2_return_analysis(
        fund_name, ann_ret, ann_bm, excess_bps, cum_ret, cum_bm,
        sharpe, calmar, volatility, max_dd, monthly_wr,
        total_months, pos_months, bond_classification,
        top10_stocks, stock_ratio, start_date, end_date,
    )

    section3 = _section3_cb_strategy(
        fund_name, cb_ratio, cb_holdings_df, bond_details,
        duration, wacs, m.stress_results,
    )

    section4 = _section4_risk_review(
        fund_name, max_dd, dd_date_str, recovery_days,
        defensive_ratio, max_dd_bm, stock_ratio, cb_ratio,
        duration, wacs, rate_prediction, m.stress_results,
    )

    section5 = _section5_advice(
        fund_name, grade, ann_ret, max_dd, sharpe, calmar,
        stock_ratio, cb_ratio, duration, wacs, rate_prediction,
        basic, m.stress_results,
    )

    return {
        "headline":  headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "section5": section5,
    }


# ============================================================
# 板块 1：资产结构概览
# ============================================================

def _section1_basic_info(
    fund_name: str,
    stock_ratio: float,
    bond_ratio: float,
    cash_ratio: float,
    cb_ratio: float,
    top10_stocks: list,
    historical_allocation: list,
    basic: Any,
) -> str:
    """板块 1：资产结构概览 + 股票仓位红线监控"""

    stock_pct = stock_ratio * 100
    bond_pct  = bond_ratio * 100
    cash_pct  = cash_ratio * 100
    cb_pct    = cb_ratio * 100
    other_pct = max(100 - stock_pct - bond_pct - cash_pct, 0)

    # 股票仓位红线监控
    red_line = 20.0  # 混合二级债基股票上限 20%
    if stock_pct > red_line:
        red_status = f"**已触及 {red_line:.0f}% 红线**（当前 {stock_pct:.1f}%）"
        red_detail = (
            "股票仓位已超过混合二级债基的法定上限，可能是季报数据更新滞后，"
            "也可能是基金大额赎回导致被动超标。需密切关注下期季报是否回归合规区间。"
        )
    elif stock_pct > red_line * 0.8:
        red_status = f"**接近红线**（当前 {stock_pct:.1f}% / 上限 {red_line:.0f}%）"
        red_detail = (
            f"股票仓位已接近 {red_line:.0f}% 上限，后续加仓空间有限。"
            "若看好后市，可能需要先减仓再换股，交易成本增加。"
        )
    elif stock_pct > 10:
        red_status = f"**适中偏积极**（当前 {stock_pct:.1f}%）"
        red_detail = (
            "股票仓位处于中等偏高水平，在获取权益增强收益的同时，"
            "也为后续加仓留有约 {:.0f}% 的操作空间。".format(red_line - stock_pct)
        )
    elif stock_pct > 5:
        red_status = f"**稳健配置**（当前 {stock_pct:.1f}%）"
        red_detail = (
            "股票仓位适中，以债券为核心收益来源，股票作为适度增强。"
            f"仍有约 {red_line - stock_pct:.0f}% 的加仓空间可用。"
        )
    else:
        red_status = f"**保守配置**（当前 {stock_pct:.1f}%）"
        red_detail = (
            "股票仓位偏低，增强力度有限，收益弹性较小。"
            "但这也意味着在股市下跌时的防护能力更强。"
        )

    # 股票持仓 Top5
    stock_detail = ""
    if top10_stocks:
        stock_detail = "\n\n**当前股票持仓 Top5：**\n\n"
        for i, s in enumerate(top10_stocks[:5]):
            sname = s.get("股票名称", s.get("name", "未知"))
            sratio = float(s.get("占净值比例", 0) or 0)
            if sratio > 1.5:
                sratio = sratio  # 百分比格式直接显示
            stock_detail += f"- **{sname}**：{sratio:.2f}%\n"
    else:
        stock_detail = "\n\n> 当前未获取到股票持仓明细数据。\n"

    # 历史仓位变化
    history_text = ""
    if len(historical_allocation) >= 2:
        latest = historical_allocation[-1]
        prev   = historical_allocation[-2] if len(historical_allocation) >= 2 else None
        history_text = "\n\n**历史资产配置变化：**\n\n"
        for q in historical_allocation[-4:]:
            qdate  = q.get("date", "")
            q_stock = q.get("stock_ratio", 0) * 100
            q_bond  = q.get("bond_ratio", 0) * 100
            q_cash  = q.get("cash_ratio", 0) * 100
            history_text += f"| {qdate} | 股票 {q_stock:.1f}% | 债券 {q_bond:.1f}% | 现金 {q_cash:.1f}% |\n"
            history_text += "| --- | --- | --- | --- |\n"
        history_text = history_text.replace(
            history_text.split("\n")[-2],
            ""
        ).rstrip("\n")

        # 加减仓趋势
        if prev and latest:
            delta_stock = (latest.get("stock_ratio", 0) - prev.get("stock_ratio", 0)) * 100
            if abs(delta_stock) > 1:
                direction = "加仓" if delta_stock > 0 else "减仓"
                history_text += (
                    f"\n\n最近两个季度股票仓位**{direction}** "
                    f"约 {abs(delta_stock):.1f}%，"
                )
                if delta_stock > 0:
                    history_text += "经理可能对后市持乐观态度。"
                else:
                    history_text += "经理可能在降低权益暴露以控制风险。"

    # 基金规模
    scale_text = ""
    if hasattr(basic, 'scale') and basic.scale:
        scale_text = f"**基金规模：** {basic.scale}\n\n"

    return f"""### 资产结构概览

混合二级债基的策略很简单：**债打底、股增强**。
债券是基本盘，股票仓位不超过 **{red_line:.0f}%** 用来赚超额收益。

[INSERT_CHART: ASSET_ALLOCATION_PIE]

**股票仓位红线监控：** {red_status}

{red_detail}
{scale_text}{stock_detail}{history_text}"""


# ============================================================
# 板块 2：收益表现
# ============================================================

def _section2_return_analysis(
    fund_name: str,
    ann_ret: float,
    ann_bm: float,
    excess_bps: float,
    cum_ret: float,
    cum_bm: float,
    sharpe: float,
    calmar: float,
    volatility: float,
    max_dd: float,
    monthly_wr: float,
    total_months: int,
    pos_months: int,
    bond_classification: dict,
    top10_stocks: list,
    stock_ratio: float,
    start_date: str,
    end_date: str,
) -> str:
    """板块 2：收益展示 + 债券类型分布 + 股票增强质量"""

    # 收益表现
    excess_sign = "跑赢" if excess_bps > 0 else "落后于"
    bps_abs = abs(int(excess_bps))

    # 夏普评价
    if sharpe >= 2.0:
        sharpe_desc = f"夏普比率 **{sharpe:.2f}**，风险收益效率**极佳**"
    elif sharpe >= 1.0:
        sharpe_desc = f"夏普比率 **{sharpe:.2f}**，风险收益效率**良好**"
    elif sharpe >= 0.5:
        sharpe_desc = f"夏普比率 **{sharpe:.2f}**，风险收益效率**一般**"
    else:
        sharpe_desc = f"夏普比率 **{sharpe:.2f}**，风险收益效率**偏低**"

    # 债券持仓分析
    bond_analysis = _analyze_bond_structure(bond_classification)

    # 股票增强质量分析
    stock_enhance = _analyze_stock_enhancement(
        top10_stocks, stock_ratio, ann_ret, ann_bm, excess_bps
    )

    return f"""### 收益表现与增强质量分析

**收益数据总览：**

| 指标 | 数值 | 评价 |
| --- | --- | --- |
| 年化收益 | {ann_ret:.2f}% | {excess_sign}基准 {bps_abs} bps |
| 累计收益 | {cum_ret:.1f}% | 基准 {cum_bm:.1f}% |
| 最大回撤 | {max_dd:.2f}% | {'优秀' if abs(max_dd) < 1 else ('良好' if abs(max_dd) < 3 else '一般')} |
| 波动率 | {volatility:.2f}% | {'低波动' if volatility < 1.5 else '中等波动' if volatility < 3 else '高波动'} |
| 月度胜率 | {monthly_wr:.1f}% | {total_months} 个月中 {pos_months} 个月正收益 |
| {sharpe_desc} | | |

[INSERT_CHART: CUM_RET]

**债券持仓结构分析：**

{bond_analysis}

**股票增强质量分析：**

{stock_enhance}

[INSERT_CHART: HEATMAP]"""


# ============================================================
# 板块 3：转债配置策略
# ============================================================

def _section3_cb_strategy(
    fund_name: str,
    cb_ratio: float,
    cb_holdings_df: Any,
    bond_details: list,
    duration: float,
    wacs: float,
    stress_results: list,
) -> str:
    """板块 3：转债配置策略 + 溢价率/仓位监控"""

    cb_pct = cb_ratio * 100

    if cb_pct < 1 and (not bond_details or not any(
        "转债" in str(b.get("债券名称", "")) for b in bond_details
    )):
        return f"""### 转债配置策略分析

**转债仓位：{cb_pct:.1f}%** —— 当前未配置可转债。

该基金以纯债 + 股票为主要策略，没有参与转债市场。
好处是收益结构简单、不受转债估值波动影响；
代价是放弃了转债「**下跌有底、上涨不封顶**」的非对称收益特征。
"""

    # 从 bond_details 中提取转债明细
    cb_bonds = [
        b for b in bond_details
        if "转债" in str(b.get("债券名称", ""))
    ]

    # 转债仓位评价
    if cb_pct >= 15:
        cb_level = "较高"
        cb_level_desc = f"转债仓位 {cb_pct:.1f}%，在组合中扮演重要角色，对净值弹性影响显著。"
    elif cb_pct >= 5:
        cb_level = "中等"
        cb_level_desc = f"转债仓位 {cb_pct:.1f}%，适度参与转债增强，兼顾攻守。"
    else:
        cb_level = "较低"
        cb_level_desc = f"转债仓位 {cb_pct:.1f}%，少量转债做增强，对整体影响有限。"

    # 转债持仓详情
    cb_detail_text = ""
    if cb_bonds:
        cb_detail_text = "**当前转债持仓明细：**\n\n"
        cb_detail_text += "| 债券名称 | 占净值比 | 溢价率 |\n"
        cb_detail_text += "| --- | --- | --- |\n"

        # 如果有 cb_holdings_df（含溢价率等详情），使用它
        if cb_holdings_df is not None and not cb_holdings_df.empty:
            for _, row in cb_holdings_df.head(10).iterrows():
                cb_name = row.get("债券名称", "未知")
                cb_r = float(row.get("占净值比例", 0) or 0)
                if cb_r > 1.5:
                    cb_r = cb_r  # 百分比格式
                premium = row.get("premium_ratio")
                prem_str = f"{premium:.1f}%" if premium is not None and not (isinstance(premium, float) and __import__('math').isnan(premium)) else "N/A"
                cb_detail_text += f"| {cb_name} | {cb_r:.2f}% | {prem_str} |\n"

            # 计算平均溢价率
            premiums = []
            for _, row in cb_holdings_df.iterrows():
                pr = row.get("premium_ratio")
                if pr is not None and not (isinstance(pr, float) and __import__('math').isnan(pr)):
                    premiums.append(float(pr))
            if premiums:
                avg_prem = __import__('numpy').mean(premiums)
                cb_detail_text += f"\n**平均转股溢价率：{avg_prem:.1f}%**\n"

                # 溢价率评价
                if avg_prem <= 15:
                    prem_eval = "**偏低（低估区间）**——转债定价便宜，下行空间有限，性价比高。"
                elif avg_prem <= 25:
                    prem_eval = "**中等**——转债定价合理，攻守较为均衡。"
                elif avg_prem <= 40:
                    prem_eval = "**偏高**——需警惕正股回调时的估值压缩风险。"
                else:
                    prem_eval = "**过高**——转债期权价值偏贵，下跌风险较大，建议关注减仓时机。"

                cb_detail_text += f"\n**溢价率评价：** {prem_eval}\n"
        else:
            # 仅从 bond_details 提取
            for b in cb_bonds[:5]:
                bname = str(b.get("债券名称", "未知"))
                br = float(b.get("占净值比例", 0) or 0)
                cb_detail_text += f"| {bname} | {br:.2f}% | 暂无 |\n"
            cb_detail_text += "\n> 转债估值详情正在加载，刷新后可查看溢价率等数据。\n"
    else:
        cb_detail_text = "当前未获取到转债持仓明细。\n"

    # 转债策略评价
    strategy_eval = ""
    if cb_pct >= 10:
        strategy_eval = (
            f"\n\n**转债策略综合评价：**\n\n"
            f"该基金转债仓位 {cb_level.lower()}，{cb_level_desc}\n"
            f"久期约 {duration:.1f} 年，信用质量 WACS {int(wacs)} 分。"
        )
        if stress_results:
            worst = min(stress_results, key=lambda x: x.get("price_impact", 0))
            strategy_eval += (
                f"\n\n在「{worst['scenario']}」压力情景下，"
                f"预计债券+转债组合净值影响约 **{worst['price_impact']:.2f}%**。"
            )
    else:
        strategy_eval = (
            f"\n\n**转债策略综合评价：**\n\n"
            f"该基金以债券和股票为主要配置，转债仓位较低（{cb_pct:.1f}%），"
            f"转债对组合整体风险收益的影响有限。"
        )

    return f"""### 转债配置策略分析

混合二级债基除了直接买股票，还经常通过**可转债**实现"股债双击"：
正股涨时跟着涨，正股跌时有债底托着。

**转债仓位：{cb_pct:.1f}%（{cb_level}）**

{cb_level_desc}

[INSERT_CHART: CB_PRICE_PREMIUM]

{cb_detail_text}{strategy_eval}"""


# ============================================================
# 板块 4：股债双杀风险复盘
# ============================================================

def _section4_risk_review(
    fund_name: str,
    max_dd: float,
    dd_date_str: str,
    recovery_days: int,
    defensive_ratio: float,
    max_dd_bm: float,
    stock_ratio: float,
    cb_ratio: float,
    duration: float,
    wacs: float,
    rate_prediction: dict,
    stress_results: list,
) -> str:
    """板块 4：股债双杀复盘 + 压力测试 + 风险预警"""

    fund_dd_abs = abs(max_dd)
    bm_dd_abs   = abs(max_dd_bm)

    # 基准对比
    if bm_dd_abs > 0:
        vs_bm = f"同期纯债基准回撤约 {bm_dd_abs:.2f}%，该基金为基准的 {defensive_ratio:.0%}"
    else:
        vs_bm = "该基金回撤表现"

    # 修复评价
    if recovery_days > 0:
        if recovery_days <= 30:
            recovery_text = f"**{recovery_days} 个交易日**完成修复，回血速度**极快**"
        elif recovery_days <= 60:
            recovery_text = f"**{recovery_days} 个交易日**完成修复，回血速度**较快**"
        elif recovery_days <= 120:
            recovery_text = f"约 **{recovery_days // 22} 个月**完成修复，回血速度**一般**"
        else:
            recovery_text = f"超过 **{recovery_days // 22} 个月**，回血速度**偏慢**"
    else:
        recovery_text = "统计区间内尚未完全修复至前高"

    # 股债双杀风险评估
    equity_risk = stock_ratio + cb_ratio * 0.4  # 加权权益暴露

    if equity_risk > 0.15:
        dual_kill_risk = "**较高**"
        dual_kill_desc = (
            f"综合权益暴露约 {equity_risk*100:.1f}%（股票 {stock_ratio*100:.1f}% + 转债Delta加权 {cb_ratio*100:.1f}%×0.4），"
            "在股债双杀环境下回撤可能显著加大。"
        )
    elif equity_risk > 0.08:
        dual_kill_risk = "**中等**"
        dual_kill_desc = (
            f"综合权益暴露约 {equity_risk*100:.1f}%，适度暴露权益风险。"
            "在股债双杀环境下会有一定回撤，但债券底仓可以缓冲。"
        )
    else:
        dual_kill_risk = "**较低**"
        dual_kill_desc = (
            f"综合权益暴露仅 {equity_risk*100:.1f}%，权益敞口极小。"
            "即使在股债双杀环境下，回撤幅度也相对可控。"
        )

    # 压力测试展示
    stress_text = ""
    if stress_results:
        stress_text = "\n\n**压力测试结果（利率+信用冲击）：**\n\n"
        stress_text += "| 情景 | 10Y利率(bp) | 信用利差(bp) | 预计净值影响 |\n"
        stress_text += "| --- | --- | --- | --- |\n"
        for s in stress_results:
            impact = s.get("price_impact", 0)
            stress_text += (
                f"| {s['scenario']} | {s.get('long_bp', 0):+.0f} | "
                f"{s.get('credit_bp', 0):+.0f} | {impact:.2f}% |\n"
            )

        # 股票 + 转债的额外冲击估算
        extra_dd = equity_risk * 0.25  # 假设权益部分跌 25%
        stress_text += (
            f"\n> **注：** 以上压力测试仅覆盖利率和信用利差冲击。"
            f"若叠加股市下跌 25%，"
            f"综合权益暴露（{equity_risk*100:.1f}%）将带来额外约 **{extra_dd*100:.2f}%** 的净值损失。"
            f"极端情景下总回撤可能达到 **{abs(max_dd)*100 + extra_dd*100:.1f}%** 左右。"
        )

    # 利率环境研判
    rate_text = ""
    direction = rate_prediction.get("direction", "sideways")
    confidence = rate_prediction.get("confidence", 0.3)
    forecast = rate_prediction.get("y10y_forecast", {})
    current_rate = forecast.get("current", 0)
    mid_rate = forecast.get("mid_term", 0)

    if confidence >= 0.5:
        dir_map = {"up": "上行", "down": "下行", "sideways": "震荡"}
        dir_cn = dir_map.get(direction, "震荡")
        rate_text = (
            f"\n\n**利率环境研判：**\n\n"
            f"技术指标模型预测10Y国债收益率从 **{current_rate:.2f}%** "
            f"在未来3个月{dir_cn}至 **{mid_rate:.2f}%**"
            f"（置信度 {int(confidence*100)}%）。\n\n"
        )

        if direction == "up" and duration >= 3:
            rate_text += (
                f"**风险提示：** 该基金久期约 {duration:.1f} 年，"
                f"利率上行阶段债券部分可能承压。叠加股票回撤，需警惕股债双杀。"
            )
        elif direction == "down":
            rate_text += (
                f"利好：利率下行有利于债券价格回升，转债的期权价值也会提升。"
            )
        else:
            rate_text += "当前利率环境对基金表现影响中性。"

        # 风险因素
        factors = rate_prediction.get("key_factors", [])
        if factors:
            rate_text += "\n\n**关键判断依据：**\n" + "\n".join(f"- {f}" for f in factors[:3])
    else:
        rate_text = (
            "\n\n**利率环境研判：**\n\n"
            "当前利率预测置信度不足，建议结合政策面和宏观数据综合判断。"
        )

    # 风险信号
    risks = rate_prediction.get("risk_signals", [])
    risk_signal_text = ""
    if risks:
        risk_signal_text = (
            "\n\n**需警惕的风险信号：**\n"
            + "\n".join(f"- {r}" for r in risks[:3])
        )

    # 综合风险等级
    if fund_dd_abs < 1 and equity_risk < 0.10:
        overall_risk = "**低风险**"
    elif fund_dd_abs < 2 and equity_risk < 0.15:
        overall_risk = "**中低风险**"
    elif fund_dd_abs < 4 and equity_risk < 0.20:
        overall_risk = "**中等风险**"
    else:
        overall_risk = "**中高风险**"

    return f"""### 股债双杀风险复盘与预警

混合二级债基同时持有债券、转债和股票，遇到**股债双杀**（如2022年11月理财赎回潮）时，
可能面临"债券端因利率上行跌 + 股票端因市场下跌亏"的双重打击。

**历史回撤表现：**

| 指标 | 数值 | 评价 |
| --- | --- | --- |
| 最大回撤 | {fund_dd_abs:.2f}% | {vs_bm} |
| 回撤发生 | {dd_date_str or '未知'} | |
| 修复耗时 | {recovery_text} | |
| 久期 | {duration:.1f} 年 | {'敏感度低' if duration < 2 else '敏感度中等' if duration < 4 else '敏感度高'} |
| WACS信用 | {int(wacs)} 分 | {'高等级' if wacs >= 75 else '中等' if wacs >= 55 else '偏低'} |

[INSERT_CHART: DRAWDOWN]

**股债双杀风险评估：{dual_kill_risk}**

{dual_kill_desc}
{stress_text}{rate_text}{risk_signal_text}

**综合风险等级：{overall_risk}**"""


# ============================================================
# 板块 5：投资建议
# ============================================================

def _section5_advice(
    fund_name: str,
    grade: str,
    ann_ret: float,
    max_dd: float,
    sharpe: float,
    calmar: float,
    stock_ratio: float,
    cb_ratio: float,
    duration: float,
    wacs: float,
    rate_prediction: dict,
    basic: Any,
    stress_results: list,
) -> str:
    """板块 5：拟购入 / 已持有 投资建议"""

    fund_dd_abs = abs(max_dd)
    equity_risk = stock_ratio + cb_ratio * 0.4

    # ── 拟购入评估 ──
    buy_checks = []

    # 收益维度
    if ann_ret >= 5:
        buy_checks.append(("OK", f"年化收益 {ann_ret:.2f}%，收益水平优良"))
    elif ann_ret >= 3:
        buy_checks.append(("OK", f"年化收益 {ann_ret:.2f}%，收益水平适中"))
    elif ann_ret >= 1:
        buy_checks.append(("WARN", f"年化收益 {ann_ret:.2f}%，收益水平偏低"))
    else:
        buy_checks.append(("BAD", f"年化收益 {ann_ret:.2f}%，收益不达预期"))

    # 回撤维度
    if fund_dd_abs < 1:
        buy_checks.append(("OK", f"最大回撤 {fund_dd_abs:.2f}%，回撤控制极优"))
    elif fund_dd_abs < 2.5:
        buy_checks.append(("OK", f"最大回撤 {fund_dd_abs:.2f}%，回撤控制良好"))
    elif fund_dd_abs < 5:
        buy_checks.append(("WARN", f"最大回撤 {fund_dd_abs:.2f}%，回撤中等"))
    else:
        buy_checks.append(("BAD", f"最大回撤 {fund_dd_abs:.2f}%，回撤较大"))

    # 夏普维度
    if sharpe >= 1.5:
        buy_checks.append(("OK", f"夏普比率 {sharpe:.2f}，风险收益效率极佳"))
    elif sharpe >= 0.8:
        buy_checks.append(("OK", f"夏普比率 {sharpe:.2f}，风险收益效率良好"))
    elif sharpe >= 0.3:
        buy_checks.append(("WARN", f"夏普比率 {sharpe:.2f}，风险收益效率一般"))
    else:
        buy_checks.append(("BAD", f"夏普比率 {sharpe:.2f}，风险收益效率偏低"))

    # 信用质量
    if wacs >= 75:
        buy_checks.append(("OK", f"WACS信用评分 {int(wacs)}，持仓信用资质优良"))
    elif wacs >= 55:
        buy_checks.append(("WARN", f"WACS信用评分 {int(wacs)}，信用资质中等"))
    else:
        buy_checks.append(("WARN", f"WACS信用评分 {int(wacs)}，信用资质偏低需关注"))

    # 仓位合规性
    if stock_ratio <= 0.20:
        buy_checks.append(("OK", f"股票仓位 {stock_ratio*100:.1f}%，合规范围内"))
    else:
        buy_checks.append(("WARN", f"股票仓位 {stock_ratio*100:.1f}%，触及20%红线需关注"))

    green_count = sum(1 for s, _ in buy_checks if s == "OK")
    yellow_count = sum(1 for s, _ in buy_checks if s == "WARN")

    if green_count >= 4 and yellow_count == 0:
        buy_verdict = "**强烈推荐购入** — 各项指标优秀，适合作为固收增强的核心配置"
    elif green_count >= 3 and yellow_count <= 1:
        buy_verdict = "**推荐购入** — 整体表现良好，可作为固收组合的增强配置"
    elif green_count >= 2:
        buy_verdict = "**可考虑购入** — 部分指标存在瑕疵，建议分批建仓"
    else:
        buy_verdict = "**建议观望** — 多项指标存在风险信号，建议等待更好的时机"

    # ── 已持有诊断 ──
    hold_checks = []

    # 收益趋势
    if ann_ret >= 3:
        hold_checks.append("收益表现正常，继续持有")
    elif ann_ret >= 0:
        hold_checks.append("收益偏低但仍有正收益，可继续观察")
    else:
        hold_checks.append("收益为负，建议关注原因")

    # 回撤状态
    if fund_dd_abs < 2:
        hold_checks.append("回撤可控，持有体验良好")
    elif fund_dd_abs < 5:
        hold_checks.append("回撤幅度可接受，关注修复速度")
    else:
        hold_checks.append("回撤较大，评估是否需要止损或减仓")

    # 压力测试
    if stress_results:
        worst = min(stress_results, key=lambda x: x.get("price_impact", 0))
        worst_impact = abs(worst.get("price_impact", 0))
        if worst_impact < 2:
            hold_checks.append("压力测试通过，抗冲击能力良好")
        elif worst_impact < 5:
            hold_checks.append("压力测试中等，极端情况下可能亏损较大")
        else:
            hold_checks.append("压力测试较差，极端冲击下损失较大")

    # 仓位管理
    if equity_risk < 0.15:
        hold_checks.append("权益敞口适中，不需要主动调整")
    elif equity_risk < 0.20:
        hold_checks.append("权益敞口偏高，若市场回调需关注补仓节奏")
    else:
        hold_checks.append("权益敞口较高，建议密切关注市场动态")

    # ── 离场信号 ──
    exit_signals = []

    if stock_ratio > 0.20:
        exit_signals.append(f"股票仓位超标（{stock_ratio*100:.1f}% > 20%）")
    if fund_dd_abs > 5:
        exit_signals.append(f"最大回撤超过 5%（当前 {fund_dd_abs:.2f}%）")
    if wacs < 40:
        exit_signals.append(f"WACS信用评分偏低（{int(wacs)}分），信用下沉风险增加")
    if ann_ret < -1:
        exit_signals.append(f"近一年收益为负（{ann_ret:.2f}%）")
    if calmar < 0:
        exit_signals.append("卡玛比率为负，风险收益严重失衡")

    if not exit_signals:
        exit_text = "当前未检测到明显离场信号，可安心持有。"
    else:
        exit_text = "检测到以下风险信号，需密切关注：\n\n" + "\n".join(
            f"- **{s}**" for s in exit_signals
        )
        exit_text += "\n\n> 出现 **2 个及以上** 信号时，建议考虑减仓或离场。"

    # ── 组装建议 ──
    buy_section = " | ".join(f"{t}" for _, t in buy_checks)
    hold_section = "\n".join(f"- {t}" for t in hold_checks)

    # 成本披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0

    return f"""### 投资建议

#### 拟购入评估

{buy_verdict}

**诊断清单：**

{buy_section}

**配置建议：**
- 混合二级债基适合作为**固收增强**配置，建议占固收组合的 **20%~40%**
- 若风险偏好较低，可适当降低比例至 **10%~20%**
- 建议在**股债利差收窄、转债估值较低**时择机建仓

#### 已持有诊断

{hold_section}

#### 离场信号监测

{exit_text}

---

**成本项披露：**

- 管理费率：{mgmt_fee:.2f}%
- 托管费率：{custody_fee:.2f}%
- 综合评级：{grade}

> **风险提示：** 以上分析基于历史数据，不构成投资建议。混合二级债基的股票和转债部分会增加净值波动，投资者应根据自身风险承受能力做出决策。
"""


# ============================================================
# 辅助函数
# ============================================================

def _build_headline(
    fund_name: str,
    stock_ratio: float,
    cb_ratio: float,
    grade: str,
    start_date: str,
    end_date: str,
) -> str:
    """报告标题行"""
    stock_pct = stock_ratio * 100
    cb_pct = cb_ratio * 100
    return (
        f"## {fund_name} — 混合二级债基分析\n\n"
        f"**统计区间：** {start_date} ~ {end_date}\n\n"
        f"**核心标签：** 债底保护 | 股票增强({stock_pct:.1f}%) | "
        f"转债配置({cb_pct:.1f}%) | 攻守兼备\n\n"
    )


def _analyze_bond_structure(bond_classification: dict) -> str:
    """分析债券持仓结构"""
    if not bond_classification:
        return "债券持仓数据正在加载中，请稍后刷新查看。\n\n[INSERT_CHART: BOND_HOLDINGS_PIE]"

    gov_ratio    = bond_classification.get('gov_bond', {}).get('ratio', 0) * 100
    credit_ratio = bond_classification.get('credit_bond', {}).get('ratio', 0) * 100
    urban_ratio  = bond_classification.get('urban_construction', {}).get('ratio', 0) * 100
    estate_ratio = bond_classification.get('real_estate', {}).get('ratio', 0) * 100

    # 转债在 bond_details 中但不一定被分类识别
    text = (
        f"[INSERT_CHART: BOND_HOLDINGS_PIE]\n\n"
        f"| 券种 | 占比 | 说明 |\n"
        f"| --- | --- | --- |\n"
        f"| 利率债（国债/政金债） | {gov_ratio:.1f}% | 风险最低的压舱石 |\n"
        f"| 信用债（企业债/公司债） | {credit_ratio:.1f}% | 收益增强来源 |\n"
        f"| 城投债 | {urban_ratio:.1f}% | 区域性债务 |\n"
        f"| 地产债 | {estate_ratio:.1f}% | 周期性较强 |\n"
    )

    # 结构评价
    if gov_ratio > 60:
        text += f"\n\n**结构评价：** 利率债占比超六成（{gov_ratio:.1f}%），防守极强，信用风险暴露有限。收益主要来自票息和久期管理。"
    elif gov_ratio > 30:
        text += f"\n\n**结构评价：** 利率债占比三成以上（{gov_ratio:.1f}%），有一定防御性，信用债（{credit_ratio:.1f}%）提供收益增强。"
    else:
        text += f"\n\n**结构评价：** 利率债占比较低（{gov_ratio:.1f}%），主要通过信用下沉获取超额收益，信用风险暴露较高。"

    return text


def _analyze_stock_enhancement(
    top10_stocks: list,
    stock_ratio: float,
    ann_ret: float,
    ann_bm: float,
    excess_bps: float,
) -> str:
    """分析股票增强质量"""

    if not top10_stocks or stock_ratio < 0.01:
        return "当前股票仓位极低或无持仓数据，股票增强作用有限。"

    stock_pct = stock_ratio * 100

    # 行业分散度
    industries = set()
    for s in top10_stocks:
        # 从名称推断行业
        name = str(s.get("股票名称", ""))
        if "银" in name or "银行" in name:
            industries.add("银行")
        elif "券" in name:
            industries.add("券商")
        elif "保" in name:
            industries.add("保险")
        elif "酒" in name or "食品" in name:
            industries.add("消费")
        elif "新能" in name or "锂" in name or "电" in name:
            industries.add("新能源")
        elif "医" in name or "药" in name:
            industries.add("医药")
        elif "房" in name or "地" in name:
            industries.add("地产")
        else:
            industries.add("其他")

    diversification = f"涉及 **{len(industries)}** 个行业（{', '.join(list(industries)[:5])}）"

    # 增强效果评价
    if excess_bps > 200:
        enhance_eval = (
            f"股票增强效果**显著**，年化超额 {int(excess_bps)} bps。"
            f"基金经理的选股能力在同类中处于**领先水平**。"
        )
    elif excess_bps > 50:
        enhance_eval = (
            f"股票增强效果**良好**，年化超额 {int(excess_bps)} bps。"
            f"选股能力为组合贡献了稳定的正收益增量。"
        )
    elif excess_bps > 0:
        enhance_eval = (
            f"股票增强效果**有限**，年化超额仅 {int(excess_bps)} bps。"
            f"超额收益主要来自债券端，股票贡献较小。"
        )
    else:
        enhance_eval = (
            f"股票增强**拖累**了组合收益（超额 {int(excess_bps)} bps）。"
            f"需关注选股策略的有效性。"
        )

    return (
        f"**股票仓位 {stock_pct:.1f}%，{diversification}**\n\n"
        f"混合二级债基的股票投资通常有以下几种策略：\n"
        f"- **打新策略**：通过参与 IPO 打新获取低风险收益\n"
        f"- **价值选股**：精选低估值、高股息的蓝筹股获取稳健收益\n"
        f"- **行业轮动**：根据市场周期切换行业配置\n\n"
        f"**增强效果评价：** {enhance_eval}\n"
    )


def _get_rate_prediction() -> dict:
    """获取利率预测"""
    try:
        return predict_rate_trend(horizon="3m")
    except Exception as e:
        logger.warning(f"[bond_mixed2_report_writer] 获取利率预测失败: {e}")
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
        end   = _format_date(x_list[-1])
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


def _fallback_report(basic) -> dict:
    """数据不足时的兜底"""
    name = getattr(basic, "name", "该基金")
    text = (
        f"## {name} — 数据不足，无法生成深度报告\n\n"
        f"当前统计周期内有效数据不足，请尝试延长分析周期或稍后重试。"
    )
    return {
        "headline": text,
        "section1": "",
        "section2": "",
        "section3": "",
        "section4": "",
        "section5": "",
    }
