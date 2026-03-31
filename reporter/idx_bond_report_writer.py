"""
指数型-固收基金 深度评价报告生成器 — fund_quant_v2
角色：固收指数分析师（债券指数策略师）
报告结构：5板块 + 10年国债技术分析专题
  1. 基本信息（跟踪精度模型 · 日偏离度分布 · 抽样复制偏离）
  2. 收益表现（收益对比 · 费用侵蚀模型 · 票息覆盖率）
  3. 深度分析（久期对照 · 信用等级对齐 · 信用下沉套利检测）
  4. 风险预警（调仓损耗监测 · 流动性冲击预警 · 季度换手损耗）
  5. 投资建议（YTM收益预期 · 适合人群 · 买卖时点 · 10年国债分析专题）
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

import numpy as np

# ============================================================
# 主入口
# ============================================================

def generate_idx_bond_report(
    report: Any,
    extra_data: Dict[str, Any],
) -> dict:
    """
    生成指数型-固收基金深度评价报告（5板块 + 利率专题）。

    Args:
        report: FundReport 对象
        extra_data: pipeline 预加载的额外数据，包含：
            - tracking_deviation: 日偏离度分析
            - duration_analysis: 久期分析
            - credit_analysis: 信用等级对齐
            - rebalance_monitor: 调仓损耗监测
            - fee_detail: 费率详情
            - coupon_coverage: 票息覆盖率
            - fee_erosion: 费率侵蚀模型
            - ytm_estimate: YTM 估算
            - rate_analysis: 10年国债技术分析
            - index_name: 标的指数名称

    Returns:
        {
          "meta": {fund_name, index_name, ...},
          "headline": 标题行,
          "section1": 一、基本信息（跟踪精度）,
          "section2": 二、收益表现（费率侵蚀）,
          "section3": 三、深度分析（久期+信用）,
          "section4": 四、风险预警（调仓损耗）,
          "section5": 五、投资建议（YTM+利率专题）,
          "full_text": 完整纯文本,
          "chart_data": {...},
        }
    """
    basic = report.basic
    m = report.index_metrics
    charts = report.chart_data

    if not m:
        return _fallback_report(basic)

    # ── 基础数据提取 ──────────────────────────────────────
    cm = m.common
    fund_name = basic.name
    symbol = basic.symbol

    index_name = extra_data.get("index_name", basic.benchmark_text or "标的指数")

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 收益数据
    cum_fund = round(cm.cumulative_return * 100, 2)
    ann_ret = round(cm.annualized_return * 100, 2)
    ann_vol = round(cm.volatility * 100, 2)
    max_dd = round(abs(cm.max_drawdown) * 100, 2)
    sharpe = round(cm.sharpe_ratio, 2)

    # 跟踪效率
    te_annual = round(m.tracking_error_annualized * 100, 4)
    ir = round(m.information_ratio, 2)
    corr = round(m.correlation, 4)

    # 基准累计收益
    bm_info = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm = round(bm_info.get("bm_last_return", 0) * 100, 2)

    # 额外数据
    tracking_dev = extra_data.get("tracking_deviation", {})
    duration_analysis = extra_data.get("duration_analysis", {})
    credit_analysis = extra_data.get("credit_analysis", {})
    rebalance_monitor = extra_data.get("rebalance_monitor", {})
    fee_detail = extra_data.get("fee_detail", {})
    coupon_coverage = extra_data.get("coupon_coverage", {})
    fee_erosion = extra_data.get("fee_erosion", {})
    ytm_estimate = extra_data.get("ytm_estimate", {})
    rate_analysis = extra_data.get("rate_analysis", {})

    ter = fee_detail.get("total_expense_ratio", 0.0)
    mgmt_fee = fee_detail.get("management_fee", 0.0)
    custody_fee = fee_detail.get("custody_fee", 0.0)
    sales_fee = fee_detail.get("sales_service_fee", 0.0)

    # 工具评分
    tool_score = round(m.tool_score, 1)
    tool_grade = m.tool_grade

    # ================================================================
    # Section 1：基本信息（跟踪精度模型）
    # ================================================================

    sec1_lines = [
        f"**{fund_name}**（{symbol}）是跟踪 **{index_name}** 的指数型固收基金。",
        "",
        "### 📈 收益表现概览",
        f"| 指标 | 数值 |",
        f"|------|------|",
        f"| 累计收益 | {cum_fund:+.2f}% |",
        f"| 年化收益 | {ann_ret:+.2f}% |",
        f"| 基准累计 | {cum_bm:+.2f}% |",
        f"| 年化波动率 | {ann_vol:.2f}% |",
        f"| 最大回撤 | {max_dd:.2f}% |",
        f"| Sharpe | {sharpe:.2f} |",
        f"| 年化跟踪误差 | {te_annual:.4f}% |",
        f"| 与基准相关系数 | {corr:.4f} |",
    ]

    # 跟踪精度分析
    sec1_lines.extend([
        "",
        "### 🎯 跟踪精度模型",
        "",
    ])

    mean_abs_dev = tracking_dev.get("mean_abs_dev", 0)
    over_pct = tracking_dev.get("over_threshold_pct", 0)
    quality = tracking_dev.get("quality", "未知")
    p50 = tracking_dev.get("p50", 0)
    p90 = tracking_dev.get("p90", 0)
    p95 = tracking_dev.get("p95", 0)

    sec1_lines.extend([
        f"**日偏离度分布**：每日基金收益与指数收益的差值。",
        "",
        f"| 统计量 | 数值 |",
        f"|--------|------|",
        f"| 日均偏离绝对值 | {mean_abs_dev*100:.4f}% |",
        f"| 偏离中位数(P50) | {p50*100:.4f}% |",
        f"| 90%分位(P90) | {p90*100:.4f}% |",
        f"| 95%分位(P95) | {p95*100:.4f}% |",
        f"| 超过0.05%阈值占比 | {over_pct:.1f}% |",
        f"| 跟踪质量 | {quality} |",
        "",
    ])

    # 跟踪质量评价
    sec1_lines.append(_assess_idx_bond_tracking(mean_abs_dev, over_pct, corr))

    # 近30天偏离
    recent_30 = tracking_dev.get("recent_30d", {})
    if recent_30.get("mean_abs"):
        sec1_lines.extend([
            "",
            f"**近30天偏离**：日均 {recent_30['mean_abs']*100:.4f}%，"
            f"超阈值天数 {recent_30.get('over_threshold_pct', 0):.1f}%。",
        ])

    section1 = "\n".join(sec1_lines)

    # ================================================================
    # Section 2：收益表现（费用侵蚀模型 + 票息覆盖率）
    # ================================================================

    sec2_lines = [
        "### 📊 净值增长 vs 业绩比较基准",
        "",
        f"基金累计收益 **{cum_fund:+.2f}%** vs 基准 **{cum_bm:+.2f}%**，"
        f"超额 {cum_fund - cum_bm:+.2f}%。",
        "",
        "[INSERT_CHART: CUM_RET]",
        "",
        "### 💰 费用侵蚀模型",
        "",
        f"| 费率项目 | 费率 |",
        f"|----------|------|",
        f"| 管理费 | {mgmt_fee*100:.2f}% |",
        f"| 托管费 | {custody_fee*100:.2f}% |",
    ]

    if sales_fee > 0:
        sec2_lines.append(f"| 销售服务费(C类) | {sales_fee*100:.2f}% |")

    sec2_lines.extend([
        f"| **综合费率 (TER)** | **{ter*100:.2f}%** |",
        "",
    ])

    # 票息覆盖率
    fee_eat = coupon_coverage.get("fee_eat_pct", "")
    coupon_est = coupon_coverage.get("coupon_income_estimate", 0)
    net_yield = coupon_coverage.get("net_yield", 0)
    assessment = coupon_coverage.get("assessment", "")

    if fee_eat:
        sec2_lines.extend([
            f"**票息覆盖率分析**：债券收益本身就薄（年化约 {coupon_est:.1f}%），",
            f"{fee_eat}。",
            f"扣除费率后，净收益约 **{net_yield:.2f}%**。",
            "",
            f"评估：{assessment}",
        ])

    # 费率侵蚀模型（多持有年限）
    if fee_erosion:
        hold_y = fee_erosion.get("hold_years", 5)
        sec2_lines.extend([
            "",
            f"**持有{hold_y}年费率侵蚀**：",
            f"| 期限 | 总收益(含费) | 总收益(扣费) | 费率吞噬 |",
            f"|------|-------------|-------------|---------|",
            f"| 年化 | {fee_erosion['annual_return_gross']:+.2f}% | {fee_erosion['annual_return_net']:+.2f}% | {ter*100:.2f}% |",
            f"| {hold_y}年累计 | {fee_erosion['total_gross']:+.2f}% | {fee_erosion['total_net']:+.2f}% | {fee_erosion['total_fee_drag']:.2f}% |",
            "",
        ])

    sec2_lines.extend([
        "[INSERT_CHART: HEATMAP]",
    ])

    section2 = "\n".join(sec2_lines)

    # ================================================================
    # Section 3：深度分析（久期 + 信用对齐）
    # ================================================================

    sec3_lines = [
        "### 📐 久期对照模型",
        "",
    ]

    est_dur = duration_analysis.get("estimated_duration", 0)
    dur_range = duration_analysis.get("duration_range", "未知")
    dur_dist = duration_analysis.get("duration_distribution", {})

    if est_dur > 0:
        sec3_lines.extend([
            f"根据持仓估算，组合**久期约 {est_dur:.2f} 年**（{dur_range}）。",
            "",
        ])

        if dur_dist:
            sec3_lines.append("### 期限分布")
            sec3_lines.append("")
            sec3_lines.append("| 期限区间 | 占比 |")
            sec3_lines.append("|----------|------|")
            for bucket, pct in sorted(dur_dist.items()):
                sec3_lines.append(f"| {bucket} | {pct:.1f}% |")

        sec3_lines.extend([
            "",
            _assess_duration_position(est_dur, dur_range, rate_analysis),
        ])
    else:
        sec3_lines.append("久期数据不足，无法进行久期对照分析。")

    # 信用等级对齐
    sec3_lines.extend([
        "",
        "### 🔒 信用等级对齐分析",
        "",
    ])

    bm_type = credit_analysis.get("benchmark_type", "未知")
    gov_r = credit_analysis.get("gov_ratio", 0)
    policy_r = credit_analysis.get("policy_ratio", 0)
    credit_r = credit_analysis.get("credit_ratio", 0)
    urban_r = credit_analysis.get("urban_ratio", 0)
    is_downgrade = credit_analysis.get("is_credit_downgrade", False)
    downgrade_detail = credit_analysis.get("credit_downgrade_detail", "")

    sec3_lines.extend([
        f"目标指数类型：**{bm_type}**",
        "",
        f"| 债券类型 | 占比 |",
        f"|----------|------|",
        f"| 利率债（国债） | {gov_r:.1f}% |",
        f"| 政策性金融债 | {policy_r:.1f}% |",
        f"| 其他信用债 | {credit_r:.1f}% |",
        f"| 城投债 | {urban_r:.1f}% |",
        "",
    ])

    if is_downgrade and downgrade_detail:
        sec3_lines.extend([
            f"**{downgrade_detail}**",
        ])
    else:
        rate_total = gov_r + policy_r
        if rate_total > 80:
            sec3_lines.append(
                f"利率债（国债+政金债）合计 {rate_total:.1f}%，"
                f"与目标指数定位高度一致，信用结构**干净**。"
            )
        elif rate_total > 60:
            sec3_lines.append(
                f"利率债合计 {rate_total:.1f}%，信用结构基本对齐。"
            )
        else:
            sec3_lines.append(
                f"利率债合计仅 {rate_total:.1f}%，信用结构偏离指数定位，需关注。"
            )

    section3 = "\n".join(sec3_lines)

    # ================================================================
    # Section 4：风险预警（调仓损耗 + 流动性冲击）
    # ================================================================

    sec4_lines = [
        "### 🔄 调仓损耗监测",
        "",
    ]

    avg_normal = rebalance_monitor.get("avg_dev_normal", 0)
    avg_rebal = rebalance_monitor.get("avg_dev_rebalance", 0)
    penalty = rebalance_monitor.get("rebalance_penalty", 0)
    liq_risk = rebalance_monitor.get("liquidity_risk", "unknown")
    rebal_windows = rebalance_monitor.get("rebalance_windows", [])

    if rebalance_monitor.get("avg_dev_normal") is not None and rebalance_monitor["avg_dev_normal"] > 0:
        sec4_lines.extend([
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 正常期日均偏离 | {avg_normal:.4f}% |",
            f"| 调仓期日均偏离 | {avg_rebal:.4f}% |",
            f"| 调仓额外损耗 | {penalty:.4f}% |",
            f"| 流动性风险等级 | {_format_liq_risk(liq_risk)} |",
            "",
        ])

        if rebal_windows:
            sec4_lines.append("### 调仓窗口偏离明细")
            sec4_lines.append("")
            sec4_lines.append("| 调仓期 | 日均偏离 | 交易日数 |")
            sec4_lines.append("|--------|---------|---------|")
            for w in rebal_windows[-6:]:
                sec4_lines.append(
                    f"| {w['period']} | {w['avg_deviation']:.4f}% | {w['trading_days']} |"
                )

        # 流动性风险评价
        sec4_lines.extend([
            "",
            _assess_liquidity_risk(liq_risk, penalty, avg_rebal),
        ])
    else:
        sec4_lines.append("调仓数据不足，无法进行调仓损耗分析。")

    # 回撤图
    sec4_lines.extend([
        "",
        "[INSERT_CHART: DRAWDOWN]",
    ])

    section4 = "\n".join(sec4_lines)

    # ================================================================
    # Section 5：投资建议（YTM + 利率专题）
    # ================================================================

    sec5_lines = [
        "### 💰 收益预期：基于YTM水位",
        "",
    ]

    est_ytm = ytm_estimate.get("estimated_ytm")
    ytm_risk_free = ytm_estimate.get("risk_free_component", 0)
    ytm_credit = ytm_estimate.get("credit_spread_component", 0)
    ytm_dur_prem = ytm_estimate.get("duration_premium", 0)
    ytm_assess = ytm_estimate.get("ytm_assessment", "数据不足")

    if est_ytm is not None:
        sec5_lines.extend([
            f"根据当前久期（{est_dur:.1f}年）和信用结构估算：",
            f"",
            f"| 收益来源 | 估算值 |",
            f"|----------|--------|",
            f"| 无风险利率 | {ytm_risk_free:.2f}% |",
            f"| 信用利差 | {ytm_credit:.2f}% |",
            f"| 久期溢价 | {ytm_dur_prem:.2f}% |",
            f"| **估算YTM** | **{est_ytm:.2f}%** |",
            f"",
            f"评估：{ytm_assess}",
        ])
    else:
        sec5_lines.append("YTM 估算数据不足。")

    # 适合人群
    sec5_lines.extend([
        "",
        "### 👥 适合人群",
        "",
    ])

    if dur_range in ("短久期", "中短久期"):
        sec5_lines.extend([
            f"该基金跟踪{dur_range}指数，**适合避险型配置**：",
            "- 追求稳定票息收入、不愿承受大幅波动的保守投资者",
            "- 作为组合的流动性管理和避险工具",
            "- 有短期资金打理需求（如3-12个月）的资金",
        ])
    elif dur_range in ("中久期", "中长久期"):
        sec5_lines.extend([
            f"该基金跟踪{dur_range}指数，**适合利率敏感型配置**：",
            "- 对利率走势有一定判断能力的中级投资者",
            "- 希望在利率下行周期获取资本利得的趋势投资者",
            "- 作为固收组合的核心配置（7-10年久期品种）",
        ])
    else:
        sec5_lines.extend([
            f"该基金跟踪{dur_range}指数，**适合趋势型配置**：",
            "- 对利率走势有较强判断能力的进阶投资者",
            "- 能承受一定净值波动的投资者",
            "- 作为固收增强配置（长久期品种在利率下行时收益可观）",
        ])

    # 买卖时点建议
    sec5_lines.extend([
        "",
        "### ⏰ 买卖时点建议：与基准利率挂钩",
        "",
        _build_timing_advice(est_dur, rate_analysis),
    ])

    # 操作建议（保守派 vs 趋势派）
    sec5_lines.extend([
        "",
        "### 📌 操作建议",
        "",
        "**如果你是保守派**：",
        f"- 选 1-3 年久期的指数品种，安心拿每年的票息",
        f"- 不择时，长期持有，关注费率成本（当前 TER {ter*100:.2f}%）",
        f"- 在利率上行周期也能控制回撤（短久期品种久期仅 1-3 年）",
        "",
        "**如果你是趋势派**：",
        "- 盯着 10 年期国债收益率看，参考下方技术分析",
        "- 只要收益率在下行通道，就选 7-10 年久期品种",
        "- 一旦收益率开始横盘或掉头向上，立刻撤退到短久期品种避险",
    ])

    # 10年国债收益率技术分析专题
    sec5_lines.extend([
        "",
        "---",
        "",
        "### 📈 10年国债收益率分析",
        "",
    ])

    current_y10y = rate_analysis.get("current_y10y")
    y10y_pct = rate_analysis.get("y10y_percentile")
    ma20 = rate_analysis.get("ma20")
    ma60 = rate_analysis.get("ma60")
    pattern = rate_analysis.get("pattern", "数据不足")
    death_cross = rate_analysis.get("death_cross", False)
    golden_cross = rate_analysis.get("golden_cross", False)
    higher_highs = rate_analysis.get("higher_highs", False)
    lower_lows = rate_analysis.get("lower_lows", False)

    if current_y10y is not None:
        sec5_lines.extend([
            f"**当前 10Y 收益率**：**{current_y10y:.3f}%**（历史分位 {y10y_pct or 0:.1f}%）",
        ])

        if ma20 is not None:
            sec5_lines.append(f"20日均线：{ma20:.3f}%")
        if ma60 is not None:
            sec5_lines.append(f"60日均线：{ma60:.3f}%")

        sec5_lines.extend([
            "",
            f"**技术面形态**：{pattern}",
            "",
        ])

        # 均线系统
        if ma20 is not None and ma60 is not None:
            sec5_lines.extend([
                "**均线系统**：",
                "",
            ])
            if death_cross:
                sec5_lines.append(
                    f"- 20日线（{ma20:.3f}%）**下穿** 60日线（{ma60:.3f}%），形成**死叉**，"
                    f"通常确认下行趋势加剧。"
                )
            elif golden_cross:
                sec5_lines.append(
                    f"- 20日线（{ma20:.3f}%）**上穿** 60日线（{ma60:.3f}%），形成**金叉**，"
                    f"通常确认上行趋势启动。"
                )
            else:
                sec5_lines.append(
                    f"- 20日线（{ma20:.3f}%）与 60日线（{ma60:.3f}%）暂无交叉信号。"
                )

        # 高低点分析
        if higher_highs or lower_lows:
            sec5_lines.extend(["", "**高低点转换（Price Action）**：", ""])
            if lower_lows and not higher_highs:
                sec5_lines.append(
                    "- 收益率呈现 **Lower Highs & Lower Lows**，"
                    "每一个新高点比前低、每一个新低点也更低，典型下行通道。"
                )
            elif higher_highs and not lower_lows:
                sec5_lines.append(
                    "- 收益率呈现 **Higher Highs & Higher Lows**，"
                    "高点不断抬升、低点也不断抬升，典型上行通道。"
                )

        sec5_lines.extend([
            "",
            "### 🏛️ 基本面判断：驱动力分析",
            "",
            "**货币政策（核心指标）**",
            "",
            "观察 MLF（中期借贷便利）和 LPR（贷款市场报价利率）：",
            "- 若央行处于降息周期或频繁下调存款准备金率（RRR），10年期国债收益率通常处于下行通道",
            "- 若 MLF/LPR 保持稳定或上调，收益率可能企稳或上行",
            "",
            "**通胀与经济数据**",
            "",
            "| 信号类型 | 指标 | 对收益率影响 |",
            "|----------|------|-------------|",
            "| 下行信号 | CPI/PPI 疲软，PMI < 50 | 收益率下行（债市牛） |",
            "| 上行信号 | 通胀抬头，社融超预期 | 收益率上行（债市熊） |",
            "",
            "### 💭 情绪面与资金面",
            "",
            "**存单利率（NCD）**",
            "- 观察 1 年期同业存单利率",
            "- 存单利率持续下行 = 银行体系资金充裕 = 向下引导 10Y 收益率",
            "",
            "**股债跷跷板**",
            "- 股市持续放量上涨时，10Y 收益率往往上行（资金从债市流向股市）",
            "- 股市阴跌时，债市通常是避风港（资金涌入债市，收益率下行）",
            "",
            "[INSERT_CHART: Y10Y_TREND]",
        ])
    else:
        sec5_lines.append("10年国债收益率数据加载失败，无法进行技术分析。")

    section5 = "\n".join(sec5_lines)

    # ================================================================
    # 组装完整报告
    # ================================================================

    # 标签
    tags = []
    if quality == "极优":
        tags.append("跟踪极优")
    elif quality == "优良":
        tags.append("跟踪优良")
    elif quality in ("一般", "较差"):
        tags.append("跟踪偏差大")

    if is_downgrade:
        tags.append("信用下沉")

    if liq_risk == "high":
        tags.append("流动性风险")
    elif liq_risk == "medium":
        tags.append("流动性关注")

    if rate_analysis.get("y10y_trend") == "down":
        tags.append("利率下行")
    elif rate_analysis.get("y10y_trend") == "up":
        tags.append("利率上行")

    meta = {
        "fund_name": fund_name,
        "symbol": symbol,
        "index_name": index_name,
        "duration_range": dur_range,
        "estimated_duration": est_dur,
        "tracking_quality": quality,
        "start_date": start_date,
        "end_date": end_date,
        "grade": tool_grade,
        "score": tool_score,
        "tags": tags,
    }

    headline = (
        f"## 📊 {fund_name} — 指数型固收深度分析  \n"
        f"标的指数：{index_name}  |  估算久期：{est_dur:.1f}年（{dur_range}）  |  "
        f"综合评分：{tool_score}（{tool_grade}）"
    )

    full_text = "\n\n".join([
        headline, section1, section2, section3, section4, section5,
    ])

    return {
        "meta": meta,
        "headline": headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "section5": section5,
        "full_text": full_text,
        "chart_data": {
            "rate_analysis": rate_analysis,
        },
    }


# ============================================================
# 辅助函数
# ============================================================

def _extract_date_range(charts: dict) -> tuple:
    """从 chart_data 提取日期范围"""
    import pandas as pd
    nav_df = charts.get("nav_df")
    if nav_df is not None and not nav_df.empty and "date" in nav_df.columns:
        dates = pd.to_datetime(nav_df["date"])
        return dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")
    return "--", "--"


def _assess_idx_bond_tracking(mean_abs: float, over_pct: float, corr: float) -> str:
    """评估指数型-固收基金跟踪质量"""
    parts = []

    if mean_abs <= 0.0003:
        parts.append(
            f"跟踪质量**极优**。日均偏离仅 {mean_abs*100:.4f}%，"
            f"抽样复制能力出色，基金经理拿货能力强。"
        )
    elif mean_abs <= 0.0005:
        parts.append(
            f"跟踪质量**优良**。日均偏离 {mean_abs*100:.4f}%，在同类中属于优秀水平。"
        )
    elif mean_abs <= 0.001:
        parts.append(
            f"跟踪质量**一般**。日均偏离 {mean_abs*100:.4f}%，偏高于理想水平，"
            f"可能受抽样复制偏差或现金拖累影响。"
        )
    else:
        parts.append(
            f"跟踪质量**较差**。日均偏离 {mean_abs*100:.4f}%，"
            f"显著偏离指数，说明经理拿货能力或抽样模型有待提高。"
        )

    # 超阈值分析
    if over_pct > 50:
        parts.append(
            f"注意：超过 0.05% 阈值的天数占比 {over_pct:.1f}%，"
            f"意味着频繁出现较大跟踪偏离。"
        )

    # 相关性
    if corr < 0.99:
        parts.append(
            f"与基准相关系数 {corr:.4f} 偏低，可能存在非跟踪操作。"
        )

    return "\n".join(parts)


def _assess_duration_position(
    est_dur: float, dur_range: str, rate_analysis: dict,
) -> str:
    """评估久期定位是否合适"""
    trend = rate_analysis.get("y10y_trend", "unknown")
    current_y10y = rate_analysis.get("current_y10y")

    lines = []

    if trend == "down" or trend == "down_flat":
        if est_dur >= 5:
            lines.append(
                f"当前处于**利率下行环境**，久期 {est_dur:.1f} 年的定位有利于获取资本利得。"
            )
        else:
            lines.append(
                f"当前处于**利率下行环境**，但久期仅 {est_dur:.1f} 年（{dur_range}），"
                f"可能错失部分利率下行带来的资本利得。如看好利率继续下行，"
                f"可考虑久期更长的同类产品。"
            )
    elif trend == "up" or trend == "up_flat":
        if est_dur <= 3:
            lines.append(
                f"当前处于**利率上行环境**，短久期（{est_dur:.1f}年）定位是明智之举，"
                f"能有效控制利率风险。"
            )
        else:
            lines.append(
                f"当前处于**利率上行环境**，久期 {est_dur:.1f} 年偏长，"
                f"面临较大的净值回撤风险。建议关注短久期品种。"
            )
    else:
        lines.append(
            f"当前利率方向不明，久期 {est_dur:.1f} 年（{dur_range}）的定位较为中性。"
        )

    if current_y10y is not None:
        pct = rate_analysis.get("y10y_percentile", 50) or 50
        if pct <= 20:
            lines.append(
                f"10Y 收益率处于历史 {pct:.0f}% 分位（低位），"
                f"未来下行空间有限、上行风险更大，注意久期风险控制。"
            )
        elif pct >= 80:
            lines.append(
                f"10Y 收益率处于历史 {pct:.0f}% 分位（高位），"
                f"均值回归概率大，长久期品种有望受益。"
            )

    return "\n".join(lines)


def _format_liq_risk(risk: str) -> str:
    risk_map = {
        "high": "🔴 高",
        "medium": "🟡 中",
        "low": "🟢 低",
        "unknown": "⚪ 未知",
    }
    return risk_map.get(risk, risk)


def _assess_liquidity_risk(liq_risk: str, penalty: float, avg_rebal: float) -> str:
    """评估流动性风险"""
    if liq_risk == "high":
        return (
            f"**流动性风险偏高**。调仓期偏离度异常放大（额外损耗 {penalty:.4f}%），"
            f"说明该基金在跟踪指数时面临流动性黑洞。"
            f"在大规模申赎时容易亏钱，建议关注基金规模和申赎状态。"
        )
    elif liq_risk == "medium":
        return (
            f"**流动性风险中等**。调仓期有轻微的偏离放大（额外损耗 {penalty:.4f}%），"
            f"属于可接受范围，但需关注调仓窗口前后的净值波动。"
        )
    elif liq_risk == "low":
        return (
            f"**流动性风险较低**。调仓期间偏离度控制良好，"
            f"基金经理在调仓时的执行能力较强。"
        )
    return "流动性风险评估数据不足。"


def _build_timing_advice(est_dur: float, rate_analysis: dict) -> str:
    """构建买卖时点建议"""
    trend = rate_analysis.get("y10y_trend", "unknown")
    current_y10y = rate_analysis.get("current_y10y")
    pattern = rate_analysis.get("pattern", "")
    pct = rate_analysis.get("y10y_percentile", 50) or 50

    lines = []

    if trend == "down" or "down_flat":
        lines.extend([
            f"**当前环境：利率下行趋势**（10Y: {current_y10y or '?'}%, {pattern}）",
            "",
            "- 预期国债收益率继续下行（债市牛市），**优先选久期偏长的指数品种**",
            "- 久期越长，利率下行时资本利得越可观",
            f"- 10Y 收益率处于历史 {pct:.0f}% 分位，"
              + ("下行空间仍大" if pct > 30 else "已处低位，注意止盈"),
        ])
    elif trend == "up" or "up_flat":
        lines.extend([
            f"**当前环境：利率上行趋势**（10Y: {current_y10y or '?'}%, {pattern}）",
            "",
            "- 预期国债收益率上行（债市调整），**立刻撤退到短久期品种避险**",
            "- 短久期品种在利率上行时回撤较小",
            "- 若持有长久期品种，建议止损或转换为短久期产品",
        ])
    else:
        lines.extend([
            f"**当前环境：利率方向不明**（10Y: {current_y10y or '?'}%, {pattern}）",
            "",
            "- 震荡市中建议持有中等久期品种（3-5年）",
            "- 密切关注 20日/60日均线交叉信号",
            "- 一旦出现死叉或金叉，及时调整久期配置",
        ])

    return "\n".join(lines)


def _fallback_report(basic: Any) -> dict:
    """数据不足时的回退报告"""
    name = basic.name if basic else "未知基金"
    return {
        "meta": {"fund_name": name, "index_name": "未知"},
        "headline": f"## {name} — 数据不足",
        "section1": "数据不足，无法生成深度分析报告。请稍后重试。",
        "section2": "",
        "section3": "",
        "section4": "",
        "section5": "",
        "full_text": f"{name} 数据不足，无法生成深度分析。",
        "chart_data": {},
    }
