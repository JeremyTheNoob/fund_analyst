"""
指数型-股票基金 深度评价报告生成器 — fund_quant_v2
角色：资深指数研究员（ETF 策略分析师）
报告结构：5板块 + 图表插入点标记
  1. 基本信息（收益展示 + 流动性规模锚点 + 分类专属分析）
  2. 费率排名（TER 竞争力 + 隐形溢价评估）
  3. 深度分析（PE/PB 估值锚点 + 收益来源拆解）
  4. 风险预警（持仓穿透 + 行业集中度 + 调仓冲击）
  5. 投资建议（被动型 vs 增强型 策略建议）
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

import numpy as np

# ============================================================
# 主入口
# ============================================================

def generate_idx_stock_report(
    report: Any,
    extra_data: Dict[str, Any],
) -> dict:
    """
    生成指数型-股票基金深度评价报告（5板块结构）。

    Args:
        report: FundReport 对象
        extra_data: pipeline 预加载的额外数据，包含：
            - subtype: "passive" / "enhanced"
            - is_etf: bool
            - fee_detail: {management_fee, custody_fee, sales_service_fee, total_expense_ratio}
            - valuation_df: PE/PB 估值历史 DataFrame
            - pe_percentile: PE 分位数据
            - pb_percentile: PB 分位数据
            - concentration: 成份股集中度分析
            - liquidity: ETF 流动性数据
            - rebalance_info: 调仓信息
            - index_name: 标的指数名称

    Returns:
        {
          "meta":       {fund_name, subtype, is_etf, index_name, ...},
          "headline":   标题行,
          "section1":   一、基本信息（收益 + 流动性 + 分类专属分析）,
          "section2":   二、费率排名（TER 竞争力 + 隐形溢价评估）,
          "section3":   三、深度分析（估值锚点 + 收益来源拆解）,
          "section4":   四、风险预警（持仓穿透 + 行业集中度 + 调仓冲击）,
          "section5":   五、投资建议（分被动/增强）,
          "full_text":  完整纯文本,
          "chart_data": {pe_history, pb_history} 供图表渲染使用,
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

    # 分类信息
    subtype = extra_data.get("subtype", "passive")
    is_etf = extra_data.get("is_etf", False)
    index_name = extra_data.get("index_name", basic.benchmark_text or "标的指数")
    subtype_cn = "指数增强型" if subtype == "enhanced" else "被动跟踪型"
    etf_label = " · ETF" if is_etf else ""

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

    # 费率
    fee = extra_data.get("fee_detail", {})
    ter = fee.get("total_expense_ratio", 0.0)
    mgmt_fee = fee.get("management_fee", 0.0)
    custody_fee = fee.get("custody_fee", 0.0)
    sales_fee = fee.get("sales_service_fee", 0.0)

    # 估值
    pe_pct = extra_data.get("pe_percentile", {})
    pb_pct = extra_data.get("pb_percentile", {})
    pe_zone = pe_pct.get("zone", "数据不足")
    pb_zone = pb_pct.get("zone", "数据不足")
    pe_current = pe_pct.get("current")
    pb_current = pb_pct.get("current")
    pe_percentile = pe_pct.get("percentile") or 0
    dividend_yield = pe_pct.get("dividend_yield")  # 从 valuation_df 提取

    # 集中度
    conc = extra_data.get("concentration", {})
    top10 = conc.get("top10", [])
    top10_sum = conc.get("top10_sum", 0)
    hhi = conc.get("hhi", 0)
    giant_risk = conc.get("giant_risk", "")

    # 流动性
    liq = extra_data.get("liquidity", {})
    avg_amount_str = liq.get("daily_avg_amount_str", "—")
    is_liquid = liq.get("is_liquid", True)
    amount_trend = liq.get("amount_trend", {})

    # 调仓
    rebal = extra_data.get("rebalance_info", {})
    next_rebalance = rebal.get("next_rebalance", "—")

    # 规模预警
    scale_str = basic.scale or ""
    scale_warning = ""
    if scale_str:
        try:
            scale_val = float(str(scale_str).replace("亿元", "").strip())
            if scale_val < 0.5:
                scale_warning = f"⚠️ **规模预警**：当前规模 {scale_str}，低于 5000 万元清盘红线，存在清盘风险！"
        except (ValueError, TypeError):
            pass

    # 超额收益（用于增强型）
    excess_ret = cum_fund - cum_bm

    # ── 工具评分 ──────────────────────────────────────────
    tool_score = round(m.tool_score, 1)
    tool_grade = m.tool_grade

    # ================================================================
    # Section 1：基本信息
    # ================================================================

    # --- 收益总览 ---
    sec1_lines = [
        f"**{fund_name}**（{symbol}）是跟踪 **{index_name}** 的{subtype_cn}{etf_label}基金。",
        "",
        "### 📈 收益表现",
        f"| 指标 | 数值 |",
        f"|------|------|",
        f"| 累计收益 | {cum_fund:+.2f}% |",
        f"| 年化收益 | {ann_ret:+.2f}% |",
        f"| 基准累计 | {cum_bm:+.2f}% |",
        f"| 年化波动率 | {ann_vol:.2f}% |",
        f"| 最大回撤 | {max_dd:.2f}% |",
        f"| Sharpe | {sharpe:.2f} |",
        f"| 年化跟踪误差 | {te_annual:.4f}% |",
    ]

    # --- 流动性与规模锚点 ---
    sec1_lines.extend([
        "",
        "### 💧 流动性与规模锚点",
    ])

    if is_etf:
        liquid_status = "✅ 流动性充足" if is_liquid else "⚠️ 流动性偏弱，买卖价差可能较大"
        sec1_lines.append(f"日均成交额：**{avg_amount_str}**（{liquid_status}）")
        if amount_trend:
            trend_parts = [f"{k}：{v}" for k, v in amount_trend.items()]
            sec1_lines.append(f"成交趋势：{' | '.join(trend_parts)}")
    else:
        sec1_lines.append(f"该基金为**场外指数基金**，通过基金公司申赎，无二级市场折溢价风险。")

    if scale_warning:
        sec1_lines.append(scale_warning)
    elif scale_str:
        sec1_lines.append(f"基金规模：**{scale_str}**")

    # --- 分类专属分析 ---
    if subtype == "passive":
        sec1_lines.extend([
            "",
            "### 🎯 跟踪精度分析",
            f"作为**被动跟踪型**基金，核心使命是精确复制指数。年化跟踪误差 {te_annual:.4f}%，",
            f"与基准相关系数 {corr:.4f}。",

            _assess_tracking_quality(te_annual, corr),

            "[INSERT_CHART: TRACKING_ERROR_SCATTER]",
        ])
    else:
        sec1_lines.extend([
            "",
            "### 🎯 增强策略有效性分析",
            f"作为**指数增强型**基金，目标是在控制跟踪误差的前提下获取超额收益。",
            "",
            f"- **累计超额收益**：{excess_ret:+.2f}%（vs 基准）",
            f"- **信息比率 (IR)**：{ir:.2f}（每承担1%跟踪风险，获取 {ir:.2f}% 超额收益）",
            f"- **年化跟踪误差**：{te_annual:.4f}%",
            "",
            _assess_enhanced_quality(ir, te_annual, excess_ret),
        ])

    section1 = "\n".join(sec1_lines)

    # ================================================================
    # Section 2：费率排名
    # ================================================================

    sec2_lines = [
        "### 💰 费率竞争力模型",
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

    # 费率评估
    ter_annual_drag = round(ter * 100, 2)
    if ter <= 0.006:
        fee_assessment = f"TER {ter_annual_drag}% 处于同类**较低水平**，成本优势明显。"
    elif ter <= 0.010:
        fee_assessment = f"TER {ter_annual_drag}% 处于同类**中等水平**。"
    elif ter <= 0.015:
        fee_assessment = f"TER {ter_annual_drag}% 处于同类**偏高水平**，长期复利下费率侵蚀显著。"
    else:
        fee_assessment = f"TER {ter_annual_drag}% 处于同类**较高水平**，每年吞噬 {ter_annual_drag}% 收益。"

    sec2_lines.append(fee_assessment)

    # 增强型：费率溢价评估
    if subtype == "enhanced":
        passive_ter = 0.005 + 0.001  # 典型被动型：管理0.5%+托管0.1%
        premium = (ter - passive_ter) * 100
        sec2_lines.extend([
            "",
            f"**隐形溢价评估**：相比典型被动型基金（TER ≈ 0.60%），",
            f"该增强型多收 {premium:.2f}% 费率。",
        ])
        if premium > 1.0:
            sec2_lines.append(
                f"⚠️ 费率溢价超过 1%，需验证其超额收益 Alpha（当前累计 {excess_ret:+.2f}%）"
                f"是否足以覆盖这笔额外费率。年化超额需 > {premium:.2f}% 才划算。"
            )
        elif premium > 0.5:
            sec2_lines.append(
                f"费率溢价约 {premium:.2f}%，属于行业常见水平。"
                f"建议关注信息比率 IR={ir:.2f}，持续稳定才是硬道理。"
            )
        else:
            sec2_lines.append(
                f"费率溢价仅 {premium:.2f}%，增强策略的成本可控。"
            )

    sec2_lines.extend([
        "",
        f"📊 **费率对收益的影响**：持有10年，{ter_annual_drag}% TER 按 8% 年化复利计算，",
        f"累计侵蚀收益约 **{round((1-ter)**10 * (1.08)**10 / 1.08**10 * 100 - 100, 1):.1f}%**。",
    ])

    section2 = "\n".join(sec2_lines)

    # ================================================================
    # Section 3：深度分析
    # ================================================================

    sec3_lines = [
        "### 📐 估值锚点模型",
        "",
    ]

    # PE 分位
    if pe_current is not None:
        sec3_lines.extend([
            f"**{index_name} PE-TTM**：当前 **{pe_current:.2f}** 倍，"
            f"处于近5年 **{pe_percentile:.1f}%** 分位（{pe_zone}）。",
            f"历史区间：{pe_pct.get('min', '—')} ~ {pe_pct.get('max', '—')} 倍，"
            f"中位数 {pe_pct.get('median', '—')} 倍。",
            "",
            "[INSERT_CHART: PE_PERCENTILE]",
        ])
    else:
        sec3_lines.append("PE 估值数据暂不可用。")

    sec3_lines.append("")

    # PB 分位
    if pb_current is not None:
        pb_percentile = pb_pct.get("percentile", 0)
        sec3_lines.extend([
            f"**{index_name} PB**：当前 **{pb_current:.2f}** 倍，"
            f"处于近5年 **{pb_percentile:.1f}%** 分位（{pb_zone}）。",
            f"历史区间：{pb_pct.get('min', '—')} ~ {pb_pct.get('max', '—')} 倍，"
            f"中位数 {pb_pct.get('median', '—')} 倍。",
            "",
            "[INSERT_CHART: PB_PERCENTILE]",
        ])
    else:
        sec3_lines.append("PB 估值数据暂不可用。")

    # 估值综合判断
    sec3_lines.extend([
        "",
        "### 🔍 收益来源拆解",
    ])

    if subtype == "passive":
        # 被动型：股息率分析
        sec3_lines.extend([
            "**被动型基金**收益来源分解：",
            "",
            "1. **价格收益**（资本利得）：指数点位变动带来的收益",
            "2. **股息收益**（分红再投资）：成份股现金分红",
            "",
        ])
        if dividend_yield is not None:
            dy = float(dividend_yield) * 100
            sec3_lines.extend([
                f"当前{index_name}股息率约 **{dy:.2f}%**，在年化 {ann_ret:+.2f}% 的总收益中，",
                f"股息贡献占比约 **{min(round(abs(dy/max(abs(ann_ret),0.01))*100, 1), 100):.1f}%**。",
                f"{'高股息指数的"安全垫"效应显著。' if dy > 3 else '股息贡献相对有限，主要依赖资本利得。'}",
            ])
        sec3_lines.extend([
            "",
            "[INSERT_CHART: CUM_RET]",
            "[INSERT_CHART: HEATMAP]",
        ])
    else:
        # 增强型：IR + 因子暴露
        sec3_lines.extend([
            "**增强型基金**收益来源分解：",
            "",
            f"1. **指数收益**（Beta）：{cum_bm:+.2f}% — 跟随标的指数的被动收益",
            f"2. **增强收益**（Alpha）：{excess_ret:+.2f}% — 基金经理主动操作创造的超额",
            "",
            f"| 指标 | 数值 | 评价 |",
            f"|------|------|------|",
            f"| 信息比率 IR | {ir:.2f} | {_rate_ir(ir)} |",
            f"| 跟踪误差 TE | {te_annual:.4f}% | {_rate_te(te_annual)} |",
            f"| 超额收益 | {excess_ret:+.2f}% | {_rate_excess(excess_ret)} |",
            "",
        ])

        # Alpha 持续性分析
        sec3_lines.extend(_analyze_alpha_persistence(charts))

        sec3_lines.extend([
            "",
            "[INSERT_CHART: EXCESS_ALPH]",
            "[INSERT_CHART: CUM_RET]",
        ])

    section3 = "\n".join(sec3_lines)

    # ================================================================
    # Section 4：风险预警
    # ================================================================

    sec4_lines = [
        "### ⚠️ 持仓穿透模型",
        "",
    ]

    # 前十大成份股
    if top10:
        sec4_lines.append(f"**{index_name}** 共 **{conc.get('total_count', 0)}** 只成份股，")
        sec4_lines.append(f"前十大权重合计 **{top10_sum:.2f}%**，前五大合计 **{conc.get('top5_sum', 0):.2f}%**。")
        sec4_lines.append("")
        sec4_lines.append("| 排名 | 成份股 | 权重 |")
        sec4_lines.append("|------|--------|------|")
        for i, stock in enumerate(top10, 1):
            w = stock.get("weight", 0)
            name = stock.get("name", "—")
            code = stock.get("code", "—")
            sec4_lines.append(f"| {i} | {name}({code}) | {w:.2f}% |")

        sec4_lines.append("")
        if giant_risk:
            sec4_lines.append(f"🔴 **{giant_risk}**")
        sec4_lines.append("")

        # 前十大柱状图
        sec4_lines.append("[INSERT_CHART: TOP10_WEIGHTS]")
    else:
        sec4_lines.append("成份股权重数据暂不可用。")
        sec4_lines.append("")

    # 行业集中度预警
    sec4_lines.extend([
        "### 🏭 行业集中度预警",
        "",
        _assess_industry_concentration(conc, index_name),
    ])

    # 调仓冲击
    sec4_lines.extend([
        "",
        "### 🔄 成份股调整冲击",
        "",
        rebal.get("description", ""),
        "",
        f"下次预计调仓日期：**{next_rebalance}**。",
        "",
    ])

    sec4_lines.extend([
        "[INSERT_CHART: DRAWDOWN]",
    ])

    section4 = "\n".join(sec4_lines)

    # ================================================================
    # Section 5：投资建议
    # ================================================================

    if subtype == "passive":
        section5 = _build_passive_advice(
            pe_zone, pe_percentile, pb_zone,
            is_etf, is_liquid, scale_warning,
            te_annual, fund_name, index_name,
        )
    else:
        section5 = _build_enhanced_advice(
            ir, te_annual, excess_ret,
            pe_zone, pe_percentile,
            fund_name, index_name, ter,
        )

    # ================================================================
    # 组装完整报告
    # ================================================================

    # 估值区间标签
    zone_tags = []
    if pe_zone in ("极度低估", "低估"):
        zone_tags.append(f"PE{pe_zone}")
    elif pe_zone in ("极度高估", "高估"):
        zone_tags.append(f"PE{pe_zone}")
    if pb_zone in ("极度低估", "低估"):
        zone_tags.append(f"PB{pb_zone}")

    # 评分标签
    score_tags = []
    if subtype == "passive":
        if te_annual <= 0.003:
            score_tags.append("跟踪精准")
        elif te_annual > 0.01:
            score_tags.append("跟踪偏差大")
    else:
        if ir >= 1.5:
            score_tags.append("增强优秀")
        elif ir >= 0.5:
            score_tags.append("增强尚可")
        elif ir < 0:
            score_tags.append("增强失效")

    if is_etf and not is_liquid:
        score_tags.append("流动性不足")
    if scale_warning:
        score_tags.append("规模预警")

    meta = {
        "fund_name": fund_name,
        "symbol": symbol,
        "subtype": subtype_cn,
        "is_etf": is_etf,
        "index_name": index_name,
        "start_date": start_date,
        "end_date": end_date,
        "grade": tool_grade,
        "score": tool_score,
        "tags": zone_tags + score_tags,
    }

    headline = (
        f"## 📊 {fund_name} — {subtype_cn}{etf_label}深度分析  \n"
        f"标的指数：{index_name}  |  综合评分：{tool_score}（{tool_grade}）"
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
            "pe_percentile": pe_pct,
            "pb_percentile": pb_pct,
            "concentration": conc,
        },
    }


# ============================================================
# 辅助函数
# ============================================================

def _extract_date_range(charts: dict) -> tuple:
    """从 chart_data 提取日期范围"""
    nav_df = charts.get("nav_df")
    if nav_df is not None and not nav_df.empty and "date" in nav_df.columns:
        dates = pd.to_datetime(nav_df["date"])
        return dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")
    return "—", "—"


# 注意：需要在文件顶部 import pandas
import pandas as pd


def _assess_tracking_quality(te: float, corr: float) -> str:
    """评估被动型基金跟踪质量"""
    if te <= 0.002 and corr >= 0.999:
        return f"跟踪质量**极优**，TE 仅 {te:.4f}%，相关系数几乎为1，堪称指数镜像。"
    elif te <= 0.005 and corr >= 0.998:
        return f"跟踪质量**优良**，TE {te:.4f}% 在同类中属于优秀水平。"
    elif te <= 0.010:
        return f"跟踪质量**一般**，TE {te:.4f}% 偏高，可能受现金拖累或管理费侵蚀影响。"
    else:
        return f"⚠️ 跟踪质量**较差**，TE {te:.4f}% 明显偏离，需排查是否频繁申赎导致。"


def _assess_enhanced_quality(ir: float, te: float, excess: float) -> str:
    """评估增强型基金增强效果"""
    parts = []
    if ir >= 1.5:
        parts.append(f"信息比率 IR={ir:.2f} 属于**优秀水平**，增强策略长期有效。")
    elif ir >= 0.8:
        parts.append(f"信息比率 IR={ir:.2f} 属于**良好水平**，增强策略有一定价值。")
    elif ir >= 0.3:
        parts.append(f"信息比率 IR={ir:.2f} 属于**一般水平**，增强效果不稳定。")
    elif ir >= 0:
        parts.append(f"信息比率 IR={ir:.2f} **较低**，增强收益微弱，可能不如直接买被动型。")
    else:
        parts.append(f"⚠️ 信息比率 IR={ir:.2f} **为负**，增强策略实际上在**拖累收益**！")

    if excess < -2:
        parts.append(f"累计超额收益 {excess:+.2f}%，跑输基准，增强策略完全失效。")

    return "\n".join(parts)


def _rate_ir(ir: float) -> str:
    if ir >= 1.5: return "🟢 优秀"
    if ir >= 0.8: return "🟡 良好"
    if ir >= 0.3: return "🟠 一般"
    if ir >= 0: return "🔴 偏弱"
    return "🔴 失效"


def _rate_te(te: float) -> str:
    if te <= 0.003: return "🟢 极低"
    if te <= 0.005: return "🟢 优秀"
    if te <= 0.010: return "🟡 一般"
    return "🔴 偏高"


def _rate_excess(excess: float) -> str:
    if excess >= 10: return "🟢 卓越"
    if excess >= 5: return "🟢 优秀"
    if excess >= 2: return "🟡 良好"
    if excess >= 0: return "🟠 微弱"
    return "🔴 跑输"


def _analyze_alpha_persistence(charts: dict) -> List[str]:
    """
    分析 Alpha 持续性：逐月超额收益是否稳定产出。
    从 chart_data 的超额收益数据中提取月度超额，评估稳定性。
    """
    lines = [
        "**Alpha 持续性分析**：",
        "",
    ]

    excess_data = charts.get("excess_return", {})
    if not excess_data or "series" not in excess_data:
        lines.append("月度超额收益数据不足，无法进行持续性分析。")
        return lines

    # 尝试从超额收益序列构建月度超额
    try:
        import pandas as pd
        x_data = excess_data.get("x", [])
        series = excess_data.get("series", [])
        if not series:
            lines.append("超额收益序列为空。")
            return lines

        # 找到基金超额收益序列
        fund_series = None
        for s in series:
            if s.get("name", "") not in ("基准", "benchmark", "沪深300"):
                fund_series = s.get("data", [])
                break

        if not fund_series or not x_data:
            lines.append("超额收益数据格式异常。")
            return lines

        # 构建DataFrame
        df = pd.DataFrame({"date": x_data[:len(fund_series)], "excess": fund_series})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna()

        if len(df) < 30:
            lines.append("数据不足60个交易日，无法进行月度分析。")
            return lines

        # 按月汇总超额收益
        df["month"] = df["date"].dt.to_period("M")
        monthly = df.groupby("month")["excess"].sum() * 100  # 转为百分比

        positive_months = (monthly > 0).sum()
        total_months = len(monthly)
        win_rate = positive_months / total_months * 100 if total_months > 0 else 0

        # 连续性：最长连续正/负超额月数
        streak_pos = streak_neg = max_streak = cur_streak = 0
        for v in monthly:
            if v > 0:
                cur_streak = cur_streak + 1 if cur_streak > 0 else 1
                max_streak = max(max_streak, cur_streak)
            else:
                cur_streak = 0

        cur_streak = 0
        for v in monthly:
            if v < 0:
                cur_streak = cur_streak + 1 if cur_streak > 0 else 1
            else:
                cur_streak = 0
        max_neg = max_streak  # reuse
        max_neg = 0
        cur_streak = 0
        for v in monthly:
            if v < 0:
                cur_streak = cur_streak + 1 if cur_streak > 0 else 1
                max_neg = max(max_neg, cur_streak)
            else:
                cur_streak = 0

        lines.extend([
            f"过去 {total_months} 个月中，**{positive_months} 个月**取得正超额（胜率 {win_rate:.0f}%）。",
        ])

        if win_rate >= 70:
            lines.append("🟢 Alpha 持续性**优秀**，大部分月份都在稳定创造超额收益，增强模型可靠。")
        elif win_rate >= 55:
            lines.append("🟡 Alpha 持续性**一般**，约一半月份有正贡献，存在时好时坏现象。")
        else:
            lines.append("🔴 Alpha 持续性**较差**，超额收益不稳定，可能存在撞大运嫌疑。")

        # 最近3个月趋势
        if len(monthly) >= 3:
            recent3 = monthly.tail(3)
            recent_names = [str(m) for m in recent3.index]
            recent_vals = [f"{v:+.2f}%" for v in recent3.values]
            lines.append(f"近3月超额：{' | '.join([f'{n} {v}' for n, v in zip(recent_names, recent_vals)])}")

    except Exception:
        lines.append("Alpha 持续性分析计算异常。")

    return lines


def _assess_industry_concentration(conc: dict, index_name: str) -> str:
    """评估行业集中度（基于权重分布推断）"""
    top5_sum = conc.get("top5_sum", 0)
    top10_sum = conc.get("top10_sum", 0)
    total = conc.get("total_count", 0)
    hhi = conc.get("hhi", 0)

    if not total:
        return "行业集中度数据暂不可用。"

    lines = []

    # 基于 HHI 判断集中度
    if hhi > 800:
        lines.append(f"🔴 **高度集中**（HHI = {hhi:.0f}）：指数成份股权重分布极为不均，")
        lines.append("少数巨头对指数走势有决定性影响，实质上已成为头部股票指数。")
    elif hhi > 500:
        lines.append(f"🟡 **中度集中**（HHI = {hhi:.0f}）：前十大占比 {top10_sum:.1f}%，")
        lines.append("头部公司有较大影响力，需关注单一行业风险暴露。")
    else:
        lines.append(f"🟢 **分散合理**（HHI = {hhi:.0f}）：权重分布较为均匀，")
        lines.append(f"前十大占比仅 {top10_sum:.1f}%，单一成份股影响有限。")

    # 宽基 vs 赛道提醒
    broad_keywords = ["沪深300", "中证500", "中证1000", "上证50", "创业板指", "科创50", "全指"]
    is_broad = any(kw in index_name for kw in broad_keywords)

    if is_broad and top10_sum > 50:
        lines.append(
            f"\n⚠️ 作为**宽基指数**，前十大占比 {top10_sum:.1f}% 偏高，"
            f"需警惕行业集中风险。建议查看该指数前三大行业权重，"
            f"若某一行业占比超过 40%，则该指数实际上更接近赛道指数。"
        )

    return "\n".join(lines)


def _build_passive_advice(
    pe_zone: str, pe_pct: float, pb_zone: str,
    is_etf: bool, is_liquid: bool, scale_warning: str,
    te: float, fund_name: str, index_name: str,
) -> str:
    """构建被动型基金投资建议"""
    lines = [
        "### 💡 投资建议 — 被动跟踪型策略",
        "",
    ]

    # 估值驱动的买卖建议
    if pe_zone in ("极度低估", "低估"):
        lines.extend([
            f"🟢 **当前估值（{pe_zone}）是较好的定投窗口。**",
            f"{index_name} 处于历史 {pe_pct:.0f}% 分位，安全边际较高。",
            "",
        ])
        if is_etf:
            if is_liquid:
                lines.append("**操作建议**：可通过场内ETF分批建仓，利用低估值区间积累筹码。")
            else:
                lines.append(
                    "**操作建议**：该ETF流动性偏弱，建议通过场外联接基金申赎，"
                    "避免场内大额交易产生滑点。"
                )
        else:
            lines.append("**操作建议**：通过基金公司官网或第三方平台定投，享受自动扣款和低成本优势。")
    elif pe_zone in ("极度高估", "高估"):
        lines.extend([
            f"🔴 **当前估值（{pe_zone}）建议谨慎。**",
            f"{index_name} 处于历史 {pe_pct:.0f}% 分位，追高风险较大。",
            "",
            "**操作建议**：已持有者可继续持有但暂停定投，等待估值回归；未持有者建议观望。",
        ])
    else:
        lines.extend([
            f"🟡 **当前估值（{pe_zone}）处于中性区间。**",
            f"{index_name} 处于历史 {pe_pct:.0f}% 分位。",
            "",
            "**操作建议**：适合长期定投策略，不择时、不追涨、不杀跌，让时间复利发挥作用。",
        ])

    # 费率提醒
    lines.extend([
        "",
        "### 📌 选购要点",
        "- 优先选择 TER 最低的同类基金（被动型核心就是省成本）",
    ])

    if te > 0.005:
        lines.append(f"- ⚠️ 该基金跟踪误差 {te:.4f}% 偏高，建议对比同类中 TE 更低的产品")

    if scale_warning:
        lines.append("- ⚠️ 规模低于清盘红线，不建议新资金进入")

    if is_etf and not is_liquid:
        lines.append("- ⚠️ ETF 流动性不足，优先考虑场外联接基金")

    return "\n".join(lines)


def _build_enhanced_advice(
    ir: float, te: float, excess: float,
    pe_zone: str, pe_pct_val: float,
    fund_name: str, index_name: str, ter: float,
) -> str:
    """构建增强型基金投资建议"""
    lines = [
        "### 💡 投资建议 — 指数增强型策略",
        "",
    ]

    # 增强效果评估
    if ir >= 1.5 and excess > 5:
        lines.extend([
            "🟢 **增强策略验证有效，值得配置。**",
            f"信息比率 {ir:.2f}（优秀），累计超额 {excess:+.2f}%，",
            "基金经理展现出持续的正 Alpha 能力。",
            "",
            "**操作建议**：可替代同标的被动型基金，享受超额收益。",
        ])
    elif ir >= 0.5 and excess > 0:
        lines.extend([
            "🟡 **增强策略有一定效果，但不够惊艳。**",
            f"信息比率 {ir:.2f}（良好），累计超额 {excess:+.2f}%。",
            "",
            f"**操作建议**：如费率溢价不大（当前 TER {ter*100:.2f}%），可考虑配置；",
            "若更看重确定性，被动型可能更省心。",
        ])
    elif ir >= 0 and excess >= 0:
        lines.extend([
            "🟠 **增强效果微弱，性价比存疑。**",
            f"信息比率 {ir:.2f}，累计超额仅 {excess:+.2f}%，可能无法覆盖额外费率。",
            "",
            "**操作建议**：建议对比同类被动型基金，评估多出的费率是否值得。",
        ])
    else:
        lines.extend([
            "🔴 **增强策略失效，跑输基准。**",
            f"累计超额 {excess:+.2f}%，IR={ir:.2f}，增强操作反而拖累收益。",
            "",
            "**操作建议**：建议赎回或转换为同标的被动型基金。",
        ])

    # 估值建议
    lines.append("")
    if pe_zone in ("极度低估", "低估"):
        lines.append(
            f"📊 当前 {index_name} 处于 {pe_zone}（{pe_pct_val:.0f}% 分位），"
            f"估值较低，增强型基金在此区间有更大的超额发挥空间。"
        )
    elif pe_zone in ("极度高估", "高估"):
        lines.append(
            f"📊 当前 {index_name} 处于 {pe_zone}（{pe_pct_val:.0f}% 分位），"
            f"高估值环境下增强型基金的 Alpha 策略同样面临回撤风险。"
        )

    # 监控指标
    lines.extend([
        "",
        "### 📌 持仓监控清单",
        f"- **IR 走势**：持续低于 0.3 需警惕策略失效",
        f"- **跟踪误差**：超过 2% 说明基金经理可能偏离了指数约束",
        f"- **超额收益回撤**：阶段性跑输基准属正常，但连续6个月负 Alpha 需关注",
    ])

    return "\n".join(lines)


def _fallback_report(basic: Any) -> dict:
    """数据不足时的回退报告"""
    name = basic.name if basic else "未知基金"
    return {
        "meta": {"fund_name": name, "subtype": "未知", "is_etf": False},
        "headline": f"## 📊 {name} — 数据不足",
        "section1": "数据不足，无法生成深度分析报告。请稍后重试。",
        "section2": "",
        "section3": "",
        "section4": "",
        "section5": "",
        "full_text": f"{name} 数据不足，无法生成深度分析。",
        "chart_data": {},
    }
