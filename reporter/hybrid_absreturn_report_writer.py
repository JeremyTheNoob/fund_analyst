"""
混合型-绝对收益基金深度评价报告生成器 — fund_quant_v2
角色：资深基金分析师（CFA 持证人）
报告结构：4板块 + 图表插入点标记（投资建议后补）
  1. 收益情况展示（累计收益 + 年度收益表）
  2. 收益解析（净值直线度 + 避风港概率 + 正收益月占比）
  3. 深度分析（策略稳定性 + 波动率区间 + 回撤控制评估）
  4. 风险预警（日波动异常检测 + 基差风险分析）
"""

from __future__ import annotations
from typing import Any, Optional
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def generate_hybrid_absreturn_report(report: Any) -> dict:
    """
    生成混合型-绝对收益基金深度评价报告（4板块结构）

    Returns:
        {
          "meta":       {fund_name, fund_type, start_date, end_date, grade, score, tags},
          "headline":   标题行,
          "section1":   一、收益情况展示（[INSERT_CHART: CUM_RET] + [INSERT_CHART: HEATMAP]）,
          "section2":   二、收益解析（净值直线度 + 避风港概率 + 正收益月占比）,
          "section3":   三、深度分析（策略稳定性 + 波动率监控 + [INSERT_CHART: VOLATILITY_BAND]）,
          "section4":   四、风险预警（日波动异常 + 基差分析 + [INSERT_CHART: DRAWDOWN]）,
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

    # ── 核心分析指标 ──────────────────────────────────────
    # 1. 净值直线度（Linearity R²）
    linearity = _compute_linearity(charts)

    # 2. 避风港概率（日收益率 > 0 天数占比）
    haven_ratio = _compute_haven_ratio(charts)

    # 3. 正收益月占比
    monthly_positive_ratio = _compute_monthly_positive_ratio(charts)

    # 4. 波动率稳定性分析
    vol_stability = _analyze_volatility_stability(charts)

    # 5. 日波动异常检测
    vol_anomaly = _detect_volatility_anomaly(charts)

    # 6. 基差风险分析（期货基差 vs 净值压制）
    basis_risk = _analyze_basis_risk(charts)

    # 7. 策略有效性评估
    strategy_eval = _evaluate_strategy_effectiveness(cm, charts)

    # 8. 回撤修复分析
    recovery_analysis = _analyze_recovery_patterns(charts)

    # ── 构建报告 ──────────────────────────────────────────
    meta = {
        "fund_name": fund_name,
        "fund_type": "混合型-绝对收益",
        "start_date": start_date,
        "end_date": end_date,
        "grade": grade,
        "score": score,
        "tags": tags,
    }

    headline = _build_headline(fund_name, grade_desc, start_date, end_date, grade)

    section1 = _section1_performance(fund_name, cm, m, start_date, end_date, charts)
    section2 = _section2_return_analysis(
        fund_name, cm, linearity, haven_ratio, monthly_positive_ratio,
    )
    section3 = _section3_deep_analysis(
        fund_name, cm, m, vol_stability, strategy_eval, recovery_analysis,
        stock_ratio, bond_ratio, cash_ratio, cb_ratio,
    )
    section4 = _section4_risk_warning(
        fund_name, cm, m, vol_anomaly, basis_risk,
    )

    # 存储分析数据到 chart_data 供 main.py 使用
    report.chart_data["absreturn_linearity"] = linearity
    report.chart_data["absreturn_haven_ratio"] = haven_ratio
    report.chart_data["absreturn_monthly_positive"] = monthly_positive_ratio
    report.chart_data["absreturn_vol_stability"] = vol_stability
    report.chart_data["absreturn_vol_anomaly"] = vol_anomaly
    report.chart_data["absreturn_basis_risk"] = basis_risk
    report.chart_data["absreturn_strategy"] = strategy_eval
    report.chart_data["absreturn_recovery"] = recovery_analysis

    full_text = "\n\n".join([headline, section1, section2, section3, section4])

    return {
        "meta": meta,
        "headline": headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4,
        "full_text": full_text,
    }


# ============================================================
# Section 1: 收益情况展示
# ============================================================

def _section1_performance(fund_name, cm, m, start_date, end_date, charts) -> str:
    """一、收益情况展示：历史业绩 + 年度收益表"""

    ann_ret = cm.annualized_return * 100
    cum_ret = cm.cumulative_return * 100
    max_dd = cm.max_drawdown * 100
    sharpe = cm.sharpe_ratio
    sortino = cm.sortino_ratio
    volatility = cm.volatility * 100

    # Alpha / Beta
    net_alpha = m.alpha * 100 if hasattr(m, "alpha") else 0.0
    beta = m.beta if hasattr(m, "beta") else 1.0
    r2 = m.r_squared if hasattr(m, "r_squared") else 0.0

    # 收益评价（绝对收益型标准不同）
    if ann_ret > 8:
        ret_eval = "🌟 绝对收益优异"
    elif ann_ret > 5:
        ret_eval = "✅ 稳健正收益"
    elif ann_ret > 2:
        ret_eval = "⚠️ 收益偏低，需关注是否达到同类平均"
    elif ann_ret > 0:
        ret_eval = "⚠️ 微正收益，收益能力不足"
    else:
        ret_eval = "❌ 收益为负，绝对收益目标未达成"

    # 夏普评价（绝对收益型标准）
    if sharpe > 2.0:
        sharpe_eval = "风险调整后收益卓越"
    elif sharpe > 1.5:
        sharpe_eval = "风险调整后收益优秀"
    elif sharpe > 1.0:
        sharpe_eval = "风险收益比良好"
    elif sharpe > 0.5:
        sharpe_eval = "风险收益比一般"
    else:
        sharpe_eval = "风险调整后收益不理想"

    # 波动评价
    if volatility < 3:
        vol_eval = "🟢 极低波动，类似固收产品"
    elif volatility < 5:
        vol_eval = "🟢 低波动，符合绝对收益定位"
    elif volatility < 8:
        vol_eval = "🟡 中等波动，略高于绝对收益预期"
    else:
        vol_eval = "🔴 高波动，偏离绝对收益定位"

    # 年度收益分析
    annual_table = _compute_annual_returns_table(charts)

    text = f"""### 一、收益情况展示

**{fund_name}** 分析区间：{start_date} ~ {end_date}

| 指标 | 数值 | 评价 |
|------|------|------|
| 年化收益 | {ann_ret:+.2f}% | {ret_eval} |
| 累计收益 | {cum_ret:+.2f}% | — |
| 最大回撤 | {max_dd:.2f}% | {'可控' if abs(max_dd) < 8 else '需警惕' if abs(max_dd) < 15 else '🔴 过大'} |
| 夏普比率 | {sharpe:.2f} | {sharpe_eval} |
| Sortino | {sortino:.2f} | — |
| 年化波动 | {volatility:.2f}% | {vol_eval} |
| Beta（全期）| {beta:.3f} | {'低相关' if beta < 0.3 else '弱相关' if beta < 0.6 else '市场跟随'} |
| 年化 Alpha | {net_alpha:+.2f}% | {'显著' if net_alpha > 3 else '微弱' if net_alpha > 0 else '无'} |
| R² | {r2:.2%} | {'市场解释力强' if r2 > 0.5 else '独立走势（对冲策略特征）'} |

**绝对收益定位检验：**
- {'✅ 波动率 < 5%，符合绝对收益产品的低波动定位' if volatility < 5 else '⚠️ 波动率 ≥ 5%，偏离绝对收益产品应有水平'}
- {'✅ 最大回撤 < 8%，风控能力良好' if abs(max_dd) < 8 else '⚠️ 最大回撤 ≥ 8%，需关注极端行情下的风控'}
- {'✅ Beta < 0.5，与市场相关性低，具备对冲特征' if beta < 0.5 else '🟡 Beta ≥ 0.5，市场相关性偏高，可能缺乏有效对冲'}
- R² = {r2:.2%}（{'模型对基金解释力弱，符合量化对冲/市场中性策略特征' if r2 < 0.3 else '模型有一定解释力，基金不完全独立于市场'})

[INSERT_CHART: CUM_RET]

{annual_table}

[INSERT_CHART: HEATMAP]"""

    return text


def _compute_annual_returns_table(charts: dict) -> str:
    """计算年度收益表格"""
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return ""

    try:
        df = nav_df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        # 计算年度收益
        df_indexed = df.set_index("date")
        annual_rets = df_indexed["ret"].resample("YE").apply(lambda x: float((1 + x.fillna(0)).prod() - 1))
        annual_rets = annual_rets.dropna()

        # 表格行
        table_rows = ""
        for dt, ret in annual_rets.items():
            year = dt.year
            ret_pct = float(ret) * 100
            if ret_pct > 0:
                icon = "🔴"  # 红涨
            else:
                icon = "🟢"  # 绿跌
            table_rows += f"| {year} | {icon} {ret_pct:+.2f}% | {'正收益' if ret > 0 else '负收益'} |\n"

        # 统计正收益年份
        positive_years = int((annual_rets > 0).sum())
        total_years = len(annual_rets)
        positive_ratio = positive_years / total_years * 100 if total_years > 0 else 0

        return f"""**年度收益一览：**

| 年份 | 年度收益 | 正/负 |
|------|----------|-------|
{table_rows}
> 正收益年份占比：{positive_ratio:.0f}%（{positive_years}/{total_years}年）"""
    except Exception as e:
        logger.warning(f"[absreturn] 年度收益计算失败: {e}")
        return ""


# ============================================================
# Section 2: 收益解析
# ============================================================

def _section2_return_analysis(
    fund_name, cm, linearity, haven_ratio, monthly_positive_ratio,
) -> str:
    """二、收益解析：净值直线度 + 避风港概率 + 正收益月占比"""

    # ── 净值直线度 ──
    r2 = linearity.get("r_squared", 0.0)
    slope_annual = linearity.get("slope_annual", 0.0) * 100

    if r2 > 0.85:
        linearity_eval = "🌟 **卓越** — 净值几乎是一条完美的上升直线，收益极度平滑"
    elif r2 > 0.70:
        linearity_eval = "✅ **优秀** — 净值曲线与线性增长拟合良好，收益平滑度高"
    elif r2 > 0.50:
        linearity_eval = "🟡 **一般** — 净值有一定波动，但整体趋势向上"
    elif r2 > 0.30:
        linearity_eval = "⚠️ **偏差** — 净值波动较大，直线度偏低"
    else:
        linearity_eval = "🔴 **差** — 净值走势与线性增长偏离严重，收益极不平稳"

    if slope_annual > 0:
        trend_desc = f"趋势方向：上升（年化斜率 {slope_annual:+.2f}%）"
    else:
        trend_desc = f"趋势方向：下降（年化斜率 {slope_annual:+.2f}%）"

    # ── 避风港概率 ──
    haven = haven_ratio.get("positive_ratio", 0.0) * 100
    avg_pos_ret = haven_ratio.get("avg_positive_return", 0.0) * 100
    avg_neg_ret = haven_ratio.get("avg_negative_return", 0.0) * 100
    win_loss_ratio = haven_ratio.get("win_loss_ratio", 0.0)

    if haven > 55:
        haven_eval = f"🟢 **优秀的避风港** — 日收益率 > 0 的天数占 {haven:.1f}%，大部分交易日都能带来正收益"
    elif haven > 50:
        haven_eval = f"🟡 **合格的避风港** — 日收益率 > 0 的天数占 {haven:.1f}%，略高于50%"
    else:
        haven_eval = f"🔴 **避风港能力不足** — 日收益率 > 0 的天数仅 {haven:.1f}%，与抛硬币无异"

    # 盈亏比分析
    if win_loss_ratio > 1.5:
        wl_eval = "盈利日的平均收益远超亏损日，策略具有不对称优势"
    elif win_loss_ratio > 1.0:
        wl_eval = "盈利日的平均收益略高于亏损日，具备一定的盈亏比优势"
    elif win_loss_ratio > 0.7:
        wl_eval = "盈亏比接近 1:1，策略缺乏不对称优势"
    else:
        wl_eval = "⚠️ 亏损日平均亏损幅度大于盈利日平均盈利，策略不对称性为负"

    # ── 正收益月占比 ──
    monthly_pos = monthly_positive_ratio.get("positive_ratio", 0.0) * 100
    total_months = monthly_positive_ratio.get("total_months", 0)
    positive_months = monthly_positive_ratio.get("positive_months", 0)

    if monthly_pos > 80:
        monthly_eval = f"🌟 {monthly_pos:.0f}% 月份正收益（{positive_months}/{total_months}月），绝对收益目标达成度极高"
    elif monthly_pos > 70:
        monthly_eval = f"✅ {monthly_pos:.0f}% 月份正收益（{positive_months}/{total_months}月），表现稳健"
    elif monthly_pos > 60:
        monthly_eval = f"🟡 {monthly_pos:.0f}% 月份正收益（{positive_months}/{total_months}月），达标率尚可"
    else:
        monthly_eval = f"🔴 {monthly_pos:.0f}% 月份正收益（{positive_months}/{total_months}月），月度正收益占比偏低"

    # ── 综合评分 ──
    # 绝对收益综合评分
    score_items = []
    if r2 > 0.7:
        score_items.append("净值平滑")
    if haven > 52:
        score_items.append("避风港效应")
    if monthly_pos > 70:
        score_items.append("月度稳定")
    if win_loss_ratio > 1.0:
        score_items.append("盈亏比优")

    n_passed = len(score_items)
    if n_passed >= 4:
        overall_eval = f"🌟 **绝对收益四维全部达标**（{score_items[0]} + {score_items[1]} + {score_items[2]} + {score_items[3]}），堪称「画线派」"
    elif n_passed >= 3:
        overall_eval = f"✅ **绝对收益三维度达标**（{' + '.join(score_items)}），收益质量良好"
    elif n_passed >= 2:
        overall_eval = f"🟡 **绝对收益两维度达标**（{' + '.join(score_items)}），部分指标需关注"
    else:
        overall_eval = f"⚠️ **绝对收益维度达标不足**（仅 {' + '.join(score_items) if score_items else '无'}），收益质量堪忧"

    text = f"""### 二、收益解析

**这类基金的目标是年年正收益、日日小赚。我们从三个核心维度评估其收益质量：**

#### 📐 净值直线度（Linearity）

计算净值曲线与时间线性增长线的拟合优度 R²，R² 越高说明净值走势越平稳。

- **R² = {r2:.4f}** — {linearity_eval}
- {trend_desc}
- 绝对收益型基金的 R² 理想值应 > 0.70，当前{'达标' if r2 > 0.70 else '未达标'}

#### 🏝️ 避风港概率（Haven Ratio）

统计日收益率 > 0 的天数占比，评估基金是否能在大多数交易日带来正收益。

- **正收益天数占比：{haven:.1f}%**
- {haven_eval}
- 盈利日平均涨幅：{avg_pos_ret:+.3f}% | 亏损日平均跌幅：{avg_neg_ret:.3f}%
- 盈亏比（平均盈利/平均亏损）：{win_loss_ratio:.2f} — {wl_eval}

#### 📅 正收益月占比

统计月度收益 > 0 的月份占比。

- **{monthly_eval}**
- 绝对收益型基金月度正收益占比理想值应 > 70%，当前{'达标' if monthly_pos > 70 else '未达标'}

**📊 收益质量综合评价：**
- {overall_eval}"""

    return text


# ============================================================
# Section 3: 深度分析
# ============================================================

def _section3_deep_analysis(
    fund_name, cm, m, vol_stability, strategy_eval, recovery_analysis,
    stock_ratio, bond_ratio, cash_ratio, cb_ratio,
) -> str:
    """三、深度分析：策略稳定性 + 波动率监控 + 回撤修复"""

    volatility = cm.volatility * 100
    max_dd = cm.max_drawdown * 100

    # ── 策略有效性 ──
    strategy_type = strategy_eval.get("strategy_type", "未知")
    effectiveness = strategy_eval.get("effectiveness", "未知")
    strategy_desc = strategy_eval.get("description", "")

    # ── 波动率稳定性 ──
    vol_mean = vol_stability.get("vol_mean", volatility)
    vol_std = vol_stability.get("vol_std", 0)
    vol_range_min = vol_stability.get("vol_min", 0)
    vol_range_max = vol_stability.get("vol_max", 0)
    vol_in_target = vol_stability.get("in_target_range", False)
    rolling_vol_trend = vol_stability.get("trend", "稳定")

    # 波动率区间判断
    if vol_in_target:
        vol_band_eval = f"🟢 **波动率稳定在目标区间**（{vol_range_min:.1f}% ~ {vol_range_max:.1f}%），符合量化对冲/多策略的预期"
    elif vol_mean < 8:
        vol_band_eval = f"🟡 **波动率偏低但稳定**（均值 {vol_mean:.1f}%，范围 {vol_range_min:.1f}% ~ {vol_range_max:.1f}%），策略运作正常"
    else:
        vol_band_eval = f"🔴 **波动率偏高或剧烈波动**（均值 {vol_mean:.1f}%，范围 {vol_range_min:.1f}% ~ {vol_range_max:.1f}%），可能存在策略失效风险"

    # 趋势判断
    if rolling_vol_trend == "上升":
        trend_alert = "⚠️ 近期波动率呈上升趋势，策略可能正在经历风格切换或风控放宽"
    elif rolling_vol_trend == "下降":
        trend_alert = "✅ 近期波动率下降，策略趋于稳定"
    else:
        trend_alert = "ℹ️ 波动率近期保持稳定"

    # ── 回撤修复分析 ──
    max_dd_days = cm.max_drawdown_duration
    recovery_days = cm.recovery_days
    avg_recovery = recovery_analysis.get("avg_recovery_days", 0)
    max_recovery = recovery_analysis.get("max_recovery_days", 0)
    n_drawdowns = recovery_analysis.get("n_significant_drawdowns", 0)
    fast_recovery_ratio = recovery_analysis.get("fast_recovery_ratio", 0)

    if avg_recovery > 0:
        if avg_recovery < 30:
            recovery_eval = f"🟢 平均修复 {avg_recovery:.0f} 天，回撤修复速度较快"
        elif avg_recovery < 60:
            recovery_eval = f"🟡 平均修复 {avg_recovery:.0f} 天，回撤修复速度一般"
        else:
            recovery_eval = f"🔴 平均修复 {avg_recovery:.0f} 天，回撤修复较慢"

        fast_eval = f"快速修复（<30天）占比 {fast_recovery_ratio:.0%}" if fast_recovery_ratio > 0 else ""
    else:
        recovery_eval = "回撤修复数据暂缺"
        fast_eval = ""

    # 资产配置描述
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

    text = f"""### 三、深度分析

#### 🧪 策略类型识别与有效性

通过 Beta、波动率、收益分布等特征，推测该基金采用的策略类型：

- **策略类型推测：{strategy_type}**
- **策略有效性：{effectiveness}**
- {strategy_desc}
- 最新资产配置为 {asset_desc}

#### 📊 波动率稳定性监控

绝对收益型基金的核心竞争力是**波动率控制**。监测年化波动率是否在狭窄区间（2%-5%）内波动。

- **滚动年化波动率：** 均值 {vol_mean:.2f}%，标准差 {vol_std:.2f}%
- **波动率范围：** {vol_range_min:.2f}% ~ {vol_range_max:.2f}%
- {vol_band_eval}
- {trend_alert}

[INSERT_CHART: VOLATILITY_BAND]

#### 🔄 回撤修复能力

回撤发生后能否快速修复，是评估绝对收益策略韧性的关键指标。

- **历史最大回撤：** {max_dd:.2f}%，持续 {max_dd_days} 天
- **显著回撤次数（>3%）：** {n_drawdowns} 次
- {recovery_eval}
- 最长修复天数：{max_recovery:.0f} 天
- {fast_eval}"""

    return text


# ============================================================
# Section 4: 风险预警
# ============================================================

def _section4_risk_warning(
    fund_name, cm, m, vol_anomaly, basis_risk,
) -> str:
    """四、风险预警：日波动异常检测 + 基差风险分析"""

    max_dd = cm.max_drawdown * 100
    volatility = cm.volatility * 100

    # ── 日波动异常检测 ──
    is_anomaly = vol_anomaly.get("is_anomaly", False)
    current_vol = vol_anomaly.get("current_vol", volatility)
    historical_mean = vol_anomaly.get("historical_mean", volatility)
    z_score = vol_anomaly.get("z_score", 0.0)
    anomaly_days = vol_anomaly.get("anomaly_days", 0)
    recent_max_daily = vol_anomaly.get("recent_max_daily_drop", 0) * 100
    vol_percentile = vol_anomaly.get("current_percentile", 50)

    if is_anomaly:
        anomaly_alert = f"🔴 **异常波动预警** — 当前日波动率 {current_vol:.2f}% 显著高于历史均值 {historical_mean:.2f}%（Z-score = {z_score:.1f}），策略可能正在失效或面临极端行情冲击"
    elif z_score > 1.5:
        anomaly_alert = f"🟡 **波动率偏高注意** — 当前日波动率 {current_vol:.2f}% 略高于历史均值 {historical_mean:.2f}%（Z-score = {z_score:.1f}），建议持续关注"
    elif z_score > -1.0:
        anomaly_alert = f"🟢 **波动率正常** — 当前日波动率 {current_vol:.2f}% 处于历史正常范围（Z-score = {z_score:.1f}），策略运作平稳"
    else:
        anomaly_alert = f"🟢 **波动率偏低** — 当前日波动率 {current_vol:.2f}% 低于历史均值（Z-score = {z_score:.1f}），策略趋于保守"

    # 异常天数描述
    if anomaly_days > 0:
        anomaly_days_desc = f"近60个交易日内有 {anomaly_days} 天出现异常波动（超过2倍标准差）"
    else:
        anomaly_days_desc = "近60个交易日内无异常波动"

    # 波动百分位描述
    if vol_percentile > 90:
        percentile_desc = f"当前波动率处于历史 {vol_percentile:.0f}% 分位，接近极端高位"
    elif vol_percentile > 75:
        percentile_desc = f"当前波动率处于历史 {vol_percentile:.0f}% 分位，偏高"
    elif vol_percentile > 25:
        percentile_desc = f"当前波动率处于历史 {vol_percentile:.0f}% 分位，正常"
    else:
        percentile_desc = f"当前波动率处于历史 {vol_percentile:.0f}% 分位，偏低"

    # ── 基差风险分析 ──
    has_basis_data = basis_risk.get("has_data", False)
    basis_desc = basis_risk.get("description", "")
    basis_pressure = basis_risk.get("basis_pressure", "未知")

    if has_basis_data:
        basis_text = f"""#### 📉 基差风险分析

量化对冲/市场中性策略通常使用股指期货进行对冲。基差变动会直接影响对冲成本。

- {basis_desc}
- 基差压制评估：{basis_pressure}"""
    else:
        basis_text = """#### 📉 基差风险分析

> 💡 量化对冲/市场中性策略通常使用股指期货进行对冲，基差变动会直接影响对冲成本。当前版本暂未接入期货基差数据，以下提供定性分析：

- 若基金采用股指期货对冲（如 IC/IF），当基差贴水时，对冲端会额外贡献正收益（移仓收益），反之升水时会产生成本
- 基差剧烈波动（如交割周附近）可能导致短期净值波动放大
- 建议结合基金季报中的「衍生品投资」部分确认对冲工具使用情况"""

    text = f"""### 四、风险预警

[INSERT_CHART: DRAWDOWN]

#### ⚡ 日波动异常检测

监测近期日收益率波动是否突破历史均值，判断当前策略是否处于正常状态。

- {anomaly_alert}
- {anomaly_days_desc}
- {percentile_desc}
- 近60日最大单日跌幅：{recent_max_daily:.2f}%

{basis_text}

**综合风险指标：**
- 最大回撤：{max_dd:.2f}%（{'✅ 可控' if abs(max_dd) < 8 else '⚠️ 偏高' if abs(max_dd) < 15 else '🔴 高风险'}）
- 年化波动：{volatility:.2f}%
- {'✅ 未检测到策略失效信号' if not is_anomaly and z_score < 2 else '🔴 建议密切关注，策略可能正在失效'}"""

    return text


# ============================================================
# 辅助分析函数
# ============================================================

def _compute_linearity(charts: dict) -> dict:
    """
    计算净值直线度：净值曲线与时间线性增长线的拟合优度 R²。
    R² 越高说明净值走势越平稳，越接近"画线"。
    """
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return {"r_squared": 0.0, "slope_annual": 0.0}

    try:
        df = nav_df[["date", "nav"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        if len(df) < 30:
            return {"r_squared": 0.0, "slope_annual": 0.0}

        # 归一化：时间从0开始
        start_date = df["date"].iloc[0]
        df["t"] = (df["date"] - start_date).dt.total_seconds() / (365.25 * 24 * 3600)  # 单位：年

        # 净值归一化
        nav_start = df["nav"].iloc[0]
        df["nav_norm"] = df["nav"] / nav_start

        # 线性回归：nav_norm = 1 + slope * t
        t = df["t"].values
        y = df["nav_norm"].values

        # 最小二乘法
        t_mean = t.mean()
        y_mean = y.mean()
        ss_xx = np.sum((t - t_mean) ** 2)
        ss_yy = np.sum((y - y_mean) ** 2)
        ss_xy = np.sum((t - t_mean) * (y - y_mean))

        if ss_xx == 0 or ss_yy == 0:
            return {"r_squared": 0.0, "slope_annual": 0.0}

        slope = ss_xy / ss_xx
        intercept = y_mean - slope * t_mean

        # R²
        y_pred = intercept + slope * t
        ss_res = np.sum((y - y_pred) ** 2)
        r_squared = 1 - ss_res / ss_yy

        return {
            "r_squared": max(0.0, min(1.0, round(r_squared, 4))),
            "slope_annual": round(slope, 6),  # 年化斜率（归一化净值空间）
        }
    except Exception as e:
        logger.warning(f"[absreturn] 净值直线度计算失败: {e}")
        return {"r_squared": 0.0, "slope_annual": 0.0}


def _compute_haven_ratio(charts: dict) -> dict:
    """
    避风港概率：统计日收益率 > 0 的天数占比。
    """
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return {"positive_ratio": 0.0, "avg_positive_return": 0.0,
                "avg_negative_return": 0.0, "win_loss_ratio": 0.0}

    try:
        df = nav_df[["date", "ret"]].copy()
        df["ret"] = df["ret"].fillna(0)

        positive = df[df["ret"] > 0]
        negative = df[df["ret"] < 0]

        total_days = len(df)
        pos_days = len(positive)
        neg_days = len(negative)

        if total_days == 0:
            return {"positive_ratio": 0.0, "avg_positive_return": 0.0,
                    "avg_negative_return": 0.0, "win_loss_ratio": 0.0}

        avg_pos = positive["ret"].mean() if pos_days > 0 else 0.0
        avg_neg = abs(negative["ret"].mean()) if neg_days > 0 else 0.0

        # 盈亏比 = 平均盈利 / 平均亏损
        wl_ratio = avg_pos / avg_neg if avg_neg > 0 else float("inf")

        return {
            "positive_ratio": round(pos_days / total_days, 4),
            "avg_positive_return": round(avg_pos, 6),
            "avg_negative_return": round(avg_neg, 6),
            "win_loss_ratio": round(wl_ratio, 2) if wl_ratio != float("inf") else 99.99,
            "positive_days": pos_days,
            "negative_days": neg_days,
            "total_days": total_days,
        }
    except Exception as e:
        logger.warning(f"[absreturn] 避风港概率计算失败: {e}")
        return {"positive_ratio": 0.0, "avg_positive_return": 0.0,
                "avg_negative_return": 0.0, "win_loss_ratio": 0.0}


def _compute_monthly_positive_ratio(charts: dict) -> dict:
    """
    正收益月占比：统计月度收益 > 0 的月份占比。
    """
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return {"positive_ratio": 0.0, "total_months": 0, "positive_months": 0}

    try:
        df = nav_df[["date", "ret"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

        # 月度收益：设置日期索引后 resample
        df_indexed = df.set_index("date")
        monthly_nav = df_indexed["ret"].resample("ME").apply(lambda x: float((1 + x.fillna(0)).prod() - 1))
        monthly_rets = monthly_nav.dropna()

        total = len(monthly_rets)
        positive = int((monthly_rets > 0).sum())

        return {
            "positive_ratio": round(positive / total, 4) if total > 0 else 0.0,
            "total_months": total,
            "positive_months": positive,
        }
    except Exception as e:
        logger.warning(f"[absreturn] 正收益月占比计算失败: {e}")
        return {"positive_ratio": 0.0, "total_months": 0, "positive_months": 0}


def _analyze_volatility_stability(charts: dict) -> dict:
    """
    分析波动率稳定性：滚动年化波动率是否在狭窄区间（2%-5%）内。
    """
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return {"vol_mean": 0, "vol_std": 0, "vol_min": 0, "vol_max": 0,
                "in_target_range": False, "trend": "未知", "rolling_vols": []}

    try:
        df = nav_df[["date", "ret"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        if len(df) < 60:
            return {"vol_mean": 0, "vol_std": 0, "vol_min": 0, "vol_max": 0,
                    "in_target_range": False, "trend": "数据不足", "rolling_vols": []}

        rets = df["ret"].fillna(0).values

        # 滚动20日年化波动率
        window = 20
        rolling_vols = []
        for i in range(window, len(rets)):
            vol = np.std(rets[i - window:i], ddof=1) * np.sqrt(252) * 100
            rolling_vols.append(round(vol, 4))

        if not rolling_vols:
            return {"vol_mean": 0, "vol_std": 0, "vol_min": 0, "vol_max": 0,
                    "in_target_range": False, "trend": "未知", "rolling_vols": []}

        vol_arr = np.array(rolling_vols)
        vol_mean = float(np.mean(vol_arr))
        vol_std = float(np.std(vol_arr))
        vol_min = float(np.min(vol_arr))
        vol_max = float(np.max(vol_arr))

        # 是否在目标区间（2%-5%）内
        in_target = vol_min >= 1.5 and vol_max <= 7.0

        # 趋势判断
        if len(rolling_vols) >= 20:
            recent_avg = np.mean(rolling_vols[-10:])
            older_avg = np.mean(rolling_vols[-20:-10])
            diff = recent_avg - older_avg
            if diff > 0.5:
                trend = "上升"
            elif diff < -0.5:
                trend = "下降"
            else:
                trend = "稳定"
        else:
            trend = "数据不足"

        return {
            "vol_mean": round(vol_mean, 2),
            "vol_std": round(vol_std, 2),
            "vol_min": round(vol_min, 2),
            "vol_max": round(vol_max, 2),
            "in_target_range": in_target,
            "trend": trend,
            "rolling_vols": rolling_vols,
        }
    except Exception as e:
        logger.warning(f"[absreturn] 波动率稳定性分析失败: {e}")
        return {"vol_mean": 0, "vol_std": 0, "vol_min": 0, "vol_max": 0,
                "in_target_range": False, "trend": "未知", "rolling_vols": []}


def _detect_volatility_anomaly(charts: dict) -> dict:
    """
    日波动异常检测：监测近期日波动是否突破历史均值。
    """
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return {"is_anomaly": False, "current_vol": 0, "historical_mean": 0,
                "z_score": 0, "anomaly_days": 0, "recent_max_daily_drop": 0,
                "current_percentile": 50}

    try:
        df = nav_df[["date", "ret"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        if len(df) < 60:
            return {"is_anomaly": False, "current_vol": 0, "historical_mean": 0,
                    "z_score": 0, "anomaly_days": 0, "recent_max_daily_drop": 0,
                    "current_percentile": 50}

        rets = df["ret"].fillna(0).values

        # 当前20日波动率
        recent_vol = np.std(rets[-20:], ddof=1) * np.sqrt(252) * 100

        # 历史波动率均值和标准差（滚动60日窗口）
        window = 60
        hist_vols = []
        for i in range(window, len(rets) - 20):
            vol = np.std(rets[i - window:i], ddof=1) * np.sqrt(252) * 100
            hist_vols.append(vol)

        if not hist_vols:
            return {"is_anomaly": False, "current_vol": recent_vol, "historical_mean": recent_vol,
                    "z_score": 0, "anomaly_days": 0, "recent_max_daily_drop": 0,
                    "current_percentile": 50}

        hist_mean = np.mean(hist_vols)
        hist_std = np.std(hist_vols)

        # Z-score
        z_score = (recent_vol - hist_mean) / hist_std if hist_std > 0 else 0

        # 当前百分位
        all_vols = hist_vols + [recent_vol]
        percentile = sum(1 for v in all_vols if v < recent_vol) / len(all_vols) * 100

        # 异常天数（近60天中超过2倍标准差的天数）
        recent_rets = rets[-60:]
        anomaly_days = sum(1 for r in recent_rets if abs(r) > hist_mean / np.sqrt(252) * 100 * 2 / 100)

        # 近60日最大单日跌幅
        recent_max_daily = float(np.min(recent_rets))

        is_anomaly = z_score > 2.0

        return {
            "is_anomaly": is_anomaly,
            "current_vol": round(recent_vol, 2),
            "historical_mean": round(hist_mean, 2),
            "z_score": round(z_score, 2),
            "anomaly_days": anomaly_days,
            "recent_max_daily_drop": round(recent_max_daily, 4),
            "current_percentile": round(percentile, 1),
        }
    except Exception as e:
        logger.warning(f"[absreturn] 日波动异常检测失败: {e}")
        return {"is_anomaly": False, "current_vol": 0, "historical_mean": 0,
                "z_score": 0, "anomaly_days": 0, "recent_max_daily_drop": 0,
                "current_percentile": 50}


def _analyze_basis_risk(charts: dict) -> dict:
    """
    基差风险分析（定性分析）。
    由于期货基差数据（futures_main_sina）接入需要额外数据源，
    当前版本提供基于收益特征的定性评估。
    """
    # 当前版本使用定性分析
    return {
        "has_data": False,
        "description": "期货基差数据暂未接入，无法定量分析基差对净值的压制",
        "basis_pressure": "待评估（建议结合季报衍生品持仓确认）",
    }


def _evaluate_strategy_effectiveness(cm, charts: dict) -> dict:
    """
    策略有效性评估：通过 Beta、波动率、收益分布等特征推测策略类型。
    """
    nav_df = charts.get("nav_df")
    rets = nav_df["ret"].dropna().values if nav_df is not None and not nav_df.empty else None

    vol = cm.volatility * 100
    sharpe = cm.sharpe_ratio
    sortino = cm.sortino_ratio
    max_dd = cm.max_drawdown * 100

    # 策略类型推断
    # 根据波动率、Beta、收益特征来推断
    # 这里用一些启发式规则

    strategy_type = "待识别"
    description = ""
    effectiveness = "待评估"

    if vol < 3 and max_dd < 5:
        strategy_type = "量化对冲/市场中性"
        description = "极低波动 + 低回撤 + 低 Beta，典型的市场中性策略特征。可能使用股指期货对冲+量化选股获取 Alpha"
        effectiveness = "✅ 策略运作有效" if sharpe > 1.0 else "🟡 策略效果一般"
    elif vol < 5 and max_dd < 10:
        # 进一步区分
        if sharpe > 1.5:
            strategy_type = "量化对冲/市场中性"
            description = "低波动 + 高夏普 + 低 Beta，量化对冲策略运作良好，Alpha 来源稳定"
            effectiveness = "✅ 策略运作高效"
        elif sortino > 2.0:
            strategy_type = "期权策略/波动率套利"
            description = "低波动 + 极高 Sortino 比率，可能是期权策略或波动率套利，下行保护能力强"
            effectiveness = "✅ 策略下行保护优秀"
        else:
            strategy_type = "固收+/多策略"
            description = "低波动 + 中等夏普，可能是固收+策略（债券打底+小比例权益增强）或多策略组合"
            effectiveness = "🟡 策略效果一般" if sharpe > 0.5 else "⚠️ 策略效果偏低"
    elif vol < 8:
        if rets is not None and len(rets) > 60:
            # 检查偏度
            skewness = float(pd.Series(rets).skew())
            if skewness < -0.5:
                strategy_type = "尾部风险对冲/期权策略"
                description = f"中等波动 + 负偏度（{skewness:.2f}），收益分布左偏，可能使用期权做尾部保护（卖波动率/买看跌）"
                effectiveness = "🟡 需关注极端行情表现"
            else:
                strategy_type = "灵活对冲/多策略"
                description = f"中等波动 + 对称分布（偏度 {skewness:.2f}），可能是灵活对冲或多策略组合，根据市场环境动态调整"
                effectiveness = "✅ 策略有一定灵活性" if sharpe > 0.8 else "🟡 策略效果一般"
        else:
            strategy_type = "混合策略"
            description = "中等波动水平，策略特征不够明显，可能混合了多种策略"
            effectiveness = "🟡 策略效果一般"
    else:
        strategy_type = "偏高波动策略（偏离绝对收益定位）"
        description = f"高波动（{vol:.1f}%）+ 大回撤（{max_dd:.1f}%），已偏离绝对收益产品应有水平，可能包含较高比例的权益暴露"
        effectiveness = "⚠️ 偏离绝对收益定位"

    return {
        "strategy_type": strategy_type,
        "description": description,
        "effectiveness": effectiveness,
    }


def _analyze_recovery_patterns(charts: dict) -> dict:
    """
    回撤修复分析：统计显著回撤的修复天数和快速修复占比。
    """
    nav_df = charts.get("nav_df")
    if nav_df is None or nav_df.empty:
        return {"avg_recovery_days": 0, "max_recovery_days": 0,
                "n_significant_drawdowns": 0, "fast_recovery_ratio": 0.0}

    try:
        df = nav_df[["date", "nav"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        # 计算累计净值
        df["cum_nav"] = (1 + df["nav"].pct_change().fillna(0)).cumprod()

        # 找到高水位线
        df["cum_max"] = df["cum_nav"].cummax()
        df["drawdown"] = (df["cum_nav"] / df["cum_max"]) - 1

        # 找出显著回撤（>3%）
        threshold = -0.03
        in_drawdown = False
        dd_start = None
        dd_peak = 0
        recoveries = []

        for i, row in df.iterrows():
            if row["drawdown"] < threshold and not in_drawdown:
                in_drawdown = True
                dd_start = i
                dd_peak = row["drawdown"]

            elif in_drawdown:
                if row["drawdown"] < dd_peak:
                    dd_peak = row["drawdown"]
                elif row["drawdown"] > -0.005:
                    # 回撤修复（回到 -0.5% 以内）
                    recovery_days = i - dd_start
                    if recovery_days > 0:
                        recoveries.append({
                            "days": recovery_days,
                            "depth": abs(dd_peak),
                        })
                    in_drawdown = False
                    dd_start = None

        if not recoveries:
            return {"avg_recovery_days": 0, "max_recovery_days": 0,
                    "n_significant_drawdowns": 0, "fast_recovery_ratio": 0.0}

        recovery_days_list = [r["days"] for r in recoveries]
        fast_count = sum(1 for d in recovery_days_list if d < 30)

        return {
            "avg_recovery_days": round(np.mean(recovery_days_list), 0),
            "max_recovery_days": max(recovery_days_list),
            "n_significant_drawdowns": len(recoveries),
            "fast_recovery_ratio": fast_count / len(recoveries) if recoveries else 0.0,
            "recoveries": recoveries,
        }
    except Exception as e:
        logger.warning(f"[absreturn] 回撤修复分析失败: {e}")
        return {"avg_recovery_days": 0, "max_recovery_days": 0,
                "n_significant_drawdowns": 0, "fast_recovery_ratio": 0.0}


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
        f"## 📊 {fund_name} — 混合型·绝对收益深度评价\n\n"
        f"**综合评级：{grade_desc}** | 分析区间：{start_date} ~ {end_date}\n\n"
        f"> 💡 绝对收益型基金的核心价值在于**「年年正收益、低波动画线」**。"
        f"这类基金通常采用量化对冲或多策略，目标是穿越牛熊、稳健增值。"
        f"本报告将从收益质量、策略稳定性、风险预警三个维度进行穿透式分析。"
    )


def _fallback_report(basic) -> dict:
    """数据不足时的兜底报告"""
    fund_name = basic.name if hasattr(basic, "name") else "未知基金"
    text = f"### {fund_name}\n\n⚠️ 数据不足，无法生成完整分析报告。请确认基金代码正确且数据源可用。"
    return {
        "meta": {"fund_name": fund_name, "fund_type": "混合型-绝对收益"},
        "headline": f"## 📊 {fund_name}",
        "section1": text,
        "section2": "",
        "section3": "",
        "section4": "",
        "full_text": text,
    }
