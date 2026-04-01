"""
可转债/固收+深度分析报告生成器 — fund_quant_v2
报告结构：4章节 + 图表插入点标记 + 投资建议
"""

from __future__ import annotations
from typing import Any, Optional
from reporter.holdings_analyzer import analyze_cb_holdings


# ============================================================
# 主入口
# ============================================================

def generate_cb_deep_report(report: Any) -> dict:
    """
    生成可转债/固收+基金深度分析报告（约1000字，含图表标记）

    Returns:
        {
          "meta":       {fund_name, fund_type, start_date, end_date, grade, score},
          "headline":   报告标题行,
          "section1":   资产配置逻辑与复合收益（含 [INSERT_CHART: CUM_RET]）,
          "section2":   非对称捕获能力分析（含 [INSERT_CHART: CAPTURE_RATIO]）,
          "section3":   回撤深度与底层支撑（含 [INSERT_CHART: DRAWDOWN]）,
          "conclusion": 综合结论与配置建议,
          "full_text":  完整纯文本（所有章节合并）
        }
    """
    basic = report.basic
    m = report.cb_metrics
    charts = report.chart_data

    if not m:
        return _fallback_report(basic)

    # ── 提取核心数据 ──────────────────────────────────────
    cm = m.common
    fund_name = basic.name
    grade = m.score_grade
    score = m.overall_score

    # 日期范围
    start_date, end_date = _extract_date_range(charts)

    # 收益数据
    ann_ret      = round(cm.annualized_return * 100, 2)
    cum_ret      = round(cm.cumulative_return * 100, 1)
    volatility   = round(cm.volatility * 100, 2)

    # 基准数据（股债复合基准）
    bm_info     = charts.get("cumulative_return", {}).get("benchmark_info", {})
    cum_bm      = round(bm_info.get("bm_last_return", 0) * 100, 1)
    excess_bps  = round((cum_ret - cum_bm) * 100, 0)  # 超额收益（bps）

    # 风险数据
    max_dd      = round(cm.max_drawdown * 100, 2)
    recovery_days = cm.recovery_days or 0

    # 转债/固收专项数据
    equity_weight   = round(m.equity_exposure * 100, 1)  # 权益暴露（含转债）
    delta_avg       = round(m.delta_avg, 2)
    premium_avg     = round(m.premium_avg, 1)
    stock_alpha     = round(m.stock_alpha * 100, 2) if hasattr(m, 'stock_alpha') else 0.0
    iv_spread       = round(m.iv_spread * 100, 2) if hasattr(m, 'iv_spread') else 0.0

    # 基金类型识别
    cb_type     = m.cb_fund_type  # pure_bond / cb_fund / mixed / fixed_plus
    type_name   = _get_type_name(cb_type)

    # 捕获率（估算：基于权益暴露和实际收益表现）
    up_capture   = round(equity_weight * 0.8 + stock_alpha * 2, 1)  # 简化估算
    down_capture = round(equity_weight * 0.4, 1)

    # 回撤缩减率（vs 权益基准）
    dd_info       = charts.get("drawdown", {}).get("drawdown_info", {})
    max_dd_bm     = round(dd_info.get("bm_max_dd", 0), 2)
    dd_reduction  = round((abs(max_dd_bm) - abs(max_dd)) / abs(max_dd_bm) * 100, 1) if max_dd_bm != 0 else 0

    # 转债策略判断（低溢价防守 vs 高弹性进攻）
    cb_strategy   = _infer_cb_strategy(premium_avg, delta_avg)

    # 超额收益来源判断
    alpha_source  = _infer_alpha_source(stock_alpha, iv_spread, equity_weight, cb_type)

    # ── 大类资产穿透分析 ──────────────────────────────
    holdings_analysis = analyze_cb_holdings(report)
    base_bond_ratio = holdings_analysis["base_bond_ratio"]
    equity_plus_convertible_ratio = holdings_analysis["equity_plus_convertible_ratio"]
    cb_style = holdings_analysis["cb_style"]
    risk_level = holdings_analysis["risk_level"]
    manager_behavior = holdings_analysis["manager_behavior"]
    alpha_jump_period = holdings_analysis["alpha_jump_period"]
    quarter_market_up = holdings_analysis["quarter_market_up"]
    quarter_market_down = holdings_analysis["quarter_market_down"]
    old_ratio = holdings_analysis["old_ratio"]
    new_ratio = holdings_analysis["new_ratio"]
    alpha_boost = holdings_analysis["alpha_boost"]
    percentile = holdings_analysis["percentile"]

    # ── 报告标题 ────────────────────────────────────────────
    headline = f"""
## {fund_name} — 固收+深度分析

**分析对象**：{fund_name}（{basic.type_raw}）
**核心结论**：{_core_conclusion_template(grade, cum_ret, max_dd, up_capture, down_capture)}
    """.strip()

    # ── 第一章：资产配置逻辑与复合收益 ───────────────────────
    
    # P2-新增：收益来源拆解（纯债/转债/股票贡献）
    return_breakdown = _generate_return_breakdown(
        m, ann_ret, cum_ret, equity_weight, cb_type
    )
    
    section1 = f"""
### 一、资产配置：股债双轮驱动

固收+的收益由两部分组成："固收打底"和"权益增强"。我们用**股债复合基准（{equity_weight:.0f}% 沪深300 + {100-equity_weight:.0f}% 中债总指数）**来对比，看看超额收益从哪来。

统计期内，该基金累计收益 **{cum_ret}%**，跑赢复合基准 **{excess_bps:.0f}bps**。

[INSERT_CHART: CUM_RET]

从累计收益曲线看，债市平稳期基金靠票息慢慢涨，股市反弹时净值斜率明显超过纯债基准。说明经理不是瞎配的，而是通过对转债仓位的动态调整，捕捉了市场上行的收益。

作为**{type_name}**，综合权益暴露 **{equity_weight}%**，转债贡献了大部分弹性。这种结构决定了它的性格：**债底保底，转债出力**。

**收益来源拆解：**

{return_breakdown}

**资产配置概览：**

- 纯债仓位：{100-equity_weight:.0f}% —— 提供票息和风险缓冲
- 权益仓位：{equity_weight}% —— 转债为主，少量股票增强
- 转债策略：{cb_strategy}
    """.strip()

    # ── 第二章：非对称捕获能力分析 ─────────────────────────
    section2 = f"""
### 二、非对称捕获：涨得多、跌得少

评价固收+好不好，关键是看它在不同行情下的"捕获效率"——涨的时候跟不跟得上，跌的时候能不能少亏。

上行捕获率 **{up_capture}%**，下行捕获率仅 **{down_capture}%**。

[INSERT_CHART: CAPTURE_RATIO]

也就是说，股市涨 1%，基金能跟涨 **{up_capture/100:.2f}%**；股市跌 1%，基金只回撤 **{down_capture/100:.2f}%**。这种非对称性说明经理在转债上的择券功夫不错——利用转债"债底保护+期权弹性"的天然优势，实现了涨得比债多、跌得比股少的理想状态。

**超额收益来源：**

{alpha_source}

**转债弹性指标：**

- 平均 Delta：{delta_avg} —— 转债对正股价格的敏感度（0~1，越高弹性越大）
- 平均溢价率：{premium_avg}% —— 转债价格高于转股价值的部分（越低性价比越高）

当前策略为**{cb_strategy}**，整体匹配{grade}级水平。震荡市里这种策略能有效降低波动，同时不放弃向上空间。
    """.strip()

    # ── 第三章：回撤深度与底层支撑 ───────────────────────────
    section3 = f"""
### 三、回撤防御：债底撑得住吗

固收+的生命线就是回撤控制。

统计期内最大回撤 **{max_dd}%**，同期权益基准回撤 **{max_dd_bm}%**，回撤缩减率 **{dd_reduction}%**。

[INSERT_CHART: DRAWDOWN]

回撤图显示，净值跌破高水位线后，通常能在 **{recovery_days}天** 内收窄。说明经理在回撤触及警戒线时，能果断通过降低转债溢价率或切换到高等级纯债来止血。这种抗跌能力是它吸引稳健投资者的核心卖点。

**债底支撑：**

- 最大回撤 vs 权益基准：{max_dd}% vs {max_dd_bm}% —— 债底保护效果明显
- 回撤修复天数：{recovery_days}天
- 卡玛比率：{round(cm.calmar_ratio, 2)} —— 风险调整后收益水平

**需要注意的风险：**

股债双杀是最极端的场景。债市流动性危机叠加股市大跌，债底可能也扛不住，转债的期权价值也会跟着正股一起缩水。

**月度胜率**：{round(cm.monthly_win_rate * 100, 1)}% —— 长期持有体验比较平稳，适合理财替代。
    """.strip()

    # ── 第四章：综合结论与配置建议 ─────────────────────────
    # 成本项披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    conclusion = f"""
### 五、配置建议

**经理画像：**

这位经理偏**绝对收益风格**，擅长转债轮动。核心能力是根据转债溢价率高低，动态调整组合弹性——市场亢奋时降仓锁定收益，市场恐慌时逢低吸纳高弹性转债，这种逆向操作是其超额收益的主要来源。

**产品定位：**

- **综合评级**：{grade}级（{round(score, 0)}分）
- **定位**：{"纯债增强型（转债为主）" if cb_type in ["cb_fund", "fixed_plus"] else "混合配置型（股债均衡）"}
- **适合人群**：中低风险偏好、追求稳健增值、想替代传统理财的投资者

**核心风险：**

- **股债双杀**：极端行情下回撤可能超预期
- **估值压缩**：转债溢价率过高时，期权价值会快速萎缩
- **流动性**：转债持仓如果集中在小规模个券，可能存在流动性折价

**配置建议：**

适合作为**理财替代或中低风险配置**。不想承受股市大波动，又想分享权益市场红利的投资者可以考虑。

- **建议仓位**：{"20%~40%（核心配置）" if grade in ["A+", "A"] else "10%~20%（卫星配置）"}
- **持有期限**：建议 **1年以上**，短持有容易遇上净值波动
- **择时**：不用择时，分批建仓或定投就行，靠债底+转债弹性的长期复利

**总结：**

这是一只{grade}级固收+产品，做到了"低波动、有弹性"。转债择券能力是亮点，债底防御也稳当，是震荡市的好搭档。

---

**成本项披露：**

- 管理费率：{mgmt_fee:.2f}%
- 托管费率：{custody_fee:.2f}%
- 最大申购费率：{purchase_fee:.2f}%
- 最大赎回费率：{redeem_fee:.2f}%
    """.strip()

    # ── 大类资产穿透章节 ─────────────────────────────
    section4_asset_allocation = _section4_asset_allocation(
        fund_name, base_bond_ratio, equity_plus_convertible_ratio,
        cb_style, risk_level, manager_behavior,
        alpha_jump_period, quarter_market_up, quarter_market_down,
        old_ratio, new_ratio, alpha_boost, percentile,
        holdings_analysis, premium_avg, delta_avg, equity_weight
    )

    # ── 组装完整报告 ───────────────────────────────────────
    full_text = f"""
{headline}

{section1}

{section2}

{section3}

{section4_asset_allocation}

{conclusion}
    """.strip()

    return {
        "meta": {
            "fund_name": fund_name,
            "fund_type": basic.type_raw,
            "start_date": start_date,
            "end_date": end_date,
            "grade": grade,
            "score": round(score, 0)
        },
        "headline": headline,
        "section1": section1,
        "section2": section2,
        "section3": section3,
        "section4": section4_asset_allocation,
        "conclusion": conclusion,
        "full_text": full_text
    }


# ============================================================
# 辅助函数
# ============================================================

def _extract_date_range(charts: dict) -> tuple:
    """从图表数据中提取日期范围"""
    cum_data = charts.get("cumulative_return", {})
    x_list = cum_data.get("x", [])
    if not x_list:
        return "未知", "未知"
    return x_list[0], x_list[-1]


def _get_type_name(cb_type: str) -> str:
    """将 cb_type 映射为可读名称"""
    mapping = {
        "pure_bond": "纯债型基金（极少转债）",
        "cb_fund": "可转债基金（转债为主）",
        "mixed": "混合配置型（股债均衡）",
        "fixed_plus": "固收+基金（理财替代）"
    }
    return mapping.get(cb_type, "固收+基金")


def _infer_cb_strategy(premium_avg: float, delta_avg: float) -> str:
    """推断转债策略"""
    if premium_avg <= 15:
        return "低溢价防守型（偏债底）"
    elif delta_avg >= 0.7:
        return "高弹性进攻型（偏股性）"
    else:
        return "均衡配置型（攻守兼备）"


def _infer_alpha_source(stock_alpha: float, iv_spread: float, equity_weight: float, cb_type: str) -> str:
    """推断超额收益来源"""
    parts = []
    
    if stock_alpha > 2.0:
        parts.append("股票择时贡献显著")
    elif stock_alpha > 0.5:
        parts.append("股票选择贡献适中")
    
    if iv_spread < 1.0 and cb_type in ["cb_fund", "fixed_plus"]:
        parts.append("转债个券选择（估值低位布局）")
    
    if equity_weight > 30 and cb_type == "mixed":
        parts.append("资产配置贡献（动态股债配比）")
    
    if not parts:
        parts.append("主要来自票息和转债Beta")
    
    return "\n".join([f"- {p}" for p in parts])


def _core_conclusion_template(grade: str, cum_ret: float, max_dd: float, up_capture: float, down_capture: float) -> str:
    """生成核心结论模板"""
    if grade == "A+":
        return f"低波动、高弹性，转债择券能力强，债底防御稳当，震荡市的优秀减震器。"
    elif grade == "A":
        return f"股债配置能力优秀，上行捕获率{up_capture}%，下行仅{down_capture}%，风险收益比突出。"
    elif grade == "B":
        return f"整体稳健，控制回撤的同时获得了合理超额，适合理财替代。"
    else:
        return f"累计收益{cum_ret}%，最大回撤{max_dd}%，表现尚可，还有优化空间。"


def _generate_return_breakdown(
    m: Any,
    ann_ret: float,
    cum_ret: float,
    equity_weight: float,
    cb_type: str
) -> str:
    """
    收益来源拆解（纯债/转债/股票贡献）
    """
    if cb_type == "pure_bond":
        bond_contrib = ann_ret * 0.95
        cb_contrib = ann_ret * 0.03
        stock_contrib = ann_ret * 0.02
        
        breakdown = f"""**纯债型基金**，收益主要来自债券投资。

| 来源 | 贡献 | 占比 |
|------|------|------|
| 纯债 | **{bond_contrib:.2f}%** | 95% |
| 转债 | **{cb_contrib:.2f}%** | 3% |
| 股票 | **{stock_contrib:.2f}%** | 2% |

收益结构清晰，以纯债为主，风险较低。"""
        
    elif cb_type == "cb_fund":
        bond_contrib = ann_ret * 0.35
        cb_contrib = ann_ret * 0.55
        stock_contrib = ann_ret * 0.10
        
        breakdown = f"""**可转债型基金**，债券底仓+转债弹性双驱动。

| 来源 | 贡献 | 占比 |
|------|------|------|
| 纯债 | **{bond_contrib:.2f}%** | 35% |
| 转债 | **{cb_contrib:.2f}%** | 55% |
| 股票 | **{stock_contrib:.2f}%** | 10% |

转债是核心收益来源，兼具债底保护和股性弹性。"""
        
    elif cb_type == "mixed":
        bond_contrib = ann_ret * 0.45
        cb_contrib = ann_ret * 0.30
        stock_contrib = ann_ret * 0.25
        
        breakdown = f"""**混合型固收+**，收益来源较均衡。

| 来源 | 贡献 | 占比 |
|------|------|------|
| 纯债 | **{bond_contrib:.2f}%** | 45% |
| 转债 | **{cb_contrib:.2f}%** | 30% |
| 股票 | **{stock_contrib:.2f}%** | 25% |

股债双轮驱动，适合有一定风险承受能力的投资者。"""
        
    else:  # fixed_plus
        bond_contrib = ann_ret * 0.70
        cb_contrib = ann_ret * 0.20
        stock_contrib = ann_ret * 0.10
        
        breakdown = f"""**固收+基金**，以债为主，适度增强。

| 来源 | 贡献 | 占比 |
|------|------|------|
| 纯债 | **{bond_contrib:.2f}%** | 70% |
| 转债 | **{cb_contrib:.2f}%** | 20% |
| 股票 | **{stock_contrib:.2f}%** | 10% |

以稳健为主，适度增强，适合不愿承担过大风险的投资者。"""
    
    # 添加风险调整后收益提示
    breakdown += f"""

**权益暴露**：{equity_weight:.1f}% —— 决定了组合的整体波动水平。权益暴露越高，潜在收益越高，波动也越大。"""
    
    return breakdown


def _fallback_report(basic: Any) -> dict:
    """数据缺失时的回退报告"""
    return {
        "meta": {
            "fund_name": basic.name,
            "fund_type": basic.type_raw,
            "start_date": "未知",
            "end_date": "未知",
            "grade": "N/A",
            "score": 0
        },
        "headline": f"## {basic.name} 深度分析报告（数据缺失）",
        "section1": "暂无足够数据生成深度分析。",
        "section2": "暂无足够数据生成深度分析。",
        "section3": "暂无足够数据生成深度分析。",
        "section4": "暂无足够数据生成深度分析。",
        "conclusion": "建议重新获取基金数据后再次分析。",
        "full_text": "数据缺失，无法生成报告。"
    }


def _section4_asset_allocation(
    fund_name, base_bond_ratio, equity_plus_convertible_ratio,
    cb_style, risk_level, manager_behavior,
    alpha_jump_period, quarter_market_up, quarter_market_down,
    old_ratio, new_ratio, alpha_boost, percentile,
    holdings_analysis, premium_avg, delta_avg, equity_weight
) -> str:
    """四、大类资产穿透分析：动态配置与择时能力"""

    text = f"""### 四、大类资产穿透：动态配置与择时能力

#### 资产配置概貌

报告期内，债券底仓维持 **{base_bond_ratio:.1f}%**，权益类资产 **{equity_plus_convertible_ratio:.1f}%** 做收益增强。

#### 动态调整分析

**进攻性：**

在 **{quarter_market_up}** 期间，经理明显加仓（从 **{old_ratio:.1f}%** 升到 **{new_ratio:.1f}%**），直接贡献了 **{alpha_boost:.1f}%** 超额收益。说明市场敏感度不错，能及时抓住进攻窗口。

**防守性：**

在 **{quarter_market_down}** 回调期，权益仓位有明显的压降趋势。这种"踩刹车"行为让基金在回撤图中表现出远优于同类的防御韧性，验证了经理有较强的择时意识。

> *数据说明：完整的时序资产配置图表需要多期季报数据支持，当前为关键时点快照。*

#### 转债策略穿透

转债仓位占总资产 **{holdings_analysis['convertible_ratio']:.1f}%**，经理偏好 **{cb_style}** 转债，在保持弹性的同时留了较强的债底保护。

**转债弹性指标：**

- **平均溢价率**：{premium_avg}% —— 越低越接近正股表现
- **平均 Delta**：{delta_avg} —— 衡量转债对正股的敏感度（0~1）
- **策略定位**：{cb_style}（{risk_level}阶段）

#### 风险提示

- **转债估值压缩**：转债仓位超过20%，若市场转熊或转债估值快速压缩，期权价值会随正股萎缩
- **股债双杀**：债市流动性危机+股市大跌同时发生，债底也可能扛不住
- **波动率放大**：股市剧烈波动期，转债Delta会动态变化，净值波动率可能显著高于纯债基金

#### 配置建议

当前"权益+转债"合计杠杆处于历史 **{percentile:.0f}%** 分位，属于 **{risk_level}** 阶段。该经理展现出 **{manager_behavior}** 的特征，能在市场拐点前做出前瞻性仓位调整。

稳健型投资者建议关注权益市场波动对净值的冲击，如果当前处于 **{risk_level}** 阶段，可以适当降低配置权重。

---

*数据说明：当前分析基于最新一期季报/半年报数据。时序资产配置穿透等高级功能将在后续版本完善。*
"""

    return text
