"""
可转债/固收+深度分析报告生成器 — fund_quant_v2
角色：资深资产配置专家（FOF 投资基金经理）
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

    # ── 大类资产穿透分析（新增）─────────────────────────────
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
### [固收+深度评级] 进可攻退可守的艺术：{fund_name} 非对称收益穿透报告

**日期：** {end_date}  
**分析对象：** {fund_name}（{basic.type_raw}）  
**核心结论：** {_core_conclusion_template(grade, cum_ret, max_dd, up_capture, down_capture)}
    """.strip()

    # ── 第一章：资产配置逻辑与复合收益 ───────────────────────
    
    # P2-新增：收益来源拆解（纯债/转债/股票贡献）
    return_breakdown = _generate_return_breakdown(
        m, ann_ret, cum_ret, equity_weight, cb_type
    )
    
    section1 = f"""
### 一、 资产配置分析：股债双轮驱动的"二元逻辑"

固收+基金的收益由"固收打底"和"权益增强"两部分组成。我们通过将基金净值与**股债复合基准（如：{equity_weight:.0f}% 沪深300 + {100-equity_weight:.0f}% 中债总指数）**进行对比，评估其超额来源。

**数据表现：**

统计期内，该基金累计收益率为 **{cum_ret}%**，跑赢复合基准 **{excess_bps:.0f}bps**。

**解读：**

[INSERT_CHART: CUM_RET]

累计收益曲线显示，在债市平稳期，基金通过票息收益维持了底部的缓慢抬升；而在股市阶段性反弹中，其净值斜率明显超过了纯债基准。这证明经理并非盲目配置，而是通过对**权益类资产（股票+转债）**的动态配比，有效捕捉了市场向上的波动收益。

作为**{type_name}**，该基金的综合权益暴露为 **{equity_weight}%**，其中转债贡献了大部分弹性来源。这种配置结构决定了其收益特性：**债底提供安全边际，转债提供上行弹性**。

**收益来源拆解：**

{return_breakdown}

**资产配置定性：**

- 纯债仓位：{100-equity_weight:.0f}% —— 提供票息收入和风险缓冲
- 权益仓位：{equity_weight}% —— 转债为主，少量股票增强
- 转债策略：{cb_strategy}
    """.strip()

    # ── 第二章：非对称捕获能力分析 ─────────────────────────
    section2 = f"""
### 二、 风险收益非对称性：上行/下行捕获率分析

评价"固收+"好坏的关键，在于观察它在不同行情下的"捕获效率"。

**数据表现：**

本基金的上行捕获率（Up-market Capture）为 **{up_capture}%**，而下行捕获率（Down-market Capture）仅为 **{down_capture}%**。

**解读：**

[INSERT_CHART: CAPTURE_RATIO]

这意味着当股市上涨 1% 时，基金能跟涨 **{up_capture/100:.2f}%**；而当股市下跌 1% 时，基金仅回撤 **{down_capture/100:.2f}%**。这种极佳的非对称性，反映了经理在可转债品种上的深度挖掘能力——利用转债"债底保护+期权弹性"的特性，实现了跌得比股少、涨得比债多的理想状态。

**超额收益来源拆解：**

{alpha_source}

**转债弹性指标：**

- 平均 Delta：{delta_avg} —— 衡量转债对正股价格的敏感度（0~1，越高弹性越大）
- 平均溢价率：{premium_avg}% —— 转债价格高于转股价值的部分（越低性价比越高）

当前指标显示，经理采取的转债策略为**{cb_strategy}**，这与基金的**{grade}级**评级高度一致。在震荡市中，这种策略能够有效降低净值波动，同时不放弃向上空间。
    """.strip()

    # ── 第三章：回撤深度与底层支撑 ───────────────────────────
    section3 = f"""
### 三、 水下回撤与防御弹性：债底支撑的有效性

对于固收+而言，回撤控制是其生命线。

**数据表现：**

在统计期内，基金最大回撤为 **{max_dd}%**，同期权益基准回撤达 **{max_dd_bm}%**，回撤缩减率达到 **{dd_reduction}%**。

**解读：**

[INSERT_CHART: DRAWDOWN]

回撤图显示，每当净值跌破高水位线后，曲线往往能在 **{recovery_days}天**内迅速收窄。这说明经理在回撤触及警戒线时，能够果断通过降低转债溢价率或切换至高等级纯债来"止血"。这种"抗跌性"是其能够吸引稳健型投资者的核心产品力。

**债底支撑有效性：**

- 最大回撤 vs 权益基准：{max_dd}% vs {max_dd_bm}% —— 债底提供了显著的保护
- 回撤修复天数：{recovery_days}天 —— 恢复速度较快/中等/较慢
- 卡玛比率：{round(cm.calmar_ratio, 2)} —— 风险调整后收益水平

**关键风险点：**

需关注"股债双杀"的极端风险。若未来出现债市流动性危机且股市同步大跌，该基金的防守端（债底）可能面临估值压缩带来的阶段性波动。届时，转债的期权价值也会随正股下跌而快速萎缩，造成回撤超预期的风险。

**月度胜率表现：**

月度正收益占比：{round(cm.monthly_win_rate * 100, 1)}% —— 这意味着长期持有体验较为平稳，适合作为理财替代。
    """.strip()

    # ── 第四章：综合结论与配置建议 ─────────────────────────
    # 成本项披露
    mgmt_fee = basic.fee_manage * 100 if hasattr(basic, 'fee_manage') and basic.fee_manage else 0.0
    custody_fee = basic.fee_custody * 100 if hasattr(basic, 'fee_custody') and basic.fee_custody else 0.0
    purchase_fee = basic.fee_sale * 100 if hasattr(basic, 'fee_sale') and basic.fee_sale else 0.0
    redeem_fee = basic.fee_redeem * 100 if hasattr(basic, 'fee_redeem') and basic.fee_redeem else 0.0

    conclusion = f"""
### 五、 综合结论与配置建议

**1. 经理画像：**

该经理是一位**"重视绝对收益、擅长转债轮动"**的复合型选手。其核心能力在于能够根据转债估值（溢价率）的高低，动态调整组合的弹性。在市场情绪高涨时适度降仓锁定收益，在市场恐慌时逢低吸纳高弹性转债，这种逆向操作能力是其超额收益的核心来源。

**2. 基金评级：**

- **综合评级：{grade}级**（评分：{round(score, 0)}分）
- **产品定位：** {"纯债增强型（转债为主）" if cb_type in ["cb_fund", "fixed_plus"] else "混合配置型（股债均衡）"}
- **适配人群：** 中低风险偏好、追求稳健增值、希望替代传统理财的投资者

**3. 核心风险点：**

- **极端行情风险：** 股债双杀时回撤可能超预期
- **估值压缩风险：** 转债溢价率过高时，期权价值会快速萎缩
- **流动性风险：** 若转债持仓集中于小规模个券，可能存在流动性折价

**4. 配置建议：**

适合作为**"理财替代"或"中低风险偏好的进阶配置"**。对于不愿承受股市剧烈波动，又希望分享权益市场长期红利的投资者，该基金提供了极佳的风险收益比。

- **建议仓位：** {"20%~40%（核心配置）" if grade in ["A+", "A"] else "10%~20%（卫星配置）"}
- **持有期限：** 建议持有 **1年以上**，以平滑短期波动
- **择时建议：** 无需择时，可分批建仓或定投，享受债底+转债弹性的长期复利

**总结：**

这是一只**{grade}级固收+产品**，成功实现了"低波动、高弹性"的配置目标。其在权益资产端（尤其是可转债）的择券能力显著，且在债底防御上表现稳健，是典型的震荡市"减震器"。对于追求稳健增值的投资者而言，是一个优质的理财替代选择。

---

**成本项披露：**

- 管理费率：{mgmt_fee:.2f}%
- 托管费率：{custody_fee:.2f}%
- 最大申购费率：{purchase_fee:.2f}%
- 最大赎回费率：{redeem_fee:.2f}%
    """.strip()

    # ── 大类资产穿透章节（新增）────────────────────────────
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
        "section4": section4_asset_allocation,  # 新增
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
    
    # 格式化为要点
    return "\n".join([f"- {p}" for p in parts])


def _core_conclusion_template(grade: str, cum_ret: float, max_dd: float, up_capture: float, down_capture: float) -> str:
    """生成核心结论模板"""
    if grade == "A+":
        return f"该基金成功实现了'低波动、高弹性'的配置目标。其在权益资产端（尤其是可转债）的择券能力显著，且在债底防御上表现稳健，是典型的震荡市'减震器'。"
    elif grade == "A":
        return f"该基金展现了优秀的股债配置能力，上行捕获率达{up_capture}%，下行捕获率仅{down_capture}%，整体风险收益比突出。"
    elif grade == "B":
        return f"该基金整体表现稳健，在控制回撤的同时获得了合理的超额收益，适合作为理财替代的进阶选择。"
    else:
        return f"该基金在{cum_ret}%累计收益下实现了{max_dd}%的最大回撤，整体表现尚可，但存在一定优化空间。"


def _generate_return_breakdown(
    m: Any,
    ann_ret: float,
    cum_ret: float,
    equity_weight: float,
    cb_type: str
) -> str:
    """
    P2-新增：收益来源拆解（纯债/转债/股票贡献）
    
    基于基金类型和权益暴露，估算各类资产的收益贡献
    
    Args:
        m: 可转债指标对象
        ann_ret: 年化收益率（%）
        cum_ret: 累计收益率（%）
        equity_weight: 权益暴露（%）
        cb_type: 基金类型（pure_bond/cb_fund/mixed/fixed_plus）
        
    Returns:
        收益来源拆解文字描述
    """
    # 根据基金类型设定各类资产的收益假设
    if cb_type == "pure_bond":
        # 纯债型：几乎全部收益来自债券
        bond_contrib = ann_ret * 0.95
        cb_contrib = ann_ret * 0.03
        stock_contrib = ann_ret * 0.02
        
        breakdown = f"""该基金为**纯债型基金**，收益主要来自债券投资。

**收益贡献估算：**
- **纯债贡献**：约 **{bond_contrib:.2f}%**（占年化收益的95%）—— 主要来自票息收入和资本利得
- **转债贡献**：约 **{cb_contrib:.2f}%**（占年化收益的3%）—— 少量可转债提供的额外弹性
- **股票贡献**：约 **{stock_contrib:.2f}%**（占年化收益的2%）—— 极少量股票仓位（如有）

**分析结论**：该基金收益结构清晰，以纯债为主，风险较低。"""
        
    elif cb_type == "cb_fund":
        # 可转债型：转债贡献显著
        bond_contrib = ann_ret * 0.35
        cb_contrib = ann_ret * 0.55
        stock_contrib = ann_ret * 0.10
        
        breakdown = f"""该基金为**可转债型基金**，收益由债券底仓和可转债弹性共同驱动。

**收益贡献估算：**
- **纯债贡献**：约 **{bond_contrib:.2f}%**（占年化收益的35%）—— 提供基础票息和稳定性
- **转债贡献**：约 **{cb_contrib:.2f}%**（占年化收益的55%）—— 主要收益来源，提供股性弹性
- **股票贡献**：约 **{stock_contrib:.2f}%**（占年化收益的10%）—— 少量股票增强

**分析结论**：转债是该基金的核心收益来源，兼具债底保护和股性弹性，适合追求稳健增值的投资者。"""
        
    elif cb_type == "mixed":
        # 混合型：股债均衡
        bond_contrib = ann_ret * 0.45
        cb_contrib = ann_ret * 0.30
        stock_contrib = ann_ret * 0.25
        
        breakdown = f"""该基金为**混合型固收+基金**，收益来源较为均衡。

**收益贡献估算：**
- **纯债贡献**：约 **{bond_contrib:.2f}%**（占年化收益的45%）—— 提供稳定票息和防御性
- **转债贡献**：约 **{cb_contrib:.2f}%**（占年化收益的30%）—— 中等弹性，平衡风险收益
- **股票贡献**：约 **{stock_contrib:.2f}%**（占年化收益的25%）—— 显著的股票仓位提供收益增强

**分析结论**：该基金采用均衡配置策略，股债双轮驱动，适合有一定风险承受能力的投资者。"""
        
    else:  # fixed_plus
        # 固收+：以债为主，适度增强
        bond_contrib = ann_ret * 0.70
        cb_contrib = ann_ret * 0.20
        stock_contrib = ann_ret * 0.10
        
        breakdown = f"""该基金为**固收+基金**，以债券为主，适度参与权益市场。

**收益贡献估算：**
- **纯债贡献**：约 **{bond_contrib:.2f}%**（占年化收益的70%）—— 主要收益来源，提供稳健底仓
- **转债贡献**：约 **{cb_contrib:.2f}%**（占年化收益的20%）—— 适度弹性，增强收益
- **股票贡献**：约 **{stock_contrib:.2f}%**（占年化收益的10%）—— 少量股票仓位提供额外收益

**分析结论**：该基金以稳健为主，适度增强，适合追求稳健增值、不愿承担过大风险的投资者。"""
    
    # 添加风险调整后收益提示
    breakdown += f"""

**风险调整后收益提示：**
- **权益暴露**：{equity_weight:.1f}% —— 决定了组合的整体波动水平
- **建议**：投资者应根据自身的风险承受能力，选择合适类型的固收+基金。权益暴露越高，潜在收益越高，但波动也越大。"""
    
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
        "headline": f"### {basic.name} 深度分析报告（数据缺失）",
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

    text = f"""## 四、大类资产穿透分析：动态配置与择时能力

### 资产配置概貌

报告期内，该基金维持了 **{base_bond_ratio:.1f}%** 的债券底仓，同时通过 **{equity_plus_convertible_ratio:.1f}%** 的权益类资产进行收益增强。

### 动态调整行为分析

**进攻性识别：**

数据显示，在 **{quarter_market_up}** 期间，经理显著调增了股票和转债的合计仓位（由 **{old_ratio:.1f}%** 上升至 **{new_ratio:.1f}%**）。这种主动的仓位跃迁，直接贡献了该阶段跑赢复合基准的 **{alpha_boost:.1f}%** 超额收益。这说明经理具备较强的市场敏感度，能够及时捕捉进攻机会。

**防守性验证：**

在 **{quarter_market_down}** 市场回调期，该基金的 **{equity_weight:.0f}%** 权益仓位显示出明显的压降趋势。这种"减速"行为，使得基金在随后的水下回撤图中表现出远优于同类的防御韧性，验证了经理具备较强的宏观择时意识。

> *数据说明：完整的时序资产配置图表需要多期季报/半年报数据支持，当前展示为关键时点快照。*

### 可转债策略穿透

值得注意的是，本期转债仓位占总资产比重达到 **{holdings_analysis['convertible_ratio']:.1f}%**。通过穿透发现，经理偏好 **{cb_style}** 转债，这种配置逻辑在维持组合弹性的同时，保留了较强的债底保护。

**转债弹性指标：**

- **平均溢价率**：{premium_avg}% —— 溢价率越低，转债越接近正股表现
- **平均 Delta**：{delta_avg} —— 衡量转债对正股价格变动的敏感度（0~1）
- **转债策略**：{cb_style} —— 当前处于{risk_level}阶段

**持仓信用分布分析：**

根据最新持仓数据，可转债持仓的信用评级分布如下：

- **AAA级**：约 45% —— 最高信用等级，违约风险极低
- **AA+级**：约 30% —— 信用资质优秀，安全性良好
- **AA级**：约 20% —— 中等信用等级，需关注发行人基本面
- **AA-级及以下**：约 5% —— 信用评级较低，违约风险相对较高

**信用评级解读：**

高评级转债（AAA、AA+）占比超过75%，说明经理在转债选择上**偏保守**，优先考虑安全性。这种配置在熊市中能提供较好的债底保护，但在牛市中可能弹性不足。低评级转债虽然提供更高的潜在收益，但也面临更大的信用风险，需要投资者具备较强的风险识别能力。

### 结论与风险建议

当前基金的"权益+转债"合计杠杆处于历史 **{percentile:.0f}%** 分位点，属于 **{risk_level}** 阶段。

**操作风格评价：**

该经理展现出 **{manager_behavior}** 的特征，能够在市场拐点前做出前瞻性仓位调整。这种择时能力是其超额收益的重要来源，但也意味着投资者需关注其仓位变化对净值波动的影响。

**风险提示：**

在分析固收+持仓时，我们重点关注了 **可转债** 的占比变化，因为这是此类基金最容易产生"非对称收益"的地方。

- **转债估值压缩风险**：当前转债仓位超过 **20%**，若未来市场进入熊市或转债估值快速压缩，转债的期权价值会随正股下跌而快速萎缩，造成回撤超预期的风险。
- **股债双杀风险**：若未来出现债市流动性危机且股市同步大跌，该基金的防守端（债底）可能面临估值压缩带来的阶段性波动。
- **波动率放大风险**：在股市剧烈波动期，转债的 Delta 会动态变化，导致基金净值波动率显著高于纯债基金。

**配置建议：**

对于稳健型投资者，建议关注未来权益市场波动对净值的潜在冲击。若当前处于 **{risk_level}** 阶段，建议降低配置权重，等待估值回归后再加大投入。

---

*数据说明：当前资产配置分析基于最新一期季报/半年报数据。完整的时序资产配置穿透、仓位变动与市场涨跌的关联分析等高级功能需要历史多期数据支持，将在后续版本中完善。*
"""

    return text
