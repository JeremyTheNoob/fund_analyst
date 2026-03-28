"""
文字报告翻译层 — fund_quant_v2
将量化结果转化为用户友好的中文报告
四类基金：权益 / 固收 / 指数ETF / 转债固收+
"""

from __future__ import annotations
from typing import Optional

from models.schema import (
    FundReport, FundBasicInfo,
    EquityMetrics, BondMetrics, IndexMetrics, ConvertibleBondMetrics,
)


# ============================================================
# 统一入口
# ============================================================

def generate_text_report(report: FundReport) -> dict:
    """
    根据基金类型分发到对应翻译器，生成文字报告。

    Returns:
        {
            headline:     标题行（含评级/评分）
            body:         主体诊断（四维分析）
            advice:       投资建议
            risk_warning: 风险提示
        }
    """
    fund_type = report.fund_type
    basic     = report.basic

    if fund_type in ("equity", "mixed", "sector") and report.equity_metrics:
        return _translate_equity(basic, report.equity_metrics, report.tags)

    elif fund_type == "bond" and report.bond_metrics:
        return _translate_bond(basic, report.bond_metrics)

    elif fund_type in ("index",) and report.index_metrics:
        return _translate_index(basic, report.index_metrics)

    elif fund_type == "convertible_bond" and report.cb_metrics:
        return _translate_cb(basic, report.cb_metrics)

    else:
        return _fallback_report(basic)


# ============================================================
# 权益类翻译器
# ============================================================

def _translate_equity(
    basic: FundBasicInfo,
    m: EquityMetrics,
    tags: list,
) -> dict:
    """权益类四维诊断报告"""
    grade = m.score_grade
    score = m.overall_score

    # 标题
    tag_str  = " ".join([f"【{t}】" for t in tags[:3]])
    headline = f"{'🏆' if grade=='A+' else '✅' if grade=='A' else '📊' if grade=='B' else '🟡'}【{basic.name}：{tag_str}】综合评分 {score:.0f} 分 / {grade}级"

    # 性格诊断
    alpha_pct = m.alpha * 100
    beta_val  = m.beta
    r2        = m.r_squared
    ir        = m.information_ratio

    character = (
        f"**性格诊断**\n\n"
        f"年化Alpha {alpha_pct:+.1f}%/年，{_alpha_desc(alpha_pct)}\n\n"
        f"Beta {beta_val:.2f}，{_beta_desc(beta_val)}\n\n"
        f"因子解释力 R²={r2:.2f}，{'🔴 风格漂移警告：' if r2 < 0.6 else '✅'}因子模型拟合良好\n\n"
        f"信息比率 IR={ir:.2f}，{'优秀' if ir > 0.5 else '一般' if ir > 0 else '跑输基准'}"
    )

    # 风险诊断
    mdd      = m.common.max_drawdown * 100
    vol      = m.common.volatility * 100
    sharp    = m.common.sharpe_ratio
    ann_ret  = m.common.annualized_return * 100

    risk = (
        f"**风险诊断**\n\n"
        f"年化收益 {ann_ret:+.1f}%\n\n"
        f"年化波动 {vol:.1f}%\n\n"
        f"最大回撤 {mdd:.1f}%，{'🔴 回撤较大，注意仓位管理' if mdd < -20 else '🟡 回撤适中' if mdd < -10 else '🟢 回撤可控'}\n\n"
        f"夏普比率 {sharp:.2f}，{'优秀' if sharp > 1.5 else '良好' if sharp > 1.0 else '一般' if sharp > 0.5 else '偏低'}"
    )

    # Brinson 归因
    brinson = m.brinson
    if brinson and brinson.get("total", 0) != 0:
        alloc   = brinson.get('allocation', 0) * 100
        select  = brinson.get('selection', 0) * 100
        inter   = brinson.get('interaction', 0) * 100
        total   = brinson.get('total', 0) * 100

        alloc_desc  = "资产配置贡献" if alloc > 0 else "资产配置拖累"
        select_desc = "选股贡献" if select > 0 else "选股拖累"

        skill = (
            f"**超额来源**\n\n"
            f"配置效应 {alloc:+.2f}%，{alloc_desc}\n\n"
            f"选股效应 {select:+.2f}%，{select_desc}\n\n"
            f"交互效应 {inter:+.2f}%\n\n"
            f"合计超额 {total:+.2f}%"
        )
    else:
        skill = "**超额来源**\n· 持仓数据不足，归因暂不显示"

    # 投资建议
    advice = _equity_advice(m, basic)

    # 风险提示
    warnings = []
    if m.style_drift_flag:
        warnings.append("⚠️ 检测到风格漂移，近期 Beta 偏离历史均值\n\n")
    if mdd < -20:
        warnings.append("⚠️ 最大回撤超过 20%，高波动品种\n\n")
    if r2 < 0.4:
        warnings.append("ℹ️ 因子模型 R² 较低，该基金具有独立风格，对冲方向需单独评估\n\n")
    if not warnings:
        warnings.append("✅ 暂无明显风险预警\n\n")

    return {
        "headline":     headline,
        "body":         f"{character}\n\n\n{risk}\n\n\n{skill}",
        "advice":       advice,
        "risk_warning": "".join(warnings),
    }


# ============================================================
# 固收类翻译器
# ============================================================

def _translate_bond(basic: FundBasicInfo, m: BondMetrics) -> dict:
    """纯债基金四段式金字塔报告"""
    grade = m.score_grade
    score = m.overall_score

    # 债券风格标签
    duration = m.duration
    wacs     = m.wacs_score
    hhi      = m.hhi
    label    = _bond_label(duration, wacs, hhi)

    headline = (
        f"🏆【{basic.name}：{label}】综合评分 {score:.0f} 分 / {grade}级"
        if grade in ("A+", "A") else
        f"📊【{basic.name}：{label}】综合评分 {score:.0f} 分 / {grade}级"
    )

    # 收益来源
    b_long   = m.factor_loadings.get("long_rate", 0)
    b_short  = m.factor_loadings.get("short_rate", 0)
    b_credit = m.factor_loadings.get("credit", 0)
    alpha_b  = m.alpha_bond * 100

    income_source = (
        f"**收益来源**\n\n"
        f"长端利率敏感度 β₁₀={b_long:.2f}，{'高久期' if abs(b_long) > 5 else '中等久期' if abs(b_long) > 2 else '低久期'}\n\n"
        f"短端利率敏感度 β₂={b_short:.2f}\n\n"
        f"信用利差敏感度 βcs={b_credit:.2f}，{'高信用下沉' if b_credit < -2 else '中性' if b_credit > -1 else '防守型'}\n\n"
        f"因子外纯Alpha {alpha_b:+.2f}%/年"
    )

    # 持仓结构
    ann_ret = m.common.annualized_return * 100
    vol     = m.common.volatility * 100
    sharp   = m.common.sharpe_ratio

    structure = (
        f"**持仓结构**\n\n"
        f"加权久期 D={duration:.1f}年\n\n"
        f"凸性 C={m.convexity:.3f}\n\n"
        f"WACS 信用评分 {wacs:.0f}分\n\n"
        f"HHI集中度 {hhi:.0f}\n\n"
        f"年化收益 {ann_ret:+.1f}%\n\n"
        f"波动率 {vol:.2f}%\n\n"
        f"夏普 {sharp:.2f}"
    )

    # 压力测试
    stress_lines = []
    for sc in m.stress_results[:3]:
        stress_lines.append(
            f"· {sc['scenario']}（+{sc['long_bp']}BP）：预估影响 {sc['price_impact']:+.2f}%"
        )
    stress_text = "**压力测试**\n" + "\n".join(stress_lines) if stress_lines else ""

    # 综合风险
    R = duration * hhi * (100 - wacs)
    risk_check = (
        f"🔴【极高风险】综合风险指数 R={R:.0f}，突破 500000 阈值，请高度警惕！"
        if R > 500000 else
        f"🟡 综合风险指数 R={R:.0f}（{'中等偏高' if R > 200000 else '可控'}）"
    )

    # 建议
    advice = _bond_advice(m)

    return {
        "headline":     headline,
        "body":         f"{income_source}\n\n\n{structure}\n\n\n{stress_text}",
        "advice":       advice,
        "risk_warning": risk_check,
    }


# ============================================================
# 指数/ETF 翻译器
# ============================================================

def _translate_index(basic: FundBasicInfo, m: IndexMetrics) -> dict:
    """指数/ETF 工具推荐度报告"""
    grade      = m.tool_grade
    score      = m.tool_score
    is_enhanced = "增强" in basic.name

    grade_emoji = {"A+": "🏆", "A": "✅", "B": "📊", "C": "🟡", "D": "❌"}.get(grade, "📊")
    grade_text  = {
        "A+": "配置利器：几乎无损的指数复刻",
        "A":  "高质量工具：跟踪误差极低",
        "B":  "良好工具：建议配合定投降低成本",
        "C":  "一般工具：建议关注费率更低的同类替代品",
        "D":  "效率偏低：大幅偏离基准，谨慎使用",
    }.get(grade, "")

    headline = f"{grade_emoji}【{basic.name}】工具推荐度 {grade} 级（{score:.0f}分）— {grade_text}"

    # 效率分析
    te   = m.tracking_error_annualized * 100
    corr = m.correlation
    ir   = m.information_ratio

    efficiency = (
        f"**效率分析**\n\n"
        f"年化跟踪误差 TE={te:.2f}%，{'✅ 精准' if te < 0.5 else '⚠️偏高' if te > 1.5 else '良好'}\n\n"
        f"与基准相关性 ρ={corr:.3f}\n\n"
        f"信息比率 IR={ir:.2f}"
    )

    # 成本拆解
    fee_pct  = basic.fee_total * 100
    drag_pct = m.cash_drag * 100
    enh_pct  = m.enhanced_return * 100
    total_cost = -(fee_pct + abs(drag_pct))

    cost_lines = [
        f"**持有成本**\n\n",
        f"管理损耗 -{fee_pct:.3f}%/年\n\n",
        f"现金拖累 {drag_pct:+.3f}%/年\n\n",
    ]
    if is_enhanced and enh_pct > 0:
        cost_lines.append(f"增强收益 +{enh_pct:.2f}%/年（指数增强）\n\n")
    cost_lines.append(f"综合年化损耗约 {total_cost:+.3f}%")
    cost_text = "".join(cost_lines)

    # 折溢价
    if m.premium_discount_grade != "无数据":
        pd_text = (
            f"**折溢价**\n\n"
            f"折溢价均值 {m.premium_discount_mean:+.3f}%\n\n"
            f"折溢价标准差 {m.premium_discount_std:.3f}%\n\n"
            f"稳定性评级：{m.premium_discount_grade}"
        )
    else:
        pd_text = ""

    # 建议
    advice = _index_advice(m, basic)

    # 风险提示
    warnings = []
    if corr < 0.7:
        warnings.append("🔴 与基准相关性严重偏低，该工具已严重失真，不建议作为被动工具使用\n\n")
    if te > 2.0:
        warnings.append(f"⚠️ 跟踪误差 {te:.1f}%，远超行业水平（基准 0.5%），需关注跟踪能力\n\n")
    if m.premium_discount_grade == "较差":
        warnings.append("⚠️ 折溢价波动较大，建议用申赎代替二级市场交易\n\n")
    if not warnings:
        warnings.append("✅ 暂无重大风险提示\n\n")

    return {
        "headline":     headline,
        "body":         f"{efficiency}\n\n\n{cost_text}\n\n\n{pd_text}".strip(),
        "advice":       advice,
        "risk_warning": "".join(warnings),
    }


# ============================================================
# 转债/固收+ 翻译器
# ============================================================

def _translate_cb(basic: FundBasicInfo, m: ConvertibleBondMetrics) -> dict:
    """转债基金三维诊断报告"""
    grade = m.score_grade
    score = m.overall_score

    # 性格标签
    char_label = _cb_character_label(m.delta_avg, m.premium_avg)
    headline = f"【{basic.name}：{char_label}】综合评分 {score:.0f} 分 / {grade}级"

    # 权益暴露
    exposure = m.equity_exposure * 100
    delta    = m.delta_avg
    premium  = m.premium_avg
    equity_text = (
        f"**权益暴露**\n\n"
        f"综合权益暴露 E_total={exposure:.1f}%\n\n"
        f"转债平均Delta={delta:.2f}，{'偏股性强' if delta > 0.6 else '债股均衡' if delta > 0.4 else '偏债性强'}\n\n"
        f"平均转股溢价率 {premium:.1f}%，{'便宜' if premium < 10 else '贵' if premium > 30 else '合理'}"
    )

    # 估值分析
    ytm   = m.ytm * 100
    floor = m.bond_floor
    val_text = (
        f"**估值分析**\n\n"
        f"估算 YTM={ytm:.2f}%/年，纯债底仓收益\n\n"
        f"债底价格约 {floor:.1f}元，安全边际{'高' if floor > 95 else '中' if floor > 85 else '低'}"
    )

    # 投资建议
    advice = _cb_advice(m, basic)

    # 风险提示
    warnings = []
    ann_ret = m.common.annualized_return * 100
    mdd     = m.common.max_drawdown * 100
    if mdd < -15:
        warnings.append(f"⚠️ 最大回撤 {mdd:.1f}%，转债基金回撤较大，需关注正股下行风险\n\n")
    if premium > 40:
        warnings.append(f"⚠️ 平均溢价率 {premium:.0f}% 偏高，当前性价比一般，下行保护有限\n\n")
    if floor < 85:
        warnings.append("⚠️ 债底较低，纯债保底能力弱，须承担较大价格波动\n\n")
    if not warnings:
        warnings.append("✅ 暂无重大风险提示\n\n")

    return {
        "headline":     headline,
        "body":         f"{equity_text}\n\n\n{val_text}",
        "advice":       advice,
        "risk_warning": "".join(warnings),
    }


# ============================================================
# 投资建议生成（个性化）
# ============================================================

def _equity_advice(m: EquityMetrics, basic: FundBasicInfo) -> str:
    ann = m.common.annualized_return * 100
    alpha = m.alpha * 100
    mdd   = abs(m.common.max_drawdown) * 100

    lines = []
    if alpha > 3 and mdd < 20:
        lines.append("✅ 该基金具备持续超额能力且回撤可控，适合长期持有或分批买入。\n\n")
    elif alpha > 0 and mdd > 25:
        lines.append("🟡 超额能力尚可，但波动偏大。建议控制仓位，采用定投方式降低成本。\n\n")
    elif alpha < -3:
        lines.append("🔴 近期 Alpha 为负，持续跑输基准。建议观察下一季度后再决策。\n\n")
    else:
        lines.append("ℹ️ 综合来看，表现中性。建议在行情明朗后再考虑加仓。\n\n")

    return "".join(lines)


def _bond_advice(m: BondMetrics) -> str:
    duration = m.duration
    wacs     = m.wacs_score
    ann      = m.common.annualized_return * 100

    lines = []
    if duration > 5 and wacs > 70:
        lines.append("✅ 中高久期 + 较高信用质量，适合利率下行周期配置。\n\n")
    elif duration < 2:
        lines.append("ℹ️ 短久期策略，利率风险低，适合稳健资金停泊。\n\n")
    elif wacs < 40:
        lines.append("⚠️ 信用等级偏低，关注信用风险。建议审视持仓明细。\n\n")
    else:
        lines.append("ℹ️ 综合表现均衡，可作为组合底仓配置。\n\n")

    return "".join(lines)


def _index_advice(m: IndexMetrics, basic: FundBasicInfo) -> str:
    te   = m.tracking_error_annualized * 100
    corr = m.correlation
    fee  = basic.fee_total * 100

    lines = []
    if te < 0.5 and corr > 0.99 and fee < 0.3:
        lines.append("✅ 高效低费的被动工具，推荐优先选用。\n\n")
    elif te > 1.5:
        lines.append("⚠️ 跟踪误差较大，若有低 TE 替代产品可考虑切换。\n\n")
    if "增强" in basic.name and m.enhanced_return > 0:
        lines.append(f"✅ 增强收益 {m.enhanced_return*100:.2f}%，策略有效，可适当超配。\n\n")

    return "".join(lines)


def _cb_advice(m: ConvertibleBondMetrics, basic: FundBasicInfo) -> str:
    delta   = m.delta_avg
    premium = m.premium_avg
    floor   = m.bond_floor

    lines = []
    if premium < 10 and floor > 95:
        lines.append("✅ 低溢价 + 高债底，当前安全边际充分，适合布局。\n\n")
    elif delta > 0.7:
        lines.append("ℹ️ 转债已高度偏股性，风险收益特征类似股票基金。\n\n")
    elif delta < 0.3:
        lines.append("ℹ️ 转债偏债性强，收益来自票息，波动低但弹性有限。\n\n")
    else:
        lines.append("ℹ️ 攻守兼备型策略，适合震荡市场下的稳健配置。\n\n")

    return "".join(lines)


# ============================================================
# 辅助文本函数
# ============================================================

def _alpha_desc(alpha_pct: float) -> str:
    if alpha_pct > 10:
        return "🚀 强力超额，具备显著选股能力"
    elif alpha_pct > 3:
        return "✅ 正超额，持续跑赢基准"
    elif alpha_pct > -3:
        return "⚖️ 基本贴近基准，主动管理价值一般"
    else:
        return "🔴 持续跑输基准，选股能力待验证"


def _beta_desc(beta_val: float) -> str:
    if beta_val > 1.3:
        return "⚡ 高弹性，市场涨跌放大"
    elif beta_val > 0.9:
        return "📊 与市场同步，中性敏感度"
    elif beta_val > 0.5:
        return "🛡️ 低波动防守型"
    else:
        return "🧩 极低系统风险，独立走势"


def _bond_label(duration: float, wacs: float, hhi: float) -> str:
    if wacs < 50 and duration > 4:
        return "信用下沉博弈型"
    elif wacs > 80 and duration > 5:
        return "高等级长久期型"
    elif duration < 2:
        return "短久期稳健型"
    elif hhi > 2000:
        return "集中持仓型"
    else:
        return "均衡配置型"


def _cb_character_label(delta: float, premium: float) -> str:
    if delta > 0.6 and premium < 10:
        return "🚀 低溢价进攻型"
    elif delta > 0.6:
        return "🚀 进攻型转债"
    elif delta < 0.3:
        return "🧊 防御型转债"
    elif premium < 5:
        return "💎 低估值挖掘者"
    elif premium > 30:
        return "💸 高溢价收割机"
    else:
        return "⚖️ 攻守均衡型"


def _fallback_report(basic: FundBasicInfo) -> dict:
    return {
        "headline":     f"【{basic.name}】分析结果",
        "body":         "数据获取不完整，部分指标无法计算。",
        "advice":       "建议获取更多历史数据后再进行评估。",
        "risk_warning": "ℹ️ 数据不足，风险评估暂不可用。",
    }


# ============================================================
# 性格标签生成引擎（Label Engine）
# ============================================================

def generate_equity_tags(m: EquityMetrics) -> list:
    """根据量化指标自动生成权益基金性格标签（最多 3 个）"""
    tags = []

    smb = m.factor_loadings.get("SMB", 0)
    hml = m.factor_loadings.get("HML", 0)
    mom = m.factor_loadings.get("Short_MOM", 0)
    beta_val = m.beta
    r2   = m.r_squared
    alpha = m.alpha * 100

    # 规模标签
    if smb > 0.5:
        tags.append("小盘猎手")
    elif smb < -0.5:
        tags.append("大盘稳健")

    # 风格标签
    if hml > 0.5:
        tags.append("价值挖掘")
    elif hml < -0.5:
        tags.append("成长高飞")

    # 动量标签
    if mom > 0.3:
        tags.append("趋势追随")

    # Alpha 标签
    if alpha > 8:
        tags.append("超级选手")
    elif alpha > 3:
        tags.append("稳定Alpha")

    # Beta 标签
    if beta_val > 1.3:
        tags.append("高弹性进攻")
    elif beta_val < 0.6:
        tags.append("低波动防守")

    # 风格漂移（优先保留）
    if r2 < 0.6:
        if "⚠️ 风格漂移中" not in tags:
            tags.insert(0, "⚠️ 风格漂移中")

    return tags[:3]  # Top 3，防止溢出
