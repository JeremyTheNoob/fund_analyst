"""
翻译服务模块
将量化分析结果翻译成用户友好的大白话
依赖：config, utils, models
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np

import config


def _calc_rolling_alpha_trend(fund_ret: pd.Series, bm_ret: pd.Series) -> dict:
    """
    计算滚动Alpha趋势

    Args:
        fund_ret: 基金收益率序列
        bm_ret: 基准收益率序列

    Returns:
        包含趋势信息的字典
    """
    # 简化实现：计算最近的月度Alpha
    if len(fund_ret) < 30:
        return {'trend_text': ''}

    recent_fund = fund_ret.tail(30)
    recent_bm = bm_ret.tail(30)

    excess = recent_fund - recent_bm
    excess_mean = excess.mean()

    # 检查是否连续3个月下降
    if len(excess) >= 60:
        alpha_monthly = [
            excess[i:i+21].mean() for i in range(0, len(excess)-20, 21)
        ]
        if len(alpha_monthly) >= 3:
            is_declining = all(alpha_monthly[i] > alpha_monthly[i+1] for i in range(len(alpha_monthly)-1))
            if is_declining and alpha_monthly[-1] < 0:
                return {
                    'trend_text': '📉 **情绪预警**：最近3个月Alpha连续下降，超额收益在消退。'
                }

    if excess_mean > 0.01:
        return {'trend_text': ''}
    elif excess_mean < -0.01:
        return {'trend_text': '⚠️ **情绪提示**：近期Alpha转负，超额收益承压。'}
    else:
        return {'trend_text': ''}


def translate_results(model: str, results: dict,
                      basic: dict, holdings: dict,
                      rolling_df: pd.DataFrame = None,
                      bm_ret_for_trend: pd.Series = None,
                      fund_ret_for_trend: pd.Series = None) -> dict:
    """
    将量化分析结果翻译为大白话四维诊断
    返回：{character, skill, risk, advice, score, tags, emotion_note}

    新增字段：
      tags        - 性格标签列表，如 ['市场捕手', '小盘偏好', '成长风格']
      emotion_note- 情绪指标文本（滚动Alpha趋势），空字符串表示无警示
      consistency_warn - 一致性预警文本（beta高/alpha低 = 无效加杠杆）
    """
    out = {
        'character': '', 'skill': '', 'risk': '', 'advice': '', 'score': 60,
        'tags': [],          # 新增：性格标签
        'emotion_note': '',  # 新增：情绪指标
        'consistency_warn': ''  # 新增：一致性预警
    }

    name = basic.get('name', '该基金')
    fee_total = basic.get('fee_total', 0)

    if model == 'equity':
        alpha   = results.get('alpha')
        alpha_p = results.get('alpha_pval', 1.0)
        r2      = results.get('r_squared', 0.5)
        betas   = results.get('factor_betas', {})
        mkt_b   = betas.get('Mkt', 1.0)
        smb_b   = betas.get('SMB', 0.0)
        hml_b   = betas.get('HML', 0.0)
        mom_b   = betas.get('Short_MOM', 0.0)

        alpha_f = alpha if alpha is not None else 0.0

        # ============ 性格标签体系（多标签并列） ============
        tags = []

        # 主标签：由 Beta + Alpha 共同决定
        if mkt_b > 1.2 and alpha_f > 0.05 and alpha_p < 0.05:
            tags.append('⚡ 市场捕手')   # 高Beta + 真Alpha → 进攻型但有真本事
        elif mkt_b > 1.2:
            tags.append('🎯 激进放大镜')  # 高Beta 但Alpha无显著性 → 纯Beta押注
        elif mkt_b < 0.7 and alpha_f > 0.03:
            tags.append('🛡️ 稳健老兵')   # 低Beta + 正Alpha → 防守有余还能超额
        elif mkt_b < 0.7:
            tags.append('🧊 防御专家')   # 纯防御，跑输牛市
        elif r2 > 0.9:
            tags.append('🪞 指数影子')   # 高度复制基准
        elif alpha_f > 0.05 and alpha_p < 0.05:
            tags.append('💎 明星选股手')  # 均衡Beta + 显著Alpha → 最理想
        elif alpha_f > 0.02:
            tags.append('🎓 努力型选手')  # 有超额但不够显著
        else:
            tags.append('🌊 随波逐流型')  # 无明显Alpha，跟随大盘

        # 风格附加标签
        if smb_b > 0.4:
            tags.append('📦 小盘偏好')
        elif smb_b < -0.3:
            tags.append('🏛️ 大盘偏好')

        if hml_b > 0.3:
            tags.append('🏷️ 价值风格')
        elif hml_b < -0.3:
            tags.append('🚀 成长风格')

        if mom_b > 0.3:
            tags.append('📈 追势动量')
        elif mom_b < -0.3:
            tags.append('🔄 逆势反转')

        out['tags'] = tags

        # ============ 性格文本（character） ============
        main_tag = tags[0] if tags else ''
        if '市场捕手' in main_tag:
            out['character'] = (
                f"**{main_tag}**。Beta≈{mkt_b:.2f}，牛市弹性强；"
                f"同时有真实Alpha，说明经理不只靠Beta吃饭，选股也有真功夫。"
                f"进攻型中的佼佼者。"
            )
        elif '激进放大镜' in main_tag:
            out['character'] = (
                f"**{main_tag}**。Beta≈{mkt_b:.2f}，本质是市场的「放大镜」。"
                f"牛市跑快熊市跑更快，超额收益尚无统计显著性——"
                f"目前的超额可能只是Beta的附带品，而非经理真本事。"
            )
        elif '稳健老兵' in main_tag:
            out['character'] = (
                f"**{main_tag}**。Beta约{mkt_b:.2f}，跌得少、跌得慢；"
                f"叠加年化Alpha {alpha_f*100:.1f}%，能在控制波动的同时创造超额。"
                f"防守中不忘进攻，稳中求优。"
            )
        elif '防御专家' in main_tag:
            out['character'] = (
                f"**{main_tag}**。Beta约{mkt_b:.2f}，大盘下行时抗跌，"
                f"但强牛市会明显跑输指数——适合保守投资者或高波动期的防御配置。"
            )
        elif '指数影子' in main_tag:
            out['character'] = (
                f"**{main_tag}**。R²={r2:.2f}，基金走势几乎贴着基准走。"
                "你花了主动管理费，买了一个「伪指数基金」。"
                "建议直接比较同类低费率ETF的替代可能性。"
            )
        elif '明星选股手' in main_tag:
            out['character'] = (
                f"**{main_tag}**。Beta适中（{mkt_b:.2f}），年化Alpha {alpha_f*100:.1f}%且统计显著（p={alpha_p:.3f}）。"
                f"不靠押注大盘方向，靠选股能力创造超额——这是最理想的主动基金形态。"
            )
        elif '努力型选手' in main_tag:
            out['character'] = (
                f"**{main_tag}**。有一定超额（{alpha_f*100:.1f}%），但统计显著性不足，"
                f"可能有运气成分。需要更长时间观察是否具备可复制的选股逻辑。"
            )
        else:
            out['character'] = (
                f"**{main_tag}**。Beta≈{mkt_b:.2f}，Alpha趋近于零，"
                f"主要靠市场Beta驱动收益，没有展现明显的主动管理价值。"
            )

        # 风格附加文字
        style_notes = [t for t in tags[1:]]
        if style_notes:
            out['character'] += f" | 风格标签：{'、'.join(style_notes)}。"

        # ============ 实力（skill） ============
        if alpha is None:
            out['skill'] = "数据不足，无法评估Alpha。"
        elif r2 > 0.9:
            out['skill'] = (
                f"R²={r2:.2f}，基金在高度复制基准，几乎没有主动管理。"
                "与其支付管理费，不如买低费率指数基金。"
            )
        elif alpha_f > 0.05 and alpha_p < 0.05:
            out['skill'] = (
                f"年化Alpha {alpha_f*100:.1f}%，统计显著（p={alpha_p:.3f}）。"
                "**这是真本事**，超额收益并非运气，经理有可复制的获利逻辑。"
            )
        elif alpha_f > 0.02 and alpha_p < 0.1:
            out['skill'] = (
                f"年化Alpha {alpha_f*100:.1f}%，有一定主动能力，但统计显著性不够强（p={alpha_p:.3f}）。"
                "需要更长时间验证。"
            )
        elif alpha_f > 0:
            out['skill'] = (
                f"年化Alpha {alpha_f*100:.1f}%，但统计不显著（p={alpha_p:.3f}），"
                "超额收益可能有运气成分。"
            )
        else:
            out['skill'] = (f"年化Alpha {alpha_f*100:.1f}%为负，跑输风险调整后基准，需警惕。")

        # ============ 一致性判定：Beta高 + Alpha低 = 无效加杠杆 ============
        consistency_warn = ''
        latest_dyn_beta = None
        if rolling_df is not None and not rolling_df.empty:
            col = 'equity_beta_20' if 'equity_beta_20' in rolling_df.columns else 'equity_beta'
            if col in rolling_df.columns and rolling_df[col].notna().any():
                latest_dyn_beta = rolling_df[col].dropna().iloc[-1]

        if latest_dyn_beta is not None:
            if latest_dyn_beta > 0.85 and (alpha_f < 0.02 or alpha_p > 0.1):
                consistency_warn = (
                    f"⚡ **一致性预警 · 无效加杠杆**：动态Beta估算约 {latest_dyn_beta*100:.0f}%，"
                    f"但年化Alpha仅 {alpha_f*100:.1f}%（p={alpha_p:.3f}，不显著）。"
                    f"经理满仓押注市场，但没有换来相应的超额收益——"
                    f"这是典型的「加了杠杆但没有Alpha」，风险/收益严重不对等。"
                )
            elif latest_dyn_beta < 0.30 and alpha_f > 0.05 and alpha_p < 0.05:
                consistency_warn = (
                    f"💡 **一致性观察**：动态仓位仅约 {latest_dyn_beta*100:.0f}%（轻仓），"
                    f"但年化Alpha {alpha_f*100:.1f}%显著。"
                    f"说明经理用较少的股票仓位创造了较高的超额——"
                    f"选股精准度极高，或在特定时间窗口有集中获利。"
                )

        out['consistency_warn'] = consistency_warn

        # ============ 情绪指标：滚动Alpha连续3月下降 ============
        emotion_note = ''
        if fund_ret_for_trend is not None and bm_ret_for_trend is not None:
            trend_res = _calc_rolling_alpha_trend(fund_ret_for_trend, bm_ret_for_trend)
            emotion_note = trend_res.get('trend_text', '')
            out['_trend_data'] = trend_res  # 供展示层使用
        out['emotion_note'] = emotion_note

        # ============ 风险（定性结论）============
        risks = []
        if r2 > 0.9 and fee_total > 0.01:
            risks.append("管理费过高但实质上是伪指数基金，费效比严重失衡")
        if mkt_b > 1.3:
            risks.append("高Beta放大器——牛市超额赚，熊市超额亏，需严格控制仓位比例")
        if smb_b > 0.5:
            risks.append("小盘股集中持仓，流动性风险偏高，市场下行时可能形成踩踏")
        if consistency_warn and '无效加杠杆' in consistency_warn:
            risks.append("满仓运作但无Alpha保护——典型的「用风险换收益却没换到」")

        out['risk'] = '、'.join(risks) if risks else "无明显异常风险。"

        # ============ 建议（advice）============
        advices = []
        if alpha_f > 0.05 and alpha_p < 0.05:
            advices.append("经理具备真实选股能力，可作为核心持仓长期持有")
        elif alpha_f < 0:
            advices.append("Alpha为负，建议降低配置或切换到同类优秀基金")
        elif r2 > 0.9:
            advices.append("高度复制基准，建议考虑低费率指数基金替代")

        if mkt_b > 1.2:
            advices.append("高Beta特性明显，建议在市场低位增加配置，高位减仓")
        elif mkt_b < 0.7:
            advices.append("低Beta特性，适合作为防御性配置")

        out['advice'] = '；'.join(advices) if advices else "保持观察，建议定期评估"

        # ============ 综合评分 ============
        score = 60.0
        # Alpha加分
        if alpha_f > 0.05 and alpha_p < 0.05:
            score += 20
        elif alpha_f > 0.02:
            score += 10
        elif alpha_f < 0:
            score -= 15

        # R²加分
        if 0.6 <= r2 <= 0.85:  # 理想区间
            score += 10
        elif r2 > 0.9:  # 太接近指数
            score -= 10

        # Beta调整
        if mkt_b > 1.3:
            score -= 5
        elif mkt_b < 0.7:
            score -= 5

        out['score'] = max(0, min(100, score))

    elif model == 'bond':
        # 债券模型翻译
        duration = results.get('duration', 0)
        convexity = results.get('convexity', 0)
        alpha = results.get('alpha', 0)

        tags = []
        if duration > 3:
            tags.append('📈 长久期')
            out['character'] = (
                f"**长久期进攻型**。久期{duration:.2f}年，利率下行时收益弹性强；"
                f"但利率上行时回撤也大——典型的「利率债放大器」。"
            )
        elif duration < 1:
            tags.append('🧊 短久期')
            out['character'] = (
                f"**短久期防御型**。久期{duration:.2f}年，对利率变动不敏感；"
                f"收益率稳定但较低——适合保守投资者或短期理财。"
            )
        else:
            tags.append('⚖️ 中久期')
            out['character'] = (
                f"**中久期平衡型**。久期{duration:.2f}年，进可攻退可守。"
                f"在控制利率风险的同时追求适度收益。"
            )

        if alpha > 0:
            tags.append('💎 信用Alpha')
            out['skill'] = (
                f"信用Alpha {alpha*100:.1f}%，说明在久期之外还有其他贡献。"
            )
        else:
            out['skill'] = "主要靠久期策略获取收益，信用溢价不明显。"

        out['risk'] = (
            f"久期风险：{duration:.2f}年。"
            f"若利率上行100BP，预估回撤约{duration}%。"
        )
        out['advice'] = "根据对利率走势的判断，动态调整久期配置。"
        out['tags'] = tags
        out['score'] = 70

    else:
        # 其他模型
        out['character'] = "暂无详细分析"
        out['skill'] = "暂无详细分析"
        out['risk'] = "暂无详细分析"
        out['advice'] = "暂无详细分析"
        out['score'] = 60
        out['tags'] = []

    return out


# ============================================================
# 🔬 纯债基金专属翻译层（四段式金字塔结构）
# ============================================================

def translate_pure_bond_results(
    model_results: dict,
    macro_plugin: dict = None,
) -> dict:
    """
    纯债基金大白话四段式报告

    金字塔结构：
    1. 核心定调（结论先行）
    2. 收益溯源（钱从哪里来）
    3. 风险排查（坑在哪里）
    4. 投资建议（怎么买）

    Args:
        model_results: run_pure_bond_analysis()的结果
        macro_plugin: get_macro_plugin()的结果

    Returns:
        {
          'headline': str,          # 标题行（一句话定调）
          'fund_label': str,        # 性格标签
          'scores_summary': str,    # 评分摘要
          'income_source': str,     # 第2段：收益溯源
          'risk_check': str,        # 第3段：风险排查
          'advice': str,            # 第4段：投资建议
          'duration_highlight': str,# 久期专项说明
          'duration_grade_note': str, # 久期评级说明（A+时特殊）
          'macro_notes': str,       # 宏观环境备注
          'tags': list,             # 标签列表
          'overall_grade': str,     # 综合评级
        }
    """
    scores = model_results.get('scores', {})
    asset_struct = model_results.get('asset_structure', {})
    credit = model_results.get('credit_quality', {})
    conc = model_results.get('concentration', {})
    dur = model_results.get('duration_system', {})
    three_factor = model_results.get('three_factor_results', {})
    identity = model_results.get('identity', {})
    data_quality = model_results.get('data_quality', {})

    fund_label = scores.get('fund_label', model_results.get('fund_label', '📊 均衡配置型'))
    grade = scores.get('grade', 'B')
    total_score = scores.get('total_score', 70.0)
    one_vote_veto = scores.get('one_vote_veto', False)
    veto_reason = scores.get('veto_reason', '')

    # ── 1. 核心定调 ───────────────────────────────────────────────
    label_core = fund_label.split('（')[0].replace('🚀', '').replace('🛡️', '').replace(
        '🌾', '').replace('⚡', '').replace('🎲', '').replace('🎯', '').replace(
        '💰', '').replace('📊', '').strip()

    grade_emoji = {'A+': '🏆', 'A': '✅', 'B': '📊', 'C': '🟡', 'D': '❌'}.get(grade, '📊')

    if one_vote_veto:
        headline = f"❌【高风险警示】{label_core} · 触发一票否决"
        scores_summary = f"综合评级：D级 | 原因：{veto_reason}"
    else:
        headline = f"{grade_emoji}【{label_core}】综合评分 {total_score:.0f}分 / {grade}级"
        scores_summary = (
            f"底层资产质量 {scores.get('score_quality', 75):.0f}分 | "
            f"信用 {scores.get('s_credit', 80):.0f}分 · "
            f"集中度 {scores.get('s_conc', 80):.0f}分 · "
            f"结构 {scores.get('s_struct', 80):.0f}分 | "
            f"久期管理 {scores.get('s_duration', 80):.0f}分（{scores.get('duration_grade', 'B')}级）"
        )

    # ── 2. 收益溯源 ───────────────────────────────────────────────
    income_parts = []
    rate_r = asset_struct.get('rate_ratio', 0)
    credit_r = asset_struct.get('credit_ratio', 0)
    ncd_r = asset_struct.get('ncd_ratio', 0)
    duration_val = dur.get('duration', 2.0)
    alpha = three_factor.get('alpha', 0.0)
    alpha_pval = three_factor.get('alpha_pval', 1.0)

    # 主要收益来源分析
    if ncd_r > 0.50:
        income_parts.append(
            f"💰 收益来源主要是同业存单的稳定票息（占比{ncd_r*100:.0f}%），"
            f"本质上是一只增强版的货币基金，收益稳健但Alpha空间有限"
        )
    elif rate_r > 0.60:
        if duration_val > 4:
            income_parts.append(
                f"🚀 主要靠利率债资本利得赚钱（利率债{rate_r*100:.0f}%，久期{duration_val:.1f}年）。"
                f"经理对利率走势判断精准，在债牛行情中吃到了最肥的一段"
            )
        else:
            income_parts.append(
                f"🛡️ 以利率债票息为主（{rate_r*100:.0f}%），久期{duration_val:.1f}年，"
                f"赚的是稳稳的利息钱，波动极低"
            )
    elif credit_r > 0.60:
        wacs = credit.get('wacs', 80)
        if credit.get('is_credit_sinking'):
            income_parts.append(
                f"⚡ 主要靠信用债挖掘赚钱（{credit_r*100:.0f}%），"
                f"经理在做'信用下沉'，博取高收益债的风险溢价。"
                f"WACS评分{wacs:.0f}（偏低），收益质量需关注"
            )
        else:
            income_parts.append(
                f"🌾 主要靠优质信用债的票息赚钱（{credit_r*100:.0f}%），"
                f"WACS评分{wacs:.0f}（高等级）。"
                f"赚的是辛苦的选券的钱，收益质量高"
            )
    else:
        income_parts.append(
            f"📊 利率债+信用债均衡配置（利率{rate_r*100:.0f}% / 信用{credit_r*100:.0f}%），"
            f"收益来源多元化"
        )

    # Alpha贡献
    if alpha_pval < 0.05 and alpha > 0:
        income_parts.append(
            f"✨ 三因子模型显示：剔除市场因子后，年化Alpha为{alpha*100:.2f}%（显著）"
        )
    elif alpha_pval < 0.1 and alpha > 0:
        income_parts.append(f"经理存在一定的超额选券能力（Alpha={alpha*100:.2f}%，弱显著）")

    income_source = '；'.join(income_parts)

    # ── 3. 风险排查 ───────────────────────────────────────────────
    risk_parts = []

    # HHI集中度风险
    hhi = conc.get('static_hhi', 500)
    top5 = conc.get('top5_ratio', 0.2)
    hhi_trend = conc.get('hhi_trend', 'stable')

    if hhi > 1500:
        risk_parts.append(
            f"🎯 集中度较高（HHI={hhi:.0f}），前五大重仓债占比{top5*100:.0f}%。"
            f"鸡蛋比较集中，单只债券踩雷会导致净值明显跳水"
        )
    elif hhi_trend == 'rising':
        risk_parts.append(
            f"📈 动态预警：近期HHI持续上升（当前{hhi:.0f}），"
            f"经理正在增加押注——需警惕风格激进化"
        )
    else:
        risk_parts.append(f"✅ 持仓较为分散（HHI={hhi:.0f}），单券踩雷风险可控")

    # 久期风险
    std_range = dur.get('standard_range', (1.0, 5.0))
    is_in_std = dur.get('is_in_standard', True)
    if not is_in_std:
        risk_parts.append(
            f"⚠️ 久期漂移：实测{duration_val:.1f}年，"
            f"超出{dur.get('fund_subtype', '基金类型')}标准区间{std_range}，"
            f"风格与宣传不符"
        )

    # 利率压力测试
    stress_10bp = dur.get('stress_10bp', -0.2)
    risk_parts.append(
        f"📐 压力测试：利率若上行10BP，净值约跌{abs(stress_10bp):.2f}%"
    )

    # 信用下沉风险
    if credit.get('is_credit_sinking'):
        sink_r = credit.get('sinking_ratio', 0)
        risk_parts.append(
            f"⚡ 信用下沉：AA+以下占比{sink_r*100:.0f}%。"
            f"经理在用风险换收益，适合风险偏好较高的投资者"
        )

    # 数据质量警告
    if not data_quality.get('is_valid', True):
        risk_parts.append(
            f"⚠️ 数据质量：{data_quality.get('warnings', [''])[0]}"
        )

    risk_check = '；'.join(risk_parts)

    # ── 4. 投资建议 ───────────────────────────────────────────────
    market_trend = ''
    if macro_plugin and macro_plugin.get('rate_macro_text'):
        pass  # 宏观文本已在 macro_notes 中展示

    if one_vote_veto:
        advice = f"❌ 不建议配置。{veto_reason}"
    elif grade == 'A':
        if credit_r > 0.60 and credit.get('is_credit_sinking'):
            advice = (
                f"适合作为组合中的'进攻角'（{label_core}）。"
                f"鉴于信用下沉策略，建议分批买入，"
                f"控制单基金配置比例不超过15%，分散尾部风险。"
            )
        elif rate_r > 0.60 and duration_val > 4:
            advice = (
                f"当前久期处于高位，在利率下行趋势中表现亮眼。"
                f"建议顺势持有，但密切关注利率拐点信号，"
                f"利率开始上行时及时减仓。"
            )
        else:
            advice = (
                f"优质纯债基金，适合作为组合底仓。"
                f"可长期持有，利率上行时适当减持，利率下行时加仓。"
            )
    elif grade == 'B':
        advice = (
            f"整体表现合格，风格基本稳定。"
            f"适合稳健型投资者作为组合的安全垫，"
            f"建议配置比例不超过组合的30%。"
        )
    elif grade == 'C':
        advice = (
            f"存在风格漂移或信用质量问题，需谨慎配置。"
            f"建议先观察一个季度，确认风格改善后再考虑买入。"
        )
    else:
        advice = (
            f"综合评分偏低，不建议作为主要持仓。"
            f"如确实需要配置，建议小仓位（<5%）并设置止损。"
        )

    # ── 久期专项说明 ──────────────────────────────────────────────
    dur_grade = scores.get('duration_grade', 'B')
    dur_score = scores.get('s_duration', 80)
    timing_score = dur.get('timing_score', 0)

    if dur_grade == 'A+':
        duration_highlight = (
            f"🏆 【卓越表现】久期管理得分{dur_score:.0f}分（A+级），"
            f"主要源于在{'利率下行' if timing_score > 0 else '利率波动'}阶段的精准择时操作，"
            f"超额收益显著。"
        )
        duration_grade_note = (
            f"久期择时得分{dur_score:.0f}分（突破100分基准），"
            f"代表经理在合规区间内展现了卓越的择时能力。"
        )
    elif dur_grade == 'A':
        duration_highlight = (
            f"✅ 久期管理优秀（{dur_score:.0f}分 / A级），"
            f"久期{duration_val:.1f}年严格匹配基金类型，风格稳健。"
        )
        duration_grade_note = ''
    elif dur_grade == 'D':
        duration_highlight = (
            f"❌ 久期管理警示（{dur_score:.0f}分 / D级），"
            f"实测久期{duration_val:.1f}年严重偏离基金类型标准，"
            f"风格背离风险较高。"
        )
        duration_grade_note = f"D级标准：久期偏离超出合理区间，或短债基金久期>3年。"
    else:
        duration_highlight = (
            f"久期管理{'良好' if dur_grade == 'B' else '尚可'}（{dur_score:.0f}分 / {dur_grade}级），"
            f"久期{duration_val:.1f}年{'在' if dur.get('is_in_standard') else '略超出'}标准区间内。"
        )
        duration_grade_note = ''

    # ── 宏观备注 ──────────────────────────────────────────────────
    macro_notes_parts = []
    if macro_plugin:
        if macro_plugin.get('rate_macro_text'):
            macro_notes_parts.append(macro_plugin['rate_macro_text'])
        if macro_plugin.get('credit_industry_text'):
            macro_notes_parts.append(macro_plugin['credit_industry_text'])

    macro_notes = '\n\n'.join(macro_notes_parts)

    # ── 标签 ──────────────────────────────────────────────────────
    tags = [fund_label]
    if credit.get('is_credit_sinking'):
        tags.append('⚡ 信用下沉')
    if asset_struct.get('is_ncd_heavy'):
        tags.append('💰 存单重仓')
    if dur_grade in ('A+', 'A'):
        tags.append('⏱️ 久期精准')
    if hhi > 1500:
        tags.append('🎯 高集中度')
    if one_vote_veto:
        tags.append('❌ 一票否决')

    return {
        'headline': headline,
        'fund_label': fund_label,
        'scores_summary': scores_summary,
        'income_source': income_source,
        'risk_check': risk_check,
        'advice': advice,
        'duration_highlight': duration_highlight,
        'duration_grade_note': duration_grade_note,
        'macro_notes': macro_notes,
        'tags': tags,
        'overall_grade': grade,
        'one_vote_veto': one_vote_veto,
    }

