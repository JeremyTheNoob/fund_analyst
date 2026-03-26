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
