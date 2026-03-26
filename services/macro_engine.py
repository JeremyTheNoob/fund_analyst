"""
宏观分析插件（MacroEngine）
============================

为纯债基金提供"数据驱动的条件插件"：
  1. 宏观利率分析（利率债占比>50%触发）
  2. 行业信用利差分析（信用债占比>50%触发）
  3. 核心持仓发行人预警（单券占比>5%触发）

设计原则：
  - 不是万字长文，只输出"结论关联法"三段式
  - 静态化文案 + 动态化指标 → 自动套公式
  - 容错：数据缺失时退化为通用文案，不中断流程
  - 宏观观点仅需每两周更新一次

依赖：data.fetcher
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta


# ============================================================
# 🌊 宏观利率分析（利率债占比>50%时触发）
# ============================================================

def get_rate_bond_macro_analysis(
    market_indicators: dict,
    rate_ratio: float,
    duration: float,
) -> str:
    """
    利率债宏观环境分析（三段式结论关联）

    Args:
        market_indicators: fetch_market_indicators()的结果
        rate_ratio: 利率债占比
        duration: 基金有效久期

    Returns:
        三段式分析文本
    """
    if rate_ratio < 0.50:
        return ''

    if not market_indicators or market_indicators.get('current_y10y') is None:
        return (
            f"【宏观环境】该基金持有{rate_ratio*100:.0f}%的利率债，"
            f"对利率环境较为敏感。当前宏观数据获取失败，"
            f"建议关注10Y国债收益率走势，利率上行时需警惕净值回调。"
        )

    y10y = market_indicators.get('current_y10y', 2.5)
    pct = market_indicators.get('y10y_percentile', 50.0)
    spread_status = market_indicators.get('term_spread_status', 'normal')
    trend = market_indicators.get('y10y_trend', 'flat')

    # 利率分位数描述
    if pct < 10:
        pct_desc = '历史极低位（低配性价比低，利率易升难降）'
        view = '资本利得空间有限，估值回调压力较大'
        risk_level = '⚠️ 高'
    elif pct < 30:
        pct_desc = f'历史偏低位（{pct:.0f}%分位）'
        view = '利率继续下行空间有限，但短期趋势仍偏强'
        risk_level = '🟡 中等'
    elif pct > 80:
        pct_desc = f'历史高位（{pct:.0f}%分位）'
        view = '利率已在高位，具有较高的安全边际，适合布局中长久期'
        risk_level = '🟢 低'
    else:
        pct_desc = f'历史中性位置（{pct:.0f}%分位）'
        view = '利率处于均衡区间，收益主要来自票息积累'
        risk_level = '🟢 正常'

    # 期限结构描述
    spread_desc_map = {
        'flat': '曲线极度平坦，做多长端性价比低，短端更具吸引力',
        'normal': '曲线形态正常，期限溢价结构健康',
        'steep': '曲线陡峭，长端利率高于短端，布局长端有较高的骑乘收益',
        'unknown': '期限利差数据暂缺',
    }
    spread_desc = spread_desc_map.get(spread_status, '')

    # 久期操作建议
    if trend == 'down' and duration > 3:
        op_suggestion = f'✅ 当前利率下行趋势中，基金主动持有{duration:.1f}年久期，方向正确，可关注资本利得机会'
    elif trend == 'up' and duration > 3:
        op_suggestion = f'⚠️ 当前利率上行压力下，{duration:.1f}年的高久期存在净值回调风险，建议关注利率拐点'
    elif trend == 'up' and duration <= 2:
        op_suggestion = f'✅ 利率上行期中，经理已将久期压缩至{duration:.1f}年，有效规避了利率风险'
    else:
        op_suggestion = f'当前久期{duration:.1f}年，利率走势平稳，收益主要来自票息累积'

    return (
        f"【宏观利率穿透】该基金持有{rate_ratio*100:.0f}%利率债，\n"
        f"📍 利率环境：当前10Y国债收益率{y10y:.2f}%，处于{pct_desc}。\n"
        f"📍 期限结构：{spread_desc}。\n"
        f"📍 风险等级：{risk_level}。\n"
        f"📍 操作解读：{op_suggestion}。\n"
        f"📍 结论：{view}。"
    )


# ============================================================
# 💼 行业信用利差分析（信用债占比>50%时触发）
# ============================================================

def get_credit_bond_industry_analysis(
    asset_structure: dict,
    credit_quality: dict,
) -> str:
    """
    信用债行业风险穿透（三段式结论关联）

    Args:
        asset_structure: analyze_asset_structure()结果
        credit_quality: analyze_credit_quality()结果

    Returns:
        三段式分析文本
    """
    credit_ratio = asset_structure.get('credit_ratio', 0)
    if credit_ratio < 0.50:
        return ''

    type_dist = asset_structure.get('type_distribution', [])
    wacs = credit_quality.get('wacs', 80)
    sinking_ratio = credit_quality.get('sinking_ratio', 0)

    # 识别主要信用债类型
    credit_types = [t for t in type_dist if t.get('macro_type') == 'credit']
    if not credit_types:
        top_type = '信用债'
        top_ratio = credit_ratio
    else:
        top_type = credit_types[0]['type']
        top_ratio = credit_types[0]['ratio']

    # 按类型给出针对性分析
    industry_analysis = _get_industry_credit_view(top_type, top_ratio)

    # 下沉系数分析
    if sinking_ratio > 0.30:
        sinking_desc = (
            f"⚠️ 信用下沉显著：AA+及以下债券占比{sinking_ratio*100:.0f}%，"
            f"经理采用'下沉信用'策略博取高收益溢价，适合风险偏好较高的投资者"
        )
    elif sinking_ratio > 0.15:
        sinking_desc = (
            f"适度下沉：AA+及以下占比{sinking_ratio*100:.0f}%，"
            f"在控制风险的前提下适度获取信用溢价"
        )
    else:
        sinking_desc = (
            f"信用质量高：{sinking_ratio*100:.0f}%为AA+以下，"
            f"底层资产以高等级债券为主，安全边际较高"
        )

    return (
        f"【行业信用穿透】该基金持有{credit_ratio*100:.0f}%信用债，\n"
        f"📍 主要类型：{industry_analysis}\n"
        f"📍 信用质量：WACS加权评分{wacs:.0f}分（满分100），{_wacs_desc(wacs)}。\n"
        f"📍 下沉分析：{sinking_desc}。"
    )


def _get_industry_credit_view(bond_type: str, ratio: float) -> str:
    """根据债券类型给出针对性行业观点"""
    views = {
        '城投债': (
            f"持有{ratio*100:.0f}%城投债。"
            f"城投债当前处于'化债'政策红利期，信用利差持续收窄，"
            f"短期违约风险极低，但利差已压至历史低位，安全垫较薄，"
            f"需警惕政策转向后的估值回调风险。"
        ),
        '地产债': (
            f"持有{ratio*100:.0f}%地产债。"
            f"地产行业信用修复进程不均，"
            f"国央企地产债与民营地产债利差分化显著，"
            f"需关注主要发行人的销售回款和债务滚续情况，"
            f"建议聚焦国央企发行人，规避民营高杠杆主体。"
        ),
        '金融债': (
            f"持有{ratio*100:.0f}%金融债（银行/非银）。"
            f"商业银行债和政策性金融债信用资质稳健，"
            f"但票息较低；证券公司债需关注业绩波动风险。"
        ),
        '产业债': (
            f"持有{ratio*100:.0f}%产业债。"
            f"产业债行业分化较大，煤炭/电力等资源类企业现金流稳健，"
            f"制造业需关注盈利周期波动对偿债能力的影响。"
        ),
        '企业债': (
            f"持有{ratio*100:.0f}%企业债/公司债。"
            f"此类债券信用质量差异较大，建议结合发行人评级和到期分布综合判断。"
        ),
    }
    return views.get(bond_type, f"持有{ratio*100:.0f}%信用债，整体以中高等级为主。")


def _wacs_desc(wacs: float) -> str:
    if wacs >= 95:
        return '近乎全部为AAA级，违约风险极低'
    elif wacs >= 85:
        return '以AAA/AA+为主，信用质量优良'
    elif wacs >= 70:
        return '以AA+/AA为主，信用质量良好'
    else:
        return '含有一定比例AA及以下债券，需关注信用风险'


# ============================================================
# 🔍 核心持仓发行人预警（单券>5%时触发）
# ============================================================

def get_key_issuer_alert(
    issuer_concentration: list,
    threshold: float = 0.05,
) -> list:
    """
    核心持仓发行人预警

    Args:
        issuer_concentration: analyze_bond_concentration()中的issuer_concentration列表
        threshold: 触发阈值（默认5%）

    Returns:
        预警信息列表，每条为 {'issuer', 'weight', 'alert_level', 'message'}
    """
    alerts = []
    for item in issuer_concentration:
        ratio = item.get('ratio', 0)
        issuer = item.get('issuer', '')
        weight = item.get('weight', 0)

        if ratio < threshold:
            continue

        # 简单规则判断（实际应接入舆情API）
        risk_keywords = ['恒大', '融创', '碧桂园', '华夏幸福', '雅居乐', '世茂']
        has_risk = any(kw in issuer for kw in risk_keywords)

        if ratio > 0.15:
            level = 'high'
            emoji = '🚨'
            msg = (f"重大集中度风险：{issuer}占比{ratio*100:.1f}%，"
                   f"单只发行人权重过高，一旦出现信用事件将严重冲击净值")
        elif has_risk:
            level = 'high'
            emoji = '⚠️'
            msg = f"风险发行人预警：{issuer}（占比{ratio*100:.1f}%），历史信用事件较多，建议保持关注"
        elif ratio > 0.08:
            level = 'medium'
            emoji = '🟡'
            msg = f"中等集中度：{issuer}占比{ratio*100:.1f}%，建议关注该发行人最新动态"
        else:
            level = 'low'
            emoji = '📋'
            msg = f"{issuer}占比{ratio*100:.1f}%，需穿透确认同一母公司下无其他债券持仓"

        alerts.append({
            'issuer': issuer,
            'weight': weight,
            'ratio': ratio,
            'alert_level': level,
            'emoji': emoji,
            'message': msg,
        })

    return sorted(alerts, key=lambda x: -x['ratio'])


# ============================================================
# 🎯 综合宏观插件入口
# ============================================================

def get_macro_plugin(
    portfolio_summary: dict,
    asset_structure: dict,
    credit_quality: dict,
    concentration: dict,
    duration_results: dict,
    market_indicators: dict = None,
) -> dict:
    """
    综合宏观分析插件（主入口）

    根据持仓特征自动选择触发的分析模块：
    - 利率债>50% → 宏观利率分析
    - 信用债>50% → 行业信用分析
    - 单券>5%    → 发行人预警

    Args:
        portfolio_summary: 持仓摘要（含 rate_ratio/credit_ratio）
        asset_structure: 券种结构分析结果
        credit_quality: 信用资质分析结果
        concentration: 集中度分析结果
        duration_results: 久期系统分析结果
        market_indicators: 宏观指标快照（可为None）

    Returns:
        {
          'rate_macro_text': str,        # 利率宏观分析文本
          'credit_industry_text': str,   # 信用行业分析文本
          'issuer_alerts': list,         # 发行人预警列表
          'has_macro_content': bool,     # 是否有宏观内容
          'macro_risk_level': str,       # '低'/'中'/'高'
        }
    """
    rate_ratio = asset_structure.get('rate_ratio', 0)
    credit_ratio = asset_structure.get('credit_ratio', 0)
    duration = duration_results.get('duration', 2.0)

    # 宏观利率分析（利率债>50%触发）
    rate_text = ''
    if rate_ratio > 0.50:
        rate_text = get_rate_bond_macro_analysis(
            market_indicators=market_indicators,
            rate_ratio=rate_ratio,
            duration=duration,
        )

    # 信用行业分析（信用债>50%触发）
    credit_text = ''
    if credit_ratio > 0.50:
        credit_text = get_credit_bond_industry_analysis(
            asset_structure=asset_structure,
            credit_quality=credit_quality,
        )

    # 发行人预警
    issuer_alerts = get_key_issuer_alert(
        issuer_concentration=concentration.get('issuer_concentration', []),
        threshold=0.05,
    )

    # 宏观风险等级汇总
    pct = (market_indicators or {}).get('y10y_percentile', 50)
    wacs = credit_quality.get('wacs', 80)
    hhi = concentration.get('static_hhi', 500)
    high_risk_alerts = [a for a in issuer_alerts if a['alert_level'] == 'high']

    if high_risk_alerts or (pct is not None and pct < 10) or wacs < 70:
        macro_risk = '⚠️ 高'
    elif (pct is not None and pct < 30) or wacs < 80 or hhi > 1500:
        macro_risk = '🟡 中等'
    else:
        macro_risk = '🟢 低'

    return {
        'rate_macro_text': rate_text,
        'credit_industry_text': credit_text,
        'issuer_alerts': issuer_alerts,
        'has_macro_content': bool(rate_text or credit_text or issuer_alerts),
        'macro_risk_level': macro_risk,
    }
