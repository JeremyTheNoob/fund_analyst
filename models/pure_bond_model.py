"""
纯债基金分析模型（Pure Bond Fund Model）
=========================================

架构设计：3+2 分析体系
  - 静态三维：券种结构 / 信用资质 / 持仓集中度
  - 动态两维：久期择时能力 / 三因子Alpha

评分系统：
  - 底层资产质量得分 Score_quality = 0.4×S_credit + 0.3×S_conc + 0.3×S_struct
  - 久期择时得分 S_duration（可突破100分，最高120分）
  - 综合风险指数 R = Duration × HHI × (100 - WACS)

识别条件（三重过滤）：
  1. 债券占比均值 > 90%
  2. 股票仓位 = 0 + 可转债 < 5%
  3. 与中债综合财富指数相关性 > 0.8

依赖：config, utils, data
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from typing import Optional

import config
from utils.helpers import (
    normalize_score,
    safe_divide,
    annualize_return,
    annualize_volatility,
    calculate_sharpe,
    calculate_max_drawdown,
)


# ============================================================
# 📌 债券名称 → 债券类型 映射规则
# ============================================================

# 债券类型识别关键词（按优先级排序）
BOND_TYPE_KEYWORDS = {
    '国债':        ['国债', '记账式', '储蓄国债'],
    '政金债':      ['国开', '农发', '进出口', '政金', '口行', '农发行', '国家开发'],
    '同业存单':    ['同业存单', 'NCD', '存单'],
    '可转债':      ['可转债', '转债', '可交换'],
    '城投债':      ['城投', '建设投资', '交通投资', '开发投资', '产业投资', '国有资本',
                    '城市建设', '城市发展', '城建', '城市投资'],
    '金融债':      ['银行', '证券', '保险', '信托', '基金', '金融债', '商业银行',
                    '次级', '混合资本'],
    '地产债':      ['房地产', '地产', '置业', '房产', '物业', '地产', '万科', '保利',
                    '碧桂园', '恒大', '中海', '融创', '绿地', '龙湖'],
    '产业债':      ['煤炭', '电力', '钢铁', '化工', '医药', '交通', '电信', '航空',
                    '高速', '港口', '能源', '矿业'],
    '企业债':      ['企业债', '公司债', '短期融资券', 'CP', 'MTN', '中票', '超短融'],
    '资产支持证券': ['ABS', 'CLO', '资产支持', '专项计划', '信贷资产'],
}

# 债券类型 → 利率/信用/同业存单 大类分组
BOND_MACRO_TYPE = {
    '国债':        'rate',      # 利率债
    '政金债':      'rate',      # 利率债
    '同业存单':    'ncd',       # 同业存单（单列）
    '可转债':      'convert',   # 可转债（单列）
    '城投债':      'credit',    # 信用债
    '金融债':      'credit',    # 信用债
    '地产债':      'credit',    # 信用债
    '产业债':      'credit',    # 信用债
    '企业债':      'credit',    # 信用债
    '资产支持证券':'credit',    # 信用债
    '其他':        'other',
}

# 信用评级 → 数字分（WACS评分映射表）
CREDIT_SCORE_MAP = {
    'AAA':       100,
    'AA+':        80,
    'AA':         60,
    'AA-':        40,
    'A+':         20,
    'A':          10,
    'A-':          5,
    'BBB+':        2,
    'BBB':         1,
    '未评级':       0,
    '无评级':       0,
    '':            0,
}

# 基金类型 → 标准久期区间
FUND_DURATION_STANDARD = {
    '短债':      (0.3, 1.0),   # 标准: 0.3-1.0年
    '中短债':    (1.0, 2.0),   # 标准: 1.0-2.0年
    '中长债':    (2.0, 5.0),   # 标准: 2.0-5.0年
    '长债':      (5.0, 15.0),  # 标准: 5.0-15.0年
    '纯债':      (1.0, 5.0),   # 通用纯债: 1.0-5.0年
    '货币':      (0.0, 0.5),   # 货币: 极短
}


# ============================================================
# 🔍 Step 1: 纯债基金识别器（三重过滤）
# ============================================================

def identify_pure_bond_fund(
    nav_data: pd.DataFrame,
    holdings_data: dict,
    quarterly_alloc_history: pd.DataFrame,
) -> dict:
    """
    三重过滤条件验证纯债基金身份

    Args:
        nav_data: 净值数据 DataFrame(date, nav, ret)
        holdings_data: 持仓数据字典（stock_ratio/bond_ratio/bond_holdings等）
        quarterly_alloc_history: 季报资产配置历史 DataFrame(period, stock_ratio, bond_ratio)

    Returns:
        {
          'is_pure_bond': bool,
          'filter1_bond_ratio': float,  # 债券占比均值
          'filter2_stock_ratio': float, # 股票仓位
          'filter2_convert_ratio': float, # 可转债占比
          'filter3_bond_corr': float,   # 与中债指数相关性
          'filter3_equity_corr': float, # 与沪深300相关性
          'pass_filter1': bool,
          'pass_filter2': bool,
          'pass_filter3': bool,
          'fund_subtype': str,          # '纯债'/'短债'/'中短债'等
          'notes': list[str],
        }
    """
    notes = []
    result = {
        'is_pure_bond': False,
        'filter1_bond_ratio': 0.0,
        'filter2_stock_ratio': 0.0,
        'filter2_convert_ratio': 0.0,
        'filter3_bond_corr': None,
        'filter3_equity_corr': None,
        'pass_filter1': False,
        'pass_filter2': False,
        'pass_filter3': False,
        'fund_subtype': '纯债',
        'notes': notes,
    }

    # ── Filter 1: 债券占比均值 > 90% ──────────────────────────
    bond_ratios = []
    if not quarterly_alloc_history.empty and 'bond_ratio' in quarterly_alloc_history.columns:
        bond_ratios = quarterly_alloc_history['bond_ratio'].dropna().tolist()

    # 补充当前最新持仓
    cur_bond = holdings_data.get('bond_ratio', 0.0)
    if cur_bond > 0:
        bond_ratios.append(cur_bond)

    if bond_ratios:
        avg_bond = np.mean(bond_ratios)
        result['filter1_bond_ratio'] = round(avg_bond, 4)
        result['pass_filter1'] = avg_bond > 0.90
        if not result['pass_filter1']:
            notes.append(f"Filter1未通过：债券占比均值={avg_bond*100:.1f}%，需>90%")
    else:
        # 没有历史数据，使用当前持仓保守估计
        result['filter1_bond_ratio'] = cur_bond
        result['pass_filter1'] = cur_bond > 0.80
        notes.append("Filter1：仅用当前持仓估算（历史季报数据缺失）")

    # ── Filter 2: 股票仓位=0 + 可转债<5% ──────────────────────
    stock_ratio = holdings_data.get('stock_ratio', 0.0)
    result['filter2_stock_ratio'] = stock_ratio

    # 计算可转债占比
    convert_ratio = _calc_convert_ratio(holdings_data.get('bond_holdings', pd.DataFrame()))
    result['filter2_convert_ratio'] = round(convert_ratio, 4)

    pass_stock = stock_ratio < 0.01     # 股票仓位几乎为0
    pass_convert = convert_ratio < 0.05  # 可转债<5%
    result['pass_filter2'] = pass_stock and pass_convert

    if not pass_stock:
        notes.append(f"Filter2未通过：股票仓位={stock_ratio*100:.1f}%，需<1%")
    if not pass_convert:
        notes.append(f"Filter2未通过：可转债占比={convert_ratio*100:.1f}%，需<5%（股性干扰久期回归）")

    # ── Filter 3: 相关性验证 ──────────────────────────────────
    if not nav_data.empty and 'ret' in nav_data.columns:
        try:
            from data.fetcher import fetch_bond_index_corr_data
            nav_sorted = nav_data.sort_values('date') if 'date' in nav_data.columns else nav_data
            start = nav_sorted['date'].min().strftime('%Y%m%d') if 'date' in nav_sorted.columns else ''
            end = nav_sorted['date'].max().strftime('%Y%m%d') if 'date' in nav_sorted.columns else ''

            if start and end:
                corr_data = fetch_bond_index_corr_data(start, end)
                if not corr_data.empty:
                    fund_ret = nav_sorted.set_index('date')['ret'] if 'date' in nav_sorted.columns else nav_sorted['ret']
                    merged = corr_data.merge(
                        fund_ret.rename('fund_ret').reset_index(),
                        on='date', how='inner'
                    )
                    if len(merged) > 30:
                        if 'bond_ret' in merged.columns:
                            bond_corr = merged['fund_ret'].corr(merged['bond_ret'])
                            result['filter3_bond_corr'] = round(float(bond_corr), 3)
                        if 'equity_ret' in merged.columns:
                            eq_corr = merged['fund_ret'].corr(merged['equity_ret'])
                            result['filter3_equity_corr'] = round(float(eq_corr), 3)

                        bond_c = result.get('filter3_bond_corr') or 0
                        eq_c = abs(result.get('filter3_equity_corr') or 0)
                        result['pass_filter3'] = bond_c > 0.8 and eq_c < 0.3
                        if not result['pass_filter3']:
                            notes.append(
                                f"Filter3：与中债相关={bond_c:.2f}(需>0.8), "
                                f"与股票相关={result.get('filter3_equity_corr', 'N/A')}(需~0)"
                            )
        except Exception as e:
            notes.append(f"Filter3：相关性计算失败（{e}），使用宽松判定")
            result['pass_filter3'] = True  # 数据缺失时不阻断分析

    else:
        result['pass_filter3'] = True
        notes.append("Filter3：净值数据不足，跳过相关性验证")

    # ── 综合判定 ─────────────────────────────────────────────
    result['is_pure_bond'] = (
        result['pass_filter1'] and
        result['pass_filter2'] and
        result['pass_filter3']
    )

    if not result['is_pure_bond']:
        notes.append("⚠️ 该基金不满足纯债基金三重识别条件，将使用通用债券模型分析")

    return result


def _calc_convert_ratio(bond_holdings: pd.DataFrame) -> float:
    """计算可转债在债券持仓中的占比"""
    if bond_holdings.empty or '占净值比例' not in bond_holdings.columns:
        return 0.0

    total = bond_holdings['占净值比例'].sum()
    if total <= 0:
        return 0.0

    convert_mask = bond_holdings.get('债券名称', pd.Series(dtype=str)).apply(
        lambda x: any(kw in str(x) for kw in BOND_TYPE_KEYWORDS.get('可转债', []))
    )
    convert_weight = bond_holdings.loc[convert_mask, '占净值比例'].sum()
    return float(convert_weight / total)


# ============================================================
# 🏗️ Step 2: 券种结构分析
# ============================================================

def analyze_asset_structure(bond_holdings: pd.DataFrame) -> dict:
    """
    券种结构分析：分类计算利率债/信用债/同业存单/可转债各比率

    Args:
        bond_holdings: 债券持仓 DataFrame（含债券名称/占净值比例/信用等级）

    Returns:
        {
          'rate_ratio': float,       # 利率债占比
          'credit_ratio': float,     # 信用债占比
          'ncd_ratio': float,        # 同业存单占比
          'convert_ratio': float,    # 可转债占比
          'other_ratio': float,      # 其他
          'bond_type_breakdown': dict,  # 细分类型占比
          'rate_sensitivity': float,    # 利率敏感度比率（利率债/总持仓）
          'credit_exposure': float,     # 信用暴露度（信用债/总持仓）
          'cash_substitute_rate': float,# 现金替代率（同业存单/总资产估算）
          'is_ncd_heavy': bool,         # 存单占比>50%（大号货币基金警告）
          'quality_score': float,       # 券种结构得分（0-100）
          'leverage_ratio': float,      # 杠杆率（可从净值>100%推断）
          'type_distribution': list,    # UI展示用列表
        }
    """
    default_result = {
        'rate_ratio': 0.0, 'credit_ratio': 0.0, 'ncd_ratio': 0.0,
        'convert_ratio': 0.0, 'other_ratio': 0.0,
        'bond_type_breakdown': {}, 'rate_sensitivity': 0.0,
        'credit_exposure': 0.0, 'cash_substitute_rate': 0.0,
        'is_ncd_heavy': False, 'quality_score': 60.0,
        'leverage_ratio': 1.0, 'type_distribution': [],
    }

    if bond_holdings.empty:
        return default_result

    # 识别各债券类型
    name_col = _find_col(bond_holdings, ['债券名称', '证券名称', '名称'])
    weight_col = _find_col(bond_holdings, ['占净值比例', '比例', '权重'])

    if not name_col or not weight_col:
        return default_result

    type_weights = {}
    for _, row in bond_holdings.iterrows():
        bond_name = str(row.get(name_col, ''))
        weight = float(row.get(weight_col, 0) or 0)

        btype = _classify_bond_type(bond_name)
        type_weights[btype] = type_weights.get(btype, 0) + weight

    total_w = sum(type_weights.values())
    if total_w <= 0:
        return default_result

    # 计算大类占比
    rate_w = sum(v for k, v in type_weights.items() if BOND_MACRO_TYPE.get(k) == 'rate')
    credit_w = sum(v for k, v in type_weights.items() if BOND_MACRO_TYPE.get(k) == 'credit')
    ncd_w = type_weights.get('同业存单', 0)
    convert_w = type_weights.get('可转债', 0)
    other_w = total_w - rate_w - credit_w - ncd_w - convert_w

    rate_r = rate_w / total_w
    credit_r = credit_w / total_w
    ncd_r = ncd_w / total_w
    convert_r = convert_w / total_w
    other_r = max(0, other_w / total_w)

    # 判断杠杆（简化：如果总权重>100%说明有杠杆）
    leverage = total_w / 100.0 if total_w > 100 else 1.0

    # 券种结构得分 S_struct（0-100）
    quality_score = _score_asset_structure(rate_r, credit_r, ncd_r, convert_r, leverage)

    # UI展示列表
    type_dist = []
    for btype, w in sorted(type_weights.items(), key=lambda x: -x[1]):
        if w > 0:
            macro = BOND_MACRO_TYPE.get(btype, 'other')
            color_map = {'rate': '#3498db', 'credit': '#e74c3c', 'ncd': '#2ecc71',
                         'convert': '#f39c12', 'other': '#95a5a6'}
            type_dist.append({
                'type': btype,
                'weight': round(w, 2),
                'ratio': round(w / total_w, 4),
                'macro_type': macro,
                'color': color_map.get(macro, '#95a5a6'),
            })

    return {
        'rate_ratio': round(rate_r, 4),
        'credit_ratio': round(credit_r, 4),
        'ncd_ratio': round(ncd_r, 4),
        'convert_ratio': round(convert_r, 4),
        'other_ratio': round(other_r, 4),
        'bond_type_breakdown': {k: round(v / total_w, 4) for k, v in type_weights.items()},
        'rate_sensitivity': round(rate_r, 4),
        'credit_exposure': round(credit_r, 4),
        'cash_substitute_rate': round(ncd_r, 4),
        'is_ncd_heavy': ncd_r > 0.50,
        'quality_score': quality_score,
        'leverage_ratio': round(leverage, 3),
        'total_weight': round(total_w, 2),
        'type_distribution': type_dist,
    }


def _classify_bond_type(bond_name: str) -> str:
    """从债券名称识别债券类型（按优先级）"""
    for btype, keywords in BOND_TYPE_KEYWORDS.items():
        if any(kw in bond_name for kw in keywords):
            return btype
    return '其他'


def _score_asset_structure(
    rate_r: float, credit_r: float, ncd_r: float,
    convert_r: float, leverage: float
) -> float:
    """
    券种结构得分（S_struct，0-100）

    规则：
    - 利率债+AAA信用债>80%: 100分
    - 同业存单>50%: 70分（流动性管理工具）
    - 杠杆>120%: ×0.9折扣
    - 可转债>10%: -10分（股性干扰）
    """
    # 基础分：以高质量资产占比为主
    high_quality = rate_r + credit_r * 0.5   # 信用债质量打折
    if high_quality > 0.80:
        base = 100.0
    elif high_quality > 0.60:
        base = 85.0
    elif ncd_r > 0.50:
        base = 70.0   # 大号货币基金
    else:
        base = 65.0

    # 惩罚项
    if convert_r > 0.10:
        base -= 10.0   # 可转债股性干扰
    elif convert_r > 0.05:
        base -= 5.0

    # 杠杆折扣
    if leverage > 1.40:
        base *= 0.80
    elif leverage > 1.20:
        base *= 0.90

    return round(max(0, min(100, base)), 1)


# ============================================================
# 💳 Step 3: 信用资质分析
# ============================================================

def analyze_credit_quality(bond_holdings: pd.DataFrame) -> dict:
    """
    信用资质分析：WACS加权评分 + 信用下沉系数

    Args:
        bond_holdings: 债券持仓（含债券名称/占净值比例/信用等级）

    Returns:
        {
          'wacs': float,              # 加权平均信用分（0-100）
          'wacs_rating': str,         # 文字评级（AAA/AA+/AA等）
          'sinking_ratio': float,     # AA+及以下占比（信用下沉系数）
          'is_credit_sinking': bool,  # 是否存在信用下沉
          'rating_breakdown': dict,   # 各评级占比
          'credit_score': float,      # S_credit评分（0-100）
          'has_unrated': bool,        # 是否有未评级债券
          'implied_downgrade': list,  # 隐含评级下调的债券（基于收益率）
          'breakdown_list': list,     # UI展示用列表
        }
    """
    default_result = {
        'wacs': 80.0, 'wacs_rating': 'AA+', 'sinking_ratio': 0.2,
        'is_credit_sinking': False, 'rating_breakdown': {},
        'credit_score': 80.0, 'has_unrated': False,
        'implied_downgrade': [], 'breakdown_list': [],
    }

    if bond_holdings.empty:
        return default_result

    rating_col = _find_col(bond_holdings, ['信用等级', '评级', '债券评级', '信用评级'])
    weight_col = _find_col(bond_holdings, ['占净值比例', '比例', '权重'])
    name_col = _find_col(bond_holdings, ['债券名称', '证券名称'])

    if not weight_col:
        return default_result

    # 统计各评级权重
    rating_weights = {}
    total_w = 0.0
    sinking_w = 0.0   # AA+ 及以下
    unrated_w = 0.0

    breakdown_list = []
    for _, row in bond_holdings.iterrows():
        w = float(row.get(weight_col, 0) or 0)
        if w <= 0:
            continue
        total_w += w

        rating_raw = str(row.get(rating_col, '') or '') if rating_col else ''
        rating = _normalize_rating(rating_raw)
        score = CREDIT_SCORE_MAP.get(rating, 0)

        rating_weights[rating] = rating_weights.get(rating, 0) + w

        if rating in ('AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', '未评级', '无评级', ''):
            sinking_w += w
        if rating in ('未评级', '无评级', ''):
            unrated_w += w

        name = str(row.get(name_col, '')) if name_col else ''
        breakdown_list.append({
            'name': name[:20],
            'weight': round(w, 2),
            'rating': rating,
            'score': score,
        })

    if total_w <= 0:
        return default_result

    # WACS计算
    wacs = sum(
        CREDIT_SCORE_MAP.get(r, 0) * (w / total_w)
        for r, w in rating_weights.items()
    )
    sinking_ratio = sinking_w / total_w

    # 评级分布归一化
    rating_breakdown = {r: round(w / total_w, 4) for r, w in rating_weights.items()}

    # 信用得分 S_credit（0-100）
    credit_score = _score_credit_quality(wacs, sinking_ratio)

    # WACS → 文字评级
    wacs_rating = _wacs_to_rating(wacs)

    return {
        'wacs': round(wacs, 1),
        'wacs_rating': wacs_rating,
        'sinking_ratio': round(sinking_ratio, 4),
        'is_credit_sinking': sinking_ratio > 0.20,
        'rating_breakdown': rating_breakdown,
        'credit_score': credit_score,
        'has_unrated': unrated_w / total_w > 0.05,
        'implied_downgrade': [],   # TODO: 接入二级市场收益率偏离检测
        'breakdown_list': breakdown_list,
    }


def _normalize_rating(raw: str) -> str:
    """标准化评级字符串"""
    raw = raw.strip().upper()
    # 处理常见格式
    raw = raw.replace('Aaa', 'AAA').replace('Aa', 'AA')
    for key in CREDIT_SCORE_MAP:
        if raw == key.upper():
            return key
    # 模糊匹配
    if raw.startswith('AAA'):
        return 'AAA'
    if raw.startswith('AA+') or raw == 'AA＋':
        return 'AA+'
    if raw.startswith('AA-'):
        return 'AA-'
    if raw.startswith('AA'):
        return 'AA'
    if raw.startswith('A+'):
        return 'A+'
    if raw.startswith('A-'):
        return 'A-'
    if raw.startswith('A'):
        return 'A'
    return '未评级'


def _wacs_to_rating(wacs: float) -> str:
    """WACS分数 → 文字评级"""
    if wacs >= 95:
        return 'AAA'
    elif wacs >= 85:
        return 'AA+'
    elif wacs >= 70:
        return 'AA'
    elif wacs >= 50:
        return 'AA-'
    elif wacs >= 30:
        return 'A+'
    else:
        return 'A及以下'


def _score_credit_quality(wacs: float, sinking_ratio: float) -> float:
    """
    信用资质评分 S_credit（0-100）

    规则：
    - WACS≥95: 100分
    - 90≤WACS<95: 80分
    - WACS<90: 60分（基础）
    - AA+及以下占比>20%: -10分
    """
    if wacs >= 95:
        base = 100.0
    elif wacs >= 90:
        base = 80.0
    elif wacs >= 80:
        base = 70.0
    else:
        base = 60.0
        # 按WACS下沉幅度追加扣分
        extra_deduct = (80 - wacs) * 0.5
        base -= min(extra_deduct, 20)

    # 信用下沉惩罚
    if sinking_ratio > 0.20:
        base -= 10.0
    elif sinking_ratio > 0.10:
        base -= 5.0

    return round(max(0, min(100, base)), 1)


# ============================================================
# 📊 Step 4: 持仓集中度分析（静态+动态HHI）
# ============================================================

def analyze_bond_concentration(
    bond_holdings: pd.DataFrame,
    historical_holdings: Optional[dict] = None,
) -> dict:
    """
    债券持仓集中度分析：静态HHI + 动态HHI轨迹 + 发行人集中度

    Args:
        bond_holdings: 最新债券持仓
        historical_holdings: 历史持仓字典 {'2024': df, '2023': df, ...}

    Returns:
        {
          'static_hhi': float,          # 静态HHI（前十大债券）
          'top5_ratio': float,          # 前五大占比
          'top10_ratio': float,         # 前十大占比
          'hhi_level': str,             # 'low'/'medium'/'high'
          'dynamic_hhi': list,          # [{period, hhi}] 历史轨迹
          'hhi_trend': str,             # 'rising'/'falling'/'stable'
          'hhi_drift': float,           # 当前HHI偏离历史均值百分比
          'issuer_concentration': list, # 发行人集中度（合并同一母公司）
          'conc_score': float,          # S_conc评分（0-100）
          'dynamic_adjustment': float,  # 动态调整分
          'conc_score_final': float,    # 最终集中度评分
        }
    """
    default_result = {
        'static_hhi': 500.0, 'top5_ratio': 0.2, 'top10_ratio': 0.35,
        'hhi_level': 'low', 'dynamic_hhi': [], 'hhi_trend': 'stable',
        'hhi_drift': 0.0, 'issuer_concentration': [],
        'conc_score': 80.0, 'dynamic_adjustment': 0.0, 'conc_score_final': 80.0,
    }

    if bond_holdings.empty:
        return default_result

    weight_col = _find_col(bond_holdings, ['占净值比例', '比例'])
    name_col = _find_col(bond_holdings, ['债券名称', '证券名称'])

    if not weight_col:
        return default_result

    weights = bond_holdings[weight_col].fillna(0).values.astype(float)
    total_w = weights.sum()
    if total_w <= 0:
        return default_result

    # 归一化（将占净值比例转为权重）
    w_norm = weights / total_w

    # ── 静态HHI ────────────────────────────────────────────────
    # HHI = Σ(wᵢ * 100)² （权重转为百分数后求平方和）
    static_hhi = float(np.sum((w_norm * 100) ** 2))

    # 前五/十大占比
    top5_ratio = float(np.sum(sorted(w_norm, reverse=True)[:5]))
    top10_ratio = float(np.sum(sorted(w_norm, reverse=True)[:10]))

    if static_hhi < 500:
        hhi_level = 'low'
    elif static_hhi < 1500:
        hhi_level = 'medium'
    else:
        hhi_level = 'high'

    # ── 动态HHI轨迹 ────────────────────────────────────────────
    dynamic_hhi = [{'period': '当前', 'hhi': round(static_hhi, 1)}]
    if historical_holdings:
        for period, hist_df in sorted(historical_holdings.items(), reverse=True):
            if isinstance(hist_df, pd.DataFrame) and not hist_df.empty:
                w_col = _find_col(hist_df, ['占净值比例', '比例'])
                if w_col:
                    hw = hist_df[w_col].fillna(0).values.astype(float)
                    hw_total = hw.sum()
                    if hw_total > 0:
                        hw_norm = hw / hw_total
                        hist_hhi = float(np.sum((hw_norm * 100) ** 2))
                        dynamic_hhi.append({'period': period, 'hhi': round(hist_hhi, 1)})

    # HHI趋势判断（比较当前 vs 最早历史）
    hhi_trend = 'stable'
    hhi_drift = 0.0
    if len(dynamic_hhi) >= 3:
        recent_hhi = [d['hhi'] for d in dynamic_hhi[:3]]
        hist_hhi_all = [d['hhi'] for d in dynamic_hhi[1:]]
        hist_mean = np.mean(hist_hhi_all)
        hhi_drift = (static_hhi - hist_mean) / hist_mean if hist_mean > 0 else 0.0

        if dynamic_hhi[0]['hhi'] > dynamic_hhi[-1]['hhi'] * 1.2:
            hhi_trend = 'rising'    # 集中度上升（风险在增加）
        elif dynamic_hhi[0]['hhi'] < dynamic_hhi[-1]['hhi'] * 0.8:
            hhi_trend = 'falling'   # 集中度下降（风险在减小）

    # ── 发行人集中度（合并同一母公司） ────────────────────────
    issuer_conc = []
    if name_col:
        issuer_map = {}
        for i, row in bond_holdings.iterrows():
            bname = str(row.get(name_col, ''))
            bw = float(row.get(weight_col, 0) or 0)
            issuer = _extract_issuer(bname)
            issuer_map[issuer] = issuer_map.get(issuer, 0) + bw

        for issuer, w in sorted(issuer_map.items(), key=lambda x: -x[1])[:10]:
            issuer_conc.append({
                'issuer': issuer,
                'weight': round(w, 2),
                'ratio': round(w / total_w, 4),
            })

    # ── 集中度评分 S_conc ────────────────────────────────────
    conc_score = _score_concentration(static_hhi)
    dynamic_adj = -5.0 if abs(hhi_drift) > 0.30 else 0.0  # 偏离>30%扣5分
    conc_score_final = max(0, min(100, conc_score + dynamic_adj))

    return {
        'static_hhi': round(static_hhi, 1),
        'top5_ratio': round(top5_ratio, 4),
        'top10_ratio': round(top10_ratio, 4),
        'hhi_level': hhi_level,
        'dynamic_hhi': dynamic_hhi,
        'hhi_trend': hhi_trend,
        'hhi_drift': round(hhi_drift, 4),
        'issuer_concentration': issuer_conc,
        'conc_score': conc_score,
        'dynamic_adjustment': dynamic_adj,
        'conc_score_final': round(conc_score_final, 1),
    }


def _extract_issuer(bond_name: str) -> str:
    """
    从债券名称提取发行人（去除年份、期数、评级等后缀）
    如 '23万科01' → '万科'，'21城投债02' → '城投'
    """
    import re
    # 去除年份前缀（2位或4位）
    name = re.sub(r'^[0-9]{2,4}', '', bond_name)
    # 去除末尾的数字期数
    name = re.sub(r'[0-9]+$', '', name)
    # 去除常见后缀
    for suffix in ['企业债', '公司债', '中票', 'MTN', '短融', 'SCP', 'CP',
                   '债券', '转债', '存单', '可转债']:
        name = name.replace(suffix, '')
    return name.strip()[:10] or bond_name[:10]


def _score_concentration(hhi: float) -> float:
    """
    集中度评分 S_conc（0-100）

    规则：
    - HHI<500: 100分（极度分散）
    - 500≤HHI<1500: 80分（适度集中）
    - HHI≥1500: 60分（高度集中）
    """
    if hhi < 500:
        return 100.0
    elif hhi < 1000:
        return 90.0
    elif hhi < 1500:
        return 80.0
    elif hhi < 2500:
        return 65.0
    else:
        return 50.0


# ============================================================
# ⏱️ Step 5: 久期体系（有效久期 + 择时评分）
# ============================================================

def analyze_duration_system(
    fund_ret: pd.Series,
    nav_data: pd.DataFrame,
    fund_subtype: str = '纯债',
    market_indicators: dict = None,
) -> dict:
    """
    久期完整分析体系：有效久期 + 类型匹配度 + 择时加分 + DE指数

    Args:
        fund_ret: 基金日收益率序列
        nav_data: 净值数据 DataFrame(date, nav, ret)
        fund_subtype: 基金子类型（短债/中短债/中长债/纯债）
        market_indicators: 宏观指标快照（来自 fetch_market_indicators）

    Returns:
        {
          'duration': float,            # 有效久期（年）
          'duration_r_squared': float,  # 回归R²
          'fund_subtype': str,
          'standard_range': tuple,      # 标准久期区间
          'is_in_standard': bool,       # 是否在标准区间内
          'drift_score': float,         # 类型匹配得分（基础分，可0-100）
          'timing_score': float,        # 择时加分（-15到+15）
          'duration_score': float,      # 综合久期评分（可>100）
          'duration_grade': str,        # 评级 A+/A/B/C/D
          'duration_efficiency': float, # DE = Alpha / Duration
          'carry_efficiency': float,    # Efficiency = Carry / Duration
          'stress_10bp': float,         # 利率上行10BP预计跌幅
          'stress_30bp': float,         # 利率上行30BP预计跌幅
          'interpretation': str,
        }
    """
    result = {
        'duration': 2.0, 'duration_r_squared': 0.0,
        'fund_subtype': fund_subtype,
        'standard_range': FUND_DURATION_STANDARD.get(fund_subtype, (1.0, 5.0)),
        'is_in_standard': True,
        'drift_score': 80.0, 'timing_score': 0.0, 'duration_score': 80.0,
        'duration_grade': 'B',
        'duration_efficiency': 0.0, 'carry_efficiency': 0.0,
        'stress_10bp': -0.20, 'stress_30bp': -0.60,
        'interpretation': '',
    }

    if fund_ret.empty or len(fund_ret) < 60:
        result['interpretation'] = '数据不足(<60天)，无法进行久期分析'
        return result

    # ── 1. 有效久期回归 ────────────────────────────────────────
    duration, r_squared = _regress_duration(fund_ret, nav_data)
    result['duration'] = duration
    result['duration_r_squared'] = r_squared

    # ── 2. 类型匹配得分（基础分）────────────────────────────────
    std_range = FUND_DURATION_STANDARD.get(fund_subtype, (1.0, 5.0))
    d_min, d_max = std_range
    drift_score, is_in_std, drift_note = _score_duration_match(duration, d_min, d_max)
    result['standard_range'] = std_range
    result['is_in_standard'] = is_in_std
    result['drift_score'] = drift_score

    # ── 3. 择时加分（结合宏观利率趋势）────────────────────────
    timing_score = 0.0
    timing_note = ''
    if market_indicators and market_indicators.get('y10y_trend'):
        trend = market_indicators['y10y_trend']
        if trend == 'down':  # 债牛：利率下行
            # 久期高且处于区间中高位 → 加分
            mid_point = (d_min + d_max) / 2
            if duration >= mid_point and is_in_std:
                timing_score = 12.0
                timing_note = '债牛行情中主动拉长久期，择时加分+12'
            elif duration >= mid_point and not is_in_std:
                timing_score = 5.0
                timing_note = '虽久期偏高，但顺势操作，小额加分+5'
        elif trend == 'up':  # 债熊：利率上行
            # 久期缩短 → 加分
            if duration < (d_min + d_max) / 2:
                timing_score = 15.0
                timing_note = '债熊行情中成功缩短久期，风险控制加分+15'
            else:
                timing_score = -5.0
                timing_note = '债熊行情中久期偏高，未有效规避利率风险，扣分-5'
        else:
            timing_note = '利率趋势平稳，择时贡献中性'

    result['timing_score'] = timing_score

    # ── 4. 综合久期评分（可突破100分）────────────────────────────
    duration_score = drift_score + timing_score
    result['duration_score'] = round(max(0, duration_score), 1)

    # 久期评级映射
    result['duration_grade'] = _score_to_grade(duration_score)

    # ── 5. 效率指标 ────────────────────────────────────────────
    # 压力测试（简化）
    result['stress_10bp'] = round(-duration * 0.001 * 100, 3)   # 单位：%
    result['stress_30bp'] = round(-duration * 0.003 * 100, 3)

    # ── 6. 大白话解读 ──────────────────────────────────────────
    parts = []
    # 久期绝对水平
    d_desc = {
        duration < 1:      f'极短久期（{duration:.2f}年），利率风险极低',
        1 <= duration < 2: f'短久期（{duration:.2f}年），利率风险可控',
        2 <= duration < 4: f'中短久期（{duration:.2f}年），中等利率敏感度',
        4 <= duration < 6: f'中长久期（{duration:.2f}年），较高利率敏感度',
        duration >= 6:     f'长久期（{duration:.2f}年），高利率风险',
    }
    for cond, desc in d_desc.items():
        if cond is True:
            parts.append(desc)
            break

    # 类型匹配
    if is_in_std:
        parts.append(f'✅ 久期在{fund_subtype}基金标准区间（{d_min}-{d_max}年）内，风格合规')
    else:
        parts.append(f'⚠️ {drift_note}')

    # 择时
    if timing_note:
        parts.append(timing_note)

    # 压力测试
    parts.append(
        f'利率上行10BP → 净值约跌{abs(result["stress_10bp"]):.2f}%；'
        f'上行30BP → 净值约跌{abs(result["stress_30bp"]):.2f}%'
    )

    result['interpretation'] = '；'.join(parts)
    return result


def _regress_duration(fund_ret: pd.Series, nav_data: pd.DataFrame) -> tuple:
    """从净值收益率回归有效久期，返回(duration, r_squared)"""
    try:
        from data.fetcher import fetch_treasury_10y
        if 'date' in nav_data.columns:
            start = nav_data['date'].min().strftime('%Y-%m-%d')
            end = nav_data['date'].max().strftime('%Y-%m-%d')
        else:
            return 2.0, 0.0

        treasury_df = fetch_treasury_10y(start, end)
        if treasury_df.empty:
            return 2.0, 0.0

        fund_df = pd.DataFrame({'date': fund_ret.index, 'fund_ret': fund_ret.values})
        fund_df['date'] = pd.to_datetime(fund_df['date'])
        merged = fund_df.merge(treasury_df, on='date', how='inner').sort_values('date')
        if len(merged) < 60:
            return 2.0, 0.0

        merged['rate_change'] = merged['rate'].diff()
        X = sm.add_constant(-merged['rate_change'].fillna(0).values)
        y = merged['fund_ret'].values
        model = sm.OLS(y, X).fit()
        duration = float(np.clip(model.params[1], 0, 20))  # 防止离群值
        return duration, float(model.rsquared)
    except Exception:
        return 2.0, 0.0


def _score_duration_match(duration: float, d_min: float, d_max: float) -> tuple:
    """
    久期类型匹配得分（基础分），返回(score, is_in_std, note)

    满分100 = 在标准区间内
    偏离惩罚：每偏离0.1年扣5分（显著偏离）
    """
    if d_min <= duration <= d_max:
        return 100.0, True, ''

    # 计算偏离量
    if duration < d_min:
        deviation = d_min - duration
        note = f'久期({duration:.2f}年)低于标准下限({d_min}年)，可能偏保守'
    else:
        deviation = duration - d_max
        note = f'久期({duration:.2f}年)高于标准上限({d_max}年)，风格漂移'

    # 惩罚
    if deviation <= 0.2 * (d_max - d_min):  # 20%以内轻微偏离
        score = 95.0
    else:
        deduct = min((deviation / 0.1) * 5, 100)  # 每0.1年扣5分
        score = max(0, 100 - deduct)

    # 极端偏离（短债基金久期>3年）
    if d_max <= 1.0 and duration > 3.0:
        score = 0.0
        note = f'⚠️ 严重漂移：短债基金久期达{duration:.2f}年，风格严重背离！'

    return round(score, 1), False, note


def _score_to_grade(score: float) -> str:
    """评分 → 评级"""
    if score > 105:
        return 'A+'
    elif score >= 95:
        return 'A'
    elif score >= 85:
        return 'B'
    elif score >= 70:
        return 'C'
    else:
        return 'D'


# ============================================================
# 🏆 Step 6: 综合评分系统
# ============================================================

def calculate_pure_bond_scores(
    asset_structure: dict,
    credit_quality: dict,
    concentration: dict,
    duration_results: dict,
    nav_data: pd.DataFrame,
    three_factor_results: dict,
) -> dict:
    """
    纯债基金五维综合评分

    权重：信用40% + 集中度30% + 结构30%（底层资产质量）
    久期评分：独立评分维度（可>100）

    Args:
        asset_structure: analyze_asset_structure()的结果
        credit_quality: analyze_credit_quality()的结果
        concentration: analyze_bond_concentration()的结果
        duration_results: analyze_duration_system()的结果
        nav_data: 净值数据
        three_factor_results: 债券三因子回归结果

    Returns:
        {
          's_credit': float,       # 信用资质得分（0-100）
          's_conc': float,         # 集中度得分（0-100）
          's_struct': float,       # 结构得分（0-100）
          'score_quality': float,  # 底层资产质量得分（加权均）
          's_duration': float,     # 久期择时得分（0-120）
          'duration_grade': str,   # 久期评级
          'composite_risk_r': float,  # 综合风险指数R
          'radar_scores': dict,    # 雷达图五维
          'total_score': float,    # 综合总分（雷达图加权）
          'fund_label': str,       # 基金性格标签
          'grade': str,            # 总分评级
          'one_vote_veto': bool,   # 是否触发一票否决
          'veto_reason': str,      # 否决原因
        }
    """
    s_credit = credit_quality.get('credit_score', 80.0)
    s_conc = concentration.get('conc_score_final', 80.0)
    s_struct = asset_structure.get('quality_score', 80.0)

    # 底层资产质量得分
    score_quality = 0.4 * s_credit + 0.3 * s_conc + 0.3 * s_struct

    # 久期择时得分
    s_duration = duration_results.get('duration_score', 80.0)
    duration_grade = duration_results.get('duration_grade', 'B')

    # 综合风险指数 R（越大越高危）
    duration_val = duration_results.get('duration', 2.0)
    hhi = concentration.get('static_hhi', 500.0)
    wacs = credit_quality.get('wacs', 80.0)
    composite_risk_r = duration_val * hhi * (100 - wacs)

    # ── 雷达图五维评分 ────────────────────────────────────────
    # 债券型雷达维度：超额能力/风险控制/性价比/风格稳定/业绩持续
    alpha = three_factor_results.get('alpha', 0.0)
    r_squared = three_factor_results.get('r_squared', 0.5)

    # 计算风险控制（回撤+波动）
    risk_score = 80.0
    persistency_score = 70.0
    sharpe_score = 70.0
    if not nav_data.empty and 'ret' in nav_data.columns:
        try:
            ret_s = nav_data.set_index('date')['ret'] if 'date' in nav_data.columns else nav_data['ret']
            nav_s = nav_data.set_index('date')['nav'] if 'date' in nav_data.columns else nav_data['nav']
            max_dd, _ = calculate_max_drawdown(nav_s)
            vol = ret_s.std() * np.sqrt(252)
            risk_score = normalize_score(-max_dd, -0.10, 0) * 0.7 + normalize_score(-vol, -0.05, 0) * 0.3
            sharpe = calculate_sharpe(ret_s)
            sharpe_score = normalize_score(sharpe, 0, 3.0)
            win_rate = (ret_s > 0).mean()
            persistency_score = normalize_score(win_rate, 0.45, 0.65)
        except Exception:
            pass

    # 超额能力（三因子Alpha）
    alpha_score = normalize_score(alpha, -0.02, 0.04)

    # 风格稳定（三因子R² + 久期合规度）
    stability_score = normalize_score(r_squared, 0, 1.0) * 0.6 + \
                      normalize_score(duration_results.get('drift_score', 80) / 100, 0, 1) * 0.4

    radar_scores = {
        '超额能力': round(alpha_score, 1),
        '风险控制': round(risk_score, 1),
        '性价比': round(sharpe_score, 1),
        '风格稳定': round(stability_score, 1),
        '业绩持续': round(persistency_score, 1),
    }

    weights = config.RADAR_WEIGHTS.get('bond', {k: 0.2 for k in radar_scores})
    total_score = sum(radar_scores[k] * weights.get(k, 0.2) for k in radar_scores)

    # ── 一票否决条件 ─────────────────────────────────────────
    one_vote_veto = False
    veto_reason = ''

    # 1. 严重久期漂移（D级）
    if duration_grade == 'D':
        one_vote_veto = True
        veto_reason = f'久期严重漂移（{duration_val:.2f}年），不符合基金类型承诺'

    # 2. 规模预警（需外部传入，这里打标记）
    # 3. 综合风险R过高（参考阈值：Duration×HHI×(100-WACS) > 500000）
    if composite_risk_r > 500000 and not one_vote_veto:
        one_vote_veto = True
        veto_reason = f'综合风险指数R={composite_risk_r:.0f}，重仓低评级长久期债券，风险过高'

    # ── 基金性格标签 ─────────────────────────────────────────
    fund_label = _generate_fund_label(
        asset_structure, credit_quality, concentration, duration_results
    )

    # ── 总分评级 ─────────────────────────────────────────────
    if one_vote_veto:
        grade = 'D'
    elif total_score >= 85:
        grade = 'A'
    elif total_score >= 70:
        grade = 'B'
    elif total_score >= 55:
        grade = 'C'
    else:
        grade = 'D'

    return {
        's_credit': round(s_credit, 1),
        's_conc': round(s_conc, 1),
        's_struct': round(s_struct, 1),
        'score_quality': round(score_quality, 1),
        's_duration': round(s_duration, 1),
        'duration_grade': duration_grade,
        'composite_risk_r': round(composite_risk_r, 0),
        'radar_scores': radar_scores,
        'total_score': round(total_score, 1),
        'fund_label': fund_label,
        'grade': grade,
        'one_vote_veto': one_vote_veto,
        'veto_reason': veto_reason,
    }


def _generate_fund_label(
    asset_structure: dict, credit_quality: dict,
    concentration: dict, duration_results: dict
) -> str:
    """
    生成基金性格标签（用于报告定调）

    示例：
    - '稳健盾牌'（高信用+低久期+低集中度）
    - '利率先锋'（高利率债+高久期）
    - '票息收割机'（高信用债+中等久期）
    - '激进进攻型'（低信用+高久期）
    - '流动性仓库'（高存单）
    """
    ncd_r = asset_structure.get('ncd_ratio', 0)
    rate_r = asset_structure.get('rate_ratio', 0)
    credit_r = asset_structure.get('credit_ratio', 0)
    wacs = credit_quality.get('wacs', 80)
    duration = duration_results.get('duration', 2.0)
    hhi = concentration.get('static_hhi', 500)

    if ncd_r > 0.50:
        return '💰 流动性仓库（大额存单型）'
    elif rate_r > 0.60 and duration > 3.0:
        return '🚀 利率先锋（高久期利率债）'
    elif rate_r > 0.60 and duration <= 3.0:
        return '🛡️ 稳健盾牌（短端利率债）'
    elif credit_r > 0.60 and wacs >= 90 and hhi < 1000:
        return '🌾 票息收割机（分散优质信用）'
    elif credit_r > 0.60 and wacs < 80:
        return '⚡ 信用挖掘型（下沉策略）'
    elif duration > 5.0 and wacs < 80:
        return '🎲 激进进攻型（高久期+低评级）'
    elif hhi > 2000:
        return '🎯 集中押注型（高集中度）'
    else:
        return '📊 均衡配置型（综合策略）'


# ============================================================
# 🚨 Step 7: 压力测试升级（多因子情景模拟）
# ============================================================

def bond_stress_test_advanced(
    duration: float,
    convexity: float,
    credit_exposure: float,
    hhi: float,
    leverage_ratio: float,
    static_hhi: float = None,
) -> dict:
    """
    多因子情景模拟压力测试

    Args:
        duration: 有效久期
        convexity: 凸性
        credit_exposure: 信用债占比
        hhi: HHI指数
        leverage_ratio: 杠杆率
        static_hhi: 静态HHI（集中度）

    Returns:
        {
          'scenarios': list,       # 各情景结果
          'worst_case': dict,      # 最坏情景
          'risk_distribution': dict,  # 风险来源分解
        }
    """
    scenarios = [
        {
            'name': '债市小地震（利率+10BP）',
            'rate_up_bp': 10, 'credit_spread_bp': 0, 'liquidity_shock': False,
            'icon': '🌊',
        },
        {
            'name': '债熊初现（利率+50BP）',
            'rate_up_bp': 50, 'credit_spread_bp': 10, 'liquidity_shock': False,
            'icon': '🐻',
        },
        {
            'name': '信用黑天鹅（信用利差+30BP）',
            'rate_up_bp': 0, 'credit_spread_bp': 30, 'liquidity_shock': False,
            'icon': '🦢',
        },
        {
            'name': '股债双杀（利率+30BP+信用+20BP）',
            'rate_up_bp': 30, 'credit_spread_bp': 20, 'liquidity_shock': False,
            'icon': '💥',
        },
        {
            'name': '流动性踩踏（极端赎回压力）',
            'rate_up_bp': 15, 'credit_spread_bp': 50, 'liquidity_shock': True,
            'icon': '🌊🌊',
        },
    ]

    results = []
    for s in scenarios:
        rate_bp = s['rate_up_bp']
        credit_bp = s['credit_spread_bp']

        # 利率冲击：ΔP ≈ -D×Δy + 0.5×Conv×(Δy)²
        delta_y = rate_bp / 10000
        rate_impact = (-duration * delta_y + 0.5 * convexity * delta_y ** 2) * 100

        # 信用利差冲击：信用债权重 × 信用Beta × 利差变动
        credit_beta = duration * 0.6  # 信用债久期约为总久期的60%
        credit_impact = -credit_exposure * credit_beta * (credit_bp / 10000) * 100

        # 流动性冲击（HHI修正 + 杠杆乘数）
        liquidity_impact = 0.0
        if s.get('liquidity_shock'):
            hhi_factor = min(hhi / 1000, 2.0)  # HHI越高，流动性风险越大
            liquidity_impact = -0.2 * hhi_factor * leverage_ratio

        total = rate_impact + credit_impact + liquidity_impact

        results.append({
            'name': s['name'],
            'icon': s['icon'],
            'rate_impact_pct': round(rate_impact, 3),
            'credit_impact_pct': round(credit_impact, 3),
            'liquidity_impact_pct': round(liquidity_impact, 3),
            'total_impact_pct': round(total, 3),
            'rate_up_bp': rate_bp,
            'credit_spread_bp': credit_bp,
        })

    worst = min(results, key=lambda x: x['total_impact_pct'])

    return {
        'scenarios': results,
        'worst_case': worst,
        'risk_interpretation': _interpret_advanced_stress(worst, duration, credit_exposure),
    }


def _interpret_advanced_stress(worst: dict, duration: float, credit_exposure: float) -> str:
    """解读压力测试结果"""
    impact = worst['total_impact_pct']
    if impact > -0.5:
        level = '极低'
        emoji = '✅'
    elif impact > -1.5:
        level = '较低'
        emoji = '🟡'
    elif impact > -3.0:
        level = '中等'
        emoji = '🟠'
    else:
        level = '较高'
        emoji = '❌'

    main_risk = '利率风险' if duration > 3 else ('信用风险' if credit_exposure > 0.6 else '综合风险')
    return (f"{emoji} 极端情景下最大预估回撤约{abs(impact):.2f}%，风险等级{level}。"
            f"主要风险来源：{main_risk}。")


# ============================================================
# 🏃 主入口：运行纯债基金完整分析
# ============================================================

def run_pure_bond_analysis(
    symbol: str,
    nav_data: pd.DataFrame,
    basic_info: dict,
    holdings_data: dict,
    quarterly_alloc_history: pd.DataFrame = None,
    market_indicators: dict = None,
) -> dict:
    """
    纯债基金完整分析入口

    Returns:
        {
          'model_name': 'pure_bond_model',
          'identity': dict,         # 三重过滤结果
          'asset_structure': dict,  # 券种结构
          'credit_quality': dict,   # 信用资质
          'concentration': dict,    # 集中度（静态+动态HHI）
          'duration_system': dict,  # 久期体系
          'duration_results': dict, # 与旧bond_model兼容（含duration/r_squared）
          'three_factor_results': dict,
          'stress_test_advanced': dict,
          'scores': dict,           # 综合评分
          'radar_scores': dict,     # 雷达图数据
          'data_quality': dict,     # 数据管道摘要
        }
    """
    from data.processor import BondDataPipeline
    from models.bond_model import run_bond_three_factors, bond_stress_test
    from data.fetcher import fetch_bond_quarterly_holdings, fetch_fund_asset_allocation_history

    # ── 数据管道清洗 ────────────────────────────────────────────
    pipeline = BondDataPipeline(nav_data)
    clean_nav = (pipeline
                 .remove_outliers()
                 .check_data_continuity()
                 .validate_nav_consistency()
                 .get_processed_data())
    data_quality = pipeline.get_summary()

    nav_series = clean_nav.set_index('date')['nav'] if 'date' in clean_nav.columns else clean_nav['nav']
    ret_series = clean_nav.set_index('date')['ret'] if 'date' in clean_nav.columns else clean_nav['ret']

    # ── 三重识别过滤 ─────────────────────────────────────────────
    alloc_hist = quarterly_alloc_history if quarterly_alloc_history is not None else pd.DataFrame()
    if alloc_hist.empty:
        try:
            alloc_hist = fetch_fund_asset_allocation_history(symbol)
        except Exception:
            alloc_hist = pd.DataFrame()

    identity = identify_pure_bond_fund(clean_nav, holdings_data, alloc_hist)

    # ── 获取历史债券持仓 ────────────────────────────────────────
    historical_bond_holdings = {}
    try:
        historical_bond_holdings = fetch_bond_quarterly_holdings(symbol)
    except Exception:
        pass

    # ── 券种结构分析 ─────────────────────────────────────────────
    bond_holdings = holdings_data.get('bond_holdings', pd.DataFrame())
    asset_structure = analyze_asset_structure(bond_holdings)

    # ── 信用资质 ─────────────────────────────────────────────────
    credit_quality = analyze_credit_quality(bond_holdings)

    # ── 集中度（静态+动态） ───────────────────────────────────────
    concentration = analyze_bond_concentration(bond_holdings, historical_bond_holdings)

    # ── 久期体系 ─────────────────────────────────────────────────
    fund_name = basic_info.get('name', '')
    fund_subtype = _detect_fund_subtype(fund_name, basic_info)
    duration_system = analyze_duration_system(
        fund_ret=ret_series,
        nav_data=clean_nav,
        fund_subtype=fund_subtype,
        market_indicators=market_indicators,
    )

    # 兼容旧 bond_model 字段
    duration_results = {
        'duration': duration_system.get('duration', 2.0),
        'convexity': duration_system.get('duration', 2.0) ** 2 / 100,
        'r_squared': duration_system.get('duration_r_squared', 0.0),
        'interpretation': duration_system.get('interpretation', ''),
    }

    # ── 债券三因子 ────────────────────────────────────────────────
    three_factor_results = {}
    try:
        three_factor_results = run_bond_three_factors(ret_series, clean_nav)
    except Exception as e:
        three_factor_results = {'note': str(e)}

    # ── 压力测试升级版 ─────────────────────────────────────────────
    stress_advanced = bond_stress_test_advanced(
        duration=duration_system.get('duration', 2.0),
        convexity=duration_results.get('convexity', 0.09),
        credit_exposure=asset_structure.get('credit_exposure', 0.0),
        hhi=concentration.get('static_hhi', 500),
        leverage_ratio=asset_structure.get('leverage_ratio', 1.0),
    )

    # ── 综合评分 ──────────────────────────────────────────────────
    scores = calculate_pure_bond_scores(
        asset_structure=asset_structure,
        credit_quality=credit_quality,
        concentration=concentration,
        duration_results=duration_system,
        nav_data=clean_nav,
        three_factor_results=three_factor_results,
    )

    return {
        'model_name': 'pure_bond_model',
        'identity': identity,
        'asset_structure': asset_structure,
        'credit_quality': credit_quality,
        'concentration': concentration,
        'duration_system': duration_system,
        'duration_results': duration_results,    # 兼容旧字段
        'three_factor_results': three_factor_results,
        'stress_test_results': bond_stress_test(  # 旧版压力测试（兼容UI）
            duration=duration_results['duration'],
            convexity=duration_results['convexity'],
        ),
        'stress_test_advanced': stress_advanced,
        'bond_structure': asset_structure,        # 兼容旧字段 analyze_bond_structure
        'scores': scores,
        'radar_scores': scores.get('radar_scores', {}),
        'fund_label': scores.get('fund_label', ''),
        'data_quality': data_quality,
    }


def _detect_fund_subtype(fund_name: str, basic_info: dict) -> str:
    """从基金名称/信息判断纯债子类型"""
    name = fund_name + str(basic_info.get('fund_type', ''))
    if '短债' in name or '短期' in name:
        return '短债'
    elif '中短' in name:
        return '中短债'
    elif '长债' in name or '长期' in name:
        return '长债'
    elif '中长' in name:
        return '中长债'
    else:
        return '纯债'  # 通用


# ============================================================
# 🔧 辅助函数
# ============================================================

def _find_col(df: pd.DataFrame, candidates: list) -> Optional[str]:
    """在DataFrame中找到第一个存在的候选列名"""
    for col in candidates:
        if col in df.columns:
            return col
    return None
