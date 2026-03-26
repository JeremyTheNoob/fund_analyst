"""
数据预处理模块

功能:
  - 申万行业检测
  - 收益分解
  - 滚动Beta计算
  - 行业模型

作者: JeremyTheNoob
日期: 2026-03-26
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from typing import Tuple, Optional


# 申万行业代码映射
SW_INDUSTRY_MAP = {
    '农林牧渔': '801010', '基础化工': '801030', '钢铁': '801040',
    '有色金属': '801050', '电子': '801080', '汽车': '801880',
    '家用电器': '801110', '食品饮料': '801120', '纺织服饰': '801130',
    '轻工制造': '801140', '医药生物': '801150', '公用事业': '801160',
    '交通运输': '801170', '房地产': '801180', '商贸零售': '801200',
    '社会服务': '801210', '银行': '801780', '非银金融': '801790',
    '综合': '801230', '建筑材料': '801710', '建筑装饰': '801720',
    '电力设备': '801730', '机械设备': '801890', '国防军工': '801740',
    '计算机': '801750', '传媒': '801760', '通信': '801770',
    '煤炭': '801960', '石油石化': '801970', '环保': '801950',
    '美容护理': '801980',
}

# 基金名称关键词 → 申万一级行业代码（快速匹配）
FUND_NAME_TO_SW = {
    '医药': '801150', '医疗': '801150', '生物': '801150', '健康': '801150', '药': '801150',
    '消费': '801120', '食品': '801120', '饮料': '801120', '白酒': '801120',
    '科技': '801080', '半导体': '801080', '芯片': '801080', '电子': '801080',
    '新能源': '801730', '电力': '801730', '光伏': '801730', '储能': '801730',
    '军工': '801740', '国防': '801740', '航空': '801740',
    '银行': '801778', '金融': '801790', '证券': '801790',
    '地产': '801180', '房地产': '801180', '房': '801180',
    '农业': '801010', '养殖': '801010', '畜牧': '801010',
    '钢铁': '801040', '有色': '801050', '铜': '801050', '黄金': '801050',
    '化工': '801030', '化学': '801030',
    '汽车': '801880', '新车': '801880',
    '计算机': '801750', '软件': '801750', '互联网': '801750',
    '通信': '801770', '5G': '801770',
    '传媒': '801760', '游戏': '801760', '娱乐': '801760',
    '煤炭': '801960', '煤': '801960',
    '石油': '801970', '石化': '801970', '能源': '801970',
    '环保': '801950', '水务': '801950',
}


def detect_sw_industry(fund_name: str, sector_weights: dict) -> Tuple[str, str]:
    """
    从基金名称或持仓行业权重推断主要申万行业,返回 (sw_code, industry_name)
    优先级:
      1. 持仓行业权重(前两大行业占比>50%,取第一大)
      2. 基金名称关键词匹配
      3. 无法识别 → ('', '')
    """
    # 策略1:持仓行业权重(如果有季报数据)
    if sector_weights:
        # sector_weights 形如 {'电子': 0.45, '医药': 0.20, ...}
        top_sector = max(sector_weights, key=sector_weights.get)
        top_ratio = sector_weights[top_sector]
        if top_ratio > 0.30:  # 第一大行业超过30%,认定为主行业
            for kw, code in FUND_NAME_TO_SW.items():
                if kw in top_sector:
                    return code, top_sector
            # 直接查申万表
            for iname, code in SW_INDUSTRY_MAP.items():
                if iname in top_sector or top_sector in iname:
                    return code, iname

    # 策略2:基金名称关键词
    for kw, code in FUND_NAME_TO_SW.items():
        if kw in fund_name:
            iname = next((k for k, v in SW_INDUSTRY_MAP.items() if v == code), kw)
            return code, iname

    return '', ''


def run_rolling_beta(
    fund_ret: pd.Series,
    stock_index_ret: pd.Series,
    bond_index_ret: pd.Series,
    window: int = 20
) -> pd.DataFrame:
    """
    滚动窗口双因子回归,同时计算 20日 和 60日 两条曲线,用于双重验证。

    约束说明:
      - 使用普通 OLS 回归后,对 equity_beta clip(0, 1);
        量化实务的更严格做法是带约束优化(scipy.optimize.minimize),
        但 clip 已足以消除杠杆/对冲引起的越界,且计算量更低。
      - 若需完整约束(β股≥0, β债≥0, β股+β债≤1),可扩展到 constrained 版本。

    返回 DataFrame 列:
      date / equity_beta_20 / bond_beta_20 / r2_20 /
              equity_beta_60 / bond_beta_60 / r2_60

    可信度判断(r2):
      ≥0.80 → 信号可信(基金与股债基准高度相关)
      0.50~0.80 → 参考价值一般
      <0.50 → 近期信号有噪音,谨慎参考
    """
    df = pd.DataFrame({'fund': fund_ret}).reset_index()
    df.columns = ['date', 'fund']
    df['date'] = pd.to_datetime(df['date'])

    si = pd.DataFrame({
        'date': stock_index_ret.index.tolist() if hasattr(stock_index_ret, 'index') else range(len(stock_index_ret)),
        'stock': stock_index_ret.values
    })
    bi = pd.DataFrame({
        'date': bond_index_ret.index.tolist() if hasattr(bond_index_ret, 'index') else range(len(bond_index_ret)),
        'bond': bond_index_ret.values
    })

    si['date'] = pd.to_datetime(si['date'])
    bi['date'] = pd.to_datetime(bi['date'])

    df = df.merge(si, on='date', how='inner').merge(bi, on='date', how='inner').dropna()

    def _regress_window(chunk: pd.DataFrame) -> dict:
        """对单个窗口做 OLS,返回 equity_beta / bond_beta / r2"""
        try:
            X = sm.add_constant(chunk[['stock', 'bond']])
            m = sm.OLS(chunk['fund'], X).fit()
            beta_s = float(m.params.get('stock', np.nan))
            beta_b = float(m.params.get('bond', np.nan))
            # clip(0, 1):消除加杠杆 >1 或对冲 <0 导致的越界
            beta_s = float(np.clip(beta_s, 0.0, 1.0))
            beta_b = float(np.clip(beta_b, 0.0, 1.0))
            return {'equity_beta': beta_s, 'bond_beta': beta_b, 'r2': float(m.rsquared)}
        except Exception:
            return {'equity_beta': np.nan, 'bond_beta': np.nan, 'r2': np.nan}

    # ---- 20日窗口 ----
    rows_20 = []
    for i in range(window, len(df)):
        chunk = df.iloc[i - window: i]
        r = _regress_window(chunk)
        rows_20.append({'date': df['date'].iloc[i], **{f'{k}_20': v for k, v in r.items()}})

    # ---- 60日窗口 ----
    rows_60 = []
    for i in range(60, len(df)):
        chunk = df.iloc[i - 60: i]
        r = _regress_window(chunk)
        rows_60.append({'date': df['date'].iloc[i], **{f'{k}_60': v for k, v in r.items()}})

    df_20 = pd.DataFrame(rows_20) if rows_20 else pd.DataFrame(
        columns=['date', 'equity_beta_20', 'bond_beta_20', 'r2_20'])
    df_60 = pd.DataFrame(rows_60) if rows_60 else pd.DataFrame(
        columns=['date', 'equity_beta_60', 'bond_beta_60', 'r2_60'])

    result = df_20.merge(df_60, on='date', how='left')

    # 兼容旧字段(detect_style_drift 读 equity_beta)
    result['equity_beta'] = result['equity_beta_20']
    result['bond_beta'] = result['bond_beta_20']

    return result


def run_sector_model(
    fund_ret: pd.Series,
    bm_ret: pd.Series,
    sw_industry_ret: pd.Series = None,
    sw_industry_name: str = '',
    fund_name: str = ''
) -> dict:
    """
    行业/主题基金:精准行业基准 Alpha + 跟踪误差 + 信息比率

    核心升级:
      - 如果提供 sw_industry_ret(申万行业指数),优先用它做行业基准
      - 避免"用沪深300衡量医药基金"的伪Alpha陷阱:
        → 医药行业整体暴涨时,经理躺平也能跑赢300指数,Alpha虚高
        → 只有跑赢"医药指数"的医药基金经理,才是真本事
      - 同时保留 bm_ret(招募说明书基准)供参考对比

    返回:
      neutral_alpha      - 行业内年化Alpha(vs 申万行业指数,"窝里横"指数)
      neutral_alpha_bm   - vs 招募说明书基准的传统Alpha(对比用)
      tracking_error     - 年化跟踪误差(vs 行业指数)
      info_ratio         - 信息比率 = neutral_alpha / tracking_error
      bm_source          - 基准来源说明
      excess_series      - 每日超额收益序列(用于后续可视化)
      interpretation     - 大白话解读
    """
    # ---- 确定行业基准:申万行业指数优先 ----
    use_sw = sw_industry_ret is not None and not sw_industry_ret.empty and len(sw_industry_ret) > 20
    actual_bm_ret = sw_industry_ret if use_sw else bm_ret

    bm_source = (f'申万{sw_industry_name}指数(精准行业基准)' if use_sw
                 else '招募说明书基准(未匹配行业指数)')

    # ---- 主计算:vs 行业指数 ----
    df = pd.DataFrame({'fund': fund_ret, 'bm': actual_bm_ret}).dropna()
    if len(df) < 30:
        return {
            'neutral_alpha': 0.0, 'neutral_alpha_bm': 0.0,
            'tracking_error': 0.0, 'info_ratio': 0.0,
            'bm_source': bm_source, 'excess_series': pd.Series(dtype=float),
            'sw_code': '', 'sw_name': sw_industry_name,
            'interpretation': '数据不足(需至少30个交易日)'
        }

    excess = df['fund'] - df['bm']
    # 年化用 *252(与建议代码保持一致,简洁直接)
    neutral_alpha = excess.mean() * 252
    tracking_error = excess.std() * np.sqrt(252)
    info_ratio = neutral_alpha / tracking_error if tracking_error > 0 else 0.0

    # ---- 对比:vs 招募说明书基准(可选,仅在两者不同时计算)----
    neutral_alpha_bm = neutral_alpha  # 默认相同
    if use_sw and not bm_ret.empty:
        df_bm = pd.DataFrame({'fund': fund_ret, 'bm': bm_ret}).dropna()
        if len(df_bm) >= 30:
            neutral_alpha_bm = (df_bm['fund'] - df_bm['bm']).mean() * 252

    # ---- 解读层:大白话 ----
    parts = []
    industry_label = sw_industry_name or '同行业'

    # Alpha 解读
    if neutral_alpha > 0.08:
        parts.append(
            f"「窝里横」指数 🏆:行业内年化Alpha {neutral_alpha*100:.1f}%。"
            f"就算{industry_label}指数不涨不跌,经理靠选股一年也能多赚{neutral_alpha*100:.1f}%。"
            f"这是真本事。"
        )
    elif neutral_alpha > 0.03:
        parts.append(
            f"「窝里横」指数 ✅:行业内年化Alpha {neutral_alpha*100:.1f}%,"
            f"在{industry_label}内部具备选股超额能力。"
        )
    elif neutral_alpha > 0:
        parts.append(
            f"行业内年化Alpha {neutral_alpha*100:.1f}%,"
            f"勉强跑赢{industry_label}指数,优势不明显。"
        )
    else:
        parts.append(
            f"⚠️ 行业内年化Alpha {neutral_alpha*100:.1f}%(负值)。"
            f"连{industry_label}指数都跑不赢,买指数基金反而更划算。"
        )

    # 对比传统Alpha(仅在有申万基准时)
    if use_sw and abs(neutral_alpha - neutral_alpha_bm) > 0.02:
        if neutral_alpha_bm > neutral_alpha:
            parts.append(
                f"【基准陷阱提醒】vs沪深300 Alpha={neutral_alpha_bm*100:.1f}%,"
                f"但vs行业指数只有{neutral_alpha*100:.1f}%——"
                f"部分超额来自行业Beta,不是经理真本事。"
            )
        else:
            parts.append(
                f"vs沪深300 Alpha={neutral_alpha_bm*100:.1f}%,"
                f"vs行业指数Alpha={neutral_alpha*100:.1f}%,两者接近,Alpha较为纯粹。"
            )

    # 跟踪误差解读
    if tracking_error < 0.03:
        parts.append(
            f"「不走寻常路」程度 🟢:跟踪误差{tracking_error*100:.1f}%极低,"
            f"经理几乎按行业指数持仓,是增强型指数风格。"
        )
    elif tracking_error < 0.08:
        parts.append(
            f"「不走寻常路」程度 🟡:跟踪误差{tracking_error*100:.1f}%,"
            f"经理做了一定个股偏离,有主动管理色彩。"
        )
    elif tracking_error < 0.15:
        parts.append(
            f"「不走寻常路」程度 🟠:跟踪误差{tracking_error*100:.1f}%偏高,"
            f"经理在行业内部大量偏离——比如{industry_label}里重点押注某细分赛道。"
        )
    else:
        parts.append(
            f"「不走寻常路」程度 🔴:跟踪误差{tracking_error*100:.1f}%极高,"
            f"基金与行业指数差异巨大,个股集中度风险显著。"
        )

    # 信息比率解读
    if info_ratio > 1.5:
        parts.append(
            f"「选股性价比」💎:IR={info_ratio:.2f},每冒1%偏离风险换回{info_ratio:.2f}%超额,"
            f"选股不仅准而且稳,属于高效Alpha。"
        )
    elif info_ratio > 0.5:
        parts.append(
            f"「选股性价比」🟡:IR={info_ratio:.2f},每冒1%偏离风险换回{info_ratio:.2f}%超额,"
            f"选股效率尚可,但还未到顶级水准。"
        )
    elif info_ratio > 0:
        parts.append(
            f"「选股性价比」🟠:IR={info_ratio:.2f},超额收益靠较高的个股集中度「赌」出来,"
            f"风险收益比不够划算。"
        )
    else:
        parts.append(f"「选股性价比」🔴:IR={info_ratio:.2f},超额为负,主动管理未带来价值。")

    return {
        'neutral_alpha': neutral_alpha,
        'neutral_alpha_bm': neutral_alpha_bm,
        'tracking_error': tracking_error,
        'info_ratio': info_ratio,
        'bm_source': bm_source,
        'excess_series': excess.rename('excess'),
        'excess_return': neutral_alpha,   # 与 performance_decomposition 接口对齐
        'sw_code': '',              # 由上层填入
        'sw_name': sw_industry_name,
        'interpretation': '；'.join(parts)
    }


def performance_decomposition(
    model_results: dict,
    sector_results: dict = None,
    nav_df: pd.DataFrame = None,
    bm_df: pd.DataFrame = None,
) -> dict:
    """
    三层收益拆解(混合类/行业类)

    层级:
      仓位择时贡献  ← Brinson 配置效应
      行业选股贡献  ← 中性化 Alpha × 行业权重(仅行业型可精确;混合型用选择效应代替)
      其他残差      ← 总超额 - 仓位 - 行业

    返回:
      {
        'total_excess':     年化总超额(浮点),
        'allocation':       仓位择时贡献(浮点),
        'sector_alpha':     行业选股贡献(浮点),
        'residual':         残差(浮点),
        'narrative':        一句话描述(str),
        'credit_lines':     功劳簿列表(list of str),
        'data_quality':     数据质量说明(str),
      }
    """
    # sector 类型的结果存在 model_results['sector'] 子字典中
    # 需要先展开,再读 excess_return/allocation_effect 等字段
    _mr = model_results
    if sector_results and isinstance(sector_results, dict):
        # 行业型:sector_results 本身就是 run_sector_model 的返回
        # 它有 neutral_alpha/excess_return,但没有 allocation_effect
        # total_excess 直接用 neutral_alpha(行业型无Brinson分解)
        _mr = sector_results
    elif 'sector' in model_results:
        _mr = model_results['sector']

    allocation = _mr.get('allocation_effect', 0.0) or 0.0
    sel_inter = _mr.get('selection_inter_effect',
                         _mr.get('selection_effect', 0.0)) or 0.0
    # 行业型:优先取 neutral_alpha(即 excess_return),fallback 再取 excess_return
    total_excess = (_mr.get('neutral_alpha') or _mr.get('excess_return', 0.0)) or 0.0

    data_quality = '正常'

    # 行业选股贡献 ─────────────────────────────────────────────────
    # 方案A(精确):有行业Alpha × 行业权重
    # 方案B(近似):直接用 Brinson 选择效应(已剔除 allocation 的残差)
    sector_alpha = 0.0
    sector_label = '行业选股贡献'
    credit_lines = []

    if sector_results and isinstance(sector_results, dict):
        _na = sector_results.get('neutral_alpha', 0.0) or 0.0
        _sw_wt = sector_results.get('sector_weight', None)  # 行业权重(如有)
        _sw_name = sector_results.get('sw_name', '主要持仓行业')

        if _sw_wt and _sw_wt > 0:
            sector_alpha = _na * _sw_wt
            sector_label = f'{_sw_name}选股贡献'
            data_quality = '精确(行业Alpha × 行业权重)'
        else:
            # 无权重时用 neutral_alpha × 0.7(估算行业占股票仓位的约 70%)
            sector_alpha = _na * 0.70
            sector_label = f'{_sw_name}选股贡献(估算)'
            data_quality = '估算(中性化Alpha × 0.7权重)'
    else:
        # 无行业模型时,选择效应近似为行业选股
        sector_alpha = sel_inter
        sector_label = '个股选择贡献'
        data_quality = '近似(Brinson选择效应)'

    residual = total_excess - allocation - sector_alpha

    # 一句话叙事 ─────────────────────────────────────────────────
    _total_pct = total_excess * 100
    _alloc_pct = allocation * 100
    _sector_pct = sector_alpha * 100
    _resid_pct = residual * 100

    # 找主因
    _parts_signed = [
        ('仓位择时', _alloc_pct),
        (sector_label, _sector_pct),
        ('其他收益', _resid_pct),
    ]
    _positive = [(n, v) for n, v in _parts_signed if v > 0.005]
    _negative = [(n, v) for n, v in _parts_signed if v < -0.005]

    if _total_pct >= 0:
        _total_desc = f'今年超额收益 {_total_pct:+.1f}%'
    else:
        _total_desc = f'今年落后基准 {_total_pct:+.1f}%'

    _breakdown_parts = []
    for name, val in _parts_signed:
        if abs(val) >= 0.1:  # 只说明显的部分
            _breakdown_parts.append(f'{val:+.1f}% 来自{name}')

    if _breakdown_parts:
        narrative = _total_desc + ',其中 ' + ', '.join(_breakdown_parts) + ',剩余为杂音。'
    else:
        narrative = _total_desc + ',各分项贡献均较微弱。'

    # 功劳簿 ─────────────────────────────────────────────────────
    if abs(_alloc_pct) >= 0.1:
        if _alloc_pct > 0:
            credit_lines.append(
                f'📍 **头号功臣:仓位择时**(+{_alloc_pct:.1f}%)'
                f'  经理在行情转换节点调整了股债比例,站队正确,带来正贡献。'
            )
        else:
            credit_lines.append(
                f'📍 **拖累项:仓位择时**({_alloc_pct:.1f}%)'
                f'  大类资产配置方向判断失误,是本期超额落后的主因之一。'
            )

    if abs(_sector_pct) >= 0.1:
        if _sector_pct > 0:
            credit_lines.append(
                f'🎯 **核心技能:{sector_label}**(+{_sector_pct:.1f}%)'
                f'  即便行业整体平淡,经理通过精选个股获得了显著超额。'
            )
        else:
            credit_lines.append(
                f'⚠️ **拖累项:{sector_label}**({_sector_pct:.1f}%)'
                f'  行业内个股选择跑输了同行业基准,拖累整体表现。'
            )

    if abs(_resid_pct) >= 0.1:
        if _resid_pct > 0:
            credit_lines.append(
                f'💡 **意外之财:模型残差**(+{_resid_pct:.1f}%)'
                f'  含打新收益、交易超额及模型无法解释的随机收益。'
            )
        else:
            credit_lines.append(
                f'🔇 **其他损耗**({_resid_pct:.1f}%)'
                f'  含交易成本、微小滑点及模型残差。'
            )

    if not credit_lines:
        credit_lines.append('📊 各分项贡献均较微弱,超额主要来自整体市场的系统性波动。')

    return {
        'total_excess': total_excess,
        'allocation': allocation,
        'sector_alpha': sector_alpha,
        'sector_label': sector_label,
        'residual': residual,
        'narrative': narrative,
        'credit_lines': credit_lines,
        'data_quality': data_quality,
    }
