"""
基金穿透式诊断系统 v8.0 - 专业量化版
FOF研究员级别的深度量化分析框架

核心架构：
  数据层  → 基本信息 / 净值 / 因子 / 国债利率 / 持仓
  模型层  → 权益(FF三/五/Carhart) / 债券(T-Model久期) / 混合(Brinson) / 主题(中性化Alpha)
  网关层  → 根据股票仓位自动选择模型
  翻译层  → 专业术语 → 大白话 + 综合评分
  展示层  → Streamlit UI

作者：JeremyTheNoob
日期：2026-03-24
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import statsmodels.api as sm
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import re
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 页面配置
# ============================================================
st.set_page_config(
    page_title="DeepInFund · 基金深度诊断",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================
# CSS 样式
# ============================================================
def render_css():
    st.markdown("""
<style>
.stApp { background-color: #f4f6f9; }

.hero { background: linear-gradient(135deg,#1a1a2e,#0f3460);
        padding:28px 36px; border-radius:16px; color:white; margin-bottom:20px; }
.hero h1 { font-size:1.9rem; margin:0 0 6px 0; }
.hero p  { font-size:0.95rem; opacity:0.7; margin:0; }

.kpi { background:white; border-radius:12px; padding:18px; text-align:center;
       box-shadow:0 2px 10px rgba(0,0,0,.06); border-top:3px solid #0f3460; }
.kpi-val { font-size:1.55rem; font-weight:700; color:#0f3460; }
.kpi-lbl { font-size:0.78rem; color:#888; margin-top:4px; }
.kpi-red  .kpi-val { color:#e74c3c; }
.kpi-green .kpi-val { color:#27ae60; }
.kpi-orange .kpi-val { color:#e67e22; }

.card { background:white; border-radius:12px; padding:20px 24px;
        box-shadow:0 2px 10px rgba(0,0,0,.06); margin:8px 0; }
.card-warn   { border-left:4px solid #e67e22; background:#fffbf0; }
.card-danger { border-left:4px solid #e74c3c; background:#fff5f5; }
.card-good   { border-left:4px solid #27ae60; background:#f0fff4; }
.card-info   { border-left:4px solid #3498db; background:#f0f7ff; }

.tag { display:inline-block; padding:2px 10px; border-radius:16px;
       font-size:.73rem; font-weight:600; margin:2px; }
.tag-blue   { background:#e8f0fe; color:#1967d2; }
.tag-red    { background:#fce8e6; color:#c5221f; }
.tag-green  { background:#e6f4ea; color:#137333; }
.tag-orange { background:#fef3e2; color:#e37400; }
.tag-gray   { background:#f1f3f4; color:#5f6368; }

.section-title { font-size:1.05rem; font-weight:700; color:#1a1a2e;
                 border-bottom:2px solid #e8eaf0; padding-bottom:8px; margin:20px 0 12px 0; }
.drift-alert { background:#fff3cd; border:1px solid #ffc107;
               border-radius:8px; padding:12px 16px; font-size:0.88rem; color:#856404; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# ██████████████  DATA LAYER  ██████████████
# ============================================================

# ---------- 1. 基金基本信息 ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_basic_info(symbol: str) -> dict:
    """获取基金基本信息（雪球优先，天天补充）"""
    r = {
        'name': symbol, 'type_raw': '', 'type_category': 'equity',
        'establish_date': '', 'scale': '', 'company': '',
        'manager': '', 'manager_tenure': 0.0, 'manager_start_date': '',
        'benchmark_text': '', 'benchmark_parsed': {},
        'fee_manage': 0.0, 'fee_sale': 0.0, 'fee_redeem': 0.0,
        'fee_custody': 0.0, 'fee_total': 0.0
    }

    # 雪球接口
    try:
        df = ak.fund_individual_basic_info_xq(symbol=symbol)
        info = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        r['name']                = info.get('基金名称', symbol)
        r['type_raw']            = info.get('基金类型', '')
        r['establish_date']      = info.get('成立时间', '')
        r['scale']               = info.get('最新规模', '')
        r['company']             = info.get('基金公司', '')
        r['manager']             = info.get('基金经理', '')
        r['benchmark_text']      = info.get('业绩比较基准', '')
        r['fee_manage']          = _parse_fee(info.get('管理费率', ''))
        r['fee_custody']         = _parse_fee(info.get('托管费率', ''))
        r['fee_sale']            = _parse_fee(info.get('销售服务费率', ''))
        # 现任基金经理任职日期：雪球有时包含"任职日期"字段
        mgr_since = info.get('任职日期', '') or info.get('基金经理任职日期', '')
        r['manager_start_date']  = mgr_since
    except Exception:
        pass

    # 天天基金补充
    try:
        df2 = ak.fund_open_fund_info_em(symbol=symbol, indicator="基金概况")
        info2 = dict(zip(df2.iloc[:, 0], df2.iloc[:, 1]))
        if not r['type_raw']:
            r['type_raw'] = info2.get('基金类型', '')
        if not r['name'] or r['name'] == symbol:
            r['name'] = info2.get('基金名称', symbol)
        if not r['fee_manage']:
            r['fee_manage'] = _parse_fee(info2.get('管理费率', ''))
        if not r['fee_sale']:
            r['fee_sale'] = _parse_fee(info2.get('托管费率', ''))
    except Exception:
        pass

    r['benchmark_parsed'] = _parse_benchmark(r['benchmark_text'])
    r['type_category']    = _classify_fund(r)
    r['fee_total']        = r['fee_manage'] + r['fee_custody'] + r['fee_sale']

    return r


def _parse_fee(text: str) -> float:
    if not text:
        return 0.0
    m = re.search(r'(\d+\.?\d*)\s*%', str(text))
    return float(m.group(1)) / 100 if m else 0.0


def _classify_fund(info: dict) -> str:
    t = info.get('type_raw', '')
    if any(k in t for k in ['股票', '权益']):     return 'equity'
    if any(k in t for k in ['债券', '纯债']):      return 'bond'
    if any(k in t for k in ['混合', '配置', '平衡']): return 'mixed'
    if any(k in t for k in ['指数', 'ETF', '联接']): return 'index'
    if any(k in t for k in ['行业', '主题', 'QDII']): return 'sector'
    return 'equity'


# ---------- 2. 净值历史 ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_nav(symbol: str, years: int = 5,
              since_inception: bool = False,
              manager_start: str = '') -> pd.DataFrame:
    """
    获取历史净值，返回 date / nav / ret 列
    优先级：since_inception → manager_start → years
    """
    try:
        df = ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        if df is None or df.empty:
            return pd.DataFrame(columns=['date','nav','ret'])
        # 实际列名：['净值日期', '单位净值', '日增长率']
        df = df.iloc[:, :2]
        df.columns = ['date', 'nav']
        df['date'] = pd.to_datetime(df['date'])
        df['nav']  = pd.to_numeric(df['nav'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)

        # 根据时间模式裁剪
        if since_inception:
            pass  # 不裁剪，取全部
        elif manager_start:
            try:
                cutoff = pd.to_datetime(manager_start)
                df = df[df['date'] >= cutoff]
            except Exception:
                # 解析失败则退回 years 模式
                start = datetime.now() - timedelta(days=years*365)
                df = df[df['date'] >= start]
        else:
            start = datetime.now() - timedelta(days=years*365)
            df = df[df['date'] >= pd.to_datetime(start)]

        df['ret'] = df['nav'].pct_change()
        return df
    except Exception as e:
        return pd.DataFrame(columns=['date','nav','ret'])


# ---------- 3. Fama-French 因子数据（方案 A+C，含 RMW 代理） ----------

INDEX_MAP = {
    'mkt':      ('sh000300', '沪深300'),    # 市场因子
    'small':    ('sh000852', '中证1000'),   # 规模小盘
    'large':    ('sh000300', '沪深300'),    # 规模大盘
    'value':    ('sz399371', '国证价值'),   # 价值
    'growth':   ('sz399370', '国证成长'),   # 成长
    'quality':  ('sh000803', '300质量成长'), # RMW代理：质量因子（赚钱公司）
    # CMA（投资保守因子）在A股无合适代理，降维用MOM替代（A股动能解释力更强）
}

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_index_daily(symbol_code: str, start: str, end: str) -> pd.DataFrame:
    """通用指数日行情获取，返回 date / ret"""
    try:
        df = ak.stock_zh_index_daily_em(symbol=symbol_code)
        if df is None or df.empty:
            return pd.DataFrame(columns=['date','ret'])
        df = df[['date','close']].copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
        df['ret'] = df['close'].pct_change()
        return df[['date','ret']].dropna().reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=['date','ret'])


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ff_factors(start: str, end: str) -> pd.DataFrame:
    """
    构建 FF 因子代理序列（方案 A+C，扩展 RMW）
    因子列：date / Mkt / SMB / HML / MOM / RMW

    因子说明：
      SMB = 中证1000 - 沪深300（小盘溢价）
      HML = 国证价值 - 国证成长（价值溢价）
      MOM = 21日滚动均值收益（前一日，防前视偏差）；A股动能解释力 > CMA
      RMW = 中证质量/300质量成长 - 沪深300（盈利质量溢价）
            若质量指数拉取失败，RMW 列置 NaN，回归时自动跳过
    """
    mkt   = fetch_index_daily('sh000300', start, end).rename(columns={'ret': 'Mkt'})
    small = fetch_index_daily('sh000852', start, end).rename(columns={'ret': 'ret_small'})
    large = fetch_index_daily('sh000300', start, end).rename(columns={'ret': 'ret_large'})
    val   = fetch_index_daily('sz399371', start, end).rename(columns={'ret': 'ret_val'})
    grw   = fetch_index_daily('sz399370', start, end).rename(columns={'ret': 'ret_grw'})

    df = mkt.copy()
    df = df.merge(small, on='date', how='inner')
    df = df.merge(large, on='date', how='inner', suffixes=('', '_dup'))
    df = df.merge(val,   on='date', how='inner')
    df = df.merge(grw,   on='date', how='inner')

    df['SMB'] = df['ret_small'] - df['ret_large']   # 规模因子
    df['HML'] = df['ret_val']   - df['ret_grw']     # 价值因子
    # 动能因子：21日滚动均值（前一日），防前视偏差
    df['MOM'] = df['Mkt'].rolling(21).mean().shift(1)

    # RMW：质量因子代理（300质量成长 - 沪深300）
    # 备用：若 sh000803 失败，尝试 sz399311 国证质量
    rmw_ok = False
    for rmw_code in ('sh000803', 'sz399311', 'sh000919'):
        qual = fetch_index_daily(rmw_code, start, end).rename(columns={'ret': 'ret_qual'})
        if not qual.empty and len(qual) > 30:
            df = df.merge(qual, on='date', how='left')
            df['RMW'] = df['ret_qual'] - df['ret_large']
            rmw_ok = True
            break
    if not rmw_ok:
        df['RMW'] = np.nan   # 拉取失败时置 NaN，回归自动跳过此因子

    cols = ['date', 'Mkt', 'SMB', 'HML', 'MOM', 'RMW']
    return df[cols].reset_index(drop=True)


# ---------- 4. 国债收益率（用于久期回归） ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_treasury_10y(start: str, end: str) -> pd.DataFrame:
    """
    获取 10 年期国债收益率日变动（Δy），单位：%
    优先使用 bond_china_yield（分曲线逐列），备用 bond_zh_us_rate
    返回 date / yield_pct / delta_y
    """
    # 方案 A：bond_china_yield — 列=曲线名称/日期/3月/6月/.../10年/30年
    try:
        df = ak.bond_china_yield(start_date=start, end_date=end)
        # 取国债曲线（有时曲线名称字段和行数不一致，直接取所有行的10年列）
        mask = df['曲线名称'].str.contains('国债', na=False)
        df_gov = df[mask] if mask.sum() > 0 else df
        if df_gov.empty:
            raise ValueError("国债行为空，切换备用接口")
        df_gov = df_gov[['日期', '10年']].copy()
        df_gov.columns = ['date', 'yield_pct']
        df_gov['date']      = pd.to_datetime(df_gov['date'])
        df_gov['yield_pct'] = pd.to_numeric(df_gov['yield_pct'], errors='coerce')
        df_gov = df_gov.sort_values('date').dropna()
        df_gov['delta_y']   = df_gov['yield_pct'].diff()
        return df_gov[['date','yield_pct','delta_y']].dropna().reset_index(drop=True)
    except Exception:
        pass

    # 方案 B：bond_zh_us_rate — 包含中国国债10年收益率
    try:
        df = ak.bond_zh_us_rate(start_date=start)
        col10 = '中国国债收益率10年'
        if col10 not in df.columns:
            return pd.DataFrame(columns=['date','yield_pct','delta_y'])
        df = df[['日期', col10]].copy()
        df.columns = ['date', 'yield_pct']
        df['date']      = pd.to_datetime(df['date'])
        df['yield_pct'] = pd.to_numeric(df['yield_pct'], errors='coerce')
        df = df[df['date'] >= pd.to_datetime(start)].sort_values('date').dropna()
        df['delta_y'] = df['yield_pct'].diff()
        return df[['date','yield_pct','delta_y']].dropna().reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=['date','yield_pct','delta_y'])


# ---------- 5. 中债综合指数（债券基准） ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_bond_index(start: str, end: str) -> pd.DataFrame:
    """
    获取中债综合财富指数（indicator='财富'），返回 date / ret
    列结构：date / value
    """
    try:
        df = ak.bond_new_composite_index_cbond(indicator="财富")
        if df is None or df.empty:
            return pd.DataFrame(columns=['date','ret'])
        df.columns = ['date', 'close']
        df['date']  = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
        df['ret'] = df['close'].pct_change()
        return df[['date','ret']].dropna().reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=['date','ret'])


# ---------- 6. 基准构建（招募说明书解析 + 动态再平衡） ----------

_INDEX_NAME_CODE = {
    '沪深300': 'sh000300', '中证500': 'sh000905', '中证1000': 'sh000852',
    '上证50':  'sh000016', '创业板': 'sz399006', '科创50': 'sh000688',
    '中债综合': None,   # 单独用 fetch_bond_index 处理
    '银行活期': None,   # 活期存款，视为 0 收益
}

def _parse_benchmark(text: str) -> dict:
    """
    解析招募说明书基准文本
    例："沪深300指数收益率×80%+中债综合财富指数收益率×20%"
    返回 {type, components:[{name,code,weight}]}
    """
    if not text:
        return {}
    pattern = r'([^×＊*\+\-]+?)[指数]?[收益率]?\s*[×＊*]\s*(\d+)\s*[%％]'
    matches = re.findall(pattern, text)

    if not matches:
        return {}

    components = []
    for name_raw, w_str in matches:
        name = name_raw.strip()
        weight = int(w_str) / 100.0
        # 匹配已知指数代码
        code = None
        for k, v in _INDEX_NAME_CODE.items():
            if k in name:
                code = v
                break
        components.append({'name': name, 'code': code, 'weight': weight})

    bm_type = 'single' if len(components) == 1 else 'custom'
    return {'type': bm_type, 'components': components}


@st.cache_data(ttl=3600, show_spinner=False)
def build_benchmark_ret(parsed: dict, start: str, end: str) -> pd.DataFrame:
    """
    构建基准每日收益率（先算各成分日收益率，再加权）
    返回 date / bm_ret
    """
    if not parsed or 'components' not in parsed:
        # 默认沪深300
        df = fetch_index_daily('sh000300', start, end).rename(columns={'ret':'bm_ret'})
        return df

    parts = []
    for comp in parsed['components']:
        w = comp['weight']
        code = comp['code']

        if code is None and '中债' in comp.get('name',''):
            df_part = fetch_bond_index(start, end).rename(columns={'ret':'part_ret'})
        elif code is None:
            # 银行活期等，收益率视为 0
            df_part = pd.DataFrame({'date': pd.date_range(start, end, freq='B'),
                                    'part_ret': 0.0})
        else:
            df_part = fetch_index_daily(code, start, end).rename(columns={'ret':'part_ret'})

        df_part['weighted'] = df_part['part_ret'] * w
        parts.append(df_part[['date','weighted']])

    if not parts:
        df = fetch_index_daily('sh000300', start, end).rename(columns={'ret':'bm_ret'})
        return df

    merged = parts[0].rename(columns={'weighted':'bm_ret'})
    for p in parts[1:]:
        merged = merged.merge(p, on='date', how='outer')
        merged['bm_ret'] = merged['bm_ret'].fillna(0) + merged['weighted'].fillna(0)
        merged.drop(columns=['weighted'], inplace=True)

    return merged[['date','bm_ret']].dropna().reset_index(drop=True)


# ---------- 7. 持仓数据（季报） ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_holdings(symbol: str, type_category: str = 'equity') -> dict:
    """
    获取最新季报持仓
    返回：{stock_ratio, bond_ratio, cash_ratio, top10, sector_weights, report_date}
    """
    # 根据基金类型设置合理默认值（避免债券基金被误判为股票基金）
    _defaults = {
        'equity': {'stock_ratio': 0.90, 'bond_ratio': 0.05, 'cash_ratio': 0.05},
        'bond':   {'stock_ratio': 0.05, 'bond_ratio': 0.85, 'cash_ratio': 0.10},
        'mixed':  {'stock_ratio': 0.55, 'bond_ratio': 0.35, 'cash_ratio': 0.10},
        'sector': {'stock_ratio': 0.90, 'bond_ratio': 0.05, 'cash_ratio': 0.05},
    }
    _def = _defaults.get(type_category, _defaults['equity'])
    result = {
        'stock_ratio': _def['stock_ratio'],
        'bond_ratio':  _def['bond_ratio'],
        'cash_ratio':  _def['cash_ratio'],
        'top10': pd.DataFrame(), 'sector_weights': {},
        'report_date': '',
        'alloc_source': 'default'   # 标记数据来源，便于调试
    }

    # 先尝试从资产配置接口获取（最准确）
    alloc_ok = False
    try:
        df2 = ak.fund_open_fund_info_em(symbol=symbol, indicator="资产配置")
        if df2 is not None and not df2.empty:
            alloc = dict(zip(df2.iloc[:, 0].astype(str), df2.iloc[:, 1].astype(str)))
            sr = _parse_pct(alloc.get('股票', '') or alloc.get('股票仓位', ''))
            br = _parse_pct(alloc.get('债券', '') or alloc.get('债券仓位', ''))
            cr = _parse_pct(alloc.get('现金', '') or alloc.get('现金及其他', ''))
            # 有效性校验：三者之和应在 [0.5, 1.5] 区间（允许一定误差）
            # 且不能全为0（接口返回空字段的情况）
            total = sr + br + cr
            if total > 0.3:  # 有实质数据
                # 如果三者之和明显不等于1，做归一化
                if total > 0.01:
                    sr = sr / total
                    br = br / total
                    cr = cr / total
                result['stock_ratio'] = sr
                result['bond_ratio']  = br
                result['cash_ratio']  = cr
                result['alloc_source'] = '资产配置接口'
                alloc_ok = True
    except Exception:
        pass

    # 次级方案：用前十大股票持仓估算股票仓位（仅在资产配置接口失败时使用）
    try:
        df = ak.fund_portfolio_hold_em(symbol=symbol, date="2024")
        if df is not None and not df.empty:
            result['top10'] = df.head(10)
            if not alloc_ok and '占净值比例' in df.columns:
                top10_ratio = pd.to_numeric(df['占净值比例'], errors='coerce').head(10).sum()
                # 前十大通常占股票仓位的 60-70%，修正系数 1.4
                est_stock = min(top10_ratio / 100 * 1.4, 0.95)
                if est_stock > 0.01:  # 有有效数据才覆盖
                    result['stock_ratio'] = est_stock
                    result['alloc_source'] = '前十大持仓推算'
    except Exception:
        pass

    return result


def _parse_pct(text: str) -> float:
    if not text:
        return 0.0
    m = re.search(r'(\d+\.?\d*)', str(text))
    return float(m.group(1)) / 100 if m else 0.0


# ---------- 9. 隐性费率估算 ----------

@st.cache_data(ttl=3600, show_spinner=False)
def estimate_hidden_cost(symbol: str, nav_df: pd.DataFrame) -> dict:
    """
    估算隐性费率（交易成本+其他费用）
    方案A：年报倒推法（从利润表的"交易费用"科目）
    方案B（备用）：换手率估算法（换手率 × 双边佣金率 0.06%）

    返回：{
        hidden_cost_rate: float,       # 年化隐性费率
        turnover_rate: float,          # 估算换手率（双边）
        method: str,                   # 使用的估算方法
        is_high_turnover: bool,        # 是否高换手（>400%）
        alert: str                     # 预警信息
    }
    """
    result = {
        'hidden_cost_rate': None,
        'turnover_rate': None,
        'method': '暂无数据',
        'is_high_turnover': False,
        'alert': ''
    }

    # -- 方案A：尝试从基金年报利润表获取交易费用 --
    try:
        df_profit = ak.fund_open_fund_info_em(symbol=symbol, indicator="利润分配")
        # 尝试获取基金利润表（交易费用科目）
        if df_profit is not None and not df_profit.empty:
            info = dict(zip(df_profit.iloc[:, 0].astype(str),
                            df_profit.iloc[:, 1].astype(str)))
            # 寻找"交易费用"或"买卖证券差价收入"字段
            for key in ['交易费用', '买卖差价', '交易成本']:
                v = info.get(key)
                if v:
                    cost = abs(_parse_pct(v))
                    if cost > 0:
                        # 除以基金规模得出费率（简化）
                        result['hidden_cost_rate'] = cost
                        result['method'] = '年报倒推法'
                        break
    except Exception:
        pass

    # -- 方案B：换手率估算法（主力方案） --
    # 基于净值日收益率的绝对波动估算交易频率（当无法获得换手率原始数据时）
    if result['hidden_cost_rate'] is None and not nav_df.empty and len(nav_df) >= 60:
        try:
            # 取最近252个交易日
            recent = nav_df.dropna(subset=['ret']).tail(252)
            daily_ret = recent['ret']

            # 利用日收益率方差和自相关系数估算换手率
            # 高换手率通常伴随更高的收益率序列负自相关（回均值）
            autocorr = daily_ret.autocorr(lag=1) if len(daily_ret) >= 10 else 0
            volatility = daily_ret.std() * np.sqrt(252)

            # 粗略的换手率估算：高频调仓基金的波动率通常更高
            # 结合负自相关的强度进行调整
            base_turnover = 2.0  # 基准双边换手率 200%
            if autocorr < -0.05:
                # 负自相关越强，说明调仓越频繁
                turnover = base_turnover * (1 + abs(autocorr) * 5)
            else:
                turnover = base_turnover

            # 单边佣金率约 0.03%（近年来已大幅下降），双边 0.06%
            COMMISSION = 0.0003
            hidden_rate = turnover * 2 * COMMISSION  # 双边换手 × 单边佣金

            result['hidden_cost_rate'] = hidden_rate
            result['turnover_rate']    = turnover
            result['method']           = f'换手率估算法（推算双边换手约{turnover*100:.0f}%）'

            # 高换手预警
            if turnover > 4.0:   # 双边>400%
                result['is_high_turnover'] = True
                result['alert'] = (
                    f"⚡ 高换手预警：估算年双边换手率约 {turnover*100:.0f}%，"
                    f"隐性交易成本约 {hidden_rate*100:.2f}%/年。"
                    "这可能意味着基金经理正在频繁调仓，需关注是否发生风格切换。"
                )
        except Exception:
            pass

    return result


# ---------- 8. 逻辑网关：根据股票仓位自动选模型 ----------

def detect_fund_model(stock_ratio: float, type_category: str,
                      convertible_ratio: float = 0.0) -> tuple:
    """
    漏斗式模型选择，返回 (model_type, warning_msg)

    漏斗层级（优先级从高到低）：
      L1. 行业/指数型           → sector（不受仓位影响）
      L2. 可转债迷雾检测         → 如果可转债>20%，标注波动风险
      L3. 注册类型强信号
            bond  + 股票<30%   → bond
            equity + 股票>60%  → equity
      L4. 仓位阈值（严格）
            >80%               → equity
            <20%               → bond
      L5. 兜底                  → mixed（包括50%这种"三不管"地带）

    注意：mixed 是最终兜底，不会出现"卡死"情况。
    """
    warn = ''

    # L1: 行业/指数直接走sector
    if type_category in ('sector', 'index'):
        return 'sector', warn

    # L2: 可转债迷雾检测（可转债>20%但股票仓位低，实际波动像股票）
    if convertible_ratio > 0.20 and stock_ratio < 0.20:
        warn = (f'⚠️ 可转债提示：该基金持有约{convertible_ratio*100:.0f}%可转债，'
                f'虽股票仓位仅{stock_ratio*100:.0f}%，但可转债含权属性使实际波动接近权益，'
                f'债券模型解释力可能偏低。')

    # L3: 注册类型作为强信号（允许一定容差）
    if type_category == 'bond' and stock_ratio < 0.30:
        return 'bond', warn
    if type_category == 'equity' and stock_ratio > 0.60:
        return 'equity', warn

    # L4: 仓位阈值（高/低极端情况）
    if stock_ratio > 0.80:
        return 'equity', warn
    if stock_ratio < 0.20:
        return 'bond', warn

    # L5: 兜底 → mixed（含50%这类"三不管"地带）
    return 'mixed', warn


# ============================================================
# ██████████████  MODEL LAYER  ██████████████
# ============================================================

# ---------- M1. 权益模型：FF三因子 / 五因子(含RMW) / Carhart ----------

def _run_single_ff(df: pd.DataFrame, use_cols: list) -> dict:
    """
    内部函数：对已对齐好的 df 跑一次OLS回归。
    同时计算：
      1. 标准化Beta（Z-Score后的系数）→ 用于因子暴露度横向对比
      2. 原始Beta（不标准化）→ 用于"大盘涨1%，基金涨X%"的弹性解释
    Alpha 年化使用复利公式：(1+alpha_daily)^252 - 1
    """
    if len(df) < 60:
        return None

    y = df['fund_ret'].values
    X_raw = df[use_cols].values

    # --- 回归1：Z-Score标准化（因子暴露横向对比）---
    scaler = StandardScaler()
    X_scaled = sm.add_constant(scaler.fit_transform(X_raw))
    try:
        m_scaled = sm.OLS(y, X_scaled).fit()
    except Exception:
        return None

    alpha_daily = float(m_scaled.params[0])
    # 复利公式年化 Alpha（比 ×252 更严谨）
    alpha_annual = (1 + alpha_daily) ** 252 - 1
    alpha_pval   = float(m_scaled.pvalues[0])
    r2           = float(m_scaled.rsquared)
    # 标准化 Beta（反映相对暴露强度）
    factor_betas_std = {col: float(m_scaled.params[i+1])
                        for i, col in enumerate(use_cols)}

    # --- 回归2：原始（未标准化）→ 用于"涨1%弹性"解释 ---
    X_raw_const = sm.add_constant(X_raw)
    try:
        m_raw = sm.OLS(y, X_raw_const).fit()
        factor_betas_raw = {col: float(m_raw.params[i+1])
                            for i, col in enumerate(use_cols)}
    except Exception:
        factor_betas_raw = {}

    return {
        'alpha': alpha_annual,
        'alpha_pval': alpha_pval,
        'r_squared': r2,
        'factor_betas': factor_betas_std,      # 暴露度用（标准化）
        'factor_betas_raw': factor_betas_raw,  # 弹性用（未标准化）
        'n_obs': len(df),
    }


def run_ff_model(fund_ret: pd.Series,
                 factors: pd.DataFrame,
                 model_type: str = 'ff3') -> dict:
    """
    权益类多因子回归，支持双窗口（全期 + 近126天）对比 Beta 漂移。

    model_type:
      'capm'    → [Mkt]
      'ff3'     → [Mkt, SMB, HML]
      'ff5'     → [Mkt, SMB, HML, RMW]  （RMW=质量因子；CMA降维→不再用）
      'carhart' → [Mkt, SMB, HML, MOM]

    返回字段：
      alpha / alpha_pval / r_squared / factor_betas（标准化）
      factor_betas_raw（原始，用于弹性解释）
      beta_drift（近126天 vs 全期 的 Mkt Beta 变动）
      interpretation
    """
    # ---------- 数据对齐 ----------
    df_base = pd.DataFrame({'fund_ret': fund_ret}).reset_index()
    df_base.columns = ['date', 'fund_ret']
    df_base['date'] = pd.to_datetime(df_base['date'])
    df_base = df_base.merge(factors, on='date', how='inner')

    # 选择因子列（自动跳过 RMW 列全为 NaN 的情况）
    _factor_map = {
        'capm':    ['Mkt'],
        'ff3':     ['Mkt', 'SMB', 'HML'],
        'ff5':     ['Mkt', 'SMB', 'HML', 'RMW'],   # 真实 RMW，不再重复 SMB/HML
        'carhart': ['Mkt', 'SMB', 'HML', 'MOM'],
    }
    desired_cols = _factor_map.get(model_type, ['Mkt', 'SMB', 'HML'])
    # 过滤：只保留在 df 中存在且非全 NaN 的列
    use_cols = [c for c in desired_cols
                if c in df_base.columns and df_base[c].notna().sum() > 30]
    if not use_cols:
        return _empty_ff_result('因子列缺失，无法回归')

    df_full = df_base[['date', 'fund_ret'] + use_cols].dropna()
    if len(df_full) < 60:
        return _empty_ff_result('数据不足(<60天)，无法回归')

    # ---------- 全期回归 ----------
    res_full = _run_single_ff(df_full, use_cols)
    if res_full is None:
        return _empty_ff_result('全期回归失败')

    # ---------- 近126天（半年）回归，检测 Beta 漂移 ----------
    df_recent = df_full.tail(126)
    res_recent = _run_single_ff(df_recent, use_cols) if len(df_recent) >= 40 else None

    # Beta 漂移检测（全期 Mkt Beta vs 近期 Mkt Beta）
    beta_drift = None
    drift_warn = ''
    if res_recent and 'Mkt' in res_full['factor_betas_raw'] and \
                      'Mkt' in res_recent['factor_betas_raw']:
        full_mkt   = res_full['factor_betas_raw']['Mkt']
        recent_mkt = res_recent['factor_betas_raw']['Mkt']
        beta_drift = recent_mkt - full_mkt
        if abs(beta_drift) > 0.2:
            direction = '大幅加仓（Beta 升高）' if beta_drift > 0 else '大幅减仓（Beta 降低）'
            drift_warn = (f'⚡ Beta漂移提示：近半年市场Beta {recent_mkt:.2f} '
                          f'vs 全期 {full_mkt:.2f}，偏差{beta_drift:+.2f}，'
                          f'经理近期可能{direction}。')

    # ---------- 生成解读文本 ----------
    alpha_annual = res_full['alpha']
    alpha_pval   = res_full['alpha_pval']
    r2           = res_full['r_squared']

    if r2 > 0.9:
        interpretation = (f"R²={r2:.2f}，基金高度复制基准，主动管理有限。"
                          f"经理几乎没有添加额外价值，可考虑用低费率指数基金替代。")
    elif r2 < 0.7 and alpha_annual > 0.03:
        interpretation = (f"R²={r2:.2f}，市场因子仅能解释{r2*100:.0f}%的收益，"
                          f"但年化Alpha达{alpha_annual*100:.1f}%，"
                          f"{'统计显著（真本事）' if alpha_pval<0.05 else '统计不显著（需继续观察）'}。"
                          f"经理有独特的获利逻辑。")
    elif alpha_annual > 0 and alpha_pval < 0.05:
        interpretation = (f"年化Alpha {alpha_annual*100:.1f}%，统计显著(p={alpha_pval:.3f})，"
                          f"**这是真本事**，不是市场Beta的附带品。")
    elif alpha_annual > 0:
        interpretation = (f"年化Alpha {alpha_annual*100:.1f}%，但p={alpha_pval:.3f}不显著，"
                          f"超额收益存疑，可能有运气成分。")
    else:
        interpretation = (f"年化Alpha {alpha_annual*100:.1f}%为负，"
                          f"跑输经风险调整后的基准，需警惕。")

    if drift_warn:
        interpretation += f'；{drift_warn}'

    return {
        'alpha':            alpha_annual,
        'alpha_pval':       alpha_pval,
        'r_squared':        r2,
        'factor_betas':     res_full['factor_betas'],         # 标准化，用于暴露图
        'factor_betas_raw': res_full['factor_betas_raw'],     # 原始，用于弹性解释
        'beta_drift':       beta_drift,                       # 近半年 vs 全期 Mkt Beta 差值
        'recent_betas_raw': res_recent['factor_betas_raw'] if res_recent else {},
        'n_obs':            res_full['n_obs'],
        'model_type':       model_type,
        'interpretation':   interpretation
    }


def _empty_ff_result(reason: str) -> dict:
    return {
        'alpha': None, 'alpha_pval': 1.0, 'r_squared': 0,
        'factor_betas': {}, 'factor_betas_raw': {},
        'beta_drift': None, 'recent_betas_raw': {},
        'n_obs': 0, 'model_type': 'unknown', 'interpretation': reason
    }


def select_equity_model(fund_name: str) -> str:
    """
    根据基金名称推断最合适的因子模型：
      科技/创业/动能主题 → carhart（含 MOM 动能因子，A股解释力最强）
      小盘/微盘/中小盘   → ff5（含 RMW 质量因子，筛出真正赚钱的小盘）
      中证500/中盘        → ff3
      大盘/价值/默认      → capm
    """
    name = fund_name.lower()
    if any(k in name for k in ['创业', '科技', '科创', '成长', '新能源', '人工智能', 'ai']):
        return 'carhart'
    if any(k in name for k in ['小盘', '微盘', '中小', '中证1000']):
        return 'ff5'
    if any(k in name for k in ['中证500', '中盘']):
        return 'ff3'
    return 'capm'


# ---------- M2. 债券模型：T-Model 久期归因 ----------

def run_duration_model(fund_ret: pd.Series,
                       treasury_df: pd.DataFrame,
                       bond_ratio: float = 1.0) -> dict:
    """
    从净值反推有效久期（T-Model）

    回归方程：
        fund_ret = α + β₁·(-ΔY) + β₂·(0.5·ΔY²)
        其中：
          β₁ = 组合有效久期（Duration_portfolio）
          β₂ = 组合有效凸性（Convexity_portfolio）
          α  = 综合 carry（票息收入 + 信用溢价 + 骑乘收益），非纯信用风险

    ΔY 单位说明：
        保持原始百分比（如 0.05 代表当日利率变动 0.05%）。
        这样 β₁（久期）的系数单位为"基金日收益 / 利率变动%"，
        即 Duration_portfolio ≈ β₁（可直接读为"年"）。
        ⚠️ 不转为小数，避免 ΔY² 极小（0.0001²）导致凸性系数失真。

    bond_ratio 仓位修正：
        若基金非纯债（如混合型债券仓位 20%），回归出的久期是组合级。
        底层债券头寸真实久期 = Duration_portfolio / bond_ratio
        （仅当 bond_ratio < 0.8 时才做此修正并展示）
    """
    df = pd.DataFrame({'fund_ret': fund_ret}).reset_index()
    df.columns = ['date', 'fund_ret']
    df['date'] = pd.to_datetime(df['date'])

    # ΔY 保持百分比单位（不 ÷100），原始 treasury_df 中 delta_y 已是百分比变动
    t = treasury_df[['date', 'delta_y']].copy()

    df = df.merge(t, on='date', how='inner').dropna()

    if len(df) < 60:
        return {
            'duration': 5.0, 'duration_underlying': 5.0,
            'convexity': 0.0, 'carry_alpha': 0.0,
            'r_squared': 0.0, 'bond_ratio_used': bond_ratio,
            'interpretation': '数据不足，无法回归，默认久期5年'
        }

    # X 矩阵说明：
    #   neg_dy       = -ΔY         → 系数即 Duration_portfolio（正数直觉）
    #   dy_sq_half   = 0.5 × ΔY²  → 系数即 Convexity_portfolio
    df['neg_dy']     = -df['delta_y']
    df['dy_sq_half'] = 0.5 * df['delta_y'] ** 2

    X = sm.add_constant(df[['neg_dy', 'dy_sq_half']])
    y = df['fund_ret']

    try:
        model = sm.OLS(y, X).fit()
    except Exception as e:
        return {'duration': 5.0, 'duration_underlying': 5.0,
                'convexity': 0.0, 'carry_alpha': 0.0,
                'r_squared': 0.0, 'bond_ratio_used': bond_ratio,
                'interpretation': f'回归失败: {e}'}

    duration_portfolio = float(model.params.get('neg_dy', 5.0))
    convexity          = float(model.params.get('dy_sq_half', 0.0))
    alpha_daily        = float(model.params.get('const', 0.0))

    # Alpha 年化用复利公式（与 M1 保持一致）
    carry_alpha = (1 + alpha_daily) ** 252 - 1

    r2 = float(model.rsquared)

    # 仓位修正：底层债券头寸真实久期
    bond_ratio_clamp = max(bond_ratio, 0.05)   # 防止除零
    if bond_ratio < 0.80:
        duration_underlying = duration_portfolio / bond_ratio_clamp
        position_note = (f'（组合久期 {duration_portfolio:.1f}年 ÷ '
                         f'债券仓位 {bond_ratio*100:.0f}% = '
                         f'底层债券真实久期 {duration_underlying:.1f}年）')
    else:
        duration_underlying = duration_portfolio
        position_note = ''

    # -------- 解读文本 --------
    parts = []

    # 久期诊断（展示底层真实久期）
    dur_display = duration_underlying
    if dur_display > 7:
        parts.append(
            f"有效久期 {dur_display:.1f}年（**偏长**）{position_note}，"
            f"利率每上行 1%，底层债券约损失 {dur_display:.1f}%，加息环境下风险较高"
        )
    elif dur_display < 2:
        parts.append(
            f"有效久期 {dur_display:.1f}年（**较短**）{position_note}，"
            f"对利率变动不敏感，偏货币/短债策略，适合稳健配置"
        )
    else:
        parts.append(
            f"有效久期 {dur_display:.1f}年（中等）{position_note}，"
            f"利率中性，兼顾收益与波动"
        )

    # 凸性诊断
    if convexity > 30:
        parts.append(f"凸性{convexity:.0f}（**较高**），价格'涨得比跌得快'，利率大幅波动时有缓冲保护")
    elif convexity > 0:
        parts.append(f"凸性{convexity:.0f}，正常范围，有一定减震效果")
    else:
        parts.append(f"凸性{convexity:.1f}（偏低/负），对利率冲击缺乏缓冲，注意利率风险")

    # 综合 carry 诊断（α 包含票息+信用溢价+骑乘收益）
    if carry_alpha > 0.04:
        parts.append(
            f"年化综合carry {carry_alpha*100:.1f}%（**偏高**），"
            f"可能含高票息信用债或骑乘收益，需关注信用风险和流动性"
        )
    elif carry_alpha > 0.015:
        parts.append(
            f"年化综合carry {carry_alpha*100:.1f}%，"
            f"包含票息+信用溢价+骑乘收益，整体合理"
        )
    elif carry_alpha > 0:
        parts.append(f"年化综合carry {carry_alpha*100:.2f}%，收益主要来自利率敞口，信用暴露较低")
    else:
        parts.append("综合carry为负，纯利率博弈型策略，信用成分极低")

    return {
        'duration':            duration_portfolio,    # 组合级久期（含仓位）
        'duration_underlying': duration_underlying,   # 底层债券真实久期（仓位修正后）
        'convexity':           convexity,
        'carry_alpha':         carry_alpha,           # 年化综合carry（票息+信用+骑乘）
        'credit_spread_alpha': carry_alpha,           # 旧字段名保留兼容
        'r_squared':           r2,
        'bond_ratio_used':     bond_ratio,
        'interpretation':      '；'.join(parts)
    }


# ---------- M3. 混合模型：Brinson 归因 + 动态漂移监控 ----------

def run_brinson(fund_ret: pd.Series,
                bm_ret: pd.Series,
                stock_ratio_fund: float,
                bond_ratio_fund: float,
                stock_index_ret: pd.Series,
                bond_index_ret: pd.Series,
                bm_stock_weight: float = 0.6) -> dict:
    """
    Brinson 归因（资产级，股 + 债两类）

    效应定义（BHB模型）：
      配置效应  = Σ (w_fund_i - w_bm_i) × r_bm_i
                  → 衡量"站队"能力：大类资产配置比例是否踩对了方向
      选择效应  = Σ w_bm_i × (r_fund_i - r_bm_i)
                  → 衡量"挑货"能力：同类资产内部选标的是否优于基准
      交互效应  = Σ (w_fund_i - w_bm_i) × (r_fund_i - r_bm_i)
                  → 两种能力叠加的"化学反应"奖金/惩罚

    ⭐ 展示策略：
      对外只展示"配置效应"和"选择效应+交互效应"两个维度，
      交互效应在内部保留但并入选择效应展示，符合机构实务惯例，
      避免向非专业用户解释复杂的交互概念。

    基准分量收益率（真实数据，不再用倍数近似）：
      r_bm_stock = stock_index_ret（沪深300日收益率，直接传入）
      r_bm_bond  = bond_index_ret（中债综合日收益率，直接传入）

    基金分量收益率的近似（无法直接获取基金内部股/债分仓净值）：
      r_fund_stock ≈ stock_index_ret（用沪深300代理，含经理选股能力的残差）
      r_fund_bond  ≈ bond_index_ret（用中债综合代理）
      注：精确版需要基金分仓净值数据（季报无此信息），当前近似已优于倍数法
    """
    # ---------- 数据对齐（日度层面） ----------
    # 将所有序列对齐到共同日期，用日度数据计算（非年化，最后统一年化）
    def _to_df(s, col):
        if isinstance(s, pd.Series) and not s.empty:
            d = s.reset_index()
            d.columns = ['date', col]
            d['date'] = pd.to_datetime(d['date'])
            return d
        return pd.DataFrame(columns=['date', col])

    df_fund  = _to_df(fund_ret,        'fund')
    df_stock = _to_df(stock_index_ret, 'r_stock')
    df_bond  = _to_df(bond_index_ret,  'r_bond')
    df_bm    = _to_df(bm_ret,          'r_bm')

    # 尝试日度对齐
    use_daily = True
    df = df_fund.copy()
    for d in [df_stock, df_bond, df_bm]:
        if d.empty:
            use_daily = False
            break
        df = df.merge(d, on='date', how='inner')

    df = df.dropna()

    if use_daily and len(df) >= 20:
        # ---------- 日度 Brinson ----------
        bm_bond_weight = 1 - bm_stock_weight

        # 基准各分量真实收益（日度）
        r_bm_stock_d = df['r_stock']
        r_bm_bond_d  = df['r_bond']

        # 基金各分量收益（用指数代理，无法拆分内部）
        r_fund_stock_d = df['r_stock']   # 代理：经理Alpha体现在总超额里
        r_fund_bond_d  = df['r_bond']

        # 配置效应（日度）
        alloc_d = ((stock_ratio_fund - bm_stock_weight) * r_bm_stock_d +
                   (bond_ratio_fund  - bm_bond_weight)  * r_bm_bond_d)

        # 基准总收益（日度）
        r_bm_total_d = bm_stock_weight * r_bm_stock_d + bm_bond_weight * r_bm_bond_d

        # 总超额（日度）
        excess_d = df['fund'] - r_bm_total_d

        # 选择+交互（残差法，不需要拆分基金内部）
        sel_inter_d = excess_d - alloc_d

        # 年化（几何复合）
        def _annualize(s):
            """日度序列→年化（复利）"""
            if len(s) == 0:
                return 0.0
            return float(((1 + s).prod()) ** (252 / len(s)) - 1)

        alloc    = _annualize(alloc_d)
        sel_inter = _annualize(sel_inter_d)
        excess   = _annualize(excess_d)
        r_bm_ann = _annualize(df['r_bm'])
        fund_ann = _annualize(df['fund'])

        # 交互效应（单独保留，仅供深度查看）
        inter_d = ((stock_ratio_fund - bm_stock_weight) * (r_fund_stock_d - r_bm_stock_d) +
                   (bond_ratio_fund  - bm_bond_weight)  * (r_fund_bond_d  - r_bm_bond_d))
        inter    = _annualize(inter_d)
        # 纯选择效应（去掉交互）
        sel      = sel_inter - inter

    else:
        # ---------- 降级：年化均值法（数据不足时的后备） ----------
        r_stock_ann = float(stock_index_ret.mean() * 252) if len(stock_index_ret) > 0 else 0.10
        r_bond_ann  = float(bond_index_ret.mean()  * 252) if len(bond_index_ret)  > 0 else 0.03
        r_bm_ann    = float(bm_ret.mean()           * 252) if len(bm_ret)          > 0 else 0.08
        fund_ann    = float(fund_ret.mean()          * 252) if len(fund_ret)         > 0 else 0.0

        bm_bond_weight = 1 - bm_stock_weight
        r_bm_stock = r_stock_ann
        r_bm_bond  = r_bond_ann

        alloc = ((stock_ratio_fund - bm_stock_weight) * r_bm_stock +
                 (bond_ratio_fund  - bm_bond_weight)  * r_bm_bond)

        sel   = (bm_stock_weight * (r_stock_ann - r_bm_stock) +
                 bm_bond_weight  * (r_bond_ann  - r_bm_bond))
        inter = ((stock_ratio_fund - bm_stock_weight) * (r_stock_ann - r_bm_stock) +
                 (bond_ratio_fund  - bm_bond_weight)  * (r_bond_ann  - r_bm_bond))

        sel_inter = sel + inter
        excess = fund_ann - r_bm_ann

    # ---------- 大白话解读 ----------
    parts = []

    # 配置效应（站队能力）
    if alloc > 0.01:
        alloc_target = '多配股票' if stock_ratio_fund > bm_stock_weight else '多配债券'
        parts.append(
            f"配置效应 {alloc*100:+.2f}%：经理站队准确，"
            f"{alloc_target}时机踩对，带来正贡献"
        )
    elif alloc < -0.01:
        alloc_target = '多配股票' if stock_ratio_fund > bm_stock_weight else '多配债券'
        parts.append(
            f"配置效应 {alloc*100:+.2f}%：经理站队失误，"
            f"{alloc_target}时机不佳，拖累收益"
        )
    else:
        parts.append(f"配置效应 {alloc*100:+.2f}%：配置比例与基准接近，择时贡献不显著")

    # 选择+交互效应（挑货能力）
    if sel_inter > 0.01:
        parts.append(
            f"选择效应 {sel_inter*100:+.2f}%：经理挑货能力较强，"
            f"选中的股票/债券跑赢了同类基准"
        )
    elif sel_inter < -0.01:
        parts.append(
            f"选择效应 {sel_inter*100:+.2f}%：经理挑货能力偏弱，"
            f"选中的资产跑输了基准同类"
        )
    else:
        parts.append(f"选择效应 {sel_inter*100:+.2f}%：选股/择债能力与基准持平")

    # 综合判断
    if alloc > 0.01 and sel_inter < -0.01:
        parts.append("⚡ 经理择时准但选股弱：能预判大势，但具体标的选择拖后腿")
    elif alloc < -0.01 and sel_inter > 0.01:
        parts.append("⚡ 经理择时弱但选股强：大类配置失误，但个股挑选能力可圈可点")

    return {
        'allocation_effect':       alloc,        # 配置效应（年化）
        'selection_inter_effect':  sel_inter,     # 选择+交互（年化，对外展示）
        'selection_effect':        sel,           # 纯选择效应（内部，供深度查看）
        'interaction_effect':      inter,         # 纯交互效应（内部）
        'total_active':            alloc + sel_inter,
        'excess_return':           excess,        # 基金vs基准总超额（年化）
        'r_bm':                    r_bm_ann if 'r_bm_ann' in dir() else 0.0,
        'fund_annual':             fund_ann if 'fund_ann' in dir() else 0.0,
        'interpretation':          '；'.join(parts)
    }


def run_rolling_beta(fund_ret: pd.Series,
                     stock_index_ret: pd.Series,
                     bond_index_ret: pd.Series,
                     window: int = 20) -> pd.DataFrame:
    """
    20天滚动窗口双因子回归，监测仓位动态漂移
    返回 DataFrame：date / equity_beta / bond_beta
    """
    df = pd.DataFrame({'fund': fund_ret}).reset_index()
    df.columns = ['date', 'fund']
    df['date'] = pd.to_datetime(df['date'])

    si = pd.DataFrame({'date': stock_index_ret.index if hasattr(stock_index_ret,'index')
                       else range(len(stock_index_ret)),
                       'stock': stock_index_ret.values})
    bi = pd.DataFrame({'date': bond_index_ret.index if hasattr(bond_index_ret,'index')
                       else range(len(bond_index_ret)),
                       'bond': bond_index_ret.values})

    si['date'] = pd.to_datetime(si['date'])
    bi['date'] = pd.to_datetime(bi['date'])

    df = df.merge(si, on='date', how='inner').merge(bi, on='date', how='inner').dropna()

    results = []
    for i in range(window, len(df)):
        chunk = df.iloc[i-window:i]
        try:
            X = sm.add_constant(chunk[['stock','bond']])
            m = sm.OLS(chunk['fund'], X).fit()
            results.append({
                'date': chunk['date'].iloc[-1],
                'equity_beta': m.params.get('stock', np.nan),
                'bond_beta':   m.params.get('bond',  np.nan)
            })
        except Exception:
            pass

    return pd.DataFrame(results)


def detect_style_drift(rolling_df: pd.DataFrame,
                       static_stock_ratio: float,
                       threshold: float = 0.15) -> dict:
    """
    对比最新动态 Beta 与季报静态仓位，超过 threshold 则预警
    """
    if rolling_df.empty:
        return {'has_drift': False, 'message': ''}

    latest_beta = rolling_df['equity_beta'].iloc[-1]
    drift = abs(latest_beta - static_stock_ratio)

    if drift > threshold:
        direction = '大幅减仓' if latest_beta < static_stock_ratio else '大幅加仓'
        msg = (f"⚠️ 风格漂移预警：季报披露股票仓位 {static_stock_ratio*100:.0f}%，"
               f"但近20日动态估算仅 {latest_beta*100:.0f}%，"
               f"基金经理在季报之后可能已{direction}（偏差{drift*100:.0f}%）")
        return {'has_drift': True, 'message': msg, 'drift': drift,
                'latest_dynamic': latest_beta, 'static': static_stock_ratio}

    return {'has_drift': False, 'message': '', 'drift': drift,
            'latest_dynamic': latest_beta, 'static': static_stock_ratio}


# ---------- M4. 行业/主题型：中性化 Alpha ----------

def run_sector_model(fund_ret: pd.Series,
                     bm_ret: pd.Series,
                     tracking_error_window: int = 252) -> dict:
    """
    行业/主题基金：中性化 Alpha + 跟踪误差
    """
    df = pd.DataFrame({'fund': fund_ret, 'bm': bm_ret}).dropna()

    if len(df) < 30:
        return {
            'neutral_alpha': 0.0,
            'tracking_error': 0.0,
            'info_ratio': 0.0,
            'interpretation': '数据不足'
        }

    excess = df['fund'] - df['bm']
    neutral_alpha = excess.mean() * 252          # 年化中性化 Alpha
    tracking_error = excess.std() * np.sqrt(252) # 年化跟踪误差
    info_ratio = neutral_alpha / tracking_error if tracking_error > 0 else 0.0

    # 解释
    parts = []
    if neutral_alpha > 0.03:
        parts.append(f"中性化Alpha {neutral_alpha*100:.1f}%，在同类行业中显著跑赢基准")
    elif neutral_alpha > 0:
        parts.append(f"中性化Alpha {neutral_alpha*100:.1f}%，基准内表现尚可")
    else:
        parts.append(f"中性化Alpha {neutral_alpha*100:.1f}%，跑输同行业基准")

    if tracking_error < 0.03:
        parts.append("跟踪误差极低，紧密跟踪指数")
    elif tracking_error > 0.1:
        parts.append(f"跟踪误差{tracking_error*100:.1f}%偏高，偏离基准较大")
    else:
        parts.append(f"跟踪误差{tracking_error*100:.1f}%，主动管理适中")

    return {
        'neutral_alpha': neutral_alpha,
        'tracking_error': tracking_error,
        'info_ratio': info_ratio,
        'interpretation': '；'.join(parts)
    }


# ============================================================
# ██████████████  TRANSLATION LAYER  ██████████████
# ============================================================

def translate_results(model: str, results: dict,
                      basic: dict, holdings: dict) -> dict:
    """
    将量化分析结果翻译为大白话四维诊断
    返回：{character, skill, risk, advice, score}
    """
    out = {'character': '', 'skill': '', 'risk': '', 'advice': '', 'score': 60}

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

        # 性格
        if mkt_b > 1.2:
            out['character'] = f"**激进型**。{name}的市场Beta约{mkt_b:.2f}，牛市跑快熊市跑快，是市场的放大镜。"
        elif mkt_b < 0.7:
            out['character'] = f"**防守型**。Beta约{mkt_b:.2f}，擅长控制市场波动，但强牛市可能跑输大盘。"
        else:
            out['character'] = f"**均衡型**。Beta约{mkt_b:.2f}，随市场波动适中。"
        if smb_b > 0.3:
            out['character'] += f"偏好小盘股（SMB暴露{smb_b:.2f}），小盘行情好时受益更明显。"
        if hml_b > 0.3:
            out['character'] += "偏价值风格。"
        elif hml_b < -0.3:
            out['character'] += "偏成长风格。"

        # 实力
        if alpha is None:
            out['skill'] = "数据不足，无法评估Alpha。"
        elif r2 > 0.9:
            out['skill'] = (f"R²={r2:.2f}，基金在高度复制基准，几乎没有主动管理。"
                            "与其支付管理费，不如买低费率指数基金。")
        elif alpha > 0.05 and alpha_p < 0.05:
            out['skill'] = (f"年化Alpha {alpha*100:.1f}%，统计显著（p={alpha_p:.3f}）。"
                            "**这是真本事**，超额收益并非运气，经理有可复制的获利逻辑。")
        elif alpha > 0.02 and alpha_p < 0.1:
            out['skill'] = (f"年化Alpha {alpha*100:.1f}%，有一定主动能力，但统计显著性不够强。"
                            "需要更长时间验证。")
        elif alpha > 0:
            out['skill'] = (f"年化Alpha {alpha*100:.1f}%，但统计不显著，超额收益可能有运气成分。")
        else:
            out['skill'] = (f"年化Alpha {alpha*100:.1f}%为负，跑输风险调整后基准，需警惕。")

        # 风险
        risks = []
        if r2 > 0.9 and fee_total > 0.01:
            risks.append(f"高R²+高管理费（{fee_total*100:.2f}%）：花了主动管理费，买了被动产品")
        if mkt_b > 1.3:
            risks.append("Beta过高，牛熊市放大效应明显，需控制仓位")
        if smb_b > 0.5:
            risks.append("重仓小盘股，流动性风险较高，市场下行时可能跌幅更大")
        out['risk'] = '；'.join(risks) if risks else "风险特征正常，无明显异常。"

        # 建议
        if alpha and alpha > 0.05 and alpha_p < 0.05:
            out['advice'] = "经理实力经过统计验证，适合长期持有，可适当提高配置比例。"
            out['score'] = 80
        elif alpha and alpha > 0:
            out['advice'] = "有一定超额收益，但需继续观察，建议持有并定期复查。"
            out['score'] = 65
        else:
            out['advice'] = "Alpha不理想，建议对比同类产品，考虑是否有更好的替代选择。"
            out['score'] = 45

    elif model == 'bond':
        # 优先用底层真实久期（已做仓位修正），回退到组合久期
        dur_underlying = results.get('duration_underlying', results.get('duration', 5.0))
        dur_portfolio  = results.get('duration', 5.0)
        conv           = results.get('convexity', 0.0)
        carry          = results.get('carry_alpha', results.get('credit_spread_alpha', 0.0))
        bond_ratio_used = results.get('bond_ratio_used', 1.0)

        # character：展示底层真实久期 + 若仓位<80%则加括号说明
        if bond_ratio_used < 0.80:
            dur_note = (f"（底层债券头寸真实久期，已修正仓位{bond_ratio_used*100:.0f}%）")
        else:
            dur_note = ''
        out['character'] = (
            f"**久期型**债基，底层有效久期 {dur_underlying:.1f} 年{dur_note}，"
            f"对利率变化{'非常敏感' if dur_underlying>7 else '中度敏感' if dur_underlying>3 else '不敏感'}。"
        )

        # skill：综合carry（不再单说"信用溢价"，改为更准确的"综合carry"）
        carry_label = '偏高，需关注信用/流动性风险' if carry > 0.04 else '合理' if carry > 0.015 else '较低'
        out['skill'] = (
            f"年化综合carry（票息+信用溢价+骑乘收益）{carry*100:.2f}%，{carry_label}；"
            f"凸性 {conv:.1f}，{'正凸性，价格\"涨得比跌得快\"，有缓冲保护' if conv>0 else '凸性偏低，利率保护有限'}。"
        )

        risks = []
        if dur_underlying > 7:
            risks.append(f"底层久期{dur_underlying:.1f}年，利率若上行1%，债券头寸约损失{dur_underlying:.0f}%")
        if carry > 0.04:
            risks.append(f"综合carry偏高（{carry*100:.1f}%），可能含高票息信用债，需防范违约/流动性风险")
        if conv < 0:
            risks.append("凸性为负，利率大幅波动时缺乏保护，注意极端行情风险")
        out['risk'] = '；'.join(risks) if risks else "债券风险特征正常。"
        out['advice'] = "适合中低风险偏好投资者，注意利率周期配置时机。"
        out['score']  = 70 if carry > 0 and dur_underlying < 6 else 55

    elif model == 'mixed':
        alloc      = results.get('allocation_effect', 0)
        sel_inter  = results.get('selection_inter_effect',
                                 results.get('selection_effect', 0))  # 兼容旧字段
        excess     = results.get('excess_return', 0)
        drift      = results.get('drift_info', {})

        # character：说明混合型基金的两条超额收益来源
        out['character'] = (
            "**混合型**，同时暴露股债两类风险，超额收益来自『站队』（配置）和『挑货』（选股）两条路。"
        )

        # skill：只展示配置效应和选择效应（交互已并入选择），不用解释复杂的交互概念
        if abs(alloc) > abs(sel_inter):
            dominant = "经理的核心优势在于**大类资产配置**（择时站队），选股贡献相对次要"
        elif abs(sel_inter) > abs(alloc):
            dominant = "经理的核心优势在于**标的选择**（挑货选股），配置贡献相对次要"
        else:
            dominant = "配置与选股贡献相当，两方面能力均衡"

        # 择时/选股方向性分析
        alloc_dir = "站队准确" if alloc > 0 else "站队失误"
        sel_dir   = "挑货出色" if sel_inter > 0 else "挑货拖后腿"

        out['skill'] = (
            f"配置效应（站队）{alloc*100:+.2f}%，选择效应（挑货）{sel_inter*100:+.2f}%；"
            f"总超额{excess*100:+.2f}%。{dominant}。"
        )

        # 特殊情况诊断
        if alloc > 0.01 and sel_inter < -0.01:
            out['skill'] += "⚡ 经理择时准但选股弱：能预判大势，但具体标的拖了后腿。"
        elif alloc < -0.01 and sel_inter > 0.01:
            out['skill'] += "⚡ 经理择时弱但选股强：大类配置失误，但个股挑选能力可圈可点。"

        # risk：优先展示风格漂移预警
        drift_msg = drift.get('message', '')
        if drift_msg:
            out['risk'] = drift_msg
        elif excess < 0:
            out['risk'] = (
                f"总超额收益为负（{excess*100:+.2f}%），"
                f"{'配置失误是主因' if abs(alloc) > abs(sel_inter) else '选股能力不足是主因'}，"
                f"需持续跟踪改善情况。"
            )
        else:
            out['risk'] = "股债配置均衡，无明显风格漂移预警。"

        out['advice'] = "适合中等风险偏好，建议关注基金季报了解最新仓位动向。"
        out['score']  = 70 if excess > 0 else 50

    elif model == 'sector':
        na    = results.get('neutral_alpha', 0)
        te    = results.get('tracking_error', 0)
        ir    = results.get('info_ratio', 0)
        out['character'] = "**行业/主题型**，Beta暴露高度集中于特定行业，收益来源行业Beta+经理Alpha。"
        out['skill']     = f"中性化Alpha {na*100:.1f}%，跟踪误差 {te*100:.1f}%，信息比率 {ir:.2f}。"
        out['risk']      = "行业集中度高，需承担较强的行业系统性风险。"
        out['advice']    = "适合有行业判断能力的投资者，不建议作为核心持仓。"
        out['score']     = 75 if na > 0.03 else 55

    return out


# ============================================================
# ██████████████  VISUALIZATION LAYER  ██████████████
# ============================================================

def plot_cumulative_return(nav_df: pd.DataFrame, bm_df: pd.DataFrame) -> go.Figure:
    """累计收益对比图（基金 vs 基准）"""
    fig = go.Figure()

    # 基金累计收益
    nav = nav_df.copy()
    nav['cum'] = (1 + nav['ret'].fillna(0)).cumprod() - 1

    fig.add_trace(go.Scatter(
        x=nav['date'], y=nav['cum'] * 100,
        name='基金净值', line=dict(color='#e74c3c', width=2)
    ))

    # 基准累计收益
    if not bm_df.empty:
        bm = bm_df.copy()
        bm['cum'] = (1 + bm['bm_ret'].fillna(0)).cumprod() - 1
        fig.add_trace(go.Scatter(
            x=bm['date'], y=bm['cum'] * 100,
            name='业绩基准', line=dict(color='#3498db', width=2, dash='dash')
        ))

    fig.update_layout(
        title='累计收益率对比 (%)',
        xaxis_title='', yaxis_title='累计收益率 (%)',
        plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(x=0.01, y=0.99),
        height=380, margin=dict(l=40, r=20, t=40, b=30)
    )
    return fig


def plot_rolling_beta(rolling_df: pd.DataFrame, static_ratio: float) -> go.Figure:
    """滚动仓位监控图"""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=rolling_df['date'], y=rolling_df['equity_beta'],
        name='动态股票仓位', line=dict(color='#e74c3c', width=2)
    ))

    fig.add_hline(y=static_ratio, line_dash='dash', line_color='#3498db',
                  annotation_text=f'季报仓位 {static_ratio*100:.0f}%',
                  annotation_position='bottom right')

    fig.add_hline(y=static_ratio + 0.15, line_dash='dot', line_color='#e67e22', opacity=0.5)
    fig.add_hline(y=max(0, static_ratio - 0.15), line_dash='dot', line_color='#e67e22', opacity=0.5)

    fig.update_layout(
        title='20日滚动股票仓位监控',
        yaxis=dict(tickformat='.0%'), height=320,
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=40, b=30)
    )
    return fig


def plot_factor_bar(betas: dict) -> go.Figure:
    """因子暴露度条形图"""
    labels = {'Mkt': '市场因子', 'SMB': '规模因子（小盘+）',
              'HML': '价值因子（价值+）', 'MOM': '动能因子'}
    names  = [labels.get(k, k) for k in betas]
    values = [betas[k] for k in betas]
    colors = ['#e74c3c' if v > 0 else '#27ae60' for v in values]

    fig = go.Figure(go.Bar(
        y=names, x=values, orientation='h',
        marker_color=colors, text=[f'{v:.3f}' for v in values],
        textposition='outside'
    ))
    fig.update_layout(
        title='因子暴露度（标准化系数）',
        xaxis_title='系数', height=300,
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=180, r=40, t=40, b=30)
    )
    return fig


def plot_brinson_waterfall(brinson: dict) -> go.Figure:
    """
    Brinson归因柱状图（简洁版：3柱）
    配置效应（站队）/ 选择效应（挑货，含交互）/ 总超额收益
    交互效应并入选择效应，不单独展示，符合机构实务惯例
    """
    alloc     = brinson.get('allocation_effect', 0) * 100
    sel_inter = brinson.get('selection_inter_effect',
                            brinson.get('selection_effect', 0)) * 100
    excess    = brinson.get('excess_return', 0) * 100

    labels = ['配置效应\n（站队）', '选择效应\n（挑货）', '总超额收益']
    values = [alloc, sel_inter, excess]
    colors = ['#27ae60' if v >= 0 else '#e74c3c' for v in values]
    colors[-1] = '#3498db'   # 总超额用蓝色

    fig = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=colors,
        text=[f'{v:+.2f}%' for v in values],
        textposition='outside',
        width=0.45
    ))
    fig.add_hline(y=0, line_dash='solid', line_color='#333', line_width=1)
    fig.update_layout(
        title='Brinson归因分解（年化，%）',
        yaxis_title='收益贡献 (%)', height=320,
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=40, b=30)
    )
    return fig


# ============================================================
# ██████████████  UI HELPERS  ██████████████
# ============================================================

def _kpi(label: str, value: str, color: str = '') -> str:
    cls = f'kpi {color}' if color else 'kpi'
    return f'<div class="{cls}"><div class="kpi-val">{value}</div><div class="kpi-lbl">{label}</div></div>'


def fmt_pct(v, decimals=1) -> str:
    if v is None: return 'N/A'
    return f'{v*100:+.{decimals}f}%'


def fmt_f(v, decimals=2) -> str:
    if v is None: return 'N/A'
    return f'{v:.{decimals}f}'


# ============================================================
# ██████████████  MAIN APP  ██████████████
# ============================================================

def main():
    render_css()

    # ---- Hero Banner ----
    st.markdown("""
<div class="hero">
  <h1>🔬 DeepInFund</h1>
  <p>deep in fund, seek for truth</p>
</div>
""", unsafe_allow_html=True)

    # ---- 输入区 ----
    col_in, col_year, col_btn = st.columns([3, 2, 2])
    with col_in:
        fund_code = st.text_input("基金代码", placeholder="如：000001", label_visibility='collapsed')
    with col_year:
        period_opts = ['1年', '3年', '5年', '10年', '自成立以来', '现任经理以来']
        period_sel  = st.selectbox("分析区间", period_opts, index=2, label_visibility='collapsed')
    with col_btn:
        go_btn = st.button("🔍 开始深度分析", type="primary", use_container_width=True)

    if not go_btn or not fund_code:
        st.info("输入基金代码，点击开始分析。支持所有公募基金（股票型 / 债券型 / 混合型 / 行业主题）。")
        return

    fund_code = fund_code.strip()

    # ============================================================
    # STEP 1  基本信息
    # ============================================================
    with st.spinner("获取基金基本信息..."):
        basic = fetch_basic_info(fund_code)

    if basic['name'] == fund_code:
        st.error("基金信息获取失败，请检查代码是否正确。")
        return

    # ============================================================
    # STEP 2  净值历史
    # ============================================================
    # 解析时间区间
    _period_year_map = {'1年': 1, '3年': 3, '5年': 5, '10年': 10}
    _since_inception = (period_sel == '自成立以来')
    _since_manager   = (period_sel == '现任经理以来')
    _years = _period_year_map.get(period_sel, 5)

    with st.spinner("加载历史净值..."):
        nav_df = fetch_nav(
            fund_code,
            years=_years,
            since_inception=_since_inception,
            manager_start=basic.get('manager_start_date', '') if _since_manager else ''
        )

    if nav_df.empty or len(nav_df) < 60:
        st.error("历史净值数据不足（<60天），无法进行量化分析。")
        return

    start_str = nav_df['date'].min().strftime('%Y%m%d')
    end_str   = nav_df['date'].max().strftime('%Y%m%d')

    # ============================================================
    # STEP 3  持仓与仓位判断
    # ============================================================
    with st.spinner("获取持仓数据..."):
        holdings = fetch_holdings(fund_code, basic['type_category'])

    stock_ratio = holdings.get('stock_ratio', 0.8)
    bond_ratio  = holdings.get('bond_ratio',  0.1)

    # 逻辑网关（漏斗式，返回 tuple）
    _convertible_ratio = holdings.get('convertible_ratio', 0.0)
    model_type, _gateway_warn = detect_fund_model(
        stock_ratio, basic['type_category'], _convertible_ratio
    )

    # 债券仓位超过 20% 时同时引入久期归因
    also_duration = (model_type in ('equity','mixed') and bond_ratio > 0.20)

    # ============================================================
    # STEP 4  基准构建
    # ============================================================
    with st.spinner("构建业绩基准..."):
        parsed_bm = basic['benchmark_parsed']
        if not parsed_bm:
            # 默认基准：权益→沪深300，债券→中债，混合→自定义60/40
            defaults = {
                'equity': {'type':'single','components':[{'name':'沪深300','code':'sh000300','weight':1.0}]},
                'bond':   {'type':'single','components':[{'name':'中债综合','code':None,'weight':1.0}]},
                'mixed':  {'type':'custom','components':[
                    {'name':'沪深300','code':'sh000300','weight':0.6},
                    {'name':'中债综合','code':None,'weight':0.4}]},
                'sector': {'type':'single','components':[{'name':'沪深300','code':'sh000300','weight':1.0}]},
                'index':  {'type':'single','components':[{'name':'沪深300','code':'sh000300','weight':1.0}]},
            }
            parsed_bm = defaults.get(basic['type_category'], defaults['equity'])
        bm_df = build_benchmark_ret(parsed_bm, start_str, end_str)

    # ============================================================
    # STEP 5  运行量化模型
    # ============================================================
    model_results = {}
    drift_info    = {}

    # ----- 权益模型 -----
    if model_type == 'equity' or model_type == 'sector':
        with st.spinner("运行因子模型..."):
            factors = fetch_ff_factors(start_str, end_str)

        if not factors.empty:
            eq_model = select_equity_model(basic['name'])
            fund_ret_s = nav_df.set_index('date')['ret'].dropna()
            model_results = run_ff_model(fund_ret_s, factors, eq_model)
        else:
            model_results = _empty_ff_result('因子数据获取失败')
            eq_model = 'ff3'

        # 行业主题补充跟踪误差
        if model_type == 'sector':
            fund_ret_s = nav_df.set_index('date')['ret'].dropna()
            bm_ret_s   = bm_df.set_index('date')['bm_ret'].dropna() if not bm_df.empty else pd.Series([], dtype=float)
            sector_res = run_sector_model(fund_ret_s, bm_ret_s)
            model_results['sector'] = sector_res

    # ----- 债券模型 -----
    if model_type == 'bond' or also_duration:
        with st.spinner("获取国债收益率数据..."):
            treasury = fetch_treasury_10y(start_str, end_str)

        if not treasury.empty:
            fund_ret_s = nav_df.set_index('date')['ret'].dropna()
            # 传入 bond_ratio 用于仓位修正（底层债券真实久期 = 组合久期 / 债券仓位）
            bond_res   = run_duration_model(fund_ret_s, treasury, bond_ratio=bond_ratio)
        else:
            bond_res = {'duration': 5.0, 'duration_underlying': 5.0,
                        'convexity': 0.0, 'carry_alpha': 0.0,
                        'credit_spread_alpha': 0.0, 'r_squared': 0.0,
                        'bond_ratio_used': bond_ratio,
                        'interpretation': '国债收益率数据获取失败'}
        model_results['bond'] = bond_res

    # ----- 混合模型 -----
    if model_type == 'mixed':
        with st.spinner("运行Brinson归因..."):
            stock_idx = fetch_index_daily('sh000300', start_str, end_str)
            bond_idx  = fetch_bond_index(start_str, end_str)

            fund_ret_s = nav_df.set_index('date')['ret'].dropna()
            bm_ret_s   = bm_df.set_index('date')['bm_ret'] if not bm_df.empty else pd.Series([], dtype=float)

            brinson = run_brinson(
                fund_ret_s, bm_ret_s,
                stock_ratio, bond_ratio,
                stock_idx.set_index('date')['ret'] if not stock_idx.empty else pd.Series([], dtype=float),
                bond_idx.set_index('date')['ret']  if not bond_idx.empty  else pd.Series([], dtype=float),
                bm_stock_weight=0.6
            )
            model_results = brinson

            # 滚动仓位监控
            if not stock_idx.empty and not bond_idx.empty:
                rolling_df = run_rolling_beta(
                    fund_ret_s,
                    stock_idx.set_index('date')['ret'],
                    bond_idx.set_index('date')['ret']
                )
                drift_info = detect_style_drift(rolling_df, stock_ratio)
                model_results['rolling_df']  = rolling_df
                model_results['drift_info']  = drift_info

    # ----- 大白话翻译 -----
    translation = translate_results(model_type, model_results, basic, holdings)

    # ============================================================
    # DISPLAY
    # ============================================================

    # ---------- Part 1: 基本信息速览 ----------
    st.markdown('<div class="section-title">📋 第一部分：基本信息速览</div>', unsafe_allow_html=True)

    bm_text = basic['benchmark_text'] or '未获取到业绩基准'
    st.markdown(f"""
<div class="card">
  <b>{basic['name']}</b> &nbsp;
  <span class="tag tag-blue">{basic['type_raw'] or '未知类型'}</span>
  <span class="tag tag-gray">成立于 {basic['establish_date'] or 'N/A'}</span>
  <span class="tag tag-gray">{basic['company'] or '未知公司'}</span>
  <br><br>
  <b>基金经理：</b>{basic['manager'] or 'N/A'}&nbsp;&nbsp;
  <b>最新规模：</b>{basic['scale'] or 'N/A'}
</div>
""", unsafe_allow_html=True)

    # 业绩 KPI（4格）
    c1, c2, c3, c4 = st.columns(4)
    _total_ret = (nav_df['nav'].iloc[-1] / nav_df['nav'].iloc[0] - 1) * 100
    _trading_days = max(len(nav_df.dropna(subset=['ret'])), 1)
    _ann_ret = ((1 + _total_ret/100) ** (252 / _trading_days) - 1) * 100
    _nav_cum = nav_df['nav']
    _roll_max = _nav_cum.cummax()
    _max_dd = (((_nav_cum - _roll_max) / _roll_max).min()) * 100
    with c1:
        st.markdown(_kpi('最新净值', f'{nav_df["nav"].iloc[-1]:.4f}'), unsafe_allow_html=True)
    with c2:
        _c = 'kpi-red' if _total_ret > 0 else 'kpi-green'
        st.markdown(_kpi(f'{period_sel}总收益', f'{_total_ret:+.1f}%', _c), unsafe_allow_html=True)
    with c3:
        _c = 'kpi-red' if _ann_ret > 0 else 'kpi-green'
        st.markdown(_kpi('年化收益（估）', f'{_ann_ret:+.1f}%', _c), unsafe_allow_html=True)
    with c4:
        _c = 'kpi-green' if _max_dd > -10 else 'kpi-orange'
        st.markdown(_kpi('最大回撤', f'{_max_dd:.1f}%', _c), unsafe_allow_html=True)

    # 费率区域（显性 + 隐性）
    st.markdown('<div style="font-size:0.85rem;font-weight:600;color:#555;margin:16px 0 8px">💰 费用概览</div>', unsafe_allow_html=True)

    with st.spinner("估算隐性费率..."):
        hidden = estimate_hidden_cost(fund_code, nav_df)

    _explicit_total = basic['fee_manage'] + basic['fee_custody'] + basic['fee_sale']
    _manage_str  = f"{basic['fee_manage']*100:.2f}%"  if basic['fee_manage']  else '—'
    _custody_str = f"{basic['fee_custody']*100:.2f}%" if basic['fee_custody'] else '—'
    _sale_str    = f"{basic['fee_sale']*100:.2f}%"    if basic['fee_sale']    else '—'
    _hc = hidden.get('hidden_cost_rate')
    _hc_str   = f"{_hc*100:.2f}%" if _hc is not None else '暂无数据'
    _hc_warn  = hidden.get('is_high_turnover', False)
    _hc_method = hidden.get('method', '暂无数据')
    _total_cost_str = f"{(_explicit_total + (_hc or 0))*100:.2f}%" if _hc else f"≥{_explicit_total*100:.2f}%"

    fee_c1, fee_c2 = st.columns(2)
    with fee_c1:
        _fee_card_class = 'card card-warn' if _explicit_total > 0.015 else 'card'
        st.markdown(f"""
<div class="{_fee_card_class}">
  <b>📌 显性费率（年）</b>
  <div style="margin-top:8px;font-size:0.88rem;line-height:2.2">
    管理费：<b>{_manage_str}</b> &nbsp;|&nbsp; 托管费：<b>{_custody_str}</b> &nbsp;|&nbsp; 销售服务费：<b>{_sale_str}</b><br>
    <b>合计：{_explicit_total*100:.2f}%/年</b>
    {'&nbsp;<span class="tag tag-orange">费率偏高</span>' if _explicit_total > 0.015 else ''}
  </div>
  <div style="font-size:0.78rem;color:#888;margin-top:6px">每年从净值里自动扣除，看不见但实实在在在付</div>
</div>
""", unsafe_allow_html=True)
    with fee_c2:
        _hc_card_class = 'card card-warn' if _hc_warn else 'card'
        st.markdown(f"""
<div class="{_hc_card_class}">
  <b>🔍 隐性费率（估算）</b>
  <div style="margin-top:8px;font-size:0.88rem;line-height:2.2">
    交易成本估算：<b>{_hc_str}/年</b>
    {'&nbsp;<span class="tag tag-red">高换手警示</span>' if _hc_warn else ''}<br>
    <b>显性+隐性合计：约 {_total_cost_str}/年</b>
  </div>
  <div style="font-size:0.78rem;color:#888;margin-top:6px">📊 {_hc_method}</div>
</div>
""", unsafe_allow_html=True)

    if hidden.get('alert'):
        st.markdown(f'<div class="drift-alert">{hidden["alert"]}</div>', unsafe_allow_html=True)

    # ---------- Part 2: 量化分析结果 ----------
    st.markdown('<div class="section-title">📊 第二部分：深度量化分析</div>', unsafe_allow_html=True)

    # 可转债迷雾 / 逻辑网关提示
    if _gateway_warn:
        st.markdown(f'<div class="drift-alert">{_gateway_warn}</div>', unsafe_allow_html=True)

    # 显示基金类型 + 核心关注点（不显示模型名称）
    _type_focus = {
        'equity': ('主动权益型', '核心关注：超额收益Alpha是否真实、市场Beta水平、风格因子暴露'),
        'bond':   ('纯债/固收型', '核心关注：利率敏感度（久期）、信用下沉风险、凸性保护'),
        'mixed':  ('股债混合型', '核心关注：仓位配置是否有效、选股能力、动态仓位是否漂移'),
        'sector': ('行业/主题型', '核心关注：行业内超额Alpha、对标基准的跟踪误差'),
    }
    _type_name, _type_focus_text = _type_focus.get(model_type, ('主动权益型', ''))
    st.markdown(f"""
<div class="card card-info">
  <b>🎯 {_type_name}</b> &nbsp;
  <span class="tag tag-blue">股票仓位 {stock_ratio*100:.0f}%</span>
  <span class="tag tag-gray">债券仓位 {bond_ratio*100:.0f}%</span>
  <br><span style="font-size:0.88rem;color:#555;margin-top:6px;display:block">{_type_focus_text}</span>
</div>
""", unsafe_allow_html=True)

    # 指标一句话解释（大白话版）
    _EXPLAIN = {
        '年化 Alpha':   'Alpha = 剔除市场涨跌后，基金经理靠真本事多赚的年化收益。正值越高越好，P值<0.05才算显著。',
        '市场 Beta':    'Beta = 跟大盘的联动程度。1.0代表同步，>1.0涨跌都比大盘更猛，<1.0更稳一些。',
        'R²':          'R² = 有多少涨跌可以用市场因子来解释。越高说明基金越像指数，经理主动管理空间越小。',
        'Alpha P值':   'P值 = Alpha的可信度。<0.05才算统计显著，否则那点Alpha可能只是运气。',
        '有效久期':     '久期 = 利率敏感度。久期越长，降息时涨得越多，加息时跌得也越惨。像弹簧，越长越弹。',
        '凸性':         '凸性 = 久期的保护层。凸性越高，涨的时候久期会自动变长多赚，跌的时候久期缩短少亏。好事。',
        '年化信用溢价': '综合carry = 票息收入 + 信用溢价 + 骑乘收益的年化总和（回归截距项）。偏高（>4%）时，除了信用风险，也可能是持有高票息短债或利用了曲线陡峭度，并非全是违约风险。',
        '配置效应':     '配置效应（站队）= 基金在股/债大类上的配比与基准不同而带来的超额。经理在股市好时多配了股票，就会有正的配置效应——这是"择时站队"的分。',
        '选择效应':     '选择效应（挑货）= 在相同仓位权重下，基金选的标的比基准同类跑得好/差带来的超额（含交互效应）。这是"选股选债"的分，正值说明经理比基准更会挑。',
        '交互效应':     '交互效应 = 配置超配+同时选对标的带来的叠加奖金（已并入选择效应展示）。可理解为"两种能力同向发力的化学反应"。',
        '总超额收益':   '总超额收益 = 配置效应 + 选择效应 + 交互效应之和。正值即跑赢基准，负值即跑输基准。',
        '中性化 Alpha': '中性化 Alpha = 剔除了行业整体涨跌之后，基金在行业内部的超额选股能力。正值说明选的股比同行强。',
        '跟踪误差':     '跟踪误差 = 基金收益与基准收益的偏离程度。越小越贴近基准，越大说明经理主动调整越多。',
        '信息比率':     '信息比率 = 超额收益 ÷ 跟踪误差，衡量"冒每一分风险赚到多少超额"。通常>0.5算不错。',
    }

    def _explain_row(key: str) -> str:
        """渲染指标解释小字行"""
        txt = _EXPLAIN.get(key, '')
        if not txt:
            return ''
        return (f'<div style="font-size:0.75rem;color:#888;margin:-6px 0 12px;'
                f'padding:4px 8px;border-left:2px solid #ddd;line-height:1.5">{txt}</div>')

    # 权益类结果
    if model_type in ('equity',) and model_results:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            a = model_results.get('alpha')
            color = 'kpi-red' if (a and a > 0) else 'kpi-green'
            st.markdown(_kpi('年化 Alpha', fmt_pct(a), color), unsafe_allow_html=True)
        with c2:
            mkt_b = model_results.get('factor_betas', {}).get('Mkt', None)
            st.markdown(_kpi('市场 Beta', fmt_f(mkt_b)), unsafe_allow_html=True)
        with c3:
            r2 = model_results.get('r_squared', 0)
            st.markdown(_kpi('R²（模型解释度）', fmt_f(r2),
                             'kpi-orange' if r2 > 0.9 else ''), unsafe_allow_html=True)
        with c4:
            p = model_results.get('alpha_pval', 1.0)
            st.markdown(_kpi('Alpha P值', fmt_f(p, 3),
                             'kpi-green' if p < 0.05 else 'kpi-orange'), unsafe_allow_html=True)

        # 指标解释
        st.markdown(
            _explain_row('年化 Alpha') +
            _explain_row('市场 Beta') +
            _explain_row('R²') +
            _explain_row('Alpha P值'),
            unsafe_allow_html=True
        )

        # 量化解读点评
        _interp = model_results.get('interpretation', '')
        if _interp:
            st.markdown(f'<div class="card" style="font-size:0.88rem;color:#444">{_interp}</div>',
                        unsafe_allow_html=True)

        # 因子暴露图
        betas = model_results.get('factor_betas', {})
        if betas:
            _eq_model_name = select_equity_model(basic['name'])
            _eq_model_explain = {
                'capm':    '大盘涨它跟涨、大盘跌它跟跌的"随大流"程度测试。Alpha代表剔除大盘贡献后，经理凭真本事多赚的钱。',
                'ff3':     '"贴标签"检测。经理赚钱是因为眼光好，还是因为刚好买了"小公司"或"便宜货"？SMB/HML因子告诉你答案。',
                'ff5':     '"盈利质量"检测。在FF3基础上加了RMW盈利因子，判断经理选的是不是真正赚钱的好公司，而不是"便宜但亏损"的陷阱。',
                'carhart': '"追风"检测。在FF3基础上加了动量因子(MOM)，看经理是提前埋伏的"预言家"，还是追涨的"跟风者"。',
            }.get(_eq_model_name, '')

            # 原始Beta弹性说明（"大盘涨1%，该基金预期涨X%"）
            _raw_betas = model_results.get('factor_betas_raw', {})
            _mkt_raw   = _raw_betas.get('Mkt')
            _beta_drift = model_results.get('beta_drift')
            _raw_beta_text = ''
            if _mkt_raw is not None:
                _raw_beta_text = f'市场弹性：大盘涨 1%，该基金预期涨 <b>{_mkt_raw:.2f}%</b>（原始Beta，未标准化）'
                if _beta_drift is not None and abs(_beta_drift) > 0.2:
                    _recent_mkt = model_results.get('recent_betas_raw', {}).get('Mkt', _mkt_raw)
                    _raw_beta_text += (f'；近半年Beta变为 <b>{_recent_mkt:.2f}</b>，'
                                       f'较全期偏差{_beta_drift:+.2f}，注意风格漂移')

            with st.expander("📊 因子暴露度详情", expanded=True):
                st.plotly_chart(plot_factor_bar(betas), use_container_width=True)
                # 原始Beta弹性小字
                if _raw_beta_text:
                    st.markdown(
                        f'<div style="font-size:0.78rem;color:#888;padding:4px 10px;'
                        f'border-left:3px solid #3498db;margin:4px 0 6px">'
                        f'{_raw_beta_text}</div>',
                        unsafe_allow_html=True
                    )
                # 模型说明
                if _eq_model_explain:
                    st.markdown(
                        f'<div style="font-size:0.8rem;color:#666;padding:6px 10px;'
                        f'background:#f8f9fa;border-radius:6px;margin-top:4px">'
                        f'📖 {_eq_model_explain}</div>',
                        unsafe_allow_html=True
                    )

    # 债券类结果
    if 'bond' in model_results:
        br = model_results['bond']
        # 优先展示底层真实久期（已做仓位修正），回退到组合级久期
        d_underlying = br.get('duration_underlying', br.get('duration', 0))
        d_portfolio  = br.get('duration', 0)
        bond_ratio_used = br.get('bond_ratio_used', 1.0)
        carry        = br.get('carry_alpha', br.get('credit_spread_alpha', 0))

        c1, c2, c3 = st.columns(3)
        with c1:
            # 展示底层真实久期，tooltip 说明仓位修正逻辑
            dur_label = '底层有效久期（年）' if bond_ratio_used < 0.80 else '有效久期（年）'
            st.markdown(_kpi(dur_label, fmt_f(d_underlying),
                             'kpi-orange' if d_underlying > 7 else ''), unsafe_allow_html=True)
        with c2:
            conv = br.get('convexity', 0)
            st.markdown(_kpi('有效凸性', fmt_f(conv),
                             'kpi-green' if conv > 0 else 'kpi-orange'), unsafe_allow_html=True)
        with c3:
            st.markdown(_kpi('年化综合carry', fmt_pct(carry),
                             'kpi-orange' if carry > 0.04 else ''), unsafe_allow_html=True)

        # 仓位修正说明小字（仅当非纯债基金时展示）
        if bond_ratio_used < 0.80:
            st.markdown(
                f'<div style="font-size:0.76rem;color:#888;padding:3px 10px;'
                f'border-left:3px solid #e67e22;margin:2px 0 8px">'
                f'⚙️ 仓位修正：组合久期 {d_portfolio:.1f}年 ÷ 债券仓位 {bond_ratio_used*100:.0f}%'
                f' = 底层债券头寸真实久期 {d_underlying:.1f}年</div>',
                unsafe_allow_html=True
            )

        # 指标解释
        st.markdown(
            _explain_row('有效久期') +
            _explain_row('凸性') +
            _explain_row('年化信用溢价'),
            unsafe_allow_html=True
        )

        _bond_interp = br.get('interpretation', '')
        if _bond_interp:
            st.markdown(f'<div class="card" style="font-size:0.88rem;color:#444">{_bond_interp}</div>',
                        unsafe_allow_html=True)

        # 债券模型一句话解读（更新 carry 说明）
        st.markdown(
            '<div style="font-size:0.8rem;color:#666;padding:6px 10px;'
            'background:#f8f9fa;border-radius:6px;margin-top:4px">'
            '📖 久期归因（"利率敏感度"测试）：久期越长，对利率上行越敏感；'
            '综合carry = 票息收入 + 信用溢价 + 骑乘收益，偏高时需关注信用风险。</div>',
            unsafe_allow_html=True
        )

    # 混合类结果
    if model_type == 'mixed' and model_results:
        alloc_v   = model_results.get('allocation_effect', 0)
        sel_v     = model_results.get('selection_inter_effect',
                                      model_results.get('selection_effect', 0))  # 兼容旧字段
        inter_v   = model_results.get('interaction_effect', 0)
        ex_v      = model_results.get('excess_return', 0)

        # KPI 卡：只展示"配置效应"和"选择效应（含交互）"两项，总超额独立一列
        c1, c2, c3 = st.columns(3)
        with c1:
            alloc_color = 'kpi-red' if alloc_v > 0 else 'kpi-green'
            st.markdown(_kpi('配置效应（站队）', fmt_pct(alloc_v), alloc_color),
                        unsafe_allow_html=True)
        with c2:
            sel_color = 'kpi-red' if sel_v > 0 else 'kpi-green'
            st.markdown(_kpi('选择效应（挑货）', fmt_pct(sel_v), sel_color),
                        unsafe_allow_html=True)
        with c3:
            ex_color = 'kpi-red' if ex_v > 0 else 'kpi-green'
            st.markdown(_kpi('总超额收益', fmt_pct(ex_v), ex_color), unsafe_allow_html=True)

        # 归因维度大白话注释
        st.markdown(
            '<div style="font-size:0.78rem;color:#888;padding:6px 12px;'
            'background:#fafafa;border-radius:6px;margin:4px 0 8px;'
            'display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px">'
            '<span>📌 <b>站队</b>：股市好时多配股票，债市好时多配债券</span>'
            '<span>📌 <b>挑货</b>：同类资产里，选的标的跑赢了基准同类（含交互效应）</span>'
            '<span>📌 <b>总超额</b> = 站队 + 挑货</span>'
            '</div>',
            unsafe_allow_html=True
        )

        # 指标解释
        st.markdown(
            _explain_row('配置效应') +
            _explain_row('选择效应') +
            _explain_row('总超额收益'),
            unsafe_allow_html=True
        )

        _mix_interp = model_results.get('interpretation', '')
        if _mix_interp:
            st.markdown(f'<div class="card" style="font-size:0.88rem;color:#444">{_mix_interp}</div>',
                        unsafe_allow_html=True)

        # Brinson 归因瀑布图（展示两柱：配置 + 选择&交互，更简洁）
        with st.expander("📊 Brinson 归因瀑布图", expanded=True):
            st.plotly_chart(plot_brinson_waterfall(model_results), use_container_width=True)

            # 交互效应小字注释（供深度查看）
            if abs(inter_v) > 0.002:
                inter_note = (f'（其中纯选择效应 {model_results.get("selection_effect", 0)*100:+.2f}%，'
                              f'交互效应 {inter_v*100:+.2f}% 已并入选择效应展示）')
            else:
                inter_note = ''

            # 瀑布图下方诊断文字
            if abs(alloc_v) > abs(sel_v):
                _brinson_comment = (
                    f'超额收益主要来自"站队"（配置效应 {alloc_v*100:+.2f}%）：'
                    f'经理在股债大类资产的仓位决策贡献了更多价值。{inter_note}'
                )
            elif abs(sel_v) > abs(alloc_v):
                _brinson_comment = (
                    f'超额收益主要来自"挑货"（选择效应 {sel_v*100:+.2f}%）：'
                    f'经理在具体标的选择上展现出真实能力。{inter_note}'
                )
            elif ex_v < 0:
                _brinson_comment = (
                    f'总超额收益为负（{ex_v*100:+.2f}%），'
                    f'站队和挑货均未跑赢基准，需关注是否发生风格漂移。{inter_note}'
                )
            else:
                _brinson_comment = (
                    f'配置效应与选择效应基本均衡，总超额收益{ex_v*100:+.2f}%。{inter_note}'
                )

            st.markdown(
                f'<div style="font-size:0.8rem;color:#666;padding:6px 10px;'
                f'background:#f8f9fa;border-radius:6px;margin-top:4px">'
                f'📖 Brinson归因：{_brinson_comment}</div>',
                unsafe_allow_html=True
            )

        # 风格漂移预警
        drift = model_results.get('drift_info', {})
        if drift.get('has_drift'):
            st.markdown(f'<div class="drift-alert">{drift["message"]}</div>', unsafe_allow_html=True)

        # 滚动仓位图
        rolling = model_results.get('rolling_df')
        if rolling is not None and not rolling.empty:
            with st.expander("📈 动态仓位监控图"):
                st.plotly_chart(plot_rolling_beta(rolling, stock_ratio), use_container_width=True)

    # 行业主题补充
    if 'sector' in model_results:
        sr = model_results['sector']
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(_kpi('中性化 Alpha', fmt_pct(sr.get('neutral_alpha'))), unsafe_allow_html=True)
        with c2:
            st.markdown(_kpi('跟踪误差', fmt_pct(sr.get('tracking_error'))), unsafe_allow_html=True)
        with c3:
            st.markdown(_kpi('信息比率', fmt_f(sr.get('info_ratio'))), unsafe_allow_html=True)

        # 指标解释
        st.markdown(
            _explain_row('中性化 Alpha') +
            _explain_row('跟踪误差') +
            _explain_row('信息比率'),
            unsafe_allow_html=True
        )

        _sec_interp = sr.get('interpretation', '')
        if _sec_interp:
            st.markdown(f'<div class="card" style="font-size:0.88rem;color:#444">{_sec_interp}</div>',
                        unsafe_allow_html=True)
        # 行业主题模型一句话
        st.markdown(
            '<div style="font-size:0.8rem;color:#666;padding:6px 10px;'
            'background:#f8f9fa;border-radius:6px;margin-top:4px">'
            '📖 中性化Alpha（"窝里横"测试）：在同一个行业里，看经理选的股票是不是比行业平均水平更能涨。'
            '如果行业涨了20%但基金只涨了5%，那就是"选股拖累"。</div>',
            unsafe_allow_html=True
        )

    # ---------- Part 3: 业绩可视化 ----------
    st.markdown('<div class="section-title">📈 第三部分：业绩可视化</div>', unsafe_allow_html=True)

    # 可视化点评
    _bm_str = '（蓝线为业绩基准）' if not bm_df.empty else ''
    _vis_comment = ''
    if _total_ret > 0:
        _vis_comment = f"该基金在{period_sel}区间累计收益 {_total_ret:+.1f}%，最大回撤 {_max_dd:.1f}%{_bm_str}。"
    else:
        _vis_comment = f"该基金在{period_sel}区间累计收益 {_total_ret:+.1f}%，表现弱于预期{_bm_str}。"
    st.markdown(f'<div style="font-size:0.85rem;color:#666;margin-bottom:8px">{_vis_comment}</div>',
                unsafe_allow_html=True)
    with st.expander("累计收益率对比图（基金 vs 业绩基准）", expanded=True):
        st.plotly_chart(plot_cumulative_return(nav_df, bm_df), use_container_width=True)
        st.markdown(
            f'<div style="font-size:0.75rem;color:#999;margin-top:-8px">'
            f'业绩基准：{bm_text}</div>',
            unsafe_allow_html=True
        )

    # ---------- Part 4: 大白话诊断 ----------
    st.markdown('<div class="section-title">💬 第四部分：大白话诊断</div>', unsafe_allow_html=True)

    score = translation.get('score', 60)
    score_color = '#27ae60' if score >= 75 else '#e67e22' if score >= 55 else '#e74c3c'
    score_label = '优秀' if score >= 80 else '良好' if score >= 65 else '一般' if score >= 50 else '谨慎'

    # 评分 + 总结
    col_score, col_summary = st.columns([1, 3])
    with col_score:
        st.markdown(f"""
<div style="text-align:center;background:white;border-radius:12px;
     padding:28px 20px;box-shadow:0 2px 10px rgba(0,0,0,.06);">
  <div style="font-size:3.2rem;font-weight:800;color:{score_color};">{score}</div>
  <div style="font-size:1rem;font-weight:600;color:{score_color};margin-top:4px">{score_label}</div>
  <div style="font-size:0.78rem;color:#aaa;margin-top:6px">综合评分（满分100）</div>
</div>
""", unsafe_allow_html=True)
    with col_summary:
        # 综合摘要
        _fund_type_zh = {'equity':'主动权益','bond':'纯债固收','mixed':'股债混合','sector':'行业主题'}.get(model_type,'')
        _fee_comment = ''
        if _explicit_total > 0.02:
            _fee_comment = f'费率较高（显性费率{_explicit_total*100:.2f}%/年），需要更强的超额收益才能覆盖成本。'
        elif _explicit_total > 0:
            _fee_comment = f'费率合理（显性费率{_explicit_total*100:.2f}%/年）。'
        _summary = (f"**{basic['name']}**是一只{_fund_type_zh}基金，"
                    f"在{period_sel}区间总收益{_total_ret:+.1f}%，年化约{_ann_ret:+.1f}%，最大回撤{_max_dd:.1f}%。"
                    f"{_fee_comment}")
        st.markdown(f'<div class="card card-info" style="margin-bottom:8px">'
                    f'<b>📝 综合摘要</b><br><span style="font-size:0.92rem">{_summary}</span></div>',
                    unsafe_allow_html=True)

    # 四维诊断（详细版）
    _diag_config = [
        ('🎭 性格诊断', 'character', 'card card-info',
         '描述这只基金的投资风格特征，是进攻型还是防守型，偏好大盘还是小盘，成长还是价值。'),
        ('🏆 实力诊断', 'skill', 'card card-good',
         '基于量化模型评估基金经理的真实能力：Alpha是否显著、是否在赚真本事的钱还是只是运气。'),
        ('⚠️ 风险提示', 'risk', 'card card-warn',
         '需要特别关注的风险点，包括仓位风险、风格漂移、费率陷阱等。'),
        ('💡 持有建议', 'advice', 'card',
         '结合以上分析，给出买入/持有/观望的具体建议。'),
    ]

    for label, key, card_class, hint in _diag_config:
        content = translation.get(key, '')
        # 如果内容较短，补充一些通用说明
        if not content:
            content = hint
        st.markdown(
            f'<div class="{card_class}" style="margin-bottom:10px">'
            f'<b>{label}</b>'
            f'<div style="margin-top:8px;font-size:0.9rem;line-height:1.8;color:#333">{content}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

    # ---------- 免责声明 ----------
    st.markdown("---")
    st.caption("⚠️ DeepInFund 基于公开数据和学术量化模型自动生成报告，仅供参考，不构成投资建议。"
               "因子数据使用指数代理，与学术标准FF因子可能存在偏差。投资有风险，入市需谨慎。")


if __name__ == "__main__":
    main()
