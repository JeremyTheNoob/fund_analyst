"""
基金穿透式诊断系统 v10.5 - 雷达图评分权重优化版
FOF研究员级别的深度量化分析框架

核心架构：
  数据层  → 基本信息 / 净值 / 因子 / 国债利率 / 持仓
  模型层  → 权益(FF三/五/Carhart) / 债券(T-Model久期) / 混合(Brinson) / 主题(中性化Alpha)
  网关层  → 根据股票仓位自动选择模型
  翻译层  → 专业术语 → 大白话 + 综合评分
  展示层  → Streamlit UI

v10.5 评分权重优化（2026-03-25）：
  - 偏股型(主动权益):  超额30% 风控15% 性价比20% 稳定15% 持续20%（选股是核心）
  - 纯债型(如000069):  超额15% 风控35% 性价比20% 稳定10% 持续20%（不能跌，风控最重要）
  - 量化/指数增强:      超额35% 风控10% 性价比25% 稳定20% 持续10%（超额是核心，策略一致性极重要）
  - 其他类型（qdii等）：等权重20%
  - 综合分改为加权平均，Part 0/Part 4/雷达图tooltip统一显示权重占比

作者：JeremyTheNoob
日期：2026-03-25
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
import time
from functools import wraps
import warnings
warnings.filterwarnings('ignore')


# ============================================================
# 🛡️ 全局重试装饰器：网络抖动自动重试（最多3次，间隔1s）
# ============================================================
def retry_on_failure(retries: int = 3, delay: float = 1.0):
    """
    自动重试装饰器。遇到任何异常自动重试，最后一次失败才向上抛出。
    用法：@retry_on_failure(retries=3, delay=2)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if i < retries - 1:
                        time.sleep(delay)
            # 全部重试失败，静默返回（由各函数内部返回空 DataFrame）
            return None
        return wrapper
    return decorator

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

# ── 全量列表缓存（全天有效，所有 symbol 共享，避免每次重新拉几千行） ──
@st.cache_data(ttl=86400, show_spinner=False)
def _get_fund_name_list() -> pd.DataFrame:
    """天天基金全量基金名称/类型列表，全天缓存"""
    try:
        return ak.fund_name_em()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=86400, show_spinner=False)
def _get_fund_scale_sina() -> pd.DataFrame:
    """新浪开放式基金规模全量表，全天缓存"""
    try:
        return ak.fund_scale_open_sina()
    except Exception:
        return pd.DataFrame()


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

    # 雪球接口（对ETF/次新基金可能返回 KeyError:'data'）
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

    # ── 雪球兜底1：fund_name_em（天天基金全量列表，ETF/次新基金均有）──────────────
    # 雪球接口对 ETF 和部分次新基金返回 KeyError:'data'，
    # fund_name_em 包含全部公募，可以至少补全基金名称和大类类型
    if r['name'] == symbol or not r['type_raw']:
        try:
            df_names = _get_fund_name_list()
            # 字段：基金代码 / 拼音缩写 / 基金简称 / 基金类型 / 拼音全称
            row = df_names[df_names['基金代码'] == symbol]
            if not row.empty:
                row = row.iloc[0]
                if r['name'] == symbol:
                    r['name']     = row.get('基金简称', symbol)
                if not r['type_raw']:
                    r['type_raw'] = row.get('基金类型', '')
        except Exception:
            pass

    # ── 雪球兜底2：ETF/指数型基金补充信息 ────────────────────────────────────
    # 优先用新浪开放式基金规模表：一次拉取可补全「规模+经理+成立日」三字段
    # fund_scale_open_sina 列：基金代码/基金简称/单位净值/总募集规模/最近总份额/成立日期/基金经理/更新日期
    _is_etf = 'ETF' in r.get('name', '') or 'ETF' in r.get('type_raw', '') or 'etf' in r.get('name', '').lower()
    _need_scale   = not r['scale']
    _need_manager = not r['manager']
    _need_estdate = not r['establish_date']

    if _need_scale or _need_manager or _need_estdate:
        try:
            _df_sina = _get_fund_scale_sina()
            if _df_sina is not None and not _df_sina.empty:
                # symbol 可能是 int 或 str，做宽松匹配
                _row_sina = _df_sina[_df_sina['基金代码'].astype(str).str.zfill(6) == str(symbol).zfill(6)]
                if not _row_sina.empty:
                    _r_sina = _row_sina.iloc[0]
                    # 规模：最近总份额(份) × 单位净值 → 转为 亿元
                    if _need_scale:
                        _shares = float(_r_sina.get('最近总份额', 0) or 0)
                        _nav_v  = float(_r_sina.get('单位净值', 1) or 1)
                        if _shares > 0:
                            _scale_yi = _shares * _nav_v / 1e8
                            if _scale_yi >= 100:
                                r['scale'] = f'{_scale_yi:.1f}亿元'
                            elif _scale_yi >= 1:
                                r['scale'] = f'{_scale_yi:.2f}亿元'
                            else:
                                r['scale'] = f'{_scale_yi*100:.1f}百万元'
                    # 基金经理
                    if _need_manager:
                        _mgr = str(_r_sina.get('基金经理', '') or '')
                        if _mgr and _mgr != 'nan':
                            r['manager'] = _mgr
                    # 成立日期
                    if _need_estdate:
                        _est = _r_sina.get('成立日期', None)
                        if _est is not None and str(_est) not in ('NaT', 'nan', ''):
                            try:
                                r['establish_date'] = pd.to_datetime(_est).strftime('%Y-%m-%d')
                            except Exception:
                                pass
        except Exception:
            pass

    # ETF兜底：若规模/成立日仍为空，从净值历史最早日期推断成立日
    if _is_etf and not r['establish_date']:
        try:
            _nav_hist = ak.fund_open_fund_info_em(symbol=symbol, indicator='单位净值走势')
            if not _nav_hist.empty and '净值日期' in _nav_hist.columns:
                _earliest = pd.to_datetime(_nav_hist['净值日期']).min()
                if not pd.isna(_earliest):
                    r['establish_date'] = _earliest.strftime('%Y-%m-%d')
        except Exception:
            pass
    if _is_etf and not r['manager']:
        r['manager'] = '被动跟踪（指数型）'

    # ── 兜底：从基金名称推断公司名 ──────────────────────────────────────────
    # 基金名称通常以公司简称打头（国泰、华夏、易方达等），雪球接口失败时从名称推断
    if not r['company'] and r['name'] and r['name'] != symbol:
        _known_companies = [
            '华夏', '易方达', '嘉实', '南方', '博时', '富国', '汇添富', '广发',
            '鹏华', '招商', '工银瑞信', '建信', '农银汇理', '交银施罗德',
            '中欧', '兴全', '景顺长城', '华安', '大成', '万家', '海富通',
            '国泰', '天弘', '华宝', '国联安', '诺安', '长城', '上投摩根',
            '东方', '平安', '银华', '光大保德信', '太平洋', '浦银安盛',
            '摩根士丹利', '泰康', '华泰柏瑞', '国寿安保', '中银', '前海开源',
            '创金合信', '永赢', '中泰', '信达澳亚', '财通', '西部利得',
            # 补充：中字头券商系基金公司
            '中金', '中信建投', '中信保诚', '中邮', '中融', '中海', '中航',
            # 补充：其他常见
            '汇泉', '睿远', '东证', '基石', '安信', '方正富邦', '长安',
            '德邦', '国投瑞银', '申万菱信', '民生加银', '金鹰', '上银',
        ]
        _fname = r['name']
        for _co in _known_companies:
            if _fname.startswith(_co):
                r['company'] = _co + '基金'
                break

    # ── 天天基金「运作费用」接口（最可靠的费率来源）──────────────────────────
    # fund_fee_em(indicator='运作费用') 返回：
    #   col0=管理费率  col1=1.20%(每年)  col2=托管费率  col3=0.20%(每年)  col4=销售服务费率  col5=---
    try:
        df_fee = ak.fund_fee_em(symbol=symbol, indicator='运作费用')
        if not df_fee.empty:
            row0 = df_fee.iloc[0]
            # 列序固定：0管理费率名,1管理费率值, 2托管费率名,3托管费率值, 4销售服务费率名,5销售服务费率值
            if not r['fee_manage'] and len(row0) > 1:
                r['fee_manage']  = _parse_fee(str(row0.iloc[1]))
            if not r['fee_custody'] and len(row0) > 3:
                r['fee_custody'] = _parse_fee(str(row0.iloc[3]))
            if not r['fee_sale'] and len(row0) > 5:
                r['fee_sale']    = _parse_fee(str(row0.iloc[5]))
    except Exception:
        pass

    # ── 兜底：雪球详细费用（含管理费+托管费）────────────────────────────────
    # fund_individual_detail_info_xq 返回「其他费用」行，含基金管理费/基金托管费
    if not r['fee_manage'] or not r['fee_custody']:
        try:
            df_xq2 = ak.fund_individual_detail_info_xq(symbol=symbol)
            if not df_xq2.empty and '费用类型' in df_xq2.columns:
                other = df_xq2[df_xq2['费用类型'] == '其他费用']
                for _, row in other.iterrows():
                    name_val = str(row.get('条件或名称', ''))
                    fee_val  = str(row.get('费用', ''))
                    if '管理' in name_val and not r['fee_manage']:
                        r['fee_manage']  = _parse_fee(fee_val + '%')  # 值已是百分数数字
                    if '托管' in name_val and not r['fee_custody']:
                        r['fee_custody'] = _parse_fee(fee_val + '%')
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
    name = info.get('name', '')
    # 货币基金：直接归类为 money，跳过量化分析
    if any(k in t for k in ['货币', '现金', '活期']): return 'money'
    # QDII 单独识别（跨市场基金，需要特殊分析逻辑）
    if 'QDII' in t or 'QDII' in name:           return 'qdii'
    # 指数/ETF 优先（避免"指数型-股票"被误识别为股票型）
    if any(k in t for k in ['指数', 'ETF', '联接']): return 'index'
    # 行业/主题基金
    if any(k in t for k in ['行业', '主题']):     return 'sector'
    # 按资产类别分
    if any(k in t for k in ['债券', '纯债']):     return 'bond'
    if any(k in t for k in ['混合', '配置', '平衡']): return 'mixed'
    if any(k in t for k in ['股票', '权益']):     return 'equity'
    # 代码本身含 ETF 关键词（fund_name_em 简称中有 ETF）
    if 'ETF' in name or 'etf' in name.lower():   return 'index'
    return 'equity'


# ---------- 2. 净值历史 ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_nav(symbol: str, years: int = 5,
              since_inception: bool = False,
              manager_start: str = '') -> pd.DataFrame:
    """
    获取历史净值，返回 date / nav / ret 列
    优先级：since_inception → manager_start → years

    ⚠️ 关键：强制使用「累计净值走势」而非「单位净值走势」
    原因：单位净值在基金分红/拆分时会产生跳空（如净值1.5→派息0.5→变1.0，
          pct_change 会误算为 -33.3%）。累计净值已将分红再投入计入，不会跳空。
    """
    # 内层函数加重试
    @retry_on_failure(retries=3, delay=2)
    def _fetch():
        return ak.fund_open_fund_info_em(symbol=symbol, indicator="累计净值走势")

    try:
        df = _fetch()
        if df is None or df.empty:
            return pd.DataFrame(columns=['date', 'nav', 'ret'])

        # 列结构：['净值日期', '累计净值']（取前两列，兼容接口字段变化）
        df = df.iloc[:, :2]
        df.columns = ['date', 'nav']
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['nav']  = pd.to_numeric(df['nav'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)

        # 异常值过滤：累计净值不应为负或为 0
        df = df[df['nav'] > 0]

        # 根据时间模式裁剪
        if since_inception:
            pass  # 不裁剪，取全部
        elif manager_start:
            try:
                cutoff = pd.to_datetime(manager_start)
                df = df[df['date'] >= cutoff]
            except Exception:
                start_dt = datetime.now() - timedelta(days=years * 365)
                df = df[df['date'] >= start_dt]
        else:
            start_dt = datetime.now() - timedelta(days=years * 365)
            df = df[df['date'] >= pd.to_datetime(start_dt)]

        if df.empty:
            return pd.DataFrame(columns=['date', 'nav', 'ret'])

        # 用累计净值计算真正的复权日收益率
        df['ret'] = df['nav'].pct_change()
        # 第一行 NaN 填 0（防止 merge 时丢掉第一天）
        df['ret'] = df['ret'].fillna(0)

        return df[['date', 'nav', 'ret']].reset_index(drop=True)

    except Exception:
        return pd.DataFrame(columns=['date', 'nav', 'ret'])


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

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_index_daily(symbol_code: str, start: str, end: str) -> pd.DataFrame:
    """通用指数日行情获取，返回 date / ret（公共因子数据，全天缓存 86400s）
    ⚠️ 横线防护：pct_change() 首行永远是 NaN，这里统一填 0，
       确保 cumprod 时基准不会因首行 NaN 导致整条线塌为 0。
    双接口策略：
       主力：stock_zh_index_daily（稳定，无 em 后缀依赖）
       备用：stock_zh_index_daily_em（部分时期可用）
    """
    def _build(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=['date', 'ret'])
        df = df[['date', 'close']].copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
        df['ret'] = df['close'].pct_change().fillna(0)
        return df[['date', 'ret']].reset_index(drop=True)

    # ── 主力接口：stock_zh_index_daily ──
    try:
        raw = ak.stock_zh_index_daily(symbol=symbol_code)
        result = _build(raw)
        if not result.empty:
            return result
    except Exception:
        pass

    # ── 备用接口：stock_zh_index_daily_em ──
    try:
        raw = ak.stock_zh_index_daily_em(symbol=symbol_code)
        result = _build(raw)
        if not result.empty:
            return result
    except Exception:
        pass

    return pd.DataFrame(columns=['date', 'ret'])


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_hk_index_daily(sina_symbol: str, start: str, end: str) -> pd.DataFrame:
    """港股指数日行情获取（恒生/国企/科技等），返回 date / ret（全天缓存）"""
    def _build(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=['date', 'ret'])
        df = df[['date', 'close']].copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
        df['ret'] = df['close'].pct_change().fillna(0)
        return df[['date', 'ret']].reset_index(drop=True)

    try:
        raw = ak.stock_hk_index_daily_sina(symbol=sina_symbol)
        return _build(raw)
    except Exception:
        return pd.DataFrame(columns=['date', 'ret'])


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_ff_factors(start: str, end: str) -> pd.DataFrame:
    """
    构建 FF 因子代理序列（方案 A+C，扩展 RMW）【全天缓存，公共因子全市场通用】
    因子列：date / Mkt / SMB / HML / Short_MOM / RMW（RMW失败时只有前4列）

    因子说明：
      SMB       = 中证1000 - 沪深300（小盘溢价）
      HML       = 国证价值 - 国证成长（价值溢价）
      Short_MOM = 21日滚动均值收益（前一日）。注意：这是"短期动量代理"，
                  非经典 Carhart 12月-1月 定义，命名加前缀以示区别。
      RMW       = 300质量成长 - 沪深300（盈利质量溢价）
                  若全部备用指数拉取失败 → 直接 drop 此列，降维至三/四因子，
                  绝不用 NaN 列污染整个 DataFrame（NaN 列会让 dropna 清零全表）
    """
    # ── 串行拉取 4 个基础指数（st.cache_data 函数不能在子线程调用） ──
    mkt   = fetch_index_daily('sh000300', start, end).rename(columns={'ret': 'Mkt'})
    small = fetch_index_daily('sh000852', start, end).rename(columns={'ret': 'ret_small'})
    val   = fetch_index_daily('sz399371', start, end).rename(columns={'ret': 'ret_val'})
    grw   = fetch_index_daily('sz399370', start, end).rename(columns={'ret': 'ret_grw'})
    large = mkt[['date']].copy().assign(ret_large=mkt['Mkt'])

    # ── 以沪深300(Mkt)日期为基准，其余指数 left join + ffill(3天) ──
    # 避免单个指数停牌/数据缺失导致整个因子矩阵被 inner join 截断
    df = mkt.copy()
    df = df.merge(small, on='date', how='left')
    df = df.merge(large, on='date', how='left', suffixes=('', '_dup'))
    df = df.merge(val,   on='date', how='left')
    df = df.merge(grw,   on='date', how='left')
    # 前向填充（节假日/停牌等短暂缺口，限3天）
    for _fc in ['ret_small', 'ret_large', 'ret_val', 'ret_grw']:
        if _fc in df.columns:
            df[_fc] = df[_fc].ffill(limit=3)

    df['SMB']       = df['ret_small'] - df['ret_large']
    df['HML']       = df['ret_val']   - df['ret_grw']
    # 短期动量代理（21日均线前置，防前视偏差）
    df['Short_MOM'] = df['Mkt'].rolling(21).mean().shift(1)

    # RMW：质量因子代理（依次尝试三个备用指数）
    rmw_ok = False
    for rmw_code in ('sh000803', 'sz399311', 'sh000919'):
        qual = fetch_index_daily(rmw_code, start, end).rename(columns={'ret': 'ret_qual'})
        if not qual.empty and len(qual) > 30:
            df = df.merge(qual, on='date', how='left')
            df['RMW'] = df['ret_qual'] - df['ret_large']
            rmw_ok = True
            break

    # ── 列级降维：NaN 比例 > 50% 的列直接 drop，绝不用 NaN 污染全表 ──
    # 这样 dropna 时只会按实际有效列清理，不会因为一列 NaN 清零整个 df
    base_cols = ['date', 'Mkt', 'SMB', 'HML', 'Short_MOM']
    if rmw_ok:
        # 检查 RMW 列的 NaN 比例
        if df['RMW'].isna().mean() > 0.5:
            df.drop(columns=['RMW'], inplace=True)
        else:
            base_cols.append('RMW')
    # 过滤掉 df 中不存在的列（防御性处理）
    cols = [c for c in base_cols if c in df.columns]
    return df[cols].reset_index(drop=True)


# ---------- 4. 国债收益率（用于久期回归） ----------

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_treasury_10y(start: str, end: str) -> pd.DataFrame:
    """
    获取 10 年期国债收益率日变动（Δy），单位：%
    返回 date / yield_pct / delta_y

    方案优先级（已调整）：
      ① bond_zh_us_rate     — 结构固定，列名稳定，升为主力方案
      ② bond_china_yield    — 曲线名称易变、节假日易返回空行，降为备用
      两个方案均加 ffill() 填充非交易日（周末/节假日），保证 delta_y 连续

    注：delta_y 仅用于债券久期回归，非交易日利率不变 → ffill 完全合理
    """
    def _clean(df_in: pd.DataFrame, date_col: str, val_col: str) -> pd.DataFrame:
        """统一清洗：类型转换 + 排序 + ffill + 截断 + diff"""
        df_in = df_in[[date_col, val_col]].copy()
        df_in.columns = ['date', 'yield_pct']
        df_in['date']      = pd.to_datetime(df_in['date'], errors='coerce')
        df_in['yield_pct'] = pd.to_numeric(df_in['yield_pct'], errors='coerce')
        df_in = df_in.dropna(subset=['date']).sort_values('date').reset_index(drop=True)
        # ffill 填补非交易日（周末/节假日利率沿用上一个工作日）
        df_in['yield_pct'] = df_in['yield_pct'].ffill()
        # 截断到请求区间
        df_in = df_in[(df_in['date'] >= pd.to_datetime(start)) &
                      (df_in['date'] <= pd.to_datetime(end))]
        df_in = df_in.dropna(subset=['yield_pct'])
        df_in['delta_y'] = df_in['yield_pct'].diff()
        return df_in[['date', 'yield_pct', 'delta_y']].dropna().reset_index(drop=True)

    # ── 方案 A（主力）：bond_zh_us_rate ──
    @retry_on_failure(retries=3, delay=2)
    def _try_zh_us():
        return ak.bond_zh_us_rate(start_date=start)

    try:
        df = _try_zh_us()
        col10 = '中国国债收益率10年'
        if df is not None and not df.empty and col10 in df.columns:
            result = _clean(df, '日期', col10)
            if len(result) >= 10:
                return result
    except Exception:
        pass

    # ── 方案 B（备用）：bond_china_yield ──
    @retry_on_failure(retries=3, delay=2)
    def _try_china_yield():
        return ak.bond_china_yield(start_date=start, end_date=end)

    try:
        df = _try_china_yield()
        if df is not None and not df.empty:
            mask = df['曲线名称'].str.contains('国债', na=False)
            df_gov = df[mask] if mask.sum() > 0 else df
            if not df_gov.empty and '10年' in df_gov.columns:
                result = _clean(df_gov, '日期', '10年')
                if len(result) >= 10:
                    return result
    except Exception:
        pass

    return pd.DataFrame(columns=['date', 'yield_pct', 'delta_y'])


# ---------- 4b. 债基三因子数据：期限结构 + 信用利差 ----------

# ---------- 4b-1. 债券持仓穿透分析 ----------

def _classify_bond(name: str, code: str) -> str:
    """从债券名称+代码推断债券类型"""
    if '转债' in name or '转2' in name or '转3' in name:
        return '可转债'
    if 'EB' in name and '债' not in name:
        return '可交换债'
    if '国开' in name or '国发' in name or '农发' in name or '进出口' in name:
        return '政策性金融债'
    if '国债' in name:
        return '国债'
    if 'MTN' in name or '中票' in name:
        return '中期票据'
    if 'CP' in name or 'SCP' in name or '短融' in name:
        return '短期融资券'
    if 'ABN' in name or 'ABS' in name or '资产支持' in name:
        return '资产支持证券'
    if 'CD' in name or '存单' in name:
        return '同业存单'
    if 'PPN' in name:
        return '非公开定向债'
    if '地方' in name or '省债' in name or '市债' in name:
        return '地方政府债'
    # 从代码前缀辅助判断
    code_str = str(code)
    if code_str.startswith('10') or code_str.startswith('019') or code_str.startswith('018'):
        return '利率债（交易所）'
    return '信用债'


def _bond_credit_tier(bond_type: str) -> str:
    """将债券类型映射到信用层级"""
    if bond_type in ('国债', '政策性金融债', '地方政府债'):
        return '利率债（零信用风险）'
    if bond_type in ('可转债', '可交换债'):
        return '含权债（股性+债性）'
    if bond_type in ('同业存单',):
        return '货币市场（极低风险）'
    if bond_type in ('短期融资券', '资产支持证券'):
        return '短端信用债'
    return '中长端信用债'


def analyze_bond_structure(bond_holdings_df: pd.DataFrame) -> dict:
    """
    对季报债券持仓明细做穿透分析，返回：
    - type_dist:    按债券类型分组统计（DataFrame，含 只数/占净值/占比）
    - tier_dist:    按信用层级分组（DataFrame）
    - conv_detail:  可转债详情（DataFrame，含评级/到期日/剩余年限）
    - total_weight: 债券持仓总占净值比例
    - quarter:      数据所属季度
    - wtd_duration_est: 加权估算剩余期限（年，启发式）
    - credit_ratio:  信用债占全部债券持仓比例（0-1）
    - convert_ratio: 可转债占全部债券持仓比例（0-1）
    - rate_ratio:    利率债占全部债券持仓比例（0-1）
    """
    if bond_holdings_df is None or bond_holdings_df.empty:
        return {}

    # 取最新季度
    df = bond_holdings_df.copy()
    latest_q = df['季度'].iloc[0]
    df = df[df['季度'] == latest_q].copy()

    # 标准化列名
    df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce').fillna(0)
    df['bond_type'] = df.apply(lambda r: _classify_bond(r['债券名称'], r['债券代码']), axis=1)
    df['credit_tier'] = df['bond_type'].apply(_bond_credit_tier)

    # 总占净值
    total_weight = df['占净值比例'].sum()
    if total_weight <= 0:
        return {}

    # 类型分组
    type_dist = df.groupby('bond_type').agg(
        只数=('债券名称', 'count'),
        占净值=('占净值比例', 'sum')
    ).sort_values('占净值', ascending=False).reset_index()
    type_dist['占比%'] = (type_dist['占净值'] / total_weight * 100).round(1)

    # 信用层级分组
    tier_dist = df.groupby('credit_tier').agg(
        只数=('债券名称', 'count'),
        占净值=('占净值比例', 'sum')
    ).sort_values('占净值', ascending=False).reset_index()
    tier_dist['占比%'] = (tier_dist['占净值'] / total_weight * 100).round(1)

    # 可转债详情（从 bond_zh_cov_info 批量查询评级和到期日）
    conv_rows = df[df['bond_type'].isin(['可转债', '可交换债'])].copy()
    conv_detail_list = []
    if not conv_rows.empty:
        current_year_f = datetime.now().year + datetime.now().month / 12.0
        for _, row in conv_rows.iterrows():
            code = str(row['债券代码']).zfill(6)
            info = {'债券代码': code, '债券名称': row['债券名称'],
                    '占净值比例': row['占净值比例'],
                    '评级': '—', '到期日': '—', '剩余年限': None,
                    '票面利率': '—'}
            try:
                cov_df = ak.bond_zh_cov_info(symbol=code, indicator='基本信息')
                if not cov_df.empty:
                    r = cov_df.iloc[0]
                    # 评级
                    info['评级'] = str(r.get('RATING', '—') or '—')
                    # 到期日
                    expire_raw = r.get('EXPIRE_DATE', None)
                    if expire_raw and str(expire_raw) not in ('None', 'NaT', ''):
                        expire_dt = pd.to_datetime(expire_raw, errors='coerce')
                        if pd.notna(expire_dt):
                            info['到期日'] = expire_dt.strftime('%Y-%m-%d')
                            remaining = expire_dt.year + expire_dt.month/12.0 - current_year_f
                            info['剩余年限'] = round(max(0, remaining), 2)
                    # 票面利率（最后一年）
                    coupon = r.get('COUPON_IR', None)
                    if coupon and str(coupon) not in ('None', ''):
                        info['票面利率'] = f"{coupon}%"
            except Exception:
                pass
            conv_detail_list.append(info)
        conv_detail = pd.DataFrame(conv_detail_list)
    else:
        conv_detail = pd.DataFrame()

    # 加权估算剩余期限（启发式，仅供参考）
    # 可转债用精确到期日；其他用类型经验值
    tenor_map = {
        '可转债': None,  # 从精确到期日算
        '可交换债': None,
        '国债': 7.0, '政策性金融债': 5.0, '地方政府债': 5.0,
        '中期票据': 3.0, '信用债': 3.0, '非公开定向债': 3.0,
        '短期融资券': 0.5, '同业存单': 0.5,
        '资产支持证券': 2.0, '利率债（交易所）': 5.0,
    }
    total_dur_contrib = 0.0
    for _, row in df.iterrows():
        btype = row['bond_type']
        wt = row['占净值比例']
        if btype in ('可转债', '可交换债') and not conv_detail.empty:
            match = conv_detail[conv_detail['债券名称'] == row['债券名称']]
            if not match.empty and pd.notna(match.iloc[0].get('剩余年限')):
                total_dur_contrib += wt * float(match.iloc[0]['剩余年限'])
                continue
        tenor = tenor_map.get(btype, 3.0)
        # 从名称提取发行年份修正
        ym = re.match(r'^(\d{2})', str(row['债券名称']))
        if ym:
            issue_yr = int('20' + ym.group(1))
            expire_yr = issue_yr + tenor
            current_yr_f = datetime.now().year + datetime.now().month / 12.0
            remaining = max(0.1, expire_yr - current_yr_f)
            total_dur_contrib += wt * remaining
        else:
            total_dur_contrib += wt * tenor
    wtd_dur_est = round(total_dur_contrib / total_weight, 2) if total_weight > 0 else 0.0

    # 各类别占比
    rate_types = {'国债', '政策性金融债', '地方政府债', '利率债（交易所）'}
    credit_types = {'中期票据', '信用债', '非公开定向债', '短期融资券', '资产支持证券', '同业存单'}
    convert_types = {'可转债', '可交换债'}

    rate_w  = df[df['bond_type'].isin(rate_types)]['占净值比例'].sum()
    credit_w= df[df['bond_type'].isin(credit_types)]['占净值比例'].sum()
    conv_w  = df[df['bond_type'].isin(convert_types)]['占净值比例'].sum()

    return {
        'type_dist':      type_dist,
        'tier_dist':      tier_dist,
        'conv_detail':    conv_detail,
        'total_weight':   round(total_weight, 2),
        'quarter':        latest_q,
        'wtd_duration_est': wtd_dur_est,
        'rate_ratio':     rate_w / total_weight if total_weight > 0 else 0,
        'credit_ratio':   credit_w / total_weight if total_weight > 0 else 0,
        'convert_ratio':  conv_w / total_weight if total_weight > 0 else 0,
        'n_bonds':        len(df),
        'raw_df':         df,
    }


# ---------- 4b. 债基三因子数据：期限结构 + 信用利差 ----------

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_bond_three_factors(start: str, end: str) -> pd.DataFrame:
    """
    获取债基三因子日度变动序列，用于三因子回归：
        R_p = α + β_short·ΔY_1Y + β_long·ΔY_10Y + β_credit·ΔCS + ε

    因子来源（双轨策略）：
      短端 (ΔY_2Y)  ← bond_zh_us_rate「中国国债收益率2年」（最稳定，每日更新）
      长端 (ΔY_10Y) ← bond_zh_us_rate「中国国债收益率10年」（同上）
      信用利差 (ΔCS) ← bond_china_yield 中短期票据(AAA) 3年 - 国债 3年
                       （按自然年分段拉取，拼合后差分）

    返回列：date / delta_y2 / delta_y10 / delta_spread
    （均为日变动量，单位 %，即百分比变动，与 fetch_treasury_10y 保持一致）
    """
    # ── 步骤1：获取期限结构（2年 + 10年）──
    term_df = pd.DataFrame()
    try:
        raw = ak.bond_zh_us_rate(start_date=start)
        if raw is not None and not raw.empty:
            raw['日期'] = pd.to_datetime(raw['日期'], errors='coerce')
            raw = raw.dropna(subset=['日期']).sort_values('日期')
            raw = raw[(raw['日期'] >= pd.to_datetime(start)) &
                      (raw['日期'] <= pd.to_datetime(end))]
            for col in ['中国国债收益率2年', '中国国债收益率10年']:
                raw[col] = pd.to_numeric(raw[col], errors='coerce').ffill()
            term_df = raw[['日期', '中国国债收益率2年', '中国国债收益率10年']].copy()
            term_df.columns = ['date', 'y2', 'y10']
    except Exception:
        pass

    if term_df.empty:
        return pd.DataFrame(columns=['date', 'delta_y2', 'delta_y10', 'delta_spread'])

    # ── 步骤2：按年分段获取信用利差（并行）──
    # bond_china_yield 必须按整年请求（跨年数据异常），分段后拼合
    start_yr = pd.to_datetime(start).year
    end_yr   = pd.to_datetime(end).year

    def _fetch_year(yr: int):
        s = f"{yr}0101"
        e = f"{yr}1231"
        try:
            df_yr = ak.bond_china_yield(start_date=s, end_date=e)
            if df_yr is None or df_yr.empty:
                return None, None
            gov_yr = df_yr[df_yr['曲线名称'] == '中债国债收益率曲线'][['日期', '3年']].copy()
            gov_yr.columns = ['date', 'g_3y']
            cre_yr = df_yr[df_yr['曲线名称'] == '中债中短期票据收益率曲线(AAA)'][['日期', '3年']].copy()
            cre_yr.columns = ['date', 'c_3y']
            return gov_yr, cre_yr
        except Exception:
            return None, None

    gov_frames = []
    cre_frames = []
    years = list(range(start_yr, end_yr + 1))
    from concurrent.futures import ThreadPoolExecutor as _TPE2
    with _TPE2(max_workers=min(len(years), 4)) as _ex3:
        _yr_futures = {_ex3.submit(_fetch_year, yr): yr for yr in years}
        from concurrent.futures import as_completed as _ac
        for _fut in _ac(_yr_futures):
            gov_yr, cre_yr = _fut.result()
            if gov_yr is not None:
                gov_frames.append(gov_yr)
            if cre_yr is not None:
                cre_frames.append(cre_yr)

    # 组装信用利差序列
    spread_df = pd.DataFrame()
    if gov_frames and cre_frames:
        gov_all = pd.concat(gov_frames).drop_duplicates('date')
        cre_all = pd.concat(cre_frames).drop_duplicates('date')
        gov_all['date'] = pd.to_datetime(gov_all['date'])
        cre_all['date'] = pd.to_datetime(cre_all['date'])
        gov_all['g_3y'] = pd.to_numeric(gov_all['g_3y'], errors='coerce')
        cre_all['c_3y'] = pd.to_numeric(cre_all['c_3y'], errors='coerce')
        merged = gov_all.merge(cre_all, on='date', how='inner')
        merged = merged.sort_values('date')
        merged['spread'] = merged['c_3y'] - merged['g_3y']  # 信用利差（%）
        spread_df = merged[['date', 'spread']].copy()

    # ── 步骤3：合并 + 计算日变动量 ──
    term_df['date'] = pd.to_datetime(term_df['date'])
    term_df = term_df.sort_values('date').reset_index(drop=True)

    if not spread_df.empty:
        result = term_df.merge(spread_df, on='date', how='left')
        result['spread'] = result['spread'].ffill(limit=5)  # 信用利差数据更稀疏，填3→5天
    else:
        result = term_df.copy()
        result['spread'] = np.nan  # 降级：无信用利差数据

    # 差分（日变动量）
    result['delta_y2']     = result['y2'].diff()
    result['delta_y10']    = result['y10'].diff()
    result['delta_spread'] = result['spread'].diff() if 'spread' in result.columns else np.nan

    result = result.dropna(subset=['delta_y2', 'delta_y10'])
    return result[['date', 'delta_y2', 'delta_y10', 'delta_spread']].reset_index(drop=True)


# ---------- 5. 中债综合指数（债券基准） ----------

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_bond_index(start: str, end: str) -> pd.DataFrame:
    """
    获取中债综合财富指数（indicator='财富'），返回 date / ret【全天缓存】
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
        df['ret'] = df['close'].pct_change().fillna(0)  # 首行NaN→0，与fetch_index_daily保持一致
        return df[['date','ret']].reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=['date','ret'])


# ---------- 6. 基准构建（招募说明书解析 + 动态再平衡） ----------

_INDEX_NAME_CODE = {
    # A股宽基
    '沪深300':      'sh000300',
    '中证500':      'sh000905',
    '中证800':      'sh000906',
    '中证1000':     'sh000852',
    '中证全指':     'sh000985',
    '上证50':       'sh000016',
    '上证180':      'sh000010',
    '上证综合':     'sh000001',
    '上证综指':     'sh000001',
    '创业板':       'sz399006',
    '创业板指':     'sz399006',
    '科创50':       'sh000688',
    '深证成指':     'sz399001',
    '深证综指':     'sz399106',
    # 主动基金指数
    '偏股混合型基金': 'sh004851',   # 中证偏股混合型基金指数
    '偏债混合型基金': 'sh930950',
    '股票型基金':   'sh930890',
    # 债券/货币
    '中债综合':     None,    # 单独用 fetch_bond_index 处理
    '中债全价':     None,
    '中债财富':     None,
    '中债新综合':   None,
    # 活期/定期存款（视为0收益）
    '银行活期':     None,
    '活期存款':     None,
    '定期存款':     None,
    '一年定期':     None,
    # 港股/海外（用 hk: 前缀标识，走 fetch_hk_index_daily 接口）
    '恒生':         'hk:HSI',
    '恒生指数':     'hk:HSI',
    '恒生科技':     'hk:HSTECH',
    '恒生国企':     'hk:HSCEI',
}

def _parse_benchmark(text: str) -> dict:
    """
    解析招募说明书基准文本（v9.6：扩展正则，兼容更多格式）

    支持写法（均已实测）：
      "沪深300指数收益率×80%+中债综合财富指数收益率×20%"
      "沪深300指数*80%+中证500指数*20%"
      "业绩比较基准为沪深300指数收益率的80%加上中债综合财富指数收益率的20%"
      "沪深300指数80%+中债综合财富指数20%"（无乘号，直接跟百分比）

    返回 {type, components:[{name,code,weight}]}
    """
    if not text:
        return {}

    # ── 策略：先提取所有"权重百分比"，再在其上下文找已知指数名称 ──
    # 1. 从文本中提取所有权重值（如 80%, 20%, 60%, 40%）
    pct_pattern = r'(\d+\.?\d*)\s*[%％]'
    weights_raw = [float(m) / 100.0 for m in re.findall(pct_pattern, text)]
    # 过滤掉不合理的权重（0和>1）
    weights = [w for w in weights_raw if 0 < w <= 1]

    if not weights:
        return {}

    # 2. 对每个已知指数名称，检测是否在文本中出现
    # 按名称长度降序（长的优先匹配，防止"中证1000"被"中证"先截走）
    sorted_keys = sorted(_INDEX_NAME_CODE.keys(), key=len, reverse=True)

    found = []   # (pos_in_text, name, code)
    covered_ranges = []
    for k in sorted_keys:
        idx = text.find(k)
        while idx != -1:
            # 检查是否已被更长的匹配覆盖
            overlap = any(s <= idx < e for s, e in covered_ranges)
            if not overlap:
                found.append((idx, k, _INDEX_NAME_CODE[k]))
                covered_ranges.append((idx, idx + len(k)))
            idx = text.find(k, idx + 1)

    # 按在文本中出现的位置排序（保持阅读顺序）
    found.sort(key=lambda x: x[0])

    if not found:
        return {}

    # 3. 如果找到的指数数量与权重数量一致，直接配对
    components = []
    if len(found) == len(weights):
        for (pos, name, code), w in zip(found, weights):
            components.append({'name': name, 'code': code, 'weight': w})
    elif len(found) == 1 and len(weights) >= 1:
        # 只有一个指数：取第一个有效权重
        pos, name, code = found[0]
        components.append({'name': name, 'code': code, 'weight': weights[0]})
    else:
        # 数量不匹配时，给每个指数分配距离最近的权重
        # 简单策略：按顺序依次分配，多余的权重忽略
        for i, (pos, name, code) in enumerate(found):
            if i < len(weights):
                components.append({'name': name, 'code': code, 'weight': weights[i]})

    if not components:
        return {}

    # 权重归一化
    total_weight = sum(c['weight'] for c in components)
    if abs(total_weight - 1.0) > 0.05 and total_weight > 0:
        for c in components:
            c['weight'] = round(c['weight'] / total_weight, 4)

    bm_type = 'single' if len(components) == 1 else 'custom'
    return {'type': bm_type, 'components': components}


@st.cache_data(ttl=3600, show_spinner=False)
def build_benchmark_ret(parsed: dict, start: str, end: str) -> pd.DataFrame:
    """
    构建基准每日收益率（先算各成分日收益率，再加权）
    返回 date / bm_ret

    ⚠️ 基准横线防护（v9.4）：
      - 每个成分 fetch_index_daily 返回的第一行已是 dropna() 后的数据（pct_change丢弃第一行）
      - 加权合并后仍需确保首行不为 NaN，否则 cumprod 时 (1+NaN) 会污染全序列
      - 最终 bm_ret 首行强制填 0（代表起点收益率为0，不影响累计收益从1出发）
    """
    if not parsed or 'components' not in parsed:
        # 默认沪深300
        df = fetch_index_daily('sh000300', start, end).rename(columns={'ret':'bm_ret'})
        if not df.empty:
            df['bm_ret'] = df['bm_ret'].fillna(0)  # 防横线：首行NaN→0
        return df

    parts = []
    for comp in parsed['components']:
        w = comp['weight']
        code = comp['code']

        if code is None and '中债' in comp.get('name',''):
            df_part = fetch_bond_index(start, end).rename(columns={'ret':'part_ret'})
        elif code is not None and code.startswith('hk:'):
            # 港股指数（恒生/国企/科技等）——用新浪港股接口
            hk_symbol = code[3:]  # 去掉 'hk:' 前缀，如 'HSI'
            df_part = fetch_hk_index_daily(hk_symbol, start, end).rename(columns={'ret':'part_ret'})
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
        if not df.empty:
            df['bm_ret'] = df['bm_ret'].fillna(0)
        return df

    merged = parts[0].rename(columns={'weighted':'bm_ret'})
    for p in parts[1:]:
        # 用 inner join，只保留所有成分都有数据的交易日，防止停牌日 0 收益污染基准
        merged = merged.merge(p, on='date', how='inner')
        merged['bm_ret'] = merged['bm_ret'] + merged['weighted']
        merged.drop(columns=['weighted'], inplace=True)

    result = merged[['date','bm_ret']].dropna().reset_index(drop=True)

    # 横线防护：确保基准收益率序列首行不是 NaN（pct_change第一行是NaN）
    # 填充0表示起点收益为0，cumprod() 从1.0出发，不会变成横线
    if not result.empty:
        result['bm_ret'] = result['bm_ret'].fillna(0)

    return result


# ---------- 7. 申万一级行业指数 ----------

# 申万一级行业代码表（31个行业）
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
    '银行': '801780', '金融': '801790', '证券': '801790',
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


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_sw_industry_ret(sw_code: str, start: str, end: str) -> pd.Series:
    """
    获取申万一级行业指数日收益率【全天缓存，公共行业数据】
    sw_code: 如 '801150'（医药生物）
    返回 index=date, value=日收益率 Series
    """
    try:
        df = ak.index_hist_sw(symbol=sw_code, period='day')
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        df = df[(df['日期'] >= pd.Timestamp(start)) & (df['日期'] <= pd.Timestamp(end))].copy()
        if df.empty or len(df) < 5:
            return pd.Series(dtype=float)
        close_col = '收盘' if '收盘' in df.columns else df.columns[4]  # 备用：取第5列
        df['ret'] = df[close_col].pct_change()
        df = df.dropna(subset=['ret'])
        return df.set_index('日期')['ret']
    except Exception:
        return pd.Series(dtype=float)


def detect_sw_industry(fund_name: str, sector_weights: dict) -> tuple[str, str]:
    """
    从基金名称或持仓行业权重推断主要申万行业，返回 (sw_code, industry_name)
    优先级：
      1. 持仓行业权重（前两大行业占比>50%，取第一大）
      2. 基金名称关键词匹配
      3. 无法识别 → ('', '')
    """
    # 策略1：持仓行业权重（如果有季报数据）
    if sector_weights:
        # sector_weights 形如 {'电子': 0.45, '医药': 0.20, ...}
        top_sector = max(sector_weights, key=sector_weights.get)
        top_ratio  = sector_weights[top_sector]
        if top_ratio > 0.30:  # 第一大行业超过30%，认定为主行业
            for kw, code in FUND_NAME_TO_SW.items():
                if kw in top_sector:
                    return code, top_sector
            # 直接查申万表
            for iname, code in SW_INDUSTRY_MAP.items():
                if iname in top_sector or top_sector in iname:
                    return code, iname

    # 策略2：基金名称关键词
    for kw, code in FUND_NAME_TO_SW.items():
        if kw in fund_name:
            iname = next((k for k, v in SW_INDUSTRY_MAP.items() if v == code), kw)
            return code, iname

    return '', ''


# ---------- 8. 持仓数据（季报） ----------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_holdings(symbol: str, type_category: str = 'equity') -> dict:
    """
    获取最新季报持仓，四层兜底策略：

    层级0（最准）：fund_portfolio_asset_allocation_em — 历代季度资产配置表（股/债/现金占净值比）
    层级1（准确）：fund_open_fund_info_em indicator="资产配置" — 最新资产配置百分比
    层级2（推算）：fund_portfolio_hold_em — 前十大股票持仓（已弃用粗暴1.4系数，
                   改为按前十大集中度自适应推算）
    层级3（兜底）：按基金注册类型设定行业经验默认值

    返回：{stock_ratio, bond_ratio, cash_ratio, top10, sector_weights,
           report_date, alloc_source}
    """
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
        'alloc_source': 'default（类型默认值）'
    }

    # ── 层级0：季度资产配置历史表（最权威）──
    alloc_ok = False
    try:
        @retry_on_failure(retries=3, delay=2)
        def _fetch_alloc_hist():
            return ak.fund_portfolio_asset_allocation_em(symbol=symbol)

        df_hist = _fetch_alloc_hist()
        if df_hist is not None and not df_hist.empty:
            # 该接口返回：报告期 / 股票占净值比 / 债券占净值比 / 现金占净值比 / 其他
            # 取最新一期（第一行，接口按报告期倒序）
            row = df_hist.iloc[0]
            # 字段名称可能略有差异，做宽松匹配
            _col_map = {}
            for col in df_hist.columns:
                if '股票' in str(col):
                    _col_map['stock'] = col
                elif '债券' in str(col):
                    _col_map['bond'] = col
                elif '现金' in str(col):
                    _col_map['cash'] = col
                elif '报告' in str(col) or '日期' in str(col):
                    _col_map['date'] = col

            sr = _parse_pct(str(row.get(_col_map.get('stock', ''), '')))
            br = _parse_pct(str(row.get(_col_map.get('bond', ''), '')))
            cr = _parse_pct(str(row.get(_col_map.get('cash', ''), '')))
            rd = str(row.get(_col_map.get('date', ''), ''))

            total = sr + br + cr
            if total > 0.3:
                # 接口已经是占净值比，不需要归一化；但如果总和明显超过100%做修正
                if total > 1.5:
                    sr, br, cr = sr / total, br / total, cr / total
                result.update({
                    'stock_ratio': sr, 'bond_ratio': br, 'cash_ratio': cr,
                    'report_date': rd,
                    'alloc_source': f'季度配置历史表（{rd}期）'
                })
                alloc_ok = True
    except Exception:
        pass

    # ── 层级1：资产配置接口（点查最新）──
    if not alloc_ok:
        try:
            @retry_on_failure(retries=3, delay=2)
            def _fetch_alloc():
                return ak.fund_open_fund_info_em(symbol=symbol, indicator="资产配置")

            df2 = _fetch_alloc()
            if df2 is not None and not df2.empty:
                alloc = dict(zip(df2.iloc[:, 0].astype(str), df2.iloc[:, 1].astype(str)))
                sr = _parse_pct(alloc.get('股票', '') or alloc.get('股票仓位', ''))
                br = _parse_pct(alloc.get('债券', '') or alloc.get('债券仓位', ''))
                cr = _parse_pct(alloc.get('现金', '') or alloc.get('现金及其他', ''))
                total = sr + br + cr
                if total > 0.3:
                    if total > 0.01:
                        sr, br, cr = sr / total, br / total, cr / total
                    result.update({
                        'stock_ratio': sr, 'bond_ratio': br, 'cash_ratio': cr,
                        'alloc_source': '资产配置接口（最新期）'
                    })
                    alloc_ok = True
        except Exception:
            pass

    # ── 层级2：前十大持仓（弃用1.4系数，改用自适应推算）──
    try:
        _current_year = datetime.now().year

        @retry_on_failure(retries=2, delay=2)
        def _fetch_hold(yr: str):
            return ak.fund_portfolio_hold_em(symbol=symbol, date=yr)

        df = None
        for _year in [str(_current_year - 1), str(_current_year), str(_current_year - 2)]:
            try:
                _tmp = _fetch_hold(_year)
                if _tmp is not None and not _tmp.empty:
                    df = _tmp
                    break
            except Exception:
                continue

        if df is not None and not df.empty:
            result['top10'] = df.head(10)

            # 仅在上层未获取到可信仓位时，才用前十大推算
            if not alloc_ok and '占净值比例' in df.columns:
                top10_pct = pd.to_numeric(df['占净值比例'], errors='coerce').head(10).sum()
                # 自适应系数：不再固定1.4，而是按前十大总占比判断集中度
                # 前十大 > 60%：高集中（指数型/集中型），系数约1.1；
                # 前十大 30-60%：均衡型，系数约1.6；
                # 前十大 < 30%：高分散，系数约2.2
                if top10_pct >= 60:
                    corr_factor, desc = 1.1, '高集中度修正×1.1'
                elif top10_pct >= 30:
                    corr_factor, desc = 1.6, '均衡型修正×1.6'
                else:
                    corr_factor, desc = 2.2, '高分散度修正×2.2'

                est_stock = min(top10_pct / 100 * corr_factor, 0.95)
                if est_stock > 0.01:
                    result['stock_ratio'] = est_stock
                    result['alloc_source'] = f'前十大持仓推算（{desc}，前10占{top10_pct:.1f}%）'
    except Exception:
        pass

    # ── 债券持仓明细（债基/混合基专用）──
    # 拉取 fund_portfolio_bond_hold_em，用于债券穿透分析
    if type_category in ('bond', 'mixed'):
        try:
            _current_year = datetime.now().year
            _bond_hold_df = None
            for _yr in [str(_current_year - 1), str(_current_year), str(_current_year - 2)]:
                try:
                    _tmp = ak.fund_portfolio_bond_hold_em(symbol=symbol, date=_yr)
                    if _tmp is not None and not _tmp.empty:
                        _bond_hold_df = _tmp
                        break
                except Exception:
                    continue
            if _bond_hold_df is not None and not _bond_hold_df.empty:
                result['bond_holdings'] = _bond_hold_df
            else:
                result['bond_holdings'] = pd.DataFrame()
        except Exception:
            result['bond_holdings'] = pd.DataFrame()
    else:
        result['bond_holdings'] = pd.DataFrame()

    return result


def _parse_pct(text: str) -> float:
    """解析百分比字符串为小数（必须含 % 符号，防止将日期/代码误解析）"""
    if not text:
        return 0.0
    m = re.search(r'(\d+\.?\d*)\s*[%％]', str(text))
    if m:
        v = float(m.group(1)) / 100
        return v if v <= 2.0 else 0.0  # 超过200%视为解析错误
    return 0.0


# ---------- 9. 隐性费率估算 ----------

@st.cache_data(ttl=3600, show_spinner=False)
def estimate_hidden_cost(symbol: str, nav_df: pd.DataFrame,
                         type_category: str = 'equity') -> dict:
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

    # -- 方案B（备用兜底）：行业经验中位数 --
    # ⚠️ 原"负自相关估算换手率"方案已废弃：
    #    基金净值是一揽子股票的平滑，负自相关不反映调仓频率（Roll模型适用于个股买卖价差）。
    #    改为直接使用行业经验固定值：主动股票基金约 0.10-0.20%，指数/债券基金约 0.03-0.05%。
    if result['hidden_cost_rate'] is None:
        _type_default = {
            'equity': 0.0015,   # 主动权益：约0.15%（中等换手率估计）
            'mixed':  0.0012,   # 混合型：约0.12%
            'bond':   0.0003,   # 债券型：约0.03%（低换手）
            'sector': 0.0010,   # 行业/主题：约0.10%
            'index':  0.0003,   # 指数型：约0.03%（被动低换手）
        }
        _tc = _type_default.get(
            type_category, 0.0015  # 传入类型不在映射表时用0.15%通用中位数
        )
        result['hidden_cost_rate'] = _tc
        result['method'] = f'行业经验中位数（{_tc*100:.2f}%/年，无利润表数据时使用）'

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

    # L0: QDII跨市场基金 → 走equity模型，但标注"A股因子仅供参考"
    if type_category == 'qdii':
        warn = ('🌐 **QDII跨市场基金**：该基金主要投资于境外市场，当前使用A股FF因子模型。'
                '由于持仓为港股/美股/其他境外资产，Alpha/Beta结果仅供趋势参考，不能直接与A股基金比较。'
                '建议重点关注超额收益（vs MSCI基准）和夏普比率，而非因子敞口。')
        return 'equity', warn

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

    ⚠️ Alpha 量纲修复（v9.4）：
      - X 标准化后截距 ≠ 真正的 Alpha（标准化仅针对 X，不改 Y）
      - Alpha 必须从**原始回归（m_raw）**的截距取，单位才是"日收益率"
      - 再用复利公式年化：(1 + alpha_daily_raw)^252 - 1
      - 标准化回归只用来取 R²（两个回归的 R² 相同）和标准化 Beta
    """
    if len(df) < 60:
        return None

    y = df['fund_ret'].values
    X_raw = df[use_cols].values

    # --- 回归1：原始（未标准化）→ Alpha 必须从这里取，量纲才对 ---
    X_raw_const = sm.add_constant(X_raw)
    try:
        m_raw = sm.OLS(y, X_raw_const).fit()
    except Exception:
        return None

    # Alpha：从原始回归截距取（单位=日收益率，如 0.0004 表示日均+0.04%）
    alpha_daily  = float(m_raw.params[0])
    # 复利公式年化 Alpha
    alpha_annual = (1 + alpha_daily) ** 252 - 1
    alpha_pval   = float(m_raw.pvalues[0])
    r2           = float(m_raw.rsquared)

    factor_betas_raw = {col: float(m_raw.params[i+1])
                        for i, col in enumerate(use_cols)}

    # --- 回归2：Z-Score标准化 → 仅用于因子暴露横向对比（标准化Beta）---
    scaler = StandardScaler()
    X_scaled = sm.add_constant(scaler.fit_transform(X_raw))
    try:
        m_scaled = sm.OLS(y, X_scaled).fit()
        factor_betas_std = {col: float(m_scaled.params[i+1])
                            for i, col in enumerate(use_cols)}
    except Exception:
        # 标准化回归失败时降级用原始 Beta 归一化
        factor_betas_std = factor_betas_raw

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
    # ---------- 数据对齐（v9.4 升级：left join + ffill，防因子缺失导致样本量骤降）----------
    df_base = pd.DataFrame({'fund_ret': fund_ret}).reset_index()
    df_base.columns = ['date', 'fund_ret']
    df_base['date'] = pd.to_datetime(df_base['date'])

    # 因子 date 列规范化
    factors_aligned = factors.copy()
    factors_aligned['date'] = pd.to_datetime(factors_aligned['date'])

    # 以基金净值日期为准做 left join（防止因子缺几天导致 inner join 丢失大量数据）
    df_base = df_base.merge(factors_aligned, on='date', how='left')

    # 选择因子列（RMW 已在 fetch_ff_factors 中做列级降维，不存在时自动缺席）
    _factor_map = {
        'capm':    ['Mkt'],
        'ff3':     ['Mkt', 'SMB', 'HML'],
        'ff5':     ['Mkt', 'SMB', 'HML', 'RMW'],
        'carhart': ['Mkt', 'SMB', 'HML', 'Short_MOM'],  # 短期动量代理
    }
    desired_cols = _factor_map.get(model_type, ['Mkt', 'SMB', 'HML'])
    # 过滤：只保留在 df 中存在且非全 NaN 的列
    use_cols = [c for c in desired_cols
                if c in df_base.columns and df_base[c].notna().sum() > 30]
    if not use_cols:
        return _empty_ff_result('因子列缺失，无法回归')

    # 前向填充因子缺口（节假日/停牌等导致的短暂缺失），限制3天防过度失真
    df_base[use_cols] = df_base[use_cols].ffill(limit=3)

    df_full = df_base[['date', 'fund_ret'] + use_cols].dropna()
    if len(df_full) < 60:
        # 日志：方便排查"明明有数据却不满60天"的问题
        n_total = len(df_base)
        n_valid = len(df_full)
        return _empty_ff_result(
            f'数据不足(<60天)，无法回归'
            f'（原始净值行数={n_total}，对齐后有效行数={n_valid}，'
            f'可能原因：因子数据时间范围覆盖不足）'
        )

    # ---------- 全期回归 ----------
    res_full = _run_single_ff(df_full, use_cols)
    if res_full is None:
        return _empty_ff_result('全期回归失败')

    # ── 先把全期结果取出来，后面所有逻辑都用这些变量 ──
    alpha_annual = res_full['alpha']
    alpha_pval   = res_full['alpha_pval']
    r2           = res_full['r_squared']

    # ---------- 近126天（半年）回归，检测 Beta 漂移 + R² 突降 ----------
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

    # ── R² 突降检测（r2 已在上方赋值，无歧义）──
    r2_recent = res_recent['r_squared'] if res_recent else None
    residual_insight = ''
    if r2_recent is not None:
        r2_drop = r2 - r2_recent   # 全期 - 近期，正值表示近期R²下降
        if r2_drop > 0.25:
            residual_insight = (
                f'⚠️ 模型解释力近期明显下滑（全期R²={r2:.2f} → 近半年R²={r2_recent:.2f}）：'
                f'常规因子已无法解释该基金近期的大部分波动，'
                f'疑有非标资产收益（定增、大宗交易、原始股）或极端个股偏离，建议关注季报持仓变化。'
            )
        elif r2 < 0.4:
            residual_insight = (
                f'🔵 全期R²={r2:.2f}，该基金与市场相关性极低，风格高度独立，'
                f'可能持有大量另类资产或采用对冲策略，常规因子分析仅供参考。'
            )
        elif r2 > 0.85:
            residual_insight = (
                f'📎 全期R²={r2:.2f}，高度贴合市场因子，'
                f'主动管理价值需结合Alpha显著性综合判断。'
            )

    # ---------- 生成解读文本 ----------
    # （alpha_annual / alpha_pval / r2 已在上方赋值）

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
        'r_squared_recent': r2_recent,          # 新增：近半年R²，用于突降检测
        'residual_insight': residual_insight,   # 新增：残差分析提示文案
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
        'r_squared_recent': None,          # 展示层会读这个字段，必须存在
        'residual_insight': '',            # 展示层会读这个字段，必须存在
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
                       bond_ratio: float = 1.0,
                       three_factor_df: pd.DataFrame = None,
                       holdings: dict = None) -> dict:
    """
    债基三因子归因模型（v10.0 升级）

    回归方程（三因子）：
        R_p = α + β_short·(-ΔY_2Y) + β_long·(-ΔY_10Y) + β_credit·ΔCS + ε
        其中：
          β_short  = 短端敏感度（对应短端久期，短债/存单为主基金此项显著）
          β_long   = 长端敏感度（对应长端久期，利率债基金此项显著）
          β_credit = 信用利差敏感度（负值=信用下沉；利差扩大净值跌）
          α        = 综合carry（票息+骑乘+杠杆溢价，扣除三因子后的截距）

    降级策略：
      如果 three_factor_df 为空（接口异常），自动降级为单因子T-Model（保持向后兼容）

    凸性静态穿透法（回归凸性不显著时）：
      根据持仓结构判断凸性质量：
        国债+政策性金融债 > 50% → 凸性强（正凸性保护好）
        信用债+存单 > 50%       → 凸性弱（几乎为0，含权债有负凸性风险）

    bond_ratio 仓位修正：
        组合级久期 → 底层债券真实久期 = 总久期 / bond_ratio（仓位<80%时展示）
    """
    DURATION_FALLBACK = 2.5  # 国内偏债型基金历史经验中值

    def _build_base(fr: pd.Series) -> pd.DataFrame:
        """把fund_ret转为date/fund_ret DataFrame"""
        d = pd.DataFrame({'fund_ret': fr}).reset_index()
        d.columns = ['date', 'fund_ret']
        d['date'] = pd.to_datetime(d['date'])
        return d

    # ════════════════════════════════════════════════════════════
    # 路径A：三因子回归（short端久期 + long端久期 + 信用利差）
    # ════════════════════════════════════════════════════════════
    three_factor_ok = (three_factor_df is not None and
                       not three_factor_df.empty and
                       'delta_y2' in three_factor_df.columns)

    if three_factor_ok:
        df = _build_base(fund_ret)
        tf = three_factor_df[['date', 'delta_y2', 'delta_y10', 'delta_spread']].copy()
        tf['date'] = pd.to_datetime(tf['date'])

        df = df.merge(tf, on='date', how='left')
        for col in ['delta_y2', 'delta_y10', 'delta_spread']:
            df[col] = df[col].ffill(limit=3)
        df = df.dropna(subset=['fund_ret', 'delta_y2', 'delta_y10'])

        has_spread = df['delta_spread'].notna().sum() >= 60

        if len(df) >= 60:
            # 构建回归矩阵：系数符号约定同单因子（-ΔY → 正久期）
            df['neg_dy2']  = -df['delta_y2']
            df['neg_dy10'] = -df['delta_y10']

            if has_spread:
                # 完整三因子（含信用利差）
                X = sm.add_constant(df[['neg_dy2', 'neg_dy10', 'delta_spread']].dropna())
                y = df.loc[X.index, 'fund_ret']
                factor_mode = '三因子（短端+长端+信用利差）'
            else:
                # 降级到双因子（无信用利差）
                X = sm.add_constant(df[['neg_dy2', 'neg_dy10']])
                y = df['fund_ret']
                factor_mode = '双因子（短端+长端，信用利差数据不足）'

            try:
                model3 = sm.OLS(y, X).fit()
            except Exception as e:
                three_factor_ok = False  # 回归失败，降级到单因子

        else:
            three_factor_ok = False  # 样本不足，降级

    if three_factor_ok:
        r2_adj = float(model3.rsquared_adj)
        r2     = float(model3.rsquared)
        alpha_daily = float(model3.params.get('const', 0.0))
        carry_alpha = (1 + alpha_daily) ** 252 - 1

        dur_short  = float(model3.params.get('neg_dy2',  0.0))
        dur_long   = float(model3.params.get('neg_dy10', 0.0))
        dur_short  = float(np.clip(dur_short,  -15.0, 15.0))
        dur_long   = float(np.clip(dur_long,   -15.0, 15.0))
        credit_beta = float(model3.params.get('delta_spread', 0.0)) if has_spread else 0.0

        # p值（显著性检验）
        p_short  = float(model3.pvalues.get('neg_dy2',        1.0))
        p_long   = float(model3.pvalues.get('neg_dy10',       1.0))
        p_credit = float(model3.pvalues.get('delta_spread',   1.0)) if has_spread else 1.0

        # 组合有效久期（短端+长端加总，忽略信用利差贡献）
        duration_portfolio   = abs(dur_short) + abs(dur_long)
        duration_portfolio   = float(np.clip(duration_portfolio, 0.1, 20.0))
        duration_source      = 'three_factor' if r2_adj > 0.05 else 'fallback_low_r2'

        if duration_source == 'fallback_low_r2':
            duration_portfolio = DURATION_FALLBACK

        # 仓位修正
        bond_ratio_clamp = max(bond_ratio, 0.05)
        if bond_ratio < 0.80:
            duration_underlying = duration_portfolio / bond_ratio_clamp
            position_note = (f'（组合久期 {duration_portfolio:.1f}年 ÷ '
                             f'债券仓位 {bond_ratio*100:.0f}% = '
                             f'底层债券真实久期 {duration_underlying:.1f}年）')
        else:
            duration_underlying = duration_portfolio
            position_note = ''

        # ── 凸性：静态穿透法 ──
        # 三因子模式下，凸性改用持仓结构判断（回归法对日频数据不准）
        convexity = 0.0  # 向后兼容字段
        convexity_quality, convexity_note = _convexity_from_holdings(holdings)

        # ── 基金类型诊断 ──
        fund_type_note = ''
        if p_short < 0.05 and abs(dur_short) > abs(dur_long) * 1.5:
            fund_type_note = '主要买短期债（1-2年），利率一动净值就跟着动，类似短债/货币增强型基金。'
        elif p_long < 0.05 and abs(dur_long) > abs(dur_short) * 1.5:
            fund_type_note = '主要买长期国债（7-10年），降息时赚得多，加息时跌得也多，利率走势影响很大。'
        elif p_short < 0.05 and p_long < 0.05:
            fund_type_note = '同时配置短债和长债，利率两端都有敞口，类似"哑铃"结构。'

        credit_note = ''
        if has_spread and p_credit < 0.05:
            if credit_beta < -1.0:
                credit_note = f'信用敞口较重——市场出现信用风波（债券违约/评级下调）时，该基金净值会比较难看。高收益背后对应着信用风险。'
            elif credit_beta > 1.0:
                credit_note = f'信用利差收窄时该基金受益，持有较多信用债或类似策略。'
        elif has_spread:
            credit_note = f'信用风险影响不显著，多余的收益更可能来自骑乘（买久一点吃利差）或杠杆操作。'

        # ── 解读文本 ──
        parts = []

        # 回归质量
        if duration_source == 'fallback_low_r2':
            parts.append(
                f'⚠️ 模型拟合效果偏弱（Adj.R²={r2_adj:.3f}），这期间的净值变化不太能用利率解释，'
                f'久期已用行业经验值 {DURATION_FALLBACK:.1f}年替代，压力测试仅供参考'
            )
        else:
            parts.append(
                f'✅ 模型拟合Adj.R²={r2_adj:.3f}，{"解释力不错" if r2_adj > 0.3 else "解释力一般，净值还受其他因素影响"}'
            )

        # 基金类型诊断
        if fund_type_note:
            parts.append(fund_type_note)

        # 久期分解
        dur_display = duration_underlying
        dur_src_note = '（行业经验估算）' if duration_source == 'fallback_low_r2' else position_note
        if dur_display > 7:
            parts.append(f"利率敏感度较高（久期 {dur_display:.1f}年{dur_src_note}），加息时净值压力大")
        elif dur_display < 2:
            parts.append(f"利率敏感度很低（久期 {dur_display:.1f}年{dur_src_note}），基本不怕加息，收益主要靠票息")
        else:
            parts.append(f"利率敏感度中等（久期 {dur_display:.1f}年{dur_src_note}）")

        if duration_source != 'fallback_low_r2':
            if p_short < 0.1:
                parts.append(f"  → 短债贡献 {abs(dur_short):.2f}年（β={dur_short:.2f}, p={p_short:.3f}）")
            if p_long < 0.1:
                parts.append(f"  → 长债贡献 {abs(dur_long):.2f}年（β={dur_long:.2f}, p={p_long:.3f}）")

        # 信用利差诊断
        if credit_note:
            parts.append(credit_note)

        # 凸性（静态穿透）
        parts.append(convexity_note)

        # Carry
        if carry_alpha > 0.04:
            parts.append(f"年化收益来源估算 {carry_alpha*100:.1f}%（**偏高**），需留意信用风险或流动性风险")
        elif carry_alpha > 0.015:
            parts.append(f"年化收益来源估算 {carry_alpha*100:.1f}%，水平合理")
        elif carry_alpha > 0:
            parts.append(f"年化收益来源估算 {carry_alpha*100:.2f}%，主要靠利率方向赚钱")
        else:
            parts.append("收益来源估算为负，纯博利率方向，信用成分极少")

        return {
            'duration':             duration_portfolio,
            'duration_underlying':  duration_underlying,
            'dur_short':            dur_short,
            'dur_long':             dur_long,
            'p_short':              p_short,
            'p_long':               p_long,
            'credit_beta':          credit_beta,
            'p_credit':             p_credit,
            'has_credit_factor':    has_spread,
            'convexity':            convexity,
            'convexity_quality':    convexity_quality,
            'carry_alpha':          carry_alpha,
            'credit_spread_alpha':  carry_alpha,
            'r_squared':            r2,
            'r_squared_adj':        r2_adj,
            'factor_mode':          factor_mode,
            'bond_ratio_used':      bond_ratio,
            'duration_source':      duration_source,
            'fund_type_note':       fund_type_note,
            'interpretation':       '；'.join(parts),
        }

    # ════════════════════════════════════════════════════════════
    # 路径B：单因子T-Model降级（三因子数据不可用时）
    # ════════════════════════════════════════════════════════════
    df = _build_base(fund_ret)
    t = treasury_df[['date', 'delta_y']].copy()
    t['date'] = pd.to_datetime(t['date'])
    df = df.merge(t, on='date', how='left')
    df['delta_y'] = df['delta_y'].ffill(limit=3)
    df = df.dropna(subset=['fund_ret', 'delta_y'])

    if len(df) < 60:
        return {
            'duration': DURATION_FALLBACK, 'duration_underlying': DURATION_FALLBACK,
            'dur_short': 0.0, 'dur_long': 0.0,
            'p_short': 1.0, 'p_long': 1.0,
            'credit_beta': 0.0, 'p_credit': 1.0, 'has_credit_factor': False,
            'convexity': 0.0, 'convexity_quality': 'unknown',
            'carry_alpha': 0.0, 'credit_spread_alpha': 0.0,
            'r_squared': 0.0, 'r_squared_adj': 0.0,
            'factor_mode': 'default（数据不足）',
            'bond_ratio_used': bond_ratio, 'duration_source': 'default',
            'fund_type_note': '',
            'interpretation': f'数据不足，无法回归，使用经验默认久期{DURATION_FALLBACK:.1f}年'
        }

    df['neg_dy']     = -df['delta_y']
    df['dy_sq_half'] = 0.5 * df['delta_y'] ** 2

    X = sm.add_constant(df[['neg_dy', 'dy_sq_half']])
    y = df['fund_ret']

    try:
        model1 = sm.OLS(y, X).fit()
    except Exception as e:
        return {'duration': DURATION_FALLBACK, 'duration_underlying': DURATION_FALLBACK,
                'dur_short': 0.0, 'dur_long': 0.0,
                'p_short': 1.0, 'p_long': 1.0,
                'credit_beta': 0.0, 'p_credit': 1.0, 'has_credit_factor': False,
                'convexity': 0.0, 'convexity_quality': 'unknown',
                'carry_alpha': 0.0, 'credit_spread_alpha': 0.0,
                'r_squared': 0.0, 'r_squared_adj': 0.0,
                'factor_mode': 'fallback（回归失败）',
                'bond_ratio_used': bond_ratio, 'duration_source': 'default',
                'fund_type_note': '',
                'interpretation': f'回归失败: {e}，使用经验默认久期{DURATION_FALLBACK:.1f}年'}

    duration_portfolio = float(model1.params.get('neg_dy', 0.0))
    convexity          = float(model1.params.get('dy_sq_half', 0.0))
    alpha_daily        = float(model1.params.get('const', 0.0))
    carry_alpha        = (1 + alpha_daily) ** 252 - 1
    r2                 = float(model1.rsquared)
    r2_adj             = float(model1.rsquared_adj)
    factor_mode        = '单因子（10年期国债，三因子数据不可用）'

    duration_source = 'regression'
    if r2 < 0.3:
        duration_portfolio = DURATION_FALLBACK
        duration_source    = 'fallback_low_r2'
    else:
        duration_portfolio = float(np.clip(duration_portfolio, 0.1, 15.0))

    bond_ratio_clamp = max(bond_ratio, 0.05)
    if bond_ratio < 0.80:
        duration_underlying = duration_portfolio / bond_ratio_clamp
        position_note = (f'（组合久期 {duration_portfolio:.1f}年 ÷ '
                         f'债券仓位 {bond_ratio*100:.0f}% = '
                         f'底层债券真实久期 {duration_underlying:.1f}年）')
    else:
        duration_underlying = duration_portfolio
        position_note = ''

    convexity_quality, convexity_note = _convexity_from_holdings(holdings)

    parts = []
    if duration_source == 'fallback_low_r2':
        parts.append(
            f'⚠️ 模型拟合偏弱（R²={r2:.2f}），这段时间净值变化不太能用利率解释，'
            f'久期改用行业经验值 {DURATION_FALLBACK:.1f}年，压力测试仅供参考'
        )
    dur_src_note = '（行业经验估算）' if duration_source == 'fallback_low_r2' else position_note
    if duration_underlying > 7:
        parts.append(f"利率敏感度高（久期 {duration_underlying:.1f}年{dur_src_note}），加息时净值压力大")
    elif duration_underlying < 2:
        parts.append(f"利率敏感度低（久期 {duration_underlying:.1f}年{dur_src_note}），基本不怕利率波动")
    else:
        parts.append(f"利率敏感度中等（久期 {duration_underlying:.1f}年{dur_src_note}）")

    parts.append(convexity_note)

    if carry_alpha > 0.04:
        parts.append(f"年化收益来源 {carry_alpha*100:.1f}%（偏高），可能含信用债或骑乘收益")
    elif carry_alpha > 0.015:
        parts.append(f"年化收益来源 {carry_alpha*100:.1f}%，水平合理")
    elif carry_alpha > 0:
        parts.append(f"年化收益来源 {carry_alpha*100:.2f}%，主要靠利率方向赚钱")
    else:
        parts.append("收益来源为负，纯利率方向博弈型")

    return {
        'duration':             duration_portfolio,
        'duration_underlying':  duration_underlying,
        'dur_short':            0.0,
        'dur_long':             duration_portfolio,
        'p_short':              1.0,
        'p_long':               1.0,
        'credit_beta':          0.0,
        'p_credit':             1.0,
        'has_credit_factor':    False,
        'convexity':            convexity,
        'convexity_quality':    convexity_quality,
        'carry_alpha':          carry_alpha,
        'credit_spread_alpha':  carry_alpha,
        'r_squared':            r2,
        'r_squared_adj':        r2_adj,
        'factor_mode':          factor_mode,
        'bond_ratio_used':      bond_ratio,
        'duration_source':      duration_source,
        'fund_type_note':       '',
        'interpretation':       '；'.join(parts),
    }


def _convexity_from_holdings(holdings: dict) -> tuple:
    """
    凸性静态穿透法：根据持仓结构判断凸性质量。
    回归法在日频小波动下不准，改用定性判断。

    Returns: (quality_str, display_note)
      quality_str: 'strong' | 'weak' | 'unknown'
      display_note: 供展示层使用的文字说明
    """
    if not holdings:
        return 'unknown', '凸性：持仓数据不足，无法穿透估算'

    # 尝试从持仓中提取债券类别占比
    bond_ratio = holdings.get('bond_ratio', 0.0)
    stock_ratio = holdings.get('stock_ratio', 0.0)

    # v10.1：优先读取季报穿透数据（由 analyze_bond_structure 注入）
    gov_bond_ratio = holdings.get('gov_bond_ratio', None)   # 利率债占净值比
    credit_ratio   = holdings.get('credit_bond_ratio', None)  # 信用债+含权债占净值比

    if gov_bond_ratio is not None and (gov_bond_ratio + (credit_ratio or 0)) > 0.01:
        _total = gov_bond_ratio + (credit_ratio or 0)
        _rate_pct   = gov_bond_ratio / _total * 100 if _total > 0 else 0
        _credit_pct = (credit_ratio or 0) / _total * 100 if _total > 0 else 0
        if _rate_pct >= 60:
            return 'strong', (
                f'凸性：**强**（利率债占持仓 {_rate_pct:.0f}%），利率大跌时净值涨得更快，下行保护好'
            )
        elif _credit_pct >= 60:
            return 'weak', (
                f'凸性：**弱**（信用/含权债占持仓 {_credit_pct:.0f}%），极端行情下净值波动不会有太多缓冲'
            )
        else:
            return 'moderate', (
                f'凸性：**中等**（利率债 {_rate_pct:.0f}% / 信用含权债 {_credit_pct:.0f}%），混合结构，有一定缓冲'
            )
    else:
        # 无细分数据，根据 bond_ratio 给出通用说明
        if bond_ratio > 0.8:
            return 'unknown', ('凸性：暂无持仓明细，无法精确判断；利率债为主则缓冲好，信用债为主则缓冲弱')
        else:
            return 'unknown', '凸性：混合仓位基金，债券持仓结构待明确后可进一步判断'


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
        'r_bm':                    r_bm_ann,
        'fund_annual':             fund_ann,
        'interpretation':          '；'.join(parts)
    }


def run_rolling_beta(fund_ret: pd.Series,
                     stock_index_ret: pd.Series,
                     bond_index_ret: pd.Series,
                     window: int = 20) -> pd.DataFrame:
    """
    滚动窗口双因子回归，同时计算 20日 和 60日 两条曲线，用于双重验证。

    约束说明：
      - 使用普通 OLS 回归后，对 equity_beta clip(0, 1)；
        量化实务的更严格做法是带约束优化（scipy.optimize.minimize），
        但 clip 已足以消除杠杆/对冲引起的越界，且计算量更低。
      - 若需完整约束（β股≥0, β债≥0, β股+β债≤1），可扩展到 constrained 版本。

    返回 DataFrame 列：
      date / equity_beta_20 / bond_beta_20 / r2_20 /
              equity_beta_60 / bond_beta_60 / r2_60

    可信度判断（r2）：
      ≥0.80 → 信号可信（基金与股债基准高度相关）
      0.50~0.80 → 参考价值一般
      <0.50 → 近期信号有噪音，谨慎参考
    """
    df = pd.DataFrame({'fund': fund_ret}).reset_index()
    df.columns = ['date', 'fund']
    df['date'] = pd.to_datetime(df['date'])

    si = pd.DataFrame({
        'date':  stock_index_ret.index.tolist() if hasattr(stock_index_ret, 'index') else range(len(stock_index_ret)),
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
        """对单个窗口做 OLS，返回 equity_beta / bond_beta / r2"""
        try:
            X = sm.add_constant(chunk[['stock', 'bond']])
            m = sm.OLS(chunk['fund'], X).fit()
            beta_s = float(m.params.get('stock', np.nan))
            beta_b = float(m.params.get('bond',  np.nan))
            # clip(0, 1)：消除加杠杆 >1 或对冲 <0 导致的越界
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

    # 兼容旧字段（detect_style_drift 读 equity_beta）
    result['equity_beta'] = result['equity_beta_20']
    result['bond_beta']   = result['bond_beta_20']

    return result


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


# ---------- M4. 行业/主题型：精准行业 Alpha ----------

def run_sector_model(fund_ret: pd.Series,
                     bm_ret: pd.Series,
                     sw_industry_ret: pd.Series = None,
                     sw_industry_name: str = '',
                     fund_name: str = '') -> dict:
    """
    行业/主题基金：精准行业基准 Alpha + 跟踪误差 + 信息比率

    核心升级：
      - 如果提供 sw_industry_ret（申万行业指数），优先用它做行业基准
      - 避免"用沪深300衡量医药基金"的伪Alpha陷阱：
        → 医药行业整体暴涨时，经理躺平也能跑赢300指数，Alpha虚高
        → 只有跑赢"医药指数"的医药基金经理，才是真本事
      - 同时保留 bm_ret（招募说明书基准）供参考对比

    返回：
      neutral_alpha      - 行业内年化Alpha（vs 申万行业指数，"窝里横"指数）
      neutral_alpha_bm   - vs 招募说明书基准的传统Alpha（对比用）
      tracking_error     - 年化跟踪误差（vs 行业指数）
      info_ratio         - 信息比率 = neutral_alpha / tracking_error
      bm_source          - 基准来源说明
      excess_series      - 每日超额收益序列（用于后续可视化）
      interpretation     - 大白话解读
    """
    # ---- 确定行业基准：申万行业指数优先 ----
    use_sw = sw_industry_ret is not None and not sw_industry_ret.empty and len(sw_industry_ret) > 20
    actual_bm_ret = sw_industry_ret if use_sw else bm_ret

    bm_source = (f'申万{sw_industry_name}指数（精准行业基准）' if use_sw
                 else '招募说明书基准（未匹配行业指数）')

    # ---- 主计算：vs 行业指数 ----
    df = pd.DataFrame({'fund': fund_ret, 'bm': actual_bm_ret}).dropna()
    if len(df) < 30:
        return {
            'neutral_alpha': 0.0, 'neutral_alpha_bm': 0.0,
            'tracking_error': 0.0, 'info_ratio': 0.0,
            'bm_source': bm_source, 'excess_series': pd.Series(dtype=float),
            'sw_code': '', 'sw_name': sw_industry_name,
            'interpretation': '数据不足（需至少30个交易日）'
        }

    excess = df['fund'] - df['bm']
    # 年化用 *252（与建议代码保持一致，简洁直接）
    neutral_alpha  = excess.mean() * 252
    tracking_error = excess.std() * np.sqrt(252)
    info_ratio     = neutral_alpha / tracking_error if tracking_error > 0 else 0.0

    # ---- 对比：vs 招募说明书基准（可选，仅在两者不同时计算）----
    neutral_alpha_bm = neutral_alpha  # 默认相同
    if use_sw and not bm_ret.empty:
        df_bm = pd.DataFrame({'fund': fund_ret, 'bm': bm_ret}).dropna()
        if len(df_bm) >= 30:
            neutral_alpha_bm = (df_bm['fund'] - df_bm['bm']).mean() * 252

    # ---- 解读层：大白话 ----
    parts = []
    industry_label = sw_industry_name or '同行业'

    # Alpha 解读
    if neutral_alpha > 0.08:
        parts.append(
            f"「窝里横」指数 🏆：行业内年化Alpha {neutral_alpha*100:.1f}%。"
            f"就算{industry_label}指数不涨不跌，经理靠选股一年也能多赚{neutral_alpha*100:.1f}%。"
            f"这是真本事。"
        )
    elif neutral_alpha > 0.03:
        parts.append(
            f"「窝里横」指数 ✅：行业内年化Alpha {neutral_alpha*100:.1f}%，"
            f"在{industry_label}内部具备选股超额能力。"
        )
    elif neutral_alpha > 0:
        parts.append(
            f"行业内年化Alpha {neutral_alpha*100:.1f}%，"
            f"勉强跑赢{industry_label}指数，优势不明显。"
        )
    else:
        parts.append(
            f"⚠️ 行业内年化Alpha {neutral_alpha*100:.1f}%（负值）。"
            f"连{industry_label}指数都跑不赢，买指数基金反而更划算。"
        )

    # 对比传统Alpha（仅在有申万基准时）
    if use_sw and abs(neutral_alpha - neutral_alpha_bm) > 0.02:
        if neutral_alpha_bm > neutral_alpha:
            parts.append(
                f"【基准陷阱提醒】vs沪深300 Alpha={neutral_alpha_bm*100:.1f}%，"
                f"但vs行业指数只有{neutral_alpha*100:.1f}%——"
                f"部分超额来自行业Beta，不是经理真本事。"
            )
        else:
            parts.append(
                f"vs沪深300 Alpha={neutral_alpha_bm*100:.1f}%，"
                f"vs行业指数Alpha={neutral_alpha*100:.1f}%，两者接近，Alpha较为纯粹。"
            )

    # 跟踪误差解读
    if tracking_error < 0.03:
        parts.append(
            f"「不走寻常路」程度 🟢：跟踪误差{tracking_error*100:.1f}%极低，"
            f"经理几乎按行业指数持仓，是增强型指数风格。"
        )
    elif tracking_error < 0.08:
        parts.append(
            f"「不走寻常路」程度 🟡：跟踪误差{tracking_error*100:.1f}%，"
            f"经理做了一定个股偏离，有主动管理色彩。"
        )
    elif tracking_error < 0.15:
        parts.append(
            f"「不走寻常路」程度 🟠：跟踪误差{tracking_error*100:.1f}%偏高，"
            f"经理在行业内部大量偏离——比如{industry_label}里重点押注某细分赛道。"
        )
    else:
        parts.append(
            f"「不走寻常路」程度 🔴：跟踪误差{tracking_error*100:.1f}%极高，"
            f"基金与行业指数差异巨大，个股集中度风险显著。"
        )

    # 信息比率解读
    if info_ratio > 1.5:
        parts.append(
            f"「选股性价比」💎：IR={info_ratio:.2f}，每冒1%偏离风险换回{info_ratio:.2f}%超额，"
            f"选股不仅准而且稳，属于高效Alpha。"
        )
    elif info_ratio > 0.5:
        parts.append(
            f"「选股性价比」🟡：IR={info_ratio:.2f}，每冒1%偏离风险换回{info_ratio:.2f}%超额，"
            f"选股效率尚可，但还未到顶级水准。"
        )
    elif info_ratio > 0:
        parts.append(
            f"「选股性价比」🟠：IR={info_ratio:.2f}，超额收益靠较高的个股集中度「赌」出来，"
            f"风险收益比不够划算。"
        )
    else:
        parts.append(f"「选股性价比」🔴：IR={info_ratio:.2f}，超额为负，主动管理未带来价值。")

    return {
        'neutral_alpha':    neutral_alpha,
        'neutral_alpha_bm': neutral_alpha_bm,
        'tracking_error':   tracking_error,
        'info_ratio':       info_ratio,
        'bm_source':        bm_source,
        'excess_series':    excess.rename('excess'),
        'excess_return':    neutral_alpha,   # 与 performance_decomposition 接口对齐
        'sw_code':          '',              # 由上层填入
        'sw_name':          sw_industry_name,
        'interpretation':   '；'.join(parts)
    }


# ============================================================
# ██████████████  TRANSLATION LAYER  ██████████████
# ============================================================

# ---- 辅助：滚动Alpha趋势检测 ----

def _calc_rolling_alpha_trend(fund_ret: pd.Series,
                               bm_ret: pd.Series,
                               window: int = 63) -> dict:
    """
    计算近三个月（约63个交易日）的滚动Alpha趋势，判断经理近期"状态"。

    逻辑：
      - 将全期切分为若干 window 天的月度滚动窗口（步长=21天，约1个月）
      - 计算每段的超额收益年化 Alpha
      - 若最近连续 3 段 Alpha 均呈下降趋势（逐步变差），触发"状态低迷"信号
      - 返回方向（上升/下降/震荡）+ 近期每月Alpha列表 + 是否状态低迷

    注意：只需要基金和基准的收益率序列，不依赖FF因子，计算量极小。
    """
    df = pd.DataFrame({'fund': fund_ret, 'bm': bm_ret}).dropna()
    if len(df) < window * 2:
        return {'trend': 'insufficient', 'monthly_alphas': [], 'is_slump': False,
                'trend_text': ''}

    # 每21天一个滚动窗口，取最近5个月
    step = 21
    alphas = []
    for i in range(len(df) - window, max(len(df) - window * 5, -1), -step):
        if i < 0:
            break
        chunk = df.iloc[i: i + window]
        if len(chunk) < 30:
            continue
        monthly_alpha = (chunk['fund'] - chunk['bm']).mean() * 252
        alphas.append(monthly_alpha)

    alphas = list(reversed(alphas))  # 时间正序

    if len(alphas) < 3:
        return {'trend': 'insufficient', 'monthly_alphas': alphas, 'is_slump': False,
                'trend_text': ''}

    recent_3 = alphas[-3:]

    # 连续下降：每一步都比前一步低
    is_slump = (recent_3[0] > recent_3[1] > recent_3[2])

    # 连续上升
    is_recovering = (recent_3[0] < recent_3[1] < recent_3[2])

    if is_slump:
        trend = 'slump'
        trend_text = (
            f"近3个月滚动Alpha分别为 {recent_3[0]*100:.1f}% / {recent_3[1]*100:.1f}% / {recent_3[2]*100:.1f}%，"
            "连续下降——经理近期可能进入<b>「审美疲劳期」</b>，"
            "原有选股逻辑出现钝化，需持续观察是否能扭转。"
        )
    elif is_recovering:
        trend = 'recovering'
        trend_text = (
            f"近3个月滚动Alpha {recent_3[0]*100:.1f}% → {recent_3[1]*100:.1f}% → {recent_3[2]*100:.1f}%，"
            "逐月回升，经理状态趋于改善，可关注后续表现。"
        )
    else:
        trend = 'volatile'
        trend_text = (
            f"近3个月滚动Alpha分别为 {recent_3[0]*100:.1f}% / {recent_3[1]*100:.1f}% / {recent_3[2]*100:.1f}%，"
            f"走势震荡，暂无明确趋势信号。"
        )

    return {
        'trend':          trend,
        'monthly_alphas': alphas,
        'is_slump':       is_slump,
        'is_recovering':  is_recovering,
        'trend_text':     trend_text,
        'recent_3':       recent_3,
    }


# ============================================================
# ██████████████  RADAR CHART MODULE  ██████████████
# ============================================================

def calc_radar_scores(
    model_type: str,
    model_results: dict,
    nav_df: pd.DataFrame,
    bm_df: pd.DataFrame,
    rolling_df: pd.DataFrame = None,
    fund_type_category: str = 'equity',
) -> dict:
    """
    计算基金综合实力雷达图5维评分（0-100分）。

    维度定义：
      Alpha   超额能力  ← 年化 Alpha / 中性化 Alpha
      Risk    风险控制  ← 最大回撤 + 年化波动率（越小越好，取反后打分）
      Eff     性价比    ← 夏普比率 + 信息比率（IR）
      Stab    风格稳定性 ← 滚动Beta偏差 + R²解释度
      Persist 业绩持续性 ← 月度胜率 + 盈亏比

    各维度均归一化到 0-100，50分为行业中位数水准。

    权重策略：
      偏股型(主动权益):  超额30% 风控15% 性价比20% 稳定15% 持续20%
      纯债型(如000069):  超额15% 风控35% 性价比20% 稳定10% 持续20%
      量化/指数增强:      超额35% 风控10% 性价比25% 稳定20% 持续10%
      其他类型:           统一使用等权重20%
    """

    # ---- 基础收益率序列 ----
    ret = nav_df.set_index('date')['ret'].dropna() if 'ret' in nav_df.columns else pd.Series(dtype=float)
    nav_vals = nav_df['nav']
    ann_factor = 252

    # ---- 权重配置 ----
    def _get_weights(ftype: str) -> dict:
        """根据基金类型返回五维权重（总和1.0）"""
        # 偏股型：选股是核心
        if ftype in ('equity', 'mixed'):
            return {'超额能力': 0.30, '风险控制': 0.15, '性价比': 0.20, '风格稳定': 0.15, '业绩持续': 0.20}
        # 纯债型：不能跌，风控最重要
        elif ftype == 'bond':
            return {'超额能力': 0.15, '风险控制': 0.35, '性价比': 0.20, '风格稳定': 0.10, '业绩持续': 0.20}
        # 量化/指数增强：超额是核心，策略一致性极重要
        elif ftype in ('index', 'sector'):
            return {'超额能力': 0.35, '风险控制': 0.10, '性价比': 0.25, '风格稳定': 0.20, '业绩持续': 0.10}
        # 其他类型（qdii等）：等权重
        else:
            return {'超额能力': 0.20, '风险控制': 0.20, '性价比': 0.20, '风格稳定': 0.20, '业绩持续': 0.20}

    weights = _get_weights(fund_type_category)

    # --------------------------------------------------
    # 维度1：超额能力 Alpha（0-100）
    # --------------------------------------------------
    alpha_raw = 0.0
    alpha_p   = 1.0
    if model_type in ('equity',):
        alpha_raw = model_results.get('alpha', 0.0) or 0.0
        alpha_p   = model_results.get('alpha_pval', 1.0) or 1.0
    elif model_type == 'sector':
        sr = model_results.get('sector', model_results)
        alpha_raw = sr.get('neutral_alpha', 0.0) or 0.0
        alpha_p   = 0.05  # sector model 直接用超额序列，默认视为显著
    elif model_type == 'mixed':
        alpha_raw = model_results.get('excess_return', 0.0) or 0.0
        alpha_p   = 0.05
    elif model_type == 'bond':
        alpha_raw = model_results.get('carry_alpha', 0.0) or 0.0
        alpha_p   = 0.05

    # 评分逻辑：Alpha越高且越显著 → 分越高
    # 基础分：[-10%,+20%] → [0,100]，每1%≈3.3分
    alpha_base = np.clip((alpha_raw + 0.10) / 0.30 * 100, 0, 100)
    # 显著性折扣：p>0.1 降低可信度
    if alpha_p > 0.1:
        alpha_base *= 0.75
    elif alpha_p > 0.05:
        alpha_base *= 0.90
    score_alpha = round(np.clip(alpha_base, 0, 100))

    # --------------------------------------------------
    # 维度2：风险控制 Risk（0-100，越小越好取反）
    # --------------------------------------------------
    _nav_cum  = nav_vals
    _roll_max = _nav_cum.cummax()
    max_dd    = ((_nav_cum - _roll_max) / _roll_max).min()   # 负值
    vol       = ret.std() * np.sqrt(ann_factor) if len(ret) > 10 else 0.2

    # 最大回撤评分：[-50%,0%] → [0,100]，回撤0分=100，回撤-50%=0
    dd_score  = np.clip((1 + max_dd / 0.50) * 100, 0, 100)
    # 年化波动率评分：[0%, 40%] → [100,0]
    vol_score = np.clip((1 - vol / 0.40) * 100, 0, 100)
    score_risk = round(np.clip(dd_score * 0.6 + vol_score * 0.4, 0, 100))

    # --------------------------------------------------
    # 维度3：性价比 Efficiency（0-100）
    # --------------------------------------------------
    # 夏普比率
    rf_daily = 0.02 / ann_factor   # 无风险利率2%
    excess_ret = ret - rf_daily
    sharpe = (excess_ret.mean() / excess_ret.std() * np.sqrt(ann_factor)
              if len(excess_ret) > 10 and excess_ret.std() > 0 else 0.0)

    # 信息比率（如果有基准）
    ir = 0.0
    if not bm_df.empty and 'bm_ret' in bm_df.columns:
        bm_ret_s = bm_df.set_index('date')['bm_ret'].dropna()
        aligned  = pd.concat([ret, bm_ret_s], axis=1, join='inner').dropna()
        if len(aligned) > 20:
            ex_s = aligned.iloc[:, 0] - aligned.iloc[:, 1]
            ir = (ex_s.mean() / ex_s.std() * np.sqrt(ann_factor)
                  if ex_s.std() > 0 else 0.0)

    # 也可以从 sector model 直接读取 IR
    if model_type == 'sector':
        sr = model_results.get('sector', model_results)
        ir = sr.get('info_ratio', ir) or ir

    # 夏普评分：[-1, 3] → [0, 100]
    sharpe_score = np.clip((sharpe + 1.0) / 4.0 * 100, 0, 100)
    # IR评分：[-1, 2] → [0, 100]
    ir_score     = np.clip((ir + 1.0) / 3.0 * 100, 0, 100)
    score_eff    = round(np.clip(sharpe_score * 0.5 + ir_score * 0.5, 0, 100))

    # --------------------------------------------------
    # 维度4：风格稳定性 Stability（0-100）
    # --------------------------------------------------
    r2        = 0.6   # 默认值
    beta_std  = 0.15  # 滚动Beta标准差，越小越稳

    if model_type == 'equity':
        r2       = model_results.get('r_squared', 0.6) or 0.6
    elif model_type == 'mixed':
        r2       = 0.5   # 混合型天然R²偏低

    if rolling_df is not None and not rolling_df.empty:
        _beta_cols = [c for c in rolling_df.columns if 'beta' in c.lower() and '20' in c]
        if _beta_cols:
            beta_series = rolling_df[_beta_cols[0]].dropna()
            if len(beta_series) > 5:
                beta_std = beta_series.std()

    # R²稳定性：如果R²太高（>0.95）说明完全贴基准，不算好的"稳定"（而是被动）
    # 理想R²范围：0.5~0.85 → 说明有主动管理且因子解释合理
    if r2 > 0.95:
        r2_score = 60   # 过于贴基准，扣分
    elif r2 >= 0.5:
        r2_score = np.clip((r2 - 0.5) / 0.45 * 100, 0, 100)
    else:
        r2_score = np.clip(r2 / 0.5 * 60, 0, 60)

    # Beta稳定性评分：[0, 0.4] → [100, 0]
    beta_stab_score = np.clip((1 - beta_std / 0.4) * 100, 0, 100)
    score_stab      = round(np.clip(r2_score * 0.4 + beta_stab_score * 0.6, 0, 100))

    # --------------------------------------------------
    # 维度5：业绩持续性 Persistence（0-100）
    # --------------------------------------------------
    # 月度胜率：基金月收益 > 基准月收益 的比例
    win_rate  = 0.5
    profit_loss_ratio = 1.0

    if not bm_df.empty and 'bm_ret' in bm_df.columns:
        bm_ret_s = bm_df.set_index('date')['bm_ret'].dropna()
        aligned  = pd.concat([ret, bm_ret_s], axis=1, join='inner').dropna()
        if len(aligned) > 20:
            ex_s       = aligned.iloc[:, 0] - aligned.iloc[:, 1]
            wins       = (ex_s > 0).sum()
            total      = len(ex_s)
            win_rate   = wins / total if total > 0 else 0.5
            avg_win    = ex_s[ex_s > 0].mean() if wins > 0 else 0.0
            losses     = (ex_s < 0).sum()
            avg_loss   = abs(ex_s[ex_s < 0].mean()) if losses > 0 else 1e-6
            profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 1.0
    else:
        # 无基准时，用对比0的胜率
        if len(ret) > 20:
            pos_days   = (ret > 0).sum()
            win_rate   = pos_days / len(ret)
            avg_pos    = ret[ret > 0].mean() if pos_days > 0 else 0
            neg_days   = (ret < 0).sum()
            avg_neg    = abs(ret[ret < 0].mean()) if neg_days > 0 else 1e-6
            profit_loss_ratio = avg_pos / avg_neg if avg_neg > 0 else 1.0

    # 胜率评分：[0.3, 0.7] → [0, 100]
    wr_score  = np.clip((win_rate - 0.30) / 0.40 * 100, 0, 100)
    # 盈亏比评分：[0.5, 2.5] → [0, 100]
    plr_score = np.clip((profit_loss_ratio - 0.5) / 2.0 * 100, 0, 100)
    score_persist = round(np.clip(wr_score * 0.5 + plr_score * 0.5, 0, 100))

    return {
        '超额能力': int(score_alpha),
        '风险控制': int(score_risk),
        '性价比':   int(score_eff),
        '风格稳定': int(score_stab),
        '业绩持续': int(score_persist),
        # 权重配置（供综合分计算）
        '_weights': weights,
        # 附加原始值供 tooltip 展示
        '_meta': {
            'alpha':     alpha_raw,
            'max_dd':    max_dd,
            'vol':       vol,
            'sharpe':    sharpe,
            'ir':        ir,
            'win_rate':  win_rate,
            'plr':       profit_loss_ratio,
        }
    }


def plot_fund_radar(fund_name: str, scores: dict) -> go.Figure:
    """
    使用 Plotly 绘制基金综合实力雷达图。

    Args:
        fund_name: 基金名称（用于标题）
        scores:    calc_radar_scores 返回的5维评分字典
                   键：'超额能力' / '风险控制' / '性价比' / '风格稳定' / '业绩持续'
                   _weights: 各维度权重配置

    Returns:
        plotly Figure 对象
    """
    _meta = scores.get('_meta', {})
    _weights = scores.get('_weights', {})

    dim_labels = ['超额能力', '风险控制', '性价比',
                  '风格稳定', '业绩持续']
    dim_keys   = ['超额能力', '风险控制', '性价比', '风格稳定', '业绩持续']
    values     = [scores.get(k, 50) for k in dim_keys]

    # 构建带权重提示的 tooltip 说明
    _tip_lines = [
        f"超额能力：年化超额 {_meta.get('alpha', 0)*100:.1f}% → {values[0]}分 (权重{int(_weights.get('超额能力', 0.2)*100)}%)",
        f"风险控制：回撤{_meta.get('max_dd', 0)*100:.1f}% · 波动{_meta.get('vol', 0)*100:.1f}% → {values[1]}分 (权重{int(_weights.get('风险控制', 0.2)*100)}%)",
        f"性价比：夏普{_meta.get('sharpe', 0):.2f} · 信息比率{_meta.get('ir', 0):.2f} → {values[2]}分 (权重{int(_weights.get('性价比', 0.2)*100)}%)",
        f"风格稳定：模型解释度 + 市场敏感度 → {values[3]}分 (权重{int(_weights.get('风格稳定', 0.2)*100)}%)",
        f"业绩持续：胜率{_meta.get('win_rate', 0)*100:.0f}% · 盈亏比{_meta.get('plr', 0):.2f} → {values[4]}分 (权重{int(_weights.get('业绩持续', 0.2)*100)}%)",
    ]
    tooltip_text = '<br>'.join(_tip_lines)

    # 闭合多边形
    theta_labels = dim_labels + [dim_labels[0]]
    r_values     = values + [values[0]]

    # 综合分：加权平均（根据基金类型权重）
    avg_score = sum(v * _weights.get(k, 0.2) for v, k in zip(values, dim_keys))
    if avg_score >= 80:
        fill_color  = 'rgba(39,174,96,0.25)'
        line_color  = '#27ae60'
        title_badge = '🟢 综合优秀'
    elif avg_score >= 60:
        fill_color  = 'rgba(230,126,34,0.20)'
        line_color  = '#e67e22'
        title_badge = '🟡 综合良好'
    else:
        fill_color  = 'rgba(231,76,60,0.20)'
        line_color  = '#e74c3c'
        title_badge = '🔴 综合偏弱'

    # 行业中位数参考线（50分基准）
    ref_vals = [50] * 5 + [50]

    fig = go.Figure()

    # 参考线（50分中位数圈）
    fig.add_trace(go.Scatterpolar(
        r=ref_vals,
        theta=theta_labels,
        fill='none',
        line=dict(color='#cccccc', width=1.5, dash='dot'),
        name='行业中位数（50分）',
        hoverinfo='skip',
    ))

    # 基金主雷达图
    fig.add_trace(go.Scatterpolar(
        r=r_values,
        theta=theta_labels,
        fill='toself',
        fillcolor=fill_color,
        line=dict(color=line_color, width=2.5),
        marker=dict(size=7, color=line_color),
        name=fund_name,
        text=[f'{v}分' for v in r_values],
        hovertemplate='%{theta}: <b>%{r}分</b><extra></extra>',
    ))

    fig.update_layout(
        polar=dict(
            bgcolor='rgba(248,250,252,0.8)',
            radialaxis=dict(
                range=[0, 100],
                tickvals=[0, 25, 50, 75, 100],
                ticktext=['0', '25', '50', '75', '100'],
                tickfont=dict(size=10, color='#999'),
                gridcolor='#e8eaf0',
                linecolor='#e8eaf0',
            ),
            angularaxis=dict(
                tickfont=dict(size=11, color='#333', family='Arial'),
                gridcolor='#e8eaf0',
                linecolor='#e8eaf0',
            ),
        ),
        showlegend=True,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.15,
            xanchor='center',
            x=0.5,
            font=dict(size=11),
        ),
        title=dict(
            text=(f'<b>{fund_name}</b> 综合实力透视图<br>'
                  f'<span style="font-size:12px;color:#888">'
                  f'{title_badge}｜均分 {avg_score:.0f}/100</span>'),
            x=0.5,
            xanchor='center',
            font=dict(size=14, color='#1a1a2e'),
        ),
        paper_bgcolor='white',
        margin=dict(t=90, b=60, l=60, r=60),
        height=420,
    )

    return fig


# ============================================================
# ██████████████  RISK ANALYSIS MODULE  ██████████████
# ============================================================

# ------------------------------------------------------------------
# 模块 A：收益拆解（仅混合/行业型基金使用）
# ------------------------------------------------------------------

def performance_decomposition(
    model_results: dict,
    sector_results: dict = None,
    nav_df: pd.DataFrame = None,
    bm_df: pd.DataFrame = None,
) -> dict:
    """
    三层收益拆解（混合类/行业类）

    层级：
      仓位择时贡献  ← Brinson 配置效应
      行业选股贡献  ← 中性化 Alpha × 行业权重（仅行业型可精确；混合型用选择效应代替）
      其他残差      ← 总超额 - 仓位 - 行业

    返回：
      {
        'total_excess':     年化总超额（浮点），
        'allocation':       仓位择时贡献（浮点），
        'sector_alpha':     行业选股贡献（浮点），
        'residual':         残差（浮点），
        'narrative':        一句话描述（str），
        'credit_lines':     功劳簿列表（list of str），
        'data_quality':     数据质量说明（str），
      }
    """
    # sector 类型的结果存在 model_results['sector'] 子字典中
    # 需要先展开，再读 excess_return/allocation_effect 等字段
    _mr = model_results
    if sector_results and isinstance(sector_results, dict):
        # 行业型：sector_results 本身就是 run_sector_model 的返回
        # 它有 neutral_alpha/excess_return，但没有 allocation_effect
        # total_excess 直接用 neutral_alpha（行业型无Brinson分解）
        _mr = sector_results
    elif 'sector' in model_results:
        _mr = model_results['sector']

    allocation   = _mr.get('allocation_effect', 0.0) or 0.0
    sel_inter    = _mr.get('selection_inter_effect',
                           _mr.get('selection_effect', 0.0)) or 0.0
    # 行业型：优先取 neutral_alpha（即 excess_return），fallback 再取 excess_return
    total_excess = (_mr.get('neutral_alpha') or _mr.get('excess_return', 0.0)) or 0.0

    data_quality = '正常'

    # 行业选股贡献 ─────────────────────────────────────────────────
    # 方案A（精确）：有行业Alpha × 行业权重
    # 方案B（近似）：直接用 Brinson 选择效应（已剔除 allocation 的残差）
    sector_alpha   = 0.0
    sector_label   = '行业选股贡献'
    credit_lines   = []

    if sector_results and isinstance(sector_results, dict):
        _na     = sector_results.get('neutral_alpha', 0.0) or 0.0
        _sw_wt  = sector_results.get('sector_weight', None)  # 行业权重（如有）
        _sw_name = sector_results.get('sw_name', '主要持仓行业')

        if _sw_wt and _sw_wt > 0:
            sector_alpha = _na * _sw_wt
            sector_label = f'{_sw_name}选股贡献'
            data_quality = '精确（行业Alpha × 行业权重）'
        else:
            # 无权重时用 neutral_alpha × 0.7（估算行业占股票仓位的约 70%）
            sector_alpha = _na * 0.70
            sector_label = f'{_sw_name}选股贡献（估算）'
            data_quality = '估算（中性化Alpha × 0.7权重）'
    else:
        # 无行业模型时，选择效应近似为行业选股
        sector_alpha = sel_inter
        sector_label = '个股选择贡献'
        data_quality = '近似（Brinson选择效应）'

    residual = total_excess - allocation - sector_alpha

    # 一句话叙事 ─────────────────────────────────────────────────
    _total_pct = total_excess * 100
    _alloc_pct = allocation   * 100
    _sector_pct= sector_alpha * 100
    _resid_pct = residual     * 100

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
        narrative = _total_desc + '，其中 ' + '，'.join(_breakdown_parts) + '，剩余为杂音。'
    else:
        narrative = _total_desc + '，各分项贡献均较微弱。'

    # 功劳簿 ─────────────────────────────────────────────────────
    if abs(_alloc_pct) >= 0.1:
        if _alloc_pct > 0:
            credit_lines.append(
                f'📍 **头号功臣：仓位择时**（+{_alloc_pct:.1f}%）'
                f'  经理在行情转换节点调整了股债比例，站队正确，带来正贡献。'
            )
        else:
            credit_lines.append(
                f'📍 **拖累项：仓位择时**（{_alloc_pct:.1f}%）'
                f'  大类资产配置方向判断失误，是本期超额落后的主因之一。'
            )

    if abs(_sector_pct) >= 0.1:
        if _sector_pct > 0:
            credit_lines.append(
                f'🎯 **核心技能：{sector_label}**（+{_sector_pct:.1f}%）'
                f'  即便行业整体平淡，经理通过精选个股获得了显著超额。'
            )
        else:
            credit_lines.append(
                f'⚠️ **拖累项：{sector_label}**（{_sector_pct:.1f}%）'
                f'  行业内个股选择跑输了同行业基准，拖累整体表现。'
            )

    if abs(_resid_pct) >= 0.1:
        if _resid_pct > 0:
            credit_lines.append(
                f'💡 **意外之财：模型残差**（+{_resid_pct:.1f}%）'
                f'  含打新收益、交易超额及模型无法解释的随机收益。'
            )
        else:
            credit_lines.append(
                f'🔇 **其他损耗**（{_resid_pct:.1f}%）'
                f'  含交易成本、微小滑点及模型残差。'
            )

    if not credit_lines:
        credit_lines.append('📊 各分项贡献均较微弱，超额主要来自整体市场的系统性波动。')

    return {
        'total_excess':  total_excess,
        'allocation':    allocation,
        'sector_alpha':  sector_alpha,
        'sector_label':  sector_label,
        'residual':      residual,
        'narrative':     narrative,
        'credit_lines':  credit_lines,
        'data_quality':  data_quality,
    }


# ------------------------------------------------------------------
# 模块 B：前十大重仓股估值风险预警
# ------------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stock_valuation_alert(stock_codes: list, period: str = '全部') -> list:
    """
    获取前十大重仓股当前 PE(TTM) 的历史分位数，生成估值风险预警。

    Args:
        stock_codes: 股票代码列表，如 ['600519', '000858', '00700', ...]
        period:      历史回溯区间，支持'近一年'/'近三年'/'近五年'

    Returns:
        list of dict，每项包含：
          {'code', 'name', 'current_pe', 'current_pb',
           'pe_percentile', 'pb_percentile', 'risk_level', 'risk_icon', 'note'}

    容错机制（v9.4 新增跨市场路由）：
      - A股（6位数字）：ak.stock_zh_valuation_baidu
      - 港股（5位数字/以0开头）：ak.stock_hk_valuation_baidu，默认PE中枢10.5x
      - 其他/ADR/B股等：跳过PE估值，标注"跨市场个股，估值数据暂缺"
      - 新股（历史数据 < 250 条，约不足1年）→ risk_level='缺乏历史', note='次新股无分位参考'
      - 亏损股（current_pe ≤ 0）→ PE分位跳过，降级使用 PB 分位；note 注明
      - 字段强类型转换（百度接口有时返回字符串），防 TypeError
      - 单只股票失败不影响其他股票
      - 百度财经接口不稳定时整体降级，显示「接口维护中」而非N/A
    """
    # 市场识别与默认PE中枢（用于数据缺失时的行业中值兜底）
    MARKET_PE_DEFAULTS = {'CN': 18.5, 'HK': 10.5, 'OTHER': 15.0}

    # 预先探测百度接口是否可用（用一只典型A股试探）
    _baidu_available = True
    try:
        _test = ak.stock_zh_valuation_baidu(symbol='600519', indicator='市盈率(TTM)', period='近三年')
        if _test is None or not isinstance(_test, pd.DataFrame):
            _baidu_available = False
    except Exception:
        _baidu_available = False

    def _identify_market(code: str) -> str:
        """识别股票所属市场"""
        code = str(code).strip()
        # A股：6位纯数字（以0/3/6开头）
        if re.match(r'^[036]\d{5}$', code):
            return 'CN'
        # 港股：纯数字且长度5位（以0开头）或以"HK"/"hk"开头
        if re.match(r'^0\d{4}$', code) or code.upper().startswith('HK'):
            return 'HK'
        # 台股：纯数字4位（如 2454）或以KS/JP/TW结尾的代码
        if re.match(r'^\d{4}$', code):
            return 'OTHER'   # 台股/日股，当前不支持
        # 韩股/日股等带后缀（如 009150KS、4062JP）
        if re.search(r'[A-Z]{2}$', code):
            return 'OTHER'
        # 美股/其他英文ticker（含字母）
        if re.search(r'[A-Za-z]', code):
            return 'OTHER'
        # 其他数字格式（6位非标准A股开头）也归OTHER
        return 'OTHER'

    # 百度财经A股接口，带自动重试
    @retry_on_failure(retries=3, delay=2)
    def _fetch_val_cn(code: str, ind: str):
        return ak.stock_zh_valuation_baidu(symbol=code, indicator=ind, period=period)

    # 百度财经港股接口，带自动重试
    @retry_on_failure(retries=3, delay=2)
    def _fetch_val_hk(code: str, ind: str):
        try:
            return ak.stock_hk_valuation_baidu(symbol=code, indicator=ind, period=period)
        except Exception:
            return None

    # 历史序列最低要求：约1年交易日（250条），低于此视为次新股
    MIN_HIST_ROWS = 250

    # 如果百度接口整体不可用，快速返回（避免每只股票都重试）
    if not _baidu_available:
        return [
            {
                'code': code, 'name': code,
                'current_pe': None, 'current_pb': None,
                'pe_percentile': None, 'pb_percentile': None,
                'risk_level': '接口维护',
                'risk_icon': '🔧',
                'note': '估值数据接口暂时不可用，请稍后再试'
            }
            for code in stock_codes[:10]
        ]

    results = []
    for code in stock_codes[:10]:
        item = {
            'code': code, 'name': code,
            'current_pe': None, 'current_pb': None,
            'pe_percentile': None, 'pb_percentile': None,
            'risk_level': '数据缺失', 'risk_icon': '⚪', 'note': ''
        }
        pe_skipped_reason = ''

        # ── 市场识别（v9.4 新增）──
        market = _identify_market(code)

        # 非 A/港 股（美股ADR、其他），跳过估值分析
        if market == 'OTHER':
            item['risk_level'] = '跨市场'
            item['risk_icon']  = '🌐'
            item['note']       = '海外/QDII持仓，当前接口暂不支持估值分析'
            results.append(item)
            continue

        # 根据市场选择抓取函数
        _fetch_val = _fetch_val_cn if market == 'CN' else _fetch_val_hk

        # ── PE(TTM) ──
        try:
            df_pe = _fetch_val(code, '市盈率(TTM)')
            if df_pe is not None and not df_pe.empty:
                # 字段强转（百度接口有时返回字符串）
                df_pe['value'] = pd.to_numeric(df_pe['value'], errors='coerce')
                df_pe = df_pe.dropna(subset=['value']).reset_index(drop=True)

                # 拦截1：次新股（历史数据不足1年）
                if len(df_pe) < MIN_HIST_ROWS:
                    pe_skipped_reason = '次新股，PE历史不足1年，分位无参考意义'
                else:
                    current_pe = float(df_pe['value'].iloc[-1])
                    # 拦截2：亏损股（PE ≤ 0，用正数历史段算分位会产生偏差）
                    if current_pe <= 0:
                        pe_skipped_reason = '当前盈利为负，PE失效，请参考PB分位'
                    else:
                        # 过滤亏损期（只用 PE>0 的历史段算分位，避免历史亏损污染）
                        df_pe_pos = df_pe[df_pe['value'] > 0]
                        if len(df_pe_pos) >= 60:
                            pe_pct = float((df_pe_pos['value'] < current_pe).mean() * 100)
                            item['current_pe']    = current_pe
                            item['pe_percentile'] = pe_pct
        except Exception:
            pass

        # ── PB ──
        try:
            df_pb = _fetch_val(code, '市净率')
            if df_pb is not None and not df_pb.empty:
                df_pb['value'] = pd.to_numeric(df_pb['value'], errors='coerce')
                df_pb = df_pb.dropna(subset=['value']).reset_index(drop=True)
                df_pb = df_pb[df_pb['value'] > 0]

                if len(df_pb) >= MIN_HIST_ROWS:
                    current_pb = float(df_pb['value'].iloc[-1])
                    if current_pb > 0:
                        pb_pct = float((df_pb['value'] < current_pb).mean() * 100)
                        item['current_pb']    = current_pb
                        item['pb_percentile'] = pb_pct
        except Exception:
            pass

        # ── note 合并 ──
        if pe_skipped_reason:
            item['note'] = pe_skipped_reason

        # ── 综合风险分级（优先用PE分位，PE失效时降级用PB）──
        main_pct = item['pe_percentile'] if item['pe_percentile'] is not None else item['pb_percentile']
        if main_pct is not None:
            if main_pct >= 85:
                item['risk_level'] = '极度高估'
                item['risk_icon']  = '🔴'
            elif main_pct >= 70:
                item['risk_level'] = '偏高估值'
                item['risk_icon']  = '🟠'
            elif main_pct <= 15:
                item['risk_level'] = '极度低估'
                item['risk_icon']  = '🟢'
            elif main_pct <= 30:
                item['risk_level'] = '相对低估'
                item['risk_icon']  = '🔵'
            else:
                item['risk_level'] = '估值合理'
                item['risk_icon']  = '⚪'
        elif pe_skipped_reason:
            # 有 PE 跳过原因但 PB 也无数据（真次新股）
            item['risk_level'] = '缺乏历史'
            item['risk_icon']  = '⚪'
        elif market == 'HK' and item['current_pe'] is None:
            # 港股：百度接口未返回数据（非亏损/次新），标注「港股暂缺」
            item['risk_level'] = '港股暂缺'
            item['risk_icon']  = '🌐'
            item['note']       = '港股估值接口暂时无数据，请在港交所官网查询'

        results.append(item)


    return results


def plot_valuation_alert_chart(alert_data: list, stock_names: dict = None) -> go.Figure:
    """
    前十大重仓股估值分位数可视化（水平条形图）。

    alert_data: fetch_stock_valuation_alert() 返回的列表
    stock_names: {code: name} 映射表（可选）
    """
    if not alert_data:
        return go.Figure()

    # 只展示有有效数据的股票
    valid = [d for d in alert_data if d['pe_percentile'] is not None or d['pb_percentile'] is not None]
    if not valid:
        return go.Figure()

    codes    = []
    pe_pcts  = []
    pb_pcts  = []
    labels   = []
    colors_pe= []

    for d in valid:
        name  = (stock_names or {}).get(d['code'], d['name'] or d['code'])
        short = name[:6] if len(name) > 6 else name
        labels.append(f"{short}\n({d['code']})")
        codes.append(d['code'])
        pe_v = d['pe_percentile']
        pb_v = d['pb_percentile']
        pe_pcts.append(pe_v if pe_v is not None else 0)
        pb_pcts.append(pb_v if pb_v is not None else 0)

        # PE 颜色
        if pe_v is None:
            colors_pe.append('#cccccc')
        elif pe_v >= 85:
            colors_pe.append('#e74c3c')
        elif pe_v >= 70:
            colors_pe.append('#e67e22')
        elif pe_v <= 15:
            colors_pe.append('#27ae60')
        elif pe_v <= 30:
            colors_pe.append('#3498db')
        else:
            colors_pe.append('#95a5a6')

    fig = go.Figure()

    # PE 分位条
    fig.add_trace(go.Bar(
        y=labels,
        x=pe_pcts,
        name='PE历史分位',
        orientation='h',
        marker_color=colors_pe,
        text=[f"{v:.0f}%" if v else 'N/A' for v in pe_pcts],
        textposition='outside',
        hovertemplate='%{y}<br>PE分位: %{x:.1f}%<extra></extra>',
    ))

    # 警戒线
    fig.add_vline(x=85, line=dict(color='#e74c3c', dash='dash', width=1.5),
                  annotation_text='高估线(85%)', annotation_position='top right',
                  annotation_font_size=10)
    fig.add_vline(x=15, line=dict(color='#27ae60', dash='dash', width=1.5),
                  annotation_text='低估线(15%)', annotation_position='top right',
                  annotation_font_size=10)

    fig.update_layout(
        title=dict(text='前十大重仓股 PE 历史分位数（近五年）',
                   font=dict(size=13), x=0.5, xanchor='center'),
        xaxis=dict(range=[0, 110], title='历史分位数 (%)', showgrid=True,
                   gridcolor='#f0f2f5', ticksuffix='%'),
        yaxis=dict(title='', autorange='reversed'),
        height=max(280, len(valid) * 42 + 80),
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=100, r=60, t=45, b=30),
        showlegend=False,
        bargap=0.35,
    )

    return fig


# ------------------------------------------------------------------
# 模块 C：债券久期压力测试
# ------------------------------------------------------------------

def bond_stress_test(eff_duration: float, bond_weight: float,
                     bp_scenarios: list = None,
                     dur_short: float = 0.0,
                     dur_long: float = 0.0,
                     credit_beta: float = 0.0,
                     has_credit_factor: bool = False) -> dict:
    """
    债券压力测试（v10.0 升级：场景化矩阵）

    当有三因子数据时，使用分场景压测矩阵：
      场景A（资金面收紧）：短端Y2年上行50BP
      场景B（债市熊平）：  长端Y10年上行50BP
      场景C（信用风险）：  信用利差扩大20BP
      场景D（极端冲击）：  短端+长端同时上行100BP

    当仅有单因子数据时，退化为原有的3场景压测（10/50/100BP）

    公式：
      ΔP_short  = -dur_short × ΔY_short  × bond_weight
      ΔP_long   = -dur_long  × ΔY_long   × bond_weight
      ΔP_credit = credit_beta × ΔCS       × bond_weight（利差扩大对净值的影响）

    Args:
        eff_duration:      底层债券有效久期（总，年）
        bond_weight:       债券仓位权重
        bp_scenarios:      单因子模式下的压测BP列表
        dur_short:         短端久期贡献（三因子模式）
        dur_long:          长端久期贡献（三因子模式）
        credit_beta:       信用利差敏感度β（三因子模式）
        has_credit_factor: 是否有信用利差因子
    """
    _dur   = max(abs(eff_duration), 0.1)
    _wt    = max(min(bond_weight, 1.0), 0.0)
    _ds    = abs(dur_short)
    _dl    = abs(dur_long)

    use_three_factor = has_credit_factor or (_ds > 0.1 and _dl > 0.1)

    if use_three_factor:
        # ── 三因子场景化矩阵 ──
        scenarios = []

        scene_defs = [
            {
                'id': 'A',
                'name': '资金面收紧',
                'desc': '短端Y2↑50BP（央行收紧/跨季资金紧张）',
                'dy_short': 50,   # BP
                'dy_long':  0,
                'dcs':      0,
                'icon_pos': '🟠',
                'icon_neg': '🟢',
            },
            {
                'id': 'B',
                'name': '债市熊平',
                'desc': '长端Y10↑50BP（经济复苏预期/供给压力）',
                'dy_short': 0,
                'dy_long':  50,
                'dcs':      0,
                'icon_pos': '🟠',
                'icon_neg': '🟢',
            },
            {
                'id': 'C',
                'name': '信用风险爆发',
                'desc': '信用利差CS扩大20BP（违约担忧/流动性冲击）',
                'dy_short': 0,
                'dy_long':  0,
                'dcs':      20 if has_credit_factor else 0,
                'icon_pos': '🔴',
                'icon_neg': '🟢',
            },
            {
                'id': 'D',
                'name': '极端冲击（熊市）',
                'desc': '短端+长端同时↑100BP',
                'dy_short': 100,
                'dy_long':  100,
                'dcs':      0,
                'icon_pos': '🔴',
                'icon_neg': '🟢',
            },
        ]

        for sc in scene_defs:
            _dy_s = sc['dy_short'] / 10000.0    # BP→小数
            _dy_l = sc['dy_long']  / 10000.0
            _dcs  = sc['dcs']      / 10000.0

            # 各分量对基金净值的冲击（已含债券仓位）
            impact_short  = -_ds * _dy_s * _wt
            impact_long   = -_dl * _dy_l * _wt
            impact_credit = credit_beta * _dcs * _wt  # β_credit × ΔCS × 债券仓位
            impact_total  = impact_short + impact_long + impact_credit

            # 风险等级（按基金净值绝对影响）
            abs_impact = abs(impact_total)
            if abs_impact < 0.003:
                risk_level, risk_icon = '低', '🟢'
            elif abs_impact < 0.01:
                risk_level, risk_icon = '中', '🟡'
            elif abs_impact < 0.03:
                risk_level, risk_icon = '较高', '🟠'
            else:
                risk_level, risk_icon = '高', '🔴'

            # 如果该场景对应的因子不存在则标注「—」
            _skip = (sc['id'] == 'C' and not has_credit_factor)

            scenarios.append({
                'id':            sc['id'],
                'name':          sc['name'],
                'desc':          sc['desc'],
                'impact_short':  impact_short,
                'impact_long':   impact_long,
                'impact_credit': impact_credit,
                'fund_impact':   impact_total,
                'risk_level':    risk_level,
                'risk_icon':     risk_icon,
                'skip':          _skip,
                # 拆解用
                'dy_short_bp':   sc['dy_short'],
                'dy_long_bp':    sc['dy_long'],
                'dcs_bp':        sc['dcs'],
            })

        max_impact = min(sc['fund_impact'] for sc in scenarios)

        # 叙事：根据主敏感因子选描述
        if _ds > _dl * 1.5:
            fund_char = f'短端主导型（β_short={_ds:.2f}年 >> β_long={_dl:.2f}年），对资金面收紧最敏感'
        elif _dl > _ds * 1.5:
            fund_char = f'长端主导型（β_long={_dl:.2f}年 >> β_short={_ds:.2f}年），对经济预期和供给压力最敏感'
        else:
            fund_char = f'哑铃型（β_short={_ds:.2f}年 + β_long={_dl:.2f}年），两端利率均有暴露'

        narrative = (
            f'三因子场景分析：{fund_char}。'
            f'极端冲击（短端+长端均↑100BP）下，预计净值影响约 {max_impact*100:.2f}%。'
            + (f'信用利差扩大20BP影响约 {scenarios[2]["fund_impact"]*100:.2f}%。'
               if has_credit_factor else '')
        )

        return {
            'scenarios':           scenarios,
            'max_impact':          max_impact,
            'narrative':           narrative,
            'eff_duration':        _dur,
            'bond_weight':         _wt,
            'mode':                'three_factor',
            'dur_short':           _ds,
            'dur_long':            _dl,
            'credit_beta':         credit_beta,
            'has_credit_factor':   has_credit_factor,
        }

    # ── 单因子降级：原有3场景压测 ──
    if bp_scenarios is None:
        bp_scenarios = [10, 50, 100]

    scenarios = []
    for bp in bp_scenarios:
        dy          = bp / 10000.0
        bond_impact = -_dur * dy
        fund_impact = bond_impact * _wt

        if abs(fund_impact) < 0.003:
            risk_level, risk_icon = '低', '🟢'
        elif abs(fund_impact) < 0.01:
            risk_level, risk_icon = '中', '🟡'
        elif abs(fund_impact) < 0.03:
            risk_level, risk_icon = '较高', '🟠'
        else:
            risk_level, risk_icon = '高', '🔴'

        scenarios.append({
            'id':          str(bp),
            'name':        f'利率↑{bp}BP',
            'desc':        f'利率上行{bp}BP',
            'bp':          bp,
            'dy':          dy,
            'bond_impact': bond_impact,
            'fund_impact': fund_impact,
            'risk_level':  risk_level,
            'risk_icon':   risk_icon,
            'skip':        False,
        })

    max_impact = scenarios[-1]['fund_impact']

    if _wt < 0.20:
        _wt_desc = '债券仓位较低，利率风险对整体净值影响有限'
    elif _dur > 7:
        _wt_desc = f'久期偏长（{_dur:.1f}年），对利率上行极度敏感'
    elif _dur > 4:
        _wt_desc = f'久期中等（{_dur:.1f}年），利率风险处于可控范围'
    else:
        _wt_desc = f'久期较短（{_dur:.1f}年），对加息冲击不敏感'

    narrative = (
        f'当前有效久期 {_dur:.1f}年，债券仓位 {_wt*100:.0f}%。{_wt_desc}。'
        f'若利率上行100BP，预计基金净值影响约 {max_impact*100:.2f}%。'
    )

    return {
        'scenarios':   scenarios,
        'max_impact':  max_impact,
        'narrative':   narrative,
        'eff_duration': _dur,
        'bond_weight':  _wt,
        'mode':         'single_factor',
    }


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

        # ============ 一致性判定（新增）：Beta高 + Alpha低 = 无效加杠杆 ============
        consistency_warn = ''
        # 从 rolling_df 中取最新动态 equity_beta（如有）
        latest_dyn_beta = None
        if rolling_df is not None and not rolling_df.empty:
            col = 'equity_beta_20' if 'equity_beta_20' in rolling_df.columns else 'equity_beta'
            if col in rolling_df.columns and rolling_df[col].notna().any():
                latest_dyn_beta = rolling_df[col].dropna().iloc[-1]

        if latest_dyn_beta is not None:
            # 情形1：动态仓位很高（>0.85）但Alpha不显著或为负 → 无效加杠杆
            if latest_dyn_beta > 0.85 and (alpha_f < 0.02 or alpha_p > 0.1):
                consistency_warn = (
                    f"⚡ **一致性预警 · 无效加杠杆**：动态Beta估算约 {latest_dyn_beta*100:.0f}%，"
                    f"但年化Alpha仅 {alpha_f*100:.1f}%（p={alpha_p:.3f}，不显著）。"
                    f"经理满仓押注市场，但没有换来相应的超额收益——"
                    f"这是典型的「加了杠杆但没有Alpha」，风险/收益严重不对等。"
                )
            # 情形2：动态仓位低（<0.30）但因子模型算出高Alpha → 可能是选股能力集中在少数窗口
            elif latest_dyn_beta < 0.30 and alpha_f > 0.05 and alpha_p < 0.05:
                consistency_warn = (
                    f"💡 **一致性观察**：动态仓位仅约 {latest_dyn_beta*100:.0f}%（轻仓），"
                    f"但年化Alpha {alpha_f*100:.1f}%显著。"
                    f"说明经理用较少的股票仓位创造了较高的超额——"
                    f"选股精准度极高，或在特定时间窗口有集中获利。"
                )

        out['consistency_warn'] = consistency_warn

        # ============ 情绪指标（新增）：滚动Alpha连续3月下降 ============
        emotion_note = ''
        if fund_ret_for_trend is not None and bm_ret_for_trend is not None:
            trend_res = _calc_rolling_alpha_trend(fund_ret_for_trend, bm_ret_for_trend)
            emotion_note = trend_res.get('trend_text', '')
            out['_trend_data'] = trend_res  # 供展示层使用
        out['emotion_note'] = emotion_note

        # ============ 风险（定性结论，数字详情见 Part 2.5 风险提示板块）============
        risks = []
        if r2 > 0.9 and fee_total > 0.01:
            risks.append("管理费过高但实质上是伪指数基金，费效比严重失衡")
        if mkt_b > 1.3:
            risks.append("高Beta放大器——牛市超额赚，熊市超额亏，需严格控制仓位比例")
        if smb_b > 0.5:
            risks.append("小盘股集中持仓，流动性风险偏高，市场下行时可能形成踩踏")
        if consistency_warn and '无效加杠杆' in consistency_warn:
            risks.append("满仓运作但无Alpha保护——典型的「用风险换收益却没换到」")
        out['risk'] = '；'.join(risks) if risks else "风险特征正常，无明显异常。具体估值与压力数据见上方风险提示板块。"

        # ============ 建议 + 评分 ============
        if alpha_f > 0.05 and alpha_p < 0.05:
            out['advice'] = "经理实力经过统计验证，适合长期持有，可适当提高配置比例。"
            out['score'] = 80
            if emotion_note and '审美疲劳' in emotion_note:
                out['advice'] += "注意：近期滚动表现有下滑，建议观察后续1~2个季度确认趋势。"
                out['score'] = 72  # 小幅降分
        elif alpha_f > 0:
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

        # ============ 性格标签（bond三分类） ============
        bond_tags = []
        # 主标签
        if dur_underlying < 3 and carry > 0.02:
            bond_tags.append('🛡️ 防御专家')        # 短久期 + 有carry → 低风险稳健型
        elif dur_underlying >= 3 and carry > 0.03:
            bond_tags.append('💰 收益挖掘机')       # 中长久期 + 高carry → 信用下沉型
        elif dur_underlying > 6:
            bond_tags.append('⚔️ 利率博弈师')       # 长久期 → 纯利率赌注
        elif carry < 0.01:
            bond_tags.append('🧊 纯利率型')         # 低carry → 只做利率敞口
        else:
            bond_tags.append('⚖️ 均衡债基')         # 中等久期+合理carry

        if conv > 1.0:
            bond_tags.append('🛡️ 正凸性保护')
        elif conv < 0:
            bond_tags.append('⚠️ 负凸性风险')
        if carry > 0.04:
            bond_tags.append('🎣 信用下沉')

        out['tags'] = bond_tags

        # ============ 性格文本 ============
        if bond_ratio_used < 0.80:
            dur_note = f"（底层债券头寸真实久期，已修正仓位{bond_ratio_used*100:.0f}%）"
        else:
            dur_note = ''

        main_bond_tag = bond_tags[0] if bond_tags else ''
        if '防御专家' in main_bond_tag:
            out['character'] = (
                f"**{main_bond_tag}**。底层有效久期{dur_underlying:.1f}年{dur_note}，利率敏感度低；"
                f"综合carry {carry*100:.2f}%，在控制风险的同时保持稳定收益来源。"
                f"适合作为组合底仓的「压舱石」。"
            )
        elif '收益挖掘机' in main_bond_tag:
            out['character'] = (
                f"**{main_bond_tag}**。久期{dur_underlying:.1f}年{dur_note}，综合carry {carry*100:.2f}%偏高，"
                f"经理通过信用下沉（买中低评级债）挖掘额外收益。"
                f"收益可观，但信用风险和流动性风险不可忽视。"
            )
        elif '利率博弈师' in main_bond_tag:
            out['character'] = (
                f"**{main_bond_tag}**。底层久期{dur_underlying:.1f}年{dur_note}，对利率极度敏感——"
                f"利率下行1%净值约涨{dur_underlying:.0f}%，利率上行1%则反向损失。"
                f"本质是在「押注利率下行」，风险收益高度不对称。"
            )
        elif '纯利率型' in main_bond_tag:
            out['character'] = (
                f"**{main_bond_tag}**。久期{dur_underlying:.1f}年，carry几乎为零（{carry*100:.2f}%）。"
                f"收益完全依赖利率波动，无票息保护——利率不动就没有收益。"
            )
        else:
            out['character'] = (
                f"**{main_bond_tag}**。底层有效久期{dur_underlying:.1f}年{dur_note}，"
                f"综合carry {carry*100:.2f}%，风险收益均处于中等水平。"
            )

        style_bond = [t for t in bond_tags[1:]]
        if style_bond:
            out['character'] += f" | 附加标签：{'、'.join(style_bond)}。"

        # ============ 实力（carry + 凸性综合） ============
        carry_label = '偏高（信用/流动性风险需关注）' if carry > 0.04 else '合理' if carry > 0.015 else '偏低（信用成分极少）'
        out['skill'] = (
            f"年化综合carry（票息+信用溢价+骑乘收益）{carry*100:.2f}%，{carry_label}；"
            f"凸性 {conv:.1f}——{'正凸性，价格「涨得比跌得快」，有缓冲保护' if conv > 0 else '凸性偏低/负，利率大幅波动时缺乏缓冲保护'}。"
        )

        # ============ 风险（定性结论，具体数据见 Part 2.5 久期压力测试）============
        risks = []
        if dur_underlying > 7:
            risks.append("超长久期，利率小幅上行即可带来较大净值损失——降息周期的利器，加息周期的定时炸弹")
        if carry > 0.04:
            risks.append("高carry策略，信用溢价偏高，需防范信用违约和流动性收缩双重冲击")
        if conv < 0:
            risks.append("负凸性结构，利率大幅波动时缺乏自然缓冲，极端行情中的弱势品种")
        out['risk'] = '；'.join(risks) if risks else "债券风险特征正常，无明显异常。具体压力测试数据见上方风险提示板块。"

        # ============ 建议 + 评分 ============
        if carry > 0.02 and dur_underlying < 4 and conv >= 0:
            out['advice'] = "短久期+稳定carry，适合作为组合底仓，建议长期配置。"
            out['score'] = 75
        elif carry > 0.04:
            out['advice'] = "高carry策略，警惕信用下沉风险，建议控制仓位，加息周期避免重仓。"
            out['score'] = 60
        elif dur_underlying > 6:
            out['advice'] = "长久期赌利率，建议仅在降息周期配置，随时关注利率拐点。"
            out['score'] = 55
        else:
            out['advice'] = "适合中低风险偏好投资者，注意利率周期配置时机。"
            out['score'] = 65

    elif model == 'mixed':
        alloc      = results.get('allocation_effect', 0)
        sel_inter  = results.get('selection_inter_effect',
                                 results.get('selection_effect', 0))  # 兼容旧字段
        excess     = results.get('excess_return', 0)
        drift      = results.get('drift_info', {})

        # ============ 性格标签（mixed三分类） ============
        mixed_tags = []
        alloc_pos  = alloc > 0.005
        sel_pos    = sel_inter > 0.005
        alloc_dom  = abs(alloc) > abs(sel_inter)
        sel_dom    = abs(sel_inter) > abs(alloc)

        if alloc_pos and sel_pos:
            if alloc_dom:
                mixed_tags.append('🎯 择时达人')     # 配置为主 + 选股辅助
            else:
                mixed_tags.append('🔬 选股匠人')     # 选股为主 + 配置辅助
        elif alloc_pos and not sel_pos:
            mixed_tags.append('📡 择时驱动型')       # 配置挣钱但选股拖后腿
        elif sel_pos and not alloc_pos:
            mixed_tags.append('🎯 逆境选股型')       # 配置亏钱但选股救场
        elif not alloc_pos and not sel_pos:
            mixed_tags.append('😵 两头挨打型')       # 配置、选股双亏
        else:
            mixed_tags.append('⚖️ 均衡混合型')

        # 附加标签
        if abs(alloc) > 0.02 and abs(sel_inter) > 0.02:
            mixed_tags.append('🔥 双维度显著')
        if drift.get('has_drift'):
            mixed_tags.append('⚠️ 风格漂移')

        out['tags'] = mixed_tags
        main_mixed_tag = mixed_tags[0] if mixed_tags else ''

        # ============ 性格文本 ============
        if '择时达人' in main_mixed_tag:
            out['character'] = (
                f"**{main_mixed_tag}**。经理最强的牌是大类资产配置（择时站队）："
                f"配置效应{alloc*100:+.2f}%是超额的主要来源，选股也有正贡献（{sel_inter*100:+.2f}%）。"
                f"适合在市场大势不明朗时发挥，波动期表现更佳。"
            )
        elif '选股匠人' in main_mixed_tag:
            out['character'] = (
                f"**{main_mixed_tag}**。经理最强的牌是个股挑选："
                f"选择效应{sel_inter*100:+.2f}%是超额的核心来源，"
                f"配置也有正贡献（{alloc*100:+.2f}%）。"
                f"在各种市场环境下选好票——这是稀缺的真本事。"
            )
        elif '择时驱动型' in main_mixed_tag:
            out['character'] = (
                f"**{main_mixed_tag}**。配置效应{alloc*100:+.2f}%，站队准确；"
                f"但选股效应{sel_inter*100:+.2f}%为负，具体标的拖了后腿。"
                f"经理能预判大势，但落地选股能力有待提升。"
            )
        elif '逆境选股型' in main_mixed_tag:
            out['character'] = (
                f"**{main_mixed_tag}**。选股效应{sel_inter*100:+.2f}%，个股选得好；"
                f"但配置效应{alloc*100:+.2f}%为负，大类择时失误拖累。"
                f"在仓位配置不利的情况下，靠选股能力硬撑——是「逆风飞翔」型选手。"
            )
        elif '两头挨打' in main_mixed_tag:
            out['character'] = (
                f"**{main_mixed_tag}** 😵。配置效应{alloc*100:+.2f}%，选择效应{sel_inter*100:+.2f}%，"
                f"两条腿都在赔钱。既没有择时站对队，也没有挑到好标的，"
                f"本期混合策略未能创造价值，建议持续观察并考虑替代选择。"
            )
        else:
            out['character'] = (
                "**混合型**，同时暴露股债两类风险，超额收益来自「站队」（配置）和「挑货」（选股）两条路。"
            )

        if len(mixed_tags) > 1:
            out['character'] += f" | 附加标签：{'、'.join(mixed_tags[1:])}。"

        # ============ 实力 ============
        out['skill'] = (
            f"配置效应（站队）{alloc*100:+.2f}%，选择效应（挑货）{sel_inter*100:+.2f}%；"
            f"总超额{excess*100:+.2f}%。"
        )
        # 特殊情况深度诊断
        if alloc > 0.01 and sel_inter < -0.01:
            out['skill'] += (
                "⚡ 「择时准但选股弱」：经理能预判大势，但具体标的拖了后腿——"
                "考虑是否在买票环节存在流动性限制或分散化执行问题。"
            )
        elif alloc < -0.01 and sel_inter > 0.01:
            out['skill'] += (
                "⚡ 「择时弱但选股强」：大类配置失误，但个股挑选能力可圈可点——"
                "如果经理能改善择时，整体表现会明显提升。"
            )
        elif not alloc_pos and not sel_pos and excess < -0.02:
            out['skill'] += "⚠️ 双维度均为负，本期主动管理全面落后于基准，需关注是否为系统性原因。"

        # ============ 风险（定性结论，具体数据见 Part 2.5）============
        drift_msg = drift.get('message', '')
        if drift_msg:
            out['risk'] = drift_msg
        elif excess < 0:
            _main_cause = (
                '配置择时失误是主因' if abs(alloc) > abs(sel_inter) and alloc < 0
                else '选股能力不足是主因' if sel_inter < 0
                else '配置与选股双向拖累'
            )
            out['risk'] = f"本期超额为负，{_main_cause}，需持续跟踪改善情况。详细归因见上方风险提示板块。"
        else:
            out['risk'] = "股债配置均衡，无明显风格漂移预警。详细估值与压测数据见上方风险提示板块。"

        # ============ 建议 + 评分 ============
        if alloc_pos and sel_pos and excess > 0.02:
            out['advice'] = "双轮驱动表现优秀，建议持有。配置+选股均贡献正超额，是混合型基金中的优质标的。"
            out['score'] = 78
        elif excess > 0:
            out['advice'] = "有超额但单轮驱动，建议持有并关注基金季报了解最新仓位动向。"
            out['score'] = 65
        elif '两头挨打' in main_mixed_tag:
            out['advice'] = "配置、选股双亏，建议降低仓位，寻找替代选择。持续持有需有明确的逻辑支撑。"
            out['score'] = 42
        else:
            out['advice'] = "超额不明显，适合中等风险偏好，建议关注基金季报了解最新仓位动向。"
            out['score'] = 55

    elif model == 'sector':
        # 读取 sector 子结果（M4已独立计算，results['sector'] 中）
        sr     = results.get('sector', results)   # 兼容两种传入方式
        na     = sr.get('neutral_alpha', 0)
        te     = sr.get('tracking_error', 0)
        ir     = sr.get('info_ratio', 0)
        sw_nm  = sr.get('sw_name', '')
        bm_src = sr.get('bm_source', '行业基准')

        # ============ 性格标签（sector四象限） ============
        sec_tags = []
        # 横轴：Alpha高低（0.05为界）
        # 纵轴：IR高低（0.7为界）
        # 象限1：高Alpha + 高IR → 赛道霸主（量又大质又好）
        # 象限2：高Alpha + 低IR → 单押赌博（高alpha但靠集中押注赌出来）
        # 象限3：低Alpha + 高IR → 行业潜水员（稳定跟踪，增强指数型）
        # 象限4：低Alpha + 低IR → 行业随从（没有真Alpha，跟行业指数吃饭）

        if na > 0.05 and ir >= 0.7:
            sec_tags.append('👑 赛道霸主')
        elif na > 0.05 and ir < 0.7:
            sec_tags.append('🎲 单押赌博型')   # 高Alpha但高TE低IR → 靠集中度赌
        elif na > 0.02 and ir >= 0.5:
            sec_tags.append('🔍 精准潜水员')   # 低Alpha但IR高 → 稳定有效的小额超额
        elif na > 0 and ir < 0.5:
            sec_tags.append('🌊 行业随从')     # 微弱Alpha + 低IR
        elif te > 0.15:
            sec_tags.append('🎢 高集中押注型')  # 极高TE，主要特征是偏离
        else:
            sec_tags.append('🪞 行业影子')     # 紧跟行业指数

        # 附加标签
        if te < 0.04:
            sec_tags.append('📊 增强指数风格')
        if na < 0:
            sec_tags.append('💸 选股拖累')

        out['tags'] = sec_tags
        main_sec_tag = sec_tags[0] if sec_tags else ''

        # ============ 性格文本 ============
        if '赛道霸主' in main_sec_tag:
            out['character'] = (
                f"**{main_sec_tag}** 👑。在{'「'+sw_nm+'」' if sw_nm else '行业'}内部有统治级别的选股能力："
                f"年化Alpha {na*100:.1f}%，信息比率IR={ir:.2f}——不仅能赚超额，还赚得高效稳定。"
                f"这是行业主题基金中最理想的形态。"
            )
        elif '单押赌博' in main_sec_tag:
            out['character'] = (
                f"**{main_sec_tag}** 🎲。Alpha看起来不错（{na*100:.1f}%），"
                f"但信息比率只有IR={ir:.2f}，跟踪误差高达{te*100:.1f}%。"
                f"这意味着超额收益是靠**极高的个股集中度「赌」出来的**——"
                f"赢了是神，输了是坑。"
            )
        elif '精准潜水员' in main_sec_tag:
            out['character'] = (
                f"**{main_sec_tag}**。Alpha适中（{na*100:.1f}%），IR={ir:.2f}，"
                f"稳扎稳打在{'「'+sw_nm+'」' if sw_nm else '行业'}内部做小额持续超额，"
                f"是「增强型指数基金」的典型特征。"
            )
        elif '行业随从' in main_sec_tag:
            out['character'] = (
                f"**{main_sec_tag}**。Alpha仅{na*100:.1f}%，IR={ir:.2f}偏低，"
                f"主要靠{'「'+sw_nm+'」' if sw_nm else '行业'}整体上涨赚钱，"
                f"经理的选股能力没有显著体现——买行业ETF可能更划算。"
            )
        elif '高集中押注' in main_sec_tag:
            out['character'] = (
                f"**{main_sec_tag}**。跟踪误差{te*100:.1f}%极高，"
                f"经理在{'「'+sw_nm+'」' if sw_nm else '行业'}内部做了高度集中的细分赛道押注。"
                f"波动剧烈，Alpha={na*100:.1f}%，需谨慎评估集中度风险。"
            )
        else:
            out['character'] = (
                f"**行业影子**（跟踪误差{te*100:.1f}%极低）。"
                f"经理紧跟行业指数，主动管理痕迹很少，更像指数基金。"
            )

        if len(sec_tags) > 1:
            out['character'] += f" | 附加标签：{'、'.join(sec_tags[1:])}。"

        # ============ 实力 ============
        if na > 0.08 and ir > 1.0:
            out['skill'] = (
                f"「窝里横」能力卓越💎：{'申万'+sw_nm+'内' if sw_nm else '行业内'}年化Alpha {na*100:.1f}%，"
                f"信息比率IR={ir:.2f}，选股又准又稳，是真正的行业内超额。"
            )
        elif na > 0.03 and ir > 0.5:
            # 与表格「✅具备选股能力」对齐
            out['skill'] = (
                f"具备行业内选股能力✅：Alpha {na*100:.1f}%，"
                f"信息比率IR={ir:.2f}（效率达标），在{'申万'+sw_nm if sw_nm else '行业'}内选股占优。"
            )
        elif na > 0.03:
            # Alpha>3%但IR≤0.5：与性格诊断「行业随从」对齐，不说「具备选股」
            out['skill'] = (
                f"Alpha {na*100:.1f}%略有超额，但信息比率IR={ir:.2f}偏低，"
                f"超额收益不稳定——主要是行业整体Beta赚钱，经理选股贡献有限。"
            )
        elif na > 0:
            out['skill'] = (
                f"行业内Alpha微弱（{na*100:.1f}%），"
                f"扣除费率后优势可能消失，建议对比同类ETF。"
            )
        else:
            out['skill'] = (
                f"⚠️ 行业内Alpha为负（{na*100:.1f}%）——"
                f"经理没跑赢{'申万'+sw_nm+'指数' if sw_nm else '行业基准'}，"
                f"买对应的行业ETF更划算。"
            )

        # ============ 风险（定性结论，具体指标见 Part 2.5）============
        risks = []
        risks.append(
            f"行业集中度高，需承担完整的{'「'+sw_nm+'」' if sw_nm else '特定行业'}系统性风险——行业景气下行时无分散保护"
        )
        if '单押赌博' in main_sec_tag:
            risks.append(
                "🎲 **性价比预警**：表面Alpha不错，但来自于对少数个股的集中押注——"
                "赢了是神，输了是坑。这类Alpha的可持续性极低，持有者需有承受极端行情的心理准备。"
            )
        if te > 0.12:
            risks.append("跟踪误差极高，个股集中度风险大，实际波动可能远超行业指数")
        out['risk'] = '；'.join(risks)

        # ============ 建议 + 评分 ============
        if na > 0.05 and ir > 0.7:
            out['advice'] = (
                f"经理在行业内具备真实Alpha，适合对{'「'+sw_nm+'」' if sw_nm else '该行业'}有判断、"
                f"且能承受行业集中风险的投资者。建议持有并观察行业景气度变化。"
            )
            out['score'] = 82
        elif '单押赌博' in main_sec_tag:
            out['advice'] = (
                "高Alpha但靠集中押注获得，可持续性存疑。"
                "建议控制仓位（≤10%），并在季报更新后评估持仓是否仍集中。"
            )
            out['score'] = 62
        elif na > 0:
            out['advice'] = (
                "超额不够显著，行业择时能力更关键。"
                "建议将该基金作为卫星仓位（<20%），不适合作为核心配置。"
            )
            out['score'] = 58
        else:
            out['advice'] = (
                f"建议优先考虑低费率的{'「'+sw_nm+'」' if sw_nm else '同行业'}ETF。"
                "若坚持持有，需明确理由：是看好经理、还是看好行业？"
            )
            out['score'] = 42

    return out


# ============================================================
# ██████████████  VISUALIZATION LAYER  ██████████████
# ============================================================

def plot_cumulative_return(nav_df: pd.DataFrame, bm_df: pd.DataFrame,
                           bm_label: str = '业绩基准') -> go.Figure:
    """
    累计收益对比图（基金 vs 基准）
    修复：两条线必须从共同起点出发（inner join 对齐日期），否则基准会偏移显示
    bm_label: 基准标签名（默认'业绩基准'，行业基金可传'申万行业指数'）
    """
    fig = go.Figure()

    nav = nav_df[['date', 'ret']].copy()
    nav['date'] = pd.to_datetime(nav['date'])

    if not bm_df.empty and 'bm_ret' in bm_df.columns:
        bm = bm_df[['date', 'bm_ret']].copy()
        bm['date'] = pd.to_datetime(bm['date'])

        # ── 关键修复：inner join 对齐日期 ──
        # 两条线必须共享同一套日期序列，才能"从同一个 0% 起点出发"进行公平对比
        merged = nav.merge(bm, on='date', how='inner').sort_values('date')
        if merged.empty:
            # 无共同日期时回退：独立画
            nav['cum'] = (1 + nav['ret'].fillna(0)).cumprod() - 1
            fig.add_trace(go.Scatter(x=nav['date'], y=nav['cum'] * 100,
                                     name='基金净值', line=dict(color='#e74c3c', width=2)))
        else:
            # 用 cumprod 保证两条线都从 0 出发
            cum_fund = (1 + merged['ret'].fillna(0)).cumprod() - 1
            cum_bm   = (1 + merged['bm_ret'].fillna(0)).cumprod() - 1
            fig.add_trace(go.Scatter(
                x=merged['date'], y=cum_fund * 100,
                name='基金净值', line=dict(color='#e74c3c', width=2)
            ))
            fig.add_trace(go.Scatter(
                x=merged['date'], y=cum_bm * 100,
                name=bm_label, line=dict(color='#3498db', width=2, dash='dash')
            ))
    else:
        # 无基准数据，只画基金
        nav['cum'] = (1 + nav['ret'].fillna(0)).cumprod() - 1
        fig.add_trace(go.Scatter(x=nav['date'], y=nav['cum'] * 100,
                                 name='基金净值', line=dict(color='#e74c3c', width=2)))

    fig.update_layout(
        title='累计收益率对比 (%)',
        xaxis_title='', yaxis_title='累计收益率 (%)',
        plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(x=0.01, y=0.99),
        height=380, margin=dict(l=40, r=20, t=40, b=30)
    )
    return fig



def plot_rolling_beta(rolling_df: pd.DataFrame, static_ratio: float) -> go.Figure:
    """
    双窗口滚动仓位监控图（20日 + 60日）

    双曲线解读规则：
      - 两条线高度重合 → 结论极度可靠，信号稳定
      - 20日线剧烈波动、60日线平稳 → 近期数据有噪音，参考60日为主
      - 两条线同步趋势变化 → 真实仓位调整，置信度高

    R² 可信度：
      ≥0.80  🟢 信号可信
      0.50~0.80  🟡 参考价值一般
      <0.50  🔴 数据噪音较大
    """
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.72, 0.28],
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=['动态股票仓位估算（双窗口）', '模型解释度 R²（信号可信度）']
    )

    # ---- 主图：双条仓位曲线 ----
    if 'equity_beta_20' in rolling_df.columns:
        fig.add_trace(go.Scatter(
            x=rolling_df['date'], y=rolling_df['equity_beta_20'],
            name='20日动态仓位', line=dict(color='#e74c3c', width=2),
            hovertemplate='20日仓位: %{y:.1%}<extra></extra>'
        ), row=1, col=1)

    if 'equity_beta_60' in rolling_df.columns and rolling_df['equity_beta_60'].notna().any():
        fig.add_trace(go.Scatter(
            x=rolling_df['date'], y=rolling_df['equity_beta_60'],
            name='60日动态仓位', line=dict(color='#8e44ad', width=2, dash='dot'),
            hovertemplate='60日仓位: %{y:.1%}<extra></extra>'
        ), row=1, col=1)

    # 季报静态仓位参考线
    fig.add_hline(y=static_ratio, line_dash='dash', line_color='#3498db',
                  annotation_text=f'季报仓位 {static_ratio*100:.0f}%',
                  annotation_position='bottom right', row=1, col=1)
    # ±15% 预警带
    fig.add_hline(y=min(static_ratio + 0.15, 1.0),
                  line_dash='dot', line_color='#e67e22', line_width=1,
                  opacity=0.5, row=1, col=1)
    fig.add_hline(y=max(static_ratio - 0.15, 0.0),
                  line_dash='dot', line_color='#e67e22', line_width=1,
                  opacity=0.5, row=1, col=1)

    # ---- 副图：R² 可信度 ----
    if 'r2_20' in rolling_df.columns:
        r2_vals = rolling_df['r2_20']
        # 颜色按区间：≥0.8绿，0.5~0.8黄，<0.5红
        r2_colors = ['#27ae60' if v >= 0.8 else '#e67e22' if v >= 0.5 else '#e74c3c'
                     for v in r2_vals.fillna(0)]
        fig.add_trace(go.Bar(
            x=rolling_df['date'], y=r2_vals,
            name='R²（20日）', marker_color=r2_colors,
            hovertemplate='R²: %{y:.3f}<extra></extra>',
            showlegend=False
        ), row=2, col=1)
        # R²=0.8参考线
        fig.add_hline(y=0.8, line_dash='dash', line_color='#27ae60',
                      line_width=1, opacity=0.6, row=2, col=1)

    fig.update_layout(
        height=430,
        plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(x=0.01, y=0.99, font=dict(size=11)),
        margin=dict(l=40, r=20, t=40, b=30),
        hovermode='x unified'
    )
    fig.update_yaxes(tickformat='.0%', row=1, col=1, range=[-0.05, 1.15])
    fig.update_yaxes(tickformat='.2f', row=2, col=1, range=[0, 1.05],
                     title_text='R²')
    return fig


def plot_factor_bar(betas: dict) -> go.Figure:
    """因子暴露度条形图"""
    labels = {'Mkt': '市场因子', 'SMB': '规模因子（小盘+）',
              'HML': '价值因子（价值+）', 'Short_MOM': '短期动量因子'}
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
  <p>买基之前搜一搜</p>
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
    with st.spinner("⏳ 获取基金信息..."):
        basic = fetch_basic_info(fund_code)

    if basic['name'] == fund_code:
        st.error("基金信息获取失败，请检查代码是否正确。")
        return

    # 货币基金：跳过量化分析，显示友好提示
    if basic['type_category'] == 'money':
        st.info(f"🪙 **这是货币基金**（{basic['type_raw']}）。\n\n"
                "货币基金主要投资短期银行存款、同业存单等现金类资产，每日收益波动极小，"
                "不适用于股票/债券量化模型分析。\n\n"
                "如需查看该基金的7日年化收益率、万份收益等指标，请参考第三方基金平台。")
        return

    # ============================================================
    # STEP 2  净值历史
    # ============================================================
    # 解析时间区间
    _period_year_map = {'1年': 1, '3年': 3, '5年': 5, '10年': 10}
    _since_inception = (period_sel == '自成立以来')
    _since_manager   = (period_sel == '现任经理以来')
    _years = _period_year_map.get(period_sel, 5)

    with st.spinner("⏳ 获取收益数据..."):
        nav_df = fetch_nav(
            fund_code,
            years=_years,
            since_inception=_since_inception,
            manager_start=basic.get('manager_start_date', '') if _since_manager else ''
        )

    if nav_df.empty or len(nav_df) < 20:
        st.error("历史净值数据严重不足（<20天），无法进行量化分析。请确认基金代码正确，或稍后重试。")
        return

    # 次新基金提示（20~60天数据，模型精度有限但可展示基础信息）
    _is_new_fund = len(nav_df) < 60
    if _is_new_fund:
        st.warning(f"⏳ **次新基金**：净值历史仅 {len(nav_df)} 天，量化模型精度有限，结论仅供参考。")

    start_str = nav_df['date'].min().strftime('%Y%m%d')
    end_str   = nav_df['date'].max().strftime('%Y%m%d')

    # ============================================================
    # STEP 3+4  持仓 & 基准构建（并行）
    # ============================================================
    # build_benchmark_ret 需要 start_str/end_str，但不依赖持仓，两者可并行
    parsed_bm = basic['benchmark_parsed']
    if not parsed_bm:
        defaults = {
            'equity': {'type':'single','components':[{'name':'沪深300','code':'sh000300','weight':1.0}]},
            'bond':   {'type':'single','components':[{'name':'中债综合','code':None,'weight':1.0}]},
            'mixed':  {'type':'custom','components':[
                {'name':'沪深300','code':'sh000300','weight':0.6},
                {'name':'中债综合','code':None,'weight':0.4}]},
            'sector': {'type':'single','components':[{'name':'沪深300','code':'sh000300','weight':1.0}]},
            'index':  {'type':'single','components':[{'name':'沪深300','code':'sh000300','weight':1.0}]},
            'qdii':   {},
        }
        parsed_bm = defaults.get(basic['type_category'], defaults['equity'])

    with st.spinner("⏳ 分析持仓..."):
        holdings = fetch_holdings(fund_code, basic['type_category'])

    with st.spinner("⏳ 计算基准..."):
        if parsed_bm:
            bm_df = build_benchmark_ret(parsed_bm, start_str, end_str)
        else:
            bm_df = pd.DataFrame(columns=['date', 'bm_ret'])

    stock_ratio = holdings.get('stock_ratio', 0.8)
    bond_ratio  = holdings.get('bond_ratio',  0.1)

    # 债券持仓穿透分析（bond/mixed 基金才做）
    bond_structure = {}
    if basic['type_category'] in ('bond', 'mixed'):
        _bh_df = holdings.get('bond_holdings', pd.DataFrame())
        if not _bh_df.empty:
            bond_structure = analyze_bond_structure(_bh_df)

    # 逻辑网关（漏斗式，返回 tuple）
    _convertible_ratio = holdings.get('convertible_ratio', 0.0)
    model_type, _gateway_warn = detect_fund_model(
        stock_ratio, basic['type_category'], _convertible_ratio
    )

    # 债券仓位超过 20% 时同时引入久期归因
    also_duration = (model_type in ('equity','mixed') and bond_ratio > 0.20)

    _sector_bm_label = ''   # 行业基金申万基准标签，空=未覆写（STEP 5 sector 分支会更新）

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

        # 行业主题：精准行业基准 Alpha 分析
        if model_type == 'sector':
            fund_ret_s = nav_df.set_index('date')['ret'].dropna()
            bm_ret_s   = bm_df.set_index('date')['bm_ret'].dropna() if not bm_df.empty else pd.Series([], dtype=float)

            # 自动识别申万一级行业
            sw_code, sw_name = detect_sw_industry(
                fund_name=basic.get('name', ''),
                sector_weights=holdings.get('sector_weights', {})
            )
            sw_ret = pd.Series(dtype=float)
            if sw_code:
                with st.spinner(f"获取申万{sw_name}行业指数（精准基准）..."):
                    sw_ret = fetch_sw_industry_ret(sw_code, start_str, end_str)

            sector_res = run_sector_model(
                fund_ret=fund_ret_s,
                bm_ret=bm_ret_s,
                sw_industry_ret=sw_ret if not sw_ret.empty else None,
                sw_industry_name=sw_name,
                fund_name=basic.get('name', '')
            )
            sector_res['sw_code'] = sw_code
            model_results['sector'] = sector_res

            # ── 行业基金基准修复：用申万行业指数覆写 bm_df 供累计收益图使用 ──
            # 原 bm_df 是沪深300，行业大涨时会造成基准严重失真
            # 只在成功拉到申万指数时覆写，失败时保留沪深300作兜底
            if not sw_ret.empty:
                sw_bm_df = sw_ret.reset_index()
                sw_bm_df.columns = ['date', 'bm_ret']
                sw_bm_df['date'] = pd.to_datetime(sw_bm_df['date'])
                bm_df = sw_bm_df   # 覆写，后续图表和标注都用申万指数
                _sector_bm_label = f'申万{sw_name}指数（精准行业基准）'
            else:
                _sector_bm_label = ''   # 空 = 未覆写，展示区继续用招募说明书基准文本



    # ----- 债券模型 -----
    if model_type == 'bond' or also_duration:
        # 把季报债券穿透结果注入 holdings，让 _convexity_from_holdings 能读到真实利率债/信用债比例
        if bond_structure:
            holdings['gov_bond_ratio']    = bond_structure.get('rate_ratio', None) * \
                                            bond_structure.get('total_weight', 0) / 100
            holdings['credit_bond_ratio'] = (bond_structure.get('credit_ratio', 0) +
                                             bond_structure.get('convert_ratio', 0)) * \
                                            bond_structure.get('total_weight', 0) / 100

        with st.spinner("⏳ 计算债券收益..."):
            treasury     = fetch_treasury_10y(start_str, end_str)
            three_factor = fetch_bond_three_factors(start_str, end_str)

        if not treasury.empty:
            fund_ret_s = nav_df.set_index('date')['ret'].dropna()
            # v10.0：传入三因子数据 + holdings（用于凸性静态穿透）
            bond_res = run_duration_model(
                fund_ret_s, treasury,
                bond_ratio=bond_ratio,
                three_factor_df=three_factor if not three_factor.empty else None,
                holdings=holdings
            )
        else:
            bond_res = {
                'duration': 5.0, 'duration_underlying': 5.0,
                'dur_short': 0.0, 'dur_long': 5.0,
                'p_short': 1.0, 'p_long': 1.0,
                'credit_beta': 0.0, 'p_credit': 1.0, 'has_credit_factor': False,
                'convexity': 0.0, 'convexity_quality': 'unknown',
                'carry_alpha': 0.0, 'credit_spread_alpha': 0.0,
                'r_squared': 0.0, 'r_squared_adj': 0.0,
                'factor_mode': 'fallback（数据获取失败）',
                'bond_ratio_used': bond_ratio, 'duration_source': 'default',
                'fund_type_note': '',
                'interpretation': '国债收益率数据获取失败'
            }
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
    # 准备情绪指标所需的基金/基准收益率序列
    _fund_ret_trend = nav_df.set_index('date')['ret'].dropna() if 'ret' in nav_df.columns else None
    _bm_ret_trend   = bm_df.set_index('date')['bm_ret'].dropna() if not bm_df.empty else None
    _rolling_df_for_translate = model_results.get('rolling_df', None)

    # ── Bond 模式特殊处理：translate_results('bond') 读的是展平字段
    #    但 model_results = {'bond': bond_res}，需要展开 bond 子字典传入
    _translate_results = model_results
    if model_type == 'bond' and 'bond' in model_results:
        _translate_results = model_results['bond']

    translation = translate_results(
        model_type, _translate_results, basic, holdings,
        rolling_df=_rolling_df_for_translate,
        bm_ret_for_trend=_bm_ret_trend,
        fund_ret_for_trend=_fund_ret_trend
    )

    # ============================================================
    # DISPLAY
    # ============================================================

    # ---------- Part 0: 综合实力雷达图 ----------
    st.markdown('<div class="section-title">🎯 综合实力透视图</div>', unsafe_allow_html=True)

    # 计算5维评分
    _rolling_for_radar = model_results.get('rolling_df', None)
    # bond 模式需要展开子字典（与 translate_results 保持一致）
    _radar_model_results = model_results
    if model_type == 'bond' and 'bond' in model_results:
        _radar_model_results = model_results['bond']
    _radar_scores = calc_radar_scores(
        model_type, _radar_model_results, nav_df, bm_df, _rolling_for_radar,
        fund_type_category=basic['type_category']
    )
    _radar_meta = _radar_scores.get('_meta', {})
    _radar_weights = _radar_scores.get('_weights', {})

    # 加权平均综合分（用于 Part 4 统一评分）
    _weighted_avg_score = sum(
        _radar_scores.get(k, 50) * _radar_weights.get(k, 0.2)
        for k in ['超额能力', '风险控制', '性价比', '风格稳定', '业绩持续']
    )

    # 覆盖 translation 的 score 为加权平均分
    translation['score'] = round(_weighted_avg_score)

    # 雷达图 + 五维说明卡并排
    _rc1, _rc2 = st.columns([1.1, 1])

    with _rc1:
        fig_radar = plot_fund_radar(basic['name'], _radar_scores)
        st.plotly_chart(fig_radar, use_container_width=True)

    with _rc2:
        # 5维评分卡片 — 小字根据得分动态生成定性评价
        def _score_label(score: int, dim: str) -> str:
            """根据得分和维度生成人话评价（替代固定小字）"""
            if dim == '超额能力':
                if score >= 80: return '✦ 超额显著，真本事赚钱'
                if score >= 60: return '↑ 有一定超额，但不够稳定'
                if score >= 40: return '→ 超额微弱，主要靠市场'
                return '↓ 几乎无超额，被动持有更划算'
            elif dim == '风险控制':
                if score >= 80: return '✦ 回撤小、波动低，防守能力强'
                if score >= 60: return '↑ 波动中等，下行控制尚可'
                if score >= 40: return '→ 波动较大，持有体验一般'
                return '↓ 回撤深/波动高，大幅震荡时压力大'
            elif dim == '性价比':
                if score >= 80: return '✦ 每单位风险回报突出，高效赚钱'
                if score >= 60: return '↑ 风险收益比合理，值得持有'
                if score >= 40: return '→ 冒了较大风险，超额收益不够匹配'
                return '↓ 风险回报效率偏低'
            elif dim == '风格稳定':
                if score >= 80: return '✦ 风格高度一致，说到做到'
                if score >= 60: return '↑ 风格基本稳定，偶有偏移'
                if score >= 40: return '→ 风格存在一定漂移，需关注'
                return '↓ 风格不稳，当心名不副实'
            else:  # 业绩持续
                if score >= 80: return '✦ 胜率高、盈亏比优，常胜将军'
                if score >= 60: return '↑ 业绩较持续，偶有落后期'
                if score >= 40: return '→ 胜率/盈亏比一般，表现时好时坏'
                return '↓ 业绩缺乏持续性，可能靠运气'

        _dim_info = [
            ('超额能力', '超额能力',
             f"年化超额 {_radar_meta.get('alpha', 0)*100:.1f}%"),
            ('风险控制', '风险控制',
             f"最大回撤 {_radar_meta.get('max_dd', 0)*100:.1f}% · 波动率 {_radar_meta.get('vol', 0)*100:.1f}%"),
            ('性价比', '性价比',
             f"夏普比率 {_radar_meta.get('sharpe', 0):.2f} · 信息比率 {_radar_meta.get('ir', 0):.2f}"),
            ('风格稳定', '风格稳定',
             '模型解释度 + 市场敏感度'),
            ('业绩持续', '业绩持续',
             f"胜率 {_radar_meta.get('win_rate', 0)*100:.0f}% · 盈亏比 {_radar_meta.get('plr', 0):.2f}"),
        ]

        st.markdown('<div style="font-size:0.82rem;color:#888;margin-bottom:8px">📊 各维度得分详解（满分100）</div>',
                    unsafe_allow_html=True)

        for _dkey, _dlabel, _metric in _dim_info:
            _dscore = _radar_scores.get(_dkey, 50)
            _ddesc  = _score_label(_dscore, _dkey)   # 动态定性评价
            # 颜色语义化：≥80 绿色优秀 / 60-79 橙色及格 / <60 红色警告
            _bar_color = '#27ae60' if _dscore >= 80 else '#e67e22' if _dscore >= 60 else '#e74c3c'
            _bar_pct = _dscore  # 0-100
            st.markdown(f"""
<div style="background:white;border-radius:8px;padding:10px 14px;margin-bottom:7px;
     box-shadow:0 1px 4px rgba(0,0,0,.06);border-left:3px solid {_bar_color}">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
    <span style="font-size:0.85rem;font-weight:600;color:#333">{_dlabel}</span>
    <span style="font-size:1.1rem;font-weight:700;color:{_bar_color}">{_dscore}</span>
  </div>
  <div style="background:#f0f2f5;border-radius:4px;height:5px;margin-bottom:5px">
    <div style="background:{_bar_color};width:{_bar_pct}%;height:5px;border-radius:4px;transition:width .4s"></div>
  </div>
  <div style="font-size:0.75rem;color:{_bar_color};font-weight:500">{_metric}</div>
  <div style="font-size:0.72rem;color:#aaa;margin-top:2px">{_ddesc}</div>
</div>
""", unsafe_allow_html=True)

        # 快速结论
        _weights = _radar_scores.get('_weights', {})
        # 综合均分：加权平均（根据基金类型权重）
        _avg_r = sum(_radar_scores.get(k, 50) * _weights.get(k, 0.2) for k in ['超额能力', '风险控制', '性价比', '风格稳定', '业绩持续'])
        _weak_dims = [k for k in ['超额能力', '风险控制', '性价比', '风格稳定', '业绩持续']
                      if _radar_scores.get(k, 50) < 45]
        _strong_dims = [k for k in ['超额能力', '风险控制', '性价比', '风格稳定', '业绩持续']
                        if _radar_scores.get(k, 50) >= 75]

        if _strong_dims and not _weak_dims:
            _shape_type = '🔵 全能均衡型' if len(_strong_dims) >= 4 else f'⚡ 优势明显（{"/".join(_strong_dims[:2])}强）'
        elif _weak_dims and not _strong_dims:
            _shape_type = f'⚠️ 偏科生（{"/".join(_weak_dims[:2])}偏弱）'
        elif _strong_dims and _weak_dims:
            _shape_type = f'🎯 典型尖刺型（强：{_strong_dims[0]}，弱：{_weak_dims[0]}）'
        else:
            _shape_type = '📊 整体居中，均衡偏弱'

        # 综合均分解读（重点解释为何有优势维度但总分仍低）
        if _avg_r >= 75:
            _avg_comment = '整体较优秀，各维度均衡。'
        elif _avg_r >= 55:
            _avg_comment = '整体中等水准。'
        else:
            # 总分低时，明确指出是哪个维度拖了后腿
            if _weak_dims:
                _drag_dim = min(_weak_dims, key=lambda k: _radar_scores.get(k, 50))
                _drag_score = _radar_scores.get(_drag_dim, 0)
                _weight = _weights.get(_drag_dim, 0.2)
                _avg_comment = (
                    f'总分按基金类型权重计算。即使某维度有优势，'
                    f'<b>「{_drag_dim}」仅{_drag_score}分</b>（权重{int(_weight*100)}%）严重拖低综合分——'
                    f'这代表该基金"能赚钱但赚得险"或"超额有限"，综合性价比偏低。'
                )
            else:
                _avg_comment = '各维度均偏中等，整体均衡但缺乏亮点。'

        st.markdown(f"""
<div style="background:#f8f9ff;border:1px solid #e0e4f5;border-radius:8px;
     padding:10px 14px;margin-top:4px;font-size:0.83rem;color:#444;line-height:1.7">
  <b>🖼️ 形状识别：</b>{_shape_type}<br>
  <b>📈 综合均分：</b>{_avg_r:.0f}/100 &nbsp;
  <span style="font-size:0.78rem;color:#666">{_avg_comment}</span>
</div>
""", unsafe_allow_html=True)

    # ---------- Part 1: 基本信息速览 ----------
    st.markdown('<div class="section-title">📋 第一部分：基本信息速览</div>', unsafe_allow_html=True)

    # 业绩基准文本：优先用行业基金申万覆写，其次招募说明书原文，最后用类型默认基准
    _default_bm_names = {
        'equity': '沪深300（默认）', 'bond': '中债综合指数（默认）',
        'mixed': '沪深300 60% + 中债 40%（默认）', 'sector': '沪深300（默认）',
        'qdii': '境外基准（无法自动获取）', 'index': '沪深300（默认）',
    }
    if _sector_bm_label:
        bm_text = _sector_bm_label
    elif basic['benchmark_text']:
        bm_text = basic['benchmark_text']
    else:
        # 无基准文本：标注「默认使用」，让用户知道这是推断值
        bm_text = _default_bm_names.get(basic['type_category'], '沪深300（默认）')
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
        hidden = estimate_hidden_cost(fund_code, nav_df, basic['type_category'])

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

    # ---------- Part 1.5: 业绩可视化（提前，先看结果再看拆解）----------
    st.markdown('<div class="section-title">📈 业绩走势一览</div>', unsafe_allow_html=True)
    _is_qdii_fund = (basic.get('type_category') == 'qdii')
    _bm_str = '（蓝线为业绩基准）' if not bm_df.empty else ''
    _vis_comment = ''
    if _total_ret > 0:
        _vis_comment = f"该基金在{period_sel}区间累计收益 {_total_ret:+.1f}%，最大回撤 {_max_dd:.1f}%{_bm_str}。"
    else:
        _vis_comment = f"该基金在{period_sel}区间累计收益 {_total_ret:+.1f}%，表现弱于预期{_bm_str}。"
    st.markdown(f'<div style="font-size:0.85rem;color:#666;margin-bottom:8px">{_vis_comment}</div>',
                unsafe_allow_html=True)
    st.plotly_chart(plot_cumulative_return(nav_df, bm_df, bm_label=bm_text), use_container_width=True)

    # QDII基金：基准是境外指数（如MSCI系列），AkShare暂不提供历史行情，图中只显示基金净值
    if _is_qdii_fund and bm_df.empty:
        st.markdown(
            f'<div style="font-size:0.75rem;color:#999;margin-top:-8px">'
            f'业绩基准：{bm_text}'
            f'&nbsp;·&nbsp;🌐 <b>境外指数暂无法自动获取</b>，图中仅展示基金净值走势。'
            f'如需对比，可在<a href="https://cn.investing.com/" target="_blank">investing.com</a>手动查询MSCI指数走势。'
            f'</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'<div style="font-size:0.75rem;color:#999;margin-top:-8px">'
            f'业绩基准：{bm_text}'
            f'&nbsp;·&nbsp;<span title="若基金历史上曾更换基准，本报告使用当前公开基准回溯，不代表历史所有时期的真实基准。">'
            f'⚠️ 基于当前公开基准回溯，历史基准变更期间数据仅供参考</span></div>',
            unsafe_allow_html=True
        )

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
        'qdii':   ('QDII跨市场型', '核心关注：超额收益趋势（相对MSCI基准）、汇率风险、境外市场Beta'),
    }
    _type_name, _type_focus_text = _type_focus.get(model_type, ('主动权益型', ''))
    # 仓位标签：
    # - ETF/指数型：「满仓跟踪」，季报仓位仅供参考（ETF有5-10%备付金，季报仓位不代表真实策略意图）
    # - 行业/主题主动型：只显示股票仓位，加「行业集中」说明
    # - 权益型：只显示股票仓位
    # - 混合/债券：显示股债双仓位
    _is_index_fund = ('ETF' in basic.get('name', '') or 'ETF' in basic.get('type_raw', '') or
                      '指数' in basic.get('type_raw', '') or '联接' in basic.get('name', ''))
    _alloc_src  = holdings.get('alloc_source', '')
    _report_date = holdings.get('report_date', '')
    _report_note = f'（{_report_date}季报）' if _report_date else ''

    if _is_index_fund and model_type == 'sector':
        # ETF行业指数基金：满仓跟踪，括注季报仓位供参考
        _stock_tag = (f'<span class="tag tag-blue">满仓跟踪指数</span>'
                      f'<span class="tag tag-gray" title="ETF通常有5-10%的备付金用于应对赎回，季报仓位≠真实运作意图">'
                      f'季报股票仓位 {stock_ratio*100:.0f}%{_report_note}</span>')
        _bond_tag  = ''
    elif model_type == 'sector':
        # 主动行业/主题型：高仓位集中，只显股票仓位
        _stock_tag = f'<span class="tag tag-blue">股票仓位 {stock_ratio*100:.0f}%（行业集中型）</span>'
        _bond_tag  = ''
    elif model_type == 'equity':
        # 次新基金建仓期（股票仓位<75%）：明确标注「建仓中」避免用户误解为保守型
        if _is_new_fund and stock_ratio < 0.75:
            _stock_tag = (f'<span class="tag tag-orange" '
                          f'title="次新基金通常有3-6个月建仓期，股票仓位逐步提升至目标仓位，不代表保守运作风格">'
                          f'建仓中 {stock_ratio*100:.0f}%</span>')
        else:
            _stock_tag = f'<span class="tag tag-blue">股票仓位 {stock_ratio*100:.0f}%</span>'
        _bond_tag  = ''  # 权益型债券仓位通常可忽略
    elif model_type == 'bond':
        # 纯债/固收型：不显示股票仓位（季报默认5%是占比极低的现金替代，不代表真实持股）
        _stock_tag = ''
        _bond_tag  = f'<span class="tag tag-gray">债券仓位 {bond_ratio*100:.0f}%</span>'
    else:
        # 混合型：股票仓位>2%才显示（避免显示意义不大的极小值）
        if stock_ratio > 0.02:
            _stock_tag = f'<span class="tag tag-blue">股票仓位 {stock_ratio*100:.0f}%</span>'
        else:
            _stock_tag = ''
        _bond_tag  = f'<span class="tag tag-gray">债券仓位 {bond_ratio*100:.0f}%</span>'

    st.markdown(f"""
<div class="card card-info">
  <b>🎯 {_type_name}</b> &nbsp;
  {_stock_tag}
  {_bond_tag}
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
        '凸性':         '凸性 = 极端行情时的缓冲保护。凸性越强，利率大跌时净值加速向上，大涨时跌得少。越高越好。',
        '年化信用溢价': '年化收益来源 = 利息收入 + 信用溢价 + 骑乘收益的年化总和。偏高（>4%）时，可能是买了信用评级偏低的债，也可能是短期高票息策略或曲线套利，不一定代表高风险。',
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
        _alpha_is_none = model_results.get('alpha') is None
        if _is_qdii_fund:
            # QDII基金：A股FF因子对境外持仓无意义，展示专属提示
            st.markdown(
                f'<div class="card card-info" style="font-size:0.88rem;color:#444">'
                f'🌐 <b>QDII基金因子说明</b>：该基金主要投资境外市场（港股/美股/台股等），'
                f'当前A股FF因子（SMB/HML/MOM）对其净值变动解释力极低，Alpha/Beta数值仅供趋势参考。'
                f'<br><br>📌 <b>建议重点关注</b>：'
                f'<br>① <b>夏普比率</b>（风险调整后收益）'
                f'<br>② <b>最大回撤</b>（境外市场波动通常更大）'
                f'<br>③ <b>汇率风险</b>（人民币升值会侵蚀境外资产收益）'
                f'<br>④ <b>MSCI基准相对表现</b>（需在基金公司官网查询）'
                f'</div>',
                unsafe_allow_html=True
            )
        elif _alpha_is_none and _is_new_fund:
            # 次新基金：数据不足导致因子模型无法运行，给专属说明
            _days_of_data = len(nav_df)
            st.markdown(
                f'<div class="card card-info" style="font-size:0.88rem;color:#444">'
                f'⏳ <b>次新基金因子模型受限</b>：该基金净值历史仅 {_days_of_data} 天，'
                f'Fama-French因子回归需要至少60个有效交易日。'
                f'<br>当前可展示：累计收益走势、回撤分析、基本面诊断。'
                f'<br>建议 <b>{max(0, 60-_days_of_data)} 个交易日后</b>再次分析，届时可获得完整量化报告。'
                f'</div>',
                unsafe_allow_html=True
            )
        elif _interp:
            st.markdown(f'<div class="card" style="font-size:0.88rem;color:#444">{_interp}</div>',
                        unsafe_allow_html=True)

        # ── 第3条：残差分析 · 神秘超额提示 ──
        _residual = model_results.get('residual_insight', '')
        if _residual:
            _r2_recent = model_results.get('r_squared_recent')
            _r2_full   = model_results.get('r_squared', 0)
            _is_drop = _r2_recent is not None and (_r2_full - _r2_recent) > 0.25
            _res_style = 'card card-warn' if _is_drop else 'card card-info'
            _res_icon  = '🔍 残差分析 · 模型解释力预警' if _is_drop else '📐 残差分析 · 模型解释力说明'
            st.markdown(
                f'<div class="{_res_style}" style="margin-top:6px;font-size:0.86rem">'
                f'<b>{_res_icon}</b>'
                f'<div style="margin-top:6px;line-height:1.8">{_residual}</div>'
                f'</div>',
                unsafe_allow_html=True
            )


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
        # 微文案：当权益基金触发债券扫描时，主动告知用户原因
        if model_type == 'equity':
            st.markdown(
                f'<div style="background:#f0f8ff;border:1px solid #74b9ff;border-radius:8px;'
                f'padding:10px 16px;font-size:0.86rem;color:#2980b9;margin:8px 0 10px;line-height:1.7">'
                f'💡 <b>跨界扫描发现</b>：该权益基金实际持有超 20% 的债券头寸，'
                f'已自动触发隐含久期与信用风险测评。以下数据反映的是债券端的风险敞口。'
                f'</div>',
                unsafe_allow_html=True
            )
        br = model_results['bond']
        # 优先展示底层真实久期（已做仓位修正），回退到组合级久期
        d_underlying    = br.get('duration_underlying', br.get('duration', 0))
        d_portfolio     = br.get('duration', 0)
        bond_ratio_used = br.get('bond_ratio_used', 1.0)
        carry           = br.get('carry_alpha', br.get('credit_spread_alpha', 0))
        dur_short       = br.get('dur_short', 0.0)
        dur_long        = br.get('dur_long', 0.0)
        p_short         = br.get('p_short', 1.0)
        p_long          = br.get('p_long', 1.0)
        credit_beta     = br.get('credit_beta', 0.0)
        p_credit        = br.get('p_credit', 1.0)
        has_credit      = br.get('has_credit_factor', False)
        r2_adj          = br.get('r_squared_adj', br.get('r_squared', 0.0))
        factor_mode     = br.get('factor_mode', '单因子')
        convexity_qual  = br.get('convexity_quality', 'unknown')
        fund_type_note  = br.get('fund_type_note', '')

        _is_three_factor = 'three_factor' in br.get('duration_source', '') or '三因子' in factor_mode or '双因子' in factor_mode

        # ── 模型标签 + 基金类型诊断 ──
        _model_badge_color = '#27ae60' if _is_three_factor else '#e67e22'
        _model_label = '🔬 利率+信用三维分析' if _is_three_factor else '📐 单利率因子分析'
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">'
            f'<span style="background:{_model_badge_color};color:white;font-size:0.75rem;'
            f'padding:2px 10px;border-radius:12px;font-weight:600">{_model_label}</span>'
            f'<span style="color:#888;font-size:0.76rem">模型解释力 Adj.R²={r2_adj:.3f}</span>'
            f'</div>',
            unsafe_allow_html=True
        )
        if fund_type_note:
            st.markdown(
                f'<div style="background:#f0f7ff;border-left:3px solid #3498db;border-radius:5px;'
                f'padding:6px 12px;font-size:0.83rem;color:#2c3e50;margin-bottom:8px">'
                f'💡 {fund_type_note}</div>',
                unsafe_allow_html=True
            )

        # ── KPI 指标卡（三因子模式 vs 单因子模式）──
        if _is_three_factor and (p_short < 0.2 or p_long < 0.2):
            # 三因子模式：展示短端久期 + 长端久期 + 信用敏感度 + carry
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                _s_sig = ' *' if p_short < 0.05 else (' ~' if p_short < 0.1 else '')
                st.markdown(_kpi(f'短债敏感度{_s_sig}', f'{abs(dur_short):.2f}年',
                                 'kpi-orange' if abs(dur_short) > 3 else ''), unsafe_allow_html=True)
            with c2:
                _l_sig = ' *' if p_long < 0.05 else (' ~' if p_long < 0.1 else '')
                st.markdown(_kpi(f'长债敏感度{_l_sig}', f'{abs(dur_long):.2f}年',
                                 'kpi-orange' if abs(dur_long) > 5 else ''), unsafe_allow_html=True)
            with c3:
                if has_credit:
                    _c_sig = ' *' if p_credit < 0.05 else ''
                    _c_color = 'kpi-orange' if credit_beta < -1.5 else ('kpi-green' if credit_beta > 1.0 else '')
                    st.markdown(_kpi(f'信用风险敞口{_c_sig}', f'{credit_beta:.2f}',
                                     _c_color), unsafe_allow_html=True)
                else:
                    st.markdown(_kpi('信用风险敞口', '—', ''), unsafe_allow_html=True)
            with c4:
                st.markdown(_kpi('年化收益来源', fmt_pct(carry),
                                 'kpi-orange' if carry > 0.04 else ''), unsafe_allow_html=True)
            # 显著性图例
            st.markdown(
                '<div style="font-size:0.72rem;color:#aaa;padding:2px 0 6px">'
                '* p&lt;0.05 显著 &nbsp;~ p&lt;0.1 弱显著</div>',
                unsafe_allow_html=True
            )
        else:
            # 单因子/降级模式：沿用原有3列展示
            c1, c2, c3 = st.columns(3)
            with c1:
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

        # 信用利差方向图（三因子模式下有信用因子时展示）
        if _is_three_factor and has_credit and p_credit < 0.1:
            _c_dir = '信用风险扩大时净值会跌（持有较多信用债）' if credit_beta < 0 else '信用利差收窄时受益（偏利率债/反向对冲）'
            _c_bg  = '#fff8f0' if credit_beta < 0 else '#f0fff8'
            _c_bd  = '#e67e22' if credit_beta < 0 else '#27ae60'
            st.markdown(
                f'<div style="background:{_c_bg};border-left:3px solid {_c_bd};border-radius:5px;'
                f'padding:7px 12px;font-size:0.82rem;color:#333;margin-bottom:6px">'
                f'📊 <b>信用风险敏感度</b>（β={credit_beta:.2f}, p={p_credit:.3f}）：{_c_dir}。'
                f'{"收益偏高可能是因为买了评级低的债，需关注违约风险。" if credit_beta < -1.0 else "信用债占比不高，利率才是主要驱动。"}'
                f'</div>',
                unsafe_allow_html=True
            )

        # 凸性静态穿透
        _conv_icon = {'strong': '🛡️', 'weak': '⚠️', 'moderate': '🔵', 'unknown': 'ℹ️'}.get(convexity_qual, 'ℹ️')
        _conv_note = br.get('interpretation', '')
        _conv_parts = [p for p in _conv_note.split('；') if '凸性' in p]
        if _conv_parts:
            st.markdown(
                f'<div style="font-size:0.8rem;color:#666;padding:4px 10px;'
                f'background:#f8f9fa;border-radius:5px;margin:2px 0 6px">'
                f'{_conv_icon} {_conv_parts[0]}</div>',
                unsafe_allow_html=True
            )

        # 指标解释
        st.markdown(
            _explain_row('有效久期') +
            _explain_row('年化信用溢价'),
            unsafe_allow_html=True
        )

        _bond_interp = br.get('interpretation', '')
        if _bond_interp:
            # 把分号分隔的 parts 拆成 bullet 列表展示
            _interp_parts = [p.strip() for p in _bond_interp.split('；') if p.strip()]
            # 过滤掉和上面 convexity 卡片重复的那条
            _interp_parts = [p for p in _interp_parts if '凸性' not in p]
            if _interp_parts:
                _bullets = ''.join(f'<li style="margin-bottom:3px">{p}</li>' for p in _interp_parts)
                st.markdown(
                    f'<div class="card" style="font-size:0.86rem;color:#444">'
                    f'<ul style="margin:0;padding-left:18px;line-height:1.75">{_bullets}</ul></div>',
                    unsafe_allow_html=True
                )

        # 债券模型方法说明
        st.markdown(
            '<div style="font-size:0.8rem;color:#666;padding:6px 10px;'
            'background:#f8f9fa;border-radius:6px;margin-top:4px">'
            '📖 <b>分析方法</b>：将基金每日净值涨跌，对短期利率、长期利率和信用利差三个因子做回归，'
            '分别算出这支基金对"短债"、"长债"、"信用债"各有多大暴露。Adj.R²越高说明三个因子越能解释净值变化。</div>',
            unsafe_allow_html=True
        )

        # ══════════════════════════════════════════════════
        # 债券持仓穿透板块（季报明细穿透）
        # ══════════════════════════════════════════════════
        if bond_structure:
            st.markdown('---')
            _bs = bond_structure
            _quarter_label = _bs.get('quarter', '最新季度')
            _total_wt      = _bs.get('total_weight', 0)
            _n_bonds       = _bs.get('n_bonds', 0)
            _wtd_dur       = _bs.get('wtd_duration_est', 0)
            _rate_r        = _bs.get('rate_ratio', 0)
            _credit_r      = _bs.get('credit_ratio', 0)
            _conv_r        = _bs.get('convert_ratio', 0)
            _type_dist     = _bs.get('type_dist', pd.DataFrame())
            _conv_detail   = _bs.get('conv_detail', pd.DataFrame())

            st.markdown(
                f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">'
                f'<span style="font-size:1.05rem;font-weight:700;color:#2c3e50">🔍 债券持仓穿透</span>'
                f'<span style="background:#8e44ad;color:white;font-size:0.72rem;'
                f'padding:2px 8px;border-radius:10px;font-weight:600">季报穿透</span>'
                f'<span style="color:#aaa;font-size:0.78rem">数据来源：{_quarter_label} | 共 {_n_bonds} 只债券 | '
                f'合计占净值 {_total_wt:.1f}%（未含银行间未披露部分）</span>'
                f'</div>',
                unsafe_allow_html=True
            )

            # ── 三列摘要 KPI ──
            _bc1, _bc2, _bc3 = st.columns(3)
            with _bc1:
                _dur_color = 'kpi-orange' if _wtd_dur > 5 else ''
                st.markdown(_kpi('加权估算剩余期限', f'{_wtd_dur:.1f}年', _dur_color),
                            unsafe_allow_html=True)
            with _bc2:
                _rate_pct = f'{_rate_r*100:.1f}%'
                st.markdown(_kpi('利率债占比（含国债/政金债）', _rate_pct,
                                 'kpi-green' if _rate_r > 0.5 else ''), unsafe_allow_html=True)
            with _bc3:
                _credit_pct = f'{(_credit_r+_conv_r)*100:.1f}%'
                st.markdown(_kpi('信用/含权债占比', _credit_pct,
                                 'kpi-orange' if _credit_r + _conv_r > 0.5 else ''), unsafe_allow_html=True)

            # ── 债券类型分布图 + 可转债评级分布 ──
            _col_left, _col_right = st.columns([3, 2])

            with _col_left:
                # Plotly 饼图：债券类型
                if not _type_dist.empty:
                    import plotly.graph_objects as _go
                    _colors_bond = ['#3498db','#2ecc71','#e74c3c','#f39c12',
                                    '#9b59b6','#1abc9c','#e67e22','#95a5a6']
                    _fig_pie = _go.Figure(_go.Pie(
                        labels=_type_dist['bond_type'].tolist(),
                        values=_type_dist['占净值'].tolist(),
                        hole=0.4,
                        marker_colors=_colors_bond[:len(_type_dist)],
                        textinfo='label+percent',
                        textfont_size=11,
                        hovertemplate='%{label}<br>占净值：%{value:.2f}%<extra></extra>'
                    ))
                    _fig_pie.update_layout(
                        title=dict(text='债券类型分布（占净值%）', font_size=13, x=0),
                        showlegend=False,
                        margin=dict(l=0, r=0, t=30, b=0),
                        height=250,
                    )
                    st.plotly_chart(_fig_pie, use_container_width=True)

            with _col_right:
                # 可转债评级分布 或 类型明细表
                if not _conv_detail.empty and '评级' in _conv_detail.columns:
                    _rating_counts = _conv_detail.groupby('评级')['占净值比例'].sum().reset_index()
                    _rating_counts = _rating_counts.sort_values('占净值比例', ascending=False)
                    # 简单 bar chart
                    import plotly.express as _px
                    _fig_bar = _px.bar(
                        _rating_counts, x='评级', y='占净值比例',
                        title=f'可转债评级分布（共{len(_conv_detail)}只，占净值{_conv_r*_total_wt:.1f}%）',
                        labels={'占净值比例': '占净值%', '评级': '信用评级'},
                        color='评级',
                        color_discrete_sequence=['#2ecc71','#3498db','#f39c12','#e74c3c','#95a5a6']
                    )
                    _fig_bar.update_layout(
                        showlegend=False, height=250,
                        margin=dict(l=0, r=0, t=30, b=0),
                        title_font_size=12,
                        xaxis_title='', yaxis_title='占净值%'
                    )
                    st.plotly_chart(_fig_bar, use_container_width=True)
                elif not _type_dist.empty:
                    # 没有可转债时，展示类型明细表
                    st.markdown('<div style="font-size:0.82rem;font-weight:600;color:#444;margin-bottom:4px">债券类型明细</div>',
                                unsafe_allow_html=True)
                    _show_df = _type_dist.rename(columns={'bond_type':'类型','只数':'只数','占净值':'占净值%','占比%':'占持仓%'})
                    st.dataframe(_show_df, use_container_width=True, hide_index=True,
                                 column_config={'占净值%': st.column_config.NumberColumn(format='%.2f')})

            # ── 可转债明细展示（折叠）──
            if not _conv_detail.empty:
                with st.expander(f'📋 可转债持仓明细（{len(_conv_detail)}只）', expanded=False):
                    _show_conv = _conv_detail[['债券名称','占净值比例','评级','到期日','剩余年限','票面利率']].copy()
                    _show_conv = _show_conv.sort_values('占净值比例', ascending=False)
                    st.dataframe(
                        _show_conv, use_container_width=True, hide_index=True,
                        column_config={
                            '占净值比例': st.column_config.NumberColumn('占净值%', format='%.2f'),
                            '剩余年限': st.column_config.NumberColumn('剩余年限(年)', format='%.2f'),
                        }
                    )

            # ── 穿透联动解读 ──
            _penetration_insight = []

            # 利率债 vs 信用债结构
            if _rate_r > 0.6:
                _penetration_insight.append('持仓以<b>国债/政金债为主</b>，主要风险是利率变化，信用违约风险低。')
            elif _credit_r + _conv_r > 0.5:
                _penetration_insight.append(f'持仓<b>信用债/可转债偏多</b>（占 {(_credit_r+_conv_r)*100:.0f}%），收益相对高一些，但也承担了更多信用风险。')
            else:
                _penetration_insight.append('国债和信用债<b>混合配置</b>，利率风险和信用风险都有一些，相对均衡。')

            # 可转债情况
            if _conv_r > 0.1:
                _n_conv = len(_conv_detail) if not _conv_detail.empty else '若干'
                _penetration_insight.append(f'可转债仓位占 <b>{_conv_r*100:.0f}%</b>（{_n_conv}只），股市涨时这部分会有弹性，但波动也比纯债更大。')

            # 与三因子β联动
            if has_credit and abs(credit_beta) > 0.5 and p_credit < 0.1:
                if credit_beta < 0:
                    _penetration_insight.append(
                        f'模型测算显示信用风险敏感度较高，和持仓中信用债占比 {_credit_r*100:.0f}% 相符——<b>信用市场出风险时，这支基金会比较难看。</b>'
                    )
                else:
                    _penetration_insight.append(
                        f'模型显示信用利差扩大反而对净值有利，可能持有利率债对冲或有特殊策略。'
                    )

            # 加权期限
            if _wtd_dur > 0:
                _dur_comment = '偏长，降息时涨得更多但加息风险也更大' if _wtd_dur > 4 else ('中短期，利率敏感度适中' if _wtd_dur > 2 else '很短，基本不怕利率波动')
                _penetration_insight.append(
                    f'从持仓看，债券剩余平均期限约 <b>{_wtd_dur:.1f}年</b>（{_dur_comment}）。'
                    f'<span style="color:#888;font-size:0.78em">（估算值，仅供参考）</span>'
                )

            if _penetration_insight:
                st.markdown(
                    '<div style="background:#f8f0ff;border-left:3px solid #8e44ad;border-radius:6px;'
                    'padding:10px 14px;font-size:0.85rem;color:#333;margin-top:4px;line-height:1.8">'
                    '🔍 <b>持仓透视</b>：' + ''.join(f'<br>• {s}' for s in _penetration_insight) +
                    '</div>',
                    unsafe_allow_html=True
                )

            # 数据说明
            st.markdown(
                '<div style="font-size:0.75rem;color:#aaa;margin-top:4px">'
                '📌 数据来源：天天基金季报债券持仓；可转债评级实时查询；期限为估算值，仅供参考，非精确YTM。</div>',
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

        # 滚动仓位图 + 诊断看板
        rolling = model_results.get('rolling_df')
        if rolling is not None and not rolling.empty:
            with st.expander("📈 动态仓位监控图（20日 + 60日双窗口）", expanded=False):
                st.plotly_chart(plot_rolling_beta(rolling, stock_ratio), use_container_width=True)

                # ---- 双窗口可信度说明 ----
                st.markdown(
                    '<div style="font-size:0.78rem;color:#666;padding:6px 12px;'
                    'background:#f8f9fa;border-radius:6px;margin:4px 0 10px">'
                    '📖 <b>双窗口解读规则</b>：'
                    '两条线高度重合 → 结论极度可靠；'
                    '20日线剧烈波动但60日线平稳 → 近期数据有噪音，参考60日为主；'
                    '两条线同步趋势变化 → 真实仓位调整，置信度高。'
                    '&nbsp;&nbsp;|&nbsp;&nbsp;'
                    '<b>R²</b>：🟢≥0.80信号可信 / 🟡0.50~0.80参考 / 🔴&lt;0.50噪音</div>',
                    unsafe_allow_html=True
                )

                # ---- 动态仓位状态诊断表格 ----
                latest_beta_20 = rolling['equity_beta_20'].dropna().iloc[-1] if 'equity_beta_20' in rolling.columns and rolling['equity_beta_20'].notna().any() else None
                latest_beta_60 = rolling['equity_beta_60'].dropna().iloc[-1] if 'equity_beta_60' in rolling.columns and rolling['equity_beta_60'].notna().any() else None
                latest_r2      = rolling['r2_20'].dropna().iloc[-1] if 'r2_20' in rolling.columns and rolling['r2_20'].notna().any() else None

                if latest_beta_20 is not None:
                    static_pct  = stock_ratio * 100
                    dyn_pct_20  = latest_beta_20 * 100
                    drift_pct   = abs(dyn_pct_20 - static_pct)

                    # 状态判断
                    if drift_pct > 15:
                        drift_status = '🔴 风格漂移'
                        drift_comment = (f'季报说 {static_pct:.0f}% 股票仓位，'
                                         f'最近表现像 {dyn_pct_20:.0f}%，可能已发生实质性调仓')
                    elif drift_pct > 8:
                        drift_status = '🟡 轻微偏差'
                        drift_comment = f'仓位有小幅变动，偏差{drift_pct:.0f}%，在正常波动范围内'
                    else:
                        drift_status = '🟢 稳定'
                        drift_comment = '动态仓位与季报披露高度一致，经理维持了声明的仓位'

                    # R² 状态
                    if latest_r2 is not None:
                        if latest_r2 >= 0.80:
                            r2_status = '🟢 信号可信'
                            r2_comment = f'R²={latest_r2:.2f}，基金波动与股债基准高度吻合，这个仓位估算非常靠谱'
                        elif latest_r2 >= 0.50:
                            r2_status = '🟡 参考价值一般'
                            r2_comment = f'R²={latest_r2:.2f}，模型解释度中等，仓位估算仅供参考'
                        else:
                            r2_status = '🔴 噪音较大'
                            r2_comment = f'R²={latest_r2:.2f}，近期基金表现难以用股债因子解释，可能发生了策略变化'
                    else:
                        r2_status = '—'
                        r2_comment = '数据不足'

                    # 60日 vs 20日 一致性
                    if latest_beta_60 is not None:
                        window_diff = abs(latest_beta_20 - latest_beta_60) * 100
                        if window_diff < 5:
                            consistency = '🟢 高度一致'
                            consistency_comment = f'20日({dyn_pct_20:.0f}%) 与 60日({latest_beta_60*100:.0f}%)高度吻合，结论可靠'
                        elif window_diff < 15:
                            consistency = '🟡 基本一致'
                            consistency_comment = f'20日({dyn_pct_20:.0f}%) 与 60日({latest_beta_60*100:.0f}%)有轻微分歧，以60日为参考基准'
                        else:
                            consistency = '🔴 分歧较大'
                            consistency_comment = f'20日({dyn_pct_20:.0f}%) 远离60日({latest_beta_60*100:.0f}%)，近期数据有噪音或正在快速调仓'
                    else:
                        consistency = '—'
                        consistency_comment = '60日窗口数据不足'

                    # 渲染表格
                    st.markdown(
                        '<div style="font-size:0.82rem;font-weight:600;color:#444;margin:8px 0 4px">📋 仓位状态诊断看板</div>',
                        unsafe_allow_html=True
                    )
                    table_rows = [
                        ('监控指标', '当前数值', '季报数值', '状态诊断', '大白话解读'),
                        (f'动态股票仓位（20日）', f'{dyn_pct_20:.1f}%', f'{static_pct:.0f}%', drift_status, drift_comment),
                        (f'双窗口一致性', f'20日{dyn_pct_20:.0f}% | 60日{latest_beta_60*100:.0f}%' if latest_beta_60 else f'{dyn_pct_20:.0f}%', '—', consistency, consistency_comment),
                        ('模型解释度 (R²)', f'{latest_r2:.2f}' if latest_r2 else '—', '—', r2_status, r2_comment),
                    ]

                    # 构造 HTML 表格
                    _th_style = 'padding:7px 10px;background:#1a1a2e;color:white;font-weight:600;font-size:0.78rem;white-space:nowrap'
                    _td_style = 'padding:6px 10px;border-bottom:1px solid #eee;font-size:0.78rem;vertical-align:top'
                    _table_html = '<table style="width:100%;border-collapse:collapse;border-radius:8px;overflow:hidden">'
                    for i, row in enumerate(table_rows):
                        if i == 0:
                            _table_html += '<tr>' + ''.join(f'<th style="{_th_style}">{c}</th>' for c in row) + '</tr>'
                        else:
                            bg = '#fafafa' if i % 2 == 0 else 'white'
                            _table_html += f'<tr style="background:{bg}">' + ''.join(f'<td style="{_td_style}">{c}</td>' for c in row) + '</tr>'
                    _table_html += '</table>'
                    st.markdown(_table_html, unsafe_allow_html=True)

                    # ---- 景气驱动型基金深度诊断 ----
                    # 如果 equity_beta 曲线波动剧烈（std>0.15），补充"心电图"解读
                    beta_std = rolling['equity_beta_20'].dropna().std()
                    if beta_std > 0.15:
                        # 判断近期趋势方向
                        recent_betas = rolling['equity_beta_20'].dropna().tail(20)
                        beta_trend = recent_betas.iloc[-1] - recent_betas.iloc[0]
                        if latest_r2 and latest_r2 >= 0.7 and latest_beta_20 < 0.15:
                            _jj_note = (
                                f'⚡ <b>深度观察</b>：equity_beta已降至 {dyn_pct_20:.0f}%，'
                                f'且R²={latest_r2:.2f}（信号高可信），基金已实质上转变为'
                                f'<b>债券型策略</b>，正在主动规避风险。'
                            )
                        elif beta_trend > 0.15 and latest_beta_20 > 0.5:
                            _jj_note = (
                                f'⚡ <b>深度观察</b>：equity_beta近20日从 '
                                f'{recent_betas.iloc[0]*100:.0f}% 快速拉升至 {dyn_pct_20:.0f}%，'
                                f'经理正在积极加仓，可能在捕捉景气度拐点机会。'
                            )
                        else:
                            _jj_note = (
                                f'⚡ <b>深度观察</b>：equity_beta曲线波动标准差{beta_std:.2f}（像心电图），'
                                f'该基金为灵活调仓型，近期仓位在 '
                                f'{rolling["equity_beta_20"].dropna().tail(60).min()*100:.0f}%~'
                                f'{rolling["equity_beta_20"].dropna().tail(60).max()*100:.0f}% 之间大幅游走。'
                            )
                        st.markdown(
                            f'<div class="card card-warn" style="margin-top:10px;font-size:0.85rem">'
                            f'{_jj_note}</div>',
                            unsafe_allow_html=True
                        )

    # ── 混合型基金残差分析（FF模型 residual_insight）──
    if model_type == 'mixed':
        _m_residual = model_results.get('residual_insight', '')
        if _m_residual:
            _m_r2_recent = model_results.get('r_squared_recent')
            _m_r2_full   = model_results.get('r_squared', 0)
            _m_is_drop = _m_r2_recent is not None and (_m_r2_full - _m_r2_recent) > 0.25
            _m_res_style = 'card card-warn' if _m_is_drop else 'card card-info'
            _m_res_icon  = '🔍 残差分析 · 模型解释力预警' if _m_is_drop else '📐 残差分析 · 模型解释力说明'
            st.markdown(
                f'<div class="{_m_res_style}" style="margin-top:6px;font-size:0.86rem">'
                f'<b>{_m_res_icon}</b>'
                f'<div style="margin-top:6px;line-height:1.8">{_m_residual}</div>'
                f'</div>',
                unsafe_allow_html=True
            )

    # 行业主题补充
    if 'sector' in model_results:
        sr = model_results['sector']
        sw_name_display = sr.get('sw_name', '')
        bm_source_text  = sr.get('bm_source', '')
        has_sw_bm       = bool(sw_name_display)

        # ---- 基准来源标签 ----
        if has_sw_bm:
            st.markdown(
                f'<div style="font-size:0.8rem;color:#27ae60;padding:5px 12px;'
                f'background:#eafaf1;border-radius:6px;margin-bottom:10px;border-left:3px solid #27ae60">'
                f'✅ <b>精准行业基准已启用：</b>{bm_source_text}'
                f'&nbsp;（避免"沪深300伪Alpha"陷阱）</div>',
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f'<div style="font-size:0.8rem;color:#e67e22;padding:5px 12px;'
                f'background:#fef9e7;border-radius:6px;margin-bottom:10px;border-left:3px solid #e67e22">'
                f'⚠️ <b>未匹配申万行业指数：</b>{bm_source_text}'
                f'&nbsp;（建议手动确认行业类别）</div>',
                unsafe_allow_html=True
            )

        # ---- KPI 卡片（三列）----
        c1, c2, c3 = st.columns(3)
        neutral_alpha_val = sr.get('neutral_alpha', 0.0)
        with c1:
            _alpha_label = f'行业内 Alpha{"（申万）" if has_sw_bm else ""}'
            st.markdown(_kpi(_alpha_label, fmt_pct(neutral_alpha_val)), unsafe_allow_html=True)
        with c2:
            st.markdown(_kpi('跟踪误差', fmt_pct(sr.get('tracking_error'))), unsafe_allow_html=True)
        with c3:
            st.markdown(_kpi('信息比率 IR', fmt_f(sr.get('info_ratio'))), unsafe_allow_html=True)

        # ---- 大白话诊断看板 ----
        na   = sr.get('neutral_alpha', 0.0)
        na_bm = sr.get('neutral_alpha_bm', na)
        te   = sr.get('tracking_error', 0.0)
        ir   = sr.get('info_ratio', 0.0)
        industry_label = sw_name_display or '同行业'

        # Alpha 行（与性格诊断标签体系保持一致：na>0.03 AND ir>0.5 才算「具备选股」）
        if na > 0.08 and ir > 1.0:
            alpha_status = '🏆 窝里横王者'
            alpha_comment = f'就算{industry_label}指数不涨不跌，经理靠选股一年也能多赚{na*100:.1f}%，IR={ir:.2f}高效稳定。这是真本事。'
        elif na > 0.03 and ir > 0.5:
            alpha_status = '✅ 具备选股能力'
            alpha_comment = f'在{industry_label}内部超额{na*100:.1f}%，IR={ir:.2f}，选股效率达标。'
        elif na > 0.03:
            # Alpha>3%但IR偏低：有超额但效率不足，对应性格诊断「行业随从」
            alpha_status = '🟡 微弱Alpha，效率不足'
            alpha_comment = f'Alpha {na*100:.1f}%有一点超额，但IR={ir:.2f}偏低，说明超额不稳定——主要靠行业Beta赚钱，非经理选股。'
        elif na > 0:
            alpha_status = '🟡 微弱超额'
            alpha_comment = f'勉强跑赢{industry_label}指数{na*100:.1f}%，优势不明显，注意费率侵蚀。'
        else:
            alpha_status = '🔴 跑输行业'
            alpha_comment = f'连{industry_label}指数都跑不赢（Alpha={na*100:.1f}%），买指数ETF更划算。'

        # 跟踪误差行
        if te < 0.03:
            te_status = '🟢 增强指数风格'
            te_comment = f'跟踪误差{te*100:.1f}%极低，经理紧跟行业指数，主动偏离很少。'
        elif te < 0.08:
            te_status = '🟡 适度主动管理'
            te_comment = f'跟踪误差{te*100:.1f}%，经理做了一定程度的行业内个股偏离，有主动管理色彩。'
        elif te < 0.15:
            te_status = '🟠 偏离较大'
            te_comment = f'跟踪误差{te*100:.1f}%，经理在{industry_label}内重点押注细分赛道，个股集中度高。'
        else:
            te_status = '🔴 高度集中押注'
            te_comment = f'跟踪误差{te*100:.1f}%极高，与行业指数大幅偏离，集中度风险显著，波动剧烈。'

        # IR 行
        if ir > 1.5:
            ir_status = '💎 高效Alpha'
            ir_comment = f'IR={ir:.2f}，每冒1%偏离风险换回{ir:.2f}%超额，选股不仅准而且稳。'
        elif ir > 0.5:
            ir_status = '🟡 效率尚可'
            ir_comment = f'IR={ir:.2f}，选股效率一般，尚有提升空间。'
        elif ir > 0:
            ir_status = '🟠 性价比偏低'
            ir_comment = f'IR={ir:.2f}，超额收益可能靠高集中度"赌"出来，风险收益比不划算。'
        else:
            ir_status = '🔴 主动管理失效'
            ir_comment = f'IR={ir:.2f}，偏离了指数但没有换来超额，主动管理未带来价值。'

        # vs 沪深300 对比行（仅在有申万基准且差异显著时显示）
        show_bm_compare = has_sw_bm and abs(na - na_bm) > 0.02

        # 渲染表格
        st.markdown(
            '<div style="font-size:0.82rem;font-weight:600;color:#444;margin:10px 0 4px">📋 行业 Alpha 诊断看板</div>',
            unsafe_allow_html=True
        )
        _th = 'padding:7px 10px;background:#1a1a2e;color:white;font-weight:600;font-size:0.78rem;white-space:nowrap'
        _td = 'padding:6px 10px;border-bottom:1px solid #eee;font-size:0.78rem;vertical-align:top'
        _tbl = '<table style="width:100%;border-collapse:collapse;border-radius:8px;overflow:hidden">'
        _tbl += f'<tr><th style="{_th}">指标</th><th style="{_th}">当前数值</th><th style="{_th}">状态诊断</th><th style="{_th}">大白话解读</th></tr>'
        _rows = [
            (f'行业内Alpha{"（申万"+sw_name_display+"）" if has_sw_bm else ""}',
             f'{na*100:.1f}%', alpha_status, alpha_comment),
            ('跟踪误差（偏离度）', f'{te*100:.1f}%', te_status, te_comment),
            ('信息比率（选股性价比）', f'{ir:.2f}', ir_status, ir_comment),
        ]
        if show_bm_compare:
            if na_bm > na + 0.02:
                cmp_status = '⚠️ 基准陷阱'
                cmp_comment = (f'vs沪深300 Alpha={na_bm*100:.1f}%，vs申万{sw_name_display}仅{na*100:.1f}%。'
                               f'差值{(na_bm-na)*100:.1f}%来自行业Beta，不是经理选股能力。')
            else:
                cmp_status = '✅ Alpha纯粹'
                cmp_comment = (f'vs沪深300({na_bm*100:.1f}%) ≈ vs行业指数({na*100:.1f}%)，'
                               f'Alpha较为纯粹，不含行业Beta虚增成分。')
            _rows.append(('沪深300 vs 行业指数对比', f'{na*100:.1f}% vs {na_bm*100:.1f}%', cmp_status, cmp_comment))

        for i, (ind, val, sta, com) in enumerate(_rows):
            bg = '#fafafa' if i % 2 == 0 else 'white'
            _tbl += f'<tr style="background:{bg}"><td style="{_td}">{ind}</td><td style="{_td}"><b>{val}</b></td><td style="{_td}">{sta}</td><td style="{_td}">{com}</td></tr>'
        _tbl += '</table>'
        st.markdown(_tbl, unsafe_allow_html=True)

        # ---- 009414景气驱动型：动态行业基准逻辑提示 ----
        fund_name_tmp = basic.get('name', '')
        if any(kw in fund_name_tmp for kw in ['景气', '主题', '灵活', '动态', '轮动', '多赛道']):
            st.markdown(
                '<div class="card card-warn" style="margin-top:10px;font-size:0.85rem">'
                '⚡ <b>动态行业基准提示（景气/主题型基金）：</b>'
                '该基金会在多个核心行业间切换（如半导体→新能源→军工），'
                '当前行业基准仅代表当前主要暴露。'
                '建议结合上方「滚动仓位监控」确认基金当前主要行业后，'
                '<b>以该行业指数作为参照系</b>——'
                '只有跑赢当期主仓行业指数的收益，才是经理真正的"选股Alpha"。'
                '</div>',
                unsafe_allow_html=True
            )

        # ---- 指标释义折叠区 ----
        with st.expander("📖 指标含义说明", expanded=False):
            st.markdown(
                _explain_row('中性化 Alpha') +
                _explain_row('跟踪误差') +
                _explain_row('信息比率'),
                unsafe_allow_html=True
            )
            st.markdown(
                '<div style="font-size:0.78rem;color:#666;margin-top:6px">'
                '📌 <b>申万行业指数精准基准说明</b>：若用沪深300衡量医药基金，医药板块集体暴涨时经理躺平也能跑赢，'
                'Alpha会虚高。本工具优先匹配申万一级行业指数，剥离行业Beta，'
                '最终剩下的才是经理在行业内部的真实选股能力。'
                '</div>',
                unsafe_allow_html=True
            )

        # ── 残差分析（sector 基金同样运行了FF模型，residual_insight已计算好）──
        _s_residual = model_results.get('residual_insight', '')
        if _s_residual:
            _s_r2_recent = model_results.get('r_squared_recent')
            _s_r2_full   = model_results.get('r_squared', 0)
            _s_is_drop = _s_r2_recent is not None and (_s_r2_full - _s_r2_recent) > 0.25
            _s_res_style = 'card card-warn' if _s_is_drop else 'card card-info'
            _s_res_icon  = '🔍 残差分析 · 模型解释力预警' if _s_is_drop else '📐 残差分析 · 模型解释力说明'
            st.markdown(
                f'<div class="{_s_res_style}" style="margin-top:6px;font-size:0.86rem">'
                f'<b>{_s_res_icon}</b>'
                f'<div style="margin-top:6px;line-height:1.8">{_s_residual}</div>'
                f'</div>',
                unsafe_allow_html=True
            )

    # ---------- Part 2.5: 风险提示板块 ----------
    st.markdown('<div class="section-title">⚠️ 风险提示</div>', unsafe_allow_html=True)

    # ── 动态列宽兜底：预判左列/右列是否有内容 ──
    # 左列有内容：mixed/sector（收益拆解） 或 equity（有持仓的估值预警）
    _top10_for_check = holdings.get('top10', pd.DataFrame())
    _has_left_content = (
        model_type in ('mixed', 'sector')  # 收益拆解
        or (model_type == 'equity' and _top10_for_check is not None and not _top10_for_check.empty)
    )
    # 右列有内容：bond_ratio>0.10（久期压测） 或 mixed/sector有持仓数据（估值预警）
    _top10_for_right = holdings.get('top10', pd.DataFrame())
    _has_right_content = (
        bond_ratio > 0.10
        or (model_type in ('mixed', 'sector') and stock_ratio > 0.15
            and _top10_for_right is not None and not _top10_for_right.empty)
    )
    # 决定布局
    if _has_left_content and _has_right_content:
        _risk_col_l, _risk_col_r = st.columns([1, 1])
        _use_single_left  = False
        _use_single_right = False
    elif _has_left_content:
        _risk_col_l  = st.container()
        _risk_col_r  = None  # 占位，实际不会渲染右列
        _use_single_left  = True
        _use_single_right = False
    elif _has_right_content:
        _risk_col_l  = None
        _risk_col_r  = st.container()
        _use_single_left  = False
        _use_single_right = True
    else:
        _risk_col_l  = st.container()
        _risk_col_r  = None
        _use_single_left  = True
        _use_single_right = False

    # ── 左列 A：收益拆解（混合/行业型）或 前十大估值预警（权益型）──────
    if _risk_col_l is not None:
     with _risk_col_l:

        # A1. 收益拆解「功劳簿」（混合型 + 行业型）
        if model_type in ('mixed', 'sector'):
            _sector_res = model_results.get('sector') if model_type == 'sector' else None
            _decomp = performance_decomposition(
                model_results=model_results,
                sector_results=_sector_res,
                nav_df=nav_df,
                bm_df=bm_df,
            )

            # 动态标题：用「超额收益」（相对基准），避免与总收益混淆
            # _total_ret 是绝对总收益，_decomp['total_excess'] 是相对基准的超额
            _decomp_excess_val = _decomp.get('total_excess', 0.0) * 100
            _decomp_excess_color = '#e74c3c' if _decomp_excess_val >= 0 else '#27ae60'
            _decomp_excess_verb  = '超额（相对基准）' if _decomp_excess_val >= 0 else '落后（相对基准）'
            st.markdown(
                f'<div style="font-size:0.9rem;font-weight:700;color:#1a1a2e;margin-bottom:8px">'
                f'📦 超额收益拆解：基准以上 '
                f'<span style="color:{_decomp_excess_color};font-size:1.05rem">'
                f'{_decomp_excess_val:+.2f}%</span> {_decomp_excess_verb}'
                f'<span style="font-size:0.78rem;color:#888;font-weight:400;margin-left:8px">'
                f'（区间总收益 {_total_ret:+.1f}%，拆解为相对基准超额）</span></div>',
                unsafe_allow_html=True
            )

            # 一句话叙事
            st.markdown(
                f'<div style="background:#f0f4ff;border-left:3px solid #3498db;'
                f'border-radius:6px;padding:10px 14px;font-size:0.86rem;'
                f'color:#333;margin-bottom:10px;line-height:1.7">'
                f'💬 {_decomp["narrative"]}</div>',
                unsafe_allow_html=True
            )

            # 三段式可视化（迷你瀑布）
            _d_alloc  = _decomp['allocation']  * 100
            _d_sector = _decomp['sector_alpha'] * 100
            _d_resid  = _decomp['residual']    * 100
            _d_total  = _decomp['total_excess'] * 100
            # 残差改名：在量化实务中"无法解释的残差"包含打新、日内交易、非标等合理超额
            # 直接展示"模型残差"会让用户误以为是误差，改为"选股择时及其他超额"更准确
            _seg_labels = ['仓位择时', _decomp.get('sector_label','行业选股'), '选股择时及其他超额']
            _seg_vals   = [_d_alloc, _d_sector, _d_resid]
            _seg_colors = ['#27ae60' if v >= 0 else '#e74c3c' for v in _seg_vals]

            # 用 CSS 进度条绘制简洁三段图
            _max_abs = max(abs(v) for v in _seg_vals if abs(v) > 0.01) if any(abs(v) > 0.01 for v in _seg_vals) else 1.0
            _decomp_html = '<div style="margin-bottom:4px">'
            for _sl, _sv, _sc in zip(_seg_labels, _seg_vals, _seg_colors):
                _bar_w = abs(_sv) / _max_abs * 85 if _max_abs > 0 else 5
                _bar_w = max(_bar_w, 3)
                _val_str = f'{_sv:+.2f}%'
                _decomp_html += f'''
<div style="display:flex;align-items:center;margin-bottom:6px;gap:6px">
  <div style="width:80px;font-size:0.78rem;color:#555;text-align:right;flex-shrink:0">{_sl}</div>
  <div style="flex:1;background:#f0f2f5;border-radius:4px;height:18px;position:relative">
    <div style="background:{_sc};width:{_bar_w}%;height:18px;border-radius:4px;
         display:flex;align-items:center;padding-left:4px">
      <span style="font-size:0.73rem;color:white;font-weight:600;white-space:nowrap">{_val_str}</span>
    </div>
  </div>
</div>'''
            # 总超额合计
            _total_color = '#27ae60' if _d_total >= 0 else '#e74c3c'
            _decomp_html += f'''
<div style="border-top:1px dashed #ddd;padding-top:6px;display:flex;justify-content:space-between;
     font-size:0.82rem;color:#333;font-weight:600">
  <span>总超额收益</span>
  <span style="color:{_total_color}">{_d_total:+.2f}%</span>
</div>
<div style="font-size:0.7rem;color:#aaa;margin-top:3px">数据质量：{_decomp["data_quality"]}</div>
'''
            _decomp_html += '</div>'
            st.markdown(_decomp_html, unsafe_allow_html=True)

            # 残差说明小字（帮用户理解为什么有"其他超额"）
            if abs(_d_resid) > 0.5:
                _resid_icon = '📈' if _d_resid > 0 else '📉'
                _resid_note = (
                    f'{_resid_icon} <b>「选股择时及其他超额」占比较大（{_d_resid:+.2f}%）</b>：'
                    f'这部分是 Brinson 仓位/行业模型无法分解的残余收益，'
                    f'通常来自个股选择、日内交易、打新收益、非标资产等，'
                    f'并非模型误差——它代表经理在"选标的"和"把握时机"上的综合贡献。'
                )
                st.markdown(
                    f'<div style="background:#f8f9fa;border-radius:8px;padding:10px 14px;'
                    f'margin-top:6px;font-size:0.80rem;color:#555;line-height:1.7">'
                    f'{_resid_note}</div>',
                    unsafe_allow_html=True
                )

            # 功劳簿
            if _decomp['credit_lines']:
                st.markdown(
                    '<div style="font-size:0.82rem;font-weight:600;color:#444;'
                    'margin:10px 0 5px">🏆 功劳簿</div>',
                    unsafe_allow_html=True
                )
                for _cl in _decomp['credit_lines']:
                    st.markdown(
                        f'<div style="background:white;border-radius:7px;padding:8px 12px;'
                        f'margin-bottom:5px;font-size:0.82rem;color:#444;line-height:1.6;'
                        f'box-shadow:0 1px 4px rgba(0,0,0,.05)">{_cl}</div>',
                        unsafe_allow_html=True
                    )

        # A2. 对于权益型/行业型：显示前十大重仓股估值预警（放左列）
        elif model_type in ('equity',):
            _top10_df = holdings.get('top10', pd.DataFrame())
            if _top10_df is not None and not _top10_df.empty:
                _stock_col = next((c for c in ['股票代码', '代码', 'code', '证券代码']
                                   if c in _top10_df.columns), None)
                _name_col  = next((c for c in ['股票名称', '名称', 'name', '证券名称']
                                   if c in _top10_df.columns), None)
                if _stock_col:
                    _codes = _top10_df[_stock_col].astype(str).head(10).tolist()
                    _names = {}
                    if _name_col:
                        _names = dict(zip(
                            _top10_df[_stock_col].astype(str),
                            _top10_df[_name_col].astype(str)
                        ))

                    st.markdown(
                        '<div style="font-size:0.9rem;font-weight:700;color:#1a1a2e;margin-bottom:8px">'
                        '📊 前十大重仓股：估值风险预警</div>',
                        unsafe_allow_html=True
                    )
                    with st.spinner("拉取历史估值数据（PE分位）..."):
                        _valert = fetch_stock_valuation_alert(_codes)

                    # 为各股填入名称
                    for _v in _valert:
                        if _v['code'] in _names:
                            _v['name'] = _names[_v['code']]

                    # 统计摘要
                    _high_risk = [v for v in _valert if v['pe_percentile'] and v['pe_percentile'] >= 85]
                    _low_risk  = [v for v in _valert if v['pe_percentile'] and v['pe_percentile'] <= 15]
                    _summary_color = '#e74c3c' if len(_high_risk) >= 3 else '#e67e22' if len(_high_risk) >= 1 else '#27ae60'
                    _summary_text  = (f'⚠️ {len(_high_risk)} 只重仓股估值处于历史高位（>85%分位）' if _high_risk
                                      else f'✅ 重仓股整体估值合理，无高风险预警')

                    st.markdown(
                        f'<div style="background:#fff8f0;border-left:3px solid {_summary_color};'
                        f'border-radius:6px;padding:8px 12px;font-size:0.84rem;color:#333;'
                        f'margin-bottom:10px">{_summary_text}</div>',
                        unsafe_allow_html=True
                    )

                    # 显示估值分位图
                    _val_fig = plot_valuation_alert_chart(_valert, _names)
                    if _val_fig.data:
                        st.plotly_chart(_val_fig, use_container_width=True)

                    # QDII基金：加说明注释
                    _cross_cnt = sum(1 for v in _valert if v['risk_level'] in ('跨市场', '港股暂缺'))
                    if _cross_cnt > 0 and _is_qdii_fund:
                        st.markdown(
                            f'<div style="background:#f0f4ff;border-radius:6px;padding:7px 12px;'
                            f'font-size:0.78rem;color:#666;margin-bottom:8px">'
                            f'🌐 该QDII基金含 {_cross_cnt} 只境外股票（港股/美股/台股），'
                            f'当前接口仅支持A股和部分港股估值分析。境外持仓建议参考各交易所官方数据。'
                            f'</div>',
                            unsafe_allow_html=True
                        )

                    # 明细表
                    _vt_html = '<table style="width:100%;border-collapse:collapse;font-size:0.78rem">'
                    _vt_html += '<tr style="background:#1a1a2e;color:white">'
                    for _hh in ['股票', '当前PE', 'PE分位', 'PB', '风险级别']:
                        _vt_html += f'<th style="padding:6px 8px;text-align:left">{_hh}</th>'
                    _vt_html += '</tr>'
                    for _i, _v in enumerate(_valert):
                        _bg = '#fafafa' if _i % 2 == 0 else 'white'
                        _nm = _v.get('name') or _v['code']
                        # 跨市场/港股暂缺：PE/PB显示「—」，更清晰
                        _is_cross = _v['risk_level'] in ('跨市场', '港股暂缺')
                        _pe_str  = ('—' if _is_cross else
                                    (f"{_v['current_pe']:.1f}" if _v['current_pe'] else 'N/A'))
                        _pct_str = ('—' if _is_cross else
                                    (f"{_v['pe_percentile']:.0f}%" if _v['pe_percentile'] is not None else 'N/A'))
                        _pb_str  = ('—' if _is_cross else
                                    (f"{_v['current_pb']:.2f}" if _v['current_pb'] else 'N/A'))
                        _rl      = f"{_v['risk_icon']} {_v['risk_level']}"
                        _vt_html += (f'<tr style="background:{_bg}">'
                                     f'<td style="padding:5px 8px">{_nm[:8]}</td>'
                                     f'<td style="padding:5px 8px">{_pe_str}</td>'
                                     f'<td style="padding:5px 8px;font-weight:600">{_pct_str}</td>'
                                     f'<td style="padding:5px 8px">{_pb_str}</td>'
                                     f'<td style="padding:5px 8px">{_rl}</td>'
                                     f'</tr>')
                    _vt_html += '</table>'
                    st.markdown(_vt_html, unsafe_allow_html=True)
                else:
                    st.info('持仓数据中未找到股票代码列，无法拉取估值数据。')
            else:
                st.info('暂无前十大持仓数据（季报尚未披露或无持仓信息）。')

        else:
            st.markdown(
                '<div style="color:#aaa;font-size:0.83rem;padding:20px 0">当前基金类型暂无收益拆解数据。</div>',
                unsafe_allow_html=True
            )

    # ── 右列 B：前十大估值预警（混合/行业型）+ 债券压力测试 ──────────
    if _risk_col_r is not None:
     with _risk_col_r:

        # B1. 混合型/行业型的前十大估值预警（放右列）
        if model_type in ('mixed', 'sector'):
            _top10_df = holdings.get('top10', pd.DataFrame())
            _has_valert = False
            if _top10_df is not None and not _top10_df.empty:
                _stock_col = next((c for c in ['股票代码', '代码', 'code', '证券代码']
                                   if c in _top10_df.columns), None)
                _name_col  = next((c for c in ['股票名称', '名称', 'name', '证券名称']
                                   if c in _top10_df.columns), None)
                if _stock_col and stock_ratio > 0.15:  # 股票仓位>15%才做估值预警
                    _codes = _top10_df[_stock_col].astype(str).head(10).tolist()
                    _names = {}
                    if _name_col:
                        _names = dict(zip(
                            _top10_df[_stock_col].astype(str),
                            _top10_df[_name_col].astype(str)
                        ))
                    st.markdown(
                        '<div style="font-size:0.9rem;font-weight:700;color:#1a1a2e;margin-bottom:8px">'
                        '📊 前十大重仓股：估值风险预警</div>',
                        unsafe_allow_html=True
                    )
                    with st.spinner("拉取历史估值数据..."):
                        _valert = fetch_stock_valuation_alert(_codes)
                    for _v in _valert:
                        if _v['code'] in _names:
                            _v['name'] = _names[_v['code']]

                    _high_risk = [v for v in _valert if v['pe_percentile'] and v['pe_percentile'] >= 85]
                    _s_color = '#e74c3c' if len(_high_risk) >= 3 else '#e67e22' if _high_risk else '#27ae60'
                    _s_text  = (f'⚠️ {len(_high_risk)} 只重仓股处于历史高估区' if _high_risk
                                else '✅ 重仓股整体估值合理')
                    st.markdown(
                        f'<div style="background:#fff8f0;border-left:3px solid {_s_color};'
                        f'border-radius:6px;padding:8px 12px;font-size:0.84rem;color:#333;'
                        f'margin-bottom:10px">{_s_text}</div>',
                        unsafe_allow_html=True
                    )

                    _val_fig2 = plot_valuation_alert_chart(_valert, _names)
                    if _val_fig2.data:
                        st.plotly_chart(_val_fig2, use_container_width=True)
                    _has_valert = True

        # B2. 债券久期压力测试（债券型 + 混合型含债券仓位的情况）
        _bond_res_for_stress = model_results.get('bond') if isinstance(model_results, dict) else None
        if model_type == 'bond':
            _bond_res_for_stress = model_results

        if _bond_res_for_stress and bond_ratio > 0.10:
            _eff_dur = _bond_res_for_stress.get('duration_underlying',
                       _bond_res_for_stress.get('duration', 2.5)) or 2.5
            _eff_dur = max(_eff_dur, 0.1)
            _dur_source = _bond_res_for_stress.get('duration_source', 'regression')
            _dur_tag = '' if _dur_source in ('regression', 'three_factor') else ' （经验估算值）'

            # 三因子参数
            _st_ds  = _bond_res_for_stress.get('dur_short', 0.0)
            _st_dl  = _bond_res_for_stress.get('dur_long', 0.0)
            _st_cb  = _bond_res_for_stress.get('credit_beta', 0.0)
            _st_hc  = _bond_res_for_stress.get('has_credit_factor', False)

            st.markdown(
                '<div style="font-size:0.9rem;font-weight:700;color:#1a1a2e;margin-bottom:8px'
                + (';margin-top:16px' if model_type in ('mixed', 'sector') else '') + '">'
                '🧪 债券端：情景压力测试</div>',
                unsafe_allow_html=True
            )

            _stress = bond_stress_test(
                _eff_dur, bond_ratio,
                dur_short=_st_ds, dur_long=_st_dl,
                credit_beta=_st_cb, has_credit_factor=_st_hc
            )

            # 一句话叙事
            st.markdown(
                f'<div style="background:#f5f7ff;border-left:3px solid #3498db;'
                f'border-radius:6px;padding:10px 14px;font-size:0.84rem;color:#333;'
                f'margin-bottom:10px;line-height:1.7">'
                f'🔬 {_stress["narrative"]}</div>',
                unsafe_allow_html=True
            )

            _stress_mode = _stress.get('mode', 'single_factor')

            if _stress_mode == 'three_factor':
                # ── 三因子场景化矩阵展示 ──
                _sc_html = '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px">'
                for _sc in _stress['scenarios']:
                    if _sc.get('skip'):
                        continue
                    _fi   = _sc['fund_impact'] * 100
                    _icon = _sc['risk_icon']
                    _rl   = _sc['risk_level']
                    _sc_bg     = {'🟢':'#eafaf1','🟡':'#fffbea','🟠':'#fff4e5','🔴':'#fef0f0'}.get(_icon,'#f8f8f8')
                    _sc_border = {'🟢':'#27ae60','🟡':'#f39c12','🟠':'#e67e22','🔴':'#e74c3c'}.get(_icon,'#ccc')
                    # 拆解说明
                    _breakdown = []
                    if abs(_sc['impact_short']) > 0.0001:
                        _breakdown.append(f"短端: {_sc['impact_short']*100:+.3f}%")
                    if abs(_sc['impact_long']) > 0.0001:
                        _breakdown.append(f"长端: {_sc['impact_long']*100:+.3f}%")
                    if abs(_sc['impact_credit']) > 0.0001:
                        _breakdown.append(f"信用: {_sc['impact_credit']*100:+.3f}%")
                    _bd_str = ' | '.join(_breakdown) if _breakdown else ''

                    _sc_html += f'''
<div style="flex:1;min-width:140px;background:{_sc_bg};border:1px solid {_sc_border};
     border-radius:8px;padding:10px 8px;text-align:center">
  <div style="font-size:1.2rem">{_icon}</div>
  <div style="font-size:0.76rem;font-weight:600;color:#333;margin:2px 0">{_sc['name']}</div>
  <div style="font-size:0.68rem;color:#888;margin-bottom:4px;line-height:1.3">{_sc['desc']}</div>
  <div style="font-size:1.15rem;font-weight:700;color:{_sc_border}">{_fi:+.3f}%</div>
  <div style="font-size:0.68rem;color:#aaa;margin:2px 0">{_bd_str}</div>
  <div style="font-size:0.74rem;font-weight:600;color:{_sc_border};margin-top:3px">{_rl}</div>
</div>'''
                _sc_html += '</div>'
                st.markdown(_sc_html, unsafe_allow_html=True)

                # 公式说明
                st.markdown(
                    f'<div style="font-size:0.73rem;color:#aaa;padding:5px 0">'
                    f'📐 三因子压测：短端影响 = −β_short({abs(_st_ds):.2f}年) × ΔY_2Y × 仓位；'
                    f'长端影响 = −β_long({abs(_st_dl):.2f}年) × ΔY_10Y × 仓位；'
                    + (f'信用影响 = β_credit({_st_cb:.2f}) × ΔCS × 仓位；' if _st_hc else '')
                    + f'债券仓位 {bond_ratio*100:.0f}%{_dur_tag}</div>',
                    unsafe_allow_html=True
                )
            else:
                # ── 单因子：原有展示（兼容降级场景）──
                _sc_html = '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px">'
                for _sc in _stress['scenarios']:
                    _bp = _sc.get('bp', _sc.get('dy_short_bp', 0))
                    _fi = _sc['fund_impact'] * 100
                    _icon = _sc['risk_icon']
                    _rl   = _sc['risk_level']
                    _sc_bg = {'🟢':'#eafaf1','🟡':'#fffbea','🟠':'#fff4e5','🔴':'#fef0f0'}.get(_icon,'#f8f8f8')
                    _sc_border = {'🟢':'#27ae60','🟡':'#f39c12','🟠':'#e67e22','🔴':'#e74c3c'}.get(_icon,'#ccc')
                    _sc_html += f'''
<div style="flex:1;min-width:120px;background:{_sc_bg};border:1px solid {_sc_border};
     border-radius:8px;padding:10px;text-align:center">
  <div style="font-size:1.3rem">{_icon}</div>
  <div style="font-size:0.78rem;color:#666;margin:2px 0">利率↑{_bp}BP</div>
  <div style="font-size:1.1rem;font-weight:700;color:{_sc_border}">{_fi:+.2f}%</div>
  <div style="font-size:0.72rem;color:#888">净值影响</div>
  <div style="font-size:0.75rem;font-weight:600;color:{_sc_border};margin-top:3px">{_rl}</div>
</div>'''
                _sc_html += '</div>'
                st.markdown(_sc_html, unsafe_allow_html=True)

                st.markdown(
                    f'<div style="font-size:0.73rem;color:#aaa;padding:5px 0">'
                    f'📐 计算公式：净值影响 = −久期({_eff_dur:.1f}年{_dur_tag}) × ΔY × 债券仓位({bond_ratio*100:.0f}%)；'
                    f'仅为线性近似，实际因凸性有所保护。</div>',
                    unsafe_allow_html=True
                )

            # 综合预警（极端场景下股债双杀）
            _max_stress = abs(_stress['max_impact']) * 100
            if model_type in ('mixed', 'sector') and stock_ratio > 0.3:
                _stock_dd = abs(model_results.get('allocation_effect', 0) - model_results.get('excess_return', 0)) * 100
                if _max_stress > 1.5 and stock_ratio > 0.4:
                    st.markdown(
                        f'<div style="background:#fff0f0;border:1px solid #e74c3c;border-radius:8px;'
                        f'padding:10px 14px;font-size:0.84rem;color:#c0392b;margin-top:8px;line-height:1.7">'
                        f'⚠️ <b>股债双杀预警</b>：该基金股票仓位 {stock_ratio*100:.0f}%，'
                        f'同时债券端久期较长（{_eff_dur:.1f}年）。'
                        f'若市场同时出现股市下跌+利率上行（如流动性收紧），'
                        f'股债两端均承压，预计利率每升100BP债券端净值影响约 {_max_stress:.1f}%，'
                        f'需警惕双重风险叠加。</div>',
                        unsafe_allow_html=True
                    )

    # ---------- Part 4: 大白话诊断 ----------
    st.markdown('<div class="section-title">💬 第四部分：大白话诊断</div>', unsafe_allow_html=True)

    score = translation.get('score', 60)
    # score 已在雷达图计算后更新为加权平均分（见 Part 0）
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
        _fund_type_zh = {'equity':'主动权益','bond':'纯债固收','mixed':'股债混合','sector':'行业主题','qdii':'QDII跨市场'}.get(model_type,'')
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

        # ---- 性格标签展示（新增）----
        _tags = translation.get('tags', [])
        if _tags:
            _tag_html = ''.join([
                f'<span style="display:inline-block;padding:3px 10px;margin:3px;'
                f'background:#f0f4ff;border:1px solid #c5d0f5;border-radius:12px;'
                f'font-size:0.8rem;color:#2c3e80;font-weight:500">{t}</span>'
                for t in _tags
            ])
            st.markdown(
                f'<div style="margin-top:6px">'
                f'<span style="font-size:0.78rem;color:#888;margin-right:6px">性格标签：</span>'
                f'{_tag_html}</div>',
                unsafe_allow_html=True
            )

    # ---- 情绪指标 + 一致性预警（新增，在四维诊断之前）----
    _emotion = translation.get('emotion_note', '')
    _consist = translation.get('consistency_warn', '')

    if _emotion:
        # 根据趋势类型选择样式
        _trend_data = translation.get('_trend_data', {})
        _is_slump   = _trend_data.get('is_slump', False)
        _is_recover = _trend_data.get('is_recovering', False)
        if _is_slump:
            _emo_style = 'card card-warn'
            _emo_icon  = '📉 情绪指标 · 状态预警'
        elif _is_recover:
            _emo_style = 'card card-good'
            _emo_icon  = '📈 情绪指标 · 状态好转'
        else:
            _emo_style = 'card'
            _emo_icon  = '📊 情绪指标 · 近期走势'
        st.markdown(
            f'<div class="{_emo_style}" style="margin-bottom:8px;font-size:0.88rem">'
            f'<b>{_emo_icon}</b>'
            f'<div style="margin-top:6px;line-height:1.8">{_emotion}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

    if _consist:
        _consist_is_warn = '无效加杠杆' in _consist
        _consist_style = 'card card-warn' if _consist_is_warn else 'card card-info'
        st.markdown(
            f'<div class="{_consist_style}" style="margin-bottom:8px;font-size:0.88rem">'
            f'<b>🔗 一致性分析</b>'
            f'<div style="margin-top:6px;line-height:1.8">{_consist}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

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

    # ---------- 合规补丁（第4条）----------
    st.markdown("---")
    st.markdown("""
<div style="background:#f8f9fa;border-radius:10px;padding:16px 20px;
     border-left:3px solid #bdc3c7;font-size:0.78rem;color:#777;line-height:1.8">
<b>📋 合规声明与免责提示</b><br>
本报告由 DeepInFund 基于公开市场数据与学术量化模型自动生成，<b>不构成任何投资建议或买卖依据</b>。<br>
· <b>模型局限</b>：因子分析基于历史数据回溯，仅能反映过去的规律，不代表未来表现；估值预警与压力测试为数学模拟，不代表实际亏损必然发生。<br>
· <b>前视偏差说明</b>：本报告使用基金当前公开的业绩基准进行回溯分析。若基金历史上曾变更基准，Alpha/Beta 计算在变更前区间仅供参考，不代表历史所有时期的真实超额收益。<br>
· <b>数据来源</b>：净值数据来自天天基金，持仓数据来自季报公告，因子数据使用指数代理，与学术标准 Fama-French 因子可能存在偏差。<br>
· <b>投资有风险，入市需谨慎。</b> 基金过往业绩不代表未来表现，请结合自身风险承受能力做出独立判断，必要时请咨询持牌投资顾问。
</div>
""", unsafe_allow_html=True)



if __name__ == "__main__":
    main()
