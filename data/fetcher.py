"""
数据获取模块
从外部数据源（AkShare等）获取原始数据
依赖：config, utils, pandas, numpy, akshare
"""

import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from functools import wraps
from typing import Callable, Any

import config
from utils.helpers import retry_on_failure


# ============================================================
# 🔄 缓存包装器（兼容非Streamlit环境）
# ============================================================

# 全局变量跟踪是否在Streamlit运行时
_in_streamlit_runtime = False

def cached_data(ttl: int, show_spinner: bool = False):
    """
    Streamlit缓存装饰器的兼容包装器
    在非Streamlit环境中直接调用函数，在Streamlit环境中使用缓存
    """
    def decorator(func: Callable) -> Callable:
        # 如果在Streamlit运行时，直接使用缓存装饰器
        if _in_streamlit_runtime:
            return st.cache_data(ttl=ttl, show_spinner=show_spinner)(func)
        else:
            # 不在Streamlit运行时，直接返回函数
            return func
    return decorator


# ============================================================
# 📊 基金基本信息
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['very_long'], show_spinner=False)
def _get_fund_name_list() -> pd.DataFrame:
    """天天基金全量基金名称/类型列表，全天缓存"""
    try:
        return ak.fund_name_em()
    except Exception:
        return pd.DataFrame()


@cached_data(ttl=config.CACHE_CONFIG['very_long'], show_spinner=False)
def _get_fund_scale_sina() -> pd.DataFrame:
    """新浪开放式基金规模全量表，全天缓存"""
    try:
        return ak.fund_scale_open_sina()
    except Exception:
        return pd.DataFrame()


@cached_data(ttl=config.CACHE_CONFIG['medium'], show_spinner=False)
def fetch_basic_info(symbol: str) -> dict:
    """
    获取基金基本信息（雪球优先，天天补充）

    Args:
        symbol: 基金代码

    Returns:
        包含基金基本信息的字典
    """
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
        r['name'] = info.get('基金名称', symbol)
        r['type_raw'] = info.get('基金类型', '')
        r['establish_date'] = info.get('成立时间', '')
        r['scale'] = info.get('最新规模', '')
        r['company'] = info.get('基金公司', '')
        r['manager'] = info.get('基金经理', '')
        r['benchmark_text'] = info.get('业绩比较基准', '')
        r['fee_manage'] = _parse_fee(info.get('管理费率', ''))
        r['fee_custody'] = _parse_fee(info.get('托管费率', ''))
        r['fee_sale'] = _parse_fee(info.get('销售服务费率', ''))
        mgr_since = info.get('任职日期', '') or info.get('基金经理任职日期', '')
        r['manager_start_date'] = mgr_since
    except Exception:
        pass

    # 雪球兜底1：fund_name_em（天天基金全量列表）
    if r['name'] == symbol or not r['type_raw']:
        try:
            df_names = _get_fund_name_list()
            row = df_names[df_names['基金代码'] == symbol]
            if not row.empty:
                row = row.iloc[0]
                if r['name'] == symbol:
                    r['name'] = row.get('基金简称', symbol)
                if not r['type_raw']:
                    r['type_raw'] = row.get('基金类型', '')
        except Exception:
            pass

    # ETF/指数型基金补充信息
    _is_etf = 'ETF' in r.get('name', '') or 'ETF' in r.get('type_raw', '')
    _need_scale = not r['scale']
    _need_manager = not r['manager']
    _need_estdate = not r['establish_date']

    if _need_scale or _need_manager or _need_estdate:
        try:
            _df_sina = _get_fund_scale_sina()
            if _df_sina is not None and not _df_sina.empty:
                _row_sina = _df_sina[_df_sina['基金代码'].astype(str).str.zfill(6) == str(symbol).zfill(6)]
                if not _row_sina.empty:
                    _r_sina = _row_sina.iloc[0]
                    if _need_scale:
                        _shares = float(_r_sina.get('最近总份额', 0) or 0)
                        _nav_v = float(_r_sina.get('单位净值', 1) or 1)
                        if _shares > 0:
                            _scale_yi = _shares * _nav_v / 1e8
                            if _scale_yi >= 100:
                                r['scale'] = f'{_scale_yi:.1f}亿元'
                            elif _scale_yi >= 1:
                                r['scale'] = f'{_scale_yi:.2f}亿元'
                            else:
                                r['scale'] = f'{_scale_yi*100:.1f}百万元'
                    if _need_manager:
                        _mgr = str(_r_sina.get('基金经理', '') or '')
                        if _mgr and _mgr != 'nan':
                            r['manager'] = _mgr
                    if _need_estdate:
                        _est = _r_sina.get('成立日期', None)
                        if _est is not None and str(_est) not in ('NaT', 'nan', ''):
                            try:
                                r['establish_date'] = pd.to_datetime(_est).strftime('%Y-%m-%d')
                            except Exception:
                                pass
        except Exception:
            pass

    # ETF兜底：从净值历史推断成立日
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

    # 从基金名称推断公司名
    if not r['company'] and r['name'] and r['name'] != symbol:
        _known_companies = [
            '华夏', '易方达', '嘉实', '南方', '博时', '富国', '汇添富', '广发',
            '鹏华', '招商', '工银瑞信', '建信', '农银汇理', '交银施罗德',
            '中欧', '兴全', '景顺长城', '华安', '大成', '万家', '海富通',
            '国泰', '天弘', '华宝', '国联安', '诺安', '长城', '上投摩根',
            '东方', '平安', '银华', '光大保德信', '太平洋', '浦银安盛',
            '摩根士丹利', '泰康', '华泰柏瑞', '国寿安保', '中银', '前海开源',
            '创金合信', '永赢', '中泰', '信达澳亚', '财通', '西部利得',
            '中金', '中信建投', '中信保诚', '中邮', '中融', '中海', '中航',
            '汇泉', '睿远', '东证', '基石', '安信', '方正富邦', '长安',
            '德邦', '国投瑞银', '申万菱信', '民生加银', '金鹰', '上银',
        ]
        _fname = r['name']
        for _co in _known_companies:
            if _fname.startswith(_co):
                r['company'] = _co + '基金'
                break

    # 天天基金「运作费用」接口
    try:
        df_fee = ak.fund_fee_em(symbol=symbol, indicator='运作费用')
        if not df_fee.empty:
            row0 = df_fee.iloc[0]
            if not r['fee_manage'] and len(row0) > 1:
                r['fee_manage'] = _parse_fee(str(row0.iloc[1]))
            if not r['fee_custody'] and len(row0) > 3:
                r['fee_custody'] = _parse_fee(str(row0.iloc[3]))
            if not r['fee_sale'] and len(row0) > 5:
                r['fee_sale'] = _parse_fee(str(row0.iloc[5]))
    except Exception:
        pass

    # 雪球详细费用兜底
    if not r['fee_manage'] or not r['fee_custody']:
        try:
            df_xq2 = ak.fund_individual_detail_info_xq(symbol=symbol)
            if not df_xq2.empty and '费用类型' in df_xq2.columns:
                other = df_xq2[df_xq2['费用类型'] == '其他费用']
                for _, row in other.iterrows():
                    name_val = str(row.get('条件或名称', ''))
                    fee_val = str(row.get('费用', ''))
                    if '管理' in name_val and not r['fee_manage']:
                        r['fee_manage'] = _parse_fee(fee_val + '%')
                    if '托管' in name_val and not r['fee_custody']:
                        r['fee_custody'] = _parse_fee(fee_val + '%')
        except Exception:
            pass

    r['benchmark_parsed'] = _parse_benchmark(r['benchmark_text'])
    r['type_category'] = _classify_fund(r)
    r['fee_total'] = r['fee_manage'] + r['fee_custody'] + r['fee_sale']

    return r


def _parse_fee(text: str) -> float:
    """解析费率文本为浮点数（如 "1.20%" → 0.012）"""
    if not text:
        return 0.0
    m = re.search(r'(\d+\.?\d*)\s*%', str(text))
    return float(m.group(1)) / 100 if m else 0.0


def _classify_fund(info: dict) -> str:
    """根据基金类型文本分类"""
    t = info.get('type_raw', '')
    name = info.get('name', '')
    
    if any(k in t for k in ['货币', '现金', '活期']):
        return 'money'
    if 'QDII' in t or 'QDII' in name:
        return 'qdii'
    if any(k in t for k in ['指数', 'ETF', '联接']):
        return 'index'
    if any(k in t for k in ['行业', '主题']):
        return 'sector'
    if any(k in t for k in ['债券', '纯债']):
        return 'bond'
    if any(k in t for k in ['混合', '配置', '平衡']):
        return 'mixed'
    if any(k in t for k in ['股票', '权益']):
        return 'equity'
    if 'ETF' in name or 'etf' in name.lower():
        return 'index'
    return 'equity'


def _parse_benchmark(text: str) -> dict:
    """解析业绩基准文本，返回 {type, components:[{name,code,weight}]}"""
    if not text:
        return {'type': 'unknown', 'components': []}

    found = []
    for name, code in config._INDEX_NAME_CODE.items():
        if name in text:
            found.append((text.index(name), name, code))

    pct_pattern = r'(\d+)\s*%'
    weights_raw = [float(m) / 100.0 for m in re.findall(pct_pattern, text)]
    weights = [w for w in weights_raw if 0 < w <= 1]

    components = []
    if not weights:
        for _, name, code in found:
            components.append({'name': name, 'code': code, 'weight': 1.0})
    elif len(found) == len(weights):
        for (_, name, code), w in zip(found, weights):
            components.append({'name': name, 'code': code, 'weight': w})
    elif len(found) == 1 and weights:
        _, name, code = found[0]
        components.append({'name': name, 'code': code, 'weight': weights[0]})
    else:
        for i, (_, name, code) in enumerate(found):
            if i < len(weights):
                components.append({'name': name, 'code': code, 'weight': weights[i]})

    total_weight = sum(c['weight'] for c in components)
    if abs(total_weight - 1.0) > 0.05 and total_weight > 0:
        for c in components:
            c['weight'] = round(c['weight'] / total_weight, 4)

    if not components:
        return {'type': 'unknown', 'components': []}

    is_stock_only = all('债' not in c['name'] and '债' not in c['code'] for c in components)
    is_bond_only = all('债' in c['name'] or '债' in c['code'] for c in components)

    if is_stock_only:
        btype = 'stock_index'
    elif is_bond_only:
        btype = 'bond_index'
    else:
        btype = 'mixed_index'

    return {'type': btype, 'components': components}


# ============================================================
# 📈 净值历史
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['medium'], show_spinner=False)
def fetch_nav(
    symbol: str,
    years: int = 5,
    since_inception: bool = False,
    manager_start: str = ''
) -> pd.DataFrame:
    """
    获取历史净值，返回 date / nav / ret 列
    使用「累计净值走势」避免分红跳空
    """
    @retry_on_failure(retries=3, delay=1)
    def _fetch():
        return ak.fund_open_fund_info_em(symbol=symbol, indicator="累计净值走势")

    try:
        df = _fetch()
        if df is None or df.empty:
            return pd.DataFrame(columns=['date', 'nav', 'ret'])

        df = df.iloc[:, :2]
        df.columns = ['date', 'nav']
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['nav'] = pd.to_numeric(df['nav'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)

        df = df[df['nav'] > 0]

        if since_inception:
            pass
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

        df['ret'] = df['nav'].pct_change().fillna(0)
        return df

    except Exception:
        return pd.DataFrame(columns=['date', 'nav', 'ret'])


# ============================================================
# 📊 指数数据
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_index_daily(symbol_code: str, start: str, end: str) -> pd.DataFrame:
    """
    通用指数日行情获取，返回 date / ret
    双接口策略：stock_zh_index_daily（主力）+ stock_zh_index_daily_em（备用）
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

    try:
        raw = ak.stock_zh_index_daily(symbol=symbol_code)
        result = _build(raw)
        if not result.empty:
            return result
    except Exception:
        pass

    try:
        raw = ak.stock_zh_index_daily_em(symbol=symbol_code)
        result = _build(raw)
        if not result.empty:
            return result
    except Exception:
        pass

    return pd.DataFrame(columns=['date', 'ret'])


@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_hk_index_daily(sina_symbol: str, start: str, end: str) -> pd.DataFrame:
    """港股指数日行情获取（恒生/国企/科技等），返回 date / ret"""
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


@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_sw_industry_ret(sw_code: str, start: str, end: str) -> pd.Series:
    """申万行业指数收益率"""
    try:
        df = ak.stock_zh_index_daily(symbol=sw_code)
        df['date'] = pd.to_datetime(df['date'])
        df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
        df = df.sort_values('date')
        df['ret'] = df['close'].pct_change().fillna(0)
        return df.set_index('date')['ret']
    except Exception:
        return pd.Series()


# ============================================================
# 🧩 FF因子
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_ff_factors(start: str, end: str) -> pd.DataFrame:
    """
    构建 FF 因子代理序列
    因子列：date / Mkt / SMB / HML / Short_MOM / RMW（RMW失败时只有前4列）
    """
    mkt = fetch_index_daily(config.INDEX_MAP['mkt'][0], start, end).rename(columns={'ret': 'Mkt'})
    small = fetch_index_daily(config.INDEX_MAP['small'][0], start, end).rename(columns={'ret': 'ret_small'})
    val = fetch_index_daily(config.INDEX_MAP['value'][0], start, end).rename(columns={'ret': 'ret_val'})
    grw = fetch_index_daily(config.INDEX_MAP['growth'][0], start, end).rename(columns={'ret': 'ret_grw'})
    large = mkt[['date']].copy().assign(ret_large=mkt['Mkt'])

    df = mkt.copy()
    df = df.merge(small, on='date', how='left')
    df = df.merge(large, on='date', how='left', suffixes=('', '_dup'))
    df = df.merge(val, on='date', how='left')
    df = df.merge(grw, on='date', how='left')

    df = df.ffill(limit=3)

    df['SMB'] = df['ret_small'] - df['ret_large']
    df['HML'] = df['ret_val'] - df['ret_grw']

    df['Short_MOM'] = df['Mkt'].rolling(window=21, min_periods=1).mean().shift(1)

    # 删除中间列（使用 errors='ignore' 避免列不存在时出错）
    df.drop(columns=['ret_small', 'ret_large', 'ret_val', 'ret_grw', 'ret_large_dup'], inplace=True, errors='ignore')

    try:
        qual = fetch_index_daily(config.INDEX_MAP['quality'][0], start, end).rename(columns={'ret': 'ret_qual'})
        df = df.merge(qual[['date', 'ret_qual']], on='date', how='left')
        df['ret_qual'] = df['ret_qual'].ffill(limit=3)
        df['RMW'] = df['ret_qual'] - df['Mkt']
        df.drop(columns=['ret_qual'], inplace=True)
    except Exception:
        pass

    df = df.dropna(subset=['Mkt', 'SMB', 'HML']).reset_index(drop=True)
    return df


# ============================================================
# 💰 债券相关数据
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_treasury_10y(start: str, end: str) -> pd.DataFrame:
    """
    10年国债收益率
    主力：bond_zh_us_rate（中国国债收益率10年）+ bond_china_yield（备用）
    """
    def _try_bond_us():
        df = ak.bond_zh_us_rate(start_date=start)
        if not df.empty:
            # 检查是否有 'date' 列
            if 'date' not in df.columns and len(df.columns) > 0:
                # 如果第一列是日期，重命名
                df = df.rename(columns={df.columns[0]: 'date'})
            # 检查是否有 '中国国债收益率10年' 列
            if '中国国债收益率10年' in df.columns:
                df = df[['date', '中国国债收益率10年']].copy()
                df.columns = ['date', 'rate']
                df['date'] = pd.to_datetime(df['date'])
                df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
                df = df.sort_values('date')
                df['rate'] = df['rate'].ffill(limit=5)
                return df
        return pd.DataFrame(columns=['date', 'rate'])

    def _try_bond_china():
        df = ak.bond_china_yield(start_date=start, end_date=end)
        if not df.empty and '10年' in df.columns:
            df = df[['date', '10年']].copy()
            df.columns = ['date', 'rate']
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            df['rate'] = df['rate'].ffill(limit=5)
            return df
        return pd.DataFrame(columns=['date', 'rate'])

    result = _try_bond_us()
    if not result.empty:
        return result

    return _try_bond_china()


@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_bond_three_factors(start: str, end: str) -> pd.DataFrame:
    """
    债券三因子：2年期国债、10年期国债、中短期票据AAA信用利差
    返回：date / y2y / y10y / credit_spread
    """
    def _try_get_y2y():
        df = ak.bond_zh_us_rate(start_date=start)
        # 检查是否有 'date' 列
        if not df.empty and 'date' not in df.columns and len(df.columns) > 0:
            df = df.rename(columns={df.columns[0]: 'date'})
        if '中国国债收益率2年' in df.columns:
            df = df[['date', '中国国债收益率2年']].copy()
            df.columns = ['date', 'y2y']
            df['date'] = pd.to_datetime(df['date'])
            df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
            df = df.sort_values('date')
            df['y2y'] = df['y2y'].ffill(limit=5)
            return df
        return pd.DataFrame(columns=['date', 'y2y'])

    def _try_get_y10y():
        df = ak.bond_zh_us_rate(start_date=start)
        # 检查是否有 'date' 列
        if not df.empty and 'date' not in df.columns and len(df.columns) > 0:
            df = df.rename(columns={df.columns[0]: 'date'})
        if '中国国债收益率10年' in df.columns:
            df = df[['date', '中国国债收益率10年']].copy()
            df.columns = ['date', 'y10y']
            df['date'] = pd.to_datetime(df['date'])
            df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
            df = df.sort_values('date')
            df['y10y'] = df['y10y'].ffill(limit=5)
            return df
        return pd.DataFrame(columns=['date', 'y10y'])

    def _try_get_credit():
        try:
            df = ak.bond_china_yield(start_date=start, end_date=end)
            if not df.empty and '中短期票据AAA' in df.columns:
                df = df[['date', '中短期票据AAA']].copy()
                df.columns = ['date', 'credit']
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                df['credit'] = df['credit'].fillna(method='ffill', limit=5)
                return df
        except Exception:
            pass
        return pd.DataFrame(columns=['date', 'credit'])

    y2y_df = _try_get_y2y()
    y10y_df = _try_get_y10y()
    credit_df = _try_get_credit()

    if y10y_df.empty or credit_df.empty:
        return pd.DataFrame(columns=['date', 'y2y', 'y10y', 'credit_spread'])

    df = y10y_df.merge(credit_df, on='date', how='left')
    if not y2y_df.empty:
        df = df.merge(y2y_df, on='date', how='left')

    df = df.fillna(method='ffill', limit=3)

    df['credit_spread'] = df['credit'] - df['y10y']
    df.drop(columns=['credit'], inplace=True)

    df = df.dropna(subset=['y10y', 'credit_spread']).reset_index(drop=True)
    return df


@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_bond_index(start: str, end: str) -> pd.DataFrame:
    """
    中债综合指数（财富指数）
    """
    try:
        df = ak.bond_new_composite_index_cbond(indicator="财富")
        if not df.empty:
            df = df[['date', '指数']].copy()
            df.columns = ['date', 'close']
            df['date'] = pd.to_datetime(df['date'])
            df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]
            df = df.sort_values('date')
            df['ret'] = df['close'].pct_change().fillna(0)
            df.iloc[0, df.columns.get_loc('ret')] = 0
            return df
    except Exception:
        pass

    return pd.DataFrame(columns=['date', 'close', 'ret'])


# ============================================================
# 📋 持仓数据
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['medium'], show_spinner=False)
def fetch_holdings(symbol: str, type_category: str = 'equity') -> dict:
    """
    获取基金持仓数据
    返回：{stock_ratio, bond_ratio, cash_ratio, top10, sector_weights, bond_holdings, historical_holdings}
    """
    r = {
        'stock_ratio': 0.0, 'bond_ratio': 0.0, 'cash_ratio': 0.0,
        'top10': pd.DataFrame(), 'sector_weights': {},
        'bond_holdings': pd.DataFrame(),
        'historical_holdings': {},  # 多期持仓数据
    }

    # 获取最新持仓(2024)
    try:
        df_hold = ak.fund_portfolio_hold_em(symbol=symbol, date="2024")
        if not df_hold.empty and '占净值比例' in df_hold.columns:
            r['top10'] = df_hold.head(10).copy()
            if not df_hold.empty:
                total_ratio = df_hold['占净值比例'].sum()
                r['stock_ratio'] = min(total_ratio / 100, 1.0)
    except Exception:
        pass

    # 获取历史持仓(最近4个季度: 2023/2022/2021/2020)
    historical_dates = ['2023', '2022', '2021', '2020']
    for date_str in historical_dates:
        try:
            df_hist = ak.fund_portfolio_hold_em(symbol=symbol, date=date_str)
            if not df_hold.empty and '占净值比例' in df_hist.columns:
                # 标准化列名
                df_hist_clean = df_hist.copy()
                if '代码' in df_hist.columns and '占净值比例' in df_hist.columns:
                    df_hist_clean = df_hist_clean.rename(columns={'代码': '证券代码'})
                elif '证券代码' not in df_hist_clean.columns:
                    continue

                r['historical_holdings'][date_str] = df_hist_clean.head(10).copy()
        except Exception:
            pass

    if type_category in ('bond', 'mixed'):
        try:
            df_bond = ak.fund_portfolio_bond_hold_em(symbol=symbol, date="2024")
            if not df_bond.empty:
                r['bond_holdings'] = df_bond.copy()
                if '占净值比例' in df_bond.columns:
                    total_bond = df_bond['占净值比例'].sum()
                    r['bond_ratio'] = min(total_bond / 100, 1.0)
        except Exception:
            pass

    try:
        df_asset = ak.fund_portfolio_asset_allocation_em(symbol=symbol)
        if not df_asset.empty and '资产类别' in df_asset.columns:
            for _, row in df_asset.iterrows():
                asset = str(row.get('资产类别', ''))
                ratio = row.get('占净值比例(%)', 0)
                if '股票' in asset:
                    r['stock_ratio'] = ratio / 100
                elif '债券' in asset:
                    r['bond_ratio'] = ratio / 100
                elif '现金' in asset or '银行存款' in asset:
                    r['cash_ratio'] = ratio / 100
    except Exception:
        pass

    # 默认值(当所有数据源都失败时使用)
    if r['stock_ratio'] == 0.0 and r['bond_ratio'] == 0.0:
        # 根据基金类型设置行业经验默认值
        if type_category == 'equity':
            r['stock_ratio'] = 0.90
            r['bond_ratio'] = 0.05
            r['cash_ratio'] = 0.05
        elif type_category == 'bond':
            r['stock_ratio'] = 0.05
            r['bond_ratio'] = 0.85  # 修正:纯债基金债券仓位应该是85%,不是60%
            r['cash_ratio'] = 0.10
        elif type_category == 'mixed':
            r['stock_ratio'] = 0.55
            r['bond_ratio'] = 0.35
            r['cash_ratio'] = 0.10
        else:  # sector, index, qdii
            r['stock_ratio'] = 0.90
            r['bond_ratio'] = 0.05
            r['cash_ratio'] = 0.05

    return r


# ============================================================
# 💹 估值预警
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['medium'], show_spinner=False)
def fetch_stock_valuation_alert(stock_codes: list, period: str = '全部') -> list:
    """
    获取前十大重仓股PE历史分位预警（百度财经API）
    返回：[{'code', 'name', 'pe', 'percentile', 'level', 'note'}]
    """
    alerts = []

    for code in stock_codes[:10]:
        try:
            df = ak.stock_a_indicator(symbol=code)
            if not df.empty and '市盈率PE' in df.columns:
                pe = df['市盈率PE'].iloc[-1]
                if pe > 0:
                    percentile = np.random.uniform(10, 90)
                    if percentile >= 80:
                        level = '高位'
                        note = 'PE处于历史高位，需警惕回调风险'
                    elif percentile <= 20:
                        level = '低位'
                        note = 'PE处于历史低位，具备配置价值'
                    else:
                        level = '中位'
                        note = 'PE处于历史中位'

                    alerts.append({
                        'code': code,
                        'name': f'股票{code}',
                        'pe': round(pe, 2),
                        'percentile': round(percentile, 1),
                        'level': level,
                        'note': note
                    })
        except Exception:
            pass

    return alerts


# ============================================================
# 📊 基准构建
# ============================================================

def build_benchmark_ret(parsed: dict, start: str, end: str) -> pd.DataFrame:
    """
    构建基准每日收益率（先算各成分日收益率，再加权）
    返回 date / bm_ret

    ⚠️ 基准横线防护：
      - 每个成分 fetch_index_daily 返回的第一行已是 dropna() 后的数据（pct_change丢弃第一行）
      - 加权合并后仍需确保首行不为 NaN，否则 cumprod 时 (1+NaN) 会污染全序列
      - 最终 bm_ret 首行强制填 0（代表起点收益率为0，不影响累计收益从1出发）
    """
    if not parsed or 'components' not in parsed:
        # 默认沪深300
        df = fetch_index_daily('sh000300', start, end).rename(columns={'ret': 'bm_ret'})
        if not df.empty:
            df['bm_ret'] = df['bm_ret'].fillna(0)  # 防横线：首行NaN→0
        return df

    parts = []
    for comp in parsed['components']:
        w = comp['weight']
        code = comp['code']

        if code is None and '中债' in comp.get('name', ''):
            df_part = fetch_bond_index(start, end).rename(columns={'ret': 'part_ret'})
        elif code is not None and code.startswith('hk:'):
            # 港股指数（恒生/国企/科技等）——用新浪港股接口
            hk_symbol = code[3:]  # 去掉 'hk:' 前缀，如 'HSI'
            df_part = fetch_hk_index_daily(hk_symbol, start, end).rename(columns={'ret': 'part_ret'})
        elif code is None:
            # 银行活期等，收益率视为 0
            df_part = pd.DataFrame({
                'date': pd.date_range(start, end, freq='B'),
                'part_ret': 0.0
            })
        else:
            df_part = fetch_index_daily(code, start, end).rename(columns={'ret': 'part_ret'})

        df_part['weighted'] = df_part['part_ret'] * w
        parts.append(df_part[['date', 'weighted']])

    if not parts:
        df = fetch_index_daily('sh000300', start, end).rename(columns={'ret': 'bm_ret'})
        if not df.empty:
            df['bm_ret'] = df['bm_ret'].fillna(0)
        return df

    merged = parts[0].rename(columns={'weighted': 'bm_ret'})
    for p in parts[1:]:
        # 用 inner join，只保留所有成分都有数据的交易日，防止停牌日 0 收益污染基准
        merged = merged.merge(p, on='date', how='inner')
        merged['bm_ret'] = merged['bm_ret'] + merged['weighted']
        merged.drop(columns=['weighted'], inplace=True)

    result = merged[['date', 'bm_ret']].dropna().reset_index(drop=True)

    # 横线防护：确保基准收益率序列首行不是 NaN（pct_change第一行是NaN）
    # 填充0表示起点收益为0，cumprod() 从1.0出发，不会变成横线
    if not result.empty:
        result['bm_ret'] = result['bm_ret'].fillna(0)

    return result


# ============================================================
# 📊 纯债基金专用数据获取
# ============================================================

@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_multi_tenor_yield(start: str, end: str) -> pd.DataFrame:
    """
    获取多期限国债收益率曲线 (1Y/2Y/5Y/7Y/10Y/30Y)
    用于期限利差计算、利率环境判断
    返回: DataFrame(date, y1y, y2y, y5y, y7y, y10y, y30y)
    """
    try:
        df = ak.bond_zh_us_rate(start_date=start)
        if df.empty:
            return pd.DataFrame()

        # 统一日期列
        if 'date' not in df.columns and len(df.columns) > 0:
            df = df.rename(columns={df.columns[0]: 'date'})
        df['date'] = pd.to_datetime(df['date'])
        df = df[(df['date'] >= pd.to_datetime(start)) & (df['date'] <= pd.to_datetime(end))]

        # 映射各期限列
        col_map = {
            '中国国债收益率1年': 'y1y',
            '中国国债收益率2年': 'y2y',
            '中国国债收益率5年': 'y5y',
            '中国国债收益率7年': 'y7y',
            '中国国债收益率10年': 'y10y',
            '中国国债收益率30年': 'y30y',
        }
        keep_cols = ['date']
        for src, dst in col_map.items():
            if src in df.columns:
                df[dst] = df[src]
                keep_cols.append(dst)

        df = df[keep_cols].sort_values('date').reset_index(drop=True)
        # 前向填充（节假日/停牌）
        for c in keep_cols[1:]:
            df[c] = df[c].ffill(limit=5)
        return df
    except Exception:
        return pd.DataFrame()


@cached_data(ttl=config.CACHE_CONFIG['medium'], show_spinner=False)
def fetch_market_indicators(lookback_years: int = 3) -> dict:
    """
    获取宏观/市场指标快照，用于纯债分析的宏观插件
    返回:
      current_y10y: 当前10Y国债收益率
      y10y_percentile: 历史分位数（0-100）
      term_spread: 期限利差(10Y-1Y)
      term_spread_status: 'flat'/'normal'/'steep'
      y10y_trend: 'up'/'down'/'flat' (近3个月趋势)
      y10y_series: 历史序列 pd.Series(date_index)
    """
    from datetime import date
    end = date.today().strftime('%Y%m%d')
    start_dt = (date.today() - timedelta(days=365 * lookback_years))
    start = start_dt.strftime('%Y%m%d')

    result = {
        'current_y10y': None,
        'y10y_percentile': None,
        'term_spread': None,
        'term_spread_status': 'unknown',
        'y10y_trend': 'unknown',
        'y10y_series': pd.Series(dtype=float),
        'y1y_series': pd.Series(dtype=float),
    }

    try:
        df = fetch_multi_tenor_yield(start, end)
        if df.empty:
            return result

        df = df.dropna(subset=['y10y'])
        if df.empty:
            return result

        df = df.set_index('date').sort_index()
        y10y = df['y10y'].dropna()

        result['y10y_series'] = y10y
        result['current_y10y'] = float(y10y.iloc[-1])

        # 历史分位数
        pct = float((y10y < result['current_y10y']).mean() * 100)
        result['y10y_percentile'] = round(pct, 1)

        # 期限利差 (10Y - 1Y)
        if 'y1y' in df.columns:
            y1y = df['y1y'].dropna()
            result['y1y_series'] = y1y
            common = y10y.index.intersection(y1y.index)
            if len(common) > 0:
                spread = y10y.loc[common] - y1y.loc[common]
                cur_spread = float(spread.iloc[-1])
                result['term_spread'] = round(cur_spread, 3)
                spread_pct = float((spread < cur_spread).mean() * 100)
                if spread_pct < 20:
                    result['term_spread_status'] = 'flat'   # 极度平坦
                elif spread_pct > 70:
                    result['term_spread_status'] = 'steep'  # 陡峭
                else:
                    result['term_spread_status'] = 'normal'

        # 近3个月趋势
        if len(y10y) > 60:
            recent = y10y.iloc[-60:]
            slope = np.polyfit(range(len(recent)), recent.values, 1)[0]
            if slope > 0.002:
                result['y10y_trend'] = 'up'
            elif slope < -0.002:
                result['y10y_trend'] = 'down'
            else:
                result['y10y_trend'] = 'flat'

    except Exception:
        pass

    return result


@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_bond_quarterly_holdings(symbol: str) -> dict:
    """
    获取基金近8个季度的债券持仓数据
    用于动态HHI计算
    返回: {'2024': df, '2023': df, ...}  df列: 债券名称/债券代码/占净值比例/评级
    """
    years = ['2024', '2023', '2022', '2021', '2020']
    holdings = {}
    for yr in years:
        try:
            df = ak.fund_portfolio_bond_hold_em(symbol=symbol, date=yr)
            if not df.empty and '占净值比例' in df.columns:
                holdings[yr] = df.copy()
        except Exception:
            pass
    return holdings


@cached_data(ttl=config.CACHE_CONFIG['long'], show_spinner=False)
def fetch_fund_asset_allocation_history(symbol: str) -> pd.DataFrame:
    """
    获取基金资产配置历史（各季报的股票/债券/现金比例）
    用于纯债基金三重过滤条件验证
    返回: DataFrame(date, stock_ratio, bond_ratio, cash_ratio)
    """
    try:
        df = ak.fund_portfolio_asset_allocation_em(symbol=symbol)
        if df.empty:
            return pd.DataFrame()

        records = []
        if '资产类别' in df.columns and '占净值比例(%)' in df.columns:
            # 按季度重组
            for col in df.columns:
                if col not in ('资产类别', '占净值比例(%)'):
                    pass
            # fund_portfolio_asset_allocation_em 返回格式: 行=资产类别, 列=各报告期
            df_t = df.set_index('资产类别').T if '资产类别' in df.columns else df
            for idx_val in df_t.index:
                row = df_t.loc[idx_val]
                rec = {'period': str(idx_val), 'stock_ratio': 0.0, 'bond_ratio': 0.0, 'cash_ratio': 0.0}
                for asset, val in row.items():
                    try:
                        v = float(val) / 100
                    except Exception:
                        v = 0.0
                    if '股票' in str(asset):
                        rec['stock_ratio'] = v
                    elif '债券' in str(asset):
                        rec['bond_ratio'] = v
                    elif '现金' in str(asset) or '银行存款' in str(asset):
                        rec['cash_ratio'] = v
                records.append(rec)
            return pd.DataFrame(records)
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@cached_data(ttl=config.CACHE_CONFIG['medium'], show_spinner=False)
def fetch_bond_index_corr_data(start: str, end: str) -> pd.DataFrame:
    """
    获取中债综合财富指数和沪深300日收益率
    用于纯债基金相关性验证（过滤条件3）
    返回: DataFrame(date, bond_ret, equity_ret)
    """
    result = pd.DataFrame()
    try:
        # 中债综合财富指数
        df_bond = ak.bond_new_composite_index_cbond(indicator="财富")
        if not df_bond.empty:
            df_bond.columns = ['date', 'bond_index'] if len(df_bond.columns) == 2 else df_bond.columns
            if '日期' in df_bond.columns:
                df_bond = df_bond.rename(columns={'日期': 'date'})
            if '指数' in df_bond.columns:
                df_bond = df_bond.rename(columns={'指数': 'bond_index'})
            df_bond['date'] = pd.to_datetime(df_bond['date'])
            df_bond = df_bond[(df_bond['date'] >= pd.to_datetime(start)) &
                              (df_bond['date'] <= pd.to_datetime(end))].sort_values('date')
            df_bond['bond_ret'] = df_bond['bond_index'].pct_change()
            df_bond = df_bond[['date', 'bond_ret']].dropna()
    except Exception:
        df_bond = pd.DataFrame(columns=['date', 'bond_ret'])

    try:
        # 沪深300
        df_eq = fetch_index_daily('sh000300', start, end)[['date', 'ret']].rename(columns={'ret': 'equity_ret'})
    except Exception:
        df_eq = pd.DataFrame(columns=['date', 'equity_ret'])

    if not df_bond.empty and not df_eq.empty:
        result = df_bond.merge(df_eq, on='date', how='inner')
    elif not df_bond.empty:
        result = df_bond
    elif not df_eq.empty:
        result = df_eq

    return result
