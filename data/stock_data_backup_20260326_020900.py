"""
股票数据模块
获取股票行业分类、基本面数据等
依赖：config, utils, pandas, numpy, akshare
约束：不能调用Streamlit
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime

import config
from utils.helpers import retry_on_failure


# ============================================================
# 🗂️ 股票→行业映射表
# ============================================================

_STOCK_TO_INDUSTRY_CACHE_FILE = Path(__file__).parent.parent / '.workbuddy' / 'cache' / 'stock_industry_mapping.json'


@retry_on_failure(retries=3, delay=2)
def build_stock_industry_mapping() -> Dict[str, Dict]:
    """
    构建股票代码→申万行业映射表

    Returns:
        {
            '000001': {'industry_code': '801180', 'industry_name': '医药生物'},
            '600000': {'industry_code': '801150', 'industry_name': '非银金融'},
            ...
        }

    注意：
        - 调用 ak.index_component_sw 获取31个申万一级行业成分股
        - 接口偶尔超时,已集成重试机制
        - 建议本地缓存,避免重复拉取
    """
    # 申万一级行业代码表
    sw_industries = {
        '801010': '农林牧渔',
        '801020': '采掘',
        '801030': '化工',
        '801040': '钢铁',
        '801050': '有色金属',
        '801080': '电子',
        '801110': '计算机',
        '801120': '传媒',
        '801130': '通信',
        '801140': '银行',
        '801150': '非银金融',
        '801160': '房地产',
        '801170': '建筑材料',
        '801180': '建筑装饰',
        '801710': '建筑装饰',
        '801720': '机械设备',
        '801730': '电气设备',
        '801740': '国防军工',
        '801750': '汽车',
        '801760': '公用事业',
        '801770': '电力设备',
        '801780': '交通运输',
        '801790': '综合',
        '801880': '医药生物',
        '801890': '休闲服务',
        '801980': '食品饮料',
    }

    mapping = {}
    import akshare as ak

    for ind_code, ind_name in sw_industries.items():
        try:
            df = ak.index_component_sw(symbol=ind_code)
            if not df.empty and '证券代码' in df.columns:
                for stock_code in df['证券代码']:
                    # 统一转换为6位字符串
                    code_str = str(stock_code).zfill(6)

                    if code_str not in mapping:
                        mapping[code_str] = {
                            'industry_code': ind_code,
                            'industry_name': ind_name
                        }
        except Exception as e:
            print(f"⚠️ 获取行业 {ind_code} 成分股失败: {str(e)[:50]}")
            continue

    print(f"✅ 股票→行业映射表构建完成,覆盖 {len(mapping)} 只股票")
    return mapping


def load_stock_industry_mapping(force_refresh: bool = False) -> Dict[str, Dict]:
    """
    加载股票→行业映射表(优先从缓存读取)

    Args:
        force_refresh: 强制刷新缓存

    Returns:
        股票代码→行业映射字典
    """
    # 尝试从缓存读取
    if not force_refresh and _STOCK_TO_INDUSTRY_CACHE_FILE.exists():
        try:
            with open(_STOCK_TO_INDUSTRY_CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_mapping = json.load(f)

            # 检查缓存是否过期(30天)
            cache_mtime = datetime.fromtimestamp(_STOCK_TO_INDUSTRY_CACHE_FILE.stat().st_mtime)
            cache_age = (datetime.now() - cache_mtime).days

            if cache_age < 30:
                print(f"✅ 从缓存加载股票→行业映射表(缓存时间: {cache_age}天前)")
                return cached_mapping
            else:
                print(f"⚠️ 缓存过期({cache_age}天),重新构建")
        except Exception as e:
            print(f"⚠️ 读取缓存失败: {str(e)[:50]}")

    # 构建新映射表
    mapping = build_stock_industry_mapping()

    # 保存到缓存
    try:
        _STOCK_TO_INDUSTRY_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_STOCK_TO_INDUSTRY_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        print(f"✅ 股票→行业映射表已缓存到 {_STOCK_TO_INDUSTRY_CACHE_FILE}")
    except Exception as e:
        print(f"⚠️ 缓存保存失败: {str(e)[:50]}")

    return mapping


# ============================================================
# 📊 行业权重计算
# ============================================================

def calculate_industry_weights(
    holdings_df: pd.DataFrame,
    stock_mapping: Dict[str, Dict],
    top_n: int = 10
) -> Dict:
    """
    计算持仓的行业权重分布

    Args:
        holdings_df: 持仓DataFrame,包含 '证券代码' 和 '占净值比例' 列
        stock_mapping: 股票代码→行业映射字典
        top_n: 返回前N大行业

    Returns:
        {
            'industry_weights': {'食品饮料': 25.3, '医药生物': 18.7, ...},  # 行业权重
            'top_industries': [{'industry': '食品饮料', 'weight': 25.3}, ...],  # 前N大行业
            'concentration': '中度集中',  # 行业集中度
            'note': '前三大行业占比61.2%,行业配置较为集中',
        }
    """
    if holdings_df.empty:
        return {
            'industry_weights': {},
            'top_industries': [],
            'concentration': '数据不足',
            'note': '持仓数据为空,无法计算行业权重'
        }

    # 检查必需列
    required_cols = ['证券代码', '占净值比例']
    for col in required_cols:
        if col not in holdings_df.columns:
            return {
                'industry_weights': {},
                'top_industries': [],
                'concentration': '数据不足',
                'note': f'缺失必需列: {col}'
            }

    # 计算行业权重
    industry_weights = {}
    total_ratio = 0.0

    for _, row in holdings_df.iterrows():
        stock_code = str(row['证券代码']).zfill(6)
        ratio = float(row['占净值比例'])
        industry_info = stock_mapping.get(stock_code)

        if industry_info:
            industry_name = industry_info['industry_name']
            if industry_name not in industry_weights:
                industry_weights[industry_name] = 0.0
            industry_weights[industry_name] += ratio
            total_ratio += ratio

    # 排序并取前N
    sorted_industries = sorted(
        industry_weights.items(),
        key=lambda x: x[1],
        reverse=True
    )

    top_industries = [
        {'industry': name, 'weight': round(weight, 2)}
        for name, weight in sorted_industries[:top_n]
    ]

    # 计算集中度
    if total_ratio < 10:
        concentration = '分散'
    elif total_ratio < 50:
        concentration = '中度集中'
    else:
        concentration = '高度集中'

    # 前三大行业占比
    top3_ratio = sum(weight for _, weight in sorted_industries[:3])

    # 生成解读
    if len(sorted_industries) >= 3:
        note = f"前三大行业占比{top3_ratio:.1f}%,行业配置{concentration}"
    else:
        note = f"行业配置{concentration}"

    return {
        'industry_weights': {k: round(v, 2) for k, v in sorted_industries},
        'top_industries': top_industries,
        'concentration': concentration,
        'note': note
    }


# ============================================================
# 🎯 持仓集中度分析
# ============================================================

def calculate_concentration(holdings_df: pd.DataFrame) -> Dict:
    """
    计算持仓集中度指标(HHI/前三大/前五大/前十大)

    Args:
        holdings_df: 持仓DataFrame,包含 '占净值比例' 列

    Returns:
        {
            'hhi': 0.08,  # HHI指数
            'top3_ratio': 35.2,  # 前三大重仓股占比
            'top5_ratio': 48.5,  # 前五大重仓股占比
            'top10_ratio': 68.5,  # 前十大重仓股占比
            'concentration_level': '中度集中',  # 集中度评级
            'dispersion_score': 65.0,  # 分散度评分(0-100)
            'note': '持仓较为均衡,前十大重仓股占比68.5%',
        }
    """
    if holdings_df.empty or '占净值比例' not in holdings_df.columns:
        return {
            'hhi': 0.0,
            'top3_ratio': 0.0,
            'top5_ratio': 0.0,
            'top10_ratio': 0.0,
            'concentration_level': '数据不足',
            'dispersion_score': 0.0,
            'note': '持仓数据为空'
        }

    # 归一化权重(转换为小数)
    weights = holdings_df['占净值比例'].values / 100.0

    # HHI指数 (Herfindahl-Hirschman Index)
    hhi = sum(w**2 for w in weights)

    # 前三大/前五大/前十大集中度
    top3_ratio = weights[:3].sum() * 100 if len(weights) >= 3 else weights.sum() * 100
    top5_ratio = weights[:5].sum() * 100 if len(weights) >= 5 else weights.sum() * 100
    top10_ratio = weights[:10].sum() * 100

    # 分散度评分(0-100)
    # HHI越小越分散,HHI=0时score=100,HHI=1时score=0
    dispersion_score = (1 - hhi) * 100

    # 集中度评级
    if hhi > 0.1:
        concentration_level = '高度集中'
        note = f"持仓高度集中,前三大重仓股占比{top3_ratio:.1f}%,存在个股风险"
    elif hhi > 0.05:
        concentration_level = '中度集中'
        note = f"持仓较为均衡,前三大重仓股占比{top3_ratio:.1f}%"
    else:
        concentration_level = '分散'
        note = f"持仓高度分散,前三大重仓股占比{top3_ratio:.1f}%"

    return {
        'hhi': round(hhi * 100, 2),
        'top3_ratio': round(top3_ratio, 2),
        'top5_ratio': round(top5_ratio, 2),
        'top10_ratio': round(top10_ratio, 2),
        'concentration_level': concentration_level,
        'dispersion_score': round(dispersion_score, 1),
        'note': note
    }


# ============================================================
# 💰 个股基本面数据
# ============================================================

@retry_on_failure(retries=3, delay=2)
def fetch_stock_fundamentals(stock_codes: List[str]) -> Dict[str, Dict]:
    """
    批量获取个股基本面数据(包含投资风格标签)

    Args:
        stock_codes: 股票代码列表

    Returns:
        {
            '000001': {
                'name': '平安银行',
                'price': 10.94,
                'market_cap': 2100.5,
                'size_tag': '大盘',  # 大盘/中盘/小盘
                'style_tag': '价值',  # 价值/成长/均衡
                'pe_ratio': 5.2,
                'pb_ratio': 0.6,
                'roe': 12.5,
                'revenue_growth': 8.2,  # 营收增长率(%)
            },
            ...
        }
    """
    import akshare as ak

    fundamentals = {}

    for code in stock_codes[:10]:  # 限制前10只
        try:
            code_str = str(code).zfill(6)

            # 基本信息
            info_df = ak.stock_individual_info_em(symbol=code_str)
            if info_df.empty:
                continue

            info_dict = dict(zip(info_df['item'], info_df['value']))

            # 解析市值(转换为亿元)
            market_cap_str = info_dict.get('总市值', '0')
            if isinstance(market_cap_str, str):
                # 提取数字
                import re
                match = re.search(r'([\d.]+)', market_cap_str)
                if match:
                    market_cap = float(match.group(1))
                    # 判断单位
                    if '万' in market_cap_str:
                        market_cap = market_cap / 10000  # 万转亿
                    elif '亿' in market_cap_str:
                        pass  # 已经是亿元
                    else:
                        # 如果是纯数字,需要除以1亿(假设单位是元)
                        # 但需要先判断是否有小数点(可能是亿单位)
                        if market_cap > 1000000:  # 超过100万,单位可能是元
                            market_cap = market_cap / 100000000
                        elif market_cap < 10000:  # 小于10000,单位可能是亿
                            pass  # 已经是亿元
                        else:
                            market_cap = market_cap / 100000000  # 默认假设单位是元
                else:
                    market_cap = 0.0
            else:
                # 尝试直接转换
                try:
                    market_cap = float(market_cap_str)
                    if market_cap > 1000000:
                        market_cap = market_cap / 100000000
                except:
                    market_cap = 0.0

            # 市值分类
            if market_cap > 500:
                size_tag = '大盘'
            elif market_cap > 100:
                size_tag = '中盘'
            else:
                size_tag = '小盘'

            # 解析价格
            price_str = info_dict.get('最新', '0')
            price = float(str(price_str).replace(',', ''))

            # 解析PE/PB/ROE
            pe_str = info_dict.get('市盈率-动态', '0')
            pb_str = info_dict.get('市净率', '0')
            roe_str = info_dict.get('净资产收益率', '0')

            try:
                pe_ratio = float(str(pe_str).replace('-', '0').replace(',', ''))
                pb_ratio = float(str(pb_str).replace('-', '0').replace(',', ''))
                roe = float(str(roe_str).replace('-', '0').replace(',', ''))
            except:
                pe_ratio = 0.0
                pb_ratio = 0.0
                roe = 0.0

            # 投资风格标签(基于PE/PB/ROE)
            # 价值特征: 低PE, 低PB, 高ROE
            # 成长特征: 高PE, 高PB, 高增长
            if pe_ratio > 0:
                if pe_ratio < 15 and pb_ratio < 2.0:
                    style_tag = '价值'
                elif pe_ratio > 40 or pb_ratio > 8.0:
                    style_tag = '成长'
                else:
                    style_tag = '均衡'
            else:
                style_tag = '未知'

            # 营收增长率(如果可用)
            revenue_growth = 0.0
            if '营收增长率' in info_dict:
                try:
                    revenue_growth = float(str(info_dict['营收增长率']).replace('%', '').replace('-', '0'))
                except:
                    pass

            fundamentals[code_str] = {
                'name': info_dict.get('股票简称', ''),
                'price': price,
                'market_cap': round(market_cap, 2),
                'size_tag': size_tag,
                'style_tag': style_tag,
                'pe_ratio': round(pe_ratio, 2),
                'pb_ratio': round(pb_ratio, 2),
                'roe': round(roe, 2),
                'revenue_growth': round(revenue_growth, 2),
            }

        except Exception as e:
            print(f"⚠️ 获取股票 {code} 基本面失败: {str(e)[:50]}")
            continue

    return fundamentals


# ============================================================
# 🏷️ 风格标签对比
# ============================================================

def compare_style_with_ff(
    holdings_fundamentals: Dict[str, Dict],
    ff_results: Dict
) -> Dict:
    """
    对比持仓风格与FF因子暴露(包含价值和成长维度)

    Args:
        holdings_fundamentals: 个股基本面数据(fetch_stock_fundamentals返回)
        ff_results: FF因子模型结果

    Returns:
        {
            'holding_style': {
                'size': '大盘',  # 大盘/中盘/小盘
                'style': '价值',  # 价值/成长/均衡
            },
            'ff_style': {
                'size': '小盘',  # 基于SMB因子
                'style': '成长',  # 基于HML因子
            },
            'is_size_consistent': False,  # 市值风格是否一致
            'is_style_consistent': True,  # 投资风格是否一致
            'note': '持仓为大盘价值,但FF模型显示小盘成长,风格严重漂移',
        }
    """
    if not holdings_fundamentals:
        return {
            'holding_style': {'size': '未知', 'style': '未知'},
            'ff_style': {'size': '未知', 'style': '未知'},
            'is_size_consistent': True,
            'is_style_consistent': True,
            'note': '持仓数据不足,无法对比风格'
        }

    # 从持仓计算平均市值风格
    market_caps = [f['market_cap'] for f in holdings_fundamentals.values() if f['market_cap'] > 0]
    if market_caps:
        avg_market_cap = np.mean(market_caps)
        if avg_market_cap > 500:
            holding_size = '大盘'
        elif avg_market_cap > 100:
            holding_size = '中盘'
        else:
            holding_size = '小盘'
    else:
        holding_size = '未知'

    # 从持仓计算投资风格(价值/成长/均衡)
    style_tags = [f.get('style_tag', '均衡') for f in holdings_fundamentals.values()]
    if style_tags:
        value_count = style_tags.count('价值')
        growth_count = style_tags.count('成长')
        if value_count > growth_count * 1.5:
            holding_style = '价值'
        elif growth_count > value_count * 1.5:
            holding_style = '成长'
        else:
            holding_style = '均衡'
    else:
        holding_style = '未知'

    # 从FF模型读取因子暴露
    factor_betas = ff_results.get('factor_betas', {})
    smb_beta = factor_betas.get('SMB', 0.0)  # SMB>0小盘, SMB<0大盘
    hml_beta = factor_betas.get('HML', 0.0)  # HML>0价值, HML<0成长

    # FF市值风格
    if smb_beta > 0.1:
        ff_size = '小盘'
    elif smb_beta < -0.1:
        ff_size = '大盘'
    else:
        ff_size = '均衡'

    # FF投资风格
    if hml_beta > 0.1:
        ff_style = '价值'
    elif hml_beta < -0.1:
        ff_style = '成长'
    else:
        ff_style = '均衡'

    # 一致性判定
    is_size_consistent = holding_size == '未知' or ff_size == '均衡' or holding_size == ff_size
    is_style_consistent = holding_style == '未知' or ff_style == '均衡' or holding_style == ff_style

    # 生成解读
    if is_size_consistent and is_style_consistent:
        note = f"✅ 风格一致: 持仓为{holding_size}{holding_style}, FF模型显示{ff_size}{ff_style}"
    elif is_size_consistent and not is_style_consistent:
        note = f"⚠️ 部分漂移: 市值风格一致({holding_size}),但投资风格不同(持仓{holding_style} vs FF模型{ff_style})"
    elif not is_size_consistent and is_style_consistent:
        note = f"⚠️ 部分漂移: 投资风格一致({holding_style}),但市值风格不同(持仓{holding_size} vs FF模型{ff_size})"
    else:
        note = f"⚠️ 风格严重漂移: 持仓为{holding_size}{holding_style},但FF模型显示{ff_size}{ff_style}"

    return {
        'holding_style': {
            'size': holding_size,
            'style': holding_style,
        },
        'ff_style': {
            'size': ff_size,
            'style': ff_style,
        },
        'is_size_consistent': is_size_consistent,
        'is_style_consistent': is_style_consistent,
        'note': note
    }


# ============================================================
# 📈 持仓变动追踪
# ============================================================

def analyze_holdings_change(
    current_holdings: pd.DataFrame,
    historical_holdings: Dict[str, pd.DataFrame]
) -> Dict:
    """
    分析持仓变动(多期季报对比)

    Args:
        current_holdings: 最新持仓DataFrame
        historical_holdings: 历史持仓字典 {date: DataFrame}

    Returns:
        {
            'new_stocks': [{'code': '000001', 'name': '平安银行', 'current_ratio': 10.5}],
            'exited_stocks': [{'code': '600000', 'name': '浦发银行', 'previous_ratio': 8.2}],
            'increased_stocks': [{'code': '600519', 'name': '贵州茅台', 'change': 2.3}],
            'decreased_stocks': [{'code': '000858', 'name': '五粮液', 'change': -1.5}],
            'turnover_rate': 25.5,  # 换手率
            'stability_score': 75.0,  # 持仓稳定性评分(0-100)
            'note': '持仓较为稳定,换手率25.5%,新增2只股票,退出1只股票',
        }
    """
    if current_holdings.empty or not historical_holdings:
        return {
            'new_stocks': [],
            'exited_stocks': [],
            'increased_stocks': [],
            'decreased_stocks': [],
            'turnover_rate': 0.0,
            'stability_score': 0.0,
            'note': '持仓数据不足,无法分析变动'
        }

    # 获取最近一期历史持仓(按日期倒序)
    sorted_dates = sorted(historical_holdings.keys(), reverse=True)
    if not sorted_dates:
        return {
            'new_stocks': [],
            'exited_stocks': [],
            'increased_stocks': [],
            'decreased_stocks': [],
            'turnover_rate': 0.0,
            'stability_score': 0.0,
            'note': '无历史持仓数据'
        }

    previous_holdings = historical_holdings[sorted_dates[0]]
    previous_date = sorted_dates[0]

    # 标准化列名
    current_codes = set()
    current_ratios = {}
    for _, row in current_holdings.iterrows():
        code = str(row.get('证券代码', '')).zfill(6)
        if code:
            current_codes.add(code)
            current_ratios[code] = float(row.get('占净值比例', 0))

    previous_codes = set()
    previous_ratios = {}
    for _, row in previous_holdings.iterrows():
        code = str(row.get('证券代码', '')).zfill(6)
        name = str(row.get('证券名称', row.get('名称', '')))
        if code:
            previous_codes.add(code)
            previous_ratios[code] = {'ratio': float(row.get('占净值比例', 0)), 'name': name}

    # 1. 新进股票
    new_stocks = []
    for code in current_codes - previous_codes:
        # 从当前持仓获取名称
        name = current_holdings[current_holdings['证券代码'] == code]['证券名称'].values[0] if not current_holdings[current_holdings['证券代码'] == code].empty else f'股票{code}'
        new_stocks.append({
            'code': code,
            'name': str(name),
            'current_ratio': current_ratios[code],
        })

    # 2. 退出股票
    exited_stocks = []
    for code in previous_codes - current_codes:
        info = previous_ratios.get(code, {})
        exited_stocks.append({
            'code': code,
            'name': info.get('name', f'股票{code}'),
            'previous_ratio': info.get('ratio', 0),
        })

    # 3. 加仓股票
    increased_stocks = []
    for code in current_codes & previous_codes:
        change = current_ratios[code] - previous_ratios[code]['ratio']
        if change > 0.5:  # 加仓幅度>0.5%
            name = previous_ratios[code]['name']
            increased_stocks.append({
                'code': code,
                'name': name,
                'previous_ratio': previous_ratios[code]['ratio'],
                'current_ratio': current_ratios[code],
                'change': round(change, 2),
            })

    # 4. 减仓股票
    decreased_stocks = []
    for code in current_codes & previous_codes:
        change = current_ratios[code] - previous_ratios[code]['ratio']
        if change < -0.5:  # 减仓幅度<-0.5%
            name = previous_ratios[code]['name']
            decreased_stocks.append({
                'code': code,
                'name': name,
                'previous_ratio': previous_ratios[code]['ratio'],
                'current_ratio': current_ratios[code],
                'change': round(change, 2),
            })

    # 5. 换手率计算
    total_new = sum(s['current_ratio'] for s in new_stocks)
    total_exit = sum(s['previous_ratio'] for s in exited_stocks)
    turnover_rate = (total_new + total_exit) / 2

    # 6. 持仓稳定性评分(0-100)
    # 换手率越低,稳定性越高
    stability_score = max(0, 100 - turnover_rate * 2)

    # 7. 生成解读
    parts = []
    if new_stocks:
        parts.append(f"新增{len(new_stocks)}只股票")
    if exited_stocks:
        parts.append(f"退出{len(exited_stocks)}只股票")
    if increased_stocks:
        parts.append(f"加仓{len(increased_stocks)}只股票")
    if decreased_stocks:
        parts.append(f"减仓{len(decreased_stocks)}只股票")

    if parts:
        note = f"持仓{', '.join(parts)},换手率{turnover_rate:.1f}%"
        if stability_score > 70:
            note += ",持仓较为稳定"
        elif stability_score > 50:
            note += ",持仓中度调整"
        else:
            note += ",持仓变动较大"
    else:
        note = f"持仓基本无变化,换手率{turnover_rate:.1f}%"

    return {
        'new_stocks': sorted(new_stocks, key=lambda x: x['current_ratio'], reverse=True),
        'exited_stocks': sorted(exited_stocks, key=lambda x: x['previous_ratio'], reverse=True),
        'increased_stocks': sorted(increased_stocks, key=lambda x: x['change'], reverse=True),
        'decreased_stocks': sorted(decreased_stocks, key=lambda x: x['change']),
        'turnover_rate': round(turnover_rate, 2),
        'stability_score': round(stability_score, 1),
        'note': note
    }
