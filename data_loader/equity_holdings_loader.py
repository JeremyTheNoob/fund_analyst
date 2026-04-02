"""
权益类基金持仓数据加载器 — 深度持仓穿透分析模块
负责：按年份加载持仓历史 / 按季度加载资产结构 / 基金经理历史
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Optional, List, Dict
from dataclasses import dataclass

import pandas as pd
import akshare as ak

from data_loader.base_api import safe_api_call, call_with_timeout
from utils.common import audit_logger

logger = logging.getLogger(__name__)


@dataclass
class HoldingsHistoryData:
    """持仓历史数据"""
    fund_code: str
    df: pd.DataFrame  # 包含：季度 | 股票代码 | 股票名称 | 占净值比例 | 持股数 | 持仓市值


@dataclass
class AssetStructureData:
    """资产结构数据"""
    fund_code: str
    df: pd.DataFrame  # 包含：日期 | 股票 | 债券 | 现金 | 其他


# ============================================================
# 辅助函数
# ============================================================

def _get_quarter_end_dates(start_year: int, end_year: int) -> List[str]:
    """
    获取指定年份范围内的季度末日期列表

    Args:
        start_year: 起始年份
        end_year: 结束年份

    Returns:
        季度末日期列表，格式："YYYYMMDD"

    Example:
        >>> _get_quarter_end_dates(2024, 2024)
        ['20240331', '20240630', '20240930', '20241231']
    """
    dates = []
    for year in range(start_year, end_year + 1):
        dates.extend([
            f"{year}0331",
            f"{year}0630",
            f"{year}0930",
            f"{year}1231"
        ])
    return dates


def _get_years_from_establishment(establish_date: str, max_years: int = 5) -> List[str]:
    """
    根据基金成立日期，获取需要加载的年份列表（最多max_years年）

    Args:
        establish_date: 基金成立日期，格式："YYYY-MM-DD"
        max_years: 最大年数，默认5年

    Returns:
        年份列表，按降序排列

    Example:
        >>> _get_years_from_establishment("2020-05-15", 5)
        ['2024', '2023', '2022', '2021', '2020']
    """
    if not establish_date:
        return []

    try:
        establish_dt = pd.to_datetime(establish_date)
        current_year = datetime.now().year

        # 计算成立年份
        establish_year = establish_dt.year

        # 计算实际年数
        actual_years = current_year - establish_year + 1

        # 限制最多max_years年
        years_to_load = min(actual_years, max_years)

        # 生成年份列表（从当前年份倒推）
        years = [str(current_year - i) for i in range(years_to_load)]

        logger.info(f"[_get_years_from_establishment] 成立日期: {establish_date}, 实际年数: {actual_years}, 限制年数: {max_years}, 加载年份: {years}")
        return years

    except Exception as e:
        logger.error(f"[_get_years_from_establishment] 解析成立日期失败: {establish_date}, {e}")
        return []


def _get_years_from_manager(manager_start_date: Optional[str], max_years: int = 5) -> List[str]:
    """
    根据基金经理上任日期，获取需要加载的年份列表（最多max_years年）

    Args:
        manager_start_date: 基金经理上任日期，格式："YYYY-MM-DD"（可为None）
        max_years: 最大年数，默认5年

    Returns:
        年份列表，按降序排列

    Example:
        >>> _get_years_from_manager("2022-03-10", 5)
        ['2024', '2023', '2022']
    """
    if not manager_start_date:
        return []

    try:
        manager_dt = pd.to_datetime(manager_start_date)
        current_year = datetime.now().year

        # 计算经理上任年份
        manager_year = manager_dt.year

        # 计算实际年数
        actual_years = current_year - manager_year + 1

        # 限制最多max_years年
        years_to_load = min(actual_years, max_years)

        # 生成年份列表（从当前年份倒推）
        years = [str(current_year - i) for i in range(years_to_load)]

        logger.info(f"[_get_years_from_manager] 上任日期: {manager_start_date}, 实际年数: {actual_years}, 限制年数: {max_years}, 加载年份: {years}")
        return years

    except Exception as e:
        logger.error(f"[_get_years_from_manager] 解析上任日期失败: {manager_start_date}, {e}")
        return []


# ============================================================
# 持仓历史加载
# ============================================================

@audit_logger
def load_holdings_by_year(
    symbol: str,
    years: List[str],
    max_retries: int = 2
) -> HoldingsHistoryData:
    """
    按年份加载基金持仓历史

    Args:
        symbol: 基金代码
        years: 年份列表，如 ["2024", "2023", "2022"]
        max_retries: 最大重试次数

    Returns:
        HoldingsHistoryData，包含所有年份的持仓数据

    Notes:
        - 使用 ak.fund_portfolio_hold_em() 接口
        - Q2/Q4 返回完整持仓（150-200只股票，年报/半年报）
        - Q1/Q3 返回简化持仓（仅前10大重仓股，季报）
    """
    all_holdings = []

    for year in years:
        # 尝试读 Supabase 缓存（股票持仓，24h TTL）
        _raw_df = None
        try:
            from data_loader.cache_layer import cache_get, cache_set as _cache_set
            _raw_df = cache_get("fund_holdings_stock", ttl_seconds=86400, expect_df=True, symbol=symbol, date=year)
        except Exception:
            pass

        if _raw_df is None:
            def _fetch():
                return ak.fund_portfolio_hold_em(symbol=symbol, date=year)

            try:
                _raw_df = safe_api_call(_fetch, timeout_seconds=15.0, max_retries=max_retries)
                # 写入缓存
                if _raw_df is not None:
                    try:
                        from data_loader.cache_layer import cache_set as _cache_set
                        _cache_set("fund_holdings_stock", _raw_df, expect_df=True, symbol=symbol, date=year)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"[load_holdings_by_year] {symbol} {year}年持仓加载失败: {e}")
                continue

        df = _raw_df
        if df is None or df.empty:
            logger.warning(f"[load_holdings_by_year] {symbol} {year}年持仓数据为空")
            continue

        # 标准化列名
        df = df.copy()
        if '股票代码' in df.columns:
            df = df.rename(columns={'股票代码': '代码', '股票名称': '名称', '占净值比例': '占比'})

        # 添加年份信息（如果不存在则添加）
        if '年份' not in df.columns:
            df['年份'] = year
        else:
            df['年份'] = df['年份'].astype(str).str.replace(r'\.0$', '', regex=True)

        all_holdings.append(df)
        logger.info(f"[load_holdings_by_year] {symbol} {year}年持仓加载成功，共 {len(df)} 只股票")

    if not all_holdings:
        logger.warning(f"[load_holdings_by_year] {symbol} 所有年份持仓数据均失败")
        return HoldingsHistoryData(fund_code=symbol, df=pd.DataFrame())

    # 合并所有年份数据
    df_combined = pd.concat(all_holdings, ignore_index=True)

    # 数据清洗
    df_combined['代码'] = df_combined['代码'].astype(str).str.zfill(6)
    df_combined['占比'] = pd.to_numeric(df_combined['占比'], errors='coerce')
    df_combined['占比'] = df_combined['占比'] / 100  # 转换为小数
    df_combined = df_combined.dropna(subset=['占比'])

    # 标准化列名
    df_combined = df_combined.rename(columns={
        '代码': '股票代码',
        '名称': '股票名称',
        '占比': '占净值比例',
        '持股数': '持股数',
        '持仓市值': '持仓市值',
        '季度': '季度'
    })

    logger.info(f"[load_holdings_by_year] {symbol} 持仓数据加载完成，共 {len(df_combined)} 条记录")
    return HoldingsHistoryData(fund_code=symbol, df=df_combined)


# ============================================================
# 资产结构加载
# ============================================================

@audit_logger
def load_asset_structure(
    symbol: str,
    quarter_end_dates: List[str],
    max_retries: int = 2
) -> AssetStructureData:
    """
    按季度末日期加载基金资产结构

    Args:
        symbol: 基金代码
        quarter_end_dates: 季度末日期列表，如 ["20241231", "20240930", "20240630"]
        max_retries: 最大重试次数

    Returns:
        AssetStructureData，包含资产结构数据

    Notes:
        - 使用 ak.fund_individual_detail_hold_xq() 接口
        - 返回资产类型（股票/债券/现金/其他）及仓位占比
    """
    all_assets = []

    for date_str in quarter_end_dates:
        # 尝试读 Supabase 缓存（资产配置，7d TTL，季报数据更新频率低）
        _raw_df = None
        try:
            from data_loader.cache_layer import cache_get, cache_set as _cache_set
            _raw_df = cache_get("fund_asset_alloc", ttl_seconds=604800, expect_df=True, symbol=symbol, date=date_str)
        except Exception:
            pass

        if _raw_df is None:
            def _fetch():
                return ak.fund_individual_detail_hold_xq(symbol=symbol, date=date_str)

            try:
                _raw_df = safe_api_call(_fetch, timeout_seconds=10.0, max_retries=max_retries)
                # 写入缓存
                if _raw_df is not None:
                    try:
                        from data_loader.cache_layer import cache_set as _cache_set
                        _cache_set("fund_asset_alloc", _raw_df, expect_df=True, symbol=symbol, date=date_str)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"[load_asset_structure] {symbol} {date_str} 资产结构加载失败: {e}")
                continue

        df = _raw_df
        if df is None or df.empty:
            logger.warning(f"[load_asset_structure] {symbol} {date_str} 资产结构数据为空")
            continue

        # 提取资产结构
        # df 列可能包含：'资产类型', '仓位占比' 或 '占净值比例(%)' 等
        if '资产类型' not in df.columns:
            logger.warning(f"[load_asset_structure] {symbol} {date_str} 资产结构缺少'资产类型'列")
            continue

        # 检查比例列名（兼容多种格式）
        ratio_col = None
        for col in ['仓位占比', '占净值比例(%)', '占净值比例']:
            if col in df.columns:
                ratio_col = col
                break

        if ratio_col is None:
            logger.warning(f"[load_asset_structure] {symbol} {date_str} 资产结构缺少比例列，可用列：{df.columns.tolist()}")
            continue

        # 转置 DataFrame，将资产类型转换为列
        asset_dict = {}
        for _, row in df.iterrows():
            asset_type = str(row['资产类型']).strip()
            ratio = float(row[ratio_col] or 0) / 100
            asset_dict[asset_type] = ratio

        # 构建标准格式的行
        row_data = {
            '日期': pd.to_datetime(date_str),
            '股票': asset_dict.get('股票', 0.0),
            '债券': asset_dict.get('债券', 0.0),
            '现金': asset_dict.get('现金', 0.0) + asset_dict.get('银行存款', 0.0),
            '其他': asset_dict.get('其他', 0.0)
        }

        all_assets.append(row_data)
        logger.info(f"[load_asset_structure] {symbol} {date_str} 资产结构加载成功")

    if not all_assets:
        logger.warning(f"[load_asset_structure] {symbol} 所有日期资产结构数据均失败")
        return AssetStructureData(fund_code=symbol, df=pd.DataFrame())

    # 构建DataFrame
    df_combined = pd.DataFrame(all_assets)
    df_combined = df_combined.sort_values('日期').reset_index(drop=True)

    logger.info(f"[load_asset_structure] {symbol} 资产结构加载完成，共 {len(df_combined)} 个季度")
    return AssetStructureData(fund_code=symbol, df=df_combined)


# ============================================================
# 便捷函数
# ============================================================

def load_holdings_analysis_data(
    symbol: str,
    analysis_period: str,
    establish_date: Optional[str] = None,
    manager_start_date: Optional[str] = None,
    max_years: int = 5
) -> tuple[HoldingsHistoryData, AssetStructureData]:
    """
    便捷函数：根据分析周期加载持仓分析所需的数据

    Args:
        symbol: 基金代码
        analysis_period: 分析周期，"成立以来" 或 "现任经理"
        establish_date: 基金成立日期
        manager_start_date: 基金经理上任日期
        max_years: 最大年数，默认5年

    Returns:
        (HoldingsHistoryData, AssetStructureData)
    """
    # 1. 确定需要加载的年份
    if analysis_period == "成立以来" and establish_date:
        years = _get_years_from_establishment(establish_date, max_years)
    elif analysis_period == "现任经理" and manager_start_date:
        years = _get_years_from_manager(manager_start_date, max_years)
    else:
        # 默认/其他：加载最近max_years年
        current_year = datetime.now().year
        years = [str(current_year - i) for i in range(max_years)]
        logger.info(f"[load_holdings_analysis_data] 使用默认周期: 最近{max_years}年 ({years})")

    if not years:
        logger.error(f"[load_holdings_analysis_data] 无法确定需要加载的年份")
        return HoldingsHistoryData(fund_code=symbol, df=pd.DataFrame()), \
               AssetStructureData(fund_code=symbol, df=pd.DataFrame())

    # 2. 加载持仓历史
    logger.info(f"[load_holdings_analysis_data] 开始加载持仓历史，年份：{years}")
    holdings_data = load_holdings_by_year(symbol, years)
    logger.info(f"[load_holdings_analysis_data] 持仓历史加载完成，数据行数：{len(holdings_data.df)}")

    # 3. 计算需要加载的季度末日期
    start_year = int(years[-1])  # 最小年份
    end_year = int(years[0])     # 最大年份
    quarter_end_dates = _get_quarter_end_dates(start_year, end_year)
    logger.info(f"[load_holdings_analysis_data] 将加载 {len(quarter_end_dates)} 个季度的资产结构数据")

    # 4. 加载资产结构
    asset_structure_data = load_asset_structure(symbol, quarter_end_dates)
    logger.info(f"[load_holdings_analysis_data] 资产结构加载完成，数据行数：{len(asset_structure_data.df)}")

    logger.info(f"[load_holdings_analysis_data] 数据加载完成：持仓历史 {len(holdings_data.df)} 行，资产结构 {len(asset_structure_data.df)} 行")
    return holdings_data, asset_structure_data


# ============================================================
# 基金经理历史
# ============================================================

@audit_logger
def get_manager_history(symbol: str) -> Dict[str, str]:
    """
    获取基金经理历史信息

    Args:
        symbol: 基金代码

    Returns:
        {
            "current_manager": "现任经理姓名",
            "manager_start_date": "上任日期",
            "history": [...]  # 历任经理列表
        }
    """
    result = {
        "current_manager": "",
        "manager_start_date": "",
        "history": []
    }

    try:
        # 从雪球获取基金经理信息
        from data_loader.base_api import _ak_fund_basic_xq
        df_xq = _ak_fund_basic_xq(symbol)

        if df_xq is not None and not df_xq.empty:
            info = dict(zip(df_xq.iloc[:, 0], df_xq.iloc[:, 1]))
            result["current_manager"] = info.get("基金经理", "")

            # 尝试解析上任日期（如果有）
            # 注：雪球接口可能不直接提供上任日期，需要从其他接口获取
            # 这里暂时留空，待后续完善

    except Exception as e:
        logger.warning(f"[get_manager_history] 获取基金经理历史失败: {e}")

    return result
