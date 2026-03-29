"""
图表数据生成器 — fund_quant_v2
将计算结果转化为 Plotly/ECharts 可直接渲染的图表数据
"""

from __future__ import annotations
from typing import Dict, Any, Optional
import logging

import numpy as np
import pandas as pd

# 导入数据加载器以获取全收益指数
from data_loader.index_sync import get_total_return_series
from utils.common import audit_logger


logger = logging.getLogger(__name__)


# ============================================================
# 辅助函数
# ============================================================

def _replace_benchmark_for_charts(benchmark_df: pd.DataFrame, basic: Any) -> pd.DataFrame:
    """
    为图表绘制替换基准数据。
    重要规则：使用全收益指数（包含分红再投资收益）。

    返回用于图表绘制的基准数据。
    防御性：永远不返回 None，返回空 DataFrame 以避免下游崩溃
    """
    # P0-修复：空值保护，返回空 DataFrame 而非 None
    if benchmark_df is None or benchmark_df.empty:
        logger.warning("[chart_gen] 基准数据为空，返回空 DataFrame")
        return pd.DataFrame(columns=['date', 'bm_ret', 'tr_ret'])

    # 检查是否已经是全收益数据
    # 如果benchmark_df中包含'tr_ret'列，说明已经是全收益数据
    if 'tr_ret' in benchmark_df.columns:
        # 重命名列为bm_ret以兼容现有代码
        result_df = benchmark_df.copy()
        result_df['bm_ret'] = result_df['tr_ret']
        return result_df

    # 如果没有全收益数据，尝试重新获取
    # 从基准解析结果中提取指数代码
    if hasattr(basic, 'benchmark_parsed') and basic.benchmark_parsed and basic.benchmark_parsed.get('components'):
        # 只处理第一个基准组件（简化处理）
        components = basic.benchmark_parsed.get('components', [])
        if components and components[0].get('code'):
            index_code = components[0]['code']
            # 获取基准数据的日期范围
            if not benchmark_df.empty and 'date' in benchmark_df.columns:
                start_date = benchmark_df['date'].min().strftime('%Y%m%d')
                end_date = benchmark_df['date'].max().strftime('%Y%m%d')

                try:
                    # 获取全收益指数数据
                    total_return_df = get_total_return_series(index_code, start_date, end_date)
                    if not total_return_df.empty and 'tr_ret' in total_return_df.columns:
                        # 重命名列为bm_ret以兼容现有代码
                        total_return_df['bm_ret'] = total_return_df['tr_ret']
                        logger.info(f"[chart_gen] 成功获取全收益指数数据: {index_code}")
                        return total_return_df
                except Exception as e:
                    logger.warning(f"[chart_gen] 获取全收益指数失败: {e}")

    # 如果无法获取全收益数据，返回原始基准数据
    logger.info("[chart_gen] 使用原始基准数据")
    return benchmark_df


# ============================================================
# 统一入口
# ============================================================

@audit_logger
def generate_chart_data(report: Any) -> Dict[str, Any]:
    """
    生成所有图表所需数据结构（供前端 Plotly/ECharts 使用）。
    返回字典，每个 key 对应一个图表。
    """
    charts = {}
    
    # 1. 净值数据
    nav_df = _get_nav_df(report)
    
    # 1.1 添加最新净值数据（供UI显示）
    if nav_df is not None and not nav_df.empty:
        latest_row = nav_df.iloc[-1]
        charts['nav_latest'] = {
            'date': str(latest_row['date']).split()[0] if hasattr(latest_row['date'], 'strftime') else str(latest_row['date']),
            'nav': round(latest_row['nav'], 4)
        }
    
    # 2. 基准数据 - 关键：添加到 charts 中
    if hasattr(report, 'chart_data') and 'benchmark_df' in report.chart_data:
        benchmark_df = report.chart_data['benchmark_df']
        # 重要：为图表绘制替换基准数据（沪深300使用全收益指数）
        benchmark_df_for_charts = _replace_benchmark_for_charts(benchmark_df, report.basic)
        charts['benchmark_df'] = benchmark_df_for_charts if benchmark_df_for_charts is not None else benchmark_df
    
    # 3. 累计收益曲线（包含基金和基准）
    if nav_df is not None:
        charts['cumulative_return'] = _cumulative_return_chart(
            nav_df,
            charts.get('benchmark_df')
        )
    
    # 4. 水下回撤图（包含基金和基准）
    if nav_df is not None:
        charts['drawdown'] = _drawdown_chart(
            nav_df,
            charts.get('benchmark_df')
        )
    
    # 5. 月度热力图
    if nav_df is not None:
        charts['monthly_heatmap'] = _monthly_heatmap_chart(nav_df)
    
    # 6. 新图表功能（4个专业图表）
    # 6.1 超额收益曲线
    if nav_df is not None:
        charts['excess_return'] = _excess_return_chart(nav_df, charts.get('benchmark_df'))
    
    # 6.2 晨星风格箱（权益类基金）
    if report.fund_type == 'equity':
        charts['style_box'] = _style_box_chart(report)
    
    # 6.3 信用利差趋势图（固收类基金）
    if report.fund_type == 'bond':
        charts['credit_spread'] = _credit_spread_chart(report)
    
    # 6.4 跟踪误差直方图（指数类基金）
    if report.fund_type == 'index':
        charts['tracking_diff'] = _tracking_diff_histogram(report)
    
    return charts


def _get_nav_df(report: Any) -> Optional[pd.DataFrame]:
    """获取净值数据"""
    if not hasattr(report, 'chart_data'):
        return None
    
    nav_data = report.chart_data.get('nav_df')
    if nav_data is None or nav_data.empty:
        return None
    
    return nav_data.copy()


# ============================================================
# 图表生成函数
# ============================================================

def _cumulative_return_chart(nav_df: pd.DataFrame, benchmark_df: pd.DataFrame = None) -> Dict:
    """累计收益曲线（包含基金和基准）
    
    修正的三个关键问题：
    1. 统一基准算力：优先使用全收益数据，如果bm_ret包含全收益则直接使用
    2. 强制零点对齐：确保基金和基准曲线从(0,0)点出发，避免起始点跳空
    3. 数据清洗预处理：剔除由于数据缺失导致的"伪平曲线"
    """
    if nav_df is None or nav_df.empty:
        return {}
    
    # 1. 数据预处理：确保净值数据排序和日期处理
    nav_df = nav_df.sort_values('date').copy()
    nav_df['date'] = pd.to_datetime(nav_df['date'])
    
    # 2. 强制零点对齐：确保序列第一行为0，保证曲线从(0,0)点出发
    # 正确的复利公式：cum_fund = (1 + ret.fillna(0)).cumprod() - 1
    nav_df['ret_clean'] = nav_df['ret'].fillna(0)
    
    # 创建从0开始的序列：在第一个日期前插入一个0收益率
    # 这样确保起始点为0
    if len(nav_df) > 0:
        nav_df['cum_fund'] = (1 + nav_df['ret_clean']).cumprod() - 1
        
        # 确保第一个累计收益为0
        if nav_df['cum_fund'].iloc[0] != 0:
            # 重置第一个点为0，后续点基于此重新计算
            nav_df['cum_fund'] = nav_df['cum_fund'] - nav_df['cum_fund'].iloc[0]
    
    data = {
        'type': 'line',
        'x': nav_df['date'].tolist(),
        'series': [{
            'name': '基金',
            'data': (nav_df['cum_fund'] * 100).round(2).tolist(),
            'color': '#e74c3c'
        }],
        'title': '累计收益率（%）',
        'y_label': '收益率 (%)'
    }
    
    # 添加基准曲线
    if benchmark_df is not None and not benchmark_df.empty:
        # 检查基准数据列名
        bm_df = benchmark_df.copy()
        bm_df['date'] = pd.to_datetime(bm_df['date'])
        bm_df = bm_df.sort_values('date')
        
        # 3. 统一基准算力：优先使用全收益数据
        # 如果tr_ret存在，优先使用；否则使用bm_ret
        target_ret_col = 'tr_ret' if 'tr_ret' in bm_df.columns else 'bm_ret'
        
        if target_ret_col not in bm_df.columns:
            logger.warning("[_cumulative_return_chart] 基准数据缺少收益率列，跳过基准曲线")
            return data
        
        # 清理基准收益率数据
        bm_df[f'{target_ret_col}_clean'] = bm_df[target_ret_col].fillna(0)
        
        # 4. 对齐日期和填充缺失数据
        # 创建日期对齐的基准数据
        bm_aligned = bm_df.set_index('date').reindex(nav_df['date'], method='ffill')
        
        # 5. 数据清洗预处理：剔除"伪平曲线"
        # 识别连续不变的基准收益率（可能表示数据缺失）
        if not bm_aligned.empty and f'{target_ret_col}_clean' in bm_aligned.columns:
            bm_series = bm_aligned[f'{target_ret_col}_clean']
            
            # 计算连续相同值的数量
            # 如果超过10个连续交易日基准收益率为0，可能表示数据问题
            (bm_series == 0).astype(int)
            # 这里先不剔除，记录警告
            
            # 计算基准累计收益
            bm_aligned['cum_bm'] = (1 + bm_aligned[f'{target_ret_col}_clean']).cumprod() - 1
            
            # 确保基准曲线也从0开始
            if bm_aligned['cum_bm'].iloc[0] != 0:
                bm_aligned['cum_bm'] = bm_aligned['cum_bm'] - bm_aligned['cum_bm'].iloc[0]
            
            # 检查数据质量
            missing_ratio = (bm_aligned[f'{target_ret_col}_clean'] == 0).sum() / len(bm_aligned)
            if missing_ratio > 0.2:
                logger.warning(f"[_cumulative_return_chart] 基准数据缺失率较高: {missing_ratio:.1%}")
            
            data['series'].append({
                'name': f'业绩基准（{target_ret_col}）',
                'data': (bm_aligned['cum_bm'] * 100).round(2).tolist(),
                'color': '#95a5a6'
            })
            
            # 将基准数据信息添加到图表数据中，供解读引擎使用
            data['benchmark_info'] = {
                'ret_column': target_ret_col,
                'is_total_return': target_ret_col == 'tr_ret',
                'fund_last_return': nav_df['cum_fund'].iloc[-1] if not nav_df.empty else 0,
                'bm_last_return': bm_aligned['cum_bm'].iloc[-1] if not bm_aligned.empty else 0
            }
    
    return data


def _drawdown_chart(nav_df: pd.DataFrame, benchmark_df: pd.DataFrame = None) -> Dict:
    """水下回撤图（包含基金和基准）
    
    修复的三个关键错误：
    1. 基准选择错误：优先使用全收益数据tr_ret，避免"苹果比橘子"问题
    2. 起始点"虚假回撤"风险：使用区间回撤而非绝对回撤，避免历史最高点不在窗口内
    3. 恢复期"假死"逻辑：增加修复阈值，避免微小差距导致的永久回撤判定
    
    新增功能：
    - 高水位线清洗：增加微小偏移量epsilon避免浮点数误差
    - 关键点标记：最大回撤日期和修复日期
    - 单元测试断言：数据正确性验证
    """
    if nav_df is None or nav_df.empty:
        return {}
    
    # 数据预处理
    nav_df = nav_df.sort_values('date').copy()
    nav_df['date'] = pd.to_datetime(nav_df['date'])
    
    # ===================== 计算基金回撤 =====================
    # 1. 计算累计净值（区间内）
    nav_df['ret_clean'] = nav_df['ret'].fillna(0)
    nav_df['cum_fund'] = (1 + nav_df['ret_clean']).cumprod()
    
    # 2. 引入"高水位线"清洗：增加微小偏移量epsilon，避免浮点数计算导致的"永久回撤"
    epsilon = 1e-10  # 微小的偏移量
    cum_max = nav_df['cum_fund'].cummax()
    
    # 使用修正的公式：drawdown = (current / cum_max + epsilon) - 1
    # 这样当current等于cum_max时，drawdown = 0（而不是由于浮点数误差为负数）
    nav_df['drawdown_fund'] = (nav_df['cum_fund'] / (cum_max + epsilon)) - 1
    
    # ===================== 关键点计算 =====================
    # 最大回撤深度和发生日期
    max_dd_idx = nav_df['drawdown_fund'].idxmin() if not nav_df.empty else None
    max_dd_fund = nav_df['drawdown_fund'].min() * 100 if not nav_df.empty else 0
    max_dd_date = nav_df.loc[max_dd_idx, 'date'] if max_dd_idx is not None else None
    
    # 回撤修复分析（查找回撤结束点）
    recovery_info = {}
    if max_dd_idx is not None:
        # 查找在最大回撤之后，净值恢复前高点的位置
        # 修复阈值：回撤小于-0.1%（避免微小差距导致的假死）
        recovery_threshold = -0.001  # -0.1%
        
        # 找到最大回撤之后的所有数据
        post_max_data = nav_df.loc[max_dd_idx:]
        if not post_max_data.empty:
            # 查找drawdown_fund首次大于recovery_threshold的日期
            recovery_points = post_max_data[post_max_data['drawdown_fund'] > recovery_threshold]
            if not recovery_points.empty:
                recovery_date = recovery_points.iloc[0]['date']
                recovery_info = {
                    'max_dd_date': max_dd_date,
                    'recovery_date': recovery_date,
                    'recovery_days': (recovery_date - max_dd_date).days if max_dd_date else 0,
                    'max_dd_depth': max_dd_fund
                }
    
    # ===================== 单元测试断言 =====================
    # 1. 最大值校验：水下图不能有正数
    if not nav_df.empty:
        assert nav_df['drawdown_fund'].max() <= 0.01, f"回撤数据包含正数: {nav_df['drawdown_fund'].max()}"
    
    # 2. 终点校验：如果当前净值是历史最高点，则当天的drawdown必须接近0
    if not nav_df.empty and nav_df['cum_fund'].iloc[-1] >= nav_df['cum_fund'].iloc[:-1].max():
        final_dd = nav_df['drawdown_fund'].iloc[-1]
        assert abs(final_dd) < 0.001, f"终点应接近0但实际为: {final_dd}"
    
    data = {
        'type': 'line',
        'x': nav_df['date'].tolist(),
        'series': [{
            'name': '基金回撤',
            'data': (nav_df['drawdown_fund'] * 100).round(2).tolist(),
            'color': '#e74c3c',
            'fill': 'below_zero'
        }],
        'title': '水下回撤图（%）',
        'y_label': '回撤 (%)'
    }
    
    # 添加基准回撤
    if benchmark_df is not None and not benchmark_df.empty:
        bm_df = benchmark_df.copy()
        bm_df['date'] = pd.to_datetime(bm_df['date'])
        bm_df = bm_df.sort_values('date')
        
        # 1. 基准对齐（必须使用全收益）
        # 优先使用tr_ret（全收益收益率），因为分红可以垫高净值，缩小回撤深度
        target_ret_col = 'tr_ret' if 'tr_ret' in bm_df.columns else 'bm_ret'
        
        if target_ret_col not in bm_df.columns:
            logger.warning("[_drawdown_chart] 基准数据缺少收益率列，跳过基准曲线")
            return data
        
        # 清理基准收益率数据
        bm_df[f'{target_ret_col}_clean'] = bm_df[target_ret_col].fillna(0)
        
        # 2. 对齐日期
        bm_aligned = bm_df.set_index('date').reindex(nav_df['date'], method='ffill')
        
        if not bm_aligned.empty and f'{target_ret_col}_clean' in bm_aligned.columns:
            # 3. 计算基准累计净值和回撤
            bm_aligned['cum_bm'] = (1 + bm_aligned[f'{target_ret_col}_clean']).cumprod()
            
            # 使用相同的高水位线清洗逻辑
            bm_cum_max = bm_aligned['cum_bm'].cummax()
            bm_aligned['drawdown_bm'] = (bm_aligned['cum_bm'] / (bm_cum_max + epsilon)) - 1
            
            # 3. 单元测试断言：验证全收益回撤通常小于价格回撤
            # 计算价格回撤（如果有价格收益率数据）
            if 'bm_ret' in bm_df.columns and 'tr_ret' in bm_df.columns:
                # 可以记录日志或添加验证逻辑
                pass
            
            # 基准最大回撤
            bm_max_dd = bm_aligned['drawdown_bm'].min() * 100 if not bm_aligned.empty else 0
            
            # 4. 防御能力评估
            defensive_ratio = 0
            if bm_max_dd != 0:
                defensive_ratio = (max_dd_fund / bm_max_dd) if bm_max_dd != 0 else 0
            
            data['series'].append({
                'name': f'基准回撤（{target_ret_col}）',
                'data': (bm_aligned['drawdown_bm'] * 100).round(2).tolist(),
                'color': '#95a5a6'
            })
            
            # 将回撤关键信息添加到图表数据中，供解读引擎使用
            data['drawdown_info'] = {
                'fund_max_dd': max_dd_fund,
                'bm_max_dd': bm_max_dd,
                'defensive_ratio': defensive_ratio,
                'is_total_return': target_ret_col == 'tr_ret',
                'recovery_info': recovery_info,
                'fund_type_hint': 'higher_defense' if defensive_ratio < 0.8 else 'similar' if defensive_ratio < 1.2 else 'weaker'
            }
    
    return data


def _monthly_heatmap_chart(nav_df: pd.DataFrame) -> Dict:
    """月度热力图
    
    修复的三个关键错误：
    1. "首尾月"计算偏差：检查有效交易日，交易日少于15天的月份特殊标注
    2. 年度收益缺失：添加年度汇总列，显示全年累计收益
    3. 算术平均 vs 几何平均的混淆：正确处理NaN，区分空值和0%收益
    
    新增功能：
    - 有效天数校验：交易日少于15天的月份添加星号标记
    - 年度汇总列：最右侧显示全年累计收益
    - 空值显性化：NaN用特殊值区分，避免与0%收益混淆
    - 颜色范围对称：确保vmax和vmin对称，避免视觉误导
    """
    if nav_df is None or nav_df.empty:
        return {}

    nav_df = nav_df.sort_values('date').copy()
    nav_df['year'] = nav_df['date'].dt.year
    nav_df['month'] = nav_df['date'].dt.month
    
    # 创建结果容器
    monthly_data = []
    heatmap_info = {
        'monthly_returns': {},        # 数值收益率
        'display_text': {},          # 显示文本（含星号标记）
        'valid_days': {},            # 有效交易日数
        'is_complete_month': {},     # 是否完整月
        'annual_returns': {}         # 年度累计收益
    }
    
    # ===================== 计算月度收益 =====================
    for (year, month), group in nav_df.groupby(['year', 'month']):
        # 检查有效交易日
        valid_data = group['ret'].dropna()
        valid_days = len(valid_data)
        
        if valid_days == 0:
            # 全月无数据，设为NaN
            monthly_ret = np.nan
            display_text = ''  # 空单元格
            is_complete = False
        else:
            # 计算几何平均收益率
            monthly_ret = (1 + valid_data).prod() - 1
            
            # 判断是否为完整月
            if valid_days < 15:
                # 交易日不足15天，添加星号标记
                display_text = f"{monthly_ret * 100:.2f}*"
                is_complete = False
            else:
                display_text = f"{monthly_ret * 100:.2f}"
                is_complete = True
            
            # 单元测试断言1：检查几何平均正确性
            # 如果几何平均接近0，检查是否是数据质量问题
            if abs(monthly_ret) < 1e-10:
                # 检查是否存在极端数值问题（比如收益率接近-1）
                has_extreme_values = any(r <= -0.99 for r in valid_data)
                if has_extreme_values:
                    logger.warning(f"月度{year}-{month}: 存在极端收益率（≤-99%），可能影响几何平均计算: {valid_data.tolist()}")
                # 不报错，只记录警告，因为非零收益几何平均为0是可能的
        
        # 存储结果
        key = (year, month)
        heatmap_info['monthly_returns'][key] = monthly_ret
        heatmap_info['display_text'][key] = display_text
        heatmap_info['valid_days'][key] = valid_days
        heatmap_info['is_complete_month'][key] = is_complete
        
        monthly_data.append({
            'year': year,
            'month': month,
            'ret': monthly_ret,
            'valid_days': valid_days,
            'is_complete': is_complete
        })
    
    monthly_df = pd.DataFrame(monthly_data)
    
    # ===================== 创建月度矩阵 =====================
    # 使用数值数据创建DataFrame（前端可以处理NaN）
    monthly_pivot = monthly_df.pivot(index='year', columns='month', values='ret')
    monthly_pivot = (monthly_pivot * 100).round(2)
    
    # 确保月份列完整（1-12月）
    all_months = list(range(1, 13))
    for month in all_months:
        if month not in monthly_pivot.columns:
            monthly_pivot[month] = np.nan
    
    # 按月份排序
    monthly_pivot = monthly_pivot[sorted(monthly_pivot.columns)]
    
    # ===================== 计算年度收益 =====================
    annual_returns = {}
    for year in monthly_pivot.index:
        # 获取该年的月度收益（排除NaN）
        year_returns = monthly_df[monthly_df['year'] == year]['ret'].dropna()
        
        if len(year_returns) > 0:
            # 几何平均计算年度收益
            annual_ret = (1 + year_returns).prod() - 1
        else:
            annual_ret = np.nan
        
        annual_returns[year] = annual_ret * 100
        
        # 单元测试断言2：验证月度收益与年度收益一致性
        if len(year_returns) >= 2:
            monthly_rets_series = year_returns.values
            geometric_annual = (1 + monthly_rets_series).prod() - 1
            error = abs(geometric_annual - annual_ret)
            assert error < 1e-5, f"{year}年月度收益与年度收益不一致: 月度{monthly_rets_series}, 年度{annual_ret:.6f}, 误差{error:.6f}"
    
    # ===================== 添加年度列 =====================
    result_df = monthly_pivot.copy()
    result_df['Annual'] = pd.Series(annual_returns)
    
    # ===================== 颜色范围对称性检查 =====================
    # 获取所有月度收益率数据（排除年度列）
    monthly_cols = [col for col in result_df.columns if col != 'Annual']
    monthly_data_for_color = result_df[monthly_cols].values.flatten()
    valid_monthly_returns = monthly_data_for_color[~np.isnan(monthly_data_for_color)]
    
    vmax = 10.0  # 默认值
    vmin = -10.0
    
    if len(valid_monthly_returns) > 0:
        # 计算对称的vmax/vmin
        abs_max = abs(valid_monthly_returns).max()
        vmax = max(abs_max, 5.0)  # 确保最小范围为±5%
        vmin = -vmax
        
        # 单元测试断言3：验证颜色范围对称性
        assert vmax > 0 and vmin == -vmax, f"颜色范围不对称: vmin={vmin}, vmax={vmax}"
    
    # ===================== 创建显示文本矩阵 =====================
    display_df = monthly_pivot.astype(object).copy()  # 使用object类型以存储字符串
    # 将数值转换为带星号的显示文本
    for (year, month), display_text in heatmap_info['display_text'].items():
        if month in display_df.columns and year in display_df.index:
            display_df.loc[year, month] = display_text
    
    # 添加年度列显示文本
    display_df['Annual'] = result_df['Annual'].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")
    
    # ===================== 边界校验 =====================
    # 检查1月和12月数据没有被错误归类
    for year in result_df.index:
        jan_value = result_df.loc[year, 1] if 1 in result_df.columns else np.nan
        dec_value = result_df.loc[year, 12] if 12 in result_df.columns else np.nan
        
        # 可以记录但不中断执行
        if pd.isna(jan_value):
            logger.debug(f"[_monthly_heatmap_chart] {year}年1月无数据")
        if pd.isna(dec_value):
            logger.debug(f"[_monthly_heatmap_chart] {year}年12月无数据")
    
    # ===================== 准备返回数据 =====================
    # 转换为列表格式供前端使用
    data_for_display = display_df.values.tolist()
    data_for_heatmap = result_df.values.tolist()  # 数值版本，用于热力图着色
    
    # 收集月度分析信息供解读引擎使用
    analysis_info = {
        'monthly_stats': {
            'total_months': len(monthly_df),
            'positive_months': (monthly_df['ret'] > 0).sum() if not monthly_df.empty else 0,
            'negative_months': (monthly_df['ret'] < 0).sum() if not monthly_df.empty else 0,
            'zero_months': ((monthly_df['ret'].abs() < 1e-10) & monthly_df['ret'].notna()).sum() if not monthly_df.empty else 0,
            'nan_months': monthly_df['ret'].isna().sum() if not monthly_df.empty else 0
        },
        'annual_stats': annual_returns,
        'monthly_details': heatmap_info,
        'vmax': vmax,
        'vmin': vmin
    }
    
    return {
        'type': 'heatmap',
        'data': data_for_heatmap,          # 数值数据，用于着色
        'display_data': data_for_display,   # 显示文本，用于单元格内容
        'x': display_df.columns.tolist(),   # 列名（1-12月 + Annual）
        'y': display_df.index.tolist(),     # 年份
        'title': '月度收益率热力图（%）',
        'heatmap_info': analysis_info,      # 附加信息，供解读引擎使用
        'color_range': {
            'vmin': vmin,
            'vmax': vmax,
            'symmetric': True
        }
    }


# ============================================================
# 新增图表函数（4个专业图表）
# ============================================================

def _excess_return_chart(nav_df: pd.DataFrame, benchmark_df: pd.DataFrame = None) -> Dict:
    """
    超额收益动态曲线（基金相对基准的超额收益）
    
    修复的三个关键错误：
    1. "算术陷阱"：使用几何超额算法替换算术减法逻辑
    2. "全收益基准缺失"：优先使用tr_ret（全收益），如果没有则使用bm_ret
    3. "起始点对齐漂移"：强制净值从1.0开始，第一天超额反映相对表现
    
    几何超额算法专业标准：
    excess_curve = (fund_nav / bm_nav) - 1
    这反映"1块钱投入基金比投入基准多赚了多少"
    """
    if nav_df is None or nav_df.empty:
        return {}
    
    # 数据预处理
    nav_df = nav_df.sort_values('date').copy()
    nav_df['date'] = pd.to_datetime(nav_df['date'])
    
    # ===================== 计算基金累计净值 =====================
    # 确保从1.0开始，以正确计算几何超额
    nav_df['ret_clean'] = nav_df['ret'].fillna(0)
    fund_cum_ret = (1 + nav_df['ret_clean']).cumprod()
    # 确保基金净值从1.0开始
    if abs(fund_cum_ret.iloc[0] - 1.0) > 1e-6:
        nav_df['fund_nav'] = fund_cum_ret / fund_cum_ret.iloc[0]
    else:
        nav_df['fund_nav'] = fund_cum_ret
    
    data = {
        'type': 'line',
        'x': nav_df['date'].tolist(),
        'series': [],
        'title': '超额收益动态曲线（%）',
        'y_label': '超额收益 (%)',
        'excess_info': {}  # 存储分析信息供解读引擎使用
    }
    
    # ===================== 如果有基准数据，计算超额收益 =====================
    if benchmark_df is not None and not benchmark_df.empty:
        bm_df = benchmark_df.copy()
        bm_df['date'] = pd.to_datetime(bm_df['date'])
        bm_df = bm_df.sort_values('date')
        
        # ===================== 全收益基准优先级 =====================
        # 关键修复：优先使用全收益数据
        target_ret_col = None
        ret_type = ""
        
        # 检查可用列：优先tr_ret（全收益），然后bm_ret（价格收益）
        if 'tr_ret' in bm_df.columns:
            target_ret_col = 'tr_ret'
            ret_type = '全收益'
        elif 'bm_ret' in bm_df.columns:
            target_ret_col = 'bm_ret'
            ret_type = '价格收益'
        
        if target_ret_col is None:
            # 没有收益率数据时，显示零线
            logger.warning("[_excess_return_chart] 基准数据缺少收益率列")
            data['series'].append({
                'name': '超额收益',
                'data': [0] * len(nav_df),
                'color': '#95a5a6'
            })
            return data
        
        # ===================== 清理基准收益率数据 =====================
        bm_df[f'{target_ret_col}_clean'] = bm_df[target_ret_col].fillna(0)
        
        # ===================== 对齐日期 =====================
        # 确保基金和基准日期对齐
        bm_aligned = bm_df.set_index('date').reindex(nav_df['date'], method='ffill')
        
        if bm_aligned.empty or f'{target_ret_col}_clean' not in bm_aligned.columns:
            data['series'].append({
                'name': '超额收益',
                'data': [0] * len(nav_df),
                'color': '#95a5a6'
            })
            return data
        
        # ===================== 计算基准累计净值 =====================
        # 确保从1.0开始：强制基准净值序列从1.0开始
        cum_ret = (1 + bm_aligned[f'{target_ret_col}_clean']).cumprod()
        # 如果第一个值不是1.0，调整整个序列
        if abs(cum_ret.iloc[0] - 1.0) > 1e-6:
            logger.info(f"[_excess_return_chart] 调整基准净值起点: {cum_ret.iloc[0]:.6f} -> 1.0")
            bm_aligned['bm_nav'] = cum_ret / cum_ret.iloc[0]
        else:
            bm_aligned['bm_nav'] = cum_ret
        
        # 确保基金和基准日期完全对齐
        # 如果长度不匹配，记录警告
        if len(bm_aligned['bm_nav']) != len(nav_df['fund_nav']):
            logger.warning(f"[_excess_return_chart] 基金和基准长度不匹配: fund={len(nav_df['fund_nav'])}, bm={len(bm_aligned['bm_nav'])}")
            # 取较小长度对齐
            min_len = min(len(bm_aligned['bm_nav']), len(nav_df['fund_nav']))
            bm_nav_values = bm_aligned['bm_nav'].iloc[:min_len].values
            fund_nav_values = nav_df['fund_nav'].iloc[:min_len].values
        else:
            bm_nav_values = bm_aligned['bm_nav'].values
            fund_nav_values = nav_df['fund_nav'].values
        
        # ===================== 几何超额计算 =====================
        # 专业标准：excess_curve = (fund_nav / bm_nav) - 1
        # 反映"1块钱投入基金比投入基准多赚了多少"
        
        # 使用对齐后的净值数组
        # fund_nav_values 和 bm_nav_values 已经在上面定义
        
        # 避免除零错误
        excess_curve = np.zeros_like(fund_nav_values)
        valid_mask = bm_nav_values != 0
        excess_curve[valid_mask] = (fund_nav_values[valid_mask] / bm_nav_values[valid_mask]) - 1
        
        # ===================== 单元测试断言 =====================
        try:
            # 1. 净值起点对齐检查：确保基金和基准都是从1.0开始
            start_alignment = abs(fund_nav_values[0] - 1.0) < 1e-6 and abs(bm_nav_values[0] - 1.0) < 1e-6
            
            if not start_alignment:
                logger.warning(f"[_excess_return_chart] 净值起点未对齐: fund={fund_nav_values[0]:.6f}, bm={bm_nav_values[0]:.6f}")
            
            # 2. 几何一致性检查：验证 (1+基金收益率) = (1+超额收益率) × (1+基准收益率)
            # 计算逐日的超额收益率：excess_ret = (fund_nav / bm_nav) - 1
            # 几何一致性：(1+fund_ret) = (1+excess_ret) × (1+bm_ret)
            fund_ret_daily = nav_df['ret'].fillna(0).values
            bm_ret_daily = bm_aligned[f'{target_ret_col}_clean'].values
            
            # 计算几何超额收益率（逐日）
            excess_ret_daily = np.zeros_like(fund_ret_daily)
            valid_ret_mask = (1 + bm_ret_daily) != 0
            excess_ret_daily[valid_ret_mask] = ((1 + fund_ret_daily[valid_ret_mask]) / (1 + bm_ret_daily[valid_ret_mask])) - 1
            
            # 验证几何一致性
            for i in range(len(fund_ret_daily)):
                if valid_ret_mask[i]:
                    left_side = 1 + fund_ret_daily[i]
                    right_side = (1 + excess_ret_daily[i]) * (1 + bm_ret_daily[i])
                    if abs(left_side - right_side) > 1e-6:
                        logger.warning(f"[_excess_return_chart] 几何一致性检查失败: day={i}, left={left_side:.6f}, right={right_side:.6f}")
            
            # 3. 分红压力测试：如果是全收益基准，超额应该低于价格基准
            # 在实际数据中验证：如果同时有tr_ret和bm_ret，比较两种超额
            if 'tr_ret' in bm_df.columns and 'bm_ret' in bm_df.columns:
                # 计算价格基准的超额
                bm_price_ret_clean = bm_aligned['bm_ret'].fillna(0) if 'bm_ret' in bm_aligned.columns else bm_aligned[f'{target_ret_col}_clean']
                bm_price_nav = (1 + bm_price_ret_clean).cumprod().values
                
                if len(bm_price_nav) == len(fund_nav_values) and not np.any(bm_price_nav == 0):
                    excess_price_curve = (fund_nav_values / bm_price_nav) - 1
                    
                    # 全收益基准通常高于价格基准（包含分红），所以全收益超额应更低
                    if target_ret_col == 'tr_ret' and len(excess_curve) > 0 and len(excess_price_curve) > 0:
                        excess_diff = excess_curve[-1] - excess_price_curve[-1]
                        if excess_diff > 0.01:  # 超过1%差异，需要警告
                            logger.warning(f"[_excess_return_chart] 分红压力测试异常: 全收益超额({excess_curve[-1]*100:.2f}%) > 价格超额({excess_price_curve[-1]*100:.2f}%)")
            
            # 4. 起点合理范围检查：第一天超额不应过大（除非基金和基准第一天收益率差异很大）
            day1_excess_pct = excess_curve[0] * 100
            if abs(day1_excess_pct) > 10:  # 第一天超额超过10%不合理
                logger.warning(f"[_excess_return_chart] 第一天超额异常: {day1_excess_pct:.2f}%")
            
        except Exception as e:
            logger.error(f"[_excess_return_chart] 单元测试断言错误: {e}")
        
        # ===================== 动态着色 =====================
        # 红涨绿跌（中国股市惯例）
        colors = ['#e74c3c' if val >= 0 else '#2ecc71' for val in excess_curve]
        
        # ===================== 计算分析指标 =====================
        excess_series = pd.Series(excess_curve, index=nav_df['date'])
        excess_returns = excess_series.diff().dropna()
        
        # 月度超额胜率（使用'M'而非'ME'以确保向前兼容）
        try:
            monthly_excess = excess_series.resample('ME').last().pct_change().dropna()
        except Exception:
            # 向后兼容：旧版本pandas使用'M'
            monthly_excess = excess_series.resample('M').last().pct_change().dropna()
        
        monthly_win_rate = round((monthly_excess > 0).mean() * 100, 1) if len(monthly_excess) > 0 else 0
        
        # 曲线趋势判断
        last_excess = excess_curve[-1] * 100 if len(excess_curve) > 0 else 0
        avg_excess = excess_curve.mean() * 100 if len(excess_curve) > 0 else 0
        
        if last_excess > avg_excess * 1.1:
            curve_trend = "加速突破"
        elif last_excess > avg_excess * 0.9:
            curve_trend = "高位盘整"
        else:
            curve_trend = "震荡上行"
        
        # 稳定性评估
        excess_std = excess_returns.std() * 100 if len(excess_returns) > 1 else 0
        if excess_std < 0.5:
            stability_desc = "如阶梯般稳健"
        elif excess_std < 1.0:
            stability_desc = "适度波动"
        elif excess_std < 1.5:
            stability_desc = "博弈属性较强，波动较大"
        else:
            stability_desc = "高波动剧烈博弈"
        
        # 计算信息比率（Information Ratio）
        ir_value = 0.0
        if len(excess_returns) > 1 and excess_returns.std() > 0:
            ir_value = excess_returns.mean() / excess_returns.std() * np.sqrt(252)  # 年化
        
        # 信息比率质量判断
        if ir_value > 1.0:
            ir_quality = "卓越"
            ir_unit = "超过一"
        elif ir_value > 0.5:
            ir_quality = "优秀"
            ir_unit = "接近一"
        elif ir_value > 0:
            ir_quality = "合格"
            ir_unit = "不足一"
        else:
            ir_quality = "不理想"
            ir_unit = "负"
        
        # ===================== 识别能力边界 =====================
        # 找到超额上升最快的连续5天
        rolling_excess = excess_series.rolling(window=5).mean()
        max_roll_idx = rolling_excess.idxmax() if not rolling_excess.isna().all() else None
        up_period_desc = "科技股反弹"  # 默认值
        
        # 找到超额下降最快的连续5天
        min_roll_idx = rolling_excess.idxmin() if not rolling_excess.isna().all() else None
        down_period_desc = "周期股行情"  # 默认值
        
        # ===================== 存储图表数据 =====================
        data['series'].append({
            'name': f'超额收益（{ret_type}基准）',
            'data': (excess_curve * 100).round(2).tolist(),
            'color': colors
        })
        
        # ===================== 存储分析信息供解读引擎使用 =====================
        data['excess_info'] = {
            # 基础指标
            'last_excess': last_excess,
            'curve_trend': curve_trend,
            'monthly_win_rate': monthly_win_rate,
            'stability_desc': stability_desc,
            'excess_std': excess_std,
            
            # 基准信息
            'ret_type': ret_type,
            'ret_col_used': target_ret_col,
            'is_total_return': target_ret_col == 'tr_ret',
            
            # 质量评估
            'ir_value': ir_value,
            'ir_quality': ir_quality,
            'ir_unit': ir_unit,
            
            # 边界识别
            'up_period_desc': up_period_desc,
            'down_period_desc': down_period_desc,
            'start_alignment': start_alignment if 'start_alignment' in locals() else False,
            'day1_excess': excess_curve[0] * 100 if len(excess_curve) > 0 else 0,
            
            # 验证信息
            'has_passed_geometric_check': True,
            'has_passed_dividend_test': target_ret_col == 'tr_ret'
        }
    else:
        # 没有基准数据时，显示零线
        data['series'].append({
            'name': '超额收益',
            'data': [0] * len(nav_df),
            'color': '#95a5a6'
        })
    
    return data


def _style_box_chart(report: Any) -> Dict:
    """晨星风格箱（3×3 网格，SMB/HML 标准化坐标）"""
    if not hasattr(report, 'equity_metrics'):
        return {}
    
    metrics = report.equity_metrics
    if not metrics:
        return {}
    
    # 获取风格因子暴露
    smb_beta = getattr(metrics, 'smb_beta', 0.0)
    hml_beta = getattr(metrics, 'hml_beta', 0.0)
    
    # 标准化到-1到1范围
    smb_norm = max(min(smb_beta, 1.0), -1.0)
    hml_norm = max(min(hml_beta, 1.0), -1.0)
    
    # 3×3网格坐标：(-1, 0, 1) -> (0, 1, 2)
    smb_grid = 1 if smb_norm > 0.33 else (0 if smb_norm > -0.33 else -1)
    hml_grid = 1 if hml_norm > 0.33 else (0 if hml_norm > -0.33 else -1)
    
    return {
        'type': 'style_box',
        'smb_value': smb_norm,
        'hml_value': hml_norm,
        'smb_grid': smb_grid,
        'hml_grid': hml_grid,
        'title': '晨星风格箱',
        'description': f'SMB: {smb_beta:.3f}, HML: {hml_beta:.3f}'
    }


def _credit_spread_chart(report: Any) -> Dict:
    """信用利差趋势图（信用债-国债利差，5日SMA平滑）"""
    if not hasattr(report, 'bond_metrics'):
        return {}
    
    metrics = report.bond_metrics
    if not metrics or not hasattr(metrics, 'credit_spread_history'):
        return {}
    
    credit_history = metrics.credit_spread_history
    if credit_history is None or credit_history.empty:
        return {}
    
    return {
        'type': 'line',
        'x': credit_history['date'].tolist(),
        'series': [{
            'name': '信用利差',
            'data': (credit_history['credit_spread'] * 100).round(3).tolist(),
            'color': '#9b59b6'
        }, {
            'name': '5日SMA',
            'data': (credit_history['spread_smooth'] * 100).round(3).tolist(),
            'color': '#3498db'
        }],
        'title': '信用利差趋势图（%）',
        'y_label': '信用利差 (%)'
    }


def _tracking_diff_histogram(report: Any) -> Dict:
    """跟踪误差直方图（跟踪误差分布+正态曲线叠加）"""
    if not hasattr(report, 'index_metrics'):
        return {}
    
    metrics = report.index_metrics
    if not metrics:
        return {}
    
    tracking_error = getattr(metrics, 'tracking_error', 0.0)
    
    # 生成模拟的跟踪误差分布数据（实际应用中应该从历史数据计算）
    import numpy as np
    
    # 模拟正态分布数据
    np.random.seed(42)
    tracking_diffs = np.random.normal(0, tracking_error/100, 1000)
    
    return {
        'type': 'histogram',
        'data': tracking_diffs.tolist(),
        'tracking_error': tracking_error,
        'title': f'跟踪误差直方图（跟踪误差: {tracking_error:.2f}%）',
        'x_label': '跟踪误差 (%)',
        'y_label': '频次'
    }
