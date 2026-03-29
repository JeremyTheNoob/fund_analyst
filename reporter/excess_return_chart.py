"""
超额收益曲线生成函数 — fund_quant_v2
计算基金相对于业绩基准的几何超额收益曲线
"""

from __future__ import annotations
from typing import Dict, Any
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def compute_excess_return_series(
    fund_ret: pd.Series,
    bm_ret: pd.Series,
    method: str = "geometric"
) -> pd.Series:
    """
    计算超额收益序列
    
    Args:
        fund_ret: 基金收益率序列
        bm_ret: 基准收益率序列（需与 fund_ret 日期对齐）
        method: 'geometric'（几何超额，推荐）或 'arithmetic'（算术超额）
    
    Returns:
        超额收益序列，索引与输入一致
    """
    # 确保日期对齐（inner join）
    aligned = pd.DataFrame({
        "fund_ret": fund_ret,
        "bm_ret": bm_ret
    }).dropna()
    
    if aligned.empty:
        logger.warning("[compute_excess_return_series] 对齐后数据为空")
        return pd.Series(dtype=float)
    
    # 几何超额：excess_ret = (1 + fund_ret) / (1 + bm_ret) - 1
    if method == "geometric":
        excess_ret = (1 + aligned["fund_ret"]) / (1 + aligned["bm_ret"]) - 1
    else:
        # 算术超额：excess_ret = fund_ret - bm_ret
        excess_ret = aligned["fund_ret"] - aligned["bm_ret"]
    
    # 处理极端情况：bm_ret = -1 导致分母为零
    excess_ret = excess_ret.replace([np.inf, -np.inf], 0)
    
    # 保持原始索引
    excess_ret.index = aligned.index
    
    return excess_ret


def generate_excess_return_chart(
    nav_df: pd.DataFrame,
    bm_df: pd.DataFrame,
    fund_name: str = "基金"
) -> Dict[str, Any]:
    """
    生成超额收益曲线的图表数据
    
    Args:
        nav_df: 基金净值数据（包含 date、ret 列）
        bm_df: 基准数据（包含 date、bm_ret 列）
        fund_name: 基金名称
    
    Returns:
        图表数据字典
    """
    if nav_df.empty or "ret" not in nav_df.columns:
        return {}
    
    if bm_df.empty or "bm_ret" not in bm_df.columns:
        logger.warning("[generate_excess_return_chart] 基准数据缺失")
        return {}
    
    nav_df = nav_df.copy()
    nav_df["date"] = pd.to_datetime(nav_df["date"])
    nav_df = nav_df.sort_values("date")
    
    bm_df = bm_df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    bm_df = bm_df.sort_values("date")
    
    # 设置日期索引
    nav_ret = nav_df.set_index("date")["ret"]
    bm_ret = bm_df.set_index("date")["bm_ret"]
    
    # 计算超额收益
    excess_ret = compute_excess_return_series(nav_ret, bm_ret, method="geometric")
    
    if excess_ret.empty:
        return {}
    
    # 累计超额收益
    cum_excess = (1 + excess_ret).cumprod() - 1
    
    # 转换为列表
    dates = cum_excess.index.strftime("%Y-%m-%d").tolist()
    values = [round(v * 100, 2) for v in cum_excess.values]
    
    # 颜色：正值用红色，负值用绿色（中国股市习惯）
    colors = []
    for v in values:
        if v >= 0:
            colors.append("#e74c3c")  # 红色
        else:
            colors.append("#27ae60")  # 绿色
    
    return {
        "type": "line",
        "x": dates,
        "series": [
            {
                "name": "累计超额收益",
                "data": values,
                "color": colors,  # 动态颜色
            }
        ],
        "title": "累计超额收益曲线（几何归因法）",
        "y_label": "超额收益率 (%)",
        "zero_line": True,  # 标注零线
    }
