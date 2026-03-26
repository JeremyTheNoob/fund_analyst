"""
模型层模块
负责量化模型计算和归因分析
依赖：config, utils, data（不依赖services/ui）
约束：模型层不能调用Streamlit（st.write等）
"""

from .gateway import (
    analyze_fund,
    classify_fund_type,
)

__all__ = [
    'analyze_fund',
    'classify_fund_type',
]
