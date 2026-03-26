"""
服务层模块
提供业务逻辑和翻译服务
依赖：config, utils, data, models
约束：不能调用Streamlit
"""

from .translator import translate_results

__all__ = [
    'translate_results',
]
