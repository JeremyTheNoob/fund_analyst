"""
数据加载器模块 — fund_quant_v2
"""

# 基础API函数
from .base_api import (
    cached, retry, safe_df,
    _ak_fund_nav, _ak_fund_asset_allocation,
    _ak_index_daily_main, _ak_index_daily_em,
)

__all__ = [
    # 基础API
    "cached", "retry", "safe_df",
    "_ak_fund_nav", "_ak_fund_asset_allocation",
    "_ak_index_daily_main", "_ak_index_daily_em",
]

# 延迟导入其他模块，避免循环依赖
def __getattr__(name):
    if name == "load_equity_data":
        from .equity_loader import load_equity_data
        return load_equity_data
    elif name == "load_bond_data":
        from .bond_loader import load_bond_data
        return load_bond_data
    elif name in ["load_etf_nav_and_price", "load_benchmark_index",
                  "load_etf_holdings_ratios", "load_etf_daily_trading", "infer_benchmark_code"]:
        from . import index_loader
        return getattr(index_loader, name)
    else:
        raise AttributeError(f"module 'fund_quant_v2.data_loader' has no attribute '{name}'")