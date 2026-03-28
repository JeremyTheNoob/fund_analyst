"""
全收益指数缓存库配置文件 — fund_quant_v2
支持本地全收益指数缓存和定期更新机制

核心特性：
1. 价格指数获取 + 全收益合成算法
2. 本地Parquet文件缓存
3. 支持增量更新
4. 自动过期检查和重新生成
"""

from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime, timedelta
import os

# ============================================================
# 📊 指数库配置
# ============================================================

# 本地缓存目录
CACHE_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "data", "index_cache")
PRICE_INDEX_CACHE_DIR = os.path.join(CACHE_ROOT, "price_indices")
TOTAL_RETURN_CACHE_DIR = os.path.join(CACHE_ROOT, "total_return_indices")
INDEX_METADATA_FILE = os.path.join(CACHE_ROOT, "index_metadata.json")

# 创建缓存目录
os.makedirs(PRICE_INDEX_CACHE_DIR, exist_ok=True)
os.makedirs(TOTAL_RETURN_CACHE_DIR, exist_ok=True)
os.makedirs(CACHE_ROOT, exist_ok=True)

# ============================================================
# 🔧 缓存配置
# ============================================================

CACHE_CONFIG = {
    "ttl_days": 7,  # 缓存有效期（天）
    "max_file_size_mb": 10,  # 单个缓存文件最大大小（MB）
    "compression": "snappy",  # Parquet压缩算法
    "auto_cleanup": True,  # 自动清理过期缓存
    "default_history_days": 365 * 3,  # 默认获取历史天数（3年）
}

# ============================================================
# 📈 支持的全收益指数列表
# ============================================================

# 核心指数配置
SUPPORTED_INDEXES: Dict[str, Dict[str, any]] = {
    # 宽基指数
    "sh000300": {
        "name": "沪深300",
        "type": "broad_market",
        "default_div_yield": 0.025,  # 2.5% 年化股息率
        "update_frequency": "daily",  # 每日更新
        "min_history_days": 60,  # 最少60天历史数据
    },
    "sh000905": {
        "name": "中证500",
        "type": "broad_market",
        "default_div_yield": 0.018,  # 1.8%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    "sh000852": {
        "name": "中证1000",
        "type": "broad_market",
        "default_div_yield": 0.015,  # 1.5%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    "sh000016": {
        "name": "上证50",
        "type": "broad_market",
        "default_div_yield": 0.028,  # 2.8%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    "sz399006": {
        "name": "创业板指",
        "type": "broad_market",
        "default_div_yield": 0.012,  # 1.2%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    "sh000688": {
        "name": "科创50",
        "type": "broad_market",
        "default_div_yield": 0.010,  # 1.0%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    
    # 行业风格指数
    "sz399370": {
        "name": "国证成长",
        "type": "style",
        "default_div_yield": 0.014,  # 1.4%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    "sz399371": {
        "name": "国证价值",
        "type": "style",
        "default_div_yield": 0.030,  # 3.0%
        "update_frequency": "daily",
        "min_history_days": 60,
    },
    
    # 申万行业指数（示例）
    "801010.SI": {
        "name": "申万农林牧渔",
        "type": "sw_industry",
        "default_div_yield": 0.010,  # 1.0%
        "update_frequency": "weekly",  # 每周更新
        "min_history_days": 60,
    },
    "801040.SI": {
        "name": "申万钢铁",
        "type": "sw_industry",
        "default_div_yield": 0.035,  # 3.5%
        "update_frequency": "weekly",
        "min_history_days": 60,
    },
    "801120.SI": {
        "name": "申万食品饮料",
        "type": "sw_industry",
        "default_div_yield": 0.022,  # 2.2%
        "update_frequency": "weekly",
        "min_history_days": 60,
    },
    "801230.SI": {
        "name": "申万银行",
        "type": "sw_industry",
        "default_div_yield": 0.055,  # 5.5%
        "update_frequency": "weekly",
        "min_history_days": 60,
    },
}

# 指数代码别名映射（支持多种输入格式）
INDEX_ALIAS_MAP: Dict[str, str] = {
    # 沪深300
    "000300.SH": "sh000300",
    "000300": "sh000300",
    "CSI300": "sh000300",
    "HS300": "sh000300",
    
    # 中证500
    "000905.SH": "sh000905",
    "000905": "sh000905",
    "CSI500": "sh000905",
    
    # 中证1000
    "000852.SH": "sh000852",
    "000852": "sh000852",
    "CSI1000": "sh000852",
    
    # 上证50
    "000016.SH": "sh000016",
    "000016": "sh000016",
    "SSE50": "sh000016",
    
    # 创业板
    "399006.SZ": "sz399006",
    "399006": "sz399006",
    "GEM": "sz399006",
    
    # 科创50
    "000688.SH": "sh000688",
    "000688": "sh000688",
    "STAR50": "sh000688",
    
    # 申万行业指数
    "SW煤炭.SI": "801040.SI",
    "SW银行.SI": "801230.SI",
    "SW食品饮料.SI": "801120.SI",
    "SW医药生物.SI": "801150.SI",
}

# ============================================================
# 🕐 缓存过期时间配置（秒）
# ============================================================

CACHE_TTL = {
    "price_index": {
        "daily": 86400,      # 24小时
        "weekly": 604800,    # 7天
        "monthly": 2592000,  # 30天
    },
    "total_return": {
        "daily": 86400,      # 24小时
        "weekly": 604800,    # 7天
        "monthly": 2592000,  # 30天
    }
}

# ============================================================
# 🔧 合成算法参数
# ============================================================

# 交易日数量（用于年化股息率转日收益率）
TRADING_DAYS_PER_YEAR = 252

# 合成算法配置
SYNTHESIS_CONFIG = {
    "default_div_yield": 0.025,  # 默认股息率 2.5%
    "min_data_points": 30,       # 最少需要30个数据点
    "max_gap_days": 5,           # 最大允许的数据间隔天数
    "reindex_method": "ffill",   # 重索引方法
}

# ============================================================
# 📁 文件命名规则
# ============================================================

def get_price_index_filename(index_code: str) -> str:
    """生成价格指数缓存文件名"""
    return f"price_{index_code.replace('.', '_')}.parquet"

def get_total_return_filename(index_code: str) -> str:
    """生成全收益指数缓存文件名"""
    return f"total_return_{index_code.replace('.', '_')}.parquet"

def get_metadata_filename(index_code: str) -> str:
    """生成元数据文件名"""
    return f"metadata_{index_code.replace('.', '_')}.json"

# ============================================================
# 📆 定期更新配置
# ============================================================

UPDATE_SCHEDULE = {
    "daily_update_time": "18:00",  # 每日18:00更新
    "weekly_update_day": "sunday",  # 每周日更新
    "retry_times": 3,              # 更新失败重试次数
    "retry_delay": 300,            # 重试间隔（秒）
}