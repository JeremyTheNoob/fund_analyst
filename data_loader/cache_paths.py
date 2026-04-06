"""
data_loader/cache_paths.py
==========================
统一的本地缓存路径常量。
所有脚本和数据加载器应从此文件导入路径，避免硬编码散落。

目录结构：
  data/local_cache/
  ├── static/     慢变数据（基础信息/费率/经理，≥7天更新）
  ├── daily/      日更数据（申购状态/评级/规模/持仓人）
  ├── history/    历史时序数据（净值/持仓/行情）
  └── raw/        临时/备用文件
"""

from pathlib import Path

# 项目根目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 缓存根目录
CACHE_ROOT = _PROJECT_ROOT / "data" / "local_cache"

# ── 分层目录 ──────────────────────────────────────────────────
STATIC_DIR = CACHE_ROOT / "static"   # 慢变数据
DAILY_DIR = CACHE_ROOT / "daily"     # 日更数据
HISTORY_DIR = CACHE_ROOT / "history" # 历史时序
RAW_DIR = CACHE_ROOT / "raw"         # 临时/备用

# ── static/ 具体文件 ──────────────────────────────────────────
FUND_META = STATIC_DIR / "fund_meta.csv"               # 基金基础信息主表（合并表）
FUND_MANAGER_CURRENT = STATIC_DIR / "fund_manager_current.csv"  # 现任经理状态
MANAGER_START_DATE = STATIC_DIR / "manager_start_date.csv"      # 经理上任公告原始表
FUND_FEE = STATIC_DIR                                  # fund_fee_em_*.csv（带日期后缀）
FUND_OVERVIEW = STATIC_DIR                             # fund_overview_em_*.csv
FUND_NAME_EM = STATIC_DIR                              # fund_name_em_*.csv
FUND_MANAGER_EM = STATIC_DIR                           # fund_manager_em_*.csv
FUND_INDIVIDUAL_BASIC_XQ = STATIC_DIR                  # fund_individual_basic_info_xq_*.csv
FUND_INDIVIDUAL_DETAIL_XQ = STATIC_DIR                 # fund_individual_detail_info_xq_*.csv
CB_RATING_LOOKUP = STATIC_DIR                          # cb_rating_lookup_*.csv
CB_INFO = STATIC_DIR                                   # cb_info_*.csv
FUND_INFO_INDEX_GP = STATIC_DIR                        # fund_info_index_em_gp_*.csv
FUND_INFO_INDEX_HS = STATIC_DIR                        # fund_info_index_em_hs_*.csv

# ── daily/ 具体文件 ───────────────────────────────────────────
FUND_PURCHASE_EM = DAILY_DIR                           # fund_purchase_em_*.csv
FUND_RATING = DAILY_DIR                                # fund_rating_all_*.csv
FUND_AUM_EM = DAILY_DIR                                # fund_aum_em_*.csv
FUND_AUM_HIST_EM = DAILY_DIR                           # fund_aum_hist_em_*.csv
FUND_HOLDER = DAILY_DIR                                # fund_holder_*.csv
FUND_PROFIT = DAILY_DIR / "fund_profit.csv"            # 基金盈利（无日期后缀）
FUND_SHARE_CHANGE = DAILY_DIR                          # fund_share_change_*.csv
FUND_SCALE_CHANGE = DAILY_DIR                          # fund_scale_change_em_*.csv
FUND_ANNOUNCEMENT = DAILY_DIR                          # fund_announcement_personnel_em_*.csv

# ── history/ 具体文件 ─────────────────────────────────────────
FUND_NAV = HISTORY_DIR / "fund_nav.csv"                # 净值历史（无日期后缀，追加更新）
FUND_NAV_ACC = HISTORY_DIR / "fund_nav_acc.csv"        # 累计净值历史
FUND_PORTFOLIO_STOCK = HISTORY_DIR                     # fund_portfolio_hold_em_*.csv
FUND_PORTFOLIO_BOND = HISTORY_DIR                      # fund_portfolio_bond_hold_em_*.csv
FUND_PORTFOLIO_INDUSTRY = HISTORY_DIR                  # fund_portfolio_industry_allocation_em_*.csv
FUND_HOLD_DETAIL = HISTORY_DIR / "fund_hold_detail.csv"
FUND_ETF_HIST = HISTORY_DIR / "fund_etf_hist_sina.csv"
CB_VALUE_ANALYSIS = HISTORY_DIR                        # cb_value_analysis_*.csv
BOND_DAILY_HIST = HISTORY_DIR                          # bond_daily_hist_*.csv
BOND_CHINA_YIELD = HISTORY_DIR                         # bond_china_yield_*.csv
STOCK_VALUE_EM = HISTORY_DIR                           # stock_value_em_*.csv
STOCK_DAILY_AMT = HISTORY_DIR                          # stock_daily_amt_*.csv
STYLE_IDX = HISTORY_DIR                                # style_idx_*.csv
TOTAL_RETURN_IDX = HISTORY_DIR                         # total_return_idx_*.csv


# ── 工具函数 ──────────────────────────────────────────────────

def latest(base_dir: Path, pattern: str) -> Path | None:
    """在目录下找最新的匹配文件（按文件名降序）"""
    files = sorted(base_dir.glob(pattern), reverse=True)
    return files[0] if files else None


def read_latest(base_dir: Path, pattern: str, **kwargs):
    """读取目录下最新匹配文件，返回 DataFrame 或 None"""
    import pandas as pd
    path = latest(base_dir, pattern)
    if path is None or not path.exists():
        return None
    raw = path.read_text(encoding="utf-8")
    skip = 1 if raw.startswith("__CSV__:") else 0
    return pd.read_csv(path, skiprows=skip, dtype=str, **kwargs)


def ensure_dirs():
    """确保所有缓存目录存在"""
    for d in [STATIC_DIR, DAILY_DIR, HISTORY_DIR, RAW_DIR]:
        d.mkdir(parents=True, exist_ok=True)
