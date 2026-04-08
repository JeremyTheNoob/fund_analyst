"""
db_accessor.py — SQLite 统一数据访问层
=======================================
替代现有散落在各 loader 中的 AkShare 实时调用。

所有查询直接走本地 SQLite 数据库，零网络依赖。
启动时若本地无 DB，可自动从腾讯云 COS 下载（需配置环境变量）。

使用方式:
    from data_loader.db_accessor import DB
    df = DB.query("SELECT * FROM fund_nav WHERE 基金代码 = ?", ("000001",))

数据库路径优先级:
    1. 环境变量 FUND_DB_PATH
    2. 项目目录 data/fund_data.db

COS 自动同步（可选，需设置环境变量）:
    COS_SECRET_ID / COS_SECRET_KEY / COS_BUCKET / COS_REGION
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── COS 自动同步（启动时本地无 DB 则下载） ─────────────────

def _try_sync_from_cos(db_path: Path) -> bool:
    """
    尝试从 COS 下载 DB。成功返回 True，失败返回 False（静默降级）。
    仅在本地 DB 不存在且 COS 环境变量齐全时触发。
    """
    required = ["COS_SECRET_ID", "COS_SECRET_KEY", "COS_BUCKET", "COS_REGION"]
    if not all(os.environ.get(k) for k in required):
        return False

    try:
        from qcloud_cos import CosConfig, CosS3Client
    except ImportError:
        logger.warning("[COS] cos-python-sdk-v5 未安装，跳过自动同步")
        return False

    manifest_key = "fund_data.manifest.json"
    db_key = "fund_data.db.tar.gz"
    bucket = os.environ["COS_BUCKET"]
    region = os.environ["COS_REGION"]

    try:
        config = CosConfig(Region=region, SecretId=os.environ["COS_SECRET_ID"],
                           SecretKey=os.environ["COS_SECRET_KEY"], Scheme="https")
        client = CosS3Client(config)

        # 检查 COS 上是否有 manifest
        resp = client.get_object(Bucket=bucket, Key=manifest_key)
        remote_manifest = json.loads(resp["Body"].read().decode("utf-8"))
        remote_md5 = remote_manifest.get("md5")
        remote_version = remote_manifest.get("version", "unknown")

        # 检查本地 manifest
        local_manifest_path = db_path.parent / ".cos_manifest.json"
        if local_manifest_path.exists():
            local_manifest = json.loads(local_manifest_path.read_text())
            if local_manifest.get("md5") == remote_md5:
                logger.info(f"[COS] 本地已是最新版本 ({remote_version})，跳过下载")
                return True

        # 下载
        compressed_path = db_path.parent / "fund_data.db.tar.gz"
        compressed_size = remote_manifest.get("compressed_size", 0)
        logger.info(f"[COS] 开始下载 DB ({compressed_size / 1024 / 1024:.0f} MB)...")
        start = time.time()

        client.download_file(Bucket=bucket, Key=db_key, DestFilePath=str(compressed_path))

        elapsed = time.time() - start
        speed = compressed_size / 1024 / 1024 / elapsed if elapsed > 0 else 0
        logger.info(f"[COS] 下载完成，耗时 {elapsed:.1f}s ({speed:.1f} MB/s)")

        # 解压
        logger.info("[COS] 解压中...")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["tar", "-xzf", str(compressed_path)], cwd=str(db_path.parent),
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            logger.error(f"[COS] 解压失败: {result.stderr}")
            compressed_path.unlink(missing_ok=True)
            return False

        compressed_path.unlink(missing_ok=True)
        local_manifest_path.write_text(json.dumps(remote_manifest, indent=2, ensure_ascii=False))

        db_size = db_path.stat().st_size if db_path.exists() else 0
        logger.info(f"[COS] 同步完成，DB 大小: {db_size / 1024 / 1024 / 1024:.2f} GB")
        return True

    except Exception as e:
        logger.error(f"[COS] 同步失败: {e}")
        return False

# ── 单例连接管理 ────────────────────────────────────────────

class _DBAccessor:
    """
    线程安全的 SQLite 数据库访问器（单例模式）。
    使用 WAL 模式支持多线程并发读。
    """
    _instance: Optional["_DBAccessor"] = None
    _lock = threading.Lock()

    def __new__(cls, db_path: Optional[str] = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db_path: Optional[str] = None):
        if self._initialized:
            return

        # 确定数据库路径
        if db_path:
            self._db_path = Path(db_path)
        else:
            # 环境变量 > 项目默认路径
            env_path = os.environ.get("FUND_DB_PATH")
            if env_path:
                self._db_path = Path(env_path)
            else:
                self._db_path = Path(__file__).resolve().parent.parent / "data" / "fund_data.db"

        if not self._db_path.exists():
            logger.info(f"[DB] 本地无数据库，尝试从 COS 同步...")
            if _try_sync_from_cos(self._db_path):
                logger.info(f"[DB] COS 同步成功")
            else:
                logger.warning(
                    f"[DB] 数据库不存在: {self._db_path}，且 COS 同步失败。"
                    "请运行 python3 scripts/sync_from_cos.py 或手动放置 DB 文件"
                )

        # 每个线程使用独立连接（SQLite WAL 模式支持并发读）
        self._local = threading.local()
        self._initialized = True
        logger.info(f"[DB] 数据访问器初始化完成: {self._db_path}")

    def _get_conn(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    @property
    def conn(self) -> sqlite3.Connection:
        return self._get_conn()

    @property
    def is_ready(self) -> bool:
        """数据库是否可用"""
        return self._db_path.exists()

    # ── 底层查询方法 ────────────────────────────────────────

    def query_df(
        self,
        sql: str,
        params: tuple = (),
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        执行 SQL 查询，返回 DataFrame。

        Args:
            sql: SQL 查询语句
            params: 参数
            columns: 指定返回的列（None = 全部列）

        Returns:
            DataFrame（空 DataFrame 如果无结果或出错）
        """
        try:
            df = pd.read_sql_query(sql, self.conn, params=params)
            if columns and not df.empty:
                existing = [c for c in columns if c in df.columns]
                if existing:
                    df = df[existing]
            return df
        except Exception as e:
            logger.error(f"[DB] 查询失败: {e}\nSQL: {sql}")
            return pd.DataFrame()

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        """查询单行，返回 dict"""
        try:
            row = self.conn.execute(sql, params).fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"[DB] 查询失败: {e}")
            return None

    def query_scalar(self, sql: str, params: tuple = ()) -> Any:
        """查询单个值"""
        try:
            row = self.conn.execute(sql, params).fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"[DB] 查询失败: {e}")
            return None

    def execute(self, sql: str, params: tuple = ()) -> None:
        """执行写操作"""
        self.conn.execute(sql, params)
        self.conn.commit()

    def table_info(self, table_name: str) -> list[str]:
        """获取表的列名列表"""
        try:
            cursor = self.conn.execute(f'PRAGMA table_info("{table_name}")')
            return [row[1] for row in cursor.fetchall()]
        except Exception:
            return []

    def table_count(self, table_name: str) -> int:
        """获取表的行数"""
        try:
            return self.query_scalar(f'SELECT COUNT(*) FROM "{table_name}"') or 0
        except Exception:
            return 0


# ── 全局单例 ─────────────────────────────────────────────────

# 首次导入时初始化
DB = _DBAccessor()


# ============================================================
# 高级查询函数（供各 loader / engine 调用）
# ============================================================

def get_fund_type(symbol: str) -> Optional[str]:
    """
    获取基金类型（来自 fund_name_em）。
    返回如 "混合型-偏股"、"债券型-长债" 等。
    """
    return DB.query_scalar(
        'SELECT "基金类型" FROM fund_name_em WHERE "基金代码" = ? LIMIT 1',
        (symbol,),
    )


def get_fund_name(symbol: str) -> Optional[str]:
    """获取基金简称"""
    return DB.query_scalar(
        'SELECT COALESCE("基金简称", "name") FROM fund_name_em WHERE "基金代码" = ? LIMIT 1',
        (symbol,),
    )


def get_fund_basic_info(symbol: str) -> Optional[dict]:
    """
    获取基金基础信息（合并多源）。
    从 fund_meta 主表读取，这是最完整的信息源。
    """
    row = DB.query_one(
        'SELECT * FROM fund_meta WHERE code = ? LIMIT 1',
        (symbol,),
    )
    return row


def get_fund_nav(symbol: str, years: int = 5) -> pd.DataFrame:
    """
    获取基金单位净值历史。
    返回列: 净值日期, 单位净值, 日增长率, 基金代码
    """
    return DB.query_df(
        'SELECT * FROM fund_nav WHERE "基金代码" = ? ORDER BY "净值日期" ASC',
        (symbol,),
    )


def get_fund_nav_acc(symbol: str) -> pd.DataFrame:
    """
    获取基金累计净值历史。
    返回列: 净值日期, 累计净值, 基金代码
    """
    return DB.query_df(
        'SELECT * FROM fund_nav_acc WHERE "基金代码" = ? ORDER BY "净值日期" ASC',
        (symbol,),
    )


def get_stock_holdings(symbol: str) -> pd.DataFrame:
    """
    获取基金股票持仓明细（最新季度）。
    返回列: 序号, 股票代码, 股票名称, 占净值比例, 持股数, 持仓市值, 季度, 基金代码, year
    """
    df = DB.query_df(
        'SELECT * FROM fund_stock_holdings WHERE "基金代码" = ? ORDER BY "季度" DESC, "序号" ASC',
        (symbol,),
    )
    if df.empty:
        return df
    # 只保留最新季度
    latest_q = df["季度"].iloc[0]
    return df[df["季度"] == latest_q].reset_index(drop=True)


def get_stock_holdings_all_quarters(symbol: str) -> pd.DataFrame:
    """获取基金所有季度的股票持仓"""
    return DB.query_df(
        'SELECT * FROM fund_stock_holdings WHERE "基金代码" = ? ORDER BY "季度" DESC, "序号" ASC',
        (symbol,),
    )


def get_bond_holdings(symbol: str) -> pd.DataFrame:
    """
    获取基金债券持仓明细（最新季度）。
    返回列: 序号, 债券代码, 债券名称, 占净值比例, 持仓市值, 季度, 基金代码, year
    """
    df = DB.query_df(
        'SELECT * FROM fund_bond_holdings WHERE "基金代码" = ? ORDER BY "季度" DESC, "序号" ASC',
        (symbol,),
    )
    if df.empty:
        return df
    latest_q = df["季度"].iloc[0]
    return df[df["季度"] == latest_q].reset_index(drop=True)


def get_industry_allocation(symbol: str) -> pd.DataFrame:
    """获取基金行业配置（最新季度）"""
    df = DB.query_df(
        'SELECT * FROM fund_industry_alloc WHERE "基金代码" = ? ORDER BY "截止时间" DESC',
        (symbol,),
    )
    if df.empty:
        return df
    latest = df["截止时间"].iloc[0]
    return df[df["截止时间"] == latest].reset_index(drop=True)


def get_asset_allocation(symbol: str) -> pd.DataFrame:
    """获取基金资产配置快照（股票/债券/现金比例）"""
    return DB.query_df(
        'SELECT * FROM fund_hold_detail WHERE "基金代码" = ? ORDER BY date DESC',
        (symbol,),
    )


def get_asset_allocation_detail(symbol: str) -> pd.DataFrame:
    """
    获取基金资产配置历史明细（来自雪球 fund_individual_detail_xq）。
    这个表包含更详细的资产配置历史。
    """
    # fund_individual_detail_xq 存储的是 item/value 键值对
    # 需要根据 symbol 过滤后透视
    return DB.query_df(
        'SELECT * FROM fund_individual_detail_xq WHERE "基金代码" = ?',
        (symbol,),
    )


def get_bond_china_yield() -> pd.DataFrame:
    """获取中债收益率曲线（全量）"""
    return DB.query_df(
        'SELECT * FROM bond_china_yield ORDER BY date ASC',
    )


def get_bond_china_yield_range(start_date: str, end_date: str) -> pd.DataFrame:
    """获取指定日期范围的中债收益率曲线"""
    return DB.query_df(
        'SELECT * FROM bond_china_yield WHERE date >= ? AND date <= ? ORDER BY date ASC',
        (start_date, end_date),
    )


def get_bond_daily_hist(code: Optional[str] = None) -> pd.DataFrame:
    """获取债券指数日行情"""
    if code:
        return DB.query_df(
            'SELECT * FROM bond_daily_hist WHERE "指数代码" = ? ORDER BY date ASC',
            (code,),
        )
    return DB.query_df('SELECT * FROM bond_daily_hist ORDER BY date ASC')


def get_cb_value_analysis() -> pd.DataFrame:
    """获取可转债市场估值数据（全量）"""
    return DB.query_df('SELECT * FROM cb_value_analysis ORDER BY date DESC')


def get_cb_value_analysis_latest() -> pd.DataFrame:
    """获取最新一天的可转债估值"""
    latest_date = DB.query_scalar('SELECT MAX(date) FROM cb_value_analysis')
    if not latest_date:
        return pd.DataFrame()
    return DB.query_df(
        'SELECT * FROM cb_value_analysis WHERE date = ?',
        (latest_date,),
    )


def get_style_index(index_code: str) -> pd.DataFrame:
    """获取风格指数日行情

    代码格式映射：数据库中存的是纯数字（如 399370），
    外部传入格式可能是 sz399370 / sh000300 等。
    """
    # 去掉交易所前缀
    normalized = index_code.lower().replace("sh", "").replace("sz", "").replace(".", "")
    code_map = {
        "399370": "399370", "399371": "399371", "399372": "399372",
        "399373": "399373", "399374": "399374", "399375": "399375",
        "399376": "399376", "399377": "399377",
    }
    target = code_map.get(normalized, normalized)
    return DB.query_df(
        'SELECT * FROM style_idx WHERE "index_code" = ? ORDER BY date ASC',
        (target,),
    )


def get_total_return_index(index_code: Optional[str] = None) -> pd.DataFrame:
    """获取全收益指数日行情

    代码格式映射（数据库中的 index_code 与外部传入格式不一致）：
      sh000300 → 000016（沪深300全收益）和 H00300（备用）
      sh000905 → H00905（中证500全收益）
      sh000852 → 000852（中证1000全收益）
    """
    if index_code:
        # 格式映射：统一数据库存储格式
        code_map = {
            "sh000300": ["000016", "H00300"],
            "sh000905": ["H00905"],
            "sh000852": ["000852"],
            "sz399006": ["399006"],
        }
        candidates = code_map.get(index_code, [index_code])
        for code in candidates:
            df = DB.query_df(
                'SELECT * FROM total_return_idx WHERE "index_code" = ? ORDER BY date ASC',
                (code,),
            )
            if df is not None and not df.empty:
                return df
        # 未命中，用原始代码查一次
        return DB.query_df(
            'SELECT * FROM total_return_idx WHERE "index_code" = ? ORDER BY date ASC',
            (index_code,),
        )
    return DB.query_df('SELECT * FROM total_return_idx ORDER BY date ASC')


def get_etf_hist(symbol: str) -> pd.DataFrame:
    """获取 ETF 二级市场行情"""
    return DB.query_df(
        'SELECT * FROM fund_etf_hist WHERE "基金代码" = ? ORDER BY date ASC',
        (symbol,),
    )


def get_stock_metrics(code: str) -> Optional[dict]:
    """
    获取个股最新估值指标。
    返回: {数据日期, PE(TTM), 市净率, PEG值, 总市值, ...}
    """
    return DB.query_one(
        'SELECT * FROM stock_value WHERE "股票代码" = ? ORDER BY "数据日期" DESC LIMIT 1',
        (code,),
    )


def get_fund_fee(symbol: str) -> pd.DataFrame:
    """获取基金费率"""
    return DB.query_df(
        'SELECT * FROM fund_fee_em WHERE "基金代码" = ?',
        (symbol,),
    )


def get_fund_manager(symbol: str) -> Optional[dict]:
    """
    获取基金经理信息。
    通过 fund_overview_em 表的"基金经理"列关联。
    """
    manager_name = DB.query_scalar(
        'SELECT "基金经理" FROM fund_overview_em WHERE "基金代码" = ? LIMIT 1',
        (symbol,),
    )
    if not manager_name:
        return None
    return DB.query_one(
        'SELECT * FROM fund_manager_em WHERE "姓名" = ? LIMIT 1',
        (manager_name,),
    )


def get_fund_purchase_status(symbol: str) -> Optional[dict]:
    """获取基金申购赎回状态"""
    return DB.query_one(
        'SELECT * FROM fund_purchase_em WHERE "基金代码" = ? LIMIT 1',
        (symbol,),
    )


def get_fund_rating(symbol: str) -> Optional[dict]:
    """获取基金评级"""
    return DB.query_one(
        'SELECT * FROM fund_rating_all WHERE "基金代码" = ? LIMIT 1',
        (symbol,),
    )


def get_cb_info(code: str) -> Optional[dict]:
    """获取可转债基本信息"""
    return DB.query_one(
        'SELECT * FROM cb_info WHERE "代码" = ? LIMIT 1',
        (code,),
    )


def get_bond_info(bond_code: str) -> Optional[dict]:
    """
    获取债券基础信息（期限、到期日等）。

    Args:
        bond_code: 债券代码（如 "240011"、"249942"）

    Returns:
        dict 含 maturity_date, bond_period, bond_period_years, coupon_type 等，
        或 None（未采集）
    """
    # 去掉交易所后缀
    clean_code = re.sub(r"\.(SH|SZ|IB)$", "", str(bond_code), flags=re.IGNORECASE)
    return DB.query_one(
        "SELECT * FROM bond_info WHERE bond_code = ? LIMIT 1",
        (clean_code,),
    )


def get_bond_info_batch(bond_codes: list[str]) -> dict[str, dict]:
    """
    批量获取债券基础信息。

    Args:
        bond_codes: 债券代码列表

    Returns:
        {bond_code: bond_info_dict} 的映射
    """
    if not bond_codes:
        return {}
    # 清理代码
    clean_codes = [re.sub(r"\.(SH|SZ|IB)$", "", str(c), flags=re.IGNORECASE) for c in bond_codes]
    placeholders = ",".join("?" for _ in clean_codes)
    df = DB.query_df(
        f"SELECT * FROM bond_info WHERE bond_code IN ({placeholders})",
        tuple(clean_codes),
    )
    if df.empty:
        return {}
    return {row["bond_code"]: dict(row) for _, row in df.iterrows()}


def get_fund_aum(symbol: str) -> Optional[dict]:
    """获取基金最新规模"""
    return DB.query_one(
        'SELECT * FROM fund_aum_em WHERE "基金代码" = ? LIMIT 1',
        (symbol,),
    )


# ============================================================
# 诊断函数
# ============================================================

def db_status() -> dict:
    """数据库状态摘要（用于调试）"""
    if not DB.is_ready:
        return {"status": "not_found", "path": str(DB._db_path)}

    tables = [
        "fund_nav", "fund_nav_acc", "fund_stock_holdings", "fund_bond_holdings",
        "fund_industry_alloc", "fund_hold_detail", "fund_etf_hist",
        "cb_value_analysis", "bond_daily_hist", "bond_china_yield",
        "stock_value", "style_idx", "total_return_idx",
        "fund_meta", "fund_name_em", "fund_overview_em",
        "fund_individual_basic_xq", "fund_individual_detail_xq",
        "fund_fee_em", "fund_manager_em", "fund_manager_current",
        # NOTE: stock_daily_amt 不导入 SQLite，保留 CSV 文件
    ]

    result = {
        "status": "ok",
        "path": str(DB._db_path),
        "size_mb": round(DB._db_path.stat().st_size / (1024 * 1024), 1),
        "tables": {},
    }

    for t in tables:
        count = DB.table_count(t)
        if count > 0:
            result["tables"][t] = count

    return result
