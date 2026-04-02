"""
Supabase 数据库初始化脚本
创建必要的表结构和 RLS 策略。

运行方式：python -m data_loader.init_supabase
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def init_database():
    """初始化 Supabase 数据库"""
    try:
        from config import SUPABASE_URL, SUPABASE_ANON_KEY
        from supabase import create_client

        client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        logger.info(f"✅ 连接到 Supabase: {SUPABASE_URL}")

    except Exception as e:
        logger.error(f"❌ Supabase 连接失败: {e}")
        logger.info("请检查 config.py 中的 SUPABASE_URL 和 SUPABASE_ANON_KEY")
        return False

    # SQL 语句：创建表 + 启用 RLS + 允许匿名读写
    sql_statements = [
        # 1. 数据缓存表
        """
        CREATE TABLE IF NOT EXISTS data_cache (
            cache_key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """,
        # 2. 启用 RLS
        "ALTER TABLE data_cache ENABLE ROW LEVEL SECURITY;",
        # 3. 允许匿名用户读写（anon key 有足够权限）
        "CREATE POLICY "Allow anon read" ON data_cache FOR SELECT USING (true);",
        "CREATE POLICY "Allow anon insert" ON data_cache FOR INSERT WITH CHECK (true);",
        "CREATE POLICY "Allow anon update" ON data_cache FOR UPDATE USING (true);",
        "CREATE POLICY "Allow anon delete" ON data_cache FOR DELETE USING (true);",
        # 4. 基金排名快照表（用于预热）
        """
        CREATE TABLE IF NOT EXISTS fund_rank_snapshot (
            id BIGSERIAL PRIMARY KEY,
            fund_code TEXT NOT NULL,
            fund_name TEXT,
            fund_type TEXT NOT NULL,
            rank_period TEXT NOT NULL,
            rank_value TEXT,
            snapshot_date DATE DEFAULT CURRENT_DATE,
            updated_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(fund_code, fund_type, rank_period, snapshot_date)
        );
        """,
        "ALTER TABLE fund_rank_snapshot ENABLE ROW LEVEL SECURITY;",
        "CREATE POLICY "Allow anon read rank" ON fund_rank_snapshot FOR SELECT USING (true);",
        "CREATE POLICY "Allow anon insert rank" ON fund_rank_snapshot FOR INSERT WITH CHECK (true);",
        "CREATE POLICY "Allow anon update rank" ON fund_rank_snapshot FOR UPDATE USING (true);",
        # 5. 创建索引（加速查询）
        "CREATE INDEX IF NOT EXISTS idx_data_cache_key ON data_cache (cache_key);",
        "CREATE INDEX IF NOT EXISTS idx_data_cache_updated ON data_cache (updated_at);",
        "CREATE INDEX IF NOT EXISTS idx_fund_rank_date ON fund_rank_snapshot (snapshot_date);",
        "CREATE INDEX IF NOT EXISTS idx_fund_rank_type ON fund_rank_snapshot (fund_type, rank_period);",
    ]

    # Supabase Python SDK 不直接支持执行原始 SQL，
    # 需要通过 RPC 或手动在 Supabase Dashboard 执行。
    # 我们尝试使用 postgres RPC。

    # 方法：先创建一个执行 SQL 的 RPC 函数（如果存在 admin 权限）
    # 但 anon key 通常没有执行 DDL 的权限。
    # 所以我们输出 SQL 供用户在 Dashboard 手动执行。

    print("\n" + "=" * 60)
    print("📋 请在 Supabase Dashboard 中手动执行以下 SQL：")
    print("=" * 60)
    print("\n操作步骤：")
    print("1. 打开 https://supabase.com/dashboard")
    print("2. 选择你的项目")
    print("3. 左侧菜单 → SQL Editor")
    print("4. 点击 'New query'")
    print("5. 粘贴下面的 SQL 并点击 'Run'")
    print()

    full_sql = "\n".join(sql_statements)
    print(full_sql)

    print("\n" + "=" * 60)
    print("⚠️  执行完后回来告诉我，我会验证连接是否正常。")
    print("=" * 60)

    return True


if __name__ == "__main__":
    success = init_database()
    sys.exit(0 if success else 1)
