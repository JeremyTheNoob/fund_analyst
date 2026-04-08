#!/usr/bin/env python3
"""
build_sqlite.py — 将 data/local_cache/ 下所有 CSV 文件导入 SQLite 数据库
================================================================
用法: python scripts/build_sqlite.py [--output data/fund_data.db] [--force]

输出: data/fund_data.db（约 200-300MB）

优化策略:
- 大表（>1M行）按基金代码/股票代码索引，查询毫秒级
- 所有 TEXT 列建索引（基金代码、日期、股票代码等）
- __CSV__: 前缀文件自动跳过第一行
"""

import sqlite3
import csv
import os
import sys
import time
import argparse
from pathlib import Path
from typing import Optional

# 增大 CSV 字段大小限制（某些单元格含长文本）
csv.field_size_limit(sys.maxsize)

# ── 项目路径 ──────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_ROOT = PROJECT_ROOT / "data" / "local_cache"


# ============================================================
# 表定义：{表名: (文件路径, 主键/索引列, 额外说明)}
# ============================================================
# 格式: (relative_path, [index_columns], description)
# 所有 CSV 文件名已统一为无日期后缀的固定名称
# ============================================================

TABLE_DEFS: dict[str, tuple[str, list[str], str]] = {
    # ── history/ 历史时序 ──────────────────────────────────
    "fund_nav": (
        "history/fund_nav.csv",
        ["基金代码", "净值日期"],
        "基金单位净值历史（追加更新）",
    ),
    "fund_nav_acc": (
        "history/fund_nav_acc.csv",
        ["基金代码", "净值日期"],
        "基金累计净值历史",
    ),
    "fund_stock_holdings": (
        "history/fund_portfolio_hold_em.csv",
        ["基金代码", "季度", "股票代码"],
        "基金股票持仓明细",
    ),
    "fund_bond_holdings": (
        "history/fund_portfolio_bond_hold_em.csv",
        ["基金代码", "季度", "债券代码"],
        "基金债券持仓明细",
    ),
    "fund_industry_alloc": (
        "history/fund_portfolio_industry_allocation_em.csv",
        ["基金代码", "季度"],
        "基金行业配置明细",
    ),
    "fund_hold_detail": (
        "history/fund_hold_detail.csv",
        ["基金代码", "date"],
        "基金资产配置快照（股票/债券/现金比例）",
    ),
    "fund_etf_hist": (
        "history/fund_etf_hist_sina.csv",
        ["基金代码", "date"],
        "ETF二级市场行情（新浪数据源）",
    ),
    "cb_value_analysis": (
        "history/cb_value_analysis.csv",
        ["date"],
        "可转债市场估值数据（均价/纯债/转股溢价率）",
    ),
    "bond_daily_hist": (
        "history/bond_daily_hist.csv",
        ["指数代码", "date"],
        "债券指数日行情（中证/国开等）",
    ),
    "bond_china_yield": (
        "history/bond_china_yield.csv",
        ["date"],
        "中债收益率曲线（AAA/国债各期限）",
    ),
    "stock_value": (
        "history/stock_value_em.csv",
        ["股票代码", "数据日期"],
        "A股个股估值（PE/PB/PEG）— 列裁剪: 只保留必要列",
    ),
    # NOTE: stock_daily_amt 不导入 SQLite，保留 CSV 文件按需加载
    # 该表当前未被业务代码使用，且数据量过大（2.1GB CSV）
    # 如需使用，请直接读取 data/local_cache/history/stock_daily_amt.csv
    "style_idx": (
        "history/style_idx.csv",
        ["index_code", "date"],
        "风格指数日行情（国证成长/价值等）",
    ),
    "total_return_idx": (
        "history/total_return_idx.csv",
        ["index_code", "date"],
        "全收益指数日行情",
    ),

    # ── static/ 慢变数据 ──────────────────────────────────
    "fund_meta": (
        "static/fund_meta.csv",
        ["code"],
        "基金基础信息主表（合并多源）",
    ),
    "fund_name_em": (
        "static/fund_name_em.csv",
        ["基金代码"],
        "基金名称+类型列表（东方财富）",
    ),
    "fund_overview_em": (
        "static/fund_overview_em.csv",
        ["基金代码"],
        "基金概览信息（含费率/基准/经理）",
    ),
    "fund_individual_basic_xq": (
        "static/fund_individual_basic_info_xq.csv",
        ["基金代码"],
        "基金基本信息详情（雪球）",
    ),
    "fund_individual_detail_xq": (
        "static/fund_individual_detail_info_xq.csv",
        ["基金代码"],
        "基金详细信息（雪球，含资产配置历史）",
    ),
    "fund_fee_em": (
        "static/fund_fee_em.csv",
        ["基金代码", "indicator"],
        "基金费率表（申购/赎回）",
    ),
    "fund_manager_em": (
        "static/fund_manager_em.csv",
        ["姓名"],
        "基金经理全量信息",
    ),
    "fund_manager_current": (
        "static/fund_manager_current.csv",
        ["基金代码"],
        "现任基金-经理映射表",
    ),
    "manager_start_date": (
        "static/manager_start_date.csv",
        ["基金代码"],
        "经理上任公告日期",
    ),
    "cb_info": (
        "static/cb_info.csv",
        ["代码"],
        "可转债基本信息",
    ),
    "cb_rating_lookup": (
        "static/cb_rating_lookup.csv",
        [],
        "信用评级映射表",
    ),
    "fund_info_index_gp": (
        "static/fund_info_index_em_gp.csv",
        ["基金代码"],
        "指数基金信息（上交所/深交所）",
    ),
    "fund_info_index_hs": (
        "static/fund_info_index_em_hs.csv",
        ["基金代码"],
        "指数基金信息（沪深两市）",
    ),

    # ── daily/ 日更数据 ───────────────────────────────────
    "fund_purchase_em": (
        "daily/fund_purchase_em.csv",
        ["基金代码"],
        "基金申购赎回状态",
    ),
    "fund_rating_all": (
        "daily/fund_rating_all.csv",
        ["基金代码"],
        "基金评级",
    ),
    "fund_aum_em": (
        "daily/fund_aum_em.csv",
        ["基金代码"],
        "基金规模（最新）",
    ),
    "fund_aum_hist_em": (
        "daily/fund_aum_hist_em.csv",
        ["基金代码"],
        "基金规模历史",
    ),
    "fund_holder": (
        "daily/fund_holder.csv",
        ["基金代码"],
        "基金持有人结构",
    ),
    "fund_profit": (
        "daily/fund_profit.csv",
        ["基金代码"],
        "基金盈利数据",
    ),
    "fund_share_change": (
        "daily/fund_share_change.csv",
        ["基金代码"],
        "基金份额变动",
    ),
    "fund_scale_change_em": (
        "daily/fund_scale_change_em.csv",
        ["基金代码"],
        "基金规模变动",
    ),
    "fund_announcement": (
        "daily/fund_announcement_personnel_em.csv",
        ["基金代码"],
        "基金人事公告",
    ),
    "fund_individual_profit_prob": (
        "daily/fund_individual_profit_probability_xq.csv",
        ["基金代码"],
        "基金盈利概率（雪球）",
    ),
}


def find_latest_file(pattern: str) -> Optional[Path]:
    """在 CACHE_ROOT 下找匹配 pattern 的最新文件（按文件名降序）"""
    matches = sorted(CACHE_ROOT.glob(pattern), reverse=True)
    return matches[0] if matches else None


def detect_encoding(filepath: Path) -> str:
    """检测文件编码"""
    # 尝试 utf-8
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            f.read(1024)
        return "utf-8"
    except UnicodeDecodeError:
        pass
    # 尝试 gbk
    try:
        with open(filepath, "r", encoding="gbk") as f:
            f.read(1024)
        return "gbk"
    except UnicodeDecodeError:
        pass
    return "utf-8-sig"


def has_csv_prefix(filepath: Path) -> bool:
    """检查文件是否有 __CSV__: 前缀"""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()
            return first_line == "__CSV__:"
    except Exception:
        return False


def import_csv_to_table(
    conn: sqlite3.Connection,
    table_name: str,
    filepath: Path,
    index_columns: list[str],
) -> tuple[int, int]:
    """
    将 CSV 文件导入 SQLite 表。
    返回 (行数, 跳过行数)
    """
    encoding = detect_encoding(filepath)
    skip_prefix = has_csv_prefix(filepath)

    # 先读取表头
    skip_rows = 1 if skip_prefix else 0
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        if skip_prefix:
            f.readline()  # 跳过 __CSV__:
        reader = csv.reader(f)
        headers = next(reader)
        # 清理表头中的空白字符和特殊字符
        headers = [h.strip().replace("\ufeff", "") for h in headers]
        if not headers or all(h == "" for h in headers):
            return 0, 0

    # 去重列名（保留第一个，后续重复的加 _N 后缀）
    seen = set()
    unique_headers = []
    for h in headers:
        if h in seen:
            i = 2
            while f"{h}_{i}" in seen:
                i += 1
            h = f"{h}_{i}"
        seen.add(h)
        unique_headers.append(h)
    headers = unique_headers

    # ── 列裁剪策略 ─────────────────────────────────────────────
    # 定义需要保留的列（裁剪大表中用不到的列）
    COLUMN_FILTERS = {
        # 历史行情表
        "stock_value": ["数据日期", "股票代码", "PE(TTM)", "市净率", "PEG值"],
        # ETF行情：只保留核心字段
        "fund_etf_hist": ["date", "close", "amount", "基金代码"],
        # 债券指数：只保留核心字段
        "bond_daily_hist": ["date", "close", "债券代码"],
        # 风格指数：只保留核心字段
        "style_idx": ["date", "close", "index_code"],
        # 全收益指数：只保留核心字段
        "total_return_idx": ["date", "close", "index_code"],
        # 股票持仓：删除序号、持股数、持仓市值、year
        "fund_stock_holdings": ["股票代码", "股票名称", "占净值比例", "季度", "基金代码"],
        # 债券持仓：删除序号、持仓市值、year
        "fund_bond_holdings": ["债券代码", "债券名称", "占净值比例", "季度", "基金代码"],
        # 行业配置：删除序号、市值、year
        "fund_industry_alloc": ["行业类别", "占净值比例", "截止时间", "基金代码"],
    }

    # 检查是否需要列裁剪
    columns_to_keep = COLUMN_FILTERS.get(table_name)
    col_indices = None
    if columns_to_keep:
        # 找出需要保留的列的索引
        col_indices = []
        new_headers = []
        for keep_col in columns_to_keep:
            if keep_col in headers:
                col_indices.append(headers.index(keep_col))
                new_headers.append(keep_col)
        if col_indices:
            headers = new_headers
            print(f"   📋 {table_name}: 列裁剪 {len(unique_headers)} → {len(headers)} 列")

    # 记录去重后的列数（用于截断数据行）
    num_cols = len(headers)

    # 创建表
    cols_def = ", ".join(f'"{h}" TEXT' for h in headers)
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_def})'
    conn.execute(create_sql)

    # 清空旧数据（支持重建）
    conn.execute(f'DELETE FROM "{table_name}"')

    # 导入数据（使用事务，快速批量插入）
    placeholders = ", ".join("?" for _ in headers)
    insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

    row_count = 0
    skip_count = 0
    batch = []
    batch_size = 50000

    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        if skip_prefix:
            f.readline()
        reader = csv.reader(f)
        next(reader)  # 跳过表头

        for row in reader:
            try:
                # 跳过空行或异常行
                if not row or (len(row) == 1 and not row[0].strip()):
                    skip_count += 1
                    continue

                # 列裁剪：只保留需要的列
                if col_indices:
                    row = [row[i] if i < len(row) else "" for i in col_indices]
                else:
                    # 补齐或截断列数
                    if len(row) < num_cols:
                        row.extend([""] * (num_cols - len(row)))
                    elif len(row) > num_cols:
                        row = row[:num_cols]
                batch.append(row)
                row_count += 1

                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    batch = []
            except Exception:
                skip_count += 1
                continue

        if batch:
            conn.executemany(insert_sql, batch)

    # 创建索引（仅对存在的列创建）
    valid_indexes = [c for c in index_columns if c in headers]
    for col in valid_indexes:
        idx_name = f"idx_{table_name}_{col[:20]}"
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ("{col}")')
        except sqlite3.OperationalError:
            pass  # 索引创建失败不影响功能

    return row_count, skip_rows


def build_database(output_path: Path, force: bool = False) -> None:
    """主构建函数"""
    if output_path.exists() and not force:
        print(f"❌ {output_path} 已存在，使用 --force 覆盖")
        sys.exit(1)

    # 确保输出目录存在
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 删除旧文件
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.connect(str(output_path))
    conn.execute("PRAGMA journal_mode=WAL")       # 写入优化
    conn.execute("PRAGMA synchronous=NORMAL")     # 平衡安全与速度
    conn.execute("PRAGMA cache_size=-64000")       # 64MB 缓存
    conn.execute("PRAGMA temp_store=MEMORY")       # 临时表放内存

    total_start = time.time()
    stats = []

    print(f"🚀 开始构建 SQLite 数据库: {output_path}")
    print(f"   数据源: {CACHE_ROOT}")
    print(f"   表数量: {len(TABLE_DEFS)}")
    print()

    for table_name, (pattern, index_cols, desc) in TABLE_DEFS.items():
        t0 = time.time()

        # 查找文件
        if "*" in pattern:
            filepath = find_latest_file(pattern)
        else:
            filepath = CACHE_ROOT / pattern

        if filepath is None or not filepath.exists():
            print(f"   ⚠️  {table_name}: 文件未找到 ({pattern})")
            stats.append((table_name, desc, 0, 0, -1))
            continue

        size_mb = filepath.stat().st_size / (1024 * 1024)

        # 导入
        rows, skipped = import_csv_to_table(conn, table_name, filepath, index_cols)
        elapsed = time.time() - t0

        if rows > 0:
            print(f"   ✅ {table_name}: {rows:>12,} 行 | {size_mb:>6.1f} MB | {elapsed:>5.1f}s")
        else:
            print(f"   ⚠️  {table_name}: 0 行（文件可能为空）")

        stats.append((table_name, desc, rows, size_mb, elapsed))

        # 每个表导入后 commit 一次，避免事务过大
        conn.commit()

    # 创建统计视图
    _create_views(conn)

    # 获取最终大小（VACUUM 前先看大小，如果太大则跳过 VACUUM）
    pre_vacuum_size = output_path.stat().st_size / (1024 * 1024)
    print()
    print(f"📦 导入完成，数据库大小: {pre_vacuum_size:.1f} MB")

    if pre_vacuum_size < 2000:  # 只有小数据库才做 VACUUM
        print("📦 优化数据库（VACUUM）...")
        conn.execute("VACUUM")
    else:
        print("📦 数据库较大，跳过 VACUUM（节省时间和磁盘空间）")

    # 获取最终大小
    final_size_mb = output_path.stat().st_size / (1024 * 1024)
    total_elapsed = time.time() - total_start

    conn.close()

    # 打印总结
    total_rows = sum(s[2] for s in stats)  # s[2] = rows
    success_tables = sum(1 for s in stats if s[2] > 0)
    print()
    print("=" * 60)
    print(f"✅ 构建完成!")
    print(f"   数据库: {output_path}")
    print(f"   大小:   {final_size_mb:.1f} MB")
    print(f"   表数量: {success_tables}/{len(TABLE_DEFS)}")
    print(f"   总行数: {total_rows:,}")
    print(f"   耗时:   {total_elapsed:.1f}s")
    print("=" * 60)


def _create_views(conn: sqlite3.Connection) -> None:
    """创建常用查询视图"""

    # ── 基金类型快速查询视图 ──
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_fund_type AS
        SELECT
            a."基金代码" AS code,
            COALESCE(a."基金类型", b."fund_type", '') AS fund_type,
            COALESCE(a."基金简称", b."name", '') AS name
        FROM fund_name_em a
        LEFT JOIN fund_meta b ON a."基金代码" = b."code"
    """)

    # ── 基金费率汇总视图 ──
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_fund_fee AS
        SELECT
            a."基金代码" AS code,
            b."invest_strategy" AS strategy,
            b."management_fee" AS mgmt_fee,
            b."trustee_fee" AS custody_fee,
            b."sales_service_fee" AS sale_fee
        FROM fund_fee_em a
        LEFT JOIN fund_meta b ON a."基金代码" = b."code"
    """)

    # ── 基金经理视图 ──
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_fund_manager AS
        SELECT
            m."基金代码" AS code,
            m."基金经理" AS manager_name,
            e."姓名" AS manager_full_name,
            e."现任基金资产总规模" AS total_aum,
            e."累计从业时间" AS experience_days
        FROM fund_overview_em m
        LEFT JOIN fund_manager_em e ON m."基金经理" = e."姓名"
    """)


def main():
    parser = argparse.ArgumentParser(description="构建基金分析 SQLite 数据库")
    parser.add_argument(
        "--output", "-o",
        default=str(PROJECT_ROOT / "data" / "fund_data.db"),
        help="输出 .db 文件路径",
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="覆盖已存在的数据库",
    )
    args = parser.parse_args()

    build_database(Path(args.output), force=args.force)


if __name__ == "__main__":
    main()
