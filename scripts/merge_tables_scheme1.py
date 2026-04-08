#!/usr/bin/env python3
"""数据库表整合 - 纯SQL版本"""
import sqlite3
from pathlib import Path

DB = str(Path(__file__).parent.parent / "data" / "fund_data.db")

def run(sql, label=""):
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(sql)
    conn.commit()
    conn.close()
    if label:
        print(f"  ✓ {label}")

def main():
    print("=" * 50)
    print("数据库表整合 - 方案一")
    print("=" * 50)

    # 1. funds
    print("[1/9] funds 表...")
    run("DROP TABLE IF EXISTS funds")
    run('''CREATE TABLE funds AS
        SELECT m.*,
               n."拼音缩写" AS pinyin_abbr, n."拼音全称" AS pinyin_full,
               o."基金全称" AS full_name, o."发行日期" AS issue_date,
               o."成立日期/规模" AS inception_scale, o."成立来分红" AS dividend,
               o."最高认购费率" AS max_subscribe_fee, o."最高申购费率" AS max_purchase_fee
        FROM fund_meta m
        LEFT JOIN fund_name_em n ON m.code = n."基金代码"
        LEFT JOIN fund_overview_em o ON m.code = o."基金代码"
    ''', "创建完成")
    run("CREATE INDEX IF NOT EXISTS idx_funds_code ON funds(code)")

    # 2. fund_nav_history
    print("[2/9] fund_nav_history 表...")
    run("DROP TABLE IF EXISTS fund_nav_history")
    run('''CREATE TABLE fund_nav_history AS
        SELECT n."基金代码" AS code, n."净值日期" AS date,
               n."单位净值" AS nav_unit, n."日增长率" AS daily_return,
               a."累计净值" AS nav_acc
        FROM fund_nav n
        LEFT JOIN fund_nav_acc a ON n."基金代码" = a."基金代码" AND n."净值日期" = a."净值日期"
    ''', "创建完成")
    run("CREATE INDEX IF NOT EXISTS idx_nav_code ON fund_nav_history(code)")

    # 3. fund_flow_history
    print("[3/9] fund_flow_history 表...")
    run("DROP TABLE IF EXISTS fund_flow_history")
    run('''CREATE TABLE fund_flow_history AS
        SELECT "基金代码" AS code, "截止日期" AS date,
               "期间申购_亿份" AS purchase_share, "期间赎回_亿份" AS redeem_share,
               "期末总份额_亿份" AS total_share, "期末净资产_亿元" AS total_aum,
               "净资产变动率" AS aum_change_rate
        FROM fund_share_change
    ''', "创建完成")
    run("CREATE INDEX IF NOT EXISTS idx_flow_code ON fund_flow_history(code)")

    # 4a. fund_stock_positions
    print("[4/9] 持仓表...")
    run("DROP TABLE IF EXISTS fund_stock_positions")
    run('''CREATE TABLE fund_stock_positions AS
        SELECT "基金代码" AS code, "股票代码" AS stock_code,
               "股票名称" AS stock_name, "占净值比例" AS weight_ratio, "季度" AS quarter
        FROM fund_stock_holdings
    ''', "股票持仓")
    run("CREATE INDEX IF NOT EXISTS idx_sp_code ON fund_stock_positions(code)")

    # 4b. fund_bond_positions
    run("DROP TABLE IF EXISTS fund_bond_positions")
    run('''CREATE TABLE fund_bond_positions AS
        SELECT "基金代码" AS code, "债券代码" AS bond_code,
               "债券名称" AS bond_name, "占净值比例" AS weight_ratio, "季度" AS quarter
        FROM fund_bond_holdings
    ''', "债券持仓")
    run("CREATE INDEX IF NOT EXISTS idx_bp_code ON fund_bond_positions(code)")

    # 4c. fund_asset_allocation
    run("DROP TABLE IF EXISTS fund_asset_allocation")
    run('''CREATE TABLE fund_asset_allocation AS
        SELECT "基金代码" AS code, "资产类型" AS asset_type,
               "仓位占比" AS allocation_ratio, date AS report_date
        FROM fund_hold_detail
    ''', "资产配置")

    # 4d. fund_industry_allocation
    run("DROP TABLE IF EXISTS fund_industry_allocation")
    run('''CREATE TABLE fund_industry_allocation AS
        SELECT "基金代码" AS code, "行业类别" AS industry_name,
               "占净值比例" AS weight_ratio, "截止时间" AS report_date
        FROM fund_industry_alloc
    ''', "行业配置")

    # 5a. managers
    print("[5/9] 经理表...")
    run("DROP TABLE IF EXISTS managers")
    run('''CREATE TABLE managers AS
        SELECT "姓名" AS name, "所属公司" AS company,
               "现任基金代码" AS current_fund_codes, "现任基金" AS current_fund_names,
               "累计从业时间" AS total_tenure,
               "现任基金资产总规模" AS current_total_aum, "现任基金最佳回报" AS best_return
        FROM fund_manager_em
    ''', "经理信息")
    run("CREATE INDEX IF NOT EXISTS idx_mgr_name ON managers(name)")

    # 5b. fund_manager_relation
    run("DROP TABLE IF EXISTS fund_manager_relation")
    run('''CREATE TABLE fund_manager_relation AS
        SELECT "基金代码" AS code, "基金名称" AS fund_name, "经理姓名" AS manager_name,
               "所属公司" AS company, "上任日期" AS start_date,
               "任职天数" AS tenure_days, "任职年限" AS tenure_years,
               "累计从业天数" AS total_tenure_days, "变更类型" AS change_type,
               "多经理标记" AS is_multi_manager, "经理序号" AS manager_order,
               "更新日期" AS updated_at
        FROM fund_manager_current
    ''', "任职关系")
    run("CREATE INDEX IF NOT EXISTS idx_rel_code ON fund_manager_relation(code)")

    # 6a. fund_ratings
    print("[6/9] 费率评级表...")
    run("DROP TABLE IF EXISTS fund_ratings")
    run('''CREATE TABLE fund_ratings AS
        SELECT "代码" AS code, "简称" AS short_name, "基金经理" AS manager,
               "基金公司" AS company, "5星评级家数" AS five_star_count,
               "上海证券" AS rating_sh, "招商证券" AS rating_zs,
               "济安金信" AS rating_jax, "晨星评级" AS rating_ms,
               "手续费" AS fee_text, "类型" AS fund_type
        FROM fund_rating_all
    ''', "基金评级")
    run("CREATE INDEX IF NOT EXISTS idx_rt_code ON fund_ratings(code)")

    # 6b. fund_redeem_fees
    run("DROP TABLE IF EXISTS fund_redeem_fees")
    run('''CREATE TABLE fund_redeem_fees AS
        SELECT "基金代码" AS code, "适用期限" AS holding_period,
               "赎回费率" AS redeem_rate, indicator AS fee_type
        FROM fund_fee_em
    ''', "赎回费率")

    # 7. fund_index_info
    print("[7/9] 指数基金表...")
    run("DROP TABLE IF EXISTS fund_index_info")
    run('''CREATE TABLE fund_index_info AS
        SELECT "基金代码" AS code, "基金名称" AS name, "单位净值" AS nav,
               "日期" AS date, "日增长率" AS daily_return,
               'A股' AS market, "跟踪标的" AS track_index, "跟踪方式" AS track_method
        FROM fund_info_index_gp
        UNION ALL
        SELECT "基金代码", "基金名称", "单位净值", "日期", "日增长率",
               '港股', "跟踪标的", "跟踪方式"
        FROM fund_info_index_hs
    ''', "指数基金")
    run("CREATE INDEX IF NOT EXISTS idx_idx_code ON fund_index_info(code)")

    # 8a. convertible_bonds
    print("[8/9] 可转债表...")
    run("DROP TABLE IF EXISTS convertible_bonds")
    run('''CREATE TABLE convertible_bonds AS
        SELECT SECURITY_CODE AS bond_code, SECURITY_NAME_ABBR AS bond_name,
               LISTING_DATE AS list_date, DELIST_DATE AS delist_date,
               CONVERT_STOCK_CODE AS convert_stock_code,
               RATING AS rating, VALUE_DATE AS value_date,
               ACTUAL_ISSUE_SCALE AS issue_scale,
               ISSUE_PRICE AS issue_price, INITIAL_TRANSFER_PRICE AS init_transfer_price,
               CONVERT_STOCK_PRICE AS stock_price, TRANSFER_PRICE AS transfer_price,
               CURRENT_BOND_PRICE AS bond_price,
               TRANSFER_PREMIUM_RATIO AS transfer_premium_ratio,
               BOND_EXPIRE AS expire_date
        FROM cb_info
    ''', "可转债基础")
    run("CREATE INDEX IF NOT EXISTS idx_cb_code ON convertible_bonds(bond_code)")

    # 8b. cb_valuation
    run("DROP TABLE IF EXISTS cb_valuation")
    run('''CREATE TABLE cb_valuation AS
        SELECT "债券代码" AS bond_code, date,
               "收盘价" AS close_price, "纯债价值" AS bond_floor_value,
               "转股价值" AS convert_value,
               "纯债溢价率" AS bond_premium_ratio,
               "转股溢价率" AS convert_premium_ratio
        FROM cb_value_analysis
    ''', "可转债估值")

    # 9. 视图
    print("[9/9] 视图...")
    run("DROP VIEW IF EXISTS v_fund_full")
    run('''CREATE VIEW v_fund_full AS
        SELECT f.*, r.rating_sh, r.rating_zs, r.rating_jax, r.rating_ms
        FROM funds f LEFT JOIN fund_ratings r ON f.code = r.code
    ''')
    run("DROP VIEW IF EXISTS v_fund_latest_nav")
    run('''CREATE VIEW v_fund_latest_nav AS
        SELECT code, date, nav_unit, nav_acc, daily_return
        FROM fund_nav_history n
        WHERE date = (SELECT MAX(date) FROM fund_nav_history WHERE code = n.code)
    ''')
    run("DROP VIEW IF EXISTS v_fund_current_manager")
    run('''CREATE VIEW v_fund_current_manager AS
        SELECT code, manager_name, start_date, tenure_days
        FROM fund_manager_relation
        WHERE manager_order = 1 OR manager_order = '1'
    ''')

    # 验证
    print("\n" + "=" * 50)
    print("验证结果:")
    conn = sqlite3.connect(DB)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    for (t,) in tables:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f"  {t}: {cnt:,} 条")
    views = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
    ).fetchall()
    for (v,) in views:
        print(f"  {v}: (视图)")
    conn.close()
    print("=" * 50)
    print("整合完成！")

if __name__ == "__main__":
    main()
