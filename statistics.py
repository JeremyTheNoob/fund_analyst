"""
统计模块 - 记录访问和搜索数据
"""
import sqlite3
from datetime import datetime
from typing import Optional

# 数据库初始化
def init_db():
    """初始化统计数据库"""
    conn = sqlite3.connect('statistics.db')
    cursor = conn.cursor()
    
    # 访问记录表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            visit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id TEXT,  # Streamlit会话ID或匿名用户ID
            ip_address TEXT,  # IP地址（如果可获取）
            user_agent TEXT,  # 用户代理
            page_url TEXT,  # 访问的页面
            session_id TEXT  # 会话ID
        )
    ''')
    
    # 搜索记录表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fund_code TEXT,  # 基金代码
            fund_name TEXT,  # 基金名称
            user_id TEXT,  # 用户ID
            ip_address TEXT,
            result_status TEXT,  # 结果状态：success/fail
            fund_type TEXT  -- 基金类型（可选）
        )
    ''')
    
    # 每日统计汇总表（用于快速查询）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_stats (
            date DATE PRIMARY KEY,
            pv INTEGER DEFAULT 0,  -- 访问量
            uv INTEGER DEFAULT 0,  -- 独立访客
            searches INTEGER DEFAULT 0,  -- 搜索次数
            unique_funds INTEGER DEFAULT 0,  -- 独立基金数
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    return True


def record_visit(user_id: str, ip_address: str = "unknown", user_agent: str = "unknown", page_url: str = "/"):
    """记录一次页面访问"""
    try:
        conn = sqlite3.connect('statistics.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO visits (user_id, ip_address, user_agent, page_url)
            VALUES (?, ?, ?, ?)
        ''', (user_id, ip_address, user_agent, page_url))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"记录访问失败: {e}")
        return False


def record_search(user_id: str, fund_code: str, fund_name: str = "", 
                   ip_address: str = "unknown", result_status: str = "success", 
                   fund_type: str = ""):
    """记录一次基金搜索"""
    try:
        conn = sqlite3.connect('statistics.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO searches (fund_code, fund_name, user_id, ip_address, result_status, fund_type)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (fund_code, fund_name, user_id, ip_address, result_status, fund_type))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"记录搜索失败: {e}")
        return False


def get_statistics(days: int = 7):
    """获取统计数据"""
    conn = sqlite3.connect('statistics.db')
    cursor = conn.cursor()
    
    # 计算日期范围
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # PV统计
    cursor.execute('''
        SELECT COUNT(*) FROM visits 
        WHERE date(visit_time) >= ?
    ''', (start_date,))
    pv = cursor.fetchone()[0]
    
    # UV统计（基于user_id去重）
    cursor.execute('''
        SELECT COUNT(DISTINCT user_id) FROM visits 
        WHERE date(visit_time) >= ?
    ''', (start_date,))
    uv = cursor.fetchone()[0]
    
    # 搜索次数
    cursor.execute('''
        SELECT COUNT(*) FROM searches 
        WHERE date(search_time) >= ?
    ''', (start_date,))
    searches = cursor.fetchone()[0]
    
    # 独立基金数（去重）
    cursor.execute('''
        SELECT COUNT(DISTINCT fund_code) FROM searches 
        WHERE date(search_time) >= ? AND result_status = 'success'
    ''', (start_date,))
    unique_funds = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'pv': pv,
        'uv': uv,
        'searches': searches,
        'unique_funds': unique_funds,
        'days': days,
        'start_date': start_date
    }


def get_daily_stats(days: int = 7):
    """获取每日统计数据（用于图表）"""
    conn = sqlite3.connect('statistics.db')
    cursor = conn.cursor()
    
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # 获取每日PV
    cursor.execute('''
        SELECT date(visit_time) as date, COUNT(*) as pv
        FROM visits 
        WHERE date(visit_time) >= ?
        GROUP BY date(visit_time)
        ORDER BY date
    ''', (start_date,))
    pv_data = cursor.fetchall()
    
    # 获取每日搜索次数
    cursor.execute('''
        SELECT date(search_time) as date, COUNT(*) as searches
        FROM searches 
        WHERE date(search_time) >= ?
        GROUP BY date(search_time)
        ORDER BY date
    ''', (start_date,))
    search_data = cursor.fetchall()
    
    conn.close()
    
    return {
        'pv_daily': pv_data,
        'search_daily': search_data
    }


def get_popular_funds(limit: int = 10):
    """获取热门基金（搜索次数最多的）"""
    conn = sqlite3.connect('statistics.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT fund_code, fund_name, COUNT(*) as search_count
        FROM searches
        WHERE result_status = 'success'
        GROUP BY fund_code
        ORDER BY search_count DESC
        LIMIT ?
    ''', (limit,))
    
    results = cursor.fetchall()
    conn.close()
    
    return [{'fund_code': r[0], 'fund_name': r[1], 'count': r[2]} for r in results]
