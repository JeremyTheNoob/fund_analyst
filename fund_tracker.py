"""
基金统计追踪模块 - 独立统计功能
不侵入主程序，通过装饰器实现统计记录
"""
import sqlite3
import json
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Callable
from contextlib import contextmanager


class FundTracker:
    """基金统计追踪器"""
    
    def __init__(self, db_file: str = "fund_tracker.db"):
        self.db_file = db_file
        self._init_db()
    
    def _init_db(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # 访问记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS page_visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                visit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                session_id TEXT,
                ip_address TEXT,
                page_url TEXT,
                user_agent TEXT
            )
        ''')
        
        # 基金搜索记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fund_searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fund_code TEXT,
                fund_name TEXT,
                session_id TEXT,
                search_duration_ms INTEGER,
                success BOOLEAN,
                error_message TEXT
            )
        ''')
        
        # 每日统计汇总表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summary (
                date DATE PRIMARY KEY,
                page_views INTEGER DEFAULT 0,
                unique_sessions INTEGER DEFAULT 0,
                search_count INTEGER DEFAULT 0,
                unique_funds INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建索引提升查询性能
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_visit_time ON page_visits(visit_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_search_time ON fund_searches(search_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_code ON fund_searches(fund_code)')
        
        conn.commit()
        conn.close()
    
    def get_session_id(self, session_state):
        """获取会话ID"""
        if not hasattr(session_state, '_tracker_session_id'):
            session_state._tracker_session_id = f"session_{hash(datetime.now())}"
        return session_state._tracker_session_id
    
    def record_page_visit(self, session_state, ip_address: str = "unknown", 
                          page_url: str = "/"):
        """记录页面访问"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            session_id = self.get_session_id(session_state)
            cursor.execute('''
                INSERT INTO page_visits (session_id, ip_address, page_url, user_agent)
                VALUES (?, ?, ?, ?)
            ''', (session_id, ip_address, page_url, "Streamlit"))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[FundTracker] 记录页面访问失败: {e}")
    
    def record_fund_search(self, session_state, fund_code: str, 
                          fund_name: str = "", success: bool = True,
                          error_message: str = "", duration_ms: int = 0):
        """记录基金搜索"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            session_id = self.get_session_id(session_state)
            cursor.execute('''
                INSERT INTO fund_searches (fund_code, fund_name, session_id, 
                                           search_duration_ms, success, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (fund_code, fund_name, session_id, duration_ms, success, error_message))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[FundTracker] 记录基金搜索失败: {e}")
    
    def get_statistics(self, days: int = 30) -> dict:
        """获取统计数据"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # 页面访问量 (PV)
        cursor.execute('''
            SELECT COUNT(*) FROM page_visits
            WHERE date(visit_time) >= ?
        ''', (start_date,))
        page_views = cursor.fetchone()[0]
        
        # 独立会话数 (UV)
        cursor.execute('''
            SELECT COUNT(DISTINCT session_id) FROM page_visits
            WHERE date(visit_time) >= ?
        ''', (start_date,))
        unique_sessions = cursor.fetchone()[0]
        
        # 搜索次数
        cursor.execute('''
            SELECT COUNT(*) FROM fund_searches
            WHERE date(search_time) >= ?
        ''', (start_date,))
        search_count = cursor.fetchone()[0]
        
        # 独立基金数
        cursor.execute('''
            SELECT COUNT(DISTINCT fund_code) FROM fund_searches
            WHERE date(search_time) >= ? AND success = 1
        ''', (start_date,))
        unique_funds = cursor.fetchone()[0]
        
        # 成功率
        cursor.execute('''
            SELECT 
                CAST(SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS FLOAT) / 
                COUNT(*) * 100
            FROM fund_searches
            WHERE date(search_time) >= ?
        ''', (start_date,))
        success_rate = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            'page_views': page_views,
            'unique_sessions': unique_sessions,
            'search_count': search_count,
            'unique_funds': unique_funds,
            'success_rate': round(success_rate, 1),
            'days': days,
            'start_date': start_date
        }
    
    def get_daily_trends(self, days: int = 30) -> dict:
        """获取每日趋势数据（用于图表）"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # 每日访问量
        cursor.execute('''
            SELECT date(visit_time) as date, COUNT(*) as count
            FROM page_visits
            WHERE date(visit_time) >= ?
            GROUP BY date(visit_time)
            ORDER BY date
        ''', (start_date,))
        daily_visits = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 每日搜索量
        cursor.execute('''
            SELECT date(search_time) as date, COUNT(*) as count
            FROM fund_searches
            WHERE date(search_time) >= ?
            GROUP BY date(search_time)
            ORDER BY date
        ''', (start_date,))
        daily_searches = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        # 填充缺失日期
        dates = []
        for i in range(days):
            date = (datetime.now() - timedelta(days=days-1-i)).strftime('%Y-%m-%d')
            dates.append(date)
        
        return {
            'dates': dates,
            'daily_visits': [daily_visits.get(d, 0) for d in dates],
            'daily_searches': [daily_searches.get(d, 0) for d in dates]
        }
    
    def get_popular_funds(self, limit: int = 10, days: int = 30) -> list:
        """获取热门基金"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        cursor.execute('''
            SELECT fund_code, fund_name, COUNT(*) as search_count,
                   COUNT(DISTINCT session_id) as unique_users
            FROM fund_searches
            WHERE date(search_time) >= ? AND success = 1
            GROUP BY fund_code
            ORDER BY search_count DESC
            LIMIT ?
        ''', (start_date, limit))
        
        results = cursor.fetchall()
        conn.close()
        
        return [
            {
                'fund_code': row[0],
                'fund_name': row[1] or '未知',
                'search_count': row[2],
                'unique_users': row[3]
            }
            for row in results
        ]
    
    def export_statistics(self, days: int = 30) -> str:
        """导出统计数据为JSON字符串"""
        stats = self.get_statistics(days)
        trends = self.get_daily_trends(days)
        popular = self.get_popular_funds(days=days)
        
        export_data = {
            'generated_at': datetime.now().isoformat(),
            'summary': stats,
            'daily_trends': trends,
            'popular_funds': popular
        }
        
        return json.dumps(export_data, ensure_ascii=False, indent=2)


# 全局追踪器实例
_tracker = None

def get_tracker(db_file: str = "fund_tracker.db") -> FundTracker:
    """获取追踪器实例（单例模式）"""
    global _tracker
    if _tracker is None:
        _tracker = FundTracker(db_file)
    return _tracker


def track_page_visit(ip_address: str = "unknown", page_url: str = "/"):
    """
    装饰器：追踪页面访问
    用法：在Streamlit应用的开始处调用
    """
    def decorator(func):
        @wraps(func)
        def wrapper(session_state, *args, **kwargs):
            tracker = get_tracker()
            tracker.record_page_visit(session_state, ip_address, page_url)
            return func(session_state, *args, **kwargs)
        return wrapper
    return decorator


def track_fund_search(fund_code_param: str = "fund_code", fund_name_param: str = "fund_name"):
    """
    装饰器：追踪基金搜索
    用法：装饰执行搜索的函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            success = True
            error_message = ""
            result = None
            
            try:
                # 执行原始函数
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                error_message = str(e)
                raise
            finally:
                # 记录搜索
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                
                # 尝试从参数中获取基金代码和名称
                fund_code = kwargs.get(fund_code_param, "")
                fund_name = kwargs.get(fund_name_param, "")
                
                # 如果是位置参数，尝试提取
                if not fund_code and len(args) > 0:
                    fund_code = str(args[0])
                
                tracker = get_tracker()
                tracker.record_fund_search(
                    session_state=None,  # 这里需要传入session_state
                    fund_code=fund_code,
                    fund_name=fund_name,
                    success=success,
                    error_message=error_message,
                    duration_ms=duration_ms
                )
        
        return wrapper
    return decorator


@contextmanager
def search_timer(tracker: FundTracker, session_state, fund_code: str, fund_name: str = ""):
    """
    上下文管理器：计时并记录搜索
    用法：
    ```
    with search_timer(tracker, st.session_state, fund_code, fund_name):
        # 执行搜索逻辑
        result = analyze_fund(fund_code)
    ```
    """
    start_time = datetime.now()
    success = True
    error_message = ""
    
    try:
        yield
    except Exception as e:
        success = False
        error_message = str(e)
        raise
    finally:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        tracker.record_fund_search(
            session_state=session_state,
            fund_code=fund_code,
            fund_name=fund_name,
            success=success,
            error_message=error_message,
            duration_ms=duration_ms
        )


def render_statistics_panel(tracker: Optional[FundTracker] = None, days: int = 30):
    """
    在Streamlit中渲染统计面板
    用法：在页面底部调用
    """
    if tracker is None:
        tracker = get_tracker()
    
    # 获取统计数据并保存到文件
    stats = tracker.get_statistics(days)
    trends = tracker.get_daily_trends(days)
    popular = tracker.get_popular_funds(limit=10, days=days)
    
    # 保存到JSON文件（可随时查看）
    export_data = {
        'generated_at': datetime.now().isoformat(),
        'summary': stats,
        'daily_trends': trends,
        'popular_funds': popular
    }
    
    import json
    # 保存到项目根目录，命名为 statistics_data.json
    try:
        with open('statistics_data.json', 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        print(f"[FundTracker] 统计数据已保存到 statistics_data.json")
    except Exception as e:
        print(f"[FundTracker] 保存统计数据失败: {e}")
    
    # 尝试同步到 Google Sheets
    try:
        from google_sheets_sync import get_sync
        from gsheets_config import SPREADSHEET_KEY
        
        sync = get_sync()
        if sync and sync.client:
            sync.sync_statistics(tracker.db_file, SPREADSHEET_KEY)
    except Exception as e:
        print(f"[FundTracker] Google Sheets 同步失败: {e}")
    
    # 前端不显示任何统计信息，仅后台记录
    # 如果需要实时查看统计，可以访问该JSON文件或通过API获取


if __name__ == "__main__":
    # 测试代码
    tracker = FundTracker()
    
    # 模拟一些测试数据
    from types import SimpleNamespace
    
    class MockSessionState:
        _tracker_session_id = "test_session_123"
    
    session_state = MockSessionState()
    
    # 记录几次访问
    tracker.record_page_visit(session_state)
    tracker.record_page_visit(session_state)
    
    # 记录几次搜索
    tracker.record_fund_search(session_state, "000069", "华夏债券", success=True)
    tracker.record_fund_search(session_state, "110022", "易方达消费行业", success=True)
    tracker.record_fund_search(session_state, "000069", "华夏债券", success=True)
    
    # 查看统计
    print("\n📊 统计数据:")
    stats = tracker.get_statistics(days=1)
    print(f"  访问量 (PV): {stats['page_views']}")
    print(f"  独立访客 (UV): {stats['unique_sessions']}")
    print(f"  搜索次数: {stats['search_count']}")
    print(f"  独立基金数: {stats['unique_funds']}")
    
    print("\n🏆 热门基金:")
    popular = tracker.get_popular_funds(limit=5, days=1)
    for fund in popular:
        print(f"  {fund['fund_code']} - {fund['fund_name']}: {fund['search_count']} 次")
