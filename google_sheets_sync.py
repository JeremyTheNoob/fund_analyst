"""
Google Sheets 集成模块 - 自动同步统计数据
"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sqlite3
from datetime import datetime
from typing import Optional
import os

# Google Sheets 凭证文件路径
CREDENTIALS_FILE = "credentials.json"  # 需要用户提供的凭证文件


class GoogleSheetsSync:
    """Google Sheets 同步器"""
    
    def __init__(self, credentials_file: str = CREDENTIALS_FILE):
        self.credentials_file = credentials_file
        self.client = None
        self._init_client()
    
    def _init_client(self):
        """初始化 Google Sheets 客户端"""
        try:
            # 检查凭证文件是否存在
            if not os.path.exists(self.credentials_file):
                print(f"[GoogleSheetsSync] 凭证文件不存在: {self.credentials_file}")
                print("[GoogleSheetsSync] 请先创建 Google Cloud 项目并下载 credentials.json")
                return
            
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)
            self.client = gspread.authorize(credentials)
            print("[GoogleSheetsSync] Google Sheets 客户端初始化成功")
        except Exception as e:
            print(f"[GoogleSheetsSync] 初始化失败: {e}")
            self.client = None
    
    def sync_statistics(self, db_file: str, spreadsheet_key: str):
        """同步统计数据到 Google Sheets"""
        if not self.client:
            print("[GoogleSheetsSync] 客户端未初始化，跳过同步")
            return False
        
        try:
            # 打开表格
            sh = self.client.open_by_key(spreadsheet_key)
            
            # 获取数据库连接
            conn = sqlite3.connect(db_file)
            
            # 同步访问记录
            self._sync_page_visits(conn, sh.worksheet("访问记录"))
            
            # 同步搜索记录
            self._sync_fund_searches(conn, sh.worksheet("搜索记录"))
            
            # 同步每日统计
            self._sync_daily_summary(conn, sh.worksheet("每日统计"))
            
            conn.close()
            print("[GoogleSheetsSync] 数据同步成功")
            return True
        except Exception as e:
            print(f"[GoogleSheetsSync] 同步失败: {e}")
            return False
    
    def _sync_page_visits(self, conn, worksheet):
        """同步页面访问记录"""
        try:
            # 获取最新数据（最近1000条）
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, visit_time, session_id, ip_address, page_url, user_agent
                FROM page_visits
                ORDER BY visit_time DESC
                LIMIT 1000
            ''')
            rows = cursor.fetchall()
            
            if not rows:
                print("[GoogleSheetsSync] 没有访问记录需要同步")
                return
            
            # 清空工作表
            worksheet.clear()
            
            # 写入表头
            worksheet.update('A1:F1', [['ID', '访问时间', '会话ID', 'IP地址', '页面URL', '用户代理']])
            
            # 写入数据（按时间倒序）
            data = []
            for row in reversed(rows):
                # 格式化时间
                row_list = list(row)
                if isinstance(row_list[1], str):
                    row_list[1] = row_list[1].replace('T', ' ')
                data.append(row_list)
            
            if data:
                # 批量更新数据
                worksheet.update(f'A2:F{len(data) + 1}', data)
                print(f"[GoogleSheetsSync] 已同步 {len(data)} 条访问记录")
        except Exception as e:
            print(f"[GoogleSheetsSync] 同步访问记录失败: {e}")
    
    def _sync_fund_searches(self, conn, worksheet):
        """同步基金搜索记录"""
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, search_time, fund_code, fund_name, session_id, 
                       search_duration_ms, success, error_message
                FROM fund_searches
                ORDER BY search_time DESC
                LIMIT 1000
            ''')
            rows = cursor.fetchall()
            
            if not rows:
                print("[GoogleSheetsSync] 没有搜索记录需要同步")
                return
            
            # 清空工作表
            worksheet.clear()
            
            # 写入表头
            worksheet.update('A1:H1', [['ID', '搜索时间', '基金代码', '基金名称', 
                                       '会话ID', '耗时(ms)', '成功', '错误信息']])
            
            # 写入数据
            data = []
            for row in reversed(rows):
                # 转换 success 为是/否
                row_list = list(row)
                row_list[6] = "是" if row_list[6] else "否"
                data.append(row_list)
            
            if data:
                worksheet.update(f'A2:H{len(data) + 1}', data)
                print(f"[GoogleSheetsSync] 已同步 {len(data)} 条搜索记录")
        except Exception as e:
            print(f"[GoogleSheetsSync] 同步搜索记录失败: {e}")
    
    def _sync_daily_summary(self, conn, worksheet):
        """同步每日统计汇总"""
        try:
            cursor = conn.cursor()
            
            # 获取最近30天的每日统计
            from datetime import timedelta
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT date, page_views, unique_sessions, 
                       search_count, unique_funds
                FROM daily_summary
                WHERE date >= ?
                ORDER BY date DESC
            ''', (start_date,))
            rows = cursor.fetchall()
            
            if not rows:
                print("[GoogleSheetsSync] 没有每日统计数据需要同步")
                return
            
            # 清空工作表
            worksheet.clear()
            
            # 写入表头
            worksheet.update('A1:E1', [['日期', '访问量', '独立访客', '搜索次数', '独立基金数']])
            
            # 写入数据
            data = []
            for row in rows:
                data.append(list(row))
            
            if data:
                worksheet.update(f'A2:E{len(data) + 1}', data)
                print(f"[GoogleSheetsSync] 已同步 {len(data)} 条每日统计")
        except Exception as e:
            print(f"[GoogleSheetsSync] 同步每日统计失败: {e}")
    
    def test_connection(self, spreadsheet_key: str) -> bool:
        """测试连接"""
        if not self.client:
            return False
        
        try:
            sh = self.client.open_by_key(spreadsheet_key)
            title = sh.title
            print(f"[GoogleSheetsSync] 连接成功: {title}")
            return True
        except Exception as e:
            print(f"[GoogleSheetsSync] 连接失败: {e}")
            return False


# 创建全局同步器实例
_sync = None


def get_sync(credentials_file: str = CREDENTIALS_FILE) -> Optional[GoogleSheetsSync]:
    """获取同步器实例（单例模式）"""
    global _sync
    if _sync is None:
        _sync = GoogleSheetsSync(credentials_file)
    return _sync


if __name__ == "__main__":
    # 测试代码
    print("=== Google Sheets 同步模块测试 ===")
    
    sync = get_sync()
    
    if sync and sync.client:
        print("\n✅ Google Sheets 客户端初始化成功")
        print("\n📝 请提供以下信息进行测试:")
        print("  1. Spreadsheet Key (在 Google Sheets URL 中: https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY/edit)")
        
        # 测试连接（需要用户输入 Spreadsheet Key）
        # spreadsheet_key = input("\n请输入 Spreadsheet Key: ")
        # if sync.test_connection(spreadsheet_key):
        #     print("\n✅ 连接测试成功")
        
        # 测试同步（需要数据库文件）
        # sync.sync_statistics("fund_tracker.db", spreadsheet_key)
    else:
        print("❌ Google Sheets 客户端初始化失败")
        print("\n📋 请按以下步骤配置:")
        print("  1. 创建 Google Cloud 项目")
        print("  2. 启用 Google Sheets API")
        print("  3. 创建服务账号并下载 credentials.json")
        print("  4. 将 credentials.json 放到项目根目录")
        print("  5. 在 Google Sheets 中共享工作表给服务账号邮箱")
