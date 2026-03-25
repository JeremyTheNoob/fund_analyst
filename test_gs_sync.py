"""
测试 Google Sheets 连接
"""
from google_sheets_sync import get_sync

# 测试连接
print("=== 测试 Google Sheets 连接 ===\n")

sync = get_sync()

if sync and sync.client:
    print("✅ Google Sheets 客户端初始化成功")
    
    # 测试连接
    spreadsheet_key = "d128584acfb31d7d4463d7f3f6804698ae74cb6a"
    
    if sync.test_connection(spreadsheet_key):
        print("\n✅ 连接测试成功！")
        print("\n接下来可以：")
        print("  1. 运行主程序：streamlit run fund_analysis.py")
        print("  2. 访问几次网站并搜索基金")
        print("  3. 统计数据会自动同步到 Google Sheets")
    else:
        print("\n❌ 连接失败，请检查：")
        print("  1. Spreadsheet Key 是否正确")
        print("  2. 工作表是否已共享给服务账号")
        print("  3. 工作表名称是否正确：访问记录、搜索记录、每日统计")
else:
    print("❌ Google Sheets 客户端初始化失败")
    print("\n请检查：")
    print("  1. credentials.json 文件是否存在于项目根目录")
    print("  2. 文件格式是否正确")
    print("  3. Google Sheets API 是否已启用")
