#!/usr/bin/env python3
"""
基金代码目录管理器
实现基金代码的本地存储和快速校验
"""

import os
import json
import time
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime

from .base_api import (
    logger, retry, _ak_fund_list_em
)

# 配置
FUND_DIRECTORY_DIR = os.path.join(os.path.dirname(__file__), "../../../data/fund_directory")
FUND_DIRECTORY_FILE = os.path.join(FUND_DIRECTORY_DIR, "fund_directory.json")
FUND_DIRECTORY_CSV = os.path.join(FUND_DIRECTORY_DIR, "fund_directory.csv")

# 创建目录
os.makedirs(FUND_DIRECTORY_DIR, exist_ok=True)

# 缓存设置
CACHE_REFRESH_HOURS = 24  # 24小时刷新一次
CACHE_TTL_DAYS = 7        # 本地文件缓存7天


class FundDirectory:
    """基金代码目录管理器"""
    
    def __init__(self, auto_refresh: bool = True):
        self.directory: Dict[str, Dict[str, Any]] = {}
        self.code_list: List[str] = []
        self.name_map: Dict[str, str] = {}  # 代码->名称映射
        self.name_reverse_map: Dict[str, List[str]] = {}  # 名称->代码映射（可能有多个同名基金）
        
        # 初始化目录
        self.load_from_cache()
        
        # 自动刷新
        if auto_refresh:
            if not self.directory or self._should_refresh():
                self.refresh_directory()
    
    def _should_refresh(self) -> bool:
        """检查是否需要刷新目录"""
        cache_metadata_file = os.path.join(FUND_DIRECTORY_DIR, "metadata.json")
        
        try:
            if not os.path.exists(cache_metadata_file):
                return True
            
            with open(cache_metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            last_refresh_str = metadata.get('last_refresh')
            if not last_refresh_str:
                return True
            
            last_refresh = datetime.fromisoformat(last_refresh_str)
            now = datetime.now()
            
            # 检查是否超过刷新间隔
            time_diff = now - last_refresh
            return time_diff.total_seconds() > CACHE_REFRESH_HOURS * 3600
            
        except Exception as e:
            logger.warning(f"[FundDirectory] 检查刷新状态失败: {e}")
            return True
    
    def _save_metadata(self):
        """保存元数据"""
        cache_metadata_file = os.path.join(FUND_DIRECTORY_DIR, "metadata.json")
        metadata = {
            'last_refresh': datetime.now().isoformat(),
            'fund_count': len(self.directory),
            'version': '1.0'
        }
        
        try:
            with open(cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[FundDirectory] 保存元数据失败: {e}")
    
    def load_from_cache(self) -> bool:
        """从缓存加载基金目录"""
        try:
            # 检查缓存文件是否存在
            if not os.path.exists(FUND_DIRECTORY_FILE):
                logger.info("[FundDirectory] 缓存文件不存在，需要刷新")
                return False
            
            # 检查缓存是否过期
            cache_mtime = os.path.getmtime(FUND_DIRECTORY_FILE)
            cache_age = time.time() - cache_mtime
            
            if cache_age > CACHE_TTL_DAYS * 24 * 3600:
                logger.info(f"[FundDirectory] 缓存已过期 ({int(cache_age/3600)}小时)")
                return False
            
            # 加载缓存
            with open(FUND_DIRECTORY_FILE, 'r', encoding='utf-8') as f:
                self.directory = json.load(f)
            
            # 更新索引
            self._update_indexes()
            
            logger.info(f"[FundDirectory] 从缓存加载 {len(self.directory)} 只基金")
            return True
            
        except Exception as e:
            logger.warning(f"[FundDirectory] 加载缓存失败: {e}")
            return False
    
    def _update_indexes(self):
        """更新索引"""
        self.code_list = list(self.directory.keys())
        
        # 代码->名称映射
        self.name_map = {}
        for code, info in self.directory.items():
            name = info.get('name', '')
            if name:
                self.name_map[code] = name
        
        # 名称->代码映射（可能有多个同名基金）
        self.name_reverse_map = {}
        for code, name in self.name_map.items():
            if name not in self.name_reverse_map:
                self.name_reverse_map[name] = []
            self.name_reverse_map[name].append(code)
    
    @retry(max_retries=3, delay=2)
    def refresh_directory(self) -> bool:
        """刷新基金目录"""
        try:
            logger.info("[FundDirectory] 开始刷新基金目录...")
            
            # 从API获取基金列表
            df_funds = _ak_fund_list_em()
            if df_funds is None or df_funds.empty:
                logger.error("[FundDirectory] 获取基金列表失败")
                return False
            
            logger.info(f"[FundDirectory] 从API获取到 {len(df_funds)} 只基金")
            
            # 转换为目录格式
            new_directory = {}
            for _, row in df_funds.iterrows():
                code = str(row['基金代码']).strip()
                name = str(row['基金名称']).strip()
                
                if len(code) != 6:
                    continue  # 跳过无效代码
                
                new_directory[code] = {
                    'code': code,
                    'name': name,
                    'update_time': datetime.now().isoformat(),
                    'source': 'akshare_api'
                }
            
            # 更新目录
            self.directory = new_directory
            self._update_indexes()
            
            # 保存到缓存
            self._save_to_cache()
            self._save_metadata()
            
            # 保存为CSV格式
            self._save_as_csv()
            
            logger.info(f"[FundDirectory] 刷新完成，共 {len(self.directory)} 只基金")
            return True
            
        except Exception as e:
            logger.error(f"[FundDirectory] 刷新目录失败: {e}")
            return False
    
    def _save_to_cache(self):
        """保存到缓存文件"""
        try:
            # 创建备份
            if os.path.exists(FUND_DIRECTORY_FILE):
                backup_file = f"{FUND_DIRECTORY_FILE}.bak.{int(time.time())}"
                import shutil
                shutil.copy2(FUND_DIRECTORY_FILE, backup_file)
            
            # 保存新文件
            with open(FUND_DIRECTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.directory, f, ensure_ascii=False, indent=2)
            
            logger.info(f"[FundDirectory] 保存缓存到 {FUND_DIRECTORY_FILE}")
            
        except Exception as e:
            logger.error(f"[FundDirectory] 保存缓存失败: {e}")
    
    def _save_as_csv(self):
        """保存为CSV格式"""
        try:
            data = []
            for code, info in self.directory.items():
                data.append({
                    '基金代码': code,
                    '基金名称': info.get('name', ''),
                    '更新时间': info.get('update_time', ''),
                    '数据来源': info.get('source', '')
                })
            
            df = pd.DataFrame(data)
            df.to_csv(FUND_DIRECTORY_CSV, index=False, encoding='utf-8-sig')
            
            logger.info(f"[FundDirectory] 保存CSV到 {FUND_DIRECTORY_CSV}")
            
        except Exception as e:
            logger.warning(f"[FundDirectory] 保存CSV失败: {e}")
    
    def validate_code(self, code: str) -> bool:
        """验证基金代码是否存在"""
        if not code or len(code) != 6:
            return False
        
        # 检查缓存中是否存在
        return code in self.directory
    
    def get_fund_name(self, code: str) -> Optional[str]:
        """获取基金名称"""
        if code in self.directory:
            return self.directory[code].get('name')
        return None
    
    def search_by_name(self, name_keyword: str) -> List[Dict[str, str]]:
        """按名称搜索基金"""
        results = []
        name_keyword = name_keyword.lower()
        
        for code, info in self.directory.items():
            fund_name = info.get('name', '')
            if name_keyword in fund_name.lower():
                results.append({
                    'code': code,
                    'name': fund_name
                })
        
        return results
    
    def get_all_codes(self) -> List[str]:
        """获取所有基金代码"""
        return self.code_list
    
    def get_all_funds(self) -> Dict[str, Dict[str, Any]]:
        """获取所有基金信息"""
        return self.directory
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'total_funds': len(self.directory),
            'last_update': self.directory.get(next(iter(self.directory), {}), {}).get('update_time', ''),
            'directory_file': FUND_DIRECTORY_FILE,
            'cache_age_hours': 0
        }


# 全局基金目录实例
_fund_directory_instance: Optional[FundDirectory] = None


def get_fund_directory() -> FundDirectory:
    """获取全局基金目录实例（单例模式）"""
    global _fund_directory_instance
    
    if _fund_directory_instance is None:
        _fund_directory_instance = FundDirectory(auto_refresh=True)
    
    return _fund_directory_instance


def validate_fund_code_local(code: str) -> bool:
    """本地验证基金代码（毫秒级响应）"""
    try:
        directory = get_fund_directory()
        return directory.validate_code(code)
    except Exception as e:
        logger.warning(f"[validate_fund_code_local] 本地验证失败: {e}")
        # 降级：使用API验证
        return validate_fund_code_fallback(code)


def validate_fund_code_fallback(code: str) -> bool:
    """降级方案：使用原API验证"""
    try:
        from .equity_loader import validate_fund_code_fast
        return validate_fund_code_fast(code)
    except Exception as e:
        logger.error(f"[validate_fund_code_fallback] 降级验证失败: {e}")
        return False


def get_fund_name_local(code: str) -> Optional[str]:
    """本地获取基金名称"""
    try:
        directory = get_fund_directory()
        return directory.get_fund_name(code)
    except Exception as e:
        logger.warning(f"[get_fund_name_local] 本地获取名称失败: {e}")
        return None


def search_fund_by_name(keyword: str) -> List[Dict[str, str]]:
    """按名称搜索基金"""
    try:
        directory = get_fund_directory()
        return directory.search_by_name(keyword)
    except Exception as e:
        logger.warning(f"[search_fund_by_name] 搜索失败: {e}")
        return []


def refresh_fund_directory_async():
    """异步刷新基金目录（用于后台任务）"""
    try:
        directory = get_fund_directory()
        # 在实际应用中，这里可以启动后台线程刷新
        return directory.refresh_directory()
    except Exception as e:
        logger.error(f"[refresh_fund_directory_async] 异步刷新失败: {e}")
        return False


def get_directory_statistics() -> Dict[str, Any]:
    """获取目录统计信息"""
    try:
        directory = get_fund_directory()
        return directory.get_statistics()
    except Exception as e:
        logger.warning(f"[get_directory_statistics] 获取统计失败: {e}")
        return {'error': str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== 基金目录管理器测试 ===")
    
    # 初始化目录
    directory = get_fund_directory()
    
    # 显示统计信息
    stats = get_directory_statistics()
    print(f"总基金数量: {stats.get('total_funds', 0)}")
    print(f"目录文件: {stats.get('directory_file', '')}")
    
    # 测试验证功能
    test_codes = ["000001", "000069", "510300", "999999"]
    for code in test_codes:
        is_valid = validate_fund_code_local(code)
        name = get_fund_name_local(code)
        print(f"基金 {code}: {'✅ 有效' if is_valid else '❌ 无效'} - {name or '未知'}")
    
    # 测试搜索功能
    print("\n搜索测试（华夏）：")
    results = search_fund_by_name("华夏")
    for result in results[:5]:  # 显示前5个
        print(f"  {result['code']}: {result['name']}")
    
    print("\n✅ 测试完成！")