"""
全收益指数定期更新器 — fund_quant_v2
负责定时更新价格指数和全收益指数缓存

核心功能：
1. 定时更新指数数据
2. 批量更新支持的所有指数
3. 更新失败重试机制
4. 更新日志和监控
"""

from __future__ import annotations
import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd

from data_loader.index_cache_config import (
    SUPPORTED_INDEXES,
    UPDATE_SCHEDULE,
    get_price_index_filename,
    get_total_return_filename,
    get_metadata_filename,
)
from data_loader.index_cache_manager import (
    IndexCacheManager,
    get_index_cache_manager,
)

logger = logging.getLogger(__name__)


class IndexUpdateScheduler:
    """指数更新调度器"""
    
    def __init__(self):
        self.cache_manager = get_index_cache_manager()
        self.update_log_file = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "index_cache", "update_log.json"
        )
        
        # 创建更新日志文件
        os.makedirs(os.path.dirname(self.update_log_file), exist_ok=True)
        
        # 加载更新日志
        self.update_log = self._load_update_log()
        
        logger.info("[IndexUpdateScheduler] 初始化完成")
    
    def _load_update_log(self) -> Dict[str, any]:
        """加载更新日志"""
        if os.path.exists(self.update_log_file):
            try:
                with open(self.update_log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"[IndexUpdateScheduler] 加载更新日志失败: {e}")
        
        # 返回默认日志结构
        return {
            "last_full_update": None,
            "last_partial_update": None,
            "index_updates": {},
            "update_statistics": {
                "total_updates": 0,
                "successful_updates": 0,
                "failed_updates": 0,
            }
        }
    
    def _save_update_log(self):
        """保存更新日志"""
        try:
            with open(self.update_log_file, 'w', encoding='utf-8') as f:
                json.dump(self.update_log, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[IndexUpdateScheduler] 保存更新日志失败: {e}")
    
    def update_all_indices(self, force_refresh: bool = False) -> Dict[str, any]:
        """
        更新所有支持的指数
        
        Args:
            force_refresh: 是否强制刷新所有指数
            
        Returns:
            更新统计信息
        """
        logger.info(f"[IndexUpdateScheduler] 开始更新所有指数，强制刷新: {force_refresh}")
        
        stats = {
            "start_time": datetime.now().isoformat(),
            "total_indices": len(SUPPORTED_INDEXES),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "failed_codes": [],
            "successful_codes": [],
        }
        
        # 初始化更新日志
        if "index_updates" not in self.update_log:
            self.update_log["index_updates"] = {}
        
        update_time = datetime.now().isoformat()
        
        for index_code, config in SUPPORTED_INDEXES.items():
            try:
                logger.info(f"[IndexUpdateScheduler] 更新指数: {index_code} ({config['name']})")
                
                # 检查是否需要更新（如果不是强制刷新）
                if not force_refresh and not self._should_update_index(index_code, config):
                    stats["skipped"] += 1
                    logger.info(f"[IndexUpdateScheduler] 跳过指数 {index_code}，未到更新时间")
                    continue
                
                # 更新指数
                update_success = self._update_single_index(index_code, force_refresh)
                
                if update_success:
                    stats["successful"] += 1
                    stats["successful_codes"].append(index_code)
                    
                    # 记录更新日志
                    self.update_log["index_updates"][index_code] = {
                        "last_update": update_time,
                        "status": "success",
                        "update_type": "full" if force_refresh else "incremental",
                    }
                else:
                    stats["failed"] += 1
                    stats["failed_codes"].append(index_code)
                    
                    # 记录失败日志
                    self.update_log["index_updates"][index_code] = {
                        "last_update": update_time,
                        "status": "failed",
                        "update_type": "full" if force_refresh else "incremental",
                    }
                
                # 更新统计
                self.update_log["update_statistics"]["total_updates"] += 1
                if update_success:
                    self.update_log["update_statistics"]["successful_updates"] += 1
                else:
                    self.update_log["update_statistics"]["failed_updates"] += 1
                
                # 避免请求过于频繁
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[IndexUpdateScheduler] 更新指数 {index_code} 时发生异常: {e}")
                import traceback
                logger.error(traceback.format_exc())
                stats["failed"] += 1
                stats["failed_codes"].append(index_code)
        
        # 更新最后更新时间
        if stats["successful"] > 0:
            if force_refresh:
                self.update_log["last_full_update"] = update_time
            else:
                self.update_log["last_partial_update"] = update_time
        
        stats["end_time"] = datetime.now().isoformat()
        stats["duration_seconds"] = (datetime.now() - datetime.fromisoformat(stats["start_time"])).total_seconds()
        
        # 保存更新日志
        self._save_update_log()
        
        logger.info(f"[IndexUpdateScheduler] 更新完成。成功: {stats['successful']}, 失败: {stats['failed']}, 跳过: {stats['skipped']}")
        
        return stats
    
    def _should_update_index(self, index_code: str, config: Dict[str, any]) -> bool:
        """
        检查是否需要更新指数
        
        判断逻辑：
        1. 检查最后更新时间
        2. 根据更新频率决定是否更新
        3. 检查数据是否过期
        """
        if index_code not in self.update_log["index_updates"]:
            return True  # 从未更新过
        
        last_update = self.update_log["index_updates"][index_code].get("last_update")
        if not last_update:
            return True  # 没有最后更新时间
        
        try:
            last_update_dt = datetime.fromisoformat(last_update)
            now = datetime.now()
            hours_since_update = (now - last_update_dt).total_seconds() / 3600
            
            # 根据更新频率判断
            update_freq = config.get('update_frequency', 'daily')
            update_intervals = {
                'daily': 24,
                'weekly': 168,
                'monthly': 720,
            }
            
            interval_hours = update_intervals.get(update_freq, 24)
            
            if hours_since_update >= interval_hours:
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"[IndexUpdateScheduler] 检查更新状态失败 {index_code}: {e}")
            return True  # 出现异常时重新更新
    
    def _update_single_index(self, index_code: str, force_refresh: bool = False) -> bool:
        """
        更新单个指数
        
        更新步骤：
        1. 更新价格指数
        2. 更新全收益指数
        3. 验证数据质量
        """
        try:
            # 1. 更新价格指数
            logger.info(f"[IndexUpdateScheduler] 更新价格指数: {index_code}")
            price_df = self.cache_manager.get_price_index(index_code, force_refresh=force_refresh)
            
            if price_df.empty:
                logger.error(f"[IndexUpdateScheduler] 价格指数更新失败: {index_code}")
                return False
            
            # 2. 更新全收益指数
            logger.info(f"[IndexUpdateScheduler] 更新全收益指数: {index_code}")
            tr_df = self.cache_manager.get_total_return_index(index_code, force_refresh=force_refresh)
            
            if tr_df.empty:
                logger.error(f"[IndexUpdateScheduler] 全收益指数更新失败: {index_code}")
                return False
            
            # 3. 验证数据质量
            is_valid = self._validate_index_data(price_df, tr_df)
            
            if not is_valid:
                logger.error(f"[IndexUpdateScheduler] 数据验证失败: {index_code}")
                return False
            
            logger.info(f"[IndexUpdateScheduler] 指数更新成功: {index_code}, 价格数据 {len(price_df)} 条, 全收益数据 {len(tr_df)} 条")
            return True
            
        except Exception as e:
            logger.error(f"[IndexUpdateScheduler] 更新指数 {index_code} 失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _validate_index_data(self, price_df: pd.DataFrame, tr_df: pd.DataFrame) -> bool:
        """
        验证指数数据质量
        
        验证规则：
        1. 数据不为空
        2. 日期范围一致
        3. 收益率计算正确
        """
        # 1. 检查数据是否为空
        if price_df.empty or tr_df.empty:
            logger.error("[IndexUpdateScheduler] 数据为空")
            return False
        
        # 2. 检查日期范围
        if 'date' not in price_df.columns or 'date' not in tr_df.columns:
            logger.error("[IndexUpdateScheduler] 缺少日期列")
            return False
        
        price_dates = set(pd.to_datetime(price_df['date']).dt.date)
        tr_dates = set(pd.to_datetime(tr_df['date']).dt.date)
        
        if not price_dates or not tr_dates:
            logger.error("[IndexUpdateScheduler] 日期集合为空")
            return False
        
        # 3. 检查数据点数量
        min_data_points = 30  # 最少需要30个数据点
        if len(price_df) < min_data_points or len(tr_df) < min_data_points:
            logger.warning(f"[IndexUpdateScheduler] 数据点数量不足: price={len(price_df)}, tr={len(tr_df)}")
            return False
        
        # 4. 检查收益率计算
        if 'total_ret' in tr_df.columns and 'total_nav' in tr_df.columns:
            # 检查全收益净值是否从1.0开始
            first_nav = tr_df['total_nav'].iloc[0]
            if abs(first_nav - 1.0) > 0.01:  # 允许1%的误差
                logger.warning(f"[IndexUpdateScheduler] 全收益净值起点异常: {first_nav}")
                return False
            
            # 检查收益率序列是否合理
            max_ret = tr_df['total_ret'].abs().max()
            if max_ret > 0.2:  # 单日收益率不应超过20%
                logger.warning(f"[IndexUpdateScheduler] 异常收益率: {max_ret:.1%}")
                return False
        
        return True
    
    def get_update_status(self) -> Dict[str, any]:
        """获取更新状态"""
        now = datetime.now()
        
        # 计算下次更新时间
        next_update_daily = None
        next_update_weekly = None
        
        if self.update_log.get("last_full_update"):
            last_full = datetime.fromisoformat(self.update_log["last_full_update"])
            next_update_daily = last_full + timedelta(days=1)
        
        status = {
            "last_full_update": self.update_log.get("last_full_update"),
            "last_partial_update": self.update_log.get("last_partial_update"),
            "next_daily_update": next_update_daily.isoformat() if next_update_daily else None,
            "next_weekly_update": next_update_weekly.isoformat() if next_update_weekly else None,
            "total_indices": len(SUPPORTED_INDEXES),
            "updated_indices": len(self.update_log.get("index_updates", {})),
            "update_statistics": self.update_log.get("update_statistics", {}),
            "recent_updates": self._get_recent_updates(10),  # 最近10次更新
        }
        
        return status
    
    def _get_recent_updates(self, limit: int = 10) -> List[Dict[str, any]]:
        """获取最近的更新记录"""
        if "index_updates" not in self.update_log:
            return []
        
        updates = []
        for index_code, info in self.update_log["index_updates"].items():
            if "last_update" in info:
                updates.append({
                    "index_code": index_code,
                    "index_name": SUPPORTED_INDEXES.get(index_code, {}).get("name", index_code),
                    "last_update": info["last_update"],
                    "status": info.get("status", "unknown"),
                })
        
        # 按时间排序，取最近的
        updates.sort(key=lambda x: x["last_update"], reverse=True)
        return updates[:limit]
    
    def cleanup_old_data(self, days_to_keep: int = 90) -> Dict[str, int]:
        """
        清理旧数据
        
        Args:
            days_to_keep: 保留多少天内的数据
            
        Returns:
            清理统计信息
        """
        logger.info(f"[IndexUpdateScheduler] 开始清理旧数据，保留{days_to_keep}天内的数据")
        
        stats = {
            "files_removed": 0,
            "errors": 0,
        }
        
        # 使用缓存管理器的清理功能
        try:
            files_removed = self.cache_manager.cleanup_old_cache(days_to_keep)
            stats["files_removed"] = files_removed
        except Exception as e:
            logger.error(f"[IndexUpdateScheduler] 清理旧数据失败: {e}")
            stats["errors"] += 1
        
        return stats


class IndexUpdateAutomation:
    """指数更新自动化脚本"""
    
    def __init__(self):
        self.scheduler = IndexUpdateScheduler()
    
    def run_daily_update(self) -> Dict[str, any]:
        """执行每日更新"""
        logger.info("[IndexUpdateAutomation] 开始执行每日更新")
        
        # 1. 执行增量更新（不强制刷新）
        update_stats = self.scheduler.update_all_indices(force_refresh=False)
        
        # 2. 清理旧数据（每周执行一次）
        today = datetime.now().weekday()  # 0=Monday, 6=Sunday
        if today == 6:  # 每周日清理
            logger.info("[IndexUpdateAutomation] 执行每周数据清理")
            cleanup_stats = self.scheduler.cleanup_old_data(days_to_keep=90)
            update_stats["cleanup_stats"] = cleanup_stats
        
        # 3. 获取更新状态
        update_stats["update_status"] = self.scheduler.get_update_status()
        
        logger.info("[IndexUpdateAutomation] 每日更新完成")
        return update_stats
    
    def run_weekly_full_update(self) -> Dict[str, any]:
        """执行每周完整更新"""
        logger.info("[IndexUpdateAutomation] 开始执行每周完整更新")
        
        # 1. 执行强制刷新更新
        update_stats = self.scheduler.update_all_indices(force_refresh=True)
        
        # 2. 清理旧数据
        cleanup_stats = self.scheduler.cleanup_old_data(days_to_keep=90)
        update_stats["cleanup_stats"] = cleanup_stats
        
        # 3. 获取更新状态
        update_stats["update_status"] = self.scheduler.get_update_status()
        
        logger.info("[IndexUpdateAutomation] 每周完整更新完成")
        return update_stats
    
    def check_and_update_if_needed(self) -> bool:
        """
        检查并执行更新（如果需要）
        
        返回:
            True - 执行了更新
            False - 不需要更新
        """
        # 获取当前状态
        status = self.scheduler.get_update_status()
        
        # 检查是否需要更新
        needs_update = False
        
        if status["last_full_update"]:
            last_full = datetime.fromisoformat(status["last_full_update"])
            days_since_full = (datetime.now() - last_full).days
            
            if days_since_full >= 7:  # 距离上次完整更新超过7天
                needs_update = True
                logger.info(f"[IndexUpdateAutomation] 距离上次完整更新已{days_since_full}天，需要更新")
        
        elif status["last_partial_update"]:
            last_partial = datetime.fromisoformat(status["last_partial_update"])
            hours_since_partial = (datetime.now() - last_partial).total_seconds() / 3600
            
            if hours_since_partial >= 24:  # 距离上次更新超过24小时
                needs_update = True
                logger.info(f"[IndexUpdateAutomation] 距离上次更新已{hours_since_partial:.1f}小时，需要更新")
        
        else:
            # 从未更新过，需要更新
            needs_update = True
            logger.info("[IndexUpdateAutomation] 从未更新过，需要首次更新")
        
        if needs_update:
            # 执行更新
            self.run_daily_update()
            return True
        else:
            logger.info("[IndexUpdateAutomation] 当前不需要更新")
            return False


# 全局实例
_automation_instance = None

def get_index_update_automation() -> IndexUpdateAutomation:
    """获取全局更新自动化实例"""
    global _automation_instance
    if _automation_instance is None:
        _automation_instance = IndexUpdateAutomation()
    return _automation_instance


def run_standalone_update():
    """独立运行更新脚本（用于命令行或定时任务）"""
    import sys
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("/tmp/index_update.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger.info("=" * 80)
    logger.info("开始执行独立全收益指数更新脚本")
    logger.info(f"时间: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    try:
        # 检查命令行参数
        force_full = "--force-full" in sys.argv
        cleanup_only = "--cleanup-only" in sys.argv
        
        automation = get_index_update_automation()
        
        if cleanup_only:
            logger.info("执行仅清理模式")
            stats = automation.scheduler.cleanup_old_data(days_to_keep=90)
            logger.info(f"清理完成: {stats}")
        elif force_full:
            logger.info("执行强制完整更新模式")
            stats = automation.run_weekly_full_update()
            logger.info(f"完整更新完成: {stats}")
        else:
            logger.info("执行日常更新模式")
            stats = automation.run_daily_update()
            logger.info(f"日常更新完成: {stats}")
        
        logger.info("=" * 80)
        logger.info("更新脚本执行完成")
        logger.info("=" * 80)
        
        # 返回退出码
        if stats.get("failed", 0) > 0:
            sys.exit(1)  # 有失败项，返回非零退出码
        else:
            sys.exit(0)  # 成功完成
            
    except Exception as e:
        logger.error(f"更新脚本执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(2)  # 执行失败


if __name__ == "__main__":
    run_standalone_update()