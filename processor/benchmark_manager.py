"""
基准管理器 — fund_quant_v2/processor/benchmark_manager.py

负责：解析业绩基准文本、合成基准收益率序列、常数序列生成
"""

import logging
import re
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

import pandas as pd
import numpy as np

from config import (
    BENCHMARK_COMPONENTS,
    DEFAULT_BENCHMARK_WEIGHTS,
    TYPE_KEYWORD_TO_DEFAULT,
    DEFAULT_CASH_RATE,
    TRADING_DAYS_PER_YEAR,
)
from data_loader.equity_loader import load_index_daily, load_hk_index_daily, load_bond_index

logger = logging.getLogger(__name__)


class BenchmarkManager:
    """业绩基准合成器
    
    功能：
    1. parse_contract(): 从合同文本解析基准组件和权重
    2. synthesize(): 日期对齐 + 加权合成 + 常数序列生成
    3. get_benchmark_series(): 一站式获取基准序列（含三级优先级逻辑）
    """
    
    def __init__(self, symbol: str, benchmark_text: str, fund_type: str):
        """
        Args:
            symbol: 基金代码
            benchmark_text: 业绩比较基准文本（从基金合同获取）
            fund_type: 基金类型（如"股票型"、"偏股混合型"）
        """
        self.symbol = symbol
        self.benchmark_text = benchmark_text
        self.fund_type = fund_type
        self.components: List[Dict[str, Any]] = []
        self.description = ""
    
    def parse_contract(self, text: str) -> bool:
        """
        解析业绩基准文本 → components列表
        
        Returns:
            bool: 解析是否成功
        """
        if not text:
            logger.debug(f"[BenchmarkManager] {self.symbol} 基准文本为空，使用保底规则")
            return False
        
        found = []
        for name, code in BENCHMARK_COMPONENTS.items():
            if name in text:
                found.append((text.index(name), name, code))
        found.sort(key=lambda x: x[0])
        
        if not found:
            logger.debug(f"[BenchmarkManager] {self.symbol} 未能识别任何基准组件")
            return False
        
        # 提取权重（正则匹配 XX% 或 XX.%）
        weights_raw = re.findall(r"(\d+\.?\d*)\s*%", text)
        weights = [float(w) / 100.0 for w in weights_raw if 0 < float(w) / 100.0 <= 1]
        
        # 检查是否包含"活期存款利率"、"现金利率"等常数收益率
        has_cash_rate = any(kw in text for kw in ["活期存款", "现金利率", "银行存款", "定期"])
        
        # 权重分配策略
        components = []
        if not weights:
            # 无权重 → 均等分配
            components = [{"name": n, "code": c, "weight": 1.0 / len(found), "is_cash": False}
                         for _, n, c in found]
        elif len(found) == len(weights):
            # 权重数量 = 基准数量 → 一一对应
            components = [{"name": n, "code": c, "weight": w, "is_cash": False}
                         for (_, n, c), w in zip(found, weights)]
        elif len(found) == 1 and weights:
            # 单一基准 + 多权重 → 取第一个权重
            _, n, c = found[0]
            components = [{"name": n, "code": c, "weight": weights[0], "is_cash": False}]
        elif has_cash_rate and len(weights) > 0:
            # 包含现金利率 → 将权重分配给基准，现金部分单独处理
            cash_weight = weights[-1] if weights else 0.05
            for i, (_, n, c) in enumerate(found):
                w = weights[i] if i < len(weights) else 0.0
                components.append({"name": n, "code": c, "weight": w, "is_cash": False})
            components.append({"name": "活期存款利率", "code": "CONST_RATE", "weight": cash_weight, "is_cash": True})
        else:
            # 权重数量 ≠ 基准数量 → 按顺序分配，超出的默认1.0
            components = [{"name": n, "code": c, "weight": weights[i] if i < len(weights) else 1.0, "is_cash": False}
                         for i, (_, n, c) in enumerate(found)]
        
        # 归一化（确保总权重 = 1.0）
        total_weight = sum(c["weight"] for c in components)
        if total_weight > 0 and abs(total_weight - 1.0) > 0.05:
            for comp in components:
                comp["weight"] = round(comp["weight"] / total_weight, 4)
            total_weight = sum(c["weight"] for c in components)
        
        # 权重校验
        if abs(total_weight - 1.0) > 0.01:
            logger.warning(f"[BenchmarkManager] {self.symbol} 解析权重总和为 {total_weight:.4f}，可能有问题")
            return False
        
        self.components = components
        self.description = " + ".join([f"{c['name']}×{int(c['weight']*100)}%" for c in components])
        logger.info(f"[BenchmarkManager] {self.symbol} 解析成功: {self.description}")
        return True
    
    def _get_default_weights(self) -> List[Dict[str, Any]]:
        """
        二级优先级：根据基金类型匹配预设权重表
        
        Returns:
            components列表
        """
        # 模糊匹配基金类型关键词
        matched_type = None
        for keyword, std_type in TYPE_KEYWORD_MAPPING.items():
            if keyword in self.fund_type:
                matched_type = std_type
                break
        
        if not matched_type:
            logger.debug(f"[BenchmarkManager] {self.symbol} 类型'{self.fund_type}'无匹配，使用默认")
            matched_type = "default"
        
        if matched_type not in DEFAULT_BENCHMARK_WEIGHTS:
            logger.warning(f"[BenchmarkManager] {self.symbol} 类型'{matched_type}'不在预设表中，使用默认")
            matched_type = "default"
        
        config = DEFAULT_BENCHMARK_WEIGHTS[matched_type]
        components = []
        
        # 权益部分
        if config.get("equity_code") and config.get("equity_weight", 0) > 0:
            name = next((k for k, v in BENCHMARK_COMPONENTS.items() if v == config["equity_code"]), config["equity_code"])
            components.append({
                "name": name,
                "code": config["equity_code"],
                "weight": config["equity_weight"],
                "is_cash": False
            })
        
        # 债券部分
        if config.get("bond_code") and config.get("bond_weight", 0) > 0:
            name = next((k for k, v in BENCHMARK_COMPONENTS.items() if v == config["bond_code"]), config["bond_code"])
            components.append({
                "name": name,
                "code": config["bond_code"],
                "weight": config["bond_weight"],
                "is_cash": False
            })
        
        self.description = f"保底权重（{matched_type}）: " + " + ".join([f"{c['name']}×{int(c['weight']*100)}%" for c in components])
        logger.info(f"[BenchmarkManager] {self.symbol} 使用保底权重: {self.description}")
        return components
    
    def _generate_constant_rate_series(self, start: str, end: str, rate: float) -> pd.DataFrame:
        """
        常数序列生成：将年化利率转换为日收益率序列
        
        Args:
            start: 开始日期
            end: 结束日期
            rate: 年化利率（如 0.015 表示 1.5%）
        
        Returns:
            DataFrame with columns: date, ret
        """
        # 复利法：日收益率 = (1 + 年化利率)^(1/252) - 1
        daily_rate = (1 + rate) ** (1 / TRADING_DAYS_PER_YEAR) - 1
        
        # 生成日期序列（仅包含交易日）
        date_range = pd.date_range(start=start, end=end, freq="B")  # B = Business days
        df = pd.DataFrame({"date": date_range, "ret": daily_rate})
        
        logger.debug(f"[BenchmarkManager] 常数序列生成: 年化 {rate:.2%} → 日频 {daily_rate:.6%} ({len(df)} 天)")
        return df
    
    def synthesize(self, fund_nav_df: pd.DataFrame) -> pd.DataFrame:
        """
        合成基准收益率序列
        
        Args:
            fund_nav_df: 基金净值数据（必须有 date 和 ret 列）
        
        Returns:
            DataFrame with columns: date, bm_ret (benchmark return)
        """
        if not self.components:
            logger.warning(f"[BenchmarkManager] {self.symbol} 无基准组件，使用默认沪深300")
            # 全程序统一使用沪深300价格指数（sh000300）
            df_default = load_index_daily("sh000300", fund_nav_df["date"].min(), fund_nav_df["date"].max())
            df_default["bm_ret"] = df_default["ret"].fillna(0)
            return df_default[["date", "bm_ret"]].reset_index(drop=True)
        
        # 以基金净值日期为 Master Clock
        master_dates = fund_nav_df["date"].sort_values().unique()
        master_df = pd.DataFrame({"date": master_dates})
        
        parts = []
        for comp in self.components:
            w = comp["weight"]
            code = comp["code"]
            name = comp["name"]
            is_cash = comp.get("is_cash", False)
            
            if is_cash:
                # 常数序列生成
                df_part = self._generate_constant_rate_series(
                    master_dates.min(),
                    master_dates.max(),
                    DEFAULT_CASH_RATE if is_cash else 0.0
                )
                df_part["weighted"] = df_part["ret"] * w
            elif code == "bond_composite":
                # 中债综合指数
                df_part = load_bond_index(master_dates.min(), master_dates.max())
                df_part = df_part.rename(columns={"ret": "part_ret"})
                df_part["weighted"] = df_part["part_ret"] * w
            elif code.startswith("hk:"):
                # 港股指数
                hk_sym = code[3:]
                df_part = load_hk_index_daily(hk_sym, master_dates.min(), master_dates.max())
                df_part = df_part.rename(columns={"ret": "part_ret"})
                df_part["weighted"] = df_part["part_ret"] * w
            else:
                # A股指数
                df_part = load_index_daily(code, master_dates.min(), master_dates.max())
                df_part = df_part.rename(columns={"ret": "part_ret"})
                df_part["weighted"] = df_part["part_ret"] * w
            
            # 日期对齐：reindex 到 Master Clock
            df_part_aligned = df_part.set_index("date").reindex(master_dates)
            
            # 缺失值处理：前向填充（最多3天）
            df_part_aligned["weighted"] = df_part_aligned["weighted"].ffill(limit=3)
            
            parts.append(df_part_aligned["weighted"].rename(f"{name}_{int(w*100)}%"))
        
        # 加权求和
        if not parts:
            logger.warning(f"[BenchmarkManager] {self.symbol} 所有基准组件获取失败")
            # 全程序统一使用沪深300价格指数（sh000300）
            df_default = load_index_daily("sh000300", master_dates.min(), master_dates.max())
            df_default["bm_ret"] = df_default["ret"].fillna(0)
            return df_default[["date", "bm_ret"]].reset_index(drop=True)
        
        df_benchmark = pd.concat(parts, axis=1).fillna(0)
        df_benchmark["bm_ret"] = df_benchmark.sum(axis=1)
        df_benchmark["date"] = master_dates
        
        logger.info(f"[BenchmarkManager] {self.symbol} 合成完成: {len(df_benchmark)} 天")
        return df_benchmark[["date", "bm_ret"]].reset_index(drop=True)
    
    def get_benchmark_series(self, fund_nav_df: pd.DataFrame) -> pd.DataFrame:
        """
        一站式获取基准序列（三级优先级逻辑）
        
        Args:
            fund_nav_df: 基金净值数据（必须有 date 和 ret 列）
        
        Returns:
            DataFrame with columns: date, bm_ret
        """
        # 一级优先级：解析合同文本
        if self.parse_contract(self.benchmark_text):
            return self.synthesize(fund_nav_df)
        
        # 二级优先级：根据基金类型匹配预设权重
        self.components = self._get_default_weights()
        return self.synthesize(fund_nav_df)


def get_benchmark_series(
    symbol: str,
    benchmark_text: str,
    fund_type: str,
    fund_nav_df: pd.DataFrame
) -> pd.DataFrame:
    """
    便捷函数：获取基准收益率序列
    
    Args:
        symbol: 基金代码
        benchmark_text: 业绩比较基准文本
        fund_type: 基金类型
        fund_nav_df: 基金净值数据（必须有 date 和 ret 列）
    
    Returns:
        DataFrame with columns: date, bm_ret
    """
    manager = BenchmarkManager(symbol, benchmark_text, fund_type)
    return manager.get_benchmark_series(fund_nav_df)
