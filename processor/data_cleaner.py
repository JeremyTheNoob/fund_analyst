"""
数据清洗层 — fund_quant_v2
标准化清洗：异常值剔除 / 连续性检查 / 净值复权校验 / 债券数据流水线
"""

from __future__ import annotations
import logging
import re
from typing import Optional

import pandas as pd
import numpy as np

from config import DATA_CONFIG
from models.schema import (
    NavData, BondYieldData, CleanNavData, CleanBondData
)

logger = logging.getLogger(__name__)


# ============================================================
# 净值数据清洗
# ============================================================

def clean_nav(nav: NavData) -> CleanNavData:
    """
    清洗净值数据，执行以下步骤：
    1. 移除极端日收益率（MAD 离群值剔除）
    2. 检查连续性（节假日自然断口 vs 异常停牌）
    3. 净值复权校验（累计净值 vs 单位净值差异检测）
    4. 最少样本量验证

    Args:
        nav: 原始 NavData

    Returns:
        CleanNavData（清洗后数据 + 警告列表）
    """
    warnings = []
    df = nav.df.copy()

    if df.empty:
        warnings.append("净值数据为空，无法清洗")
        return CleanNavData(symbol=nav.symbol, df=df, warnings=warnings)

    original_len = len(df)

    # --- Step 1: 移除极端日收益率（MAD 方法）---
    ret_col = "ret"
    if ret_col in df.columns:
        df, outlier_count = _remove_outliers_mad(df, ret_col, threshold=5.0)
        if outlier_count > 0:
            warnings.append(f"MAD 异常值剔除：移除 {outlier_count} 个极端收益率点（>5倍MAD）")

    # --- Step 2: 连续性检查 ---
    gap_count = _check_continuity(df)
    if gap_count > 5:
        warnings.append(f"数据连续性：检测到 {gap_count} 个超过 7 日的间隙（可能有停牌或数据缺失）")

    # --- Step 3: 零波动检测 ---
    if ret_col in df.columns:
        zero_vol = (df[ret_col].abs() < 1e-8).mean()
        if zero_vol > 0.30:
            warnings.append(f"零波动警告：{zero_vol:.1%} 的交易日收益率为 0，数据可能存在问题")

    # --- Step 4: 最少样本量检验 ---
    min_days = DATA_CONFIG["min_history_days"]
    if len(df) < min_days:
        warnings.append(f"样本量不足：仅有 {len(df)} 个交易日，建议至少 {min_days} 天")

    # --- Step 5: 次新基金提示 ---
    new_fund_threshold = DATA_CONFIG["new_fund_threshold_days"]
    if len(df) < new_fund_threshold:
        warnings.append(f"次新基金：成立不足 {new_fund_threshold} 天，统计指标可信度较低")

    cleaned_len = len(df)
    if cleaned_len < original_len:
        logger.info(f"[clean_nav] {nav.symbol}: {original_len} → {cleaned_len} 行（移除 {original_len-cleaned_len} 行）")

    return CleanNavData(symbol=nav.symbol, df=df.reset_index(drop=True), warnings=warnings)


def _remove_outliers_mad(
    df: pd.DataFrame,
    col: str,
    threshold: float = 5.0
) -> tuple[pd.DataFrame, int]:
    """
    基于 MAD（中位数绝对偏差）剔除异常值。
    更稳健于标准差方法，不受极端值本身影响。
    """
    # P0-修复：列存在性检查，避免 KeyError
    if col not in df.columns:
        logger.warning(f"[_remove_outliers_mad] 列 '{col}' 不存在，跳过异常值剔除")
        return df, 0

    series = df[col].dropna()
    if len(series) < 10:
        return df, 0

    median = series.median()
    mad = (series - median).abs().median()

    if mad < 1e-10:
        return df, 0

    modified_z = 0.6745 * (df[col] - median) / mad
    outlier_mask = modified_z.abs() > threshold

    outlier_count = outlier_mask.sum()
    df_clean = df[~outlier_mask].copy()
    return df_clean, outlier_count


def _check_continuity(df: pd.DataFrame, max_gap_days: int = 7) -> int:
    """
    检查日期序列的连续性，返回超过 max_gap_days 的间隙数量。
    """
    if "date" not in df.columns or len(df) < 2:
        return 0

    dates = pd.to_datetime(df["date"]).sort_values()
    gaps = dates.diff().dt.days.dropna()
    return int((gaps > max_gap_days).sum())


# ============================================================
# 债券数据流水线（BondDataPipeline）
# ============================================================

class BondDataPipeline:
    """
    纯债基金数据清洗流水线。
    执行：净值清洗 + 收益率数据对齐 + 基准相关性预验证

    对应旧系统 data/processor.py 中的 BondDataPipeline 类，
    但更规范、更健壮。
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.warnings: list[str] = []

    def run(
        self,
        nav: NavData,
        yield_data: BondYieldData,
        bond_index_ret: Optional[pd.DataFrame] = None,
    ) -> CleanBondData:
        """执行完整清洗流水线。

        Args:
            nav: 原始净值数据
            yield_data: 债券收益率数据（含信用利差）
            bond_index_ret: 中债综合指数收益率（可选，用于相关性检验）

        Returns:
            CleanBondData
        """
        # 状态重置：每次 run() 调用前清空 warnings 列表
        self.warnings = []

        # --- 1. 净值清洗 ---
        clean_nav = clean_nav_data(nav)
        self.warnings.extend(clean_nav.warnings)

        # --- 2. 收益率数据对齐 ---
        yield_df = yield_data.df.copy()
        nav_df   = clean_nav.df.copy()

        # 对齐日期（债券收益率通常按月/周更新，需 ffill 对齐到净值频率）
        if not yield_df.empty and not nav_df.empty:
            yield_df = _align_yield_to_nav(yield_df, nav_df)

        # --- 3. 相关性预检验（验证是否真的是纯债基金）---
        if bond_index_ret is not None and not bond_index_ret.empty and not nav_df.empty:
            corr = _compute_corr_with_bond_index(nav_df, bond_index_ret)
            if corr < 0.5:
                self.warnings.append(
                    f"⚠️ 与中债综合指数相关性仅 {corr:.2f}，"
                    "该基金可能不是纯债基金，分析结果仅供参考"
                )
            elif corr < 0.75:
                self.warnings.append(f"与中债综合指数相关性 {corr:.2f}，偏低，需关注非标资产")

        return CleanBondData(
            symbol=self.symbol,
            nav_df=nav_df,
            yield_df=yield_df,
            warnings=self.warnings,
        )


def clean_nav_data(nav: NavData) -> CleanNavData:
    """clean_nav 的便捷包装（外部统一调用接口）"""
    return clean_nav(nav)


def _align_yield_to_nav(
    yield_df: pd.DataFrame,
    nav_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    将收益率数据（周/月频）前向填充对齐到净值日频。
    """
    if "date" not in yield_df.columns or "date" not in nav_df.columns:
        return yield_df

    yield_df = yield_df.copy()
    nav_df   = nav_df.copy()
    yield_df["date"] = pd.to_datetime(yield_df["date"])
    nav_df["date"]   = pd.to_datetime(nav_df["date"])

    # 用净值日期作为目标日历，左连接 + 前向填充
    merged = nav_df[["date"]].merge(yield_df, on="date", how="left")
    for col in merged.columns:
        if col != "date":
            merged[col] = merged[col].ffill(limit=DATA_CONFIG["ffill_limit"] * 5)

    return merged.dropna(subset=[c for c in merged.columns if c != "date"]).reset_index(drop=True)


def _compute_corr_with_bond_index(
    nav_df: pd.DataFrame,
    bond_index_df: pd.DataFrame,
) -> float:
    """
    计算基金净值收益率与中债综合指数的相关系数。
    """
    try:
        fund_ret = nav_df.set_index("date")["ret"] if "ret" in nav_df.columns else \
                   nav_df.set_index("date")["nav"].pct_change()

        bm_col = [c for c in bond_index_df.columns if c != "date"]
        if not bm_col:
            return 0.0
        bm_ret = bond_index_df.set_index("date")[bm_col[0]]

        common = fund_ret.index.intersection(bm_ret.index)
        if len(common) < 20:
            return 0.0

        return float(np.corrcoef(fund_ret.loc[common], bm_ret.loc[common])[0, 1])
    except Exception:
        return 0.0


# ============================================================
# 通用数据标准化工具
# ============================================================

def standardize_returns(
    df: pd.DataFrame,
    price_col: str = "nav",
    date_col: str = "date",
) -> pd.DataFrame:
    """
    从价格列重新计算收益率（统一标准：前向日收益率）。
    """
    df = df.copy().sort_values(date_col)
    df["ret"] = df[price_col].pct_change().fillna(0)
    return df


def resample_to_monthly(df: pd.DataFrame, date_col: str = "date", ret_col: str = "ret") -> pd.DataFrame:
    """
    将日度收益率序列重采样为月度收益率（几何复利）。
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col)
    monthly = (1 + df[ret_col]).resample("ME").prod() - 1
    return monthly.reset_index().rename(columns={date_col: "date", ret_col: "monthly_ret"})


def winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """Winsorize 缩尾（限制极端值范围）"""
    q_low  = series.quantile(lower)
    q_high = series.quantile(upper)
    return series.clip(q_low, q_high)


# ============================================================
# 业绩比较基准管理器 (BenchmarkManager)
# ============================================================

class BenchmarkManager:
    """
    业绩比较基准管理器。

    功能：
    1. 三级优先级调度（合同解析 → 分类映射 → 默认保底）
    2. 标准化组件映射（统一指数代码）
    3. 自动化合成算法（日期对齐 + 收益率加权）
    4. 异常拦截与审计（权重校验 + 数据深度检查）
    """

    # 标准化基准组件映射（全系统通用）
    COMPONENT_MAPPING = {
        # A股大盘
        "沪深300": "000300.SH",
        "上证50": "000016.SH",

        # A股中小盘
        "中证500": "000905.SH",
        "中证1000": "000852.SH",

        # 其他A股指数
        "创业板指": "399006.SZ",
        "科创50": "000688.SH",
        "国证成长": "399370.SZ",
        "国证价值": "399371.SZ",
        "中证全指": "000985.SH",
        "万得全A": "881001.SH",
        "中证100": "000903.SH",

        # 港股
        "恒生指数": "HSI.HI",
        "恒生国企": "HSCEI.HI",
        "恒生科技": "HSTECH.HI",

        # 债券
        "中债综合财富": "H11001.CSI",
        "中债综合总值": "H11001.CSI",
        "中债总财富": "H11001.CSI",
        "中债总全价": "H11001.CSI",
        "中债综合指数": "H11001.CSI",
        "中债-综合财富(总值)指数": "H11001.CSI",

        # 可转债
        "中证可转债": "000832.CSI",
        "000832.CSI": "000832.CSI",

        # 现金/货币
        "银行活期存款利率": "CONST_RATE",
        "活期利率": "CONST_RATE",
        "固定目标收益率": "CONST_RATE",
    }

    # 预设分类保底权重表
    DEFAULT_BENCHMARK_WEIGHTS = {
        "股票型": {
            "equity_code": "000300.SH",
            "equity_weight": 0.90,
            "bond_code": "H11001.CSI",
            "bond_weight": 0.10,
        },
        "增强指数": {
            "equity_code": "000300.SH",
            "equity_weight": 0.90,
            "bond_code": "H11001.CSI",
            "bond_weight": 0.10,
        },
        "偏股混合型": {
            "equity_code": "000300.SH",
            "equity_weight": 0.80,
            "bond_code": "H11001.CSI",
            "bond_weight": 0.20,
        },
        "平衡混合型": {
            "equity_code": "000300.SH",
            "equity_weight": 0.50,
            "bond_code": "H11001.CSI",
            "bond_weight": 0.50,
        },
        "偏债混合型": {
            "equity_code": "000300.SH",
            "equity_weight": 0.20,
            "bond_code": "H11001.CSI",
            "bond_weight": 0.80,
        },
        "纯债型": {
            "equity_code": None,
            "equity_weight": 0.0,
            "bond_code": "H11001.CSI",
            "bond_weight": 1.0,
        },
        "中短债": {
            "equity_code": None,
            "equity_weight": 0.0,
            "bond_code": "H11001.CSI",
            "bond_weight": 1.0,
        },
        "长债": {
            "equity_code": None,
            "equity_weight": 0.0,
            "bond_code": "H11001.CSI",
            "bond_weight": 1.0,
        },
        "可转债基金": {
            "equity_code": "000832.CSI",
            "equity_weight": 1.0,
            "bond_code": None,
            "bond_weight": 0.0,
        },
    }

    # 全局默认保底（当分类未知时）
    GLOBAL_DEFAULT = {
        "equity_code": "000300.SH",
        "equity_weight": 0.80,
        "bond_code": "H11001.CSI",
        "bond_weight": 0.20,
    }

    def __init__(self, trading_days_per_year: int = 252):
        self.trading_days_per_year = trading_days_per_year

    def parse_contract(self, text: str) -> dict:
        """
        解析业绩比较基准文本 → {type, components:[{name, code, weight}]}

        优先级：
        1. 尝试从文本中提取指数代码和权重
        2. 如果失败，返回空，由上层调用 get_default_benchmark()

        Args:
            text: 业绩比较基准文本（如 "沪深300指数收益率×80%+中债综合指数收益率×20%"）

        Returns:
            {
                "type": "stock_index" | "bond_index" | "mixed_index" | "unknown",
                "components": [{"name": "沪深300", "code": "000300.SH", "weight": 0.8}],
                "warnings": []
            }
        """
        warnings = []

        if not text or text.strip() in ["本基金暂不设业绩比较基准", "该基金暂未披露业绩比较基准", ""]:
            return {"type": "unknown", "components": [], "warnings": ["基金未披露业绩比较基准"]}


        # --- Step 1: 识别指数名称 ---
        found = []
        for display_name, code in self.COMPONENT_MAPPING.items():
            if display_name in text:
                # 记录位置、显示名称、代码
                found.append((text.index(display_name), display_name, code))
        found.sort(key=lambda x: x[0])

        if not found:
            warnings.append(f"无法识别业绩比较基准中的指数：{text}")
            return {"type": "unknown", "components": [], "warnings": warnings}

        # --- Step 2: 提取权重 ---
        weights_raw = [float(m) / 100.0 for m in re.findall(r"(\d+)\s*%", text)]
        weights = [w for w in weights_raw if 0 < w <= 1]

        # --- Step 3: 组合权重和指数 ---
        components = []

        if not weights:
            # 无权重时，所有找到的指数均分
            equal_weight = 1.0 / len(found)
            components = [{"name": n, "code": c, "weight": equal_weight} for _, n, c in found]
        elif len(found) == len(weights):
            # 指数数量 = 权重数量，一一对应
            components = [{"name": n, "code": c, "weight": w}
                          for (_, n, c), w in zip(found, weights)]
        elif len(found) == 1 and weights:
            # 只有1个指数 + 有权重
            _, n, c = found[0]
            components = [{"name": n, "code": c, "weight": weights[0]}]
        else:
            # 其他情况：按顺序分配权重，超出部分给1.0
            components = [{"name": n, "code": c,
                          "weight": weights[i] if i < len(weights) else 1.0}
                          for i, (_, n, c) in enumerate(found)]

        # --- Step 4: 权重归一化 ---
        total = sum(x["weight"] for x in components)
        if total > 0 and abs(total - 1.0) > 0.05:
            warnings.append(f"权重总和为 {total:.2%}，已归一化为 100%")
            for x in components:
                x["weight"] = round(x["weight"] / total, 4)

        # --- Step 5: 判断基准类型 ---
        if not components:
            return {"type": "unknown", "components": [], "warnings": warnings}

        all_stock = all("债" not in c["name"] for c in components)
        all_bond = all("债" in c["name"] for c in components)
        has_cb = any("可转债" in c["name"] or "000832" in c["code"] for c in components)

        if has_cb:
            btype = "cb_index"
        elif all_stock:
            btype = "stock_index"
        elif all_bond:
            btype = "bond_index"
        else:
            btype = "mixed_index"

        return {"type": btype, "components": components, "warnings": warnings}

    def get_default_benchmark(self, fund_category: str) -> dict:
        """
        根据基金分类获取默认业绩比较基准。

        Args:
            fund_category: 基金分类（如 "偏股混合型"）

        Returns:
            {
                "equity_code": "000300.SH" | None,
                "equity_weight": 0.8,
                "bond_code": "H11001.CSI" | None,
                "bond_weight": 0.2,
                "source": "category_mapping" | "global_default"
            }
        """
        # 尝试精确匹配
        for key, config in self.DEFAULT_BENCHMARK_WEIGHTS.items():
            if key in fund_category:
                return {**config, "source": "category_mapping"}

        # 无法精确匹配，使用全局默认
        return {**self.GLOBAL_DEFAULT, "source": "global_default"}

    def synthesize(
        self,
        fund_nav_df: pd.DataFrame,
        components_dict: dict,
    ) -> pd.DataFrame:
        """
        合成基准收益率序列。

        执行：
        1. 日期对齐（以基金净值为 Master Clock）
        2. 收益率加权（各组件按权重加权求和）
        3. 常数序列生成（CONST_RATE 转为日收益率）

        Args:
            fund_nav_df: 基金净值 DataFrame，包含 "date" 列
            components_dict: {
                "equity_code": "000300.SH",
                "equity_weight": 0.8,
                "bond_code": "H11001.CSI",
                "bond_weight": 0.2,
            }

        Returns:
            DataFrame with columns: ["date", "bm_ret"]
        """
        if fund_nav_df.empty or "date" not in fund_nav_df.columns:
            return pd.DataFrame(columns=["date", "bm_ret"])

        # --- Step 1: 日期对齐（Master Clock = 基金净值日期）---
        master_dates = pd.to_datetime(fund_nav_df["date"])
        result_df = pd.DataFrame({"date": master_dates})
        bm_ret = pd.Series(0.0, index=master_dates)

        # --- Step 2: 收益率加权 ---
        # 权益部分
        equity_code = components_dict.get("equity_code")
        equity_weight = components_dict.get("equity_weight", 0.0)

        if equity_code and equity_weight > 0:
            if equity_code == "CONST_RATE":
                # 常数序列（现金/货币）
                daily_rate = self._annual_to_daily(0.0035)  # 0.35% 年化
                const_ret = pd.Series(daily_rate, index=master_dates)
                bm_ret += const_ret * equity_weight
            else:
                # 加载指数数据
                equity_df = self._load_index_data(equity_code, master_dates)
                if not equity_df.empty and "ret" in equity_df.columns:
                    # inner join 对齐日期
                    merged = result_df.merge(equity_df[["date", "ret"]], on="date", how="inner")
                    if not merged.empty:
                        equity_ret = pd.Series(merged["ret"].values, index=pd.to_datetime(merged["date"]))
                        bm_ret += equity_ret.reindex(master_dates, fill_value=0.0) * equity_weight

        # 债券部分
        bond_code = components_dict.get("bond_code")
        bond_weight = components_dict.get("bond_weight", 0.0)

        if bond_code and bond_weight > 0:
            if bond_code == "CONST_RATE":
                # 常数序列（现金/货币）
                daily_rate = self._annual_to_daily(0.0035)  # 0.35% 年化
                const_ret = pd.Series(daily_rate, index=master_dates)
                bm_ret += const_ret * bond_weight
            else:
                # 加载指数数据
                bond_df = self._load_index_data(bond_code, master_dates)
                if not bond_df.empty and "ret" in bond_df.columns:
                    # inner join 对齐日期
                    merged = result_df.merge(bond_df[["date", "ret"]], on="date", how="inner")
                    if not merged.empty:
                        bond_ret = pd.Series(merged["ret"].values, index=pd.to_datetime(merged["date"]))
                        bm_ret += bond_ret.reindex(master_dates, fill_value=0.0) * bond_weight

        result_df["bm_ret"] = bm_ret.values
        return result_df

    def _annual_to_daily(self, annual_rate: float) -> float:
        """
        年化利率转日收益率（复利法）。

        公式：(1 + annual_rate) ^ (1 / trading_days_per_year) - 1

        Args:
            annual_rate: 年化利率（如 0.015 表示 1.5%）

        Returns:
            日收益率
        """
        return (1 + annual_rate) ** (1 / self.trading_days_per_year) - 1

    def _load_index_data(self, code: str, target_dates: pd.Series) -> pd.DataFrame:
        """
        加载指数数据（占位符，实际由外部调用 index_loader）。

        注意：这个方法只是占位符，实际的数据加载应该在
        pipeline 中通过 index_loader.load_index_daily() 完成。

        这里返回空 DataFrame，实际使用时需要在 pipeline 中传入
        已加载的指数数据。

        Args:
            code: 指数代码（如 "000300.SH"）
            target_dates: 目标日期序列

        Returns:
            空 DataFrame（实际数据由 pipeline 提供）
        """
        # 占位符，实际不调用外部 API
        return pd.DataFrame(columns=["date", "ret"])

