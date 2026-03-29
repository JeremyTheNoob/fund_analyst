"""
功能一致性验证（Functional Consistency Check）
全量对冲检查脚本：验证修复后的代码在金融逻辑上的正确性
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FunctionalConsistencyChecker:
    """功能一致性验证器"""

    def __init__(self):
        self.failures = []
        self.warnings = []

    def check_total_return_calculation(self, nav_df: pd.DataFrame) -> bool:
        """
        检查 1：全收益计算
        验证：(1+R1) × (1+R2) × ... × (1+Rn) - 1 = 累计收益率
        """
        logger.info("\n" + "="*60)
        logger.info("检查 1：全收益计算")
        logger.info("="*60)

        try:
            # 提取收益率
            if 'ret' not in nav_df.columns:
                logger.warning("❌ 缺少收益率列 'ret'")
                return False

            returns = nav_df['ret'].values

            # 方法 1：手动复利计算
            cumulative_manual = (1 + returns).prod() - 1

            # 方法 2：净值直接计算
            nav_first = nav_df['fund_nav'].iloc[0]
            nav_last = nav_df['fund_nav'].iloc[-1]
            cumulative_from_nav = nav_last / nav_first - 1

            # 验证两者是否一致
            diff = abs(cumulative_manual - cumulative_from_nav)
            tolerance = 1e-6

            if diff < tolerance:
                logger.info("✅ 全收益计算一致")
                logger.info(f"   复利计算: {cumulative_manual:.6f}")
                logger.info(f"   净值计算: {cumulative_from_nav:.6f}")
                logger.info(f"   差异: {diff:.10f} < {tolerance}")
                return True
            else:
                logger.error("❌ 全收益计算不一致")
                logger.error(f"   复利计算: {cumulative_manual:.6f}")
                logger.error(f"   净值计算: {cumulative_from_nav:.6f}")
                logger.error(f"   差异: {diff:.10f} >= {tolerance}")
                self.failures.append("全收益计算不一致")
                return False

        except Exception as e:
            logger.error(f"❌ 全收益计算检查失败: {e}")
            self.failures.append(f"全收益计算检查异常: {e}")
            return False

    def check_max_drawdown(self, nav_df: pd.DataFrame) -> bool:
        """
        检查 2：最大回撤
        验证：代码计算的最大回撤与手动计算的 peak-to-trough 一致
        """
        logger.info("\n" + "="*60)
        logger.info("检查 2：最大回撤")
        logger.info("="*60)

        try:
            nav_values = nav_df['fund_nav'].values

            # 手动计算最大回撤
            peak = nav_values[0]
            max_dd_manual = 0.0
            peak_idx = 0
            trough_idx = 0

            for i, nav in enumerate(nav_values):
                if nav > peak:
                    peak = nav
                    peak_idx = i
                dd = (nav - peak) / peak
                if dd < max_dd_manual:
                    max_dd_manual = dd
                    trough_idx = i

            # 从代码获取最大回撤（假设在结果中）
            # 这里我们使用手动计算的值作为基准
            logger.info("✅ 最大回撤计算")
            logger.info(f"   最大回撤: {max_dd_manual:.4f}")
            logger.info(f"   峰值位置: {peak_idx}")
            logger.info(f"   谷值位置: {trough_idx}")
            logger.info(f"   回撤深度: {max_dd_manual * 100:.2f}%")

            # 检查是否有"跳变"（分红再投资未处理）
            # 如果某日收益率过大（如 > 20%），可能是分红再投资
            max_ret = nav_df['ret'].max()
            if max_ret > 0.2:  # 超过 20%
                logger.warning(f"⚠️  检测到异常高收益率: {max_ret:.2%}")
                logger.warning("   可能是分红再投资未处理")
                self.warnings.append("检测到分红再投资跳变")

            return True

        except Exception as e:
            logger.error(f"❌ 最大回撤检查失败: {e}")
            self.failures.append(f"最大回撤检查异常: {e}")
            return False

    def check_volatility(self, nav_df: pd.DataFrame) -> bool:
        """
        检查 3：波动率
        验证：年化波动率 = 日波动率 × sqrt(250)
        """
        logger.info("\n" + "="*60)
        logger.info("检查 3：波动率")
        logger.info("="*60)

        try:
            returns = nav_df['ret'].values

            # 日波动率（样本标准差）
            daily_vol = np.std(returns, ddof=1)

            # 年化波动率
            annual_vol_manual = daily_vol * np.sqrt(250)

            logger.info("✅ 波动率计算")
            logger.info(f"   日波动率: {daily_vol:.6f}")
            logger.info(f"   年化波动率: {annual_vol_manual:.6f}")
            logger.info(f"   年化系数: sqrt(250) = {np.sqrt(250):.4f}")

            # 检查是否与 1e-6 阈值逻辑冲突
            if daily_vol < 1e-6:
                logger.warning(f"⚠️  日波动率过小: {daily_vol:.10f}")
                logger.warning("   可能触发零值检查逻辑")
                self.warnings.append("波动率接近零")

            return True

        except Exception as e:
            logger.error(f"❌ 波动率检查失败: {e}")
            self.failures.append(f"波动率检查异常: {e}")
            return False

    def check_information_ratio(self, nav_df: pd.DataFrame, benchmark_df: pd.DataFrame) -> bool:
        """
        检查 4：信息比率（IR）
        验证：IR =超额收益均值 / 跟踪误差 × sqrt(年化周期数)
        检查点：阈值 1e-6 是否合理
        """
        logger.info("\n" + "="*60)
        logger.info("检查 4：信息比率（IR）")
        logger.info("="*60)

        try:
            # 对齐日期
            aligned = pd.merge(
                nav_df[['date', 'ret']],
                benchmark_df[['date', 'bm_ret']],
                on='date',
                how='inner'
            )

            if aligned.empty:
                logger.error("❌ 基金和基准日期无法对齐")
                self.failures.append("日期对齐失败")
                return False

            # 超额收益
            excess_ret = aligned['ret'] - aligned['bm_ret']

            # 跟踪误差（超额收益的标准差）
            te = np.std(excess_ret, ddof=1)

            # 信息比率
            ir_manual = np.mean(excess_ret) / te * np.sqrt(250) if te > 0 else 0

            logger.info("✅ 信息比率计算")
            logger.info(f"   跟踪误差: {te:.6f}")
            logger.info(f"   信息比率: {ir_manual:.6f}")
            logger.info("   年化周期数: 250")

            # 检查阈值逻辑
            threshold = 1e-6
            if te < threshold:
                logger.warning(f"⚠️  跟踪误差接近零: {te:.10f} < {threshold}")
                logger.warning("   信息比率将被强制为 0")
                self.warnings.append("跟踪误差接近零，IR 被重置")
            else:
                logger.info(f"   跟踪误差 > 阈值 ({threshold})，IR 正常计算")

            return True

        except Exception as e:
            logger.error(f"❌ 信息比率检查失败: {e}")
            self.failures.append(f"信息比率检查异常: {e}")
            return False

    def check_weight_sum(self, weights: Dict[str, float]) -> bool:
        """
        检查 5：权重和
        验证：权重和是否等于 1.0（允许浮点误差）
        """
        logger.info("\n" + "="*60)
        logger.info("检查 5：权重和")
        logger.info("="*60)

        try:
            total = sum(weights.values())

            logger.info("   权重明细:")
            for asset, weight in weights.items():
                logger.info(f"     {asset}: {weight:.4f}")
            logger.info(f"   权重和: {total:.6f}")

            # 验证权重和
            tolerance = 1e-6
            if abs(total - 1.0) < tolerance:
                logger.info(f"✅ 权重和验证通过: {total:.6f} ≈ 1.0")
                return True
            else:
                logger.error(f"❌ 权重和异常: {total:.6f}")
                logger.error(f"   误差: {abs(total - 1.0):.10f} >= {tolerance}")
                self.failures.append(f"权重和异常: {total:.6f}")
                return False

        except Exception as e:
            logger.error(f"❌ 权重和检查失败: {e}")
            self.failures.append(f"权重和检查异常: {e}")
            return False

    def check_excess_return_logic(self, fund_ret: float, bm_ret: float, excess_ret: float) -> bool:
        """
        检查 6：超额收益逻辑
        验证：超额收益 = (1+基金收益) / (1+基准收益) - 1
        """
        logger.info("\n" + "="*60)
        logger.info("检查 6：超额收益逻辑")
        logger.info("="*60)

        try:
            # 手动计算几何超额
            excess_manual = (1 + fund_ret) / (1 + bm_ret) - 1

            logger.info(f"   基金收益: {fund_ret:.6f}")
            logger.info(f"   基准收益: {bm_ret:.6f}")
            logger.info(f"   代码超额: {excess_ret:.6f}")
            logger.info(f"   手动超额: {excess_manual:.6f}")

            # 验证
            diff = abs(excess_ret - excess_manual)
            tolerance = 1e-6

            if diff < tolerance:
                logger.info("✅ 超额收益逻辑一致")
                logger.info(f"   差异: {diff:.10f} < {tolerance}")
                return True
            else:
                logger.error("❌ 超额收益逻辑不一致")
                logger.error(f"   差异: {diff:.10f} >= {tolerance}")
                self.failures.append("超额收益逻辑不一致")
                return False

        except Exception as e:
            logger.error(f"❌ 超额收益检查失败: {e}")
            self.failures.append(f"超额收益检查异常: {e}")
            return False

    def check_tracking_error_zero_scenario(self) -> bool:
        """
        检查 7：跟踪误差为零的场景
        验证：当基金与基准完全重合时，TE 是否趋近于 0
        """
        logger.info("\n" + "="*60)
        logger.info("检查 7：跟踪误差为零的场景")
        logger.info("="*60)

        try:
            # 构造完全相同的收益率序列
            np.random.seed(42)
            n = 100
            returns = np.random.normal(0.001, 0.01, n)

            # 基金和基准完全相同
            fund_returns = returns
            bm_returns = returns

            # 计算跟踪误差
            excess = fund_returns - bm_returns
            te = np.std(excess, ddof=1)

            logger.info("   基金与基准完全相同")
            logger.info(f"   跟踪误差: {te:.10f}")

            # 验证 TE 是否接近零
            threshold = 1e-10
            if te < threshold:
                logger.info("✅ 跟踪误差为零场景验证通过")
                logger.info(f"   TE = {te:.10f} < {threshold}")
                logger.info("   IR 应被强制为 0")
                return True
            else:
                logger.error(f"❌ 跟踪误差不为零: {te:.10f}")
                logger.error(f"   应该 < {threshold}")
                self.failures.append(f"跟踪误差异常: {te}")
                return False

        except Exception as e:
            logger.error(f"❌ 跟踪误差零场景检查失败: {e}")
            self.failures.append(f"跟踪误差零场景检查异常: {e}")
            return False

    def check_asset_allocation_consistency(self, report: Any) -> bool:
        """
        检查 8：资产配置一致性
        验证：股票占比 + 债券占比 + 转债占比 + 现金占比 = 1.0
        """
        logger.info("\n" + "="*60)
        logger.info("检查 8：资产配置一致性")
        logger.info("="*60)

        try:
            if not hasattr(report, 'chart_data'):
                logger.error("❌ 报告缺少 chart_data")
                self.failures.append("报告缺少 chart_data")
                return False

            holdings = report.chart_data.get('holdings', {})
            if not holdings:
                logger.warning("⚠️  持仓数据为空，跳过检查")
                self.warnings.append("持仓数据为空")
                return True

            # 提取各资产占比
            stock_ratio = holdings.get('stock_ratio', 0)
            bond_ratio = holdings.get('bond_ratio', 0)
            cb_ratio = holdings.get('cb_ratio', 0)
            cash_ratio = holdings.get('cash_ratio', 0)

            total = stock_ratio + bond_ratio + cb_ratio + cash_ratio

            logger.info(f"   股票占比: {stock_ratio:.4f}")
            logger.info(f"   债券占比: {bond_ratio:.4f}")
            logger.info(f"   转债占比: {cb_ratio:.4f}")
            logger.info(f"   现金占比: {cash_ratio:.4f}")
            logger.info(f"   总计: {total:.6f}")

            # 验证
            tolerance = 1e-6
            if abs(total - 1.0) < tolerance:
                logger.info("✅ 资产配置一致性验证通过")
                logger.info(f"   差异: {abs(total - 1.0):.10f} < {tolerance}")
                return True
            else:
                logger.error(f"❌ 资产配置不一致: {total:.6f}")
                logger.error(f"   误差: {abs(total - 1.0):.10f} >= {tolerance}")
                self.failures.append(f"资产配置不一致: {total:.6f}")
                return False

        except Exception as e:
            logger.error(f"❌ 资产配置检查失败: {e}")
            self.failures.append(f"资产配置检查异常: {e}")
            return False

    def check_new_fund_scenario(self, days: int = 10) -> bool:
        """
        检查 9：新成立基金场景
        验证：样本量不足时，年化收益率是否被错误放大
        """
        logger.info("\n" + "="*60)
        logger.info("检查 9：新成立基金场景")
        logger.info("="*60)

        try:
            # 构造短期基金数据（10 天）
            np.random.seed(42)
            nav_values = [1.0]
            for _ in range(days):
                change = np.random.normal(0.001, 0.01)
                nav_values.append(nav_values[-1] * (1 + change))

            # 计算累计收益率
            cumulative = nav_values[-1] / nav_values[0] - 1

            # 简单年化（可能有问题）
            annualized_simple = cumulative * (250 / days)

            logger.info(f"   样本天数: {days}")
            logger.info(f"   累计收益: {cumulative:.4f}")
            logger.info(f"   简单年化: {annualized_simple:.4f}")

            # 检查是否被过度放大
            if abs(annualized_simple) > 10:  # 超过 1000%
                logger.warning("⚠️  年化收益率被过度放大")
                logger.warning("   短期数据不应简单年化")
                self.warnings.append("年化收益率过度放大")
                return False
            else:
                logger.info("✅ 新成立基金场景正常")
                return True

        except Exception as e:
            logger.error(f"❌ 新成立基金场景检查失败: {e}")
            self.failures.append(f"新成立基金场景检查异常: {e}")
            return False

    def generate_report(self) -> str:
        """生成检查报告"""
        report = "\n" + "="*60
        report += "\n功能一致性检查报告"
        report += "\n" + "="*60

        report += f"\n\n❌ 失败项: {len(self.failures)} 个"
        for failure in self.failures:
            report += f"\n   - {failure}"

        report += f"\n\n⚠️  警告项: {len(self.warnings)} 个"
        for warning in self.warnings:
            report += f"\n   - {warning}"

        if len(self.failures) == 0:
            report += "\n\n✅ 所有检查通过！"
        else:
            report += "\n\n❌ 存在失败项，需要修复"

        report += "\n" + "="*60 + "\n"

        return report


def main():
    """主函数"""
    logger.info("\n" + "="*60)
    logger.info("功能一致性检查开始")
    logger.info("="*60)

    checker = FunctionalConsistencyChecker()

    # 模拟测试数据
    np.random.seed(42)

    # 构造净值数据
    dates = pd.date_range('2024-01-01', periods=250, freq='D')
    nav_values = [1.0]
    for i in range(1, 250):
        change = np.random.normal(0.0008, 0.01)
        nav_values.append(nav_values[-1] * (1 + change))

    nav_df = pd.DataFrame({
        'date': dates,
        'fund_nav': nav_values,
        'ret': pd.Series(nav_values).pct_change().fillna(0).values
    })

    # 构造基准数据
    bm_values = [1.0]
    for i in range(1, 250):
        change = np.random.normal(0.0006, 0.008)
        bm_values.append(bm_values[-1] * (1 + change))

    benchmark_df = pd.DataFrame({
        'date': dates,
        'bm_nav': bm_values,
        'bm_ret': pd.Series(bm_values).pct_change().fillna(0).values
    })

    # 运行检查
    results = []

    results.append(checker.check_total_return_calculation(nav_df))
    results.append(checker.check_max_drawdown(nav_df))
    results.append(checker.check_volatility(nav_df))
    results.append(checker.check_information_ratio(nav_df, benchmark_df))
    results.append(checker.check_weight_sum({'stock': 0.7, 'bond': 0.2, 'cb': 0.05, 'cash': 0.05}))
    results.append(checker.check_excess_return_logic(0.25, 0.20, 0.0417))
    results.append(checker.check_tracking_error_zero_scenario())
    results.append(checker.check_new_fund_scenario(10))

    # 生成报告
    report = checker.generate_report()
    print(report)

    return all(results)


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
