#!/usr/bin/env python3
"""
基金代码验证跳过模块
方案2：先跳过代码验证，功能保留但先不上线
"""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def validate_fund_code_quick(symbol: str) -> bool:
    """
    快速基金代码验证（极简版本）
    
    方案2实现：仅进行最基本的格式检查，不进行API验证
    目的：解决用户输入后校验时间过长的问题
    
    返回：
        True - 代码格式正确（但不保证真实存在）
        False - 代码格式明显错误
    """
    # 极简验证：只检查是否是6位数字
    if not symbol or len(symbol) != 6:
        logger.debug(f"[validate_fund_code_quick] 基金代码格式无效: {symbol}")
        return False
    
    # 检查是否全是数字
    if not symbol.isdigit():
        logger.debug(f"[validate_fund_code_quick] 基金代码包含非数字字符: {symbol}")
        return False
    
    # 格式检查通过
    logger.debug(f"[validate_fund_code_quick] 基金代码格式正确: {symbol}")
    return True


def validate_with_fallback(symbol: str, use_api: bool = True) -> Dict[str, Any]:
    """
    带降级的验证方案
    
    方案：优先使用快速验证，如果用户需要严格验证则降级到API
    
    返回：
        {
            'valid': True/False,
            'method': 'quick'/'api'/'none',
            'message': '验证消息',
            'warning': '警告信息（如有）'
        }
    """
    result = {
        'valid': False,
        'method': 'none',
        'message': '',
        'warning': ''
    }
    
    # 1. 快速格式验证
    if validate_fund_code_quick(symbol):
        result['valid'] = True
        result['method'] = 'quick'
        result['message'] = '代码格式正确'
        result['warning'] = '未验证基金是否存在，将尝试直接加载数据'
    else:
        result['valid'] = False
        result['method'] = 'quick'
        result['message'] = '代码格式错误'
        result['warning'] = '请输入6位数字基金代码'
        return result
    
    # 2. 如果需要API验证，尝试降级验证
    if use_api:
        try:
            from .equity_loader import validate_fund_code_fast
            is_valid_api = validate_fund_code_fast(symbol)
            
            if is_valid_api:
                result['method'] = 'api'
                result['message'] = '基金代码验证通过'
                result['warning'] = ''  # 清除警告
            else:
                result['warning'] = '基金代码格式正确但可能不存在，将继续尝试加载'
                # 不将valid设为False，让系统尝试加载
                
        except Exception as e:
            logger.debug(f"[validate_with_fallback] API验证异常: {e}")
            result['warning'] = 'API验证失败，将继续尝试加载数据'
    
    return result


def bypass_validation_directly(symbol: str) -> bool:
    """
    完全跳过验证，直接返回True（用于紧急情况）
    
    警告：这会使无效代码进入系统，可能导致后续错误
    仅在性能问题严重影响用户体验时使用
    """
    # 仅进行最基本的安全检查
    if not symbol or len(symbol) == 0:
        return False
    
    logger.warning(f"[bypass_validation_directly] 跳过验证: {symbol}")
    return True


def get_validation_strategy() -> str:
    """
    获取当前验证策略
    
    返回：
        'quick' - 快速验证（方案2）
        'local' - 本地目录验证（方案1）
        'api' - API验证（原方案）
        'bypass' - 跳过验证（紧急方案）
    """
    try:
        # 尝试检查本地目录是否存在
        import os
        from fund_directory import FUND_DIRECTORY_FILE
        
        if os.path.exists(FUND_DIRECTORY_FILE):
            return 'local'
    except:
        pass
    
    # 默认使用快速验证
    return 'quick'


def validate_fund_code_strategic(symbol: str) -> Dict[str, Any]:
    """
    策略性验证：根据系统状态选择最佳验证方式
    
    流程：
    1. 快速格式验证（必需）
    2. 如果本地目录可用，使用本地验证
    3. 如果本地不可用，使用快速验证但警告用户
    4. 如果需要严格验证，降级到API（异步）
    """
    strategy = get_validation_strategy()
    
    if strategy == 'local':
        # 方案1：本地目录验证
        try:
            from fund_directory import validate_fund_code_local
            is_valid = validate_fund_code_local(symbol)
            
            return {
                'valid': is_valid,
                'method': 'local',
                'message': '本地目录验证' + ('通过' if is_valid else '失败'),
                'warning': '' if is_valid else '基金代码不存在于本地目录'
            }
        except Exception as e:
            logger.warning(f"[validate_fund_code_strategic] 本地验证失败，降级到快速验证: {e}")
            strategy = 'quick'  # 降级
    
    if strategy == 'quick':
        # 方案2：快速验证
        return validate_with_fallback(symbol, use_api=False)
    
    if strategy == 'api':
        # 原方案：API验证
        try:
            from .equity_loader import validate_fund_code_fast
            is_valid = validate_fund_code_fast(symbol)
            
            return {
                'valid': is_valid,
                'method': 'api',
                'message': 'API验证' + ('通过' if is_valid else '失败'),
                'warning': 'API验证可能较慢'
            }
        except Exception as e:
            logger.error(f"[validate_fund_code_strategic] API验证异常: {e}")
            return validate_with_fallback(symbol, use_api=False)
    
    # 默认：快速验证
    return validate_with_fallback(symbol, use_api=False)


# 主验证函数（供外部调用）
def validate_fund_code(symbol: str, strict: bool = False) -> Dict[str, Any]:
    """
    主验证函数
    
    Args:
        symbol: 基金代码
        strict: 是否严格验证（True时降级到API验证）
    
    Returns:
        验证结果字典
    """
    # 基础检查
    if not symbol:
        return {
            'valid': False,
            'method': 'none',
            'message': '请输入基金代码',
            'warning': ''
        }
    
    # 根据strict参数选择策略
    if strict:
        return validate_with_fallback(symbol, use_api=True)
    else:
        return validate_fund_code_strategic(symbol)


def main():
    """测试验证功能"""
    test_cases = [
        ("000001", True, "有效基金"),
        ("000069", True, "有效基金"),
        ("510300", True, "有效ETF"),
        ("999999", False, "无效代码"),
        ("123", False, "过短"),
        ("1234567", False, "过长"),
        ("abc123", False, "包含字母"),
        ("", False, "空代码")
    ]
    
    print("=== 基金代码验证测试 ===")
    print("策略:", get_validation_strategy())
    print("-" * 60)
    
    for code, expected_valid, description in test_cases:
        result = validate_fund_code(code, strict=False)
        
        status = "✅" if result['valid'] == expected_valid else "❌"
        method = result['method']
        message = result['message']
        warning = result['warning']
        
        print(f"{status} {code} ({description}):")
        print(f"  方法: {method}, 有效: {result['valid']}, 消息: {message}")
        if warning:
            print(f"  警告: {warning}")
        print()


if __name__ == "__main__":
    main()