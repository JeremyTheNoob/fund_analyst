"""
AkShare 超时配置优化方案

解决 P1-3 问题：AkShare API 无超时配置
"""

import logging
import signal
import time
from functools import wraps
from typing import Callable, Any, Optional
import requests

logger = logging.getLogger(__name__)


# ============================================================
# 超时异常
# ============================================================

class TimeoutError(Exception):
    """自定义超时异常"""
    pass


# ============================================================
# 超时装饰器（使用 signal 机制）
# ============================================================

def timeout_decorator(seconds: float = 10.0):
    """
    超时装饰器（基于 signal 机制）

    注意：
    1. 仅在 Unix 系统上有效（macOS、Linux）
    2. 不能在多线程环境中使用
    3. 超时后会抛出 TimeoutError

    Args:
        seconds: 超时时间（秒）
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 定义超时处理函数
            def _timeout_handler(signum, frame):
                raise TimeoutError(f"函数 {func.__name__} 超时（{seconds} 秒）")

            # 注册信号处理
            old_signal = signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(seconds)  # 设置定时器

            try:
                result = func(*args, **kwargs)
            except TimeoutError as e:
                logger.error(f"[timeout_decorator] {e}")
                raise
            finally:
                # 恢复原始信号
                signal.alarm(0)  # 取消定时器
                signal.signal(signal.SIGALRM, old_signal)

            return result
        return wrapper
    return decorator


# ============================================================
# 通用超时包装器（使用多线程 + join 超时）
# ============================================================

def call_with_timeout(
    func: Callable,
    args: tuple = (),
    kwargs: dict = None,
    timeout: float = 10.0,
) -> Any:
    """
    通用超时包装器（跨平台，支持多线程）

    使用线程 + join(timeout) 机制实现超时控制

    Args:
        func: 要调用的函数
        args: 位置参数
        kwargs: 关键字参数
        timeout: 超时时间（秒）

    Returns:
        函数返回值

    Raises:
        TimeoutError: 超时
        Exception: 函数本身的异常
    """
    if kwargs is None:
        kwargs = {}

    import threading

    result_container = []
    exception_container = []

    def _worker():
        try:
            result = func(*args, **kwargs)
            result_container.append(result)
        except Exception as e:
            exception_container.append(e)

    # 创建并启动线程
    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()

    # 等待线程完成或超时
    thread.join(timeout=timeout)

    # 检查结果
    if thread.is_alive():
        # 线程仍在运行 → 超时
        logger.error(f"[call_with_timeout] 函数 {func.__name__} 超时（{timeout} 秒）")
        raise TimeoutError(f"函数 {func.__name__} 超时（{timeout} 秒）")

    if exception_container:
        # 函数抛出异常
        raise exception_container[0]

    if result_container:
        # 正常返回
        return result_container[0]

    raise RuntimeError(f"函数 {func.__name__} 执行异常：无返回值且无异常信息")


# ============================================================
# AkShare 请求超时包装器
# ============================================================

def akshare_timeout_wrapper(
    func: Callable,
    timeout: float = 10.0,
    timeout_msg: str = "AkShare API 请求超时",
) -> Callable:
    """
    AkShare 请求超时包装器

    使用 call_with_timeout 包装 AkShare API 调用

    Args:
        func: AkShare 函数
        timeout: 超时时间（秒）
        timeout_msg: 超时提示信息

    Returns:
        包装后的函数
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return call_with_timeout(func, args, kwargs, timeout)
        except TimeoutError as e:
            logger.warning(f"[akshare_timeout_wrapper] {timeout_msg}: {e}")
            raise
        except Exception as e:
            logger.error(f"[akshare_timeout_wrapper] AkShare API 调用失败: {e}")
            raise

    return wrapper


# ============================================================
# Requests Session 超时配置
# ============================================================

def get_timeout_session(timeout: float = 10.0, max_retries: int = 3) -> requests.Session:
    """
    创建带超时配置的 Requests Session

    注意：
    1. 此函数需要 patch AkShare 底层的 requests.Session
    2. 当前版本 AkShare 可能不支持，仅作为参考方案

    Args:
        timeout: 超时时间（秒）
        max_retries: 最大重试次数

    Returns:
        配置好的 Session 对象
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()

    # 配置重试策略
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1.0,
        status_forcelist=[408, 429, 500, 502, 503, 504],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # 设置默认超时
    # 注意：需要在实际请求时显式传入 timeout 参数
    session.timeout = timeout

    return session


# ============================================================
# 装饰器：自动超时包装
# ============================================================

def with_timeout(timeout: float = 10.0):
    """
    超时装饰器（使用 call_with_timeout）

    使用示例：
        @with_timeout(timeout=5.0)
        def load_benchmark_data(code: str):
            # ... 网络请求 ...
            return df
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return call_with_timeout(func, args, kwargs, timeout)
        return wrapper
    return decorator


# ============================================================
# 测试代码
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("AkShare 超时配置测试")
    print("=" * 60)

    # 测试 1: call_with_timeout 正常场景
    print("\n[测试 1] call_with_timeout 正常场景（2 秒超时）")
    def fast_func():
        time.sleep(1)
        return "快速完成"

    try:
        result = call_with_timeout(fast_func, timeout=2.0)
        print(f"✅ 结果: {result}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    # 测试 2: call_with_timeout 超时场景
    print("\n[测试 2] call_with_timeout 超时场景（0.5 秒超时）")
    def slow_func():
        time.sleep(2)
        return "永远不会返回"

    try:
        result = call_with_timeout(slow_func, timeout=0.5)
        print(f"✅ 结果: {result}")
    except TimeoutError as e:
        print(f"✅ 成功捕获超时: {e}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    # 测试 3: with_timeout 装饰器
    print("\n[测试 3] with_timeout 装饰器")

    @with_timeout(timeout=1.0)
    def decorated_func(seconds: int):
        time.sleep(seconds)
        return f"睡了 {seconds} 秒"

    try:
        result = decorated_func(0.5)
        print(f"✅ 结果: {result}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    try:
        result = decorated_func(2.0)
        print(f"✅ 结果: {result}")
    except TimeoutError as e:
        print(f"✅ 成功捕获超时: {e}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    # 测试 4: 异常传播
    print("\n[测试 4] 异常传播")
    def error_func():
        raise ValueError("测试异常")

    try:
        result = call_with_timeout(error_func, timeout=2.0)
        print(f"✅ 结果: {result}")
    except ValueError as e:
        print(f"✅ 成功捕获异常: {e}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
