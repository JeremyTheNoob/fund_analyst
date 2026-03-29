"""
代码质量检查清单（验证版本）
检查范围：@audit_logger 装饰器应用、魔法数字替换、日志统一
"""

import ast
import re
from pathlib import Path
from typing import Dict, List, Set

def check_audit_logger_usage():
    """检查核心函数是否应用了 @audit_logger 装饰器"""
    print("\n" + "="*60)
    print("检查点 1: @audit_logger 装饰器应用")
    print("="*60)

    expected_functions = {
        "pipeline.py": ["analyze_fund"],
        "data_loader/equity_loader.py": ["load_basic_info", "load_nav"],
        "engine/equity_engine.py": ["run_equity_analysis"],
        "engine/bond_engine.py": ["run_bond_analysis"],
        "engine/index_engine.py": ["run_index_analysis"],
        "engine/convertible_bond_engine.py": ["run_cb_analysis"],
        "reporter/translator.py": ["generate_text_report"],
        "reporter/chart_gen.py": ["generate_chart_data"],
    }

    root = Path(__file__).parent.parent
    results = []

    for file_path, func_names in expected_functions.items():
        full_path = root / file_path
        if not full_path.exists():
            results.append((file_path, None, "文件不存在"))
            continue

        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content, filename=str(full_path))

        functions_found = {}
        has_audit_import = False

        for node in ast.walk(tree):
            # 检查是否有 audit_logger 导入
            if isinstance(node, ast.ImportFrom):
                if node.module and 'audit_logger' in [alias.name for alias in node.names]:
                    has_audit_import = True

            # 检查函数定义
            if isinstance(node, ast.FunctionDef):
                if node.name in func_names:
                    has_decorator = False
                    for decorator in node.decorator_list:
                        if isinstance(decorator, ast.Name) and decorator.id == 'audit_logger':
                            has_decorator = True
                            break
                    functions_found[node.name] = has_decorator

        for func_name in func_names:
            if func_name in functions_found:
                if functions_found[func_name]:
                    results.append((file_path, func_name, "✅ 已应用 @audit_logger"))
                else:
                    results.append((file_path, func_name, "❌ 未应用 @audit_logger"))
            else:
                results.append((file_path, func_name, "⚠️ 未找到函数定义"))

        if not has_audit_import:
            print(f"\n⚠️ 警告: {file_path} 未导入 audit_logger")

    print("\n检查结果:")
    for file_path, func_name, status in results:
        print(f"  {file_path}::{func_name} - {status}")

    all_passed = all("✅" in status for _, _, status in results)
    return all_passed

def check_magic_numbers():
    """检查是否还有硬编码的 252 和 1e-6"""
    print("\n" + "="*60)
    print("检查点 2: 魔法数字替换")
    print("="*60)

    root = Path(__file__).parent.parent
    engine_files = [
        "engine/common_metrics.py",
        "engine/equity_engine.py",
        "engine/bond_engine.py",
        "engine/index_engine.py",
        "engine/convertible_bond_engine.py",
    ]

    issues = []
    for file_path in engine_files:
        full_path = root / file_path
        if not full_path.exists():
            continue

        with open(full_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for i, line in enumerate(lines, 1):
            # 检查硬编码的 252（排除 FinancialConfig 引用）
            if re.search(r'\b252\b', line) and 'FinancialConfig' not in line:
                # 排除注释和 TRADING_DAYS = 252 的定义（如果保留别名）
                if not line.strip().startswith('#') and 'TRADING_DAYS = 252' not in line:
                    issues.append((file_path, i, line.strip(), "发现硬编码 252"))

            # 检查硬编码的 1e-6
            if re.search(r'\b1e-6\b', line) and 'FinancialConfig' not in line and not line.strip().startswith('#'):
                issues.append((file_path, i, line.strip(), "发现硬编码 1e-6"))

    if issues:
        print("\n⚠️ 发现问题:")
        for file_path, line_num, line_content, reason in issues:
            print(f"  {file_path}:{line_num} - {reason}")
            print(f"    {line_content}")
        return False
    else:
        print("\n✅ 未发现硬编码的魔法数字")
        return True

def check_log_config():
    """检查日志配置是否统一"""
    print("\n" + "="*60)
    print("检查点 3: 日志统一化")
    print("="*60)

    root = Path(__file__).parent.parent
    main_path = root / "main.py"

    if not main_path.exists():
        print("❌ main.py 不存在")
        return False

    with open(main_path, 'r', encoding='utf-8') as f:
        content = f.read()

    has_setup = 'from utils.common import setup_global_logging' in content
    has_call = 'setup_global_logging()' in content
    has_basic = 'logging.basicConfig' in content

    print(f"  导入 setup_global_logging: {'✅' if has_setup else '❌'}")
    print(f"  调用 setup_global_logging(): {'✅' if has_call else '❌'}")
    basic_config_status = '❌ (已移除)' if not has_basic else '⚠️ (仍在使用)'
    print(f"  使用 basicConfig: {basic_config_status}")

    passed = has_setup and has_call and not has_basic
    return passed

def check_import_duplicates():
    """检查是否有重复导入"""
    print("\n" + "="*60)
    print("检查点 4: 导入语句检查")
    print("="*60)

    root = Path(__file__).parent.parent
    main_path = root / "main.py"

    with open(main_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查重复的 sys.path.insert
    sys_path_count = content.count('sys.path.insert')

    print(f"  sys.path.insert 出现次数: {sys_path_count}")
    if sys_path_count > 1:
        print("  ❌ 发现重复的 sys.path.insert")
        return False
    else:
        print("  ✅ 无重复导入")
        return True

def run_all_checks():
    """运行所有检查"""
    print("\n" + "="*60)
    print("基金穿透式分析 - 代码质量检查（验证版本）")
    print("="*60)

    checks = [
        ("@audit_logger 装饰器应用", check_audit_logger_usage),
        ("魔法数字替换", check_magic_numbers),
        ("日志统一化", check_log_config),
        ("导入语句检查", check_import_duplicates),
    ]

    results = {}
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"\n❌ {check_name} 检查出错: {e}")
            results[check_name] = False

    print("\n" + "="*60)
    print("检查总结")
    print("="*60)

    for check_name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {check_name}: {status}")

    all_passed = all(results.values())
    print(f"\n总体评分: {'⭐⭐⭐⭐⭐ (5/5)' if all_passed else '⭐⭐⭐⭐ (4/5)' if 3 <= sum(results.values()) < 5 else '⭐⭐⭐ (3/5)'}")

    return all_passed

if __name__ == "__main__":
    run_all_checks()
