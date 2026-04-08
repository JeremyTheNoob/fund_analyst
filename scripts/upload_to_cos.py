"""
upload_to_cos.py — 压缩 DB 并上传到腾讯云 COS
=============================================

使用前需要设置环境变量：
    export COS_SECRET_ID="your-secret-id"
    export COS_SECRET_KEY="your-secret-key"
    export COS_BUCKET="fund-analyst-1234567890"    # bucket名，不带 appid
    export COS_REGION="ap-guangzhou"

使用方式：
    # 压缩 + 上传
    python3 scripts/upload_to_cos.py

    # 仅压缩，不上传
    python3 scripts/upload_to_cos.py --compress-only

    # 强制上传（不检查远端是否已有）
    python3 scripts/upload_to_cos.py --force
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path

try:
    from qcloud_cos import CosConfig, CosS3Client
except ImportError:
    print("❌ 请先安装 COS SDK: pip install cos-python-sdk-v5")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "fund_data.db"
MANIFEST_PATH = DATA_DIR / ".cos_manifest.json"

# COS 上的文件路径
COS_DB_KEY = "fund_data.db.tar.gz"
COS_MANIFEST_KEY = "fund_data.manifest.json"


def _check_env():
    required = ["COS_SECRET_ID", "COS_SECRET_KEY", "COS_BUCKET", "COS_REGION"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"❌ 缺少环境变量: {', '.join(missing)}")
        print("请设置：")
        for k in required:
            print(f"  export {k}=<value>")
        sys.exit(1)


def _get_client() -> CosS3Client:
    config = CosConfig(
        Region=os.environ["COS_REGION"],
        SecretId=os.environ["COS_SECRET_ID"],
        SecretKey=os.environ["COS_SECRET_KEY"],
        Scheme="https",
    )
    return CosS3Client(config)


def _file_md5(filepath: Path) -> str:
    """计算文件 MD5"""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def compress_db(output_path: Path) -> dict:
    """压缩 DB 为 tar.gz，返回文件信息"""
    if not DB_PATH.exists():
        print(f"❌ 数据库不存在: {DB_PATH}")
        sys.exit(1)

    db_size = DB_PATH.stat().st_size
    print(f"📦 压缩数据库...")
    print(f"   原始大小: {db_size / 1024 / 1024 / 1024:.2f} GB")

    # 用系统 tar 命令（比 Python 快很多）
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    start = time.time()
    result = subprocess.run(
        ["tar", "-czf", str(output_path), "fund_data.db"],
        cwd=str(DATA_DIR),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"❌ 压缩失败: {result.stderr}")
        sys.exit(1)

    compressed_size = output_path.stat().st_size
    ratio = (1 - compressed_size / db_size) * 100
    elapsed = time.time() - start

    print(f"   压缩大小: {compressed_size / 1024 / 1024 / 1024:.2f} GB (节省 {ratio:.1f}%)")
    print(f"   耗时: {elapsed:.1f} 秒")

    return {
        "db_size": db_size,
        "compressed_size": compressed_size,
        "md5": _file_md5(output_path),
        "compress_time": elapsed,
    }


def read_remote_manifest(client) -> dict | None:
    """读取 COS 上的 manifest 文件"""
    try:
        resp = client.get_object(
            Bucket=os.environ["COS_BUCKET"],
            Key=COS_MANIFEST_KEY,
        )
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body)
    except Exception:
        return None


def upload_to_cos(client, compressed_path: Path, info: dict, force: bool = False):
    """上传压缩包到 COS"""
    bucket = os.environ["COS_BUCKET"]

    # 检查远端是否已有
    if not force:
        remote = read_remote_manifest(client)
        if remote and remote.get("md5") == info["md5"]:
            print("✅ COS 上已有相同版本，跳过上传（加 --force 强制上传）")
            return

    # 上传压缩包
    print(f"☁️  上传到 COS ({bucket}/{COS_DB_KEY})...")
    start = time.time()

    client.upload_file(
        Bucket=bucket,
        Key=COS_DB_KEY,
        LocalFilePath=str(compressed_path),
        EnableMD5=True,
    )

    elapsed = time.time() - start
    print(f"   上传完成，耗时 {elapsed:.1f} 秒")

    # 写入 manifest
    manifest = {
        "version": datetime.now().isoformat(),
        "date": date.today().isoformat(),
        "db_size": info["db_size"],
        "compressed_size": info["compressed_size"],
        "md5": info["md5"],
        "upload_time": elapsed,
    }

    client.put_object(
        Bucket=bucket,
        Key=COS_MANIFEST_KEY,
        Body=json.dumps(manifest, indent=2, ensure_ascii=False),
    )
    print(f"📋 Manifest 已更新: version={manifest['version']}")

    # 本地也存一份
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"✅ 上传完成！")


def main():
    parser = argparse.ArgumentParser(description="压缩 DB 并上传到 COS")
    parser.add_argument("--compress-only", action="store_true", help="仅压缩，不上传")
    parser.add_argument("--force", action="store_true", help="强制上传（不检查远端版本）")
    args = parser.parse_args()

    _check_env()

    compressed_path = PROJECT_ROOT / "data" / "fund_data.db.tar.gz"
    info = compress_db(compressed_path)

    if not args.compress_only:
        client = _get_client()
        upload_to_cos(client, compressed_path, info, force=args.force)
    else:
        print("✅ 压缩完成（未上传）")


if __name__ == "__main__":
    main()
