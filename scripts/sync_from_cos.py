"""
sync_from_cos.py — 从腾讯云 COS 下载并解压 DB
===============================================

使用方式：
    # 首次部署 / 本地无 DB 时
    python3 scripts/sync_from_cos.py

    # 强制更新（忽略本地版本）
    python3 scripts/sync_from_cos.py --force

    # 仅检查远端是否有更新，不下载
    python3 scripts/sync_from_cos.py --check

环境变量：
    COS_SECRET_ID / COS_SECRET_KEY / COS_BUCKET / COS_REGION
    FUND_DB_PATH（可选，默认 data/fund_data.db）
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

try:
    from qcloud_cos import CosConfig, CosS3Client
except ImportError:
    print("❌ 请先安装 COS SDK: pip install cos-python-sdk-v5")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = Path(os.environ.get("FUND_DB_PATH", str(DATA_DIR / "fund_data.db")))
MANIFEST_PATH = DATA_DIR / ".cos_manifest.json"

COS_DB_KEY = "fund_data.db.tar.gz"
COS_MANIFEST_KEY = "fund_data.manifest.json"


def _check_env():
    required = ["COS_SECRET_ID", "COS_SECRET_KEY", "COS_BUCKET", "COS_REGION"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"❌ 缺少环境变量: {', '.join(missing)}")
        sys.exit(1)


def _get_client() -> CosS3Client:
    config = CosConfig(
        Region=os.environ["COS_REGION"],
        SecretId=os.environ["COS_SECRET_ID"],
        SecretKey=os.environ["COS_SECRET_KEY"],
        Scheme="https",
    )
    return CosS3Client(config)


def get_remote_manifest(client) -> dict | None:
    try:
        resp = client.get_object(
            Bucket=os.environ["COS_BUCKET"],
            Key=COS_MANIFEST_KEY,
        )
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body)
    except Exception:
        return None


def get_local_manifest() -> dict | None:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return None


def download_and_extract(client, remote_manifest: dict, force: bool = False):
    """下载压缩包并解压"""
    bucket = os.environ["COS_BUCKET"]
    local_manifest = get_local_manifest()

    # 检查是否需要更新
    if not force and local_manifest:
        if local_manifest.get("md5") == remote_manifest.get("md5"):
            print("✅ 本地已是最新版本，无需更新")
            print(f"   版本: {local_manifest.get('version')}")
            print(f"   大小: {local_manifest.get('compressed_size') / 1024 / 1024:.0f} MB")
            return
        print(f"🔄 检测到远端有新版本")
        print(f"   本地: {local_manifest.get('version', 'unknown')}")
        print(f"   远端: {remote_manifest.get('version')}")

    # 准备目录
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    compressed_path = DATA_DIR / "fund_data.db.tar.gz"

    # 下载
    compressed_size = remote_manifest.get("compressed_size", 0)
    print(f"⬇️  下载中... ({compressed_size / 1024 / 1024:.0f} MB)")
    start = time.time()

    client.download_file(
        Bucket=bucket,
        Key=COS_DB_KEY,
        DestFilePath=str(compressed_path),
    )

    elapsed = time.time() - start
    speed = compressed_size / 1024 / 1024 / elapsed if elapsed > 0 else 0
    print(f"   下载完成，耗时 {elapsed:.1f} 秒 ({speed:.1f} MB/s)")

    # 验证大小
    actual_size = compressed_path.stat().st_size
    if actual_size != compressed_size:
        print(f"⚠️  文件大小不匹配: 预期 {compressed_size}, 实际 {actual_size}")

    # 解压
    print(f"📂 解压中...")
    start = time.time()

    # 先备份旧 DB
    if DB_PATH.exists():
        backup_path = DB_PATH.with_suffix(".db.bak")
        shutil.copy2(DB_PATH, backup_path)
        print(f"   已备份旧 DB → {backup_path.name}")

    result = subprocess.run(
        ["tar", "-xzf", str(compressed_path)],
        cwd=str(DATA_DIR),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"❌ 解压失败: {result.stderr}")
        sys.exit(1)

    elapsed = time.time() - start
    print(f"   解压完成，耗时 {elapsed:.1f} 秒")

    # 验证 DB
    if DB_PATH.exists():
        db_size = DB_PATH.stat().st_size
        print(f"   DB 大小: {db_size / 1024 / 1024 / 1024:.2f} GB")
    else:
        print(f"❌ 解压后 DB 文件不存在: {DB_PATH}")
        sys.exit(1)

    # 清理压缩包
    compressed_path.unlink()
    print(f"   已清理压缩包")

    # 保存 manifest
    MANIFEST_PATH.write_text(json.dumps(remote_manifest, indent=2, ensure_ascii=False))

    print(f"✅ 同步完成！版本: {remote_manifest.get('version')}")


def check_update(client):
    """仅检查是否有更新"""
    remote = get_remote_manifest(client)
    local = get_local_manifest()

    if not remote:
        print("❌ COS 上未找到 DB 文件，请先运行 upload_to_cos.py")
        return

    print(f"☁️  远端版本: {remote.get('version')}")
    print(f"   日期: {remote.get('date')}")
    print(f"   大小: {remote.get('compressed_size') / 1024 / 1024:.0f} MB")

    if local:
        print(f"📂 本地版本: {local.get('version')}")
        if local.get("md5") == remote.get("md5"):
            print("✅ 已是最新，无需更新")
        else:
            print("🔄 有新版本可用，运行以下命令更新：")
            print("   python3 scripts/sync_from_cos.py")
    else:
        print("📂 本地无 DB，需要首次同步：")
        print("   python3 scripts/sync_from_cos.py")


def main():
    parser = argparse.ArgumentParser(description="从 COS 同步 DB")
    parser.add_argument("--force", action="store_true", help="强制更新")
    parser.add_argument("--check", action="store_true", help="仅检查是否有更新")
    args = parser.parse_args()

    _check_env()
    client = _get_client()

    if args.check:
        check_update(client)
    else:
        remote = get_remote_manifest(client)
        if not remote:
            print("❌ COS 上未找到 DB 文件，请先运行 upload_to_cos.py")
            sys.exit(1)
        download_and_extract(client, remote, force=args.force)


if __name__ == "__main__":
    main()
