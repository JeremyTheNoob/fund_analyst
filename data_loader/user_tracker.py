"""
user_tracker.py — 匿名访客使用量追踪 + 反馈收集

用匿名 visitor_id 追踪每位用户的使用次数（Supabase），实现内测次数限制。

技术方案：
- visitor_id 通过 st.context.cookies 持久化（Streamlit 1.37+）
- 首次访问自动生成 UUID v4 并写入 cookie
- 使用次数和反馈数据存 Supabase 表

需要的 Supabase 表：

1. visitor_usage（使用量追踪）
   - visitor_id: text (PK)
   - usage_count: int (default 0)
   - last_used_at: timestamptz
   - first_seen_at: timestamptz

2. beta_feedback（内测反馈）
   - id: bigint (PK, auto)
   - visitor_id: text
   - submitted_at: timestamptz
   - q1_experience: text     -- 投资经验
   - q2_channels: text       -- 了解渠道（JSON array）
   - q3_features: text       -- 常用功能（JSON array）
   - q4_valuable: text       -- 有价值指标（JSON array）
   - q5_complexity: text     -- 报告复杂度评价
   - q6_pricing: text        -- 付费意愿
   - q7_open_feedback: text  -- 开放反馈
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# 内测限制
BETA_MAX_USAGE = 5
BETA_DISABLED = False  # 设为 True 可临时关闭限制


# ============================================================
# Visitor ID 管理
# ============================================================

def get_visitor_id() -> str:
    """
    获取或创建匿名访客 ID。

    优先从 cookie 读取，不存在则生成新 UUID 并写入 cookie。
    回退到 session_state（cookie 写入失败时）。
    """
    import streamlit as st

    # 优先：cookie
    try:
        cookie_key = "fund_analyst_vid"
        cookies = st.context.cookies
        if cookies and cookie_key in cookies:
            vid = cookies[cookie_key]
            if vid and len(vid) >= 16:
                return vid
    except Exception:
        pass

    # 回退：session_state
    if "visitor_id" in st.session_state:
        return st.session_state["visitor_id"]

    # 生成新 ID
    vid = uuid.uuid4().hex[:16]

    # 写入 cookie
    try:
        st.context.cookies[cookie_key] = vid
    except Exception:
        pass

    st.session_state["visitor_id"] = vid
    return vid


# ============================================================
# 使用量追踪
# ============================================================

def get_usage_count(visitor_id: str) -> int:
    """获取用户当前使用次数"""
    try:
        from data_loader.cache_layer import _get_client
        client = _get_client()
        if client is None:
            return 0

        resp = (
            client.table("visitor_usage")
            .select("usage_count")
            .eq("visitor_id", visitor_id)
            .maybe_single()
            .execute()
        )

        if resp and resp.data:
            return resp.data.get("usage_count", 0)
        return 0
    except Exception as e:
        logger.warning(f"[usage] 查询失败: {e}")
        return 0


def increment_usage(visitor_id: str) -> bool:
    """
    使用次数 +1。

    Returns:
        是否成功
    """
    try:
        from data_loader.cache_layer import _get_client
        client = _get_client()
        if client is None:
            return True  # Supabase 不可用时放行

        now = datetime.now(timezone.utc).isoformat()

        # 先查是否存在
        existing = get_usage_count(visitor_id)
        if existing > 0:
            # 更新
            client.table("visitor_usage").update({
                "usage_count": existing + 1,
                "last_used_at": now,
            }).eq("visitor_id", visitor_id).execute()
        else:
            # 新记录
            client.table("visitor_usage").insert({
                "visitor_id": visitor_id,
                "usage_count": 1,
                "first_seen_at": now,
                "last_used_at": now,
            }).execute()

        return True
    except Exception as e:
        logger.warning(f"[usage] 更新失败: {e}")
        return True  # 失败时放行


def check_usage_limit(visitor_id: str) -> tuple[bool, int]:
    """
    检查用户是否还能继续使用。

    Returns:
        (can_use, remaining): 是否可用, 剩余次数
    """
    if BETA_DISABLED:
        return True, 999

    count = get_usage_count(visitor_id)
    remaining = max(0, BETA_MAX_USAGE - count)
    return remaining > 0, remaining


# ============================================================
# 反馈收集
# ============================================================

def submit_feedback(
    visitor_id: str,
    q1_experience: str,
    q2_channels: list,
    q3_features: list,
    q4_valuable: list,
    q5_complexity: str,
    q6_pricing: str,
    q7_open_feedback: str = "",
) -> bool:
    """
    提交内测反馈到 Supabase。

    Returns:
        是否成功
    """
    try:
        from data_loader.cache_layer import _get_client
        client = _get_client()
        if client is None:
            return False

        now = datetime.now(timezone.utc).isoformat()

        client.table("beta_feedback").insert({
            "visitor_id": visitor_id,
            "submitted_at": now,
            "q1_experience": q1_experience,
            "q2_channels": json.dumps(q2_channels, ensure_ascii=False),
            "q3_features": json.dumps(q3_features, ensure_ascii=False),
            "q4_valuable": json.dumps(q4_valuable, ensure_ascii=False),
            "q5_complexity": q5_complexity,
            "q6_pricing": q6_pricing,
            "q7_open_feedback": q7_open_feedback.strip() if q7_open_feedback else None,
        }).execute()

        logger.info(f"[feedback] 反馈提交成功: {visitor_id}")
        return True
    except Exception as e:
        logger.warning(f"[feedback] 提交失败: {e}")
        return False
