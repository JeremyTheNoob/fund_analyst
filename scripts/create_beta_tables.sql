-- ============================================================
-- Phase 3: 内测使用量 + 反馈收集
-- 在 Supabase SQL Editor 中执行
-- ============================================================

-- 1. visitor_usage 表（匿名访客使用量追踪）
CREATE TABLE IF NOT EXISTS visitor_usage (
    visitor_id TEXT PRIMARY KEY,
    usage_count INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 索引：按最后使用时间查询
CREATE INDEX IF NOT EXISTS idx_visitor_usage_last_used ON visitor_usage(last_used_at);

-- RLS 策略：允许匿名用户读写自己的记录
ALTER TABLE visitor_usage ENABLE ROW LEVEL SECURITY;

CREATE POLICY "visitors_can_read_own"
    ON visitor_usage FOR SELECT
    USING (true);

CREATE POLICY "visitors_can_insert_own"
    ON visitor_usage FOR INSERT
    WITH CHECK (true);

CREATE POLICY "visitors_can_update_own"
    ON visitor_usage FOR UPDATE
    USING (true);


-- 2. beta_feedback 表（内测反馈）
CREATE TABLE IF NOT EXISTS beta_feedback (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    visitor_id TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    q1_experience TEXT,          -- 投资经验
    q2_channels TEXT,             -- 了解渠道（JSON array）
    q3_features TEXT,             -- 常用功能（JSON array）
    q4_valuable TEXT,             -- 有价值指标（JSON array）
    q5_complexity TEXT,           -- 报告复杂度评价
    q6_pricing TEXT,              -- 付费意愿
    q7_open_feedback TEXT         -- 开放反馈
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_beta_feedback_visitor ON beta_feedback(visitor_id);
CREATE INDEX IF NOT EXISTS idx_beta_feedback_submitted ON beta_feedback(submitted_at);

-- RLS 策略
ALTER TABLE beta_feedback ENABLE ROW LEVEL SECURITY;

CREATE POLICY "visitors_can_insert_feedback"
    ON beta_feedback FOR INSERT
    WITH CHECK (true);

CREATE POLICY "visitors_can_read_feedback"
    ON beta_feedback FOR SELECT
    USING (true);
