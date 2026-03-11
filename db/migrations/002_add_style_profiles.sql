-- Style profiles for user mimicry
-- Stores LLM-generated style summaries per user

CREATE TABLE IF NOT EXISTS user_style_profiles (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    guild_id    BIGINT NOT NULL,
    profile     TEXT NOT NULL,          -- LLM-generated style description
    sample_size INT DEFAULT 0,          -- Number of messages analyzed
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, guild_id)
);
