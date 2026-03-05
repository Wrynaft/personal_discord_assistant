-- =============================================
-- Discord Analytics — Dimensional Schema
-- =============================================

-- Dimension: Users
CREATE TABLE IF NOT EXISTS dim_users (
    user_id     BIGINT PRIMARY KEY,
    username    VARCHAR(100),
    display_name VARCHAR(100),
    is_bot      BOOLEAN DEFAULT FALSE,
    first_seen  TIMESTAMPTZ DEFAULT NOW(),
    last_seen   TIMESTAMPTZ DEFAULT NOW()
);

-- Dimension: Channels
CREATE TABLE IF NOT EXISTS dim_channels (
    channel_id   BIGINT PRIMARY KEY,
    channel_name VARCHAR(200),
    channel_type VARCHAR(50),   -- text, voice, category, forum, etc.
    category     VARCHAR(200),
    guild_id     BIGINT
);

-- Dimension: Guilds
CREATE TABLE IF NOT EXISTS dim_guilds (
    guild_id   BIGINT PRIMARY KEY,
    guild_name VARCHAR(200),
    member_count INT DEFAULT 0,
    first_seen TIMESTAMPTZ DEFAULT NOW()
);

-- Fact: Messages
CREATE TABLE IF NOT EXISTS fact_messages (
    id              BIGSERIAL PRIMARY KEY,
    message_id      BIGINT NOT NULL,
    user_id         BIGINT REFERENCES dim_users(user_id),
    channel_id      BIGINT REFERENCES dim_channels(channel_id),
    guild_id        BIGINT REFERENCES dim_guilds(guild_id),
    content_length  INT DEFAULT 0,
    word_count      INT DEFAULT 0,
    has_attachment  BOOLEAN DEFAULT FALSE,
    has_embed       BOOLEAN DEFAULT FALSE,
    event_type      VARCHAR(20) DEFAULT 'send',  -- send, edit, delete
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Fact: Voice Events
CREATE TABLE IF NOT EXISTS fact_voice_events (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT REFERENCES dim_users(user_id),
    channel_id  BIGINT,   -- nullable for disconnect events
    guild_id    BIGINT REFERENCES dim_guilds(guild_id),
    event_type  VARCHAR(20) NOT NULL,  -- join, leave, move, mute, unmute, deafen, undeafen
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Fact: Reactions
CREATE TABLE IF NOT EXISTS fact_reactions (
    id          BIGSERIAL PRIMARY KEY,
    message_id  BIGINT NOT NULL,
    user_id     BIGINT REFERENCES dim_users(user_id),
    channel_id  BIGINT,
    guild_id    BIGINT,
    emoji       VARCHAR(100),
    event_type  VARCHAR(10) NOT NULL,  -- add, remove
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Fact: Presence / Activity
CREATE TABLE IF NOT EXISTS fact_presence (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT REFERENCES dim_users(user_id),
    activity_type   VARCHAR(50),   -- playing, streaming, listening, watching, competing
    activity_name   VARCHAR(200),  -- game or app name
    guild_id        BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- Indexes for common queries
-- =============================================
CREATE INDEX IF NOT EXISTS idx_messages_user      ON fact_messages(user_id);
CREATE INDEX IF NOT EXISTS idx_messages_channel   ON fact_messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_time      ON fact_messages(created_at);
CREATE INDEX IF NOT EXISTS idx_voice_user         ON fact_voice_events(user_id);
CREATE INDEX IF NOT EXISTS idx_voice_time         ON fact_voice_events(created_at);
CREATE INDEX IF NOT EXISTS idx_reactions_message   ON fact_reactions(message_id);
CREATE INDEX IF NOT EXISTS idx_presence_user       ON fact_presence(user_id);
CREATE INDEX IF NOT EXISTS idx_presence_activity   ON fact_presence(activity_name);
