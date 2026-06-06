-- Casino items system
-- Tickets currency + per-guild shared inventory + active passive/active effects

ALTER TABLE gambling_bank
    ADD COLUMN IF NOT EXISTS tickets INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS gambling_inventory (
    guild_id  BIGINT NOT NULL,
    item_id   VARCHAR(30) NOT NULL,
    count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (guild_id, item_id)
);

CREATE TABLE IF NOT EXISTS gambling_active_effects (
    id            BIGSERIAL PRIMARY KEY,
    guild_id      BIGINT NOT NULL,
    item_id       VARCHAR(30) NOT NULL,
    charges_left  INTEGER NOT NULL DEFAULT 1,
    activated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_by  BIGINT
);

CREATE INDEX IF NOT EXISTS idx_active_effects_guild ON gambling_active_effects(guild_id);
