-- Casino / gambling tables
-- One shared bank per guild + per-bet transaction log

CREATE TABLE IF NOT EXISTS gambling_bank (
    guild_id          BIGINT PRIMARY KEY,
    bank              BIGINT NOT NULL,        -- current shared funds
    day_start_bank    BIGINT NOT NULL,        -- bank snapshot at midnight; caps max bet
    current_debt      BIGINT NOT NULL,        -- debt due at the next midnight settle
    day_number        INTEGER NOT NULL,       -- day count in the current run (resets on bust)
    streak_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gambling_transactions (
    id         BIGSERIAL PRIMARY KEY,
    guild_id   BIGINT NOT NULL,
    user_id    BIGINT NOT NULL,
    user_name  VARCHAR(100),
    game       VARCHAR(20) NOT NULL,   -- slots, dice, blackjack, wheel, horses
    bet        BIGINT NOT NULL,
    payout     BIGINT NOT NULL,        -- 0 = lose, bet = refund, >bet = win
    net        BIGINT NOT NULL,        -- payout - bet
    metadata   JSONB,                  -- per-game result details
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gambling_tx_guild_date ON gambling_transactions(guild_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gambling_tx_user       ON gambling_transactions(user_id, created_at DESC);
