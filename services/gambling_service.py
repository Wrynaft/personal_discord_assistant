import asyncpg
import json
import config
from datetime import datetime, timezone, timedelta

MYT = timezone(timedelta(hours=8))


class GamblingService:
    """Manages the shared casino bank, daily debt, and per-game transactions."""

    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=1,
            max_size=3,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def get_or_create_bank(self, guild_id):
        """Return the bank state for a guild. Seeds a fresh run on first call."""
        if not self.pool:
            return None
        row = await self.pool.fetchrow(
            "SELECT * FROM gambling_bank WHERE guild_id = $1", guild_id,
        )
        if row:
            return dict(row)
        await self.pool.execute(
            """
            INSERT INTO gambling_bank
                (guild_id, bank, day_start_bank, current_debt, day_number)
            VALUES ($1, $2, $2, $3, 1)
            ON CONFLICT (guild_id) DO NOTHING
            """,
            guild_id, config.GAMBLING_SEED_BANK, config.GAMBLING_BASE_DEBT,
        )
        row = await self.pool.fetchrow(
            "SELECT * FROM gambling_bank WHERE guild_id = $1", guild_id,
        )
        return dict(row) if row else None

    async def settle_day(self, guild_id):
        """Run the daily quota check. Returns a dict describing what happened."""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT * FROM gambling_bank WHERE guild_id = $1 FOR UPDATE",
                    guild_id,
                )
                if not row:
                    return None

                bank = row["bank"]
                debt = row["current_debt"]
                day = row["day_number"]

                if bank >= debt:
                    new_bank = bank - debt
                    new_day = day + 1
                    new_debt = int(round(
                        config.GAMBLING_BASE_DEBT
                        * (config.GAMBLING_DEBT_MULTIPLIER ** (new_day - 1))
                    ))
                    await conn.execute(
                        """
                        UPDATE gambling_bank
                        SET bank = $2, day_start_bank = $2,
                            current_debt = $3, day_number = $4,
                            updated_at = NOW()
                        WHERE guild_id = $1
                        """,
                        guild_id, new_bank, new_debt, new_day,
                    )
                    return {
                        "outcome": "survived",
                        "old_day": day,
                        "new_day": new_day,
                        "debt_paid": debt,
                        "carryover": new_bank,
                        "next_debt": new_debt,
                    }

                await conn.execute(
                    """
                    UPDATE gambling_bank
                    SET bank = $2, day_start_bank = $2,
                        current_debt = $3, day_number = 1,
                        streak_started_at = NOW(),
                        updated_at = NOW()
                    WHERE guild_id = $1
                    """,
                    guild_id,
                    config.GAMBLING_SEED_BANK,
                    config.GAMBLING_BASE_DEBT,
                )
                return {
                    "outcome": "reset",
                    "old_day": day,
                    "missing": debt - bank,
                    "seed": config.GAMBLING_SEED_BANK,
                }

    async def apply_bet(self, guild_id, user_id, user_name, game, bet, payout, metadata=None):
        """Atomically deduct bet, credit payout, and log the transaction.

        Returns: {"new_bank": int, "net": int} on success,
                 {"error": "insufficient_funds" | "exceeds_cap", ...} on failure.
        """
        if not self.pool:
            return None
        net = payout - bet
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT bank, day_start_bank FROM gambling_bank
                    WHERE guild_id = $1 FOR UPDATE
                    """,
                    guild_id,
                )
                if not row:
                    return None
                bank = row["bank"]
                day_start = row["day_start_bank"]

                if bet > bank:
                    return {"error": "insufficient_funds", "bank": bank, "bet": bet}
                if bet > day_start:
                    return {"error": "exceeds_cap", "cap": day_start, "bet": bet}

                new_bank = bank + net
                await conn.execute(
                    "UPDATE gambling_bank SET bank = $2, updated_at = NOW() WHERE guild_id = $1",
                    guild_id, new_bank,
                )
                await conn.execute(
                    """
                    INSERT INTO gambling_transactions
                        (guild_id, user_id, user_name, game, bet, payout, net, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                    """,
                    guild_id, user_id, user_name, game,
                    bet, payout, net,
                    json.dumps(metadata) if metadata else None,
                )
                return {"new_bank": new_bank, "net": net}

    def compute_bet_options(self, day_start_bank, current_bank, game=None):
        """Compute the three bet button amounts and whether each is affordable.

        If `game` is provided, the cap is shrunk by config.GAMBLING_GAME_CAPS[game]
        (defaults to 1.0). Mirrors the original game's per-table bet caps.
        """
        cap_ratio = config.GAMBLING_GAME_CAPS.get(game, 1.0) if game else 1.0
        cap = int(day_start_bank * cap_ratio)
        quarter = cap // 4
        half = cap // 2
        full = cap
        return {
            "quarter": {"amount": quarter, "enabled": current_bank >= quarter > 0},
            "half":    {"amount": half,    "enabled": current_bank >= half    > 0},
            "max":     {"amount": full,    "enabled": current_bank >= full    > 0},
            "cap":     cap,
            "cap_ratio": cap_ratio,
        }
