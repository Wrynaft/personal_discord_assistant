import asyncpg
import json
import config
from datetime import datetime, timezone, timedelta
from services import casino_items

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
                    ticket_bonus = casino_items.TICKETS_PER_DAY_CLEARED
                    new_tickets = (row["tickets"] or 0) + ticket_bonus
                    await conn.execute(
                        """
                        UPDATE gambling_bank
                        SET bank = $2, day_start_bank = $2,
                            current_debt = $3, day_number = $4,
                            tickets = $5,
                            updated_at = NOW()
                        WHERE guild_id = $1
                        """,
                        guild_id, new_bank, new_debt, new_day, new_tickets,
                    )
                    return {
                        "outcome": "survived",
                        "old_day": day,
                        "new_day": new_day,
                        "bank_before": bank,
                        "debt_paid": debt,
                        "carryover": new_bank,
                        "next_debt": new_debt,
                        "streak_started_at": row["streak_started_at"],
                        "tickets_awarded": ticket_bonus,
                        "tickets_after": new_tickets,
                    }

                # Bust: reset bank state AND wipe tickets/inventory/active effects.
                await conn.execute(
                    """
                    UPDATE gambling_bank
                    SET bank = $2, day_start_bank = $2,
                        current_debt = $3, day_number = 1,
                        tickets = 0,
                        streak_started_at = NOW(),
                        updated_at = NOW()
                    WHERE guild_id = $1
                    """,
                    guild_id,
                    config.GAMBLING_SEED_BANK,
                    config.GAMBLING_BASE_DEBT,
                )
                await conn.execute(
                    "DELETE FROM gambling_inventory WHERE guild_id = $1", guild_id,
                )
                await conn.execute(
                    "DELETE FROM gambling_active_effects WHERE guild_id = $1", guild_id,
                )
                return {
                    "outcome": "reset",
                    "old_day": day,
                    "bank_before": bank,
                    "debt_owed": debt,
                    "missing": debt - bank,
                    "seed": config.GAMBLING_SEED_BANK,
                    "streak_started_at": row["streak_started_at"],
                    "tickets_lost": row["tickets"] or 0,
                }

    async def apply_bet(self, guild_id, user_id, user_name, game, bet, payout, metadata=None):
        """Atomically deduct bet, credit payout, apply active passive effects, and log.

        Returns on success: {
            "new_bank": int, "net": int, "payout": int (after effects),
            "effects_applied": list[dict], "ticket_bonus": int, "tickets": int
        }
        On failure: {"error": "insufficient_funds" | "exceeds_cap", ...}
        """
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT bank, day_start_bank, tickets FROM gambling_bank
                    WHERE guild_id = $1 FOR UPDATE
                    """,
                    guild_id,
                )
                if not row:
                    return None
                bank = row["bank"]
                day_start = row["day_start_bank"]
                tickets = row["tickets"] or 0

                if bet > bank:
                    return {"error": "insufficient_funds", "bank": bank, "bet": bet}
                if bet > day_start:
                    return {"error": "exceeds_cap", "cap": day_start, "bet": bet}

                # Fetch active passive effects (oldest first — FIFO consumption)
                effect_rows = await conn.fetch(
                    """
                    SELECT id, item_id, charges_left FROM gambling_active_effects
                    WHERE guild_id = $1
                    ORDER BY activated_at ASC
                    """,
                    guild_id,
                )
                effects = [dict(e) for e in effect_rows]

                final_payout, applied, consumed_ids = casino_items.apply_passive_effects(
                    bet, payout, effects,
                )

                # Decrement charges on consumed effects; delete exhausted ones
                for eff_id in consumed_ids:
                    await conn.execute(
                        "UPDATE gambling_active_effects SET charges_left = charges_left - 1 WHERE id = $1",
                        eff_id,
                    )
                await conn.execute(
                    "DELETE FROM gambling_active_effects WHERE guild_id = $1 AND charges_left <= 0",
                    guild_id,
                )

                net = final_payout - bet
                new_bank = bank + net

                # Jackpot ticket bonus: any win with payout >= 10x the bet
                ticket_bonus = 0
                if bet > 0 and payout >= bet * 10:
                    ticket_bonus = casino_items.TICKETS_PER_JACKPOT
                    tickets += ticket_bonus

                await conn.execute(
                    """
                    UPDATE gambling_bank
                    SET bank = $2, tickets = $3, updated_at = NOW()
                    WHERE guild_id = $1
                    """,
                    guild_id, new_bank, tickets,
                )

                tx_metadata = dict(metadata or {})
                if applied:
                    tx_metadata["effects_applied"] = [
                        {"item": a["item_id"], "delta": a["delta"]} for a in applied
                    ]
                if ticket_bonus:
                    tx_metadata["ticket_bonus"] = ticket_bonus

                await conn.execute(
                    """
                    INSERT INTO gambling_transactions
                        (guild_id, user_id, user_name, game, bet, payout, net, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                    """,
                    guild_id, user_id, user_name, game,
                    bet, final_payout, net,
                    json.dumps(tx_metadata),
                )
                return {
                    "new_bank": new_bank,
                    "net": net,
                    "payout": final_payout,
                    "effects_applied": applied,
                    "ticket_bonus": ticket_bonus,
                    "tickets": tickets,
                }

    async def get_player_stats(self, guild_id, start_time, end_time=None):
        """Aggregate per-player stats for a time window. Returns list sorted by net DESC.

        Each row: user_name, net_total, bet_count, total_wagered, wins, losses
        end_time=None means "up to now".
        """
        if not self.pool:
            return []
        if end_time is None:
            query = """
                SELECT user_name,
                       sum(net)                       AS net_total,
                       count(*)                       AS bet_count,
                       sum(bet)                       AS total_wagered,
                       count(*) FILTER (WHERE net > 0) AS wins,
                       count(*) FILTER (WHERE net < 0) AS losses
                FROM gambling_transactions
                WHERE guild_id = $1 AND created_at >= $2
                GROUP BY user_name
                ORDER BY net_total DESC
            """
            rows = await self.pool.fetch(query, guild_id, start_time)
        else:
            query = """
                SELECT user_name,
                       sum(net)                       AS net_total,
                       count(*)                       AS bet_count,
                       sum(bet)                       AS total_wagered,
                       count(*) FILTER (WHERE net > 0) AS wins,
                       count(*) FILTER (WHERE net < 0) AS losses
                FROM gambling_transactions
                WHERE guild_id = $1 AND created_at >= $2 AND created_at < $3
                GROUP BY user_name
                ORDER BY net_total DESC
            """
            rows = await self.pool.fetch(query, guild_id, start_time, end_time)
        return [dict(r) for r in rows]

    async def get_user_profile(self, guild_id, user_id, streak_started_at):
        """Aggregate lifetime + current-run stats for one user."""
        if not self.pool:
            return None
        lifetime = await self.pool.fetchrow(
            """
            SELECT
                count(*)                       AS bet_count,
                coalesce(sum(bet), 0)          AS total_wagered,
                coalesce(sum(net), 0)          AS net_total,
                coalesce(max(net), 0)          AS biggest_win,
                coalesce(min(net), 0)          AS biggest_loss,
                count(*) FILTER (WHERE net > 0) AS wins,
                count(*) FILTER (WHERE net < 0) AS losses
            FROM gambling_transactions
            WHERE guild_id = $1 AND user_id = $2
            """,
            guild_id, user_id,
        )
        if not lifetime or lifetime["bet_count"] == 0:
            return None

        big_win_row = await self.pool.fetchrow(
            """
            SELECT game, net, created_at FROM gambling_transactions
            WHERE guild_id = $1 AND user_id = $2 AND net > 0
            ORDER BY net DESC LIMIT 1
            """,
            guild_id, user_id,
        )
        big_loss_row = await self.pool.fetchrow(
            """
            SELECT game, net, created_at FROM gambling_transactions
            WHERE guild_id = $1 AND user_id = $2 AND net < 0
            ORDER BY net ASC LIMIT 1
            """,
            guild_id, user_id,
        )
        fav_row = await self.pool.fetchrow(
            """
            SELECT game, count(*) AS plays FROM gambling_transactions
            WHERE guild_id = $1 AND user_id = $2
            GROUP BY game ORDER BY plays DESC LIMIT 1
            """,
            guild_id, user_id,
        )
        run_row = await self.pool.fetchrow(
            """
            SELECT count(*) AS bet_count, coalesce(sum(net), 0) AS net_total
            FROM gambling_transactions
            WHERE guild_id = $1 AND user_id = $2 AND created_at >= $3
            """,
            guild_id, user_id, streak_started_at,
        )

        return {
            "lifetime": dict(lifetime),
            "biggest_win": dict(big_win_row) if big_win_row else None,
            "biggest_loss": dict(big_loss_row) if big_loss_row else None,
            "favorite": dict(fav_row) if fav_row else None,
            "current_run": dict(run_row) if run_row else {"bet_count": 0, "net_total": 0},
        }

    # ── Items / inventory ─────────────────────────────────────────────

    async def get_inventory(self, guild_id):
        """Return {item_id: count} for items owned by the guild."""
        if not self.pool:
            return {}
        rows = await self.pool.fetch(
            "SELECT item_id, count FROM gambling_inventory WHERE guild_id = $1 AND count > 0",
            guild_id,
        )
        return {r["item_id"]: r["count"] for r in rows}

    async def get_active_effects(self, guild_id):
        """Return list of active effect dicts (item_id, charges_left, activated_at, activated_by)."""
        if not self.pool:
            return []
        rows = await self.pool.fetch(
            """
            SELECT id, item_id, charges_left, activated_at, activated_by
            FROM gambling_active_effects
            WHERE guild_id = $1
            ORDER BY activated_at ASC
            """,
            guild_id,
        )
        return [dict(r) for r in rows]

    async def buy_item(self, guild_id, item_id, cost):
        """Atomically spend tickets and add item to inventory."""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT tickets FROM gambling_bank WHERE guild_id = $1 FOR UPDATE",
                    guild_id,
                )
                if not row:
                    return {"error": "no_bank"}
                if row["tickets"] < cost:
                    return {"error": "insufficient_tickets", "have": row["tickets"], "need": cost}
                await conn.execute(
                    "UPDATE gambling_bank SET tickets = tickets - $2 WHERE guild_id = $1",
                    guild_id, cost,
                )
                await conn.execute(
                    """
                    INSERT INTO gambling_inventory (guild_id, item_id, count)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (guild_id, item_id)
                    DO UPDATE SET count = gambling_inventory.count + 1
                    """,
                    guild_id, item_id,
                )
                return {"tickets_remaining": row["tickets"] - cost}

    async def activate_passive(self, guild_id, item_id, user_id, charges):
        """Move one of `item_id` from inventory into active effects."""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT count FROM gambling_inventory WHERE guild_id = $1 AND item_id = $2 FOR UPDATE",
                    guild_id, item_id,
                )
                if not row or row["count"] <= 0:
                    return {"error": "not_owned"}
                await conn.execute(
                    "UPDATE gambling_inventory SET count = count - 1 WHERE guild_id = $1 AND item_id = $2",
                    guild_id, item_id,
                )
                await conn.execute(
                    """
                    INSERT INTO gambling_active_effects (guild_id, item_id, charges_left, activated_by)
                    VALUES ($1, $2, $3, $4)
                    """,
                    guild_id, item_id, charges, user_id,
                )
                return {"ok": True}

    async def use_quota_gun(self, guild_id, user_id):
        """Consume one quota_gun, reduce current_debt by configured %."""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                inv = await conn.fetchrow(
                    "SELECT count FROM gambling_inventory WHERE guild_id = $1 AND item_id = 'quota_gun' FOR UPDATE",
                    guild_id,
                )
                if not inv or inv["count"] <= 0:
                    return {"error": "not_owned"}
                bank_row = await conn.fetchrow(
                    "SELECT current_debt FROM gambling_bank WHERE guild_id = $1 FOR UPDATE",
                    guild_id,
                )
                if not bank_row:
                    return {"error": "no_bank"}
                debt = bank_row["current_debt"]
                reduction = int(round(debt * casino_items.QUOTA_GUN_PAYOFF_PCT))
                new_debt = max(0, debt - reduction)
                await conn.execute(
                    "UPDATE gambling_inventory SET count = count - 1 WHERE guild_id = $1 AND item_id = 'quota_gun'",
                    guild_id,
                )
                await conn.execute(
                    "UPDATE gambling_bank SET current_debt = $2, updated_at = NOW() WHERE guild_id = $1",
                    guild_id, new_debt,
                )
                return {"old_debt": debt, "new_debt": new_debt, "reduced_by": reduction}

    async def use_time_machine(self, guild_id, user_id):
        """Consume one time_machine and reverse the most recent transaction."""
        if not self.pool:
            return None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                inv = await conn.fetchrow(
                    "SELECT count FROM gambling_inventory WHERE guild_id = $1 AND item_id = 'time_machine' FOR UPDATE",
                    guild_id,
                )
                if not inv or inv["count"] <= 0:
                    return {"error": "not_owned"}
                last_tx = await conn.fetchrow(
                    """
                    SELECT id, net, game, user_name, bet, payout FROM gambling_transactions
                    WHERE guild_id = $1
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    guild_id,
                )
                if not last_tx:
                    return {"error": "no_recent_bet"}
                net = last_tx["net"]
                # Reverse the bank delta and delete the transaction.
                await conn.execute(
                    "UPDATE gambling_bank SET bank = bank - $2, updated_at = NOW() WHERE guild_id = $1",
                    guild_id, net,
                )
                await conn.execute(
                    "DELETE FROM gambling_transactions WHERE id = $1",
                    last_tx["id"],
                )
                await conn.execute(
                    "UPDATE gambling_inventory SET count = count - 1 WHERE guild_id = $1 AND item_id = 'time_machine'",
                    guild_id,
                )
                return {
                    "reversed_game": last_tx["game"],
                    "reversed_user": last_tx["user_name"],
                    "reversed_bet": last_tx["bet"],
                    "reversed_net": net,
                }

    def compute_bet_options(self, day_start_bank, current_bank, game=None):
        """Compute the three bet button amounts and whether each is affordable.

        - ¼ and ½ are fixed fractions of the (cap-adjusted) day-start bank; they
          disable when current_bank can't cover them.
        - MAX is min(cap, current_bank) so it scales down with the bank — always
          usable as long as there's any money to bet.

        If `game` is provided, the cap is shrunk by config.GAMBLING_GAME_CAPS[game]
        (defaults to 1.0). Mirrors the original game's per-table bet caps.
        """
        cap_ratio = config.GAMBLING_GAME_CAPS.get(game, 1.0) if game else 1.0
        cap = int(day_start_bank * cap_ratio)
        quarter = cap // 4
        half = cap // 2
        max_amount = min(cap, current_bank) if current_bank > 0 else 0
        return {
            "quarter": {"amount": quarter,    "enabled": current_bank >= quarter > 0},
            "half":    {"amount": half,       "enabled": current_bank >= half    > 0},
            "max":     {"amount": max_amount, "enabled": max_amount > 0},
            "cap":     cap,
            "cap_ratio": cap_ratio,
        }
