import asyncpg
import config
from datetime import datetime, timezone, timedelta


# Malaysian Time
MYT = timezone(timedelta(hours=8))


class StatsService:
    """Queries PostgreSQL for server analytics stats."""

    def __init__(self):
        self.pool = None

    async def connect(self):
        """Create a connection pool."""
        self.pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=1,
            max_size=3,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def get_server_stats(self, guild_id):
        """Get a comprehensive server activity summary."""
        if not self.pool:
            return None

        stats = {}

        # Total messages (all time)
        row = await self.pool.fetchrow(
            "SELECT count(*) as total FROM fact_messages WHERE guild_id = $1 AND event_type = 'send'",
            guild_id,
        )
        stats["total_messages"] = row["total"] if row else 0

        # Messages today
        today_start = datetime.now(MYT).replace(hour=0, minute=0, second=0, microsecond=0)
        row = await self.pool.fetchrow(
            "SELECT count(*) as total FROM fact_messages WHERE guild_id = $1 AND event_type = 'send' AND created_at >= $2",
            guild_id, today_start,
        )
        stats["messages_today"] = row["total"] if row else 0

        # Messages this week
        week_start = today_start - timedelta(days=today_start.weekday())
        row = await self.pool.fetchrow(
            "SELECT count(*) as total FROM fact_messages WHERE guild_id = $1 AND event_type = 'send' AND created_at >= $2",
            guild_id, week_start,
        )
        stats["messages_week"] = row["total"] if row else 0

        # Top 5 most active users (this week)
        rows = await self.pool.fetch(
            """
            SELECT u.username, count(*) as msg_count
            FROM fact_messages m
            JOIN dim_users u ON m.user_id = u.user_id
            WHERE m.guild_id = $1 AND m.event_type = 'send' AND m.created_at >= $2
              AND u.is_bot = FALSE
            GROUP BY u.username
            ORDER BY msg_count DESC
            LIMIT 5
            """,
            guild_id, week_start,
        )
        stats["top_users"] = [(r["username"], r["msg_count"]) for r in rows]

        # Top 5 most active channels (this week)
        rows = await self.pool.fetch(
            """
            SELECT c.channel_name, count(*) as msg_count
            FROM fact_messages m
            JOIN dim_channels c ON m.channel_id = c.channel_id
            WHERE m.guild_id = $1 AND m.event_type = 'send' AND m.created_at >= $2
            GROUP BY c.channel_name
            ORDER BY msg_count DESC
            LIMIT 5
            """,
            guild_id, week_start,
        )
        stats["top_channels"] = [(r["channel_name"], r["msg_count"]) for r in rows]

        # Voice sessions (this week)
        row = await self.pool.fetchrow(
            """
            SELECT count(*) as total
            FROM fact_voice_events
            WHERE guild_id = $1 AND event_type = 'join' AND created_at >= $2
            """,
            guild_id, week_start,
        )
        stats["voice_joins_week"] = row["total"] if row else 0

        # Most played games (this week)
        rows = await self.pool.fetch(
            """
            SELECT activity_name, count(*) as play_count
            FROM fact_presence
            WHERE guild_id = $1 AND activity_type = 'playing' AND created_at >= $2
              AND activity_name IS NOT NULL
            GROUP BY activity_name
            ORDER BY play_count DESC
            LIMIT 5
            """,
            guild_id, week_start,
        )
        stats["top_games"] = [(r["activity_name"], r["play_count"]) for r in rows]

        # Total unique active users (this week)
        row = await self.pool.fetchrow(
            """
            SELECT count(DISTINCT user_id) as total
            FROM fact_messages
            WHERE guild_id = $1 AND event_type = 'send' AND created_at >= $2
            """,
            guild_id, week_start,
        )
        stats["active_users_week"] = row["total"] if row else 0

        return stats
