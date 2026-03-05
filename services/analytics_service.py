import asyncpg
import config


class AnalyticsService:
    """Async PostgreSQL event logging for Discord analytics."""

    def __init__(self):
        self.pool = None

    async def connect(self):
        """Create a connection pool to PostgreSQL."""
        self.pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=2,
            max_size=10,
        )
        print("Analytics: Connected to PostgreSQL")

    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()

    # ── Dimension Upserts ──────────────────────────────────

    async def upsert_user(self, user):
        """Insert or update a user dimension record."""
        if not self.pool:
            return
        await self.pool.execute(
            """
            INSERT INTO dim_users (user_id, username, display_name, is_bot)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                display_name = EXCLUDED.display_name,
                last_seen = NOW()
            """,
            user.id, str(user), user.display_name, user.bot,
        )

    async def upsert_channel(self, channel):
        """Insert or update a channel dimension record."""
        if not self.pool:
            return
        channel_type = str(channel.type) if hasattr(channel, 'type') else 'unknown'
        category = channel.category.name if hasattr(channel, 'category') and channel.category else None
        guild_id = channel.guild.id if hasattr(channel, 'guild') and channel.guild else None
        await self.pool.execute(
            """
            INSERT INTO dim_channels (channel_id, channel_name, channel_type, category, guild_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name,
                channel_type = EXCLUDED.channel_type,
                category = EXCLUDED.category
            """,
            channel.id, channel.name, channel_type, category, guild_id,
        )

    async def upsert_guild(self, guild):
        """Insert or update a guild dimension record."""
        if not self.pool:
            return
        await self.pool.execute(
            """
            INSERT INTO dim_guilds (guild_id, guild_name, member_count)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                guild_name = EXCLUDED.guild_name,
                member_count = EXCLUDED.member_count
            """,
            guild.id, guild.name, guild.member_count,
        )

    # ── Fact Logging ───────────────────────────────────────

    async def log_message(self, message, event_type='send'):
        """Log a message event to fact_messages."""
        if not self.pool or not message.guild:
            return

        # Upsert dimensions
        await self.upsert_user(message.author)
        await self.upsert_channel(message.channel)
        await self.upsert_guild(message.guild)

        content = message.content or ''
        await self.pool.execute(
            """
            INSERT INTO fact_messages
                (message_id, user_id, channel_id, guild_id, content_length, word_count,
                 has_attachment, has_embed, event_type, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            message.id,
            message.author.id,
            message.channel.id,
            message.guild.id,
            len(content),
            len(content.split()) if content else 0,
            bool(message.attachments),
            bool(message.embeds),
            event_type,
            message.created_at,
        )

    async def log_voice_event(self, member, channel, guild, event_type):
        """Log a voice state change event."""
        if not self.pool:
            return

        await self.upsert_user(member)
        if guild:
            await self.upsert_guild(guild)
        if channel:
            await self.upsert_channel(channel)

        await self.pool.execute(
            """
            INSERT INTO fact_voice_events (user_id, channel_id, guild_id, event_type)
            VALUES ($1, $2, $3, $4)
            """,
            member.id,
            channel.id if channel else None,
            guild.id if guild else None,
            event_type,
        )

    async def log_reaction(self, reaction, user, event_type):
        """Log a reaction add/remove event."""
        if not self.pool or not reaction.message.guild:
            return

        await self.upsert_user(user)

        await self.pool.execute(
            """
            INSERT INTO fact_reactions (message_id, user_id, channel_id, guild_id, emoji, event_type)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            reaction.message.id,
            user.id,
            reaction.message.channel.id,
            reaction.message.guild.id,
            str(reaction.emoji),
            event_type,
        )

    async def log_presence(self, member, activity):
        """Log a presence/activity change event."""
        if not self.pool:
            return

        await self.upsert_user(member)
        guild_id = member.guild.id if hasattr(member, 'guild') and member.guild else None

        activity_type = str(activity.type).replace('ActivityType.', '') if activity else None
        activity_name = activity.name if activity else None

        await self.pool.execute(
            """
            INSERT INTO fact_presence (user_id, activity_type, activity_name, guild_id)
            VALUES ($1, $2, $3, $4)
            """,
            member.id,
            activity_type,
            activity_name,
            guild_id,
        )
