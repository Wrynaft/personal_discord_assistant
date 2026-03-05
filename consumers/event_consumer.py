"""
Kafka Consumer — reads Discord events from Kafka and writes to PostgreSQL.

Run as a separate process:
    python -m consumers.event_consumer

This decouples the Discord bot from database writes, enabling:
- Event replay if the consumer was down
- Independent scaling
- Adding more consumers later (e.g., alerting, ML pipeline)
"""

import sys
import os
import json
import asyncio
from datetime import datetime, timezone

# Add project root to path so we can import config and services
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiokafka import AIOKafkaConsumer
import asyncpg
import config
from services.kafka_producer import TOPIC_MESSAGES, TOPIC_VOICE, TOPIC_REACTIONS, TOPIC_PRESENCE


async def get_db_pool():
    """Create a PostgreSQL connection pool."""
    return await asyncpg.create_pool(
        dsn=config.DATABASE_URL,
        min_size=2,
        max_size=5,
    )


def parse_ts(ts_str):
    """Parse ISO timestamp string to datetime, or return current UTC time."""
    if not ts_str:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


async def upsert_user(pool, event):
    """Upsert user dimension from event data."""
    await pool.execute(
        """
        INSERT INTO dim_users (user_id, username, display_name, is_bot)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            display_name = EXCLUDED.display_name,
            last_seen = NOW()
        """,
        int(event["user_id"]), event.get("username"), event.get("display_name"), event.get("is_bot", False),
    )


async def upsert_channel(pool, event):
    """Upsert channel dimension from event data."""
    channel_id = event.get("channel_id")
    if not channel_id:
        return
    await pool.execute(
        """
        INSERT INTO dim_channels (channel_id, channel_name, channel_type, category, guild_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (channel_id) DO UPDATE SET
            channel_name = EXCLUDED.channel_name,
            channel_type = EXCLUDED.channel_type,
            category = EXCLUDED.category
        """,
        int(channel_id), event.get("channel_name"), event.get("channel_type"), event.get("category"),
        int(event["guild_id"]) if event.get("guild_id") else None,
    )


async def upsert_guild(pool, event):
    """Upsert guild dimension from event data."""
    guild_id = event.get("guild_id")
    if not guild_id:
        return
    await pool.execute(
        """
        INSERT INTO dim_guilds (guild_id, guild_name, member_count)
        VALUES ($1, $2, $3)
        ON CONFLICT (guild_id) DO UPDATE SET
            guild_name = EXCLUDED.guild_name,
            member_count = EXCLUDED.member_count
        """,
        int(guild_id), event.get("guild_name"), event.get("member_count", 0),
    )


async def handle_message(pool, event):
    """Process a message event."""
    await upsert_user(pool, event)
    await upsert_channel(pool, event)
    await upsert_guild(pool, event)

    await pool.execute(
        """
        INSERT INTO fact_messages
            (message_id, user_id, channel_id, guild_id, content_length, word_count,
             has_attachment, has_embed, event_type, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        int(event["message_id"]),
        int(event["user_id"]),
        int(event["channel_id"]),
        int(event["guild_id"]),
        event.get("content_length", 0),
        event.get("word_count", 0),
        event.get("has_attachment", False),
        event.get("has_embed", False),
        event.get("event_type", "send"),
        parse_ts(event.get("timestamp")),
    )


async def handle_voice(pool, event):
    """Process a voice event."""
    await upsert_user(pool, event)
    await upsert_guild(pool, event)
    if event.get("channel_id"):
        await upsert_channel(pool, event)

    await pool.execute(
        """
        INSERT INTO fact_voice_events (user_id, channel_id, guild_id, event_type)
        VALUES ($1, $2, $3, $4)
        """,
        int(event["user_id"]),
        int(event["channel_id"]) if event.get("channel_id") else None,
        int(event["guild_id"]) if event.get("guild_id") else None,
        event["event_type"],
    )


async def handle_reaction(pool, event):
    """Process a reaction event."""
    await upsert_user(pool, event)

    await pool.execute(
        """
        INSERT INTO fact_reactions (message_id, user_id, channel_id, guild_id, emoji, event_type)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        int(event["message_id"]),
        int(event["user_id"]),
        int(event["channel_id"]) if event.get("channel_id") else None,
        int(event["guild_id"]) if event.get("guild_id") else None,
        event.get("emoji"),
        event["event_type"],
    )


async def handle_presence(pool, event):
    """Process a presence event."""
    await upsert_user(pool, event)

    await pool.execute(
        """
        INSERT INTO fact_presence (user_id, activity_type, activity_name, guild_id)
        VALUES ($1, $2, $3, $4)
        """,
        int(event["user_id"]),
        event.get("activity_type"),
        event.get("activity_name"),
        int(event["guild_id"]) if event.get("guild_id") else None,
    )


# Route topic → handler
HANDLERS = {
    TOPIC_MESSAGES: handle_message,
    TOPIC_VOICE: handle_voice,
    TOPIC_REACTIONS: handle_reaction,
    TOPIC_PRESENCE: handle_presence,
}


async def main():
    print("Consumer: Connecting to PostgreSQL...")
    pool = await get_db_pool()
    print("Consumer: Connected to PostgreSQL")

    print("Consumer: Connecting to Kafka...")
    consumer = AIOKafkaConsumer(
        TOPIC_MESSAGES, TOPIC_VOICE, TOPIC_REACTIONS, TOPIC_PRESENCE,
        bootstrap_servers="localhost:9092",
        group_id="analytics-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",  # Process events from the beginning if consumer is new
    )
    await consumer.start()
    print("Consumer: Listening for events...")

    try:
        async for msg in consumer:
            handler = HANDLERS.get(msg.topic)
            if handler:
                try:
                    await handler(pool, msg.value)
                except Exception as e:
                    print(f"Consumer: Error processing {msg.topic} event: {e}")
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
