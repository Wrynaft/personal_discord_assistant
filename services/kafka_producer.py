import json
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer


# Kafka topic names
TOPIC_MESSAGES = "discord.messages"
TOPIC_VOICE = "discord.voice"
TOPIC_REACTIONS = "discord.reactions"
TOPIC_PRESENCE = "discord.presence"


class KafkaProducer:
    """Async Kafka producer for Discord events."""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def connect(self):
        """Initialize and start the Kafka producer."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        await self.producer.start()
        print("Kafka: Producer connected")

    async def close(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()

    async def send_message_event(self, message, event_type="send"):
        """Produce a message event to Kafka."""
        if not self.producer or not message.guild:
            return

        event = {
            "event_type": event_type,
            "message_id": message.id,
            "user_id": message.author.id,
            "username": str(message.author),
            "display_name": message.author.display_name,
            "is_bot": message.author.bot,
            "channel_id": message.channel.id,
            "channel_name": message.channel.name,
            "channel_type": str(message.channel.type),
            "category": message.channel.category.name if hasattr(message.channel, 'category') and message.channel.category else None,
            "guild_id": message.guild.id,
            "guild_name": message.guild.name,
            "member_count": message.guild.member_count,
            "content_length": len(message.content or ""),
            "content_preview": (message.content or "")[:200],  # Truncated for sentiment scoring
            "word_count": len((message.content or "").split()) if message.content else 0,
            "has_attachment": bool(message.attachments),
            "has_embed": bool(message.embeds),
            "timestamp": message.created_at.isoformat(),
        }
        await self.producer.send(TOPIC_MESSAGES, event)

    async def send_voice_event(self, member, channel, guild, event_type):
        """Produce a voice state event to Kafka."""
        if not self.producer:
            return

        event = {
            "event_type": event_type,
            "user_id": member.id,
            "username": str(member),
            "display_name": member.display_name,
            "is_bot": member.bot,
            "channel_id": channel.id if channel else None,
            "channel_name": channel.name if channel else None,
            "channel_type": str(channel.type) if channel else None,
            "category": channel.category.name if channel and hasattr(channel, 'category') and channel.category else None,
            "guild_id": guild.id if guild else None,
            "guild_name": guild.name if guild else None,
            "member_count": guild.member_count if guild else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.producer.send(TOPIC_VOICE, event)

    async def send_reaction_event(self, reaction, user, event_type):
        """Produce a reaction event to Kafka."""
        if not self.producer or not reaction.message.guild:
            return

        event = {
            "event_type": event_type,
            "message_id": reaction.message.id,
            "user_id": user.id,
            "username": str(user),
            "display_name": user.display_name if hasattr(user, 'display_name') else str(user),
            "is_bot": user.bot,
            "channel_id": reaction.message.channel.id,
            "guild_id": reaction.message.guild.id,
            "emoji": str(reaction.emoji),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.producer.send(TOPIC_REACTIONS, event)

    async def send_presence_event(self, member, activity):
        """Produce a presence/activity event to Kafka."""
        if not self.producer:
            return

        event = {
            "user_id": member.id,
            "username": str(member),
            "display_name": member.display_name,
            "is_bot": member.bot,
            "activity_type": str(activity.type).replace("ActivityType.", "") if activity else None,
            "activity_name": activity.name if activity else None,
            "guild_id": member.guild.id if hasattr(member, 'guild') and member.guild else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.producer.send(TOPIC_PRESENCE, event)
