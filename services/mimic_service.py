"""
Mimic Service — Generates responses mimicking a specific user's speech pattern.

Uses a hybrid approach:
1. Pre-built style profile (generated periodically by analyzing ~100 messages)
2. Random sample of 10 recent messages as few-shot examples
"""

import asyncpg
import random
import config
from services.llm_service import LLMService


PROFILE_PROMPT = """Analyze the following Discord messages from a user and create a detailed speech/writing style profile.

Focus on:
1. **Vocabulary**: Common words, slang, catchphrases, internet lingo
2. **Grammar & Punctuation**: Capitalization habits, periods, exclamation marks, question marks
3. **Emoji & Emoticon usage**: Which emojis they use, how often, placement
4. **Message length**: Short bursts vs long paragraphs
5. **Tone**: Casual, formal, sarcastic, enthusiastic, deadpan
6. **Sentence structure**: Fragments, run-ons, proper sentences
7. **Common topics**: What they tend to talk about
8. **Unique quirks**: Any distinctive patterns that make their messages recognizable

Return a concise style guide (max 200 words) that another AI could use to perfectly mimic this person's texting style. Do NOT include their name or any identifying information other than style traits.

Messages:
"""

MIMIC_PROMPT = """You are mimicking a Discord user's texting style. Here is their style profile:

{profile}

Here are some example messages from this person for reference:
{examples}

RULES:
- Match their exact style: capitalization, punctuation, emoji usage, slang, message length
- Stay in character — respond as this person would
- Keep the message natural and casual, like a real Discord message
- Do NOT mention that you are an AI or mimicking someone
- Keep your response to 1-3 messages (separated by newlines) like a real person would type

Now respond to this in their style:
{prompt}"""


class MimicService:
    def __init__(self):
        self.pool = None
        self.llm = LLMService()

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=1,
            max_size=3,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def build_style_profile(self, user_id, guild_id):
        """Analyze a user's messages and generate a style profile using the LLM."""
        if not self.pool:
            return None

        # Fetch up to 100 random messages from this user
        rows = await self.pool.fetch(
            """
            SELECT content_preview
            FROM fact_messages
            WHERE user_id = $1 AND guild_id = $2
              AND event_type = 'send'
              AND content_preview IS NOT NULL
              AND LENGTH(content_preview) > 5
            ORDER BY RANDOM()
            LIMIT 100
            """,
            user_id, guild_id,
        )

        if len(rows) < 10:
            return None  # Not enough messages to build a profile

        # Combine messages for the LLM
        messages_text = "\n".join(
            f"- {row['content_preview']}" for row in rows
        )

        prompt = [
            {"role": "system", "content": PROFILE_PROMPT + messages_text},
            {"role": "user", "content": "Create the style profile."},
        ]

        profile = await self.llm.generate_response(prompt)

        # Store/update in database
        await self.pool.execute(
            """
            INSERT INTO user_style_profiles (user_id, guild_id, profile, sample_size, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (user_id, guild_id) DO UPDATE SET
                profile = EXCLUDED.profile,
                sample_size = EXCLUDED.sample_size,
                updated_at = NOW()
            """,
            user_id, guild_id, profile, len(rows),
        )

        return profile

    async def get_style_profile(self, user_id, guild_id):
        """Retrieve a stored style profile, or build one if it doesn't exist."""
        if not self.pool:
            return None

        row = await self.pool.fetchrow(
            "SELECT profile, sample_size FROM user_style_profiles WHERE user_id = $1 AND guild_id = $2",
            user_id, guild_id,
        )

        if row:
            return row["profile"]

        # No profile exists — build one
        return await self.build_style_profile(user_id, guild_id)

    async def get_random_examples(self, user_id, guild_id, count=10):
        """Fetch random example messages from a user."""
        if not self.pool:
            return []

        rows = await self.pool.fetch(
            """
            SELECT content_preview
            FROM fact_messages
            WHERE user_id = $1 AND guild_id = $2
              AND event_type = 'send'
              AND content_preview IS NOT NULL
              AND LENGTH(content_preview) > 5
            ORDER BY RANDOM()
            LIMIT $3
            """,
            user_id, guild_id, count,
        )

        return [row["content_preview"] for row in rows]

    async def mimic_user(self, user_id, guild_id, prompt_text):
        """
        Generate a response mimicking a user's style.
        
        Args:
            user_id: Discord user ID to mimic
            guild_id: Guild ID for context
            prompt_text: The topic/question to respond to
        Returns:
            str: Generated response in the user's style, or None if not enough data
        """
        # Get style profile
        profile = await self.get_style_profile(user_id, guild_id)
        if not profile:
            return None

        # Get random example messages
        examples = await self.get_random_examples(user_id, guild_id, count=10)
        if not examples:
            return None

        examples_text = "\n".join(f"- {msg}" for msg in examples)

        # Build the mimic prompt
        full_prompt = MIMIC_PROMPT.format(
            profile=profile,
            examples=examples_text,
            prompt=prompt_text,
        )

        messages = [
            {"role": "system", "content": full_prompt},
            {"role": "user", "content": prompt_text},
        ]

        return await self.llm.generate_response(messages)

    async def get_message_count(self, user_id, guild_id):
        """Get how many scored messages a user has."""
        if not self.pool:
            return 0

        row = await self.pool.fetchrow(
            """
            SELECT COUNT(*) as total
            FROM fact_messages
            WHERE user_id = $1 AND guild_id = $2
              AND event_type = 'send'
              AND content_preview IS NOT NULL
              AND LENGTH(content_preview) > 5
            """,
            user_id, guild_id,
        )
        return row["total"] if row else 0
