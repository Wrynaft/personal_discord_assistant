"""
Mimic Service — Generates responses mimicking a specific user's speech pattern.

Uses a statistical + LLM hybrid approach:
1. Compute live stats from ALL messages in PostgreSQL (emoji rate, msg length, vocabulary, etc.)
2. LLM interprets stats into a concise style guide
3. 10 random example messages for tone/voice reference
4. Channel conversation context for relevance
"""

import re
import asyncpg
import config
from collections import Counter
from services.llm_service import LLMService


# Common English words to exclude from vocabulary analysis
STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "to", "of", "in", "for",
    "on", "with", "at", "by", "from", "as", "into", "through", "during",
    "before", "after", "and", "but", "or", "nor", "not", "no", "so",
    "if", "then", "than", "that", "this", "these", "those", "it", "its",
    "i", "me", "my", "we", "us", "our", "you", "your", "he", "him",
    "his", "she", "her", "they", "them", "their", "what", "which", "who",
    "when", "where", "how", "why", "all", "each", "every", "both",
    "few", "more", "most", "other", "some", "such", "just", "also",
    "like", "about", "up", "out", "get", "got", "go", "going", "went",
    "one", "two", "know", "think", "want", "see", "look", "make",
    "come", "take", "give", "say", "said", "tell", "told", "yeah",
    "yes", "ok", "okay", "oh", "ah", "um", "uh", "lol", "haha",
    "im", "dont", "doesnt", "didnt", "cant", "wont", "its", "thats",
    "really", "very", "much", "too", "well", "back", "now", "here",
    "there", "still", "even", "right", "good", "new", "way", "thing",
}

EMOJI_PATTERN = re.compile(
    r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    r"\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U0001FA00-\U0001FA6F"
    r"\U0001FA70-\U0001FAFF\U00002600-\U000026FF\U0000FE00-\U0000FE0F"
    r"\U0000200D\U00002B50\U00002764\U0000203C-\U00003299]+"
)

STATS_TO_STYLE_PROMPT = """Convert these Discord user statistics into a concise style guide (max 150 words).

Focus ONLY on HOW they write, NOT on WHAT they write about. Do NOT mention any specific topics, events, places, or people.

Stats:
{stats}

Write a style guide that another AI could use to perfectly replicate this person's texting style. Cover: tone, grammar habits, capitalization, punctuation, emoji usage (or lack thereof), typical message length, and any distinctive speech patterns."""

MIMIC_CONTEXT_PROMPT = """You are mimicking a Discord user's texting style.

STYLE GUIDE:
{profile}

EXAMPLE MESSAGES (for voice/tone reference only — do NOT reference their topics):
{examples}

CURRENT CONVERSATION:
{conversation}

RULES:
- Match their exact style: capitalization, punctuation, slang, message length
- ONLY focus on the current conversation. Do NOT bring up topics from the example messages.
- Actually engage with what's being discussed. Give opinions, react, or add to the topic.
- NEVER give dismissive answers like "idk", "no idea", "not sure".
- If the style guide says they rarely use emojis, do NOT use emojis.
- Keep your response to 1-3 short messages like a real Discord message.
- Do NOT mention you are an AI.

What would this person say next?"""

MIMIC_PROMPT_DIRECT = """You are mimicking a Discord user's texting style.

STYLE GUIDE:
{profile}

EXAMPLE MESSAGES (for voice/tone reference only — do NOT reference their topics):
{examples}

RULES:
- Match their exact style: capitalization, punctuation, slang, message length
- ONLY respond to the prompt below. Do NOT bring up topics from the example messages.
- Actually engage with the topic. Give opinions, share thoughts, react meaningfully.
- NEVER give dismissive answers like "idk", "no idea", "not sure".
- If the style guide says they rarely use emojis, do NOT use emojis.
- Keep your response to 1-3 short messages like a real Discord message.
- Do NOT mention you are an AI.

Respond to this:
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

    async def compute_user_stats(self, user_id, guild_id):
        """Compute live style statistics from ALL of a user's messages."""
        if not self.pool:
            return None

        rows = await self.pool.fetch(
            """
            SELECT content_preview
            FROM fact_messages
            WHERE user_id = $1 AND guild_id = $2
              AND event_type = 'send'
              AND content_preview IS NOT NULL
              AND LENGTH(content_preview) > 3
            """,
            user_id, guild_id,
        )

        if len(rows) < 10:
            return None

        messages = [row["content_preview"] for row in rows]
        total = len(messages)

        # --- Message length ---
        lengths = [len(m) for m in messages]
        avg_len = sum(lengths) / total
        short_msgs = sum(1 for l in lengths if l < 20) / total * 100
        long_msgs = sum(1 for l in lengths if l > 100) / total * 100

        # --- Capitalization ---
        starts_upper = sum(1 for m in messages if m[0].isupper()) / total * 100
        all_lower = sum(1 for m in messages if m == m.lower()) / total * 100

        # --- Punctuation ---
        ends_period = sum(1 for m in messages if m.rstrip().endswith(".")) / total * 100
        ends_exclaim = sum(1 for m in messages if m.rstrip().endswith("!")) / total * 100
        ends_question = sum(1 for m in messages if m.rstrip().endswith("?")) / total * 100
        no_punctuation = sum(1 for m in messages if not m.rstrip()[-1:] in ".!?") / total * 100

        # --- Emoji usage ---
        msgs_with_emoji = sum(1 for m in messages if EMOJI_PATTERN.search(m)) / total * 100
        all_emojis = []
        for m in messages:
            all_emojis.extend(EMOJI_PATTERN.findall(m))
        emoji_counts = Counter(all_emojis).most_common(5)

        # --- Vocabulary (top distinctive words) ---
        word_counter = Counter()
        for m in messages:
            words = re.findall(r"[a-zA-Z']+", m.lower())
            for w in words:
                if w not in STOP_WORDS and len(w) > 1:
                    word_counter[w] += 1
        top_words = word_counter.most_common(15)

        # --- Build stats string ---
        stats_parts = [
            f"Total messages analyzed: {total}",
            f"Average message length: {avg_len:.0f} characters",
            f"Short messages (<20 chars): {short_msgs:.0f}%",
            f"Long messages (>100 chars): {long_msgs:.0f}%",
            "",
            f"Starts with uppercase: {starts_upper:.0f}%",
            f"Fully lowercase messages: {all_lower:.0f}%",
            "",
            f"Ends with period: {ends_period:.0f}%",
            f"Ends with exclamation: {ends_exclaim:.0f}%",
            f"Ends with question mark: {ends_question:.0f}%",
            f"No ending punctuation: {no_punctuation:.0f}%",
            "",
            f"Messages containing emojis: {msgs_with_emoji:.0f}%",
        ]

        if emoji_counts:
            emoji_str = ", ".join(f"{e} ({c}x)" for e, c in emoji_counts)
            stats_parts.append(f"Most used emojis: {emoji_str}")
        else:
            stats_parts.append("Emojis: almost never uses them")

        if top_words:
            words_str = ", ".join(f'"{w}" ({c}x)' for w, c in top_words)
            stats_parts.append(f"\nTop distinctive words: {words_str}")

        return "\n".join(stats_parts)

    async def build_style_profile(self, user_id, guild_id):
        """Generate a style profile from live stats using the LLM."""
        stats = await self.compute_user_stats(user_id, guild_id)
        if not stats:
            return None

        prompt = [
            {"role": "system", "content": STATS_TO_STYLE_PROMPT.format(stats=stats)},
            {"role": "user", "content": "Create the style guide."},
        ]

        profile = await self.llm.generate_response(prompt)

        # Cache in database
        msg_count = stats.split("\n")[0].split(": ")[1]
        await self.pool.execute(
            """
            INSERT INTO user_style_profiles (user_id, guild_id, profile, sample_size, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (user_id, guild_id) DO UPDATE SET
                profile = EXCLUDED.profile,
                sample_size = EXCLUDED.sample_size,
                updated_at = NOW()
            """,
            user_id, guild_id, profile, int(msg_count),
        )

        return profile

    async def get_style_profile(self, user_id, guild_id):
        """Get cached profile or build fresh one."""
        if not self.pool:
            return None

        # Check cache — use if less than 24 hours old
        row = await self.pool.fetchrow(
            """
            SELECT profile FROM user_style_profiles
            WHERE user_id = $1 AND guild_id = $2
              AND updated_at > NOW() - INTERVAL '24 hours'
            """,
            user_id, guild_id,
        )

        if row:
            return row["profile"]

        # Build fresh
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

    async def mimic_user(self, user_id, guild_id, prompt_text=None, conversation_context=None):
        """Generate a response mimicking a user's style."""
        profile = await self.get_style_profile(user_id, guild_id)
        if not profile:
            return None

        examples = await self.get_random_examples(user_id, guild_id, count=10)
        if not examples:
            return None

        examples_text = "\n".join(f"- {msg}" for msg in examples)

        if conversation_context:
            convo_text = "\n".join(conversation_context)
            full_prompt = MIMIC_CONTEXT_PROMPT.format(
                profile=profile,
                examples=examples_text,
                conversation=convo_text,
            )
        else:
            full_prompt = MIMIC_PROMPT_DIRECT.format(
                profile=profile,
                examples=examples_text,
                prompt=prompt_text or "Say something random",
            )

        messages = [
            {"role": "system", "content": full_prompt},
            {"role": "user", "content": "Respond now."},
        ]

        return await self.llm.generate_response(messages)

    async def get_message_count(self, user_id, guild_id):
        """Get how many usable messages a user has."""
        if not self.pool:
            return 0

        row = await self.pool.fetchrow(
            """
            SELECT COUNT(*) as total
            FROM fact_messages
            WHERE user_id = $1 AND guild_id = $2
              AND event_type = 'send'
              AND content_preview IS NOT NULL
              AND LENGTH(content_preview) > 3
            """,
            user_id, guild_id,
        )
        return row["total"] if row else 0
