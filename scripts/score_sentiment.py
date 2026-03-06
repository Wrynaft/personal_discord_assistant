#!/usr/bin/env python3
"""
Sentiment Scoring Cron Job

Scores unscored messages with content using the LLM.
Run via cron every hour:
    0 * * * * cd /home/ubuntu/personal_discord_assistant && venv/bin/python3 scripts/score_sentiment.py >> /tmp/sentiment.log 2>&1
"""

import sys
import os
import asyncio

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
import config
from services.llm_service import LLMService
from datetime import datetime


BATCH_SIZE = 10
MAX_MESSAGES = 50  # Cap per run to stay within free tier


async def main():
    print(f"\n[{datetime.now().isoformat()}] Sentiment scoring started...")

    # Connect to database
    pool = await asyncpg.create_pool(
        dsn=config.DATABASE_URL,
        min_size=1,
        max_size=3,
    )

    # Fetch unscored messages that have content
    rows = await pool.fetch(
        """
        SELECT id, content_preview
        FROM fact_messages
        WHERE sentiment_score IS NULL
          AND event_type = 'send'
          AND content_preview IS NOT NULL
          AND LENGTH(content_preview) > 5
        ORDER BY created_at DESC
        LIMIT $1
        """,
        MAX_MESSAGES,
    )

    if not rows:
        print("No unscored messages found.")
        await pool.close()
        return

    print(f"Found {len(rows)} unscored messages. Scoring in batches of {BATCH_SIZE}...")

    llm = LLMService()
    scored = 0
    errors = 0

    scoring_prompt = """Rate the sentiment of each Discord message below on a scale of 1-5:
1 = very negative/toxic
2 = negative/frustrated  
3 = neutral
4 = positive/friendly
5 = very positive/enthusiastic

Return ONLY a comma-separated list of scores in the same order, nothing else.
Example: 3,4,2,5,1

Messages:
"""

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]

        # Build prompt with numbered messages
        numbered = "\n".join(
            f"{j+1}. {row['content_preview']}"
            for j, row in enumerate(batch)
        )

        prompt = [
            {"role": "system", "content": scoring_prompt + numbered},
            {"role": "user", "content": "Rate these messages."},
        ]

        try:
            response = await llm.generate_response(prompt)

            # Parse comma-separated scores
            scores = []
            for s in response.strip().split(","):
                s = s.strip()
                try:
                    score = int(s)
                    scores.append(max(1, min(5, score)))  # Clamp 1-5
                except ValueError:
                    scores.append(3)  # Default neutral

            # Pad if LLM returned fewer scores
            while len(scores) < len(batch):
                scores.append(3)

            # Update database
            for row, score in zip(batch, scores):
                await pool.execute(
                    "UPDATE fact_messages SET sentiment_score = $1 WHERE id = $2",
                    score, row["id"],
                )
                scored += 1

            print(f"  Batch {i // BATCH_SIZE + 1}: scored {len(batch)} messages → {scores}")

        except Exception as e:
            print(f"  Batch {i // BATCH_SIZE + 1}: ERROR — {e}")
            errors += 1

        # Small delay between batches to be nice to rate limits
        await asyncio.sleep(1)

    await pool.close()
    print(f"Done! Scored: {scored}, Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
