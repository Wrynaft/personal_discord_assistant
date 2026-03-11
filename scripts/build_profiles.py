#!/usr/bin/env python3
"""
Style Profile Builder — Cron Job

Rebuilds style profiles for all active users in the server.
Run weekly via cron:
    0 2 * * 0 cd /home/ubuntu/personal_discord_assistant && venv/bin/python3 scripts/build_profiles.py >> /tmp/profiles.log 2>&1
"""

import sys
import os
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
import config
from services.mimic_service import MimicService
from datetime import datetime


MIN_MESSAGES = 10  # Minimum messages needed to build a profile


async def main():
    print(f"\n[{datetime.now().isoformat()}] Style profile builder started...")

    pool = await asyncpg.create_pool(dsn=config.DATABASE_URL, min_size=1, max_size=3)

    # Find all active users with enough messages
    users = await pool.fetch(
        """
        SELECT user_id, guild_id, COUNT(*) as msg_count
        FROM fact_messages
        WHERE event_type = 'send'
          AND content_preview IS NOT NULL
          AND LENGTH(content_preview) > 5
        GROUP BY user_id, guild_id
        HAVING COUNT(*) >= $1
        """,
        MIN_MESSAGES,
    )

    if not users:
        print("No users with enough messages found.")
        await pool.close()
        return

    print(f"Found {len(users)} users to profile.")

    mimic = MimicService()
    mimic.pool = pool  # Share the pool

    built = 0
    errors = 0

    for row in users:
        try:
            profile = await mimic.build_style_profile(row["user_id"], row["guild_id"])
            if profile:
                print(f"  Built profile for user {row['user_id']} ({row['msg_count']} msgs)")
                built += 1
            else:
                print(f"  Skipped user {row['user_id']} (not enough data)")
        except Exception as e:
            print(f"  Error for user {row['user_id']}: {e}")
            errors += 1

        # Delay between users to respect rate limits
        await asyncio.sleep(2)

    await pool.close()
    print(f"Done! Profiles built: {built}, Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
