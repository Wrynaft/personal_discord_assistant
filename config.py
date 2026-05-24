import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

if not DISCORD_TOKEN:
    raise ValueError("DISCORD_TOKEN not found in .env file")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in .env file")

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
if not NEWS_API_KEY:
    # Warning only, don't crash if they haven't added it yet
    print("Warning: NEWS_API_KEY not found in .env file. News features will fail.")

# Channel ID for scheduled daily news posts
_news_channel = os.getenv("NEWS_CHANNEL_ID")
NEWS_CHANNEL_ID = int(_news_channel) if _news_channel else None
if not NEWS_CHANNEL_ID:
    print("Warning: NEWS_CHANNEL_ID not set. Daily scheduled news will not run.")

# PostgreSQL connection for analytics (optional — analytics disabled if not set)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://discord:discord_analytics_2026@localhost:5432/discord_analytics")

# Superset dashboard URL (for !dashboard command)
SUPERSET_URL = os.getenv("SUPERSET_URL", "")

# Danbooru daily posts
_danbooru_channel = os.getenv("DANBOORU_CHANNEL_ID")
DANBOORU_CHANNEL_ID = int(_danbooru_channel) if _danbooru_channel else None
DANBOORU_DEFAULT_TAGS = os.getenv("DANBOORU_DEFAULT_TAGS", "")

# Casino / gambling
_casino_channel = os.getenv("CASINO_CHANNEL_ID")
CASINO_CHANNEL_ID = int(_casino_channel) if _casino_channel else None
if not CASINO_CHANNEL_ID:
    print("Warning: CASINO_CHANNEL_ID not set. Casino commands will be disabled.")

GAMBLING_SEED_BANK = int(os.getenv("GAMBLING_SEED_BANK", "1000"))
GAMBLING_BASE_DEBT = int(os.getenv("GAMBLING_BASE_DEBT", "1200"))
GAMBLING_DEBT_MULTIPLIER = float(os.getenv("GAMBLING_DEBT_MULTIPLIER", "1.4"))

# Per-game max-bet ratio against day_start_bank.
# Mirrors the original game's per-table bet caps (slots is the most restricted).
# Games not listed default to 1.0 (full day_start_bank).
GAMBLING_GAME_CAPS = {
    "slots": 0.5,
}
