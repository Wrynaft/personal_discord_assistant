"""Pure game logic for casino minigames — no Discord/DB dependencies."""

import random


# ── Slots ────────────────────────────────────────────────────────────
# 3 independent reels, 6 symbols, 3-of-a-kind only.
# RTP ~95.1% (house edge ~4.9%), hit frequency ~5.5%.

SLOT_SYMBOLS = [
    {"emoji": "\U0001f352", "weight": 30, "payout": 10,  "name": "Cherry"},   # 🍒
    {"emoji": "\U0001f34b", "weight": 25, "payout": 16,  "name": "Lemon"},    # 🍋
    {"emoji": "\U0001f34a", "weight": 20, "payout": 25,  "name": "Orange"},   # 🍊
    {"emoji": "\U0001f347", "weight": 15, "payout": 50,  "name": "Grape"},    # 🍇
    {"emoji": "\U0001f514", "weight": 7,  "payout": 150, "name": "Bell"},     # 🔔
    {"emoji": "7️⃣", "weight": 3,  "payout": 400, "name": "Seven"}, # 7️⃣
]


def spin_slots():
    """Roll three reels. Returns dict with reels list and payout multiplier (0 = loss)."""
    weights = [s["weight"] for s in SLOT_SYMBOLS]
    reels = random.choices(SLOT_SYMBOLS, weights=weights, k=3)
    if reels[0]["name"] == reels[1]["name"] == reels[2]["name"]:
        return {
            "reels": reels,
            "multiplier": reels[0]["payout"],
            "win": True,
            "match_name": reels[0]["name"],
        }
    return {"reels": reels, "multiplier": 0, "win": False, "match_name": None}


def format_reels(reels):
    """Render the 3 reels as a slot display line."""
    return f"| {reels[0]['emoji']} | {reels[1]['emoji']} | {reels[2]['emoji']} |"
