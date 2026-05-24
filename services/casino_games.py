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


# ── Dice (craps with 3-roll point cap) ───────────────────────────────
# Come-out: 7/11 = instant win, 2/3/12 = instant lose, else = point.
# Point phase: up to 3 rolls. Hit point = win, roll 7 = lose, neither in 3 = refund.
# RTP ~95.6% (house edge ~4.4%) with flat 1.8x win payout.

DICE_FACE = {1: "⚀", 2: "⚁", 3: "⚂", 4: "⚃", 5: "⚄", 6: "⚅"}
DICE_POINT_MAX_ROLLS = 3
DICE_WIN_PAYOUT = 1.8  # total return multiplier (1.0 = refund baseline)


def _roll_two_dice():
    d1 = random.randint(1, 6)
    d2 = random.randint(1, 6)
    return d1, d2, d1 + d2


def dice_comeout():
    """Roll the come-out roll. Returns dict with d1, d2, total, outcome, and (if point) point."""
    d1, d2, total = _roll_two_dice()
    if total in (7, 11):
        return {"d1": d1, "d2": d2, "total": total, "outcome": "win"}
    if total in (2, 3, 12):
        return {"d1": d1, "d2": d2, "total": total, "outcome": "lose"}
    return {"d1": d1, "d2": d2, "total": total, "outcome": "point", "point": total}


def dice_point_roll(point):
    """Roll for the point phase. Returns dict with d1, d2, total, outcome (win|lose|continue)."""
    d1, d2, total = _roll_two_dice()
    if total == point:
        return {"d1": d1, "d2": d2, "total": total, "outcome": "win"}
    if total == 7:
        return {"d1": d1, "d2": d2, "total": total, "outcome": "lose"}
    return {"d1": d1, "d2": d2, "total": total, "outcome": "continue"}


def format_dice(d1, d2):
    """Render a 2-dice roll as 'face face = total'."""
    return f"{DICE_FACE[d1]} {DICE_FACE[d2]} = **{d1 + d2}**"


# ── Wheel of Fortune ─────────────────────────────────────────────────
# 6 outcomes with weights summing to 100 (= % chance).
# RTP ~95% (house edge ~5%). Refund slot acts as relief; 10x is the jackpot.

WHEEL_OUTCOMES = [
    {"label": "Lose",     "emoji": "🟥", "weight": 52, "multiplier": 0},
    {"label": "Refund",   "emoji": "⬜", "weight": 25, "multiplier": 1},
    {"label": "Double",   "emoji": "🟨", "weight": 12, "multiplier": 2},
    {"label": "Triple",   "emoji": "🟧", "weight": 7,  "multiplier": 3},
    {"label": "5x",       "emoji": "🟩", "weight": 3,  "multiplier": 5},
    {"label": "JACKPOT",  "emoji": "🌟", "weight": 1,  "multiplier": 10},
]


def spin_wheel():
    """Pick one outcome from the weighted wheel. Returns the outcome dict."""
    weights = [o["weight"] for o in WHEEL_OUTCOMES]
    return random.choices(WHEEL_OUTCOMES, weights=weights, k=1)[0]


# ── Horse Race ───────────────────────────────────────────────────────
# 4 horses with weighted win chances. Favorites win more, longshots pay more.
# Per-horse RTP: 95% / 95% / 90% / 90% (longshots carry slightly higher edge).

HORSES = [
    {"name": "Lightning", "emoji": "⚡", "weight": 50, "payout": 1.9},
    {"name": "Thunder",   "emoji": "⛈️", "weight": 25, "payout": 3.8},
    {"name": "Storm",     "emoji": "🌪️", "weight": 15, "payout": 6.0},
    {"name": "Tornado",   "emoji": "🌀", "weight": 10, "payout": 9.0},
]


def run_race():
    """Pick the winning horse using weighted probabilities. Returns the horse dict + index."""
    weights = [h["weight"] for h in HORSES]
    winner = random.choices(range(len(HORSES)), weights=weights, k=1)[0]
    return winner, HORSES[winner]


# ── Blackjack (21) ───────────────────────────────────────────────────
# Standard 52-card deck reshuffled per hand. Dealer hits to 17 and stands on all 17.
# No splits/double/insurance. Blackjack pays 2.5x, regular win pays 2x, push refunds.

SUITS = ["♠", "♥", "♦", "♣"]
RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
CARD_VALUE = {
    "A": 11, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
    "10": 10, "J": 10, "Q": 10, "K": 10,
}
BLACKJACK_NATURAL_PAYOUT = 2.5
BLACKJACK_WIN_PAYOUT = 2.0
DEALER_STAND_ON = 17


def new_deck():
    deck = [(r, s) for r in RANKS for s in SUITS]
    random.shuffle(deck)
    return deck


def hand_value(hand):
    """Sum hand, treating aces as 11 then reducing to 1 as needed to avoid bust."""
    total = sum(CARD_VALUE[r] for r, _ in hand)
    aces = sum(1 for r, _ in hand if r == "A")
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total


def is_blackjack(hand):
    return len(hand) == 2 and hand_value(hand) == 21


def format_hand(hand, hide_first=False):
    """Render a hand like '[10♥] [J♦]'. If hide_first, show first card as [??]."""
    cards = []
    for i, (r, s) in enumerate(hand):
        if hide_first and i == 0:
            cards.append("[??]")
        else:
            cards.append(f"[{r}{s}]")
    return " ".join(cards)


def play_dealer(dealer_hand, deck):
    """Play out the dealer's hand to standard rules (stand on 17+). Mutates the hand."""
    while hand_value(dealer_hand) < DEALER_STAND_ON:
        dealer_hand.append(deck.pop())
    return dealer_hand
