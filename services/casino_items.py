"""Casino items catalog + pure effect-application helpers.

Items are bought with tickets (earned from clearing daily quotas / hitting jackpots),
stored as a per-guild shared inventory, and activated either passively (auto-applied
on the next qualifying bet) or actively (consumed via /use right now).
"""

# Item registry. Each item has:
#   name, emoji, cost (tickets), description
#   charges (how many bets/uses the activation provides)
#   trigger: "passive" (auto-applies to next qualifying bet) or "active" (manual /use)
#   applies_on (passive only): "loss", "win", or "any"
ITEMS = {
    "cooler": {
        "name": "Cooler",
        "emoji": "❄️",
        "cost": 3,
        "description": "Soften your next loss to a 50% refund.",
        "charges": 1,
        "trigger": "passive",
        "applies_on": "loss",
    },
    "insurance": {
        "name": "Insurance",
        "emoji": "🛡️",
        "cost": 4,
        "description": "Full refund on your next loss.",
        "charges": 1,
        "trigger": "passive",
        "applies_on": "loss",
    },
    "drink": {
        "name": "Drink",
        "emoji": "🍺",
        "cost": 5,
        "description": "Next 3 wins pay +25% bonus on profit.",
        "charges": 3,
        "trigger": "passive",
        "applies_on": "win",
    },
    "golden_chip": {
        "name": "Golden Chip",
        "emoji": "🪙",
        "cost": 6,
        "description": "Next bet is FREE — you get the bet amount back regardless of outcome.",
        "charges": 1,
        "trigger": "passive",
        "applies_on": "any",
    },
    "time_machine": {
        "name": "Time Machine",
        "emoji": "⏰",
        "cost": 7,
        "description": "Reverse the most recent bet (refunds you and undoes the bank change).",
        "charges": 1,
        "trigger": "active",
    },
    "quota_gun": {
        "name": "Quota Gun",
        "emoji": "🔫",
        "cost": 8,
        "description": "Instantly pay 33% of today's debt for free.",
        "charges": 1,
        "trigger": "active",
    },
}

# Numeric tuning
DRINK_BONUS_PCT = 0.25   # +25% on profit
COOLER_REFUND_PCT = 0.50 # 50% refund on softened loss
QUOTA_GUN_PAYOFF_PCT = 0.33

# Ticket earning rates
TICKETS_PER_DAY_CLEARED = 2
TICKETS_PER_JACKPOT = 5


def item(item_id):
    """Return item def or None."""
    return ITEMS.get(item_id)


def all_passive():
    return {k: v for k, v in ITEMS.items() if v["trigger"] == "passive"}


def all_active():
    return {k: v for k, v in ITEMS.items() if v["trigger"] == "active"}


def apply_passive_effects(bet, payout, active_effects):
    """Given the raw (bet, payout) and a list of active passive-effect rows
    (each dict with item_id, charges_left), determine modifications.

    Returns:
        (final_payout, applied_effects, consumed_effect_ids)

    `applied_effects` is a list of dicts describing each effect applied — used
    to render "🛡️ Insurance triggered (+$X)" in the result embed.
    `consumed_effect_ids` is a list of effect row IDs whose charges should be
    decremented in the DB (caller decides whether to delete them).
    """
    net = payout - bet
    final_payout = payout
    applied = []
    consumed_ids = []

    # Process in priority order: Golden Chip first (broadest), then loss-side, then win-side.
    # Each effect type only applies once per bet.
    consumed_item_ids = set()

    for eff in active_effects:
        iid = eff["item_id"]
        if iid in consumed_item_ids:
            continue
        defn = ITEMS.get(iid)
        if not defn or defn["trigger"] != "passive":
            continue

        applies_on = defn.get("applies_on", "any")

        if iid == "golden_chip" and applies_on == "any":
            # Add bet back to payout — effectively makes the bet free
            delta = bet
            final_payout += delta
            applied.append({
                "item_id": iid,
                "name": defn["name"],
                "emoji": defn["emoji"],
                "delta": delta,
                "summary": "free bet — you keep your wager",
            })
            consumed_ids.append(eff["id"])
            consumed_item_ids.add(iid)
            continue

        # Re-eval net with current final_payout for win/loss checks
        cur_net = final_payout - bet

        if applies_on == "loss" and cur_net < 0:
            if iid == "insurance":
                # Full refund — make payout == bet
                delta = bet - final_payout
                final_payout += delta
                applied.append({
                    "item_id": iid,
                    "name": defn["name"],
                    "emoji": defn["emoji"],
                    "delta": delta,
                    "summary": "loss fully refunded",
                })
                consumed_ids.append(eff["id"])
                consumed_item_ids.add(iid)
                continue
            if iid == "cooler":
                # 50% refund on the loss amount
                loss_amount = -cur_net
                delta = int(round(loss_amount * COOLER_REFUND_PCT))
                final_payout += delta
                applied.append({
                    "item_id": iid,
                    "name": defn["name"],
                    "emoji": defn["emoji"],
                    "delta": delta,
                    "summary": f"50% loss softened (+${delta:,})",
                })
                consumed_ids.append(eff["id"])
                consumed_item_ids.add(iid)
                continue

        if applies_on == "win" and cur_net > 0:
            if iid == "drink":
                # +25% on the profit portion
                bonus = int(round(cur_net * DRINK_BONUS_PCT))
                final_payout += bonus
                applied.append({
                    "item_id": iid,
                    "name": defn["name"],
                    "emoji": defn["emoji"],
                    "delta": bonus,
                    "summary": f"win bonus +${bonus:,}",
                })
                consumed_ids.append(eff["id"])
                consumed_item_ids.add(iid)
                continue

    return final_payout, applied, consumed_ids
