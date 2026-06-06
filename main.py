import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone, timedelta
import config
from services.llm_service import LLMService
from services.news_service import NewsService
from services.hn_service import HNService
from services.arxiv_service import ArxivService
from services.kafka_producer import KafkaProducer
from services.stats_service import StatsService
from services.sentiment_service import SentimentService
from services.danbooru_service import DanbooruService
from services.mimic_service import MimicService
from services.gambling_service import GamblingService
from services import search_service
from services import casino_games
from services import casino_items

# Malaysian Time = UTC+8
MYT = timezone(timedelta(hours=8))

# Intents: default + message_content + voice + members + presences
# NOTE: members and presences are privileged — enable them in Discord Developer Portal:
# https://discord.com/developers/applications > Bot > Privileged Gateway Intents
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.members = True
intents.presences = True

bot = commands.Bot(command_prefix="!", intents=intents)
llm_service = LLMService()
news_service = NewsService()
hn_service = HNService()
arxiv_service = ArxivService()
kafka = KafkaProducer()
stats_service = StatsService()
sentiment_svc = SentimentService()
danbooru = DanbooruService()
mimic_svc = MimicService()
gambling = GamblingService()


async def _enforce_casino_channel(ctx):
    """Restrict casino commands to CASINO_CHANNEL_ID. Returns True if allowed."""
    if not config.CASINO_CHANNEL_ID:
        await ctx.send("Casino is disabled (CASINO_CHANNEL_ID not configured).", ephemeral=True)
        return False
    if ctx.channel.id != config.CASINO_CHANNEL_ID:
        channel = bot.get_channel(config.CASINO_CHANNEL_ID)
        mention = channel.mention if channel else f"<#{config.CASINO_CHANNEL_ID}>"
        await ctx.send(f"Casino commands only work in {mention}.", ephemeral=True)
        return False
    return True


def _format_applied_effects(applied):
    """Render passive effects triggered by a bet as a multi-line string. None if empty."""
    if not applied:
        return None
    lines = []
    for a in applied:
        delta = a.get("delta") or 0
        if delta:
            sign = "+" if delta >= 0 else "−"
            lines.append(f"{a['emoji']}  **{a['name']}** — {a['summary']} ({sign}${abs(delta):,})")
        else:
            lines.append(f"{a['emoji']}  **{a['name']}** — {a['summary']}")
    return "\n".join(lines)


def _augment_settle_description(desc, outcome):
    """Append item-effect and ticket-bonus blocks to a bet's settle description."""
    extras = []
    block = _format_applied_effects(outcome.get("effects_applied"))
    if block:
        extras.append(f"\n**Items used:**\n{block}")
    if outcome.get("ticket_bonus"):
        extras.append(f"\n🎟️ +{outcome['ticket_bonus']} tickets (jackpot!)")
    return desc + "".join(extras)

# Store recent news context keyed by message ID for follow-up queries
_news_context = {}

def _build_links_field(items, formatter, max_chars=1024):
    """
    Builds a link field string by adding items one-by-one,
    stopping before exceeding Discord's field character limit.
    """
    lines = []
    total = 0
    for item in items:
        line = formatter(item)
        # +1 for the newline separator
        if total + len(line) + 1 > max_chars:
            break
        lines.append(line)
        total += len(line) + 1
    return "\n".join(lines)


NEWS_SYSTEM_PROMPT = (
    "You are a tech news reporter. Summarize the following headlines into a concise daily digest. "
    "Group related headlines by category (e.g. 📱 Mobile, 🎮 Gaming, 🔒 Security, 💻 Software, 🤖 AI, 🛠️ Hardware). "
    "For each headline, write a numbered item with a one-line summary. "
    "Keep it concise and engaging. Use emoji for category headers."
)

HN_SYSTEM_PROMPT = (
    "You are a developer community reporter. Summarize the following Hacker News stories into a concise digest. "
    "Group related stories by category (e.g. 🤖 AI/ML, 🦀 Rust, 🐍 Python, 💻 Systems, 🌐 Web, 🚀 Startups, 🔧 DevTools, 📖 Career). "
    "For each story, write a numbered item with a one-line summary of why it's interesting. "
    "Mention the points/upvotes as a quality signal. Keep it concise and engaging."
)

PAPERS_SYSTEM_PROMPT = (
    "You are a research paper curator for a computer science student. Summarize the following arXiv papers into a concise digest. "
    "For each paper, explain: (1) the problem it tackles, (2) the key approach, and (3) why it matters — in 2-3 sentences max. "
    "Group by area (e.g. 🤖 AI, 🧠 Machine Learning, 🗣️ NLP, 👁️ Computer Vision). "
    "Use accessible language — assume the reader knows CS basics but may not be an expert in the paper's specific area."
)


async def post_news_digest(destination):
    """
    Shared logic for posting a news digest to a channel or context.
    Returns the sent message (for context tracking), or None on failure.
    """
    # 1. Fetch News
    result = await news_service.fetch_tech_news(limit=10)

    # Error handling — result is a string on error, list on success
    if isinstance(result, str):
        await destination.send(result)
        return None

    articles = result

    # 2. Summarize via LLM
    llm_input = news_service.format_for_llm(articles)
    prompt = [
        {"role": "system", "content": NEWS_SYSTEM_PROMPT},
        {"role": "user", "content": llm_input}
    ]
    summary = await llm_service.generate_response(prompt)

    # 3. Build Discord Embed
    today = datetime.now(MYT).strftime("%B %d, %Y")

    embed = discord.Embed(
        title="📰 Daily Tech News Digest",
        description=summary,
        color=0x5865F2,  # Discord blurple
    )

    # Add source links as a compact field
    links = _build_links_field(
        [(i, art) for i, art in enumerate(articles, 1) if art['url']],
        lambda x: f"[{x[0]}. {x[1]['title'][:50]}{'...' if len(x[1]['title']) > 50 else ''}]({x[1]['url']})"
    )
    if links:
        embed.add_field(name="🔗 Source Links", value=links, inline=False)

    embed.set_footer(text=f"{today} • 💬 Reply to this message to ask about any headline")
    embed.timestamp = datetime.now(MYT)

    # 4. Send
    sent_msg = await destination.send(embed=embed)

    # 5. Store article context for follow-up queries
    _news_context[sent_msg.id] = llm_input

    # Keep only the last 10 news contexts to avoid memory bloat
    if len(_news_context) > 10:
        oldest_key = next(iter(_news_context))
        del _news_context[oldest_key]

    return sent_msg

async def post_hn_digest(destination):
    """
    Shared logic for posting a Hacker News digest.
    Returns the sent message (for context tracking), or None on failure.
    """
    # 1. Fetch stories
    result = await hn_service.fetch_top_stories(limit=10)

    if isinstance(result, str):
        await destination.send(result)
        return None

    stories = result

    # 2. Summarize via LLM
    llm_input = hn_service.format_for_llm(stories)
    prompt = [
        {"role": "system", "content": HN_SYSTEM_PROMPT},
        {"role": "user", "content": llm_input}
    ]
    summary = await llm_service.generate_response(prompt)

    # 3. Build Discord Embed
    today = datetime.now(MYT).strftime("%B %d, %Y")

    embed = discord.Embed(
        title="💻 Hacker News Dev Digest",
        description=summary,
        color=0xFF6600,  # HN orange
    )

    # Add story links
    links = _build_links_field(
        list(enumerate(stories, 1)),
        lambda x: f"[{x[0]}. {x[1]['title'][:50]}{'...' if len(x[1]['title']) > 50 else ''}]({x[1]['url']}) ({x[1]['points']}⬆)"
    )
    if links:
        embed.add_field(name="🔗 Stories & Discussions", value=links, inline=False)

    embed.set_footer(text=f"{today} • 💬 Reply to ask about any story")
    embed.timestamp = datetime.now(MYT)

    # 4. Send
    sent_msg = await destination.send(embed=embed)

    # 5. Store context for follow-ups
    _news_context[sent_msg.id] = llm_input

    if len(_news_context) > 10:
        oldest_key = next(iter(_news_context))
        del _news_context[oldest_key]

    return sent_msg

async def post_papers_digest(destination):
    """
    Shared logic for posting a research paper digest.
    Returns the sent message (for context tracking), or None on failure.
    """
    # 1. Fetch papers
    result = await arxiv_service.fetch_recent_papers(limit=8)

    if isinstance(result, str):
        await destination.send(result)
        return None

    papers = result

    # 2. Summarize via LLM
    llm_input = arxiv_service.format_for_llm(papers)
    prompt = [
        {"role": "system", "content": PAPERS_SYSTEM_PROMPT},
        {"role": "user", "content": llm_input}
    ]
    summary = await llm_service.generate_response(prompt)

    # 3. Build Discord Embed
    today = datetime.now(MYT).strftime("%B %d, %Y")

    embed = discord.Embed(
        title="📄 Research Paper Highlights",
        description=summary,
        color=0x9B59B6,  # Purple for academia
    )

    # Add paper links
    links = _build_links_field(
        [(i, p) for i, p in enumerate(papers, 1) if p['url']],
        lambda x: f"[{x[0]}. {x[1]['title'][:50]}{'...' if len(x[1]['title']) > 50 else ''}]({x[1]['url']})"
    )
    if links:
        embed.add_field(name="🔗 Read the Papers", value=links, inline=False)

    embed.set_footer(text=f"{today} • 💬 Reply to ask about any paper")
    embed.timestamp = datetime.now(MYT)

    # 4. Send
    sent_msg = await destination.send(embed=embed)

    # 5. Store context for follow-ups
    _news_context[sent_msg.id] = llm_input

    if len(_news_context) > 10:
        oldest_key = next(iter(_news_context))
        del _news_context[oldest_key]

    return sent_msg

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user} (ID: {bot.user.id})')
    print('------')

    # Connect Kafka producer
    try:
        await kafka.connect()
    except Exception as e:
        print(f'Warning: Kafka not available ({e}). Event streaming disabled.')

    # Connect stats service (for !stats command)
    try:
        await stats_service.connect()
        print('Stats: Connected to PostgreSQL')
    except Exception as e:
        print(f'Warning: Stats DB not available ({e}). !stats command disabled.')

    # Connect sentiment service
    try:
        await sentiment_svc.connect()
        print('Sentiment: Connected to PostgreSQL')
    except Exception as e:
        print(f'Warning: Sentiment DB not available ({e}). !sentiment command disabled.')

    # Connect mimic service
    try:
        await mimic_svc.connect()
        print('Mimic: Connected to PostgreSQL')
    except Exception as e:
        print(f'Warning: Mimic DB not available ({e}). !mimic command disabled.')

    # Connect gambling service
    try:
        await gambling.connect()
        print('Gambling: Connected to PostgreSQL')
    except Exception as e:
        print(f'Warning: Gambling DB not available ({e}). Casino commands disabled.')

    # Start daily schedulers
    if config.NEWS_CHANNEL_ID:
        if not daily_news.is_running():
            daily_news.start()
            print(f'Daily news scheduled for 9:00 AM MYT in channel {config.NEWS_CHANNEL_ID}')
        if not daily_hn.is_running():
            daily_hn.start()
            print(f'Daily HN digest scheduled for 9:15 AM MYT in channel {config.NEWS_CHANNEL_ID}')
        if not daily_papers.is_running():
            daily_papers.start()
            print(f'Daily papers scheduled for 9:30 AM MYT in channel {config.NEWS_CHANNEL_ID}')

    if config.DANBOORU_CHANNEL_ID:
        if not daily_danbooru.is_running():
            daily_danbooru.start()
            print(f'Daily Danbooru scheduled for 9:45 PM MYT in channel {config.DANBOORU_CHANNEL_ID}')

    if config.CASINO_CHANNEL_ID:
        if not daily_casino_settle.is_running():
            daily_casino_settle.start()
            print(f'Daily casino settle scheduled for midnight MYT in channel {config.CASINO_CHANNEL_ID}')

    # Sync slash commands with Discord
    try:
        synced = await bot.tree.sync()
        print(f'Synced {len(synced)} slash commands')
    except Exception as e:
        print(f'Failed to sync slash commands: {e}')

@tasks.loop(time=time(hour=9, minute=0, tzinfo=MYT))
async def daily_news():
    """Automatically posts tech news digest at 9:00 AM MYT every day."""
    channel = bot.get_channel(config.NEWS_CHANNEL_ID)
    if not channel:
        print(f"Error: Could not find channel {config.NEWS_CHANNEL_ID}")
        return
    await post_news_digest(channel)

@tasks.loop(time=time(hour=9, minute=15, tzinfo=MYT))
async def daily_hn():
    """Automatically posts Hacker News digest at 9:15 AM MYT every day."""
    channel = bot.get_channel(config.NEWS_CHANNEL_ID)
    if not channel:
        print(f"Error: Could not find channel {config.NEWS_CHANNEL_ID}")
        return
    await post_hn_digest(channel)

@tasks.loop(time=time(hour=9, minute=30, tzinfo=MYT))
async def daily_papers():
    """Automatically posts research paper highlights at 9:30 AM MYT every day."""
    channel = bot.get_channel(config.NEWS_CHANNEL_ID)
    if not channel:
        print(f"Error: Could not find channel {config.NEWS_CHANNEL_ID}")
        return
    await post_papers_digest(channel)

@tasks.loop(time=time(hour=21, minute=45, tzinfo=MYT))
async def daily_danbooru():
    """Automatically posts top Danbooru art at 9:45 PM MYT every day."""
    channel = bot.get_channel(config.DANBOORU_CHANNEL_ID)
    if not channel:
        print(f"Error: Could not find Danbooru channel {config.DANBOORU_CHANNEL_ID}")
        return
    await post_danbooru_digest(channel)


@tasks.loop(time=time(hour=0, minute=0, tzinfo=MYT))
async def daily_casino_settle():
    """Run the casino quota check at midnight MYT and post a summary."""
    if not config.CASINO_CHANNEL_ID:
        return
    channel = bot.get_channel(config.CASINO_CHANNEL_ID)
    if not channel:
        print(f"Error: casino channel {config.CASINO_CHANNEL_ID} not found")
        return

    guild_id = channel.guild.id
    result = await gambling.settle_day(guild_id)
    if not result:
        return  # no bank row yet — nobody's played

    now = datetime.now(MYT)
    if result["outcome"] == "survived":
        day_start = now - timedelta(days=1)
        stats = await gambling.get_player_stats(guild_id, day_start, now)
        await _post_settle_survived(channel, result, stats)
    else:
        run_stats = await gambling.get_player_stats(
            guild_id, result["streak_started_at"], now,
        )
        await _post_settle_reset(channel, result, run_stats)


def _format_player_stats(stats):
    """Render a leaderboard line list from get_player_stats output."""
    lines = []
    for i, s in enumerate(stats, 1):
        net = s["net_total"] or 0
        sign = "+" if net >= 0 else "−"
        wins = s["wins"] or 0
        losses = s["losses"] or 0
        lines.append(
            f"`{i}.` **{s['user_name']}**: {sign}${abs(net):,}  "
            f"({s['bet_count']} bets, {wins}W / {losses}L)"
        )
    return "\n".join(lines)


async def _post_settle_survived(channel, result, stats):
    bonus = result.get("tickets_awarded", 0)
    total_tickets = result.get("tickets_after", 0)
    embed = discord.Embed(
        title=f"\U0001f319 Day {result['old_day']} Settled — Survived",
        description=(
            f"Bank: **${result['bank_before']:,}** → paid **${result['debt_paid']:,}** debt → "
            f"**${result['carryover']:,}** carries into Day {result['new_day']}\n\n"
            f"Day {result['new_day']} quota: **${result['next_debt']:,}**\n"
            f"🎟️ **+{bonus}** tickets (total: {total_tickets})"
        ),
        color=0x2ECC71,
    )
    if stats:
        embed.add_field(name="📊 Today's Top 5", value=_format_player_stats(stats[:5]), inline=False)
    else:
        embed.add_field(name="📊 Today", value="_No bets placed_", inline=False)
    embed.timestamp = datetime.now(MYT)
    await channel.send(embed=embed)


async def _post_settle_reset(channel, result, run_stats):
    started = result["streak_started_at"]
    duration_days = result["old_day"]
    lost = result.get("tickets_lost", 0)
    tickets_line = f"\n🎟️ **{lost}** tickets lost • Inventory wiped" if lost > 0 else "\nInventory wiped on bust."
    embed = discord.Embed(
        title=f"\U0001f480 RUN ENDED — Day {result['old_day']} Bust",
        description=(
            f"Bank was **${result['bank_before']:,}**, needed **${result['debt_owed']:,}**. "
            f"Short by **${result['missing']:,}**.\n\n"
            f"Run lasted **{duration_days}** day{'s' if duration_days != 1 else ''} since "
            f"{started.strftime('%Y-%m-%d')}.{tickets_line}\n\n"
            f"🔄 Fresh run starts: **${result['seed']:,}** seed, "
            f"Day 1 quota **${config.GAMBLING_BASE_DEBT:,}**"
        ),
        color=0xE74C3C,
    )
    if run_stats:
        embed.add_field(
            name="🏆 Final Run Leaderboard",
            value=_format_player_stats(run_stats[:10]),
            inline=False,
        )
    else:
        embed.add_field(name="📊 Stats", value="_No bets were placed this run_", inline=False)
    embed.timestamp = datetime.now(MYT)
    await channel.send(embed=embed)


@bot.hybrid_command(description="Check if the bot is alive")
async def ping(ctx):
    await ctx.send('Pong!')


async def post_danbooru_digest(destination):
    """Fetch and post top Danbooru art."""
    posts = await danbooru.get_top_posts(
        tags=config.DANBOORU_DEFAULT_TAGS,
        limit=5,
    )

    if not posts:
        await destination.send("No Danbooru posts found for today. Try again later!")
        return

    header = discord.Embed(
        title="\U0001f3a8 Today's Top Danbooru Art",
        color=0xE91E63,  # Pink
        url="https://danbooru.donmai.us",
    )

    for i, post in enumerate(posts, 1):
        # Character/copyright info
        info_parts = []
        if post['character']:
            info_parts.append(post['character'].split(' ')[0])  # First character
        if post['copyright']:
            info_parts.append(post['copyright'].split(' ')[0])  # First copyright
        info = " \u2022 ".join(info_parts) if info_parts else "Original"

        header.add_field(
            name=f"{i}. {info} (Score: {post['score']})",
            value=f"[View Post]({post['page_url']}) \u2022 Artist: {post['artist'].split(' ')[0]} \u2022 {danbooru.rating_emoji(post['rating'])}",
            inline=False,
        )

    # First image goes on the header embed itself
    header.set_image(url=posts[0]['file_url'])
    header.set_footer(text="Powered by Danbooru API")
    header.timestamp = datetime.now(MYT)

    # Additional image embeds share the same url so Discord renders them as a gallery
    embeds = [header]
    for post in posts[1:]:
        img_embed = discord.Embed(url="https://danbooru.donmai.us", color=0xE91E63)
        img_embed.set_image(url=post['file_url'])
        embeds.append(img_embed)

    await destination.send(embeds=embeds)


@bot.hybrid_command(name="danbooru", description="Fetch a top Danbooru post with optional tag search")
async def danbooru_cmd(ctx, *, tags: str = ""):
    """Fetches a top Danbooru post. Usage: !danbooru [tags]"""
    async with ctx.typing():
        correction_msg = ""

        # Check for nsfw flag
        tag_list = tags.strip().split()
        rating = None
        if "nsfw" in tag_list:
            rating = "explicit"
            tag_list.remove("nsfw")
            tags = " ".join(tag_list)

        if tags:
            # Fuzzy-match tags via Danbooru autocomplete
            resolved, corrections = await danbooru.resolve_tags(tags)
            tags = resolved

            if corrections:
                fixes = ", ".join(f"**{orig}** → **{fixed}**" for orig, fixed in corrections)
                correction_msg = f"🔍 Auto-corrected: {fixes}\n"

            post = await danbooru.get_random_top_post(tags=tags, rating=rating)
        else:
            post = await danbooru.get_random_top_post(rating=rating)

        if not post:
            await ctx.send("No posts found. Try different tags or try again later.")
            return

        # Character/copyright info
        info_parts = []
        if post['character']:
            info_parts.append(post['character'].replace(' ', ', '))
        if post['copyright']:
            info_parts.append(post['copyright'].replace(' ', ', '))
        info = " \u2022 ".join(info_parts) if info_parts else "Original"

        embed = discord.Embed(
            title=f"\U0001f3a8 {info}",
            url=post['page_url'],
            color=0xE91E63,  # Pink
        )
        embed.set_image(url=post['file_url'])
        embed.add_field(
            name="Details",
            value=f"Score: **{post['score']}** \u2022 Artist: **{post['artist'].split(' ')[0]}** \u2022 {danbooru.rating_emoji(post['rating'])}",
            inline=False,
        )
        if tags:
            embed.set_footer(text=f"Tags: {tags} \u2022 Powered by Danbooru")
        else:
            embed.set_footer(text="Powered by Danbooru")
        embed.timestamp = datetime.now(MYT)

        if correction_msg:
            await ctx.send(correction_msg, embed=embed)
        else:
            await ctx.send(embed=embed)

@bot.hybrid_command(description="Fetch the latest tech news digest")
async def news(ctx):
    """Fetches the latest tech news and summarizes it using the LLM."""
    async with ctx.typing():
        await post_news_digest(ctx)

@bot.hybrid_command(description="Fetch top Hacker News stories")
async def hn(ctx):
    """Fetches top Hacker News stories and summarizes them. Usage: !hn"""
    async with ctx.typing():
        await post_hn_digest(ctx)

@bot.hybrid_command(description="Fetch latest CS/AI research papers from arXiv")
async def papers(ctx):
    """Fetches latest CS/AI research papers from arXiv. Usage: !papers"""
    async with ctx.typing():
        await post_papers_digest(ctx)

@bot.hybrid_command(description="Search the web and get an AI-grounded answer")
async def search(ctx, *, query: str):
    """Searches the web and provides a grounded answer. Usage: !search <query>"""
    async with ctx.typing():
        # Search the web
        results = await search_service.search_web(query, max_results=5)

        if not results:
            await ctx.send("Sorry, I couldn't find any results for that query.")
            return

        # Ask LLM to answer grounded in search results
        prompt = [
            {"role": "system", "content": (
                "You are a helpful assistant. Answer the user's question using ONLY the search results provided below. "
                "Cite your sources by referencing the result number. If the results don't contain enough info, say so.\n\n"
                + results
            )},
            {"role": "user", "content": query}
        ]
        response = await llm_service.generate_response(prompt)

        # Send as embed for consistency
        embed = discord.Embed(
            title=f"🔍 {query[:100]}",
            description=response,
            color=0x2ECC71,  # Green
        )
        embed.set_footer(text="Powered by DuckDuckGo + Groq")
        await ctx.send(embed=embed)

@bot.hybrid_command(description="Show server activity stats")
async def stats(ctx):
    """Shows server activity stats. Usage: !stats"""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    async with ctx.typing():
        data = await stats_service.get_server_stats(ctx.guild.id)
        if not data:
            await ctx.send("Analytics not available. Make sure PostgreSQL is running.")
            return

        today = datetime.now(MYT).strftime("%B %d, %Y")

        embed = discord.Embed(
            title=f"📊 Server Activity — {ctx.guild.name}",
            color=0x3498DB,  # Blue
        )

        # Overview
        embed.add_field(
            name="📨 Messages",
            value=f"Today: **{data['messages_today']}**\nThis week: **{data['messages_week']}**\nAll time: **{data['total_messages']}**",
            inline=True,
        )
        embed.add_field(
            name="👥 Activity",
            value=f"Active users (week): **{data['active_users_week']}**\nVoice joins (week): **{data['voice_joins_week']}**",
            inline=True,
        )

        # Top users
        if data['top_users']:
            users_text = "\n".join(f"`{i}.` {u} — {c} msgs" for i, (u, c) in enumerate(data['top_users'], 1))
            embed.add_field(name="🏆 Top Users (Week)", value=users_text, inline=False)

        # Top channels
        if data['top_channels']:
            channels_text = "\n".join(f"`{i}.` #{ch} — {c} msgs" for i, (ch, c) in enumerate(data['top_channels'], 1))
            embed.add_field(name="💬 Top Channels (Week)", value=channels_text, inline=False)

        # Top games
        if data['top_games']:
            games_text = "\n".join(f"`{i}.` {g} ({c}x)" for i, (g, c) in enumerate(data['top_games'], 1))
            embed.add_field(name="🎮 Most Played Games (Week)", value=games_text, inline=False)

        embed.set_footer(text=f"{today} • Data powered by Kafka + PostgreSQL")
        embed.timestamp = datetime.now(MYT)
        await ctx.send(embed=embed)

@bot.hybrid_command(description="Show server sentiment analysis")
async def sentiment(ctx):
    """Shows server sentiment analysis. Usage: !sentiment"""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    async with ctx.typing():
        data = await sentiment_svc.get_channel_sentiment(ctx.guild.id)
        if not data:
            await ctx.send("No sentiment data available yet. Messages need to be scored first.")
            return

        # Emoji for each score
        score_emoji = {1: "😡", 2: "😟", 3: "😐", 4: "😊", 5: "😄"}
        score_label = {1: "Very Negative", 2: "Negative", 3: "Neutral", 4: "Positive", 5: "Very Positive"}

        # Overall score emoji
        avg = data['avg_score']
        if avg >= 4.5:
            mood = "😄 Very Positive"
        elif avg >= 3.5:
            mood = "😊 Positive"
        elif avg >= 2.5:
            mood = "😐 Neutral"
        elif avg >= 1.5:
            mood = "😟 Negative"
        else:
            mood = "😡 Very Negative"

        embed = discord.Embed(
            title=f"🧠 Sentiment Analysis — {ctx.guild.name}",
            description=f"**Overall Mood: {mood}**\nAverage Score: **{avg}/5.0**\nMessages Scored: **{data['total_scored']}**",
            color=0x9B59B6,  # Purple
        )

        # Distribution bar chart
        dist = data['distribution']
        total = data['total_scored']
        bars = []
        for score in range(1, 6):
            count = dist.get(score, 0)
            pct = (count / total * 100) if total > 0 else 0
            bar_len = int(pct / 5)  # Scale to max 20 chars
            bar = "█" * bar_len
            bars.append(f"{score_emoji[score]} {score_label[score]}: {bar} {count} ({pct:.0f}%)")

        embed.add_field(name="📊 Sentiment Distribution", value="\n".join(bars), inline=False)

        # Channel rankings
        rankings = await sentiment_svc.get_channel_rankings(ctx.guild.id)
        if rankings:
            rank_text = "\n".join(
                f"`{i}.` #{ch} — {s}/5.0 ({n} msgs)"
                for i, (ch, s, n) in enumerate(rankings[:5], 1)
            )
            embed.add_field(name="🏆 Happiest Channels", value=rank_text, inline=False)

        embed.set_footer(text="Sentiment scored by LLM • Updated periodically")
        embed.timestamp = datetime.now(MYT)
        await ctx.send(embed=embed)

@bot.hybrid_command(description="Link to the Superset analytics dashboard")
async def dashboard(ctx):
    """Links to the Superset analytics dashboard. Usage: !dashboard"""
    superset_url = config.SUPERSET_URL
    if not superset_url:
        await ctx.send("Dashboard URL not configured. Set `SUPERSET_URL` in your .env file.")
        return

    embed = discord.Embed(
        title="📊 Analytics Dashboard",
        description=f"View the full server analytics dashboard:\n\n🔗 **[Open Dashboard]({superset_url})**",
        color=0x2ECC71,  # Green
    )
    embed.add_field(
        name="Available Views",
        value=(
            "• 📈 Activity trends\n"
            "• 🏆 User leaderboard\n"
            "• 🔥 Hourly activity heatmap\n"
            "• 🎮 Gaming stats\n"
            "• 💬 Channel health\n"
            "• 🧠 Sentiment analysis"
        ),
        inline=False,
    )
    embed.set_footer(text="Powered by Apache Superset + PostgreSQL")
    await ctx.send(embed=embed)

@bot.hybrid_command(description="Summarize recent messages in this channel")
async def tldr(ctx, count: int = 50):
    """Summarizes recent messages in this channel. Usage: !tldr [count]"""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    count = max(10, min(count, 200))  # Clamp between 10-200

    async with ctx.typing():
        # Fetch recent messages from the channel
        messages = []
        async for msg in ctx.channel.history(limit=count + 5):  # +5 to account for bot/command msgs
            # Skip bot messages and commands
            if msg.author.bot or msg.content.startswith("!"):
                continue
            if msg.content.strip():
                author = msg.author.display_name
                messages.append(f"{author}: {msg.content[:200]}")
            if len(messages) >= count:
                break

        if len(messages) < 3:
            await ctx.send("Not enough messages to summarize.")
            return

        # Reverse to chronological order
        messages.reverse()

        # Build LLM prompt
        conversation = "\n".join(messages)
        prompt = [
            {
                "role": "system",
                "content": (
                    "You are a concise conversation summarizer. Summarize the following Discord conversation "
                    "in 3-5 bullet points. Focus on the main topics discussed, key decisions, and any notable "
                    "moments. Keep it brief and casual. Use Discord usernames when referencing people."
                ),
            },
            {"role": "user", "content": f"Summarize this conversation:\n\n{conversation}"},
        ]

        summary = await llm_service.generate_response(prompt)

        embed = discord.Embed(
            title=f"📝 TL;DR — #{ctx.channel.name}",
            description=summary,
            color=0xE67E22,  # Orange
        )
        embed.set_footer(text=f"Summarized {len(messages)} messages • Powered by Groq")
        embed.timestamp = datetime.now(MYT)
        await ctx.send(embed=embed)

@bot.hybrid_command(description="Mimic a user's speech style")
async def mimic(ctx, user: discord.Member, *, prompt: str = ""):
    """Mimic a user's speech pattern. Usage: !mimic @user [optional prompt]"""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return

    # Handle profile rebuild subcommand
    if prompt.strip().lower() == "profile":
        async with ctx.typing():
            msg_count = await mimic_svc.get_message_count(user.id, ctx.guild.id)
            if msg_count < 10:
                await ctx.send(f"\u26a0\ufe0f {user.display_name} doesn't have enough messages yet ({msg_count}/10 minimum).")
                return

            profile = await mimic_svc.build_style_profile(user.id, ctx.guild.id)
            if profile:
                embed = discord.Embed(
                    title=f"\ud83c\udfad Style Profile \u2014 {user.display_name}",
                    description=profile,
                    color=0x9B59B6,
                )
                embed.set_footer(text=f"Analyzed {msg_count} messages")
                await ctx.send(embed=embed)
            else:
                await ctx.send("Failed to build profile. Try again later.")
        return

    if user.bot:
        await ctx.send("I can't mimic other bots! \ud83e\udd16")
        return

    async with ctx.typing():
        # Check message count
        msg_count = await mimic_svc.get_message_count(user.id, ctx.guild.id)
        if msg_count < 10:
            await ctx.send(
                f"\u26a0\ufe0f {user.display_name} doesn't have enough messages yet "
                f"({msg_count}/10 minimum). Keep chatting and try again later!"
            )
            return

        # If no prompt given, read recent channel messages for context
        conversation_context = None
        if not prompt:
            messages_list = []
            async for msg in ctx.channel.history(limit=25):
                if msg.author.bot or msg.content.startswith("!") or msg.content.startswith("/"):
                    continue
                if msg.content.strip():
                    messages_list.append(f"{msg.author.display_name}: {msg.content[:200]}")
                if len(messages_list) >= 20:
                    break
            messages_list.reverse()  # Chronological order
            if messages_list:
                conversation_context = messages_list

        response = await mimic_svc.mimic_user(
            user.id, ctx.guild.id,
            prompt_text=prompt if prompt else None,
            conversation_context=conversation_context,
        )

        if not response:
            await ctx.send("Couldn't generate a response. The user might not have enough message history.")
            return

        embed = discord.Embed(
            description=response,
            color=user.color if user.color != discord.Color.default() else 0x95A5A6,
        )
        embed.set_author(
            name=f"{user.display_name} (mimicked)",
            icon_url=user.display_avatar.url,
        )
        embed.set_footer(text=f"Responding to: {prompt[:80]}")
        await ctx.send(embed=embed)

class SlotBetView(discord.ui.View):
    """Bet-size picker for /slots. Only the caller can click."""

    def __init__(self, ctx, day_start_bank, current_bank):
        super().__init__(timeout=60)
        self.ctx = ctx
        options = gambling.compute_bet_options(day_start_bank, current_bank, game="slots")
        # Store the actual capped amounts so _spin uses the per-game cap, not raw day_start.
        self.amounts = {
            "quarter": options["quarter"]["amount"],
            "half":    options["half"]["amount"],
            "max":     options["max"]["amount"],
        }

        self.quarter.label = f"¼  ${options['quarter']['amount']:,}"
        self.quarter.disabled = not options['quarter']['enabled']
        self.half.label = f"½  ${options['half']['amount']:,}"
        self.half.disabled = not options['half']['enabled']
        self.max_bet.label = f"MAX  ${options['max']['amount']:,}"
        self.max_bet.disabled = not options['max']['enabled']

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "That bet button isn't yours — run `/slots` to play your own spin.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    @discord.ui.button(style=discord.ButtonStyle.secondary)
    async def quarter(self, interaction, button):
        await self._spin(interaction, self.amounts["quarter"])

    @discord.ui.button(style=discord.ButtonStyle.primary)
    async def half(self, interaction, button):
        await self._spin(interaction, self.amounts["half"])

    @discord.ui.button(style=discord.ButtonStyle.danger)
    async def max_bet(self, interaction, button):
        await self._spin(interaction, self.amounts["max"])

    async def _spin(self, interaction, bet):
        # Lock all buttons before we touch state
        for child in self.children:
            child.disabled = True

        result = casino_games.spin_slots()
        payout = bet * result["multiplier"]

        outcome = await gambling.apply_bet(
            guild_id=self.ctx.guild.id,
            user_id=self.ctx.author.id,
            user_name=self.ctx.author.display_name,
            game="slots",
            bet=bet,
            payout=payout,
            metadata={
                "reels": [r["name"] for r in result["reels"]],
                "multiplier": result["multiplier"],
            },
        )

        if outcome is None or outcome.get("error"):
            err = (outcome or {}).get("error", "DB unavailable")
            embed = discord.Embed(
                title="\U0001f3b0 Slots — Bet failed",
                description=f"Couldn't place bet: `{err}`. Bank may have shifted since you opened this view.",
                color=0xE74C3C,
            )
            await interaction.response.edit_message(embed=embed, view=self)
            return

        net = outcome["net"]
        new_bank = outcome["new_bank"]
        reels_line = casino_games.format_reels(result["reels"])

        if result["win"]:
            color = 0x2ECC71
            title = f"\U0001f3b0 {self.ctx.author.display_name} hit {result['match_name']}s!"
            desc = (
                f"## {reels_line}\n"
                f"**+${net:,}** ({result['multiplier']}x payout on ${bet:,})\n"
                f"Bank: **${new_bank:,}**"
            )
        else:
            if net >= 0:
                color = 0x95A5A6
                amount_line = "_rescued — broke even_" if net == 0 else f"**+${net:,}** (item bonus)"
            else:
                color = 0xE74C3C
                amount_line = f"**−${abs(net):,}**"
            title = f"\U0001f3b0 {self.ctx.author.display_name} spun and lost"
            desc = (
                f"## {reels_line}\n"
                f"No match. {amount_line}\n"
                f"Bank: **${new_bank:,}**"
            )

        desc = _augment_settle_description(desc, outcome)
        embed = discord.Embed(title=title, description=desc, color=color)
        await interaction.response.edit_message(embed=embed, view=self)


class _DiceRollButton(discord.ui.Button):
    """Standalone Roll button used during the dice point phase."""

    def __init__(self, rolls_remaining):
        super().__init__(
            label=f"🎲 Roll  ({rolls_remaining} left)",
            style=discord.ButtonStyle.primary,
        )

    async def callback(self, interaction):
        view: DiceView = self.view
        await view._roll_point(interaction)


class DiceView(discord.ui.View):
    """Stateful craps view: bet picker → come-out roll → optional point phase."""

    def __init__(self, ctx, day_start_bank, current_bank):
        super().__init__(timeout=120)
        self.ctx = ctx
        options = gambling.compute_bet_options(day_start_bank, current_bank, game="dice")
        self.amounts = {
            "quarter": options["quarter"]["amount"],
            "half":    options["half"]["amount"],
            "max":     options["max"]["amount"],
        }
        self.bet = 0
        self.point = None
        self.rolls = []           # list of (d1, d2, total) for point phase
        self.comeout = None       # (d1, d2, total) for record

        self.quarter.label = f"¼  ${options['quarter']['amount']:,}"
        self.quarter.disabled = not options['quarter']['enabled']
        self.half.label = f"½  ${options['half']['amount']:,}"
        self.half.disabled = not options['half']['enabled']
        self.max_bet.label = f"MAX  ${options['max']['amount']:,}"
        self.max_bet.disabled = not options['max']['enabled']

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "That dice game isn't yours — run `/dice` to start your own.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    @discord.ui.button(style=discord.ButtonStyle.secondary)
    async def quarter(self, interaction, button):
        await self._comeout(interaction, self.amounts["quarter"])

    @discord.ui.button(style=discord.ButtonStyle.primary)
    async def half(self, interaction, button):
        await self._comeout(interaction, self.amounts["half"])

    @discord.ui.button(style=discord.ButtonStyle.danger)
    async def max_bet(self, interaction, button):
        await self._comeout(interaction, self.amounts["max"])

    async def _comeout(self, interaction, bet):
        self.bet = bet
        roll = casino_games.dice_comeout()
        self.comeout = (roll["d1"], roll["d2"], roll["total"])

        if roll["outcome"] == "win":
            await self._settle(interaction, payout=int(round(bet * casino_games.DICE_WIN_PAYOUT)),
                               reason="instant_win", final_roll=roll)
            return
        if roll["outcome"] == "lose":
            await self._settle(interaction, payout=0, reason="instant_lose", final_roll=roll)
            return

        # Point set — swap to roll button
        self.point = roll["point"]
        self.clear_items()
        self.add_item(_DiceRollButton(casino_games.DICE_POINT_MAX_ROLLS))

        embed = discord.Embed(
            title=f"\U0001f3b2 Dice — ${bet:,} bet • {self.ctx.author.display_name}",
            description=(
                f"Come-out: {casino_games.format_dice(roll['d1'], roll['d2'])}\n"
                f"**Point is {self.point}.** Roll it again to win — but roll a 7 and you bust.\n"
                f"You have **{casino_games.DICE_POINT_MAX_ROLLS}** rolls."
            ),
            color=0xF39C12,
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def _roll_point(self, interaction):
        roll = casino_games.dice_point_roll(self.point)
        self.rolls.append((roll["d1"], roll["d2"], roll["total"]))

        if roll["outcome"] == "win":
            payout = int(round(self.bet * casino_games.DICE_WIN_PAYOUT))
            await self._settle(interaction, payout=payout, reason="point_win", final_roll=roll)
            return
        if roll["outcome"] == "lose":
            await self._settle(interaction, payout=0, reason="seven_out", final_roll=roll)
            return

        # Continue. If we just exhausted our rolls → refund.
        if len(self.rolls) >= casino_games.DICE_POINT_MAX_ROLLS:
            await self._settle(interaction, payout=self.bet, reason="refund", final_roll=roll)
            return

        # Still rolls left — refresh the roll button with the new counter
        rolls_left = casino_games.DICE_POINT_MAX_ROLLS - len(self.rolls)
        self.clear_items()
        self.add_item(_DiceRollButton(rolls_left))

        history = "\n".join(
            f"Roll {i+1}: {casino_games.format_dice(d1, d2)}"
            for i, (d1, d2, _) in enumerate(self.rolls)
        )
        embed = discord.Embed(
            title=f"\U0001f3b2 Dice — ${self.bet:,} bet • {self.ctx.author.display_name}",
            description=(
                f"Come-out: {casino_games.format_dice(*self.comeout[:2])}\n"
                f"**Point: {self.point}**\n\n{history}\n\n"
                f"Neither point nor 7 yet. **{rolls_left}** rolls left."
            ),
            color=0xF39C12,
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def _settle(self, interaction, payout, reason, final_roll):
        # Lock all buttons
        for child in self.children:
            child.disabled = True

        outcome = await gambling.apply_bet(
            guild_id=self.ctx.guild.id,
            user_id=self.ctx.author.id,
            user_name=self.ctx.author.display_name,
            game="dice",
            bet=self.bet,
            payout=payout,
            metadata={
                "comeout": list(self.comeout),
                "point": self.point,
                "point_rolls": [list(r) for r in self.rolls],
                "result": reason,
            },
        )

        if outcome is None or outcome.get("error"):
            err = (outcome or {}).get("error", "DB unavailable")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="\U0001f3b2 Dice — Bet failed",
                    description=f"Couldn't settle bet: `{err}`.",
                    color=0xE74C3C,
                ),
                view=self,
            )
            return

        net = outcome["net"]
        new_bank = outcome["new_bank"]

        # Build the history block
        lines = [f"Come-out: {casino_games.format_dice(*self.comeout[:2])}"]
        for i, (d1, d2, _) in enumerate(self.rolls):
            lines.append(f"Roll {i+1}: {casino_games.format_dice(d1, d2)}")
        history = "\n".join(lines)

        labels = {
            "instant_win": ("🎯 LUCKY 7/11 — INSTANT WIN", 0x2ECC71),
            "instant_lose": ("💀 CRAPS — INSTANT LOSE", 0xE74C3C),
            "point_win":   (f"✅ HIT THE POINT ({self.point})", 0x2ECC71),
            "seven_out":   ("💀 SEVEN OUT", 0xE74C3C),
            "refund":      ("⏸️ NO HIT IN 3 ROLLS — REFUND", 0x95A5A6),
        }
        title_tag, color = labels[reason]

        if net > 0:
            settle_line = f"**+${net:,}** ({casino_games.DICE_WIN_PAYOUT}x payout on ${self.bet:,})"
        elif net < 0:
            settle_line = f"**−${abs(net):,}**"
        else:
            settle_line = f"**${self.bet:,} refunded**"

        desc = _augment_settle_description(
            f"{history}\n\n## {title_tag}\n{settle_line}\nBank: **${new_bank:,}**",
            outcome,
        )
        embed = discord.Embed(
            title=f"\U0001f3b2 Dice — ${self.bet:,} bet • {self.ctx.author.display_name}",
            description=desc,
            color=color,
        )
        await interaction.response.edit_message(embed=embed, view=self)


@bot.hybrid_command(description="Play craps — roll 2 dice, 7/11 wins, 2/3/12 loses, else chase your point")
async def dice(ctx):
    """Open a craps bet picker."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available. Try again later.")
        return

    bank = state["bank"]
    day_start = state["day_start_bank"]
    if bank <= 0:
        await ctx.send("\U0001f4b8 Bank is empty. Wait for midnight settle.")
        return

    embed = discord.Embed(
        title="\U0001f3b2 Craps Dice",
        description=(
            f"Bank: **${bank:,}**  •  Day-start cap: **${day_start:,}**\n\n"
            "**Rules:**\n"
            "• Come-out roll: **7 or 11** → instant win, **2/3/12** → instant lose\n"
            f"• Otherwise that number is your **point** — roll it again within "
            f"**{casino_games.DICE_POINT_MAX_ROLLS}** rolls to win\n"
            "• Rolling a **7** loses. Hitting neither in 3 rolls = refund\n"
            f"• Wins pay **{casino_games.DICE_WIN_PAYOUT}x**\n\n"
            "Pick your bet:"
        ),
        color=0xF39C12,
    )
    view = DiceView(ctx, day_start, bank)
    await ctx.send(embed=embed, view=view)


@bot.hybrid_command(description="Spin the slots — pick ¼, ½, or MAX of the day-start bank")
async def slots(ctx):
    """Open a slot machine bet picker."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available. Try again later.")
        return

    bank = state["bank"]
    day_start = state["day_start_bank"]

    if bank <= 0:
        await ctx.send(
            "\U0001f4b8 Bank is empty. Wait for midnight settle (bust resets to seed)."
        )
        return

    slot_cap_ratio = config.GAMBLING_GAME_CAPS.get("slots", 1.0)
    slot_cap = int(day_start * slot_cap_ratio)
    embed = discord.Embed(
        title="\U0001f3b0 Slots",
        description=(
            f"Bank: **${bank:,}**  •  Slot cap: **${slot_cap:,}** "
            f"({int(slot_cap_ratio * 100)}% of day-start)\n\n"
            "Pick your bet:"
        ),
        color=0xF1C40F,
    )
    embed.set_footer(text="3-of-a-kind only • 🍒10x  🍋16x  🍊25x  🍇50x  🔔150x  7️⃣400x")
    view = SlotBetView(ctx, day_start, bank)
    await ctx.send(embed=embed, view=view)


# ── Wheel ────────────────────────────────────────────────────────────

class WheelView(discord.ui.View):
    """Bet picker for /wheel — single weighted spin."""

    def __init__(self, ctx, day_start_bank, current_bank):
        super().__init__(timeout=60)
        self.ctx = ctx
        options = gambling.compute_bet_options(day_start_bank, current_bank, game="wheel")
        self.amounts = {
            "quarter": options["quarter"]["amount"],
            "half":    options["half"]["amount"],
            "max":     options["max"]["amount"],
        }

        self.quarter.label = f"¼  ${options['quarter']['amount']:,}"
        self.quarter.disabled = not options['quarter']['enabled']
        self.half.label = f"½  ${options['half']['amount']:,}"
        self.half.disabled = not options['half']['enabled']
        self.max_bet.label = f"MAX  ${options['max']['amount']:,}"
        self.max_bet.disabled = not options['max']['enabled']

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "That spin isn't yours — run `/wheel` to play your own.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    @discord.ui.button(style=discord.ButtonStyle.secondary)
    async def quarter(self, interaction, button):
        await self._spin(interaction, self.amounts["quarter"])

    @discord.ui.button(style=discord.ButtonStyle.primary)
    async def half(self, interaction, button):
        await self._spin(interaction, self.amounts["half"])

    @discord.ui.button(style=discord.ButtonStyle.danger)
    async def max_bet(self, interaction, button):
        await self._spin(interaction, self.amounts["max"])

    async def _spin(self, interaction, bet):
        for child in self.children:
            child.disabled = True
        result = casino_games.spin_wheel()
        payout = int(round(bet * result["multiplier"]))

        outcome = await gambling.apply_bet(
            guild_id=self.ctx.guild.id,
            user_id=self.ctx.author.id,
            user_name=self.ctx.author.display_name,
            game="wheel",
            bet=bet,
            payout=payout,
            metadata={"outcome": result["label"], "multiplier": result["multiplier"]},
        )
        if outcome is None or outcome.get("error"):
            err = (outcome or {}).get("error", "DB unavailable")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="🎡 Wheel — Bet failed",
                    description=f"Couldn't place bet: `{err}`.",
                    color=0xE74C3C,
                ),
                view=self,
            )
            return

        net = outcome["net"]
        new_bank = outcome["new_bank"]

        if result["multiplier"] >= 5:
            color = 0xF1C40F  # gold
        elif result["multiplier"] >= 2:
            color = 0x2ECC71
        elif result["multiplier"] == 1:
            color = 0x95A5A6  # grey for refund
        else:
            color = 0xE74C3C

        if net > 0:
            settle_line = f"**+${net:,}** ({result['multiplier']}x payout on ${bet:,})"
        elif net == 0:
            settle_line = f"**${bet:,} refunded**"
        else:
            settle_line = f"**−${abs(net):,}**"

        embed = discord.Embed(
            title=f"🎡 Wheel — ${bet:,} bet • {self.ctx.author.display_name}",
            description=_augment_settle_description(
                f"## {result['emoji']}  {result['label']}\n{settle_line}\nBank: **${new_bank:,}**",
                outcome,
            ),
            color=color,
        )
        await interaction.response.edit_message(embed=embed, view=self)


@bot.hybrid_command(description="Spin the wheel of fortune — 6 outcomes from lose to 10x jackpot")
async def wheel(ctx):
    """Open a wheel-of-fortune bet picker."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available. Try again later.")
        return

    bank = state["bank"]
    day_start = state["day_start_bank"]
    if bank <= 0:
        await ctx.send("\U0001f4b8 Bank is empty. Wait for midnight settle.")
        return

    outcomes_lines = []
    for o in casino_games.WHEEL_OUTCOMES:
        if o["multiplier"] > 1:
            label = f"**{o['multiplier']}x**"
        elif o["multiplier"] == 1:
            label = "Refund"
        else:
            label = "Lose"
        outcomes_lines.append(f"{o['emoji']}  {label}  ({o['weight']}%)")
    outcomes_block = "\n".join(outcomes_lines)

    embed = discord.Embed(
        title="\U0001f3a1 Wheel of Fortune",
        description=(
            f"Bank: **${bank:,}**  •  Day-start cap: **${day_start:,}**\n\n"
            f"**Outcomes:**\n{outcomes_block}\n\n"
            "Pick your bet:"
        ),
        color=0xE91E63,
    )
    view = WheelView(ctx, day_start, bank)
    await ctx.send(embed=embed, view=view)


# ── Horse Race ───────────────────────────────────────────────────────

class _HorsePickButton(discord.ui.Button):
    def __init__(self, index, horse):
        super().__init__(
            label=f"{horse['emoji']} {horse['name']}  ({horse['payout']}x)",
            style=discord.ButtonStyle.primary,
        )
        self.index = index

    async def callback(self, interaction):
        view: HorseRaceView = self.view
        await view._pick_horse(interaction, self.index)


class _HorseBetButton(discord.ui.Button):
    def __init__(self, label, amount, enabled, style):
        super().__init__(label=label, style=style, disabled=not enabled)
        self.amount = amount

    async def callback(self, interaction):
        view: HorseRaceView = self.view
        await view._place_bet(interaction, self.amount)


class HorseRaceView(discord.ui.View):
    """Two-stage view: pick horse → pick bet → race result."""

    def __init__(self, ctx, day_start_bank, current_bank):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.day_start_bank = day_start_bank
        self.current_bank = current_bank
        self.chosen_horse_idx = None
        self.bet_options = gambling.compute_bet_options(day_start_bank, current_bank, game="horses")

        for i, horse in enumerate(casino_games.HORSES):
            self.add_item(_HorsePickButton(i, horse))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "That race isn't yours — run `/horses` to bet on your own.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    async def _pick_horse(self, interaction, idx):
        self.chosen_horse_idx = idx
        horse = casino_games.HORSES[idx]

        self.clear_items()
        opts = self.bet_options
        self.add_item(_HorseBetButton(
            f"¼  ${opts['quarter']['amount']:,}",
            opts['quarter']['amount'], opts['quarter']['enabled'],
            discord.ButtonStyle.secondary,
        ))
        self.add_item(_HorseBetButton(
            f"½  ${opts['half']['amount']:,}",
            opts['half']['amount'], opts['half']['enabled'],
            discord.ButtonStyle.primary,
        ))
        self.add_item(_HorseBetButton(
            f"MAX  ${opts['max']['amount']:,}",
            opts['max']['amount'], opts['max']['enabled'],
            discord.ButtonStyle.danger,
        ))

        embed = discord.Embed(
            title=f"\U0001f40e Horse Race — {self.ctx.author.display_name}",
            description=(
                f"You picked **{horse['emoji']} {horse['name']}** "
                f"({horse['payout']}x payout, ~{horse['weight']}% win rate)\n\n"
                "Pick your bet:"
            ),
            color=0x3498DB,
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def _place_bet(self, interaction, bet):
        for child in self.children:
            child.disabled = True

        winner_idx, winner = casino_games.run_race()
        chosen = casino_games.HORSES[self.chosen_horse_idx]
        win = (winner_idx == self.chosen_horse_idx)
        payout = int(round(bet * chosen["payout"])) if win else 0

        outcome = await gambling.apply_bet(
            guild_id=self.ctx.guild.id,
            user_id=self.ctx.author.id,
            user_name=self.ctx.author.display_name,
            game="horses",
            bet=bet,
            payout=payout,
            metadata={
                "chosen": chosen["name"],
                "winner": winner["name"],
                "win": win,
            },
        )
        if outcome is None or outcome.get("error"):
            err = (outcome or {}).get("error", "DB unavailable")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="🐎 Race — Bet failed",
                    description=f"Couldn't place bet: `{err}`.",
                    color=0xE74C3C,
                ),
                view=self,
            )
            return

        net = outcome["net"]
        new_bank = outcome["new_bank"]

        lines = []
        for i, h in enumerate(casino_games.HORSES):
            tag = ""
            if i == winner_idx:
                tag += "  🏁 WINNER"
            if i == self.chosen_horse_idx:
                tag += "  ⬅️ your pick"
            lines.append(f"{h['emoji']}  {h['name']}{tag}")

        if win:
            color = 0x2ECC71
            settle_line = f"**+${net:,}** ({chosen['payout']}x on ${bet:,})"
        else:
            color = 0xE74C3C
            settle_line = f"**−${bet:,}**"

        embed = discord.Embed(
            title=f"\U0001f40e Horse Race — ${bet:,} bet • {self.ctx.author.display_name}",
            description=_augment_settle_description(
                "\n".join(lines) + f"\n\n{settle_line}\nBank: **${new_bank:,}**",
                outcome,
            ),
            color=color,
        )
        await interaction.response.edit_message(embed=embed, view=self)


@bot.hybrid_command(description="Bet on a horse race — 4 horses with weighted odds")
async def horses(ctx):
    """Open a horse race bet picker."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available. Try again later.")
        return

    bank = state["bank"]
    day_start = state["day_start_bank"]
    if bank <= 0:
        await ctx.send("\U0001f4b8 Bank is empty. Wait for midnight settle.")
        return

    lines = []
    for h in casino_games.HORSES:
        lines.append(
            f"{h['emoji']}  **{h['name']}** — {h['payout']}x payout, ~{h['weight']}% win rate"
        )
    horses_block = "\n".join(lines)

    embed = discord.Embed(
        title="\U0001f40e Horse Race",
        description=(
            f"Bank: **${bank:,}**  •  Day-start cap: **${day_start:,}**\n\n"
            f"**Field:**\n{horses_block}\n\n"
            "Pick your horse:"
        ),
        color=0x3498DB,
    )
    view = HorseRaceView(ctx, day_start, bank)
    await ctx.send(embed=embed, view=view)


# ── Blackjack ────────────────────────────────────────────────────────

class _BlackjackHitButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Hit", style=discord.ButtonStyle.primary)

    async def callback(self, interaction):
        view: BlackjackView = self.view
        await view._hit(interaction)


class _BlackjackStandButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Stand", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction):
        view: BlackjackView = self.view
        await view._stand(interaction)


class BlackjackView(discord.ui.View):
    """Two-phase blackjack: bet picker → hit/stand → settle."""

    def __init__(self, ctx, day_start_bank, current_bank):
        super().__init__(timeout=120)
        self.ctx = ctx
        options = gambling.compute_bet_options(day_start_bank, current_bank, game="blackjack")
        self.amounts = {
            "quarter": options["quarter"]["amount"],
            "half":    options["half"]["amount"],
            "max":     options["max"]["amount"],
        }
        self.bet = 0
        self.deck = None
        self.player_hand = None
        self.dealer_hand = None

        self.quarter.label = f"¼  ${options['quarter']['amount']:,}"
        self.quarter.disabled = not options['quarter']['enabled']
        self.half.label = f"½  ${options['half']['amount']:,}"
        self.half.disabled = not options['half']['enabled']
        self.max_bet.label = f"MAX  ${options['max']['amount']:,}"
        self.max_bet.disabled = not options['max']['enabled']

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "That hand isn't yours — run `/blackjack` to play your own.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    @discord.ui.button(style=discord.ButtonStyle.secondary)
    async def quarter(self, interaction, button):
        await self._deal(interaction, self.amounts["quarter"])

    @discord.ui.button(style=discord.ButtonStyle.primary)
    async def half(self, interaction, button):
        await self._deal(interaction, self.amounts["half"])

    @discord.ui.button(style=discord.ButtonStyle.danger)
    async def max_bet(self, interaction, button):
        await self._deal(interaction, self.amounts["max"])

    async def _deal(self, interaction, bet):
        self.bet = bet
        self.deck = casino_games.new_deck()
        self.player_hand = [self.deck.pop(), self.deck.pop()]
        self.dealer_hand = [self.deck.pop(), self.deck.pop()]

        player_bj = casino_games.is_blackjack(self.player_hand)
        dealer_bj = casino_games.is_blackjack(self.dealer_hand)

        if player_bj and dealer_bj:
            await self._settle(interaction, "push", payout=bet)
            return
        if player_bj:
            payout = int(round(bet * casino_games.BLACKJACK_NATURAL_PAYOUT))
            await self._settle(interaction, "player_blackjack", payout=payout)
            return
        if dealer_bj:
            await self._settle(interaction, "dealer_blackjack", payout=0)
            return

        # Swap to hit/stand
        self.clear_items()
        self.add_item(_BlackjackHitButton())
        self.add_item(_BlackjackStandButton())
        await self._render_play(interaction)

    async def _render_play(self, interaction):
        p_val = casino_games.hand_value(self.player_hand)
        embed = discord.Embed(
            title=f"\U0001f0cf Blackjack — ${self.bet:,} • {self.ctx.author.display_name}",
            description=(
                f"**Dealer:** {casino_games.format_hand(self.dealer_hand, hide_first=True)}\n"
                f"**You:** {casino_games.format_hand(self.player_hand)}  (**{p_val}**)\n\n"
                "Hit or Stand?"
            ),
            color=0x2C3E50,
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def _hit(self, interaction):
        self.player_hand.append(self.deck.pop())
        val = casino_games.hand_value(self.player_hand)
        if val > 21:
            await self._settle(interaction, "player_bust", payout=0)
            return
        if val == 21:
            await self._stand(interaction)
            return
        await self._render_play(interaction)

    async def _stand(self, interaction):
        casino_games.play_dealer(self.dealer_hand, self.deck)
        p_val = casino_games.hand_value(self.player_hand)
        d_val = casino_games.hand_value(self.dealer_hand)

        if d_val > 21:
            payout = int(round(self.bet * casino_games.BLACKJACK_WIN_PAYOUT))
            await self._settle(interaction, "dealer_bust", payout=payout)
        elif d_val > p_val:
            await self._settle(interaction, "dealer_wins", payout=0)
        elif d_val < p_val:
            payout = int(round(self.bet * casino_games.BLACKJACK_WIN_PAYOUT))
            await self._settle(interaction, "player_wins", payout=payout)
        else:
            await self._settle(interaction, "push", payout=self.bet)

    async def _settle(self, interaction, reason, payout):
        for child in self.children:
            child.disabled = True

        outcome = await gambling.apply_bet(
            guild_id=self.ctx.guild.id,
            user_id=self.ctx.author.id,
            user_name=self.ctx.author.display_name,
            game="blackjack",
            bet=self.bet,
            payout=payout,
            metadata={
                "player_hand": [f"{r}{s}" for r, s in self.player_hand],
                "dealer_hand": [f"{r}{s}" for r, s in self.dealer_hand],
                "player_total": casino_games.hand_value(self.player_hand),
                "dealer_total": casino_games.hand_value(self.dealer_hand),
                "result": reason,
            },
        )
        if outcome is None or outcome.get("error"):
            err = (outcome or {}).get("error", "DB unavailable")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="🃏 Blackjack — Bet failed",
                    description=f"Couldn't settle bet: `{err}`.",
                    color=0xE74C3C,
                ),
                view=self,
            )
            return

        net = outcome["net"]
        new_bank = outcome["new_bank"]
        p_val = casino_games.hand_value(self.player_hand)
        d_val = casino_games.hand_value(self.dealer_hand)

        labels = {
            "player_blackjack": ("🎯 BLACKJACK", 0xF1C40F),
            "dealer_blackjack": ("💀 Dealer Blackjack", 0xE74C3C),
            "player_bust":      ("💀 BUST", 0xE74C3C),
            "dealer_bust":      ("✅ Dealer Bust — you win!", 0x2ECC71),
            "player_wins":      (f"✅ You win  ({p_val} vs {d_val})", 0x2ECC71),
            "dealer_wins":      (f"💀 Dealer wins  ({d_val} vs {p_val})", 0xE74C3C),
            "push":             ("⏸️ Push", 0x95A5A6),
        }
        title_tag, color = labels[reason]

        if net > 0:
            settle_line = f"**+${net:,}**"
        elif net == 0:
            settle_line = f"**${self.bet:,} refunded** (push)"
        else:
            settle_line = f"**−${abs(net):,}**"

        embed = discord.Embed(
            title=f"\U0001f0cf Blackjack — ${self.bet:,} • {self.ctx.author.display_name}",
            description=_augment_settle_description(
                f"**Dealer:** {casino_games.format_hand(self.dealer_hand)}  (**{d_val}**)\n"
                f"**You:** {casino_games.format_hand(self.player_hand)}  (**{p_val}**)\n\n"
                f"## {title_tag}\n{settle_line}\nBank: **${new_bank:,}**",
                outcome,
            ),
            color=color,
        )
        await interaction.response.edit_message(embed=embed, view=self)


@bot.hybrid_command(description="Play blackjack — hit or stand to get closer to 21 than the dealer")
async def blackjack(ctx):
    """Open a blackjack bet picker."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available. Try again later.")
        return

    bank = state["bank"]
    day_start = state["day_start_bank"]
    if bank <= 0:
        await ctx.send("\U0001f4b8 Bank is empty. Wait for midnight settle.")
        return

    embed = discord.Embed(
        title="\U0001f0cf Blackjack",
        description=(
            f"Bank: **${bank:,}**  •  Day-start cap: **${day_start:,}**\n\n"
            "**Rules:**\n"
            "• Beat the dealer's hand without going over 21\n"
            "• Dealer hits to 17 and stands. Aces count as 11 or 1\n"
            f"• Win pays **{casino_games.BLACKJACK_WIN_PAYOUT}x** • "
            f"Blackjack pays **{casino_games.BLACKJACK_NATURAL_PAYOUT}x** • Push refunds\n\n"
            "Pick your bet:"
        ),
        color=0x2C3E50,
    )
    view = BlackjackView(ctx, day_start, bank)
    await ctx.send(embed=embed, view=view)


@bot.hybrid_command(description="Show the shared casino bank, debt, and today's bet sizes")
async def balance(ctx):
    """Show the shared casino bank state."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available. Try again later.")
        return

    bank = state["bank"]
    debt = state["current_debt"]
    day_start = state["day_start_bank"]
    day = state["day_number"]
    options = gambling.compute_bet_options(day_start, bank)

    # Time until midnight MYT
    now = datetime.now(MYT)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    delta = tomorrow - now
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes = remainder // 60

    # Progress bar — fraction of debt covered
    pct = min(100, int(bank / debt * 100)) if debt > 0 else 100
    filled = pct // 10
    bar = "█" * filled + "░" * (10 - filled)

    embed = discord.Embed(
        title=f"\U0001f3b0 Casino — Day {day}",
        color=0x2ECC71 if bank >= debt else 0xE74C3C,
    )
    embed.add_field(name="\U0001f4b0 Bank", value=f"**${bank:,}**", inline=True)
    embed.add_field(name="\U0001f4cb Today's Debt", value=f"${debt:,}", inline=True)
    embed.add_field(name="\U0001f512 Day-start Cap", value=f"${day_start:,}", inline=True)
    embed.add_field(
        name=f"\U0001f4ca Quota Progress — {pct}%",
        value=f"`{bar}` ${bank:,} / ${debt:,}",
        inline=False,
    )

    def mark(opt):
        return "✅" if opt["enabled"] else "❌"

    embed.add_field(
        name="\U0001f3af Bet Sizes (locked to day-start bank)",
        value=(
            f"`¼`  ${options['quarter']['amount']:,} {mark(options['quarter'])}\n"
            f"`½`  ${options['half']['amount']:,} {mark(options['half'])}\n"
            f"`MAX` ${options['max']['amount']:,} {mark(options['max'])}"
        ),
        inline=True,
    )
    embed.add_field(name="⏰ Settle In", value=f"{hours}h {minutes}m", inline=True)
    embed.add_field(name="🎟️ Tickets", value=f"**{state['tickets'] or 0}**", inline=True)

    active = await gambling.get_active_effects(ctx.guild.id)
    if active:
        eff_lines = []
        for e in active:
            defn = casino_items.item(e["item_id"])
            if defn:
                eff_lines.append(f"{defn['emoji']} {defn['name']} × {e['charges_left']}")
        embed.add_field(name="✨ Active Effects", value="\n".join(eff_lines), inline=False)

    started = state["streak_started_at"]
    embed.set_footer(text=f"Run started {started.strftime('%Y-%m-%d')} • Midnight MYT settle")
    embed.timestamp = datetime.now(MYT)

    await ctx.send(embed=embed)


@bot.hybrid_command(description="List all casino games and commands")
async def casino(ctx):
    """Show available casino commands and games."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    embed = discord.Embed(
        title="\U0001f3b0 Casino Commands",
        description=(
            "Shared bank, daily quota, escalating debt. "
            "Hit quota by midnight MYT or the whole run resets."
        ),
        color=0xF1C40F,
    )
    embed.add_field(
        name="\U0001f4ca Status",
        value=(
            "`/balance` — Bank, debt, day-start cap, time until settle\n"
            "`/leaderboard` — Top players for the current run\n"
            "`/tickets` — Ticket balance + earning rates"
        ),
        inline=False,
    )
    embed.add_field(
        name="\U0001f3ae Games",
        value=(
            "🎰 `/slots` — 3-reel slots, 3-of-a-kind only (50% bet cap)\n"
            "🎲 `/dice` — Craps: 7/11 instant win, point phase with 3-roll cap\n"
            "🎡 `/wheel` — Wheel of fortune, 6 outcomes incl. 10x jackpot\n"
            "🐎 `/horses` — 4 horses with weighted odds, pick then bet\n"
            "🃏 `/blackjack` — Standard 21 vs dealer (hits to 17)"
        ),
        inline=False,
    )
    embed.add_field(
        name="\U0001f6cd Items",
        value=(
            "`/shop` — Browse and buy items with tickets\n"
            "`/inventory` — See owned items and active effects\n"
            "`/use` — Activate an item from inventory"
        ),
        inline=False,
    )
    embed.add_field(
        name="\U0001f4b0 Bet Sizes",
        value=(
            "Each game offers `¼`, `½`, `MAX` buttons. "
            "Max is locked to the **bank-at-start-of-day**, not your current bank — "
            "so one bad call can't bankrupt everyone."
        ),
        inline=False,
    )
    embed.set_footer(text=f"Seed: ${config.GAMBLING_SEED_BANK:,} • Day 1 debt: ${config.GAMBLING_BASE_DEBT:,} • Quota grows {config.GAMBLING_DEBT_MULTIPLIER}x daily")
    await ctx.send(embed=embed)


# ── Items: tickets / shop / inventory / use ──────────────────────────

@bot.hybrid_command(description="Show your ticket balance and how to earn more")
async def tickets(ctx):
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return
    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available.")
        return

    embed = discord.Embed(
        title="\U0001f39f Tickets",
        description=f"Shared ticket balance: **{state['tickets'] or 0}** 🎟️",
        color=0xF39C12,
    )
    embed.add_field(
        name="How to earn",
        value=(
            f"• Clear the daily quota: **+{casino_items.TICKETS_PER_DAY_CLEARED}** 🎟️\n"
            f"• Land a jackpot (≥10x payout): **+{casino_items.TICKETS_PER_JACKPOT}** 🎟️\n"
            "_Tickets reset to 0 if the run busts._"
        ),
        inline=False,
    )
    embed.set_footer(text="Spend tickets in /shop")
    await ctx.send(embed=embed)


class _ShopBuyButton(discord.ui.Button):
    def __init__(self, item_id, defn, affordable):
        super().__init__(
            label=f"{defn['emoji']} {defn['name']}  ({defn['cost']}🎟)",
            style=discord.ButtonStyle.primary if affordable else discord.ButtonStyle.secondary,
            disabled=not affordable,
        )
        self.item_id = item_id
        self.defn = defn

    async def callback(self, interaction):
        view: ShopView = self.view
        await view._buy(interaction, self.item_id, self.defn)


class ShopView(discord.ui.View):
    """One buy button per item; refreshes the embed after each purchase."""

    def __init__(self, ctx, tickets_balance):
        super().__init__(timeout=120)
        self.ctx = ctx
        self.tickets_balance = tickets_balance
        self._rebuild_buttons()

    def _rebuild_buttons(self):
        self.clear_items()
        for item_id, defn in casino_items.ITEMS.items():
            self.add_item(_ShopBuyButton(item_id, defn, self.tickets_balance >= defn["cost"]))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "That shop isn't yours — run `/shop` to open your own.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    async def _buy(self, interaction, item_id, defn):
        result = await gambling.buy_item(self.ctx.guild.id, item_id, defn["cost"])
        if not result or result.get("error"):
            err = (result or {}).get("error", "DB unavailable")
            if err == "insufficient_tickets":
                msg = f"Not enough tickets: have {result['have']}, need {result['need']}."
            else:
                msg = f"Couldn't buy: `{err}`"
            await interaction.response.send_message(msg, ephemeral=True)
            return

        self.tickets_balance = result["tickets_remaining"]
        self._rebuild_buttons()

        embed = _build_shop_embed(self.tickets_balance, note=f"✅ Bought {defn['emoji']} {defn['name']} for {defn['cost']}🎟")
        await interaction.response.edit_message(embed=embed, view=self)


def _build_shop_embed(tickets_balance, note=None):
    lines = []
    for item_id, defn in casino_items.ITEMS.items():
        lines.append(
            f"{defn['emoji']}  **{defn['name']}** — {defn['cost']}🎟️\n"
            f"   _{defn['description']}_"
        )
    embed = discord.Embed(
        title="\U0001f6cd Casino Shop",
        description=(note + "\n\n" if note else "") + (
            f"Tickets: **{tickets_balance}** 🎟️\n\n" + "\n\n".join(lines)
        ),
        color=0x9B59B6,
    )
    embed.set_footer(text="Buttons below buy one of each • Items are shared with the whole server")
    return embed


@bot.hybrid_command(description="Browse and buy casino items with tickets")
async def shop(ctx):
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return
    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available.")
        return
    tickets_balance = state["tickets"] or 0
    embed = _build_shop_embed(tickets_balance)
    view = ShopView(ctx, tickets_balance)
    await ctx.send(embed=embed, view=view)


@bot.hybrid_command(description="Show owned items and currently active effects")
async def inventory(ctx):
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    inv = await gambling.get_inventory(ctx.guild.id)
    effects = await gambling.get_active_effects(ctx.guild.id)

    embed = discord.Embed(title="\U0001f392 Inventory", color=0x9B59B6)

    if inv:
        owned_lines = []
        for item_id, count in inv.items():
            defn = casino_items.item(item_id)
            if defn:
                owned_lines.append(f"{defn['emoji']}  **{defn['name']}** × {count}")
        embed.add_field(name="Owned", value="\n".join(owned_lines), inline=False)
    else:
        embed.add_field(name="Owned", value="_Nothing yet — visit `/shop`_", inline=False)

    if effects:
        eff_lines = []
        for e in effects:
            defn = casino_items.item(e["item_id"])
            if defn:
                charges = e["charges_left"]
                charge_label = f"{charges} charge{'s' if charges != 1 else ''}"
                eff_lines.append(f"{defn['emoji']}  **{defn['name']}** — {charge_label} left")
        embed.add_field(name="Active (auto-applies)", value="\n".join(eff_lines), inline=False)
    else:
        embed.add_field(name="Active (auto-applies)", value="_No effects armed_", inline=False)

    embed.set_footer(text="Activate with /use • Items wipe on bust")
    await ctx.send(embed=embed)


class _UseItemButton(discord.ui.Button):
    def __init__(self, item_id, defn, count):
        super().__init__(
            label=f"{defn['emoji']} {defn['name']}  (×{count})",
            style=discord.ButtonStyle.primary,
        )
        self.item_id = item_id
        self.defn = defn

    async def callback(self, interaction):
        view: UseItemView = self.view
        await view._use(interaction, self.item_id, self.defn)


class UseItemView(discord.ui.View):
    """One button per owned item; click to activate it."""

    def __init__(self, ctx, inventory):
        super().__init__(timeout=120)
        self.ctx = ctx
        for item_id, count in inventory.items():
            defn = casino_items.item(item_id)
            if defn:
                self.add_item(_UseItemButton(item_id, defn, count))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "Not your item-use session — run `/use` to open your own.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    async def _use(self, interaction, item_id, defn):
        for child in self.children:
            child.disabled = True

        if defn["trigger"] == "passive":
            result = await gambling.activate_passive(
                self.ctx.guild.id, item_id, self.ctx.author.id, defn["charges"],
            )
            if not result or result.get("error"):
                err = (result or {}).get("error", "DB unavailable")
                await interaction.response.edit_message(
                    embed=discord.Embed(
                        title="Item use failed",
                        description=f"`{err}`",
                        color=0xE74C3C,
                    ),
                    view=self,
                )
                return
            embed = discord.Embed(
                title=f"{defn['emoji']} {defn['name']} armed",
                description=(
                    f"{defn['description']}\n\n"
                    f"**Charges:** {defn['charges']}\n"
                    "It'll auto-trigger on the next qualifying bet."
                ),
                color=0x2ECC71,
            )
            await interaction.response.edit_message(embed=embed, view=self)
            return

        # Active items
        if item_id == "quota_gun":
            result = await gambling.use_quota_gun(self.ctx.guild.id, self.ctx.author.id)
            if not result or result.get("error"):
                err = (result or {}).get("error", "DB unavailable")
                await interaction.response.edit_message(
                    embed=discord.Embed(
                        title="Item use failed",
                        description=f"`{err}`",
                        color=0xE74C3C,
                    ),
                    view=self,
                )
                return
            embed = discord.Embed(
                title=f"{defn['emoji']} {defn['name']} fired",
                description=(
                    f"Debt reduced by **${result['reduced_by']:,}** "
                    f"({int(casino_items.QUOTA_GUN_PAYOFF_PCT * 100)}%)\n"
                    f"Today's debt: ~~${result['old_debt']:,}~~ → **${result['new_debt']:,}**"
                ),
                color=0x2ECC71,
            )
            await interaction.response.edit_message(embed=embed, view=self)
            return

        if item_id == "time_machine":
            result = await gambling.use_time_machine(self.ctx.guild.id, self.ctx.author.id)
            if not result or result.get("error"):
                err = (result or {}).get("error", "DB unavailable")
                msg = {
                    "not_owned": "You don't own a Time Machine.",
                    "no_recent_bet": "No recent bet to reverse.",
                }.get(err, f"`{err}`")
                await interaction.response.edit_message(
                    embed=discord.Embed(
                        title="Item use failed",
                        description=msg,
                        color=0xE74C3C,
                    ),
                    view=self,
                )
                return
            net = result["reversed_net"]
            sign = "+" if net >= 0 else "−"
            embed = discord.Embed(
                title=f"{defn['emoji']} {defn['name']} activated",
                description=(
                    f"Reversed **{result['reversed_user']}**'s `/{result['reversed_game']}` bet of "
                    f"**${result['reversed_bet']:,}** (net was {sign}${abs(net):,}).\n"
                    "Bank restored to its prior state."
                ),
                color=0x9B59B6,
            )
            await interaction.response.edit_message(embed=embed, view=self)
            return


@bot.hybrid_command(description="Activate an item from inventory")
async def use(ctx):
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return
    inv = await gambling.get_inventory(ctx.guild.id)
    if not inv:
        await ctx.send("Inventory is empty. Visit `/shop` first.")
        return
    view = UseItemView(ctx, inv)
    embed = discord.Embed(
        title="\U0001f6cd Use an item",
        description="Click an item below to activate it.\nPassive items arm for next bet; active items fire immediately.",
        color=0x9B59B6,
    )
    await ctx.send(embed=embed, view=view)


@bot.hybrid_command(description="Show the casino leaderboard for the current run")
async def leaderboard(ctx):
    """Top players by net profit/loss since the current run started."""
    if not ctx.guild:
        await ctx.send("This command only works in a server.")
        return
    if not await _enforce_casino_channel(ctx):
        return

    state = await gambling.get_or_create_bank(ctx.guild.id)
    if not state:
        await ctx.send("Casino DB not available.")
        return

    streak_start = state["streak_started_at"]
    stats = await gambling.get_player_stats(ctx.guild.id, streak_start)

    if not stats:
        await ctx.send("No bets placed yet this run. Run `/slots`, `/dice`, etc. to start.")
        return

    embed = discord.Embed(
        title=f"\U0001f3c6 Casino Leaderboard — Day {state['day_number']}",
        description=_format_player_stats(stats[:10]),
        color=0xF1C40F,
    )
    embed.set_footer(
        text=(
            f"Run started {streak_start.strftime('%Y-%m-%d')} • "
            f"{len(stats)} players • Sorted by net P/L"
        )
    )
    embed.timestamp = datetime.now(MYT)
    await ctx.send(embed=embed)


@bot.event
async def on_message(message):
    # Don't respond to ourselves
    if message.author == bot.user:
        return

    # Log message to Kafka (fire-and-forget, never block the bot)
    try:
        await kafka.send_message_event(message)
    except Exception:
        pass

    # Process commands first
    await bot.process_commands(message)
    
    # Check if the bot is mentioned or if it's a DM
    is_mentioned = bot.user in message.mentions
    is_dm = isinstance(message.channel, discord.DMChannel)

    if is_mentioned or is_dm:
        async with message.channel.typing():
            # fetch history ONLY via reply chain
            history = []
            news_context_text = None
            
            # Start traversal
            curr_msg = message
            
            # Limit depth to avoid infinite loops or huge context
            for _ in range(20):
                # Check if this message has attached news context
                if curr_msg.id in _news_context:
                    news_context_text = _news_context[curr_msg.id]

                # Add current node to history
                role = "assistant" if curr_msg.author == bot.user else "user"
                
                if role == "user":
                    content = f"{curr_msg.author.display_name}: {curr_msg.content.replace(f'<@{bot.user.id}>', '').strip()}"
                else:
                    # For bot messages with embeds (like news), include embed description
                    if curr_msg.embeds:
                        embed_text = curr_msg.embeds[0].description or ""
                        content = embed_text
                    else:
                        content = curr_msg.content
                
                # We prepend because we are walking backwards (newest -> oldest)
                history.insert(0, {"role": role, "content": content})

                # Check if this message is a reply to another message
                if curr_msg.reference and curr_msg.reference.resolved:
                    # If resolved, we have the message object already
                    parent_msg = curr_msg.reference.resolved
                    
                    # ENFORCE STRICT FILTER: Only follow chain if it goes between User <-> Bot
                    if isinstance(parent_msg, discord.Message):
                        curr_msg = parent_msg
                        continue
                
                # If we get here, there is no valid parent or we stopped
                break
            
            # Build system prompt — inject news context if this is a news follow-up
            system_content = "You are a helpful Discord assistant. User messages start with their name (e.g. 'Wrynaft: Hello'). Do NOT start your response with your own name."
            
            if news_context_text:
                system_content += (
                    "\n\nThe user is asking about a recent tech news digest. "
                    "Here are the full article details for reference:\n\n"
                    + news_context_text
                )
            
            # Auto-search: if the latest message looks like a factual question, search the web
            user_text = message.content.replace(f'<@{bot.user.id}>', '').strip()
            search_results = ""
            if not news_context_text and search_service.should_search(user_text):
                search_results = await search_service.search_web(user_text, max_results=3)
            
            if search_results:
                system_content += (
                    "\n\nThe following web search results may help answer the user's question. "
                    "Use them if relevant, and cite sources when possible. "
                    "If the search results aren't relevant, ignore them and answer normally.\n\n"
                    + search_results
                )
            
            messages = [
                {"role": "system", "content": system_content}
            ] + history
            
            response = await llm_service.generate_response(messages)
            await message.reply(response)

# ── Analytics Event Handlers (via Kafka) ─────────────────

@bot.event
async def on_message_edit(before, after):
    """Log message edits to Kafka."""
    try:
        await kafka.send_message_event(after, event_type='edit')
    except Exception:
        pass

@bot.event
async def on_message_delete(message):
    """Log message deletions to Kafka."""
    try:
        await kafka.send_message_event(message, event_type='delete')
    except Exception:
        pass

@bot.event
async def on_voice_state_update(member, before, after):
    """Log voice channel join/leave/move events to Kafka."""
    try:
        if before.channel is None and after.channel is not None:
            await kafka.send_voice_event(member, after.channel, member.guild, 'join')
        elif before.channel is not None and after.channel is None:
            await kafka.send_voice_event(member, before.channel, member.guild, 'leave')
        elif before.channel != after.channel:
            await kafka.send_voice_event(member, before.channel, member.guild, 'leave')
            await kafka.send_voice_event(member, after.channel, member.guild, 'join')
        elif before.self_mute != after.self_mute:
            event = 'mute' if after.self_mute else 'unmute'
            await kafka.send_voice_event(member, after.channel, member.guild, event)
        elif before.self_deaf != after.self_deaf:
            event = 'deafen' if after.self_deaf else 'undeafen'
            await kafka.send_voice_event(member, after.channel, member.guild, event)
    except Exception:
        pass

@bot.event
async def on_reaction_add(reaction, user):
    """Log reaction adds to Kafka."""
    try:
        await kafka.send_reaction_event(reaction, user, 'add')
    except Exception:
        pass

@bot.event
async def on_reaction_remove(reaction, user):
    """Log reaction removals to Kafka."""
    try:
        await kafka.send_reaction_event(reaction, user, 'remove')
    except Exception:
        pass

@bot.event
async def on_presence_update(before, after):
    """Log activity/game changes to Kafka."""
    try:
        if before.activities != after.activities:
            for activity in after.activities:
                await kafka.send_presence_event(after, activity)
    except Exception:
        pass

if __name__ == "__main__":
    bot.run(config.DISCORD_TOKEN)
