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
from services import search_service

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

    embed = discord.Embed(
        title="\U0001f3a8 Today's Top Danbooru Art",
        color=0xE91E63,  # Pink
    )

    for i, post in enumerate(posts, 1):
        # Character/copyright info
        info_parts = []
        if post['character']:
            info_parts.append(post['character'].split(' ')[0])  # First character
        if post['copyright']:
            info_parts.append(post['copyright'].split(' ')[0])  # First copyright
        info = " \u2022 ".join(info_parts) if info_parts else "Original"

        embed.add_field(
            name=f"{i}. {info} (Score: {post['score']})",
            value=f"[View Post]({post['page_url']}) \u2022 Artist: {post['artist'].split(' ')[0]} \u2022 {danbooru.rating_emoji(post['rating'])}",
            inline=False,
        )

    # Set the first post's image as the embed thumbnail
    if posts:
        embed.set_image(url=posts[0]['file_url'])

    embed.set_footer(text="Powered by Danbooru API")
    embed.timestamp = datetime.now(MYT)
    await destination.send(embed=embed)


@bot.hybrid_command(name="danbooru", description="Fetch a top Danbooru post with optional tag search")
async def danbooru_cmd(ctx, *, tags: str = ""):
    """Fetches a top Danbooru post. Usage: !danbooru [tags]"""
    async with ctx.typing():
        correction_msg = ""

        if tags:
            # Fuzzy-match tags via Danbooru autocomplete
            resolved, corrections = await danbooru.resolve_tags(tags)
            tags = resolved

            if corrections:
                fixes = ", ".join(f"**{orig}** → **{fixed}**" for orig, fixed in corrections)
                correction_msg = f"🔍 Auto-corrected: {fixes}\n"

            post = await danbooru.get_random_top_post(tags=tags)
        else:
            post = await danbooru.get_random_top_post()

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
