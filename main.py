import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone, timedelta
import config
from services.llm_service import LLMService
from services.news_service import NewsService
from services.hn_service import HNService
from services.arxiv_service import ArxivService
from services import search_service

# Malaysian Time = UTC+8
MYT = timezone(timedelta(hours=8))

# proper intents are required for the bot to see messages
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
llm_service = LLMService()
news_service = NewsService()
hn_service = HNService()
arxiv_service = ArxivService()

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

@bot.command()
async def ping(ctx):
    await ctx.send('Pong!')

@bot.command()
async def news(ctx):
    """Fetches the latest tech news and summarizes it using the LLM."""
    async with ctx.typing():
        await post_news_digest(ctx)

@bot.command()
async def hn(ctx):
    """Fetches top Hacker News stories and summarizes them. Usage: !hn"""
    async with ctx.typing():
        await post_hn_digest(ctx)

@bot.command()
async def papers(ctx):
    """Fetches latest CS/AI research papers from arXiv. Usage: !papers"""
    async with ctx.typing():
        await post_papers_digest(ctx)

@bot.command()
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

@bot.event
async def on_message(message):
    # Don't respond to ourselves
    if message.author == bot.user:
        return

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

if __name__ == "__main__":
    bot.run(config.DISCORD_TOKEN)
