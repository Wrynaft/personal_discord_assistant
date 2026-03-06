-- Mart: Sentiment Analysis
-- Sentiment trends by channel and day
-- Used for: toxicity meter, channel mood charts in Superset

SELECT
    m.message_date,
    m.guild_id,
    g.guild_name,
    m.channel_id,
    c.channel_name,
    COUNT(*) AS scored_messages,
    AVG(m.sentiment_score)::NUMERIC(3,2) AS avg_sentiment,
    SUM(CASE WHEN m.sentiment_score <= 2 THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN m.sentiment_score = 3 THEN 1 ELSE 0 END) AS neutral_count,
    SUM(CASE WHEN m.sentiment_score >= 4 THEN 1 ELSE 0 END) AS positive_count,
    -- Toxicity rate: % of messages scored 1 or 2
    ROUND(
        100.0 * SUM(CASE WHEN m.sentiment_score <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    ) AS toxicity_pct
FROM {{ ref('stg_messages') }} m
LEFT JOIN {{ source('raw', 'dim_channels') }} c ON m.channel_id = c.channel_id
LEFT JOIN {{ source('raw', 'dim_guilds') }} g ON m.guild_id = g.guild_id
WHERE m.sentiment_score IS NOT NULL
GROUP BY m.message_date, m.guild_id, g.guild_name, m.channel_id, c.channel_name
