-- Mart: Daily Activity
-- Aggregated daily stats per guild, per user, per channel
-- Used for: activity trend charts, heatmaps in Superset

SELECT
    m.message_date,
    m.guild_id,
    g.guild_name,
    m.user_id,
    u.username,
    u.display_name,
    m.channel_id,
    c.channel_name,
    COUNT(*) AS message_count,
    SUM(m.content_length) AS total_chars,
    SUM(m.word_count) AS total_words,
    AVG(m.content_length)::INT AS avg_message_length,
    SUM(CASE WHEN m.has_attachment THEN 1 ELSE 0 END) AS attachment_count,
    SUM(CASE WHEN m.has_embed THEN 1 ELSE 0 END) AS embed_count
FROM {{ ref('stg_messages') }} m
LEFT JOIN {{ source('raw', 'dim_users') }} u ON m.user_id = u.user_id
LEFT JOIN {{ source('raw', 'dim_channels') }} c ON m.channel_id = c.channel_id
LEFT JOIN {{ source('raw', 'dim_guilds') }} g ON m.guild_id = g.guild_id
GROUP BY
    m.message_date, m.guild_id, g.guild_name,
    m.user_id, u.username, u.display_name,
    m.channel_id, c.channel_name
