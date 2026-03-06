-- Mart: Channel Health
-- Per-channel engagement metrics
-- Used for: channel comparison dashboards in Superset

SELECT
    c.channel_id,
    c.channel_name,
    c.channel_type,
    c.category,
    m.guild_id,
    g.guild_name,
    COUNT(*) AS total_messages,
    COUNT(DISTINCT m.user_id) AS unique_posters,
    COUNT(DISTINCT m.message_date) AS active_days,
    AVG(m.word_count)::INT AS avg_words_per_message,
    SUM(CASE WHEN m.has_attachment THEN 1 ELSE 0 END) AS total_attachments,
    MIN(m.created_at) AS first_message,
    MAX(m.created_at) AS last_message
FROM {{ ref('stg_messages') }} m
LEFT JOIN {{ source('raw', 'dim_channels') }} c ON m.channel_id = c.channel_id
LEFT JOIN {{ source('raw', 'dim_guilds') }} g ON m.guild_id = g.guild_id
GROUP BY c.channel_id, c.channel_name, c.channel_type, c.category, m.guild_id, g.guild_name
