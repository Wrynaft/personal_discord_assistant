-- Staging: Messages
-- Cleans and standardizes raw message events

SELECT
    id,
    message_id,
    user_id,
    channel_id,
    guild_id,
    content_length,
    word_count,
    has_attachment,
    has_embed,
    event_type,
    created_at,
    DATE(created_at) AS message_date,
    EXTRACT(HOUR FROM created_at) AS message_hour,
    EXTRACT(DOW FROM created_at) AS day_of_week  -- 0=Sunday
FROM {{ source('raw', 'fact_messages') }}
WHERE event_type = 'send'
