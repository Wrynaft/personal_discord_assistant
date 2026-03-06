-- Mart: User Summary
-- Lifetime stats per user
-- Used for: leaderboards, user profiles in Superset

SELECT
    u.user_id,
    u.username,
    u.display_name,
    u.is_bot,
    u.first_seen,
    u.last_seen,
    COALESCE(msg.total_messages, 0) AS total_messages,
    COALESCE(msg.total_words, 0) AS total_words,
    COALESCE(msg.avg_message_length, 0) AS avg_message_length,
    COALESCE(msg.active_days, 0) AS active_days,
    COALESCE(msg.channels_used, 0) AS channels_used,
    COALESCE(voice.voice_joins, 0) AS voice_joins,
    COALESCE(react.reactions_given, 0) AS reactions_given,
    COALESCE(games.games_played, 0) AS unique_games_played
FROM {{ source('raw', 'dim_users') }} u

LEFT JOIN (
    SELECT
        user_id,
        COUNT(*) AS total_messages,
        SUM(word_count) AS total_words,
        AVG(content_length)::INT AS avg_message_length,
        COUNT(DISTINCT message_date) AS active_days,
        COUNT(DISTINCT channel_id) AS channels_used
    FROM {{ ref('stg_messages') }}
    GROUP BY user_id
) msg ON u.user_id = msg.user_id

LEFT JOIN (
    SELECT user_id, COUNT(*) AS voice_joins
    FROM {{ ref('stg_voice_events') }}
    WHERE event_type = 'join'
    GROUP BY user_id
) voice ON u.user_id = voice.user_id

LEFT JOIN (
    SELECT user_id, COUNT(*) AS reactions_given
    FROM {{ source('raw', 'fact_reactions') }}
    WHERE event_type = 'add'
    GROUP BY user_id
) react ON u.user_id = react.user_id

LEFT JOIN (
    SELECT user_id, COUNT(DISTINCT activity_name) AS games_played
    FROM {{ ref('stg_presence') }}
    WHERE activity_type = 'playing'
    GROUP BY user_id
) games ON u.user_id = games.user_id

WHERE u.is_bot = FALSE
