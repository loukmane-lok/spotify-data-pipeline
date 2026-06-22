-- ============================================================
-- Marts: Top Songs (Gold Layer)
-- ============================================================
-- Aggregates play and skip counts per song for leaderboard
-- and recommendation analytics.
--
-- Source: silver.clean_data
-- Grain:  One row per song (song_id)
-- ============================================================

{{ config(
    materialized='table',
    tags=['marts', 'gold']
) }}

SELECT
    song_id,
    song_name,
    artist_name,
    COUNT(CASE WHEN event_type = 'play' THEN 1 END)            AS total_plays,
    COUNT(CASE WHEN event_type = 'skip' THEN 1 END)            AS total_skips,
    COUNT(CASE WHEN event_type = 'add_to_playlist' THEN 1 END) AS total_playlist_adds,
    ROUND(
        COUNT(CASE WHEN event_type = 'skip' THEN 1 END)::FLOAT
        / NULLIF(COUNT(CASE WHEN event_type = 'play' THEN 1 END), 0),
        4
    )                                                           AS skip_rate
FROM {{ ref('clean_data') }}
GROUP BY song_id, song_name, artist_name
ORDER BY total_plays DESC