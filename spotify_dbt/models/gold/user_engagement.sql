-- ============================================================
-- Marts: User Engagement (Gold Layer)
-- ============================================================
-- Daily user engagement metrics by device and country.
-- Powers dashboards and cohort analysis.
--
-- Source: silver.clean_data
-- Grain:  One row per (user_id, device_type, country, day)
-- ============================================================

{{ config(
    materialized='table',
    tags=['marts', 'gold']
) }}

SELECT
    user_id,
    device_type,
    country,
    DATE_TRUNC('day', event_ts)                                  AS day,
    COUNT(*)                                                     AS total_events,
    COUNT(CASE WHEN event_type = 'play' THEN 1 END)              AS plays,
    COUNT(CASE WHEN event_type = 'skip' THEN 1 END)              AS skips,
    COUNT(CASE WHEN event_type = 'pause' THEN 1 END)             AS pauses,
    COUNT(CASE WHEN event_type = 'add_to_playlist' THEN 1 END)   AS playlist_adds
FROM {{ ref('clean_data') }}
GROUP BY user_id, device_type, country, DATE_TRUNC('day', event_ts)
ORDER BY plays DESC