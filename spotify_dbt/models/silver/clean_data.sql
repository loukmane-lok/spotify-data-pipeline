-- ============================================================
-- Staging: Clean Events (Silver Layer)
-- ============================================================
-- Cleans and type-casts raw bronze events. Filters out records
-- with NULL keys or unparseable timestamps.
--
-- Source: SPOTIFY_DB.BRONZE.RAW_EVENTS
-- Grain:  One row per event
-- ============================================================

{{ config(
    materialized='view',
    tags=['staging', 'silver']
) }}

WITH bronze_data AS (
    SELECT
        event_id,
        user_id,
        song_id,
        TRIM(artist_name)                    AS artist_name,
        TRIM(song_name)                      AS song_name,
        LOWER(event_type)                    AS event_type,
        LOWER(device_type)                   AS device_type,
        UPPER(country)                       AS country,
        TRY_TO_TIMESTAMP_TZ(timestamp)       AS event_ts
    FROM {{ source('bronze', 'RAW_EVENTS') }}
)

SELECT *
FROM bronze_data
WHERE event_ts IS NOT NULL
  AND user_id IS NOT NULL
  AND song_id IS NOT NULL