-- ============================================================================
-- KPI queries over the star schema (Workshop §6.4 requirement support; used by the dashboard in §8)
-- Each query states the requirement it supports. All read ONLY from music_dw.
-- Coverage note: every KPI describes the MATCHED subset (Spotify sample x Grammy winners); show KPI-06 next to them.
-- ============================================================================

-- KPI-01 · AR-01 · Average track popularity, Grammy-winning vs non-winning Spotify artists, and the gap
SELECT
    ROUND(AVG(f.popularity) FILTER (WHERE a.is_grammy_winner), 2)                                    AS avg_popularity_winners,
    ROUND(AVG(f.popularity) FILTER (WHERE NOT a.is_grammy_winner), 2)                                AS avg_popularity_non_winners,
    ROUND(AVG(f.popularity) FILTER (WHERE a.is_grammy_winner)
        - AVG(f.popularity) FILTER (WHERE NOT a.is_grammy_winner), 2)                                AS gap,
    COUNT(DISTINCT f.artist_key) FILTER (WHERE a.is_grammy_winner)                                   AS winner_artists,
    COUNT(DISTINCT f.artist_key) FILTER (WHERE NOT a.is_grammy_winner)                               AS non_winner_artists
FROM fact_track_artist f
JOIN dim_artist a USING (artist_key)
WHERE a.in_spotify;

-- KPI-01b · AR-01 · KPI-01 in chart-ready long form (one row per group), with a sensitivity to popularity = 0.
--                   popularity = 0 is kept as a measured value (RISK-09, TR-S06), so it can hide or invert the gap:
--                   pct_zero_popularity shows how much of each group is exactly 0, avg_popularity_excl_zero removes it.
--                   Always read the two averages together; neither one alone is "the" answer.
SELECT CASE WHEN a.is_grammy_winner THEN 'Grammy winner' ELSE 'No Grammy win' END                  AS artist_group,
       COUNT(DISTINCT f.artist_key)                                                               AS artists,
       ROUND(AVG(f.popularity), 2)                                                                AS avg_popularity,
       ROUND(AVG(f.popularity) FILTER (WHERE f.popularity > 0), 2)                                AS avg_popularity_excl_zero,
       ROUND(100.0 * COUNT(*) FILTER (WHERE f.popularity = 0) / COUNT(*), 1)                      AS pct_zero_popularity
FROM fact_track_artist f
JOIN dim_artist a USING (artist_key)
WHERE a.in_spotify
GROUP BY a.is_grammy_winner
ORDER BY a.is_grammy_winner DESC;

-- KPI-02 · AR-01 · Share of high-popularity tracks (popularity >= 70) that have at least one Grammy-winning artist
WITH track_level AS (
    SELECT f.track_key,
           MAX(f.is_high_popularity)   AS is_high,
           BOOL_OR(a.is_grammy_winner) AS has_winner_artist
    FROM fact_track_artist f
    JOIN dim_artist a USING (artist_key)
    GROUP BY f.track_key
)
SELECT COUNT(*)                                                         AS high_popularity_tracks,
       COUNT(*) FILTER (WHERE has_winner_artist)                        AS with_grammy_artist,
       ROUND(100.0 * COUNT(*) FILTER (WHERE has_winner_artist) / NULLIF(COUNT(*), 0), 2) AS pct_high_popularity_by_winners
FROM track_level
WHERE is_high = 1;

-- KPI-03 · AR-02 · Grammy wins per Spotify genre (an artist's wins count in each genre where the artist appears;
--                  NOT additive across genres)
SELECT g.genre_name,
       SUM(a.grammy_win_count) AS grammy_wins
FROM bridge_artist_genre b
JOIN dim_genre  g USING (genre_key)
JOIN dim_artist a USING (artist_key)
GROUP BY g.genre_name
ORDER BY grammy_wins DESC, g.genre_name;

-- KPI-03 sliced by decade (the Grammy win keeps its own year)
SELECT y.decade, g.genre_name, COUNT(*) AS grammy_wins
FROM fact_grammy_award fa
JOIN dim_year y USING (year_key)
JOIN bridge_artist_genre b ON b.artist_key = fa.artist_key
JOIN dim_genre g           ON g.genre_key  = b.genre_key
WHERE fa.artist_key <> 0
GROUP BY y.decade, g.genre_name
ORDER BY y.decade, grammy_wins DESC;

-- KPI-04 · AR-02 · Distinct Grammy-winning artists per genre and their share of the genre's artists
SELECT g.genre_name,
       COUNT(*) FILTER (WHERE a.is_grammy_winner)                                   AS winner_artists,
       COUNT(*)                                                                      AS genre_artists,
       ROUND(100.0 * COUNT(*) FILTER (WHERE a.is_grammy_winner) / COUNT(*), 2)      AS pct_winner_artists
FROM bridge_artist_genre b
JOIN dim_genre  g USING (genre_key)
JOIN dim_artist a USING (artist_key)
GROUP BY g.genre_name
ORDER BY winner_artists DESC, g.genre_name;

-- KPI-05 · AR-03 · Audio profile of Grammy-winning matched tracks by Grammy decade, against the catalogue average.
--                  Each (decade, track) counts once; each track is taken once through its primary artist row.
WITH matched AS (
    SELECT DISTINCT y.decade, bt.track_key
    FROM bridge_award_track bt
    JOIN fact_grammy_award fa USING (grammy_row_id)
    JOIN dim_year y           USING (year_key)
)
SELECT m.decade::text                         AS grammy_decade,
       COUNT(*)                               AS tracks,
       ROUND(AVG(f.danceability)::numeric, 3)          AS danceability,
       ROUND(AVG(f.energy)::numeric, 3)                AS energy,
       ROUND(AVG(f.valence)::numeric, 3)               AS valence,
       ROUND(AVG(f.loudness_db)::numeric, 2)           AS loudness_db,
       ROUND(AVG(f.tempo_bpm)::numeric, 1)             AS tempo_bpm,
       ROUND(AVG(f.duration_ms)::numeric / 60000.0, 2) AS duration_min
FROM matched m
JOIN fact_track_artist f ON f.track_key = m.track_key AND f.is_primary_artist
GROUP BY m.decade
UNION ALL
SELECT 'catalogue', COUNT(*),
       ROUND(AVG(danceability)::numeric, 3), ROUND(AVG(energy)::numeric, 3), ROUND(AVG(valence)::numeric, 3),
       ROUND(AVG(loudness_db)::numeric, 2), ROUND(AVG(tempo_bpm)::numeric, 1), ROUND(AVG(duration_ms)::numeric / 60000.0, 2)
FROM fact_track_artist
WHERE is_primary_artist
ORDER BY 1;

-- KPI-06 · AR-03 · Integration match rate = matched Grammy rows / matchable Grammy rows (and over all rows)
SELECT COUNT(*)                                                                        AS grammy_rows,
       COUNT(*) FILTER (WHERE is_matchable)                                            AS matchable_rows,
       COUNT(*) FILTER (WHERE is_track_matched)                                        AS matched_rows,
       ROUND(100.0 * COUNT(*) FILTER (WHERE is_track_matched)
             / NULLIF(COUNT(*) FILTER (WHERE is_matchable), 0), 2)                      AS match_rate_of_matchable_pct,
       ROUND(100.0 * COUNT(*) FILTER (WHERE is_track_matched) / COUNT(*), 2)           AS match_rate_of_all_pct
FROM fact_grammy_award;

-- V-02 · AR-02 · One row per genre with both measures of the visualization (KPI-03 wins and KPI-04 winning artists / share).
--                Same joins as KPI-03 and KPI-04; wins are NOT additive across genres (an artist counts in every genre it appears in).
SELECT g.genre_name,
       SUM(a.grammy_win_count)                                                       AS grammy_wins,
       COUNT(*) FILTER (WHERE a.is_grammy_winner)                                    AS winner_artists,
       COUNT(*)                                                                      AS genre_artists,
       ROUND(100.0 * COUNT(*) FILTER (WHERE a.is_grammy_winner) / COUNT(*), 2)       AS pct_winner_artists
FROM bridge_artist_genre b
JOIN dim_genre  g USING (genre_key)
JOIN dim_artist a USING (artist_key)
GROUP BY g.genre_name
ORDER BY grammy_wins DESC, g.genre_name;
