WITH
    1111111 AS cid,
    toDateTime('2025-01-01 00:00:00') AS start_ts,
    toDateTime('2025-01-02 00:00:00') AS end_ts,

    phrases_today AS (
        SELECT DISTINCT phrase
        FROM task4.phrases_views
        WHERE campaign_id = cid
          AND dt >= start_ts AND dt < end_ts
    ),

    anchors_all AS (
        SELECT
            phrase,
            start_ts AS h,
            argMaxIf(views, dt, dt < start_ts) AS max_views,
            countIf(dt < start_ts) AS prev_cnt
        FROM task4.phrases_views
        WHERE campaign_id = cid
        GROUP BY phrase
    ),

    anchors AS (
        SELECT a.phrase, a.h, a.max_views
        FROM anchors_all a
        INNER JOIN phrases_today pt USING (phrase)
        WHERE a.prev_cnt > 0
    ),

    hourly AS (
        SELECT
            phrase,
            toStartOfHour(dt) AS h,
            max(views) AS max_views
        FROM task4.phrases_views
        WHERE campaign_id = cid
          AND dt >= start_ts AND dt < end_ts
        GROUP BY phrase, h
    ),

    unioned AS (
        SELECT phrase, h, max_views, 1 AS is_anchor FROM anchors
        UNION ALL
        SELECT phrase, h, max_views, 0 AS is_hourly FROM hourly
    ),

    calc AS (
        SELECT
            phrase,
            h,
            is_anchor,
            max_views - lagInFrame(max_views, 1, max_views)
                OVER (PARTITION BY phrase ORDER BY h) AS hour_delta
        FROM unioned
    )

SELECT
    phrase,
    arraySort(x -> -x.1,
        groupArray((toHour(h), toInt32(hour_delta)))
    ) AS views_by_hour
FROM calc
WHERE is_anchor = 0
GROUP BY phrase
ORDER BY phrase
