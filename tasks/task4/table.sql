CREATE TABLE IF NOT EXISTS task4.phrases_views
(
    dt          DateTime,
    campaign_id Int32,
    phrase      String,
    views       Int32
) ENGINE = ReplacingMergeTree
ORDER BY (dt, campaign_id, phrase);

INSERT INTO task4.phrases_views (dt, campaign_id, phrase, views)
SELECT toDateTime('2025-01-01 11:50:00'), 1111111, 'платье', 100 UNION ALL
SELECT toDateTime('2025-01-01 12:00:00'), 1111111, 'платье', 100 UNION ALL
SELECT toDateTime('2025-01-01 12:10:00'), 1111111, 'платье', 100 UNION ALL
SELECT toDateTime('2025-01-01 12:20:00'), 1111111, 'платье', 100 UNION ALL
SELECT toDateTime('2025-01-01 12:30:00'), 1111111, 'платье', 100 UNION ALL
SELECT toDateTime('2025-01-01 12:40:00'), 1111111, 'платье', 101 UNION ALL
SELECT toDateTime('2025-01-01 12:50:00'), 1111111, 'платье', 101 UNION ALL
SELECT toDateTime('2025-01-01 13:00:00'), 1111111, 'платье', 101 UNION ALL
SELECT toDateTime('2025-01-01 13:10:00'), 1111111, 'платье', 101 UNION ALL
SELECT toDateTime('2025-01-01 13:20:00'), 1111111, 'платье', 101 UNION ALL
SELECT toDateTime('2025-01-01 13:30:00'), 1111111, 'платье', 102 UNION ALL
SELECT toDateTime('2025-01-01 13:40:00'), 1111111, 'платье', 103 UNION ALL
SELECT toDateTime('2025-01-01 13:50:00'), 1111111, 'платье', 105 UNION ALL
SELECT toDateTime('2025-01-01 14:00:00'), 1111111, 'платье', 105 UNION ALL
SELECT toDateTime('2025-01-01 14:10:00'), 1111111, 'платье', 106 UNION ALL
SELECT toDateTime('2025-01-01 14:20:00'), 1111111, 'платье', 108 UNION ALL
SELECT toDateTime('2025-01-01 14:30:00'), 1111111, 'платье', 109 UNION ALL
SELECT toDateTime('2025-01-01 14:40:00'), 1111111, 'платье', 110 UNION ALL
SELECT toDateTime('2025-01-01 14:50:00'), 1111111, 'платье', 111 UNION ALL
SELECT toDateTime('2025-01-01 15:00:00'), 1111111, 'платье', 111 UNION ALL
SELECT toDateTime('2025-01-01 15:10:00'), 1111111, 'платье', 111 UNION ALL
SELECT toDateTime('2025-01-01 15:20:00'), 1111111, 'платье', 112 UNION ALL
SELECT toDateTime('2025-01-01 15:30:00'), 1111111, 'платье', 113 UNION ALL
SELECT toDateTime('2025-01-01 15:40:00'), 1111111, 'платье', 113 UNION ALL
SELECT toDateTime('2025-01-01 15:50:00'), 1111111, 'платье', 115 UNION ALL
SELECT toDateTime('2025-01-01 16:00:00'), 1111111, 'платье', 115;
