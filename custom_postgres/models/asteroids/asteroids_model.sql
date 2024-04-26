SELECT 'Earth' AS orbiting_body,
       COUNT(*) AS earth_count
FROM closeapproach
WHERE orbiting_body = 'Earth'

UNION ALL

SELECT 'Other' AS orbiting_body,
       COUNT(*) AS total_count
FROM closeapproach
where orbiting_body != 'Earth'
