SELECT 'Non-hazardous asteroids' AS is_potentially_hazardous,
       COUNT(*) AS pha_count
FROM asteroid
WHERE is_potentially_hazardous = 'false'

UNION ALL

SELECT 'Hazardous asteroids' AS is_potentially_hazardous,
       COUNT(*) AS total_count
FROM asteroid
where is_potentially_hazardous = 'true'
