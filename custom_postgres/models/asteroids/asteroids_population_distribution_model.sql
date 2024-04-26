SELECT
  size_category,
  COUNT(*) AS category_count
FROM (
  SELECT
    id,
    estimated_diameter_min_km,
    estimated_diameter_max_km,
    CASE
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 20 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 <= 30 THEN 'Blue whale'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 10 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 20 THEN 'Colossal squid'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 5 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 10 THEN 'Elk'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 2 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 5 THEN 'Bear'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 1 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 2 THEN 'Lion'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.5 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 1 THEN 'Dog'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.2 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 0.5 THEN 'Cat'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.1 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 0.2 THEN 'Gerbil'
      WHEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.01 AND (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 0.1 THEN 'Cockroach'
      ELSE 'Tardigrades'
    END AS size_category,
    is_potentially_hazardous
  FROM asteroid
) AS subquery
GROUP BY size_category
