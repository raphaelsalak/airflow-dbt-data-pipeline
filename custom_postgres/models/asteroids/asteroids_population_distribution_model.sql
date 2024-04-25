select
  id,
  estimated_diameter_min_km,
  estimated_diameter_max_km,
  case
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 20 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 <= 30 then 'Blue whale'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 10 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 20 then 'Colossal squid'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 5 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 10 then 'Elk'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 2 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 5 then 'Bear'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 1 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 2 then 'Lion'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.5 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 1 then 'Dog'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.2 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 0.5 then 'Cat'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.1 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 0.2 then 'Gerbil'
    when (estimated_diameter_min_km + estimated_diameter_max_km) / 2 >= 0.01 and (estimated_diameter_min_km + estimated_diameter_max_km) / 2 < 0.1 then 'Cockroach'
    else 'Tardigrades'
  end as size_category
from asteroid
