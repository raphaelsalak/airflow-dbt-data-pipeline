select
    count(*) as pha_count
from 
    asteroid
where
    is_potentially_hazardous = 'true'