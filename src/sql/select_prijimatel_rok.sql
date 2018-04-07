select a.rok, sum(a.suma) as suma
from apa_prijimatelia as a
where a.custom_id={custom_id}
group by a.rok
