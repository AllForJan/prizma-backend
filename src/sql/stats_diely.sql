select count(*) as pocet, d.rok
from apa_ziadosti_diely d
where d.custom_id = {custom_id}
group by d.rok
order by d.rok
