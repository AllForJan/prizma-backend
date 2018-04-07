select a.meno, a.rok, sum(a.suma) as suma_all
from apa_prijimatelia as a
where a.meno like '%{q}%'
group by a.meno, a.rok
