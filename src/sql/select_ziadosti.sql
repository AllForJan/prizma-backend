select a.meno, a.vymera, a.lokalita, a.rok, a.ico, a.custom_id, a.diel
from apa_ziadosti_diely as a
where a.custom_id={custom_id}
