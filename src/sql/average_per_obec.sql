SELECT
  AVG(suma),obec
FROM apa_prijimatelia WHERE obec={obec}
GROUP BY obec;