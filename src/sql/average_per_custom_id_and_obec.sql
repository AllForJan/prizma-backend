  SELECT
    AVG(suma),
    obec,
    custom_id,
    meno
  FROM apa_prijimatelia WHERE obec={obec} AND custom_id={custom_id}
  GROUP BY custom_id, obec, meno;