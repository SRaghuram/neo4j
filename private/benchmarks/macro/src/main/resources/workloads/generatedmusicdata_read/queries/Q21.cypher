MATCH (t:Track)
  WHERE t.duration IN [60, 61, 62, 63, 64]
RETURN *
