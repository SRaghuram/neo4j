MATCH (t1:Track)--(al:Album)--(t2:Track)
  WHERE t1.duration = 61 AND t2.duration = 68
RETURN *
