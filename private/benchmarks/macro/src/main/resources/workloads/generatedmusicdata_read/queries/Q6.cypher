MATCH (t:Track)--(al:Album)--(a:Artist)
  WHERE t.duration = 61 AND a.gender = 'male'
RETURN *
