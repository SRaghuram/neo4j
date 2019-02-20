MATCH (t1:Track)--(al:Album)--(t2:Track)
WHERE t1.duration = 61 AND t2.duration = 68
SET al.x = 42
SET t2.y = 43
RETURN *