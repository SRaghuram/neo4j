MATCH ()-[r:ALLOWED_INHERIT]->(:Company)
RETURN count(r)