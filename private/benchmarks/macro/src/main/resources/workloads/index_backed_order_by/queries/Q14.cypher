MATCH (a:PROFILES) WHERE a.pets < "m" RETURN a.pets, count(*)
