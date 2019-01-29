MATCH (n:PROFILES) WHERE n.pets > "g" AND n.pets < "i" RETURN DISTINCT n.pets
