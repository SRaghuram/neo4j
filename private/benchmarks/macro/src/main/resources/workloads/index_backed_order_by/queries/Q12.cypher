MATCH (n:PROFILES) WHERE n.pets < "i" AND exists(n.cars)
            RETURN n.pets, n.cars ORDER BY n.pets, n.cars
