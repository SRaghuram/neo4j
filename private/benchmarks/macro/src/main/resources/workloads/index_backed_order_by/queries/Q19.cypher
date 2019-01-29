MATCH (n:PROFILES) WHERE n.pets < "i" RETURN n.children ORDER BY n.pets DESC
