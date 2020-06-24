MATCH p=(ah:AccountHolder)-[:MAKES_PAYMENTS_TO*2..4]->(ah)
RETURN p
LIMIT 100
