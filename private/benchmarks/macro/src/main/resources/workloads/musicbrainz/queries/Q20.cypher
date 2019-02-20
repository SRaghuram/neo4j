MATCH p=(:Artist)-->(al:Album)<-[:PART_OF]-(n:Release)
RETURN p
LIMIT 50