MATCH ()-[r:ENTITY_RESOLUTION]->()
  WHERE r.attributeScore >= 0.9
RETURN count(r) AS count
