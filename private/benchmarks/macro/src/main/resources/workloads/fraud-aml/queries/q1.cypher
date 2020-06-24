MATCH path=(cu1:Customer:AccountHolder)
            -[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|CURRENT_RESIDENCE|USES_DEVICE]->(info)
	        <-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|CURRENT_RESIDENCE|USES_DEVICE]-
	        (cu2:Customer:AccountHolder)
OPTIONAL MATCH (cu1)-[:HAS_GOVT_ID]-(id1:GovtIssuedID)
OPTIONAL MATCH (cu2)-[:HAS_GOVT_ID]-(id2:GovtIssuedID)
WHERE id(cu1)<>id(cu2)
RETURN path, id1, id2
