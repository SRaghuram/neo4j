MATCH (comm:Committee)<-[:INDIVIDUAL_CONTRIBUTION]-(indiv:Individual)-[:EARMARKED_BY]->(rec:Candidate)
RETURN indiv.TRANSACTION_AMT, comm.CMTE_NM
LIMIT 10