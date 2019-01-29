MATCH (cand:Candidate { CAND_ID: 'P80003338' })<-[:SUPPORTS]-(camp:Committee)<-[:INDIVIDUAL_CONTRIBUTION]-(contrib:Individual)
RETURN contrib.NAME, contrib.TRANSACTION_AMT
ORDER BY contrib.TRANSACTION_AMT DESC LIMIT 10