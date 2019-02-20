MATCH (cand:Candidate)<-[:CAMPAIGNS_FOR]-(camp:Committee)
WHERE cand.CAND_OFFICE='P' AND cand.CAND_ELECTION_YR='2012'
RETURN camp.CMTE_NM, cand.CAND_NAME