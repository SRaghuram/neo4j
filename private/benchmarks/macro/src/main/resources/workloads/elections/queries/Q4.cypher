MATCH (cand:Candidate)
WHERE cand.CAND_OFFICE='P' AND cand.CAND_ELECTION_YR='2012'
RETURN cand.CAND_NAME