MATCH (cand:Candidate)<-[r:SUPPORTS]-(camp:Committee)
WHERE cand.CAND_OFFICE='P' AND cand.CAND_ELECTION_YR='2012'
RETURN cand.CAND_NAME, COUNT(camp) AS count
ORDER BY count DESC LIMIT 10