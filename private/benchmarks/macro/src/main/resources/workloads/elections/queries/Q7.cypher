MATCH (cain:Candidate { CAND_ID: 'P00003608' }),(obama:Candidate { CAND_ID: 'P80003338' }), p=shortestPath((cain)-[*..10]-(obama))
RETURN p