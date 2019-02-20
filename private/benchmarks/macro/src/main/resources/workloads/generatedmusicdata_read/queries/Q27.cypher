MATCH (corp:Company)<-[:SIGNED_WITH]-(a1:Artist)-[:PERFORMED_AT]->(c:Concert)-[:IN]->(v:Venue)
MATCH (corp)<-[:SIGNED_WITH]-(a2:Artist)-[:PERFORMED_AT]->(c)
RETURN a1, a2, v
