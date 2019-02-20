MATCH (m:Movie { title: { title }})<-[:ACTS_IN]-()-[:ACTS_IN]->(s:Movie)
RETURN s.title, count(*)
ORDER BY count(*) DESC LIMIT 5