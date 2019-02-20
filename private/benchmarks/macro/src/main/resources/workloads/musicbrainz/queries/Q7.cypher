MATCH (a:Artist { name:'John Lennon' })-[:MEMBER_OF_BAND]-(a1)-[:MEMBER_OF_BAND]-(a2)-[:MEMBER_OF_BAND]-(o:Artist)
RETURN o.name,count(*)
ORDER BY count(*) DESC LIMIT 10