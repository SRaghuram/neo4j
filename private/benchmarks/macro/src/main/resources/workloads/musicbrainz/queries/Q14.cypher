MATCH (a:Artist { name:'John Lennon' })-[r:MEMBER_OF_BAND*3..6]-(o:Person)
RETURN o.name,count(*)
ORDER BY count(*) DESC LIMIT 10