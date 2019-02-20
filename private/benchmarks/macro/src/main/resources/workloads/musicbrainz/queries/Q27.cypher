MATCH p=(a:Artist { name:'John Lennon' })-[r:MEMBER_OF_BAND*..10]-(o:Person)
WHERE ALL (n IN nodes(p) WHERE n.prop = 42)
RETURN p