MATCH p =(s:Person { name:'n(0,0)' })-->(c1)-->(c2)-->(c3)
RETURN count(p)