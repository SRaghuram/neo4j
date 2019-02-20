MATCH p =(s:Person { name:'n(0,0)'})-->(c1)-->(c2)-->(c3)-->(c4)-->(c5:Person {name:' n(0,5)' })-->(c6)-->(c7)
RETURN count(p)