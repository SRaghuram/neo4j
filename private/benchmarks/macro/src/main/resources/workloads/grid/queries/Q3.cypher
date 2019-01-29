MATCH p =(s:Person { name:'n(0,0)'})-->(c1)-->(c2)-->(c3)-->(c4)-->(c5:Person {name:' n(0,5)'})-->(c6)-->(c7)-->(c8)-->(c9)-->(c10:Person {name:' n(0,10)' })-->(c11)-->(c12)-->(c13)
RETURN count(p)