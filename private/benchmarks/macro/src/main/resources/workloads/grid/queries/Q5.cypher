MATCH p =(s:Person { name:'n(0,0)'})-->()-->()-->()-->()-->(c5:Person {name:' n(0,5)'})-->()-->()-->()-->()-->(c10:Person {name:' n(0,10)'})-->()-->()-->()-->()-->(c15:Person {name:' n(0,15)'})-->()-->()-->()-->()-->(c20:Person {name:' n(0,20)'})-->()-->()-->()-->()-->(c25:Person {name:' n(0,25)'})-->()-->()-->()-->()-->(c30:Person {name:' n(0,30)' })
RETURN count(p)