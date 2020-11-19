MATCH (p0:Person { name:'n(0,0)' }),
      (p1:Person { name:'n(0,1)' }),
      (p2:Person { name:'n(0,2)' })
OPTIONAL MATCH (p0)-->(p0_1)-->(p0_2)
OPTIONAL MATCH (p1)-->(p1_1)-->(p1_2)
OPTIONAL MATCH (p2)-->(p2_1)-->(p2_2)
RETURN count(*)