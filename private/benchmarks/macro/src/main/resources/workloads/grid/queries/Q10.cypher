MATCH (p0:Person { name:'n(0,0)' }),
      (p1:Person { name:'n(0,1)' }),
      (p2:Person { name:'n(0,2)' }),
      (p3:Person { name:'n(0,3)' }),
      (p4:Person { name:'n(0,4)' }),
      (p5:Person { name:'n(0,5)' }),
      (p6:Person { name:'n(0,6)' })
OPTIONAL MATCH (p0)-->(p0_1)-->(p0_2)
OPTIONAL MATCH (p1)-->(p1_1)-->(p1_2)
OPTIONAL MATCH (p2)-->(p2_1)-->(p2_2)
OPTIONAL MATCH (p3)-->(p3_1)-->(p3_2)
OPTIONAL MATCH (p4)-->(p4_1)-->(p4_2)
OPTIONAL MATCH (p5)-->(p5_1)-->(p5_2)
OPTIONAL MATCH (p6)-->(p6_1)-->(p6_2)
RETURN count(*)