MATCH (:Person { name:'n(0,0)' }),
      (:Person { name:'n(0,1)' }),
      (:Person { name:'n(0,2)' }),
      (:Person { name:'n(0,3)' }),
      (:Person { name:'n(0,4)' }),
      (:Person { name:'n(0,5)' }),
      (:Person { name:'n(0,6)' })
RETURN count(*)
