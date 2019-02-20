MATCH (m:Movie { title: { title }})<-[:ACTS_IN]-(a:Actor)
RETURN a.name, a.birthplace