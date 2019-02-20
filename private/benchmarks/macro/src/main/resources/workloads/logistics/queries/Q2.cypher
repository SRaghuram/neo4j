MATCH (s:Location { name: { startLocation }}),(e:Location { name: { endLocation }})
MATCH upLeg =(s)<-[:DELIVERY_ROUTE]-(db1)
WHERE ALL (r IN relationships(upLeg) WHERE r.start_date <= { intervalStart } AND r.end_date >= { intervalEnd })
WITH e, upLeg, db1
MATCH downLeg =(db2)-[:DELIVERY_ROUTE]->(e)
WHERE ALL (r IN relationships(downLeg) WHERE r.start_date <= { intervalStart } AND r.end_date >= { intervalEnd })
WITH db1, db2, upLeg, downLeg
MATCH topRoute =(db1)<-[:CONNECTED_TO]-()-[:CONNECTED_TO]-(db2)
WHERE ALL (r IN relationships(topRoute) WHERE r.start_date <= { intervalStart } AND r.end_date >= { intervalEnd })
WITH upLeg, downLeg, topRoute, reduce(weight=0, r IN relationships(topRoute)| weight+r.cost) AS score
ORDER BY score ASC LIMIT 1
RETURN (nodes(upLeg)+ tail(nodes(topRoute))+ tail(nodes(downLeg))) AS n