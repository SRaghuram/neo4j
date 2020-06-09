MATCH (o1:Officer)-[r1]->(e:Entity)<-[r2]-(o2:Officer)
USING JOIN ON e
WHERE id(o1) < id(o2) AND NOT o1.name CONTAINS "LIMITED" AND NOT o1.name CONTAINS "Limited"
AND NOT o2.name CONTAINS "Limited" AND NOT o2.name CONTAINS "LIMITED"
AND size( (o1)-->() ) > 10 AND size( (o2)-->() ) > 10
WITH o1,o2,count(*) as freq, collect(e)[0..10] as entities
WHERE freq > 10
WITH * ORDER BY freq DESC LIMIT 10
RETURN o1.name, o2.name, freq, [e IN entities | e.name]