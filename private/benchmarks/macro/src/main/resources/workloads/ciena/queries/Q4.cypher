MATCH (port:PhysicalPort)
WHERE NOT (port:Model)
  AND $latest in port.latest
WITH port
OPTIONAL MATCH (port)<-[:HAS]-(card:Card)
WHERE NOT (card:Model)
  AND $latest in card.latest
WITH port, card
OPTIONAL MATCH (port)<-[:HAS]-(pluggable:Pluggable)
WHERE NOT (pluggable:Model)
  AND $latest in pluggable.latest
WITH port, card, pluggable
OPTIONAL MATCH (port)<-[:HAS_CONNECTION_COMPONENT{isEndOfConnection:'Y'}]-(physicalConnection:PhysicalConnection)
WHERE NOT (physicalConnection:Model)
  AND $latest in physicalConnection.latest
WITH port, card, pluggable, physicalConnection
OPTIONAL MATCH (physicalConnection)-[:HAS_CONNECTION_COMPONENT{isEndOfConnection:'Y'}]->(simplePort:PhysicalPort)
WHERE NOT(simplePort = port)
  AND NOT (simplePort:Model)
  AND $latest in simplePort.latest
WITH port, card, pluggable, physicalConnection, simplePort
RETURN  DISTINCT 
 port.drniId AS rowObjectId ,
 112 AS rowObjectClass ,
 port.name AS C0,
 port.drniId AS OBJECTID1,
 112 AS OBJECTCLASS1,
 port.precomputedtypename AS C1,
 port.status AS C2, card.name AS C3,
 card.drniId AS OBJECTID2,
 110 AS OBJECTCLASS2,
 pluggable.name AS C4,
 pluggable.drniId AS OBJECTID3,
 111 AS OBJECTCLASS3,
 physicalConnection.name AS C5,
 physicalConnection.drniId AS OBJECTID4,
 122 AS OBJECTCLASS4,
 physicalConnection.status AS C6,
 simplePort.name AS C7,
 simplePort.drniId AS OBJECTID5,
 112 AS OBJECTCLASS5,
 simplePort.status AS C8
ORDER BY C0 DESC ,  OBJECTID1,  OBJECTID2,  OBJECTID3,  OBJECTID4,  OBJECTID5
SKIP 407800 LIMIT 100