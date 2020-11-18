MATCH (device:Device)
WHERE NOT (device:Model)
  AND $latest in device.latest
  AND NOT toInteger(8) in device.latest
  AND device.name IS NOT NULL
WITH device
OPTIONAL MATCH (device)<-[:MANAGES_POWER_FOR]-(network:Network {name:'Device Power Zone'})
WHERE NOT (network:Model)
  AND $latest in network.latest
  AND network.drniId IS NOT NULL
WITH device, network
OPTIONAL MATCH (rack:Rack {name:device.rackName})<-[:MANAGES_POWER_FOR]-(network2:Network {name:'Rack Power Zone'})
WHERE NOT (network2:Model)
  AND $latest in network2.latest
  AND network2.drniId IS NOT NULL
WITH device, network, network2
RETURN DISTINCT
   device.drniId AS rowObjectId,
   108 AS rowObjectClass,
   device.name AS C0,
   device.drniId AS OBJECTID1,
   108 AS OBJECTCLASS1,
   device.precomputedtypename AS C1,
   device.modelNumber AS C2,
   device.ipAddress AS C3,
   device.role AS C4,
   device.status AS C5,
   device.rackName AS C6,
   device.rackPositions  AS C7,
   network.powerUtilization AS C8,
   network.drniId AS OBJECTID2,
   115 AS OBJECTCLASS2,
   network2.powerUtilization AS C9,
   network2.drniId AS OBJECTID3,
   115 AS OBJECTCLASS3
ORDER BY OBJECTID3
SKIP 0 LIMIT 100
