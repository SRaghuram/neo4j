MATCH (device:Device)
WHERE NOT (device:Model)
  AND $latest in device.latest
  AND NOT toInteger(8) in device.latest
  AND exists(device.name)
WITH device
OPTIONAL MATCH (device)<-[:MANAGES_POWER_FOR]-(network:Network {name:'Device Power Zone'})
WHERE NOT (network:Model)
  AND $latest in network.latest
  AND exists(network.drniId)
WITH device, network
OPTIONAL MATCH (rack:Rack {name:device.rackName})<-[:MANAGES_POWER_FOR]-(network2:Network {name:'Rack Power Zone'})
WHERE NOT (network2:Model)
  AND $latest in network2.latest
  AND exists(network2.drniId)
WITH device, network, network2
RETURN count(*)