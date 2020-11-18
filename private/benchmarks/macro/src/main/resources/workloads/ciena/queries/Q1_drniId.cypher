MATCH (port:PhysicalPort)
WHERE NOT (1 IN port.latest)
  AND port.drniId IS NOT NULL
  AND port.name IS NOT NULL
  AND port.lastModifiedDate IS NOT NULL
WITH port
OPTIONAL MATCH (port)<-[:HAS]-(card:Card)
WITH port, card
OPTIONAL MATCH (port)<-[:HAS]-(pluggable:Pluggable)
WITH port, card, pluggable
MATCH (port)<-[:HAS*1..9]-(parentDevice:Device)
WITH port, card, pluggable, parentDevice
MATCH (port)<-[:HAS*1..9]-(device:Device)<-[:HAS]-(parentLocation:Location)
WITH port, card, pluggable, parentDevice, parentLocation
OPTIONAL MATCH (port)<-[:HAS_CONNECTION_COMPONENT{isEndOfConnection:'Y'}]-(physicalConnection:PhysicalConnection)
WITH port, card, pluggable, parentDevice, parentLocation, physicalConnection
OPTIONAL MATCH (physicalConnection)-[:HAS_CONNECTION_COMPONENT{isEndOfConnection:'Y'}]->(simplePort:PhysicalPort)
WHERE NOT (simplePort.drniId = port.drniId)
WITH port, card, pluggable, parentDevice, parentLocation, physicalConnection, simplePort
MATCH (simplePort)<-[:HAS*1..9]-(otherEndParentDevice:Device)
WITH port, card, pluggable, parentDevice, parentLocation, physicalConnection, simplePort, otherEndParentDevice
MATCH (simplePort)<-[:HAS*1..9]-(device:Device)<-[:HAS]-(otherEndParentLocation:Location)
WITH port, card, pluggable, parentDevice, parentLocation, physicalConnection, simplePort, otherEndParentDevice, otherEndParentLocation
RETURN DISTINCT   port.drniId AS p_drniId,
                    port.name AS p_name,
        port.lastModifiedDate AS p_lastModifiedDate,
              port.portNumber AS p_portNumber,
                  port.status AS p_status,
                  card.drniId AS c_drniId,
                    card.name AS c_name,
             pluggable.drniId AS pl_drniId,
               pluggable.name AS pl_name,
          parentDevice.drniId AS pd_drniId,
            parentDevice.name AS pd_name,
        parentLocation.drniId AS ploc_drniId,
          parentLocation.name AS ploc_name,
      physicalConnection.name AS pc_name,
    physicalConnection.drniId AS pc_drniId,
    physicalConnection.status AS pc_status,
              simplePort.name AS sp_name,
            simplePort.drniId AS sp_drniId,
            simplePort.status AS sp_status,
    otherEndParentDevice.name AS oepd_name,
  otherEndParentDevice.drniId AS oepd_drniId,
  otherEndParentLocation.name AS oepl_name,
otherEndParentLocation.drniId AS oepl_drniId
ORDER BY p_drniId, p_name, p_lastModifiedDate
SKIP 0
LIMIT 25
