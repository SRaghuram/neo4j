MATCH (port:PhysicalPort)
WHERE NOT (1 IN port.latest)
  AND exists(port.drniId)
  AND exists(port.name)
  AND exists(port.lastModifiedDate)
RETURN port
ORDER BY port.name, port.drniId, port.lastModifiedDate
SKIP 0
LIMIT 25