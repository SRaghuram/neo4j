MATCH (port:PhysicalPort)
WHERE NOT (1 IN port.latest)
  AND port.drniId IS NOT NULL
  AND port.name IS NOT NULL
  AND port.lastModifiedDate IS NOT NULL
RETURN port
ORDER BY port.lastModifiedDate, port.name, port.drniId
SKIP 0
LIMIT 25