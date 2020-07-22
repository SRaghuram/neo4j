MATCH (port:PhysicalPort)
WHERE NOT (1 IN port.latest)
  AND exists(port.drniId)
  AND exists(port.name)
  AND exists(port.lastModifiedDate)
RETURN port
ORDER BY p_name, p_drniId, p_lastModifiedDate
SKIP 0
LIMIT 25