MATCH (n:OSMNode)
WHERE point({ latitude:50, longitude:14 })< n.location < point({ latitude:50.16, longitude:15 })
RETURN count(n)