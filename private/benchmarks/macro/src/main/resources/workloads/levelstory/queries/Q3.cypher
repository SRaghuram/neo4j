MATCH (u:user {id: {p01}})
WITH u
MATCH (u)-[:CONTACT]->(c:contact)-[:CONTACT*1..3]->(r:projectrole)-[:CONTACT*1..3]->(p:project {status: 'active'})
WITH u, r
MATCH (r)-[:ALERT]->(:alerts)-[x:ALERT]->(y)
WITH u,
     {
       id:         x.id,
       object:     x.object,
       projectid:  y.projectid,
       alerttype:  x.alerttype,
       severity:   coalesce(x.severity, 0),
       visibility: r.name,
       code:       x.code,
       data:       x.data,
       createdat:  x.createdat,
       isread:     coalesce(u.lastreadalert, 0) >= x.createdat,
       target:     {id: y.id, object: y.object, name: y.name, description: tostring(y.no)}
     } AS r
  LIMIT 100
RETURN collect(r) AS r
