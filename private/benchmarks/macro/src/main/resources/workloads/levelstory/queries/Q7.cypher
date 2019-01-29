MATCH (u:user {id: {p01}}), (x:material {id: {p02}})
WITH u, x
MATCH (p:project {id: x.projectid})
WITH u, p, x
MATCH (u)-[:CONTACT]->(c:contact {teamid: p.teamid})-[:CONTACT]->(cr:role {teamid: p.teamid})
WITH p, x, c, cr, [r IN (c)-[:CONTACT]->(:projectrole {projectid: p.id}) | last(nodes(r))] AS prs
WITH p, x, c, cr, extract(r IN prs | r.name) AS prs
WITH x,
     c,
     {
       accessedby:       c.id,
       accessedteamrole: cr.name,
       accessedrole:     CASE
                           WHEN cr.name IN ['Owners', 'Admins'] THEN cr.name
                           WHEN 'ProjectAdmins' IN prs THEN 'ProjectAdmins'
                           WHEN 'ProjectClients' IN prs THEN 'ProjectClients'
                           WHEN 'ProjectContacts' IN prs THEN 'ProjectContacts'
                           ELSE 'None'
                           END} AS r
WITH x AS m, r
MATCH (ha:activity)<-[:HISTORY]-(m)-[:CREATED]->(ca:activity)
WITH m,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    ca.createdat,
       createdby:    {id: ca.contactid, object: 'contact'},
       updatedat:    ha.createdat,
       updatedby:    {id: ha.contactid, object: 'contact'}
     } AS r
WITH m, [s IN (m)<-[:MATERIAL]-(:task)-[:SCHEDULE]->(:schedule) | last(nodes(s))] AS schedules, r
WITH m,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       neededat:     reduce(s = 0, x IN schedules |
                     CASE
                       WHEN s = 0 OR x.startat < s THEN x.startat
                       ELSE s
                       END)
     } AS r
MATCH (x)-[rel]->(m)
WITH m, collect(x) AS xs, collect(rel) AS rels, r
WITH m,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       neededat:     r.neededat,
       tasks:        [x IN xs WHERE x:task | {id: x.id, object: 'task'}],
       lineitems:    [x IN xs WHERE x:lineitem | {id:        x.id,
                                                  invoiceid: x.invoiceid,
                                                  object:    'lineitem',
                                                  amount:    x.amount,
                                                  rate:      x.rate}],
       alerts:       [x IN rels WHERE type(x) = 'ALERT' |
                     {
                       id:         x.id,
                       object:     'alert',
                       alerttype:  x.alerttype,
                       severity:   coalesce(x.severity, 0),
                       visibility: r.name,
                       code:       x.code,
                       data:       x.data,
                       createdat:  x.createdat}]
     } AS r
MATCH (m)-[rel]->(x)
WITH m, collect({node: x, rel: rel}) AS xs, r
RETURN
  collect({
    id:                 m.id,
    object:             m.object,
    projectid:          m.projectid,
    status:             m.status,
    no:                 m.no,
    name:               m.name,
    description:        m.description,
    ischange:           m.ischange,
    isfixedcost:        m.isfixedcost,
    fixedcost:          m.fixedcost,
    manufacturer:       m.manufacturer,
    partno:             m.partno,
    imageurl:           m.imageurl,
    url:                m.url,
    quantity:           m.quantity,
    unit:               m.unit,
    unitprice:          m.unitprice,
    markup:             m.markup,
    applymarkup:        m.applymarkup,
    leadtime:           m.leadtime,
    orderedat:          m.orderedat,
    deliverymethod:     m.deliverymethod,
    trackingnumber:     m.trackingnumber,
    expectedat:         m.expectedat,
    shippedat:          m.shippedat,
    deliveredat:        m.deliveredat,
    purchasenotes:      m.purchasenotes,
    needsreimbursement: m.needsreimbursement,
    accessedrole:       r.accessedrole,
    accessedby:         r.accessedby,
    createdat:          r.createdat,
    createdby:          r.createdby,
    updatedat:          r.updatedat,
    updatedby:          r.updatedby,
    neededat:           r.neededat,
    tasks:              r.tasks,
    lineitems:          r.lineitems,
    alerts:             r.alerts,
    vendor:             head([x IN xs WHERE x.node:contact AND type(x.REL) = 'VENDOR' | {id: x.node.id, object: 'contact'}]),
    purchaser:          head([x IN xs WHERE x.node:contact AND type(x.REL) = 'CONTACT' | {id: x.node.id, object: 'contact'}]),
    area:               head([x IN xs WHERE x.node:area | {id: x.node.id, object: 'area', name: x.node.name}]),
    messages:           [x IN xs WHERE x.node:message | {id: x.node.id, object: 'message'}],
    files:              [x IN xs WHERE x.node:file | {id: x.node.id, object: 'file'}],
    tags:               [x IN xs WHERE x.node:tag | {id: x.node.id, object: 'tag', name: x.node.name}]
  }) AS r
