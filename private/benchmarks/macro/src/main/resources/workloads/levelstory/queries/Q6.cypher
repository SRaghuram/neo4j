MATCH (u:user {id: {p01}}), (x:invoice {id: {p02}})
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
WITH x AS i, r
MATCH (ha:activity)<-[:HISTORY]-(i)-[:CREATED]->(ca:activity)
WITH i,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    ca.createdat,
       createdby:    {id: ca.contactid, object: 'contact'},
       updatedat:    ha.createdat,
       updatedby:    {id: ha.contactid, object: 'contact'}
     } AS r
MATCH (i)-->(x)
WITH i, collect({node: x, related: [r IN (x)-[:LINEITEM]->() | last(nodes(r))]}) AS xs, r
WITH i,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       billto:       head([x IN xs WHERE x.node:contact | {id: x.node.id, object: 'contact'}]),
       payments:     [x IN xs WHERE x.node:payment |{
       id:          x.node.id,
       object:      'payment',
       amount:      x.node.amount,
       paidat:      x.node.paidat,
       isdeposit:   x.node.isdeposit,
       description: x.node.description}],
       lineitems:
                     [x IN xs WHERE x.node:lineitem | {
                       id:          x.node.id,
                       object:      'lineitem',
                       no:          x.node.no,
                       name:        x.node.name,
                       description: x.node.description,
                       amount:      x.node.amount,
                       unit:        x.node.unit,
                       rate:        x.node.rate,
                       markup:      x.node.markup,
                       istaxed:     x.node.istaxed,
                       isheader:    x.node.isheader,
                       isfixedcost: x.node.isfixedcost,
                       fixedcost:   x.node.fixedcost,
                       item:        head([y IN x.related WHERE y:task OR y:material | {id: y.id, object: y.object}])
                     }]
     } AS r
MATCH ()-[rel]->(i)
WITH i, collect(rel) AS rels, r
RETURN
  collect({
    id:           i.id,
    object:       i.object,
    status:       i.status,
    invoiceno:    i.invoiceno,
    projectid:    i.projectid,
    no:           i.no,
    name:         i.name,
    description:  i.description,
    street:       i.street,
    locality:     i.locality,
    region:       i.region,
    postcode:     i.postcode,
    country:      i.country,
    neighborhood: i.neighborhood,
    salestax:     i.salestax,
    sentat:       i.sentat,
    graceperiod:  i.graceperiod,
    terms:        i.terms,
    conditions:   i.conditions,
    accessedrole: r.accessedrole,
    accessedby:   r.accessedby,
    createdat:    r.createdat,
    createdby:    r.createdby,
    updatedat:    r.updatedat,
    updatedby:    r.updatedby,
    lineitems:    r.lineitems,
    billto:       r.billto,
    payments:     r.payments,
    alerts:       [x IN rels WHERE type(x) = 'ALERT' | {id:         x.id,
                                                        object:     'alert',
                                                        alerttype:  x.alerttype,
                                                        severity:   coalesce(x.severity, 0),
                                                        visibility: r.name,
                                                        code:       x.code,
                                                        data:       x.data,
                                                        createdat:  x.createdat}]
  })AS r
