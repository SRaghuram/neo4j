MATCH (u:user {id: $p01}), (x:file {id: $p02})
WITH u, x
MATCH (p:project {id: x.projectid})
WITH u, p, x
MATCH (u)-[:CONTACT]->(c:contact {teamid: p.teamid})-[:CONTACT]->(cr:role {teamid: p.teamid})
WITH p, x, c, cr, [r IN (c)-[:CONTACT]->(:projectrole {projectid: p.id}) | last(nodes(r))] AS prs
WITH p, x, c, cr, [r IN prs | r.name] AS prs
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
                           END
     } AS r
WITH x AS f, r
MATCH (ha:activity)<-[:HISTORY]-(f)-[:CREATED]->(ca:activity)
WITH f,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    ca.createdat,
       createdby:    {id: ca.contactid, object: 'contact'},
       updatedat:    ha.createdat,
       updatedby:    {id: ha.contactid, object: 'contact'}
     } AS r
MATCH (f)-->(x)
WITH f, collect(x) AS xs, r
WITH f,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       tags:         [x IN xs WHERE x:tag | {id: x.id, object: 'tag', name: x.name}]
     } AS r
MATCH (x)-[:FILE]->(f)
WITH f, collect(x) AS xs, r
RETURN
  collect({
    id:           f.id,
    object:       f.object,
    projectid:    f.projectid,
    isinternal:   f.isinternal,
    no:           f.no,
    name:         f.name,
    description:  f.description,
    url:          f.url,
    mime:         f.mime,
    mimetype:     f.mimetype,
    thumbnail:    f.thumbnail,
    size:         f.size,
    accessedrole: r.accessedrole,
    accessedby:   r.accessedby,
    createdat:    r.createdat,
    createdby:    r.createdby,
    updatedat:    r.updatedat,
    updatedby:    r.updatedby,
    tags:         r.tags,
    related:      [x IN xs WHERE x.object IS NOT NULL | {id:x.id, object: x. object}]
  }) AS r
