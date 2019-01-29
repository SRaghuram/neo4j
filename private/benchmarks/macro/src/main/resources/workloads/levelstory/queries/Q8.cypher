MATCH (u:user {id: {p01}}), (x:task {id: {p02}})
WITH u, x
MATCH (p:project {id: x.projectid})
WITH u, p, x
MATCH  (u)-[:CONTACT]->(c:contact {teamid: p.teamid})-[:CONTACT]->(cr:role {teamid: p.teamid})
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
WITH x AS t, r
MATCH (ha:activity)<-[:HISTORY]-(t)-[:CREATED]->(ca:activity)
WITH t,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    ca.createdat,
       createdby:    {id: ca.contactid, object: 'contact'},
       updatedat:    ha.createdat,
       updatedby:    {id: ha.contactid, object: 'contact'}
     } AS r
MATCH (t)-[:SCHEDULE]->(s:schedule)
WITH t, s, r
MATCH (s)<-[rel]-(x)
WITH t, s, collect(x) AS xs, [c IN (:contact)-[:SCHEDULE]->(:calendar)-->(:year)-->(:week)-->(s) | head(nodes(c))] AS contacts, r
WITH t,
     {
       schedule:
                     {
                       id:              s.id,
                       object:          'schedule',
                       name:            s.name,
                       description:     s.description,
                       teamid:          s.teamid,
                       scheduletype:    s.scheduletype,
                       startat:         s.startat,
                       endat:           s.endat,
                       isautoscheduled: s.isautoscheduled,
                       flexscore:       s.flexscore,
                       schedules:       s.schedules,
                       worklogs:        s.worklogs,
                       info:            s.info,
                       contacts:
                                        extract(c IN reduce(a = [], c IN contacts |
                                        CASE
                                          WHEN c IN a THEN a
                                          ELSE a + c
                                          END) | {id: c.id, object: 'contact', name: c.name}),
                       equipments:      [x IN xs WHERE x:equipment | {id: x.id, object: 'equipment', name: x.name}]
                     },
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby
     } AS r
MATCH (t)<-[rel]-(x)
WITH t, collect({node: x, rel: rel}) AS xs, r
WITH t,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       schedule:     r.schedule,
       tasksafter:   [x IN xs WHERE x.node:task | {id: x.node.id, object: 'task'}],
       lineitems:    [x IN xs WHERE x.node:lineitem | {id: x.node.id, invoiceid: x.node.invoiceid, object: 'lineitem', amount: x.node.amount, rate: x.node.
       rate}],
       alerts:       [x IN xs WHERE type(x.rel) = 'ALERT' | {id:         x.rel.id,
                                                             object:     'alert',
                                                             alerttype:  x.rel.alerttype,
                                                             severity:   coalesce(x.rel.severity, 0),
                                                             visibility: x.rel.visibility,
                                                             code:       x.rel.code,
                                                             data:       x.rel.data,
                                                             createdat:  x.rel.createdat}]
     } AS r
MATCH (t)-[rel]->(x)
WITH t, collect({node: x, rel: rel}) AS xs, r
RETURN
  collect({
              id:            t.id,
              object:        t.object,
              projectid:     t.projectid,
              status:        t.status,
              no:            t.no,
              name:          t.name,
              description:   t.description,
              workhours:     t.workhours,
              priority:      t.priority,
              fixedcost:     t.fixedcost,
              isfixedcost:   t.isfixedcost,
              ischange:      t.ischange,
              sameday:       t.sameday,
              isunbillable:  t.isunbillable,
              minutestotal:  t.minutestotal,
              delayamount:   t.delayamount,
              delayperiod:   t.delayperiod,
              minassigned:   t.minassigned,
              maxassigned:   t.maxassigned,
              minschedule:   t.minschedule,
              scheduletype:  coalesce(t.scheduletype, 'none'),
              fixedstartat:  t.fixedstartat,
              fixedendat:    t.fixedendat,
              fixedschedule: t.fixedschedule,
              accessedrole:  r.accessedrole,
              accessedby:    r.accessedby,
              createdat:     r.createdat,
              createdby:     r.createdby,
              updatedat:     r.updatedat,
              updatedby:     r.updatedby,
              schedule:      r.schedule,
              tasksafter:    r.tasksafter,
              lineitems:     r.lineitems,
              alerts:        r.alerts,
              area:          head([x IN xs WHERE x.node:area |{id: x.node.id, object: 'area', name: x.node.name}]),
              tasktype:      head([x IN xs WHERE x.node:tasktype |{id: x.node.id, object: 'tasktype', name: x.node.name, baserate: x.node.baserate, color: x.node.color}]), assignedto:    [x IN xs WHERE x.node:contact |{id: x.node.id, object: 'contact'}], tasksbefore: [x IN xs WHERE x.node:task |{id: x.node.id, object: 'task', linkdata: {delayamount: coalesce(x.rel.delayamount, 0), delayperiod: coalesce(x.rel.delayperiod, 'hours')}}],
              materials:     [x IN xs WHERE x.node:material | {id: x.node.id, object: 'material'}],
              equipments:    [x IN xs WHERE x.node:equipment | {id: x.node.id, object: 'equipment'}],
              messages:      [x IN xs WHERE x.node:message | {id: x.node.id, object: 'message'}],
              files:         [x IN xs WHERE x.node:file | {id: x.node.id, object: 'file'}],
              tags:          [x IN xs WHERE x.node:tag | {id: x.node.id, object: 'tag', name: x.node.name}]}) AS r
