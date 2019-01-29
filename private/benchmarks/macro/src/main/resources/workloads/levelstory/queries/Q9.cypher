MATCH (u:user {id: {p01}}), (x:task {id: {p02}})
WITH u, x
MATCH (u:user {id: {p01}}), (t:task {id: {p02}})
WITH u, t
MATCH (u)-[:CONTACT]->(c:contact), (xp:project)-->(:tasks)-->(t)
  WHERE c.teamid = xp.teamid
WITH c, t, xp
OPTIONAL MATCH (c)-[:CONTACT]->(r:role)-[:CONTACT*1..10]->(xp)
WITH t, c, collect(r) AS rs
WITH t,
     {accessedby: c.id, accessedrole: CASE WHEN any(r IN rs
       WHERE r.name = 'Owners') THEN 'Owners' WHEN any(r IN rs
       WHERE r.name = 'Admins') THEN 'Admins' WHEN any(r IN rs
       WHERE r.name = 'ProjectAdmins') THEN 'ProjectAdmins' WHEN any(r IN rs
       WHERE r.name = 'ProjectClients') THEN 'ProjectClients' WHEN any(r IN rs
       WHERE r.name = 'ProjectContacts') THEN 'ProjectContacts'
       ELSE 'None'
       END} AS r
OPTIONAL MATCH (t)-[:CREATED]->(a:activity)
WITH t, {accessedrole: r.accessedrole, accessedby: r.accessedby, createdat: a.createdat, createdby: {id: a.contactid, object: 'contact'}} AS r
OPTIONAL MATCH (t)-[:HISTORY]->(a:activity)
WITH t,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    a.createdat,
       updatedby:    {id: a.contactid, object: 'contact'}}
     AS r
OPTIONAL MATCH (t)<-[:TASK|LINEITEM]-(x)
WITH t, collect(x) AS xs, r
WITH t,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       tasksafter:   [x IN xs WHERE x.object = 'task' | {id: x.id, object: x.object}],
       lineitems:    [x IN xs WHERE x.object = 'lineitem' | {id: x.invoiceid, object: 'lineitem', amount: x.amount, rate: x.rate}]
     } AS r
OPTIONAL MATCH (t)-[:TASKTYPE|AREA|CONTACT|TASK|MATERIAL|FILE|MESSAGE|TAG]->(x)
WITH t, collect(x) AS xs, r
WITH t,
     {
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       tasksafter:   r.tasksafter,
       lineitems:    r.lineitems,
       area:         head([x IN xs WHERE x.object = 'area' | {id: x.id, object: x.object}]),
       tasktype:     head([x IN xs WHERE x.object = 'tasktype' | {id: x.id, object: x.object, name: x.name}]),
       assignedto:   [x IN xs WHERE x.object = 'contact' | {id: x.id, object: x.object}],
       tasksbefore:  [x IN xs WHERE x.object = 'task' | {id: x.id, object: x.object}],
       materials:    [x IN xs WHERE x.object = 'material' | {id: x.id, object: x.object}],
       messages:     [x IN xs WHERE x.object = 'message' | {id: x.id, object: x.object}],
       files:        [x IN xs WHERE x.object = 'file' | {id: x.id, object: x.object}],
       tags:         [x IN xs WHERE x.object = 'tag' | {id: x.id, object: x.object, name: x.name}]
     } AS r
MATCH (t)-[:SCHEDULE]->(x:schedule)
OPTIONAL MATCH (x)<-[:SCHEDULE*1..4]-(c:contact)
OPTIONAL MATCH (c)<-[:CONTACT]-(u:user)
WITH t, x, collect(DISTINCT {contact: c, user: u}) AS cs, r
WITH t,
     {
       schedule:     {
                       id:              x.id,
                       object:          x.object,
                       name:            x.name,
                       description:     x.description,
                       teamid:          x.teamid,
                       scheduletype:    x.scheduletype,
                       startat:         x.startat,
                       endat:           x.endat,
                       isautoscheduled: x.isautoscheduled,
                       flexscore:       x.flexscore,
                       schedules:       x.schedules,
                       worklogs:        x.worklogs,
                       info:            x.info,
                       contacts:        [c IN cs |
                                        {
                                          id:     c.contact.id,
                                          object: c.contact.object,
                                          name:   coalesce(c.contact.displayname, c.contact.name, c.user.displayname, c.user.name)
                                        }]
                     },
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       tasksafter:   r.tasksafter,
       lineitems:    r.lineitems,
       area:         r.area,
       tasktype:     r.tasktype,
       assignedto:   r.assignedto,
       tasksbefore:  r.tasksbefore,
       materials:    r.materials,
       messages:     r.messages,
       files:        r.files,
       tags:         r.tags
     } AS r
OPTIONAL MATCH ()-[x:ALERT]->(t)
WITH t, collect(x) AS xs, r
WITH t,
     {
       schedule:     r.schedule,
       accessedrole: r.accessedrole,
       accessedby:   r.accessedby,
       createdat:    r.createdat,
       createdby:    r.createdby,
       updatedat:    r.updatedat,
       updatedby:    r.updatedby,
       tasksafter:   r.tasksafter,
       lineitems:    r.lineitems,
       area:         r.area,
       tasktype:     r.tasktype,
       assignedto:   r.assignedto,
       tasksbefore:  r.tasksbefore,
       materials:    r.materials,
       messages:     r.messages,
       files:        r.files,
       tags:         r.tags,
       alerts:       [x IN xs |
                     {
                       id:         x.id,
                       object:     x.object,
                       alerttype:  x.alerttype,
                       severity:   coalesce(x.severity, 0),
                       visibility: r.name,
                       code:       x.code,
                       data:       x.data,
                       createdat:  x.createdat
                     }]
     } AS r
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
    tasksafter:    r.tasksafter,
    lineitems:     r.lineitems,
    area:          r.area,
    tasktype:      r.tasktype,
    assignedto:    r.assignedto,
    tasksbefore:   r.tasksbefore,
    materials:     r.materials,
    messages:      r.messages,
    files:         r.files,
    tags:          r.tags,
    schedule:      r.schedule,
    alerts:        r.alerts
  }) AS r
