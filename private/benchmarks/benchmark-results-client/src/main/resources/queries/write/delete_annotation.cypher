MATCH  (a:Annotation {event_id: {event_id}})
DETACH DELETE a
