MATCH (p:PROFILES { _key: { key }})
SET p = { name:p._key,_key:p._key }