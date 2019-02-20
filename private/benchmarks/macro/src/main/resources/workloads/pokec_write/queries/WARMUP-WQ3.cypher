MATCH (p1:PROFILES { I_like_specialties_from_kitchen: 'talianskej' })
MATCH (p2:PROFILES { _key: { key }})
RETURN p1._key, p2._key