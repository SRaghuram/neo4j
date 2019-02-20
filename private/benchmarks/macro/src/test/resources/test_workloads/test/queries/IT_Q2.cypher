FOREACH (i IN range(0,1000) |
  CREATE (n)
  SET  n.string = {string}
  SET  n.integer = {integer}
  SET  n.long = {long}
  SET  n.float = {float}
  SET  n.double = {double}
  SET  n.stringArr = {stringArr}
  SET  n.integerArr = {integerArr}
  SET  n.longArr = {longArr}
  SET  n.floatArr = {floatArr}
  SET  n.doubleArr = {doubleArr}
)
