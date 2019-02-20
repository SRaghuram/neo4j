UNWIND range(0,1000) AS i
RETURN
  i,
  {string} AS string,
  {integer} AS integer,
  {long} AS long,
  {float} AS float,
  {double} AS double,
  {stringArr} AS stringArr,
  {integerArr} AS integerArr,
  {longArr} AS longArr,
  {floatArr} AS floatArr,
  {doubleArr} AS doubleArr
