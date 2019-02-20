MATCH (x:Country { name: { name_1 }}),(y:Country { name: { name_2 }}),(a:Artist)-[:FROM_AREA]-(x),(a:Artist)-[:RECORDING_CONTRACT]-(l:Label),(l)-[:FROM_AREA]-(y)
RETURN a,l,y,x