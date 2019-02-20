MATCH (gb:Country { name:'United Kingdom' }),(usa:Country { name:'United States' }),(a:Artist)-[:FROM_AREA]-(gb),(a:Artist)-[:RECORDING_CONTRACT]-(l:Label),(l)-[:FROM_AREA]-(usa)
RETURN a,l,usa,gb