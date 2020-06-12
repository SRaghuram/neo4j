MATCH path=(:CreditAccount)<-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN|HAS_UNSECUREDLOAN]-(cu1:AccountHolder),
           (cu1)-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|HAS_EIN|CURRENT_RESIDENCE|BUSINESS_ADDRESS|USES_DEVICE]->(info),
           (info)<-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|HAS_EIN|CURRENT_RESIDENCE|BUSINESS_ADDRESS|USES_DEVICE]-(cu2:AccountHolder),
           (cu2)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN|HAS_UNSECUREDLOAN]->(:CreditAccount)
WHERE id(cu1)<>id(cu2) AND
      cu1.accountName<>cu2.accountName
OPTIONAL MATCH path2=(cu1)-[:HAS_GOVT_ID]-(id1:GovtIssuedID)
OPTIONAL MATCH path3=(cu2)-[:HAS_GOVT_ID]-(id2:GovtIssuedID)
RETURN path, path2, path3