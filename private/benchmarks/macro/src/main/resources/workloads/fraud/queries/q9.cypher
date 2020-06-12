MATCH p=(emp:Employee)-[:EMPLOYEE_ACCOUNT]->(ah:AccountHolder),
      (ah)-[:ENTITY_RESOLUTION|HAS_MORTGAGE|HAS_AUTOLOAN|HAS_UNSECUREDLOAN|HAS_LINEOFCREDIT|HAS_CREDITCARD*1..2]->(acct:CreditAccount),
      (acct)-[:BASED_ON]-(app:CreditApplication)-[:APPROVED_BY|REVIEWED_BY]->(emp)
RETURN p