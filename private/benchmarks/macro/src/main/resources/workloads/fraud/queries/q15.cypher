MATCH (emp: Employee)
WHERE emp.employeeID >= $start AND emp.employeeID < $end
MATCH p=(emp)-[:EMPLOYEE_ACCOUNT]->(ah:AccountHolder)-[:ENTITY_RESOLUTION|HAS_MORTGAGE|HAS_AUTOLOAN|HAS_UNSECUREDLOAN|HAS_CREDITCARD*1..2]->
        (acct:CreditAccount)-[:BASED_ON]-(app:CreditApplication)-[:APPROVED_BY|REVIEWED_BY]->(emp2:Employee)-[:EMPLOYEE_ACCOUNT]->
        (ah2:AccountHolder)<-[:MAKES_PAYMENTS_TO*1..3]-(ah)
RETURN p