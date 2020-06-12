MATCH path=(loan1:Mortgage:CreditAccount)-[:PROPERTY_LIEN]->(ml1:MortgageLien),
      (ml1)-[:HAS_LIEN_ON]->(addr:StreetAddress),
      (addr)<-[:HAS_LIEN_ON]-(ml2:MortgageLien),
      (ml2)<-[:PROPERTY_LIEN]-(loan2:Mortgage:CreditAccount),
      p2=(loan1)-[:BASED_ON]->(app1:CreditApplication)-[]->(emp:Employee),
      p3=(loan2)-[:BASED_ON]->(app2:CreditApplication)-[]->(emp2:Employee)
WHERE id(loan1)<>id(loan2)
RETURN path, p2, p3