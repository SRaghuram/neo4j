MATCH (ah:AccountHolder)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN]->(acct1:CreditAccount),
      (ah)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN]->(acct2:Late:CreditAccount)
WHERE id(acct1)<>id(acct2) AND
      acct1.dateOpened<=acct2.dateOpened AND
      duration.inDays(acct1.dateOpened,acct2.dateOpened).days<=7
RETURN  ah.accountName AS accountName, acct1.dateOpened AS startDate, count(acct2)+1 AS numAccts, sum(acct2.balance)+acct1.balance AS balanceAtRisk
ORDER BY balanceAtRisk DESC
LIMIT 20