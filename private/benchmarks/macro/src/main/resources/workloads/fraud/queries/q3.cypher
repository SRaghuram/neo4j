MATCH (ah:AccountHolder)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN]->(acct1:CreditAccount),
      (ah)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN]->(acct2:Delinquent:Late:CreditAccount)
WHERE id(acct1)<>id(acct2) AND
      acct1.dateOpened<=acct2.dateOpened AND
      duration.inDays(acct1.dateOpened, acct2.dateOpened).days < 90
RETURN ah.accountName as accountName, acct1.dateOpened as startDate, count(acct2)+1 as numAccts, sum(acct2.balance)+acct1.balance as balanceAtRisk
ORDER BY balanceAtRisk DESC
LIMIT 20