MATCH path=(addr:StreetAddress)<-[:CURRENT_RESIDENCE|BUSINESS_ADDRESS]-(ah:AccountHolder),
      (ah)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN]->(credit:CreditAccount)
WITH addr, count(credit) as numAccounts, sum(credit.balance) as TotalBalance, sum(credit.loanAmount) as TotalLoaned
WHERE numAccounts>2 AND TotalBalance>100000
RETURN addr, numAccounts, TotalBalance, TotalLoaned
ORDER BY TotalBalance DESC
LIMIT 20