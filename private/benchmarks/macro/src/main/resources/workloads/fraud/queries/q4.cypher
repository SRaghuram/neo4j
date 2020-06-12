MATCH (acct1:CreditAccount)<-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN|HAS_UNSECUREDLOAN]-(cu1:Customer:AccountHolder),
      (cu1)-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|CURRENT_RESIDENCE|USES_DEVICE]->(info),
      (info)<-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|CURRENT_RESIDENCE|USES_DEVICE]-(cu2:Customer:AccountHolder),
      (cu2)-[:HAS_CREDITCARD|HAS_MORTGAGE|HAS_LINEOFCREDIT|HAS_AUTOLOAN]-(acct2:CreditAccount)
WHERE id(cu1)<>id(cu2) AND
      duration.inDays(acct1.dateOpened,acct2.dateOpened).days < 90 AND
      acct1.dateOpened <= acct2.dateOpened
WITH cu1.accountName AS Customer1,
     labels(acct1)[1..3] AS AccountType1,
     acct1.accountNumber AS Account1,
     acct1.dateOpened AS Opened1,
     duration.inDays(acct1.dateOpened,acct2.dateOpened).days AS DaysBetween,
     cu2.accountName AS Customer2,
     labels(acct2)[1..3] AS AccountType2,
     acct2.accountNumber AS Account2,
     acct2.dateOpened AS Opened2,
     collect(labels(info)[0]) AS SharedInfo
RETURN Customer1, AccountType1, Account1, Opened1, DaysBetween, Customer2, AccountType2, Account2, Opened2, SharedInfo
ORDER by Customer1, Customer2
LIMIT 100