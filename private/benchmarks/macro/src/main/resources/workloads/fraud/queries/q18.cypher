MATCH p=(ah:AccountHolder)-[:HAS_LINEOFCREDIT|HAS_MORTGAGE|HAS_AUTOLOAN|HAS_UNSECUREDLOAN|HAS_CREDITCARD]->(acct:Delinquent:CreditAccount),
      (acct)-[:BASED_ON]->(:CreditApplication)-[:CHECK_CREDIT]-(:CreditInquiry)-[:INQUIRY_DETAILS]->(:CreditInquiryAccount)<-[:APPEARS_ON]-(acct2:CreditAccount)
WHERE duration.inDays(acct2.dateOpened,acct.dateOpened).days <= 90 AND duration.inDays(acct2.dateOpened,acct.dateOpened).days >= 30
RETURN ah.accountName,
       acct.accountNumber AS DelinquentAccount,
       acct.balance AS DelinquentBalance,
       acct2.accountNumber AS PrevAccount,
       acct2.balance AS PrevAcctBalance,
       duration.inDays(acct2.dateOpened,acct.dateOpened).days AS daysBetween
ORDER BY DelinquentBalance DESC, PrevAcctBalance DESC
LIMIT 1000