WITH $date AS curDate
WITH curDate, curDate-duration({days:35}) AS curDate35
MATCH p=(cu1:AccountHolder)-[:HAS_ACCOUNT]->(da1:DepositAccount)-[:WITHDRAWALS]->(txn1)-[:DEBITS]->(acctB)
		-[:CREDITS]->(txn2)-[:DEPOSITS]->(da2:DepositAccount)<-[:HAS_ACCOUNT]-(cu2:AccountHolder)
		, (cuB:AccountHolder)-[:HAS_ACCOUNT]->(acctB)
WHERE id(da1) <> id(da2)
  AND txn1.transactionDate <= curDate
  AND txn1.transactionDate >= curDate35
  AND txn2.transactionDate>=txn1.transactionDate
  AND duration.inDays(txn1.transactionDate,txn2.transactionDate).days < 30
  AND txn2.amount > abs(txn1.amount) * 0.85
  AND txn2.amount < abs(txn1.amount)
  AND abs(txn1.amount) > 500.0
WITH da1.accountNumber AS senderAccount,
     cu1.accountName AS sendAccountName,
     sum(abs(txn1.amount)) AS amtSent,
     count(txn1) AS numXmit,
     acctB.accountNumber AS nextAccount,
     cuB.accountName AS nextAccountName,
     sum(abs(txn2.amount)) AS amtRcv,
     count(txn2) AS numRcv,
     da2.accountNumber AS receiverAccount,
     cu2.accountName AS rcvAccountName
WITH sum(amtSent) AS totalSent,
     sum(numXmit) AS totalTxns,
	 sum(amtRcv) AS totalRcv,
	 sum(numRcv) AS numRcv,
	 receiverAccount,
	 rcvAccountName
WHERE numRcv > 2 AND totalRcv > 6500
RETURN totalSent,
       totalTxns,
       totalRcv,
       numRcv,
       receiverAccount,
       rcvAccountName
ORDER by totalSent DESC
