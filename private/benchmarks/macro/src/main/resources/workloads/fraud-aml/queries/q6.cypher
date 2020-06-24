WITH $date AS curDate
WITH curDate, curDate-duration({days:35}) AS curDate35
MATCH (cu:Customer:AccountHolder)-[:HAS_ACCOUNT]->(da:DepositAccount)<-[:DEPOSITS]-(txn:Cash:Transaction)
WHERE txn.transactionDate >= curDate35
  AND txn.transactionDate <= curDate 
WITH cu,
     da,
	 min(txn.transactionDate) AS startDate,
	 max(txn.transactionDate) AS endDate,
	 duration.inDays(min(txn.transactionDate), max(txn.transactionDate)).days AS numDays,
	 count(txn.amount) AS numDeposits,
	 sum(txn.amount) AS totalDeposits,
	 max(txn.amount) AS maxDeposit,
	 avg(txn.amount) AS avgDeposit
WHERE totalDeposits > 8500
RETURN cu.accountName,
       da.accountNumber,
       startDate,
       endDate,
       numDays,
       numDeposits,
       avgDeposit,
       maxDeposit,
       totalDeposits
ORDER BY totalDeposits DESC
LIMIT 100
