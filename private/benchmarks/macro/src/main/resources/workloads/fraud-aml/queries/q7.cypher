WITH $date AS curDate
WITH curDate, curDate-duration({days:35}) AS curDate35
MATCH (cu:Customer:AccountHolder)-[:HAS_ACCOUNT]->(da:DepositAccount)<-[:DEPOSITS]-(txn:Cash:Transaction)
WHERE txn.transactionDate >= curDate35
  AND txn.transactionDate <= curDate
WITH da.accountID AS accountID,
	 min(txn.transactionDate) AS startDate,
	 max(txn.transactionDate) AS endDate,
	 duration.inDays(min(txn.transactionDate), max(txn.transactionDate)).days AS numDays,
	 count(txn.amount) AS numDeposits,
	 sum(txn.amount) AS totalDeposits
WHERE totalDeposits > 8500
WITH collect({accountID: accountID, startDate: startDate, endDate: endDate, numDays: numDays,
		numDeposits: numDeposits, totalDeposits: totalDeposits}) AS possAggregators

//Step 2...now find all the rapid withdrawals of high percentage of deposits
UNWIND possAggregators AS curAgg
MATCH (da:DepositAccount)-[:WITHDRAWALS]->(txn:Cash:Transaction)
WHERE da.accountID = curAgg.accountID
  AND txn.transactionDate >= curAgg.startDate
  AND txn.transactionDate <= curAgg.endDate+duration({days:10})
WITH da.accountID AS AccountID,
	curAgg.startDate AS startDate,
	max(txn.transactionDate) AS endDate,
	duration.inDays(curAgg.startDate,max(txn.transactionDate)).days AS numDays,
	curAgg.numDeposits AS numDeposits,
	curAgg.totalDeposits AS totalDeposits,
	count(txn.amount) AS numWithdrawals,
	sum(txn.amount) AS totalWithdrawals,
	abs(sum(txn.amount))/curAgg.totalDeposits AS pctWithdrawal
WHERE pctWithdrawal > 0.75

// Step 3 - Now add in the customer info
MATCH (cu:Customer:AccountHolder)-[:HAS_ACCOUNT]->(da:DepositAccount)
WHERE da.accountID = AccountID
RETURN cu.accountHolderID,
       cu.lastName,
       cu.firstName,
       da.accountNumber,
       startDate,
       endDate,
       numDeposits,
       totalDeposits,
       numWithdrawals,
       totalWithdrawals,
       pctWithdrawal
ORDER BY pctWithdrawal DESC, totalDeposits DESC
LIMIT 100
