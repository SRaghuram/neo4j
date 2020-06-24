WITH $date AS curDate, 150000 AS curThreshold
WITH curThreshold, curDate, curDate-duration({days:15}) AS curDate35
MATCH (txn:Transaction)
WHERE txn.transactionDate <= curDate
  AND txn.transactionDate >= curDate35
WITH txn.accountID AS curAccountID,
     txn.transactionDate AS transactionDate,
     curThreshold,
	 sum((CASE WHEN txn.amount<0 THEN txn.amount ELSE 0 END)) AS sumWithdrawals,
	 sum((CASE WHEN txn.amount>0 THEN txn.amount ELSE 0 END)) AS sumDeposits
WHERE sumWithdrawals < -1*curThreshold
   OR sumDeposits > curThreshold
   OR (sumDeposits > 7500 AND abs(sumWithdrawals) > sumDeposits*0.75)
WITH curAccountID, transactionDate, sumWithdrawals, sumDeposits, curThreshold
MATCH (ah:AccountHolder)-[:HAS_ACCOUNT]-(acct:DepositAccount)
WHERE acct.accountID=curAccountID
  // ignore vetted customers
  AND ((ah.FinCEN_110_status IS NULL) OR (ah.FinCEN_110_status = "ineligible"))
RETURN ah.accountName,
       acct.accountNumber,
       transactionDate,
       sumWithdrawals,
       sumDeposits
LIMIT 250
