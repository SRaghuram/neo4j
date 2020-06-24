WITH $date AS curDate, $date-duration({days:45}) AS lookbackDate
MATCH path=(sourceParty:AccountHolder)-[:HAS_ACCOUNT]->(sourceAccount:ExternalAccount)-[:CREDITS]->
	       (txnIn:Transaction)-[:DEPOSITS]->(ourAccount:DepositAccount)<-[:HAS_ACCOUNT]-(ourParty:AccountHolder)
WHERE ((sourceParty)-[:IS_LISTED_ON]->(:WatchList)
       OR ourAccount.FinCEN_110_status IS NULL
       OR ourAccount.FinCEN_110_status = "ineligible"
       )
  AND lookbackDate <= txnIn.transactionDate <= curDate
WITH curDate,
     lookbackDate,
     ourAccount,
     ourParty,
     sourceParty,
     sourceAccount,
	 sum(txnIn.amount) AS sumDeposits,
	 count(txnIn) AS numDeposits,
     avg(txnIn.amount) AS avgDeposit,
     max(txnIn.amount) AS maxDeposit,
	 min(txnIn.transactionDate) AS firstDepositDate,
	 max(txnIn.transactionDate) AS lastDepositDate
WHERE sumDeposits > 9000
WITH curDate,
     lookbackDate,
     ourAccount,
     ourParty,
	 collect({srcParty: id(sourceParty),
		      srcAccount: id(sourceAccount),
		      sumDeposits: sumDeposits,
		      numDeposits: numDeposits,
		      avgDeposit: avgDeposit,
		      maxDeposit: maxDeposit,
		      firstDate: firstDepositDate,
		      lastDate: lastDepositDate}) AS sourceList

// now that we have a list of source accounts to our accounts with high deposits
// we need to find out which of those accounts made transfers to pay off loans
// at a faster rate than normal (e.g. payments 1.5x expected)… note we earlier
// excluded accounts that had a FinCEN Form 110 on file, so this is either accounts
// that need to be watched (e.g. Casino’s) or new accounts

MATCH (ourAccount)-[:WITHDRAWALS]->(txnOut:Transaction)-[:PAYMENT_XFER]->
	(pay:Payment)<-[:ACCOUNT_PAYMENTS]-
	(credit:CreditAccount)<-[:HAS_MORTGAGE|HAS_LINEOFCREDIT]-(ourParty)
WHERE lookbackDate <= txnOut.transactionDate <= curDate
WITH curDate,
     lookbackDate,
     sourceList,
     ourAccount,
     ourParty,
     credit,
	 sum(pay.amount) AS sumPayments,
	 count(pay) AS numPayments,
     avg(pay.amount) AS avgPayment,
     max(pay.amount) AS maxPayment,
	 min(txnOut.transactionDate) AS firstXferDate,
	 max(txnOut.transactionDate) AS lastXferDate
WHERE sumPayments > credit.paymentAmount*1.5
WITH curDate, lookbackDate, sourceList, ourAccount, ourParty, credit,
	sumPayments, numPayments, avgPayment, maxPayment, firstXferDate, lastXferDate

// generate a graph of our results
UNWIND sourceList AS curSource
MATCH sourcePath=(sourceParty:AccountHolder)-[:HAS_ACCOUNT]->(sourceAccount:ExternalAccount)
	, ourPath=(ourAccount)-[:WITHDRAWALS]->(txnOut:Transaction)-[:PAYMENT_XFER]->
	(pay:Payment)<-[:ACCOUNT_PAYMENTS]-(credit)<-[:HAS_MORTGAGE|HAS_LINEOFCREDIT]-(ourParty)-[:HAS_ACCOUNT]->(ourAccount)
WHERE id(sourceParty) = curSource.srcParty
  AND id(sourceAccount) = curSource.srcAccount
  AND lookbackDate <= txnOut.transactionDate <= curDate
RETURN *
