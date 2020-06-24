WITH $date-duration({months:1}) AS vStartDate,
     $date AS vEndDate,
     50000 AS vThreshold
MATCH (custB:BusinessCustomer:AccountHolder)-[:HAS_ACCOUNT]->(acctB:DepositAccount)-[:WITHDRAWALS]->
		(txnB:Transaction)-[:DEBITS|INTERNAL_XFER|RECEIVES_WIRE]->(acctC)<-[:HAS_ACCOUNT]-(custC:AccountHolder)
WHERE vStartDate <= txnB.transactionDate <= vEndDate
  AND abs(txnB.amount) >= vThreshold
  AND ((custB.FinCEN_110_status <> "on file")
		OR (NOT (custC)-[:IS_LEGAL_ENTITY]->(:LegalEntity))  // unknown business entity
		OR (NOT (custC)-[:HAS_GOVT_ID]->(:GovtIssuedID))     // unknown individual
		OR (custC:BusinessCustomer and custC.FinCEN_110_status<>"on file")   // payment to a nonvetted business
		OR ((custC)-[:IS_LISTED_ON]->(:WatchList))           // payment to someone on watchlist
	)
WITH vStartDate,
     vEndDate,
     vThreshold,
     custB,
     acctB,
     count(txnB.amount) AS numWithdrawals,
     abs(sum(txnB.amount)) AS sumWithdrawals,
     count(distinct custC) AS numTargets
	
// now that we have our list of potential concentrators, find ones
// with a lot of deposits from similar situations (e.g. unknown entities or 
// entities on the WatchList)
MATCH (custB)-[:HAS_ACCOUNT]->(acctB)<-[:DEPOSITS]-(txnB:Transaction)<-[:CREDITS|INTERNAL_XFER|SENDS_WIRE]-
	(acctA)<-[:HAS_ACCOUNT]-(custA:AccountHolder)
WHERE vStartDate <= txnB.transactionDate <= vEndDate
  AND (
  		(NOT (custA)-[:IS_LEGAL_ENTITY]->(:LegalEntity))     // unknown business entity
		OR (NOT (custA)-[:HAS_GOVT_ID]->(:GovtIssuedID))     // unknown individual
		OR (custA:BusinessCustomer and custA.FinCEN_110_status<>"on file")   // payment from a nonvetted business
		OR ((custA)-[:IS_LISTED_ON]->(:WatchList))           // payment from someone on watchlist
	)
WITH custB,
     acctB,
     numTargets,
     numWithdrawals,
     sumWithdrawals,
     count(txnB.amount) AS numDeposits,
     sum(txnB.amount) AS sumDeposits,
     count(distinct custA) AS numSources

// now we want to find where suspicious deposits are greater than suspicious withdrawals and rank by thresholds
WHERE sumDeposits >= sumWithdrawals
  AND sumDeposits*0.85 <= sumWithdrawals
  AND numDeposits >= numWithdrawals * 5
  AND numSources >= numTargets * 5		// concentrator likely has 5+ mules
  AND sumDeposits >= 100000
RETURN custB.accountName,
       acctB.accountNumber,
       numDeposits,
       sumDeposits,
       numSources,
	   numWithdrawals,
       sumWithdrawals,
       numTargets
ORDER BY sumWithdrawals DESC
