WITH $date AS curDate, $date-duration({days:45}) AS lookbackDate
MATCH (ourAcct:DepositAccount)-[:WITHDRAWALS]->(txn:Wire:Transaction)-[:RECEIVES_WIRE]->
	  (extAcct:ExternalAccount)-[:IBAN_ROUTING]->(iban:IBAN_Code)<-[:HAS_IBAN_CODE]-
      (fi:FinancialInstitution)-[:OPERATES_IN]->(cntry:Country)
WHERE lookbackDate <= txn.transactionDate <= curDate
  AND NOT cntry.countryCode = "US"
  // ignore vetted companies unless wiring to a country with high score
  AND ((ourAcct.FinCEN_110_status IS NULL)
  		OR (ourAcct.FinCEN_110_status = "ineligible")
        OR (cntry.fraudScore > 0.3))
  AND left(iban.ibanNumber,2) = cntry.countryCode
WITH curDate,
     lookbackDate,
     ourAcct,
     abs(sum(txn.amount)) AS sumWires,
     count(txn) AS numWires,
     iban.ibanNumber AS iban,
     cntry.countryCode AS code

// step 2 - find all accounts with large deposits
MATCH (ourAcct)<-[:DEPOSITS]-(txnIn:Transaction)
WHERE lookbackDate <= txnIn.transactionDate <= curDate
WITH curDate,
     lookbackDate,
     ourAcct,
     sumWires,
     numWires,
     sum(txnIn.amount) AS sumDeposits,
     count(txnIn) AS numDeposits,
	 max(txnIn.amount) AS maxDeposit,
	 percentileDisc(txnIn.amount,0.90) AS mostDeposits
WHERE sumDeposits > 9500
  AND (sumWires/sumDeposits > 0.65 OR sumWires > 100000)

// now, for those accounts, report where the money went
MATCH (ourParty)-[:HAS_ACCOUNT]->(ourAcct)-[:WITHDRAWALS]->(txnOut:Wire:Transaction)-[:RECEIVES_WIRE]->
	(extAcct:ExternalAccount)-[:IBAN_ROUTING]->(iban:IBAN_Code)<-[:HAS_IBAN_CODE]-
    (fi:FinancialInstitution)-[:OPERATES_IN]->(cntry:Country)
WHERE lookbackDate <= txnOut.transactionDate <= curDate
  AND NOT cntry.countryCode = "US"
  // ignore vetted companies unless wiring to a country with high score
  AND ((ourAcct.FinCEN_110_status IS NULL)
  		OR (ourAcct.FinCEN_110_status = "ineligible")
        OR (cntry.fraudScore > 0.3))
  AND left(iban.ibanNumber,2) = cntry.countryCode
WITH ourParty,
     ourAcct,
     sumDeposits,
     numDeposits,
     maxDeposit,
     mostDeposits,
     extAcct,
     cntry.countryName AS countryName,
	 sum(abs(txnOut.amount)) AS sumWired,
	 count(txnOut) AS numWired,
	 max(abs(txnOut.amount)) AS maxWired,
	 percentileDisc(abs(txnOut.amount),0.90) AS mostWired

// finally, get the names and if on watch list
MATCH (extParty:External:AccountHolder)-[:HAS_ACCOUNT]->(extAcct)
OPTIONAL MATCH (extParty)-[:IS_LISTED_ON]->(wl:WatchList)
RETURN ourParty.accountName AS ourParty,
       ourAcct.accountNumber AS ourAccount,
       round(sumDeposits*100)/100.0 AS sumDeposits,
       numDeposits,
       maxDeposit,
       mostDeposits,
       extAcct.accountNumber AS externalAcct,
       countryName,
       sumWired,
       numWired,
       maxWired,
       mostWired,
       extParty.accountName AS externalParty,
       (CASE WHEN wl.watchEntityName IS NOT NULL THEN "** WATCH LIST **" ELSE NULL END) AS watchList
ORDER BY sumWired DESC, countryName, extParty.accountName, extAcct.accountNumber
