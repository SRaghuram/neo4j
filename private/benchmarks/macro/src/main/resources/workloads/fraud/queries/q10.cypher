// e.g., $startDate = date("2019-11-15")
MATCH (loc:LOC:CreditAccount)-[:LOC_WITHDRAWAL]-(credTxn:CreditTransaction)-[:CREDIT_XFER]->(depTxnIn:Transaction)-[:DEPOSITS]->(depAcct:DepositAccount)
WHERE $startDate<=credTxn.transactionDate<=($startDate + duration({days: 45}))/*1.5 months*/
WITH loc,
     depAcct,
     sum(abs(credTxn.amount)) AS sumXfered,
     count(credTxn) AS numXfers,
     min(credTxn.transactionDate) AS firstDraw,
     max(credTxn.transactionDate) AS lastDraw
WHERE sumXfered>=loc.loanAmount*0.5

// ...and who then wired most of it overseas...
MATCH (depAcct)-[:WITHDRAWALS]->(txn:Wire:Transaction)-[:RECEIVES_WIRE]->(extAcct:ExternalAccount),
      (extAcct)-[:IBAN_ROUTING]->(iban:IBAN_Code)<-[:HAS_IBAN_CODE]-(fi:FinancialInstitution)-[:OPERATES_IN]->(cntry:Country)
WHERE NOT cntry.countryCode="US" AND
      left(iban.ibanNumber,2)=cntry.countryCode AND
      $startDate<=txn.transactionDate<=($startDate + duration({days: 60}))/*2 months*/
WITH loc, depAcct, sumXfered, numXfers, firstDraw, lastDraw, abs(sum(txn.amount)) AS sumWires, count(txn) AS numWires
WHERE sumWires >= sumXfered * 0.5

// now, for those accounts, report where the money went
MATCH (ourParty:AccountHolder)-[:HAS_ACCOUNT]->(depAcct)-[:WITHDRAWALS]->(txnOut:Wire:Transaction),
	    (txnOut)-[:RECEIVES_WIRE]->(extAcct:ExternalAccount)-[:IBAN_ROUTING]->(iban:IBAN_Code),
      (iban)<-[:HAS_IBAN_CODE]-(fi:FinancialInstitution)-[:OPERATES_IN]->(cntry:Country)
WHERE $startDate<=txnOut.transactionDate<=($startDate + duration({days: 60}))/*2 months*/ AND
      NOT cntry.countryCode="US" AND
      // ignore vetted companies unless wiring to a country with high score
      ((depAcct.FinCEN_110_status is NULL) OR
		  (depAcct.FinCEN_110_status="ineligible") OR
      (cntry.fraudScore>0.3)) AND
      left(iban.ibanNumber,2)=cntry.countryCode
WITH ourParty,
     loc,
     depAcct,
     sumXfered,
     numXfers,
     firstDraw,
     lastDraw,
     extAcct,
     cntry.countryName AS countryName,
     sum(abs(txnOut.amount)) AS sumWired,
     count(txnOut) AS numWired,
     max(abs(txnOut.amount)) AS maxWired,
     percentileDisc(abs(txnOut.amount),0.90) AS mostWired,
     min(txnOut.transactionDate) AS firstWire,
     max(txnOut.transactionDate) AS lastWire

// finally, get the names and if on watch list
MATCH (extParty:External:AccountHolder)-[:HAS_ACCOUNT]->(extAcct)
OPTIONAL MATCH (extParty)-[:IS_LISTED_ON]->(wl:WatchList)
RETURN ourParty.accountName AS ourParty,
       loc.accountNumber,
       loc.loanAmount,
       loc.balance,
       round((loc.balance/loc.loanAmount)*1000)/10.0 AS LOCpct,
       sumXfered,
       numXfers,
       firstDraw,
       lastDraw,
       depAcct.accountNumber AS ourAccount,
       extAcct.accountNumber AS externalAcct,
       countryName,
       sumWired,
       numWired,
       maxWired,
       mostWired,
       firstWire,
       lastWire,
       extParty.accountName AS externalParty,
       wl.basis,
       wl.rationale
ORDER BY sumWired DESC, countryName, extParty.accountName, extAcct.accountNumber
