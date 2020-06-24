WITH $date AS curDate
WITH curDate, curDate-duration({days:35}) AS curDate35
MATCH (da:DepositAccount)-[:WITHDRAWALS]->(xfer:Wire:Transaction)-[:RECEIVES_WIRE]->
	(ext:ExternalAccount)-[:IBAN_ROUTING]->(iban:IBAN_Code)<-[:HAS_IBAN_CODE]-
	(fi:FinancialInstitution)-[:OPERATES_IN]->(cty:Country)
WHERE xfer.transactionDate <= curDate
  AND xfer.transactionDate >= curDate35
  AND cty.fraudScore > 0.6
  AND left(iban.ibanNumber,2) = cty.countryCode

// collect the accounts and xfers
WITH curDate,
     curDate35,
	 collect({custAccount: da.accountID, xferID: xfer.transactionID, extAccount: ext.accountID}) AS possBadXferList

// add up all the deposits for those accounts
UNWIND possBadXferList AS curXfer
MATCH (da:DepositAccount)<-[:DEPOSITS]-(txn)
WHERE txn.transactionDate >= curDate35
  AND txn.transactionDate <= curDate
  AND da.accountID=curXfer.custAccount
WITH curXfer,
     da,
	 count(txn.amount) AS numDeposits,
	 sum(txn.amount) AS totalDeposits

// Filter on 'a lot of incoming' AS numDeposits and add in customer info
MATCH (cu)-[:HAS_ACCOUNT]->(da)-[:WITHDRAWALS]->(xfer:Wire:Transaction)-[:RECEIVES_WIRE]->
	(ext:ExternalAccount)-[:IBAN_ROUTING]->(iban:IBAN_Code)<-[:HAS_IBAN_CODE]-
	(fi:FinancialInstitution)-[:OPERATES_IN]->(cty:Country),
    (ext)<-[:HAS_ACCOUNT]-(extAcct:External:AccountHolder)
WHERE xfer.transactionID = curXfer.xferID
  AND ext.accountID = curXfer.extAccount
  AND cty.fraudScore > 0.6
  AND numDeposits > 4
  AND left(iban.ibanNumber,2) = cty.countryCode
RETURN cu.accountName AS custAcctName,
       da.accountType AS type,
	   da.accountNumber AS accountNumber,
	   numDeposits,
	   totalDeposits,
	   xfer.transactionID AS transferID,
	   abs(xfer.amount) AS xferAmount,
	   ext.accountNumber AS extAccount,
	   extAcct.accountName AS extAcctName,
       iban.ibanNumber AS IBAN,
       fi.bankName AS bankName,
       cty.countryCode AS country
ORDER BY totalDeposits DESC, extAcctName
