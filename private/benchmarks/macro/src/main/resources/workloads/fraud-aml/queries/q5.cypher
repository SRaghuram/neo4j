WITH $date AS endDate
WITH endDate, endDate-duration({days:90}) AS startDate
MATCH p=(ctr:DepositAccount)-[:WITHDRAWALS]-(wt:Wire:Transaction)-[:RECEIVES_WIRE]-(ext:ExternalAccount)
	-[:IBAN_ROUTING]->(iban:IBAN_Code) 
	<-[:HAS_IBAN_CODE]-(fi:FinancialInstitution)-[:OPERATES_IN]->(cty:Country)
WHERE wt.transactionDate >= startDate
  AND wt.transactionDate <= endDate
  AND cty.fraudScore > 0.6

// Step 2 - find the accounts that had cash deposits that transferred to the account that did the wire transfer
WITH ctr, wt, ext, iban, fi, cty
MATCH g=(cu:AccountHolder)-[:HAS_ACCOUNT]->(src:DepositAccount)-[:WITHDRAWALS]->(srcTxn:ACH:Transaction)
	-[:INTERNAL_XFER]->(ctrTxn:ACH:Transaction)-[:DEPOSITS]->(ctr)<-[:HAS_ACCOUNT]-(cu2),
	(ctr)-[:WITHDRAWALS]-(wt)-[:RECEIVES_WIRE]-(ext)-[:IBAN_ROUTING]->(iban) 
	<-[:HAS_IBAN_CODE]-(fi)-[:OPERATES_IN]->(cty), 
	(extName:External:AccountHolder)-[:HAS_ACCOUNT]->(ext)
WHERE srcTxn.transactionDate <= wt.transactionDate
  AND duration.inDays(srcTxn.transactionDate, wt.transactionDate).days<30
RETURN cu.accountName,
       src.accountNumber,
       srcTxn.amount,
       srcTxn.transactionDate,
       ctr.accountNumber,
       cu2.accountName,
       wt.amount,
       wt.transactionDate,
       ext.accountNumber,
       extName.accountName,
       fi.bankName,
       cty.countryName
ORDER by ctr.accountNumber, src.accountNumber
