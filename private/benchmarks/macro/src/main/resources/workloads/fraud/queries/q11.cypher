MATCH (bc:BusinessCustomer:AccountHolder)-[:HAS_LINEOFCREDIT]->(loc:LOC:CreditAccount)-[:BASED_ON]->(app:CreditApplication),
      (app)-[:STATED_INCOME]->(finstmt:FinancialStatement),
      (bc)-[:HAS_ACCOUNT]->(dep:DepositAccount)<-[:DEPOSITS]-(txn:Transaction)
WHERE loc.dateOpened>date("2020-01-01") AND date("2019-01-01") <= txn.transactionDate <= date("2019-12-31")
WITH bc, loc, finstmt, toInteger(sum(txn.amount)) as TotalDeposits
WHERE finstmt.incomeLastYear > TotalDeposits*1.3 AND finstmt.lastYear=2019
RETURN bc.accountHolderID,
       bc.accountName,
       loc.accountNumber,
       loc.balance,
       loc.creditLimit,
       finstmt.lastYear,
       finstmt.incomeLastYear,
       TotalDeposits