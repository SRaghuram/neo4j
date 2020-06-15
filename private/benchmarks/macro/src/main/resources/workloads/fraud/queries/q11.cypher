// TODO move to config json: "comment": "date parameter chosen uniformly random from dates in database, between WHERE tx.dateOpened > date({year:2019,month:6,day:1}) AND tx.dateOpened < date({year:2020,month:2,day:1})
MATCH (bc:BusinessCustomer:AccountHolder)-[:HAS_LINEOFCREDIT]->(loc:LOC:CreditAccount)-[:BASED_ON]->(app:CreditApplication),
      (app)-[:STATED_INCOME]->(finstmt:FinancialStatement),
      (bc)-[:HAS_ACCOUNT]->(dep:DepositAccount)<-[:DEPOSITS]-(txn:Transaction)
WHERE loc.dateOpened>$date AND date("2019-01-01") <= txn.transactionDate <= ($date - duration({days: 1}))
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