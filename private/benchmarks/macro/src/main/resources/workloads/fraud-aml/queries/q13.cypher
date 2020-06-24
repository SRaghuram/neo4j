WITH $date AS curDate
MATCH (cd:CalendarDate)-[:IS_DATE_IN]->(cw:CalendarWeek)-[:PREV_WEEK*1..4]->(prev:CalendarWeek)
WHERE cd.date = curDate
WITH curDate, cw, prev

// get a list of customers and their avg deposits and total deposits for previous weeks
MATCH (bc:BusinessCustomer)-[:HAS_ACCOUNT]->(acct:DepositAccount)<-[:DEPOSITS]-(cash:Cash:Transaction)
	-[:TXN_DATE]->(tday:CalendarDate)-[IS_DATE_IN]->(prev)
WITH bc,
     acct,
     cw,
     prev,
     count(cash) AS numDeposits,
     sum(cash.amount) AS weeklyDeposits
WITH bc,
     acct,
     cw,
     avg(numDeposits) AS avgNumDeposits,
     avg(weeklyDeposits) AS avgWeeklyDeposits

// get a list of customers and their avg deposits and total deposits for current week where significantly higher
MATCH (bc)-[:HAS_ACCOUNT]->(acct)<-[:DEPOSITS]-(cash:Cash:Transaction)
	-[:TXN_DATE]->(tday:CalendarDate)-[IS_DATE_IN]->(cw)
WITH bc,
     acct,
     cw,
     avgNumDeposits,
     avgWeeklyDeposits,
     count(cash) AS curNumDeposits,
     sum(cash.amount) AS curWeeklyDeposits
WHERE curWeeklyDeposits > avgWeeklyDeposits*5
   OR curNumDeposits > avgNumDeposits*5
WITH bc,
     acct,
     cw,
     avgNumDeposits,
     avgWeeklyDeposits,
     curNumDeposits,
     curWeeklyDeposits

// show the results with weekly aggregation
MATCH (cw)-[:PREV_WEEK*0..4]->(lookback:CalendarWeek)
MATCH (bc)-[:HAS_ACCOUNT]->(acct)<-[:DEPOSITS]-(cash:Cash:Transaction)
	-[:TXN_DATE]->(tday:CalendarDate)-[IS_DATE_IN]->(lookback)
WITH bc.accountName AS accountName,
     acct.accountNumber AS accountNumber,
     lookback.week AS weekNum,
	 count(cash) AS numDeposits,
	 round(sum(cash.amount)*100)/100.0 AS weeklyDeposits
RETURN accountName, accountNumber, weekNum, numDeposits, weeklyDeposits
