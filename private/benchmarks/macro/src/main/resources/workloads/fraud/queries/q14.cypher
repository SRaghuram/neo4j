// first get all the stated annual incomes for LOC's from this year
// if they applied for more than one LOC, use the max() (which should be the same)
MATCH (ah:BusinessCustomer:AccountHolder)-[:HAS_LINEOFCREDIT]->(loc:LOC:CreditAccount),
      (loc)-[:BASED_ON]->(app:CreditApplication)-[:STATED_INCOME]-(finstmt:FinancialStatement)
WHERE finstmt.lastYear=2019
WITH ah, max(finstmt.incomeLastYear) AS yearlyIncome

// Then get the sum possible cohort payments where a payment is 5% of annual income
MATCH p=(ah)-[mp:MAKES_PAYMENTS_TO]->(cohort:AccountHolder)
WHERE mp.sumAmount>yearlyIncome*0.05

// get a list of accounts where the possible cohort payments are 10% the annual income
WITH ah,
     yearlyIncome,
     sum(mp.sumAmount) AS sumPayments,
     count(mp) AS numPayments,
		 collect(cohort.accountHolderID) AS cohortList
WHERE yearlyIncome*0.10 <= sumPayments

// finally see if those accounts are involved in circular payments using the possible cohorts as starting points
MATCH p=(ah)-[mp:MAKES_PAYMENTS_TO]->(cohort:AccountHolder)-[:MAKES_PAYMENTS_TO*1..3]->(ah)-[:HAS_LINEOFCREDIT]->(loc:LOC:CreditAccount)
WHERE cohort.accountHolderID IN cohortList
RETURN p