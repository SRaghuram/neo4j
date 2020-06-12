MATCH (ah:AccountHolder)-[:REQUESTS_INCREASE]->(req:CreditLimitRequest)-[:ADJUSTS_CREDIT]->(acct:Delinquent:CreditAccount)-[:LAST_PAYMENT]->(pay:Payment)
WHERE duration.inMonths(req.requestDate,pay.paymentDate).months<=6
RETURN ah.accountName, labels(acct), acct.balance, pay.paymentDate
ORDER BY acct.balance DESC