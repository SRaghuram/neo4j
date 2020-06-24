MATCH (da1:DepositAccount)<-[:HAS_ACCOUNT]-(cu1:Customer:AccountHolder)
	-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|CURRENT_RESIDENCE|USES_DEVICE]->(info)
	<-[:HAS_EMAIL|HAS_PHONE|IDENTIFIED_BY|CURRENT_RESIDENCE|USES_DEVICE]-
	(cu2:Customer:AccountHolder)-[:HAS_ACCOUNT]-(da2:DepositAccount)
WHERE id(cu1)<>id(cu2)
AND duration.inDays(da1.dateOpened,da2.dateOpened).days < 90
AND da1.dateOpened <= da2.dateOpened
WITH cu1.accountName AS Customer1,
     da1.accountNumber AS Account1,
     da1.dateOpened AS Opened1,
     duration.inDays(da1.dateOpened,da2.dateOpened).days AS DaysBetween,
     cu2.accountName AS Customer2,
     da2.accountNumber AS Account2,
     da2.dateOpened AS Opened2,
     collect(labels(info)[0]) AS SharedInfo
RETURN Customer1, Account1, Opened1, DaysBetween, Customer2, Account2, Opened2, SharedInfo
ORDER by Customer1, Customer2
