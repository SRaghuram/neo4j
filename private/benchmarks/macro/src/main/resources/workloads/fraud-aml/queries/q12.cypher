MATCH (ext:ExternalAccount)-[:SENDS_WIRE]->(wire:Wire:Transaction)-[:DEPOSITS]->(acct:DepositAccount)
	-[:WITHDRAWALS]->(cash:Cash:Transaction)-[:OCCURS_AT]->(ATMLocation)
	,(cu:Customer:AccountHolder)-[:HAS_ACCOUNT]->(acct)
WHERE cash.transactionDate >= wire.transactionDate
  AND duration.inDays(wire.transactionDate,cash.transactionDate).days <= 30
WITH cu.accountName AS accountName,
     acct.accountNumber AS localAccount,
	 ext.accountNumber AS wireAccount,
	 wire.transactionID AS wireTransaction,
	 wire.transactionDate AS transactionDate,
	 abs(wire.amount) AS wireAmount,
	 sum(abs(cash.amount)) AS sumCashWithdrawals
WHERE sumCashWithdrawals > 10000 OR wireAmount < sumCashWithdrawals*0.5
RETURN accountName,
       localAccount,
       wireAccount,
       wireTransaction,
       transactionDate,
       wireAmount,
       sumCashWithdrawals
