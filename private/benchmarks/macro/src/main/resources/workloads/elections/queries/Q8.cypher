MATCH (n:Individual)
WHERE n.TRANSACTION_AMT < 654
RETURN n.TRANSACTION_AMT
LIMIT 6