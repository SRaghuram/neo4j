MATCH (sender)-[r:MAKES_PAYMENTS_TO]->(receiver)
  WHERE r.numPayments = 300
    AND r.avgAmount >= 1000
RETURN sender.accountName AS senderName,
       receiver.accountName AS receiverName,
       r.avgAmount AS avgAmount
  ORDER BY r.avgAmount DESC
