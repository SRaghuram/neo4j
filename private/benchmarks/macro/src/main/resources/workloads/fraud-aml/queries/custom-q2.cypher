MATCH (sender)-[r:MAKES_PAYMENTS_TO]->(receiver)
RETURN sender.accountName AS senderName,
       receiver.accountName AS receiverName,
       r.numPayments AS numPayments
  ORDER BY r.numPayments DESC
  LIMIT 10
