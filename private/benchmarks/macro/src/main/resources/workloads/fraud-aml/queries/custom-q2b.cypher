MATCH (sender)-[r:MAKES_PAYMENTS_TO]->(receiver)
  WHERE r.numPayments IS NOT NULL
RETURN sender.accountName AS senderName,
       receiver.accountName AS receiverName,
       r.numPayments AS numPayments
  ORDER BY r.numPayments DESC
  LIMIT 10
