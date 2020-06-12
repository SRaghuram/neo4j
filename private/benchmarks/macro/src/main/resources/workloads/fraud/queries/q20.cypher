// TODO 3 days takes about 8s, parametrize using these
// step 1 get a list of all disputed transactions that happened within a period
WITH date("2019-12-31") AS monthEnd, date("2019-12-29") AS monthStart
MATCH (acct:Revolving:CreditAccount)-[:ACCT_DISPUTES]->(dispute:Dispute)-[:DISPUTED_TXN]->(txn:Card:CreditTransaction)
WHERE monthStart <= txn.transactionDate <= monthEnd

// step 2 get a list distinct merchants where the card was used for some prior lookback period
WITH acct, dispute, txn.transactionDate AS endDate, txn.transactionDate-duration({days: 90}) AS startDate
MATCH (acct)-[:ACCOUNT_TXNS]->(txn:Card:CreditTransaction)-[:TXN_MERCHANT]->(merch:Merchant)
WHERE startDate <= txn.transactionDate <= endDate AND NOT (txn)<-[:DISPUTED_TXN]-(:Dispute)
WITH DISTINCT acct, dispute, merch AS possMerch

// step 3 see if the merchants were common to other cardholders that filed disputes
MATCH (othDispTxn:Card:CreditTransaction)<-[:DISPUTED_TXN]-(othDisputes:Dispute)<-[:ACCT_DISPUTES]-(othAcct:Revolving:CreditAccount),
      (othAcct)-[:ACCOUNT_TXNS]->(othTxn:Card:CreditTransaction)-[:TXN_MERCHANT]->(possMerch)
WHERE id(othAcct)<>id(acct) AND id(othDisputes)<>id(dispute) AND
      othTxn.transactionDate <= othDispTxn.transactionDate AND
      othTxn.transactionDate >= othDispTxn.transactionDate-duration({days: 90}) AND
      NOT (othTxn)<-[:DISPUTED_TXN]-(:Dispute)
WITH acct.accountID AS currAccountID,
     dispute.disputedTxn AS disputedTxn,
     othAcct.accountID AS otherAcct,
     othDisputes.disputedTxn AS otherDisputes,
     collect(distinct possMerch.merchantID) AS commonMerchants

// step 4 for merchants in common provide merchant details for top 25
UNWIND commonMerchants AS curMerchant
WITH curMerchant, count(*) AS numMentions
ORDER BY numMentions DESC LIMIT 25
MATCH (merch:Merchant)
WHERE merch.merchantID=curMerchant
RETURN merch.merchantID, merch.merchantName, numMentions
ORDER BY numMentions DESC