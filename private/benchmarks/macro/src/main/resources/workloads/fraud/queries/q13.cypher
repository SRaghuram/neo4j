MATCH p1=(ah:AccountHolder)-[:HAS_MORTGAGE|HAS_UNSECUREDLOAN|HAS_AUTOLOAN|HAS_CREDITCARD]->(cred1:CreditAccount)-[:BASED_ON]->(app1:CreditApplication)-[:STATED_INCOME]->(finstmt1:FinancialStatement),
      p2=(ah)-[:HAS_MORTGAGE|HAS_UNSECUREDLOAN|HAS_AUTOLOAN|HAS_CREDITCARD]->(cred2:CreditAccount)-[:BASED_ON]->(app2:CreditApplication)-[:STATED_INCOME]->(finstmt2:FinancialStatement)
WHERE id(cred1)<>id(cred2) AND id(finstmt1)<>id(finstmt2) AND
      ((finstmt1.lastYear=finstmt2.lastYear and finstmt1.incomeLastYear<>finstmt2.incomeLastYear) OR
       (finstmt1.lastYear=finstmt2.yearBefore and finstmt1.incomeLastYear<>finstmt2.incomeYearBefore))
RETURN ah.accountHolderID,
       ah.accountName,
		   cred1.accountNumber,
       cred1.balance,
       cred1.loanAmount,
       cred1.dateOpened,
		   finstmt1.lastYear,
       finstmt1.incomeLastYear,
       finstmt1.yearBefore,
       finstmt1.incomeYearBefore,
        cred2.accountNumber,
       cred2.balance,
       cred2.loanAmount,
       cred2.dateOpened,
        finstmt2.lastYear,
       finstmt2.incomeLastYear,
       finstmt2.yearBefore,
       finstmt2.incomeYearBefore
ORDER BY cred1.balance DESC, cred2.balance DESC
