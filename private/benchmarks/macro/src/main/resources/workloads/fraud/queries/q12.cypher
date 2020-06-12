MATCH p1=(bc:BusinessCustomer:AccountHolder)-[:HAS_LINEOFCREDIT]->(loc1:LOC:CreditAccount)-[:BASED_ON]->(app1:CreditApplication)-[:STATED_INCOME]->(finstmt1:FinancialStatement),
      p2=(bc)-[:HAS_LINEOFCREDIT]->(loc2:LOC:CreditAccount)-[:BASED_ON]->(app2:CreditApplication)-[:STATED_INCOME]->(finstmt2:FinancialStatement)
WHERE id(loc1)<>id(loc2) AND id(finstmt1)<>id(finstmt2) AND
      ((finstmt1.lastYear=finstmt2.lastYear and finstmt1.incomeLastYear<>finstmt2.incomeLastYear) OR
       (finstmt1.lastYear=finstmt2.yearBefore and finstmt1.incomeLastYear<>finstmt2.incomeYearBefore))
RETURN bc.accountName,
       loc1.accountNumber,
       loc1.balance,
       loc1.creditLimit,
		   finstmt1.lastYear,
       finstmt1.incomeLastYear,
       finstmt1.yearBefore,
       finstmt1.incomeYearBefore,
       loc2.accountNumber,
       loc2.balance,
       loc2.creditLimit,
       finstmt2.lastYear,
       finstmt2.incomeLastYear,
       finstmt2.yearBefore,
       finstmt2.incomeYearBefore