MATCH path1=(ah:Customer:AccountHolder)-[:APPLICANT]->(app1:CreditApplication)-[:STATED_OCCUPATION]->(hist1:EmploymentHistory)-[:EMPLOYED_BY]->(emp1:Employer),
      path2=(ah)-[:APPLICANT]->(app2:CreditApplication)-[:STATED_OCCUPATION]->(hist2:EmploymentHistory)-[:EMPLOYED_BY]->(emp2:Employer)
WHERE id(app1)<>id(app2) AND
      app2.appliedDate>app1.appliedDate AND
      // threshold for disparate is 50% increase within 18 months
      hist1.salary*1.5<hist2.salary AND
      duration.inMonths(app1.appliedDate, app2.appliedDate).months < 18
RETURN ah.accountHolderID,
       ah.accountName,
       app1.accountID,
       app1.appliedDate,
       hist1.jobTitle,
       hist1.salary,
       emp1.employerName,
       app2.accountID,
       app2.appliedDate,
       hist2.jobTitle,
       hist2.salary,
       emp2.employerName,
       round((toFloat(hist2.salary)/hist1.salary)*100) AS pctChange,
       duration.inMonths(app1.appliedDate, app2.appliedDate).months AS numMonths