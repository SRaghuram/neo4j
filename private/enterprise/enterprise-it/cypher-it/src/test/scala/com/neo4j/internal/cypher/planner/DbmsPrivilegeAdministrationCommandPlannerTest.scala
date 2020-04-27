/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.cypher.internal.plandescription.Arguments.Details

class DbmsPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  dbmsCommands.foreach { action =>
    test(s"GRANT $action") {
      // When
      val plan = execute(s"EXPLAIN GRANT $action ON DBMS TO reader, $$role", Map("role" -> "editor")).executionPlanString()

      // Then
      plan should include(
        logPlan(
          dbmsPrivilegePlan("GrantDbmsAction", action, Details("ROLE $role"),
            dbmsPrivilegePlan("GrantDbmsAction", action, "reader",
              assertDbmsAdminPlan("ASSIGN PRIVILEGE")
            )
          )
        ).toString
      )
    }

    test(s"DENY $action") {
      // When
      val plan = execute(s"EXPLAIN DENY $action ON DBMS TO reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          dbmsPrivilegePlan("DenyDbmsAction", action, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"REVOKE $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE $action ON DBMS FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", action, "reader",
            dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", action, "reader",
              assertDbmsAdminPlan("REMOVE PRIVILEGE")
            )
          )
        ).toString
      )
    }

    test(s"REVOKE GRANT $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE GRANT $action ON DBMS FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", action, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"REVOKE DENY $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE DENY $action ON DBMS FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", action, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        ).toString
      )
    }
  }
}
