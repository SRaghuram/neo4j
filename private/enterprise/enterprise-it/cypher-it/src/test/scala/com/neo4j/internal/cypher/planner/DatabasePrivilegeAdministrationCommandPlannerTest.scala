/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.asPrettyString

class DatabasePrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  (basicDatabaseCommands ++ schemaCommands).foreach { action =>
    test(s"GRANT $action") {
      // When
      val plan = execute(s"EXPLAIN GRANT $action ON DATABASE * TO reader, editor").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("GrantDatabaseAction", action, allDatabases = true, "editor",
            databasePrivilegePlan("GrantDatabaseAction", action, allDatabases = true, "reader",
              assertDbmsAdminPlan("ASSIGN PRIVILEGE")
            )
          )
        ).toString
      )
    }

    test(s"DENY $action") {
      // When
      val plan = execute(s"EXPLAIN DENY $action ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("DenyDatabaseAction", action, databasePrivilegeArg(SYSTEM_DATABASE_NAME), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"DENY $action with parameter") {
    // When
    val plan = execute(s"EXPLAIN DENY $action ON DATABASE $$db TO $$role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", action, Details(asPrettyString.raw("DATABASE $db")), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

    test(s"REVOKE $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE $action ON DATABASE neo4j FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("RevokeDatabaseAction(DENIED)", action, databasePrivilegeArg("neo4j"), "reader",
            databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", action, databasePrivilegeArg("neo4j"), "reader",
              assertDbmsAdminPlan("REMOVE PRIVILEGE")
            )
          )
        ).toString
      )
    }

    test(s"REVOKE GRANT $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE GRANT $action ON DEFAULT DATABASE FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", action, allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"REVOKE DENY $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE DENY $action ON DATABASES foo, bar FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("RevokeDatabaseAction(DENIED)", action, databasePrivilegeArg("bar"), "reader",
            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", action, databasePrivilegeArg("foo"), "reader",
              assertDbmsAdminPlan("REMOVE PRIVILEGE")
            )
          )
        ).toString
      )
    }
  }

  // Transaction privileges

  transactionCommands.foreach { action =>
    test(s"GRANT $action") {
      // When
      val plan = execute(s"EXPLAIN GRANT $action (*) ON DATABASE * TO reader, editor").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("GrantDatabaseAction", action, allDatabases = true, Details(asPrettyString.raw("ALL USERS")), "editor",
            databasePrivilegePlan("GrantDatabaseAction", action, allDatabases = true, Details(asPrettyString.raw("ALL USERS")), "reader",
              assertDbmsAdminPlan("ASSIGN PRIVILEGE")
            )
          )
        ).toString
      )
    }

    test(s"DENY $action") {
      // When
      val plan = execute(s"EXPLAIN DENY $action (*) ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("DenyDatabaseAction", action, databasePrivilegeArg(SYSTEM_DATABASE_NAME), Details(asPrettyString.raw("ALL USERS")), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"DENY $action with parameter") {
      // When
      val plan = execute(s"EXPLAIN DENY $action (*) ON DATABASE $$db TO $$role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("DenyDatabaseAction", action, Details(asPrettyString.raw("DATABASE $db")), Details(asPrettyString.raw("ALL USERS")), "$role",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"REVOKE $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE $action (*) ON DEFAULT DATABASE FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("RevokeDatabaseAction(DENIED)", action, allDatabases = false, Details(asPrettyString.raw("ALL USERS")), "reader",
            databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", action, allDatabases = false, Details(asPrettyString.raw("ALL USERS")), "reader",
              assertDbmsAdminPlan("REMOVE PRIVILEGE")
            )
          )
        ).toString
      )
    }

    test(s"REVOKE GRANT $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE GRANT $action (username) ON DEFAULT DATABASE FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", action, allDatabases = false, qualifierArg("USER", "username"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        ).toString
      )
    }

    test(s"REVOKE DENY $action") {
      // When
      val plan = execute(s"EXPLAIN REVOKE DENY $action (user1, $$user2) ON DEFAULT DATABASE FROM reader").executionPlanString()

      // Then
      plan should include(
        logPlan(
          databasePrivilegePlan("RevokeDatabaseAction(DENIED)", action, allDatabases = false, Details(asPrettyString.raw("USER $user2")), "reader",
            databasePrivilegePlan("RevokeDatabaseAction(DENIED)", action, allDatabases = false, qualifierArg("USER", "user1"), "reader",
              assertDbmsAdminPlan("REMOVE PRIVILEGE")
            )
          )
        ).toString
      )
    }
  }
}
