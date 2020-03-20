/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Database
import org.neo4j.cypher.internal.plandescription.Arguments.Qualifier

class DatabasePrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Access/Start/Stop

  test("Grant access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ACCESS ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "ACCESS", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "ACCESS", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY ACCESS ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "ACCESS", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny access with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ACCESS ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "ACCESS", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE ACCESS ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ACCESS", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ACCESS", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke grant access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT ACCESS ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ACCESS", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny access") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY ACCESS ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ACCESS", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT START ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "START", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "START", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY START ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "START", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny start with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY START ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "START", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke start") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE START ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "START", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "START", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT STOP ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "STOP", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "STOP", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY STOP ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "STOP", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny stop with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY STOP ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "STOP", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke stop") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE STOP ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "STOP", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "STOP", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // Schema privileges

  test("Grant create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE INDEX ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE INDEX", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE INDEX ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE INDEX", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny create index with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE INDEX ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE INDEX", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke grant create index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE INDEX ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE INDEX", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP INDEX ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "DROP INDEX", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY DROP INDEX ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP INDEX", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny drop index with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP INDEX ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP INDEX", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny drop index") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY DROP INDEX ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP INDEX", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT INDEX MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "INDEX MANAGEMENT", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "INDEX MANAGEMENT", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY INDEX MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "INDEX MANAGEMENT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny index management with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY INDEX MANAGEMENT ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "INDEX MANAGEMENT", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke index management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE INDEX MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "INDEX MANAGEMENT", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "INDEX MANAGEMENT", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE CONSTRAINT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE CONSTRAINT", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny create constraint with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE CONSTRAINT ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE CONSTRAINT", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke grant create constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE CONSTRAINT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE CONSTRAINT", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP CONSTRAINT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "DROP CONSTRAINT", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY DROP CONSTRAINT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP CONSTRAINT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny drop constraint with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP CONSTRAINT ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "DROP CONSTRAINT", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny drop constraint") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY DROP CONSTRAINT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "DROP CONSTRAINT", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
          databasePrivilegePlan("GrantDatabaseAction", "CONSTRAINT MANAGEMENT", allDatabases = true, "editor",
            databasePrivilegePlan("GrantDatabaseAction", "CONSTRAINT MANAGEMENT", allDatabases = true, "reader",
                assertDbmsAdminPlan("ASSIGN PRIVILEGE")
              )
        )
      ).toString
    )
  }

  test("Deny constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CONSTRAINT MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CONSTRAINT MANAGEMENT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny constraint management with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CONSTRAINT MANAGEMENT ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CONSTRAINT MANAGEMENT", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke constraint management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE CONSTRAINT MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CONSTRAINT MANAGEMENT", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CONSTRAINT MANAGEMENT", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // Token privileges

  test("Grant create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW LABEL ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW NODE LABEL", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW LABEL ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW NODE LABEL", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny create label with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE NEW LABEL ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW NODE LABEL", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke grant create label") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT CREATE NEW LABEL ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW NODE LABEL", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW TYPE ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW TYPE ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny create type with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE NEW TYPE ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW RELATIONSHIP TYPE", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny create type") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE DENY CREATE NEW TYPE ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW RELATIONSHIP TYPE", allDatabases = false, "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Grant create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE NEW NAME ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "CREATE NEW PROPERTY NAME", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY CREATE NEW NAME ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW PROPERTY NAME", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny create property name with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE NEW NAME ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "CREATE NEW PROPERTY NAME", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create property name") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE CREATE NEW NAME ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "CREATE NEW PROPERTY NAME", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "CREATE NEW PROPERTY NAME", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT NAME MANAGEMENT ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "NAME MANAGEMENT", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "NAME MANAGEMENT", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY NAME MANAGEMENT ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "NAME MANAGEMENT", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny name management with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY NAME MANAGEMENT ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "NAME MANAGEMENT", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke name management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE NAME MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "NAME MANAGEMENT", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "NAME MANAGEMENT", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // All database privilege

  test("Grant all database privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ALL ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "ALL DATABASE PRIVILEGES", allDatabases = true, "editor",
          databasePrivilegePlan("GrantDatabaseAction", "ALL DATABASE PRIVILEGES", allDatabases = true, "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny all database privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY ALL ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "ALL DATABASE PRIVILEGES", SYSTEM_DATABASE_NAME, "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny all database privileges with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ALL ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "ALL DATABASE PRIVILEGES", Database("DATABASE $db"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke all database privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE ALL ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "ALL DATABASE PRIVILEGES", allDatabases = false, "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "ALL DATABASE PRIVILEGES", allDatabases = false, "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // Transaction privileges

  test("Grant show transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW TRANSACTION (*) ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "SHOW TRANSACTION", allDatabases = true, Qualifier("ALL USERS"), "editor",
          databasePrivilegePlan("GrantDatabaseAction", "SHOW TRANSACTION", allDatabases = true, Qualifier("ALL USERS"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny show transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY SHOW TRANSACTION (*) ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "SHOW TRANSACTION", SYSTEM_DATABASE_NAME, Qualifier("ALL USERS"), "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny show transaction with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW TRANSACTION (*) ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "SHOW TRANSACTION", Database("DATABASE $db"), Qualifier("ALL USERS"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke show transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE SHOW TRANSACTION (*) ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "SHOW TRANSACTION", allDatabases = false, Qualifier("ALL USERS"), "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "SHOW TRANSACTION", allDatabases = false, Qualifier("ALL USERS"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke grant show transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT SHOW TRANSACTION (username) ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "SHOW TRANSACTION", allDatabases = false, qualifierArg("USER", "username"), "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny show transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DENY SHOW TRANSACTION (user1, $user2) ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "SHOW TRANSACTION", allDatabases = false, Qualifier("USER $user2"), "reader",
          databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "SHOW TRANSACTION", allDatabases = false, qualifierArg("USER", "user1"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant terminate transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT TERMINATE TRANSACTION (*) ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "TERMINATE TRANSACTION", allDatabases = true, Qualifier("ALL USERS"), "editor",
          databasePrivilegePlan("GrantDatabaseAction", "TERMINATE TRANSACTION", allDatabases = true, Qualifier("ALL USERS"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny terminate transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY TERMINATE TRANSACTION (*) ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "TERMINATE TRANSACTION", SYSTEM_DATABASE_NAME, Qualifier("ALL USERS"), "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Deny terminate transaction with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY TERMINATE TRANSACTION (*) ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "TERMINATE TRANSACTION", Database("DATABASE $db"), Qualifier("ALL USERS"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke terminate transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE TERMINATE TRANSACTION (*) ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "TERMINATE TRANSACTION", allDatabases = false, Qualifier("ALL USERS"), "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "TERMINATE TRANSACTION", allDatabases = false, Qualifier("ALL USERS"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Revoke grant terminate transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE GRANT TERMINATE TRANSACTION (username) ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "TERMINATE TRANSACTION", allDatabases = false, qualifierArg("USER", "username"), "reader",
          assertDbmsAdminPlan("REMOVE PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke deny terminate transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DENY TERMINATE TRANSACTION ($user1, user2) ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "TERMINATE TRANSACTION", allDatabases = false, qualifierArg("USER", "user2"), "reader",
          databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "TERMINATE TRANSACTION", allDatabases = false, Qualifier("USER $user1"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant transaction management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT TRANSACTION MANAGEMENT (*) ON DATABASE * TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("GrantDatabaseAction", "TRANSACTION MANAGEMENT", allDatabases = true, Qualifier("ALL USERS"), "editor",
          databasePrivilegePlan("GrantDatabaseAction", "TRANSACTION MANAGEMENT", allDatabases = true, Qualifier("ALL USERS"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny transaction management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN DENY TRANSACTION (user1,user2) ON DATABASE $SYSTEM_DATABASE_NAME TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "TRANSACTION MANAGEMENT", SYSTEM_DATABASE_NAME, qualifierArg("USER", "user2"), "reader",
          databasePrivilegePlan("DenyDatabaseAction", "TRANSACTION MANAGEMENT", SYSTEM_DATABASE_NAME, qualifierArg("USER", "user1"), "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny transaction management with parameter") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY TRANSACTION ($user) ON DATABASE $db TO $role", Map("db" -> SYSTEM_DATABASE_NAME, "role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("DenyDatabaseAction", "TRANSACTION MANAGEMENT", Database("DATABASE $db"), Qualifier("USER $user"), "$role",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke transaction management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN REVOKE TRANSACTION MANAGEMENT ON DEFAULT DATABASE FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        databasePrivilegePlan("RevokeDatabaseAction(DENIED)", "TRANSACTION MANAGEMENT", allDatabases = false, Qualifier("ALL USERS"), "reader",
          databasePrivilegePlan("RevokeDatabaseAction(GRANTED)", "TRANSACTION MANAGEMENT", allDatabases = false, Qualifier("ALL USERS"), "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }
}
