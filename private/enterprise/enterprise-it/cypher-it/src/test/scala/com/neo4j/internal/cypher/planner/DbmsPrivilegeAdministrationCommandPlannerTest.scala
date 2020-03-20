/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.plandescription.Arguments.Role

class DbmsPrivilegeAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Role management privileges

  test("Grant show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW ROLE ON DBMS TO reader, $role", Map("role" -> "editor")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "SHOW ROLE", Role("ROLE $role"),
          dbmsPrivilegePlan("GrantDbmsAction", "SHOW ROLE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "SHOW ROLE", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke show role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE SHOW ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW ROLE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW ROLE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "CREATE ROLE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "CREATE ROLE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE ROLE ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "CREATE ROLE", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE CREATE ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE ROLE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE ROLE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "DROP ROLE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "DROP ROLE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "DROP ROLE", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DROP ROLE ON DBMS FROM $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "DROP ROLE", Role("ROLE $role"),
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP ROLE", Role("ROLE $role"),
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ASSIGN ROLE ON DBMS TO $role, editor", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "ASSIGN ROLE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "ASSIGN ROLE", Role("ROLE $role"),
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ASSIGN ROLE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "ASSIGN ROLE", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke assign role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ASSIGN ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "ASSIGN ROLE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "ASSIGN ROLE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT REMOVE ROLE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "REMOVE ROLE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "REMOVE ROLE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY REMOVE ROLE ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "REMOVE ROLE", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke remove role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE REMOVE ROLE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "REMOVE ROLE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "REMOVE ROLE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ROLE MANAGEMENT ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "ROLE MANAGEMENT", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "ROLE MANAGEMENT", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ROLE MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "ROLE MANAGEMENT", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke role management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ROLE MANAGEMENT ON DBMS FROM $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "ROLE MANAGEMENT", Role("ROLE $role"),
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "ROLE MANAGEMENT", Role("ROLE $role"),
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // User management privileges

  test("Grant show user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW USER ON DBMS TO $role1, $role2", Map("role1" -> "reader", "role2" -> "editor")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "SHOW USER", Role("ROLE $role2"),
          dbmsPrivilegePlan("GrantDbmsAction", "SHOW USER", Role("ROLE $role1"),
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny show user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW USER ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "SHOW USER", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke show user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE SHOW USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW USER", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW USER", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE USER ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "CREATE USER", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "CREATE USER", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE USER ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "CREATE USER", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE CREATE USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE USER", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE USER", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP USER ON DBMS TO reader, $role", Map("role" -> "editor")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "DROP USER", Role("ROLE $role"),
          dbmsPrivilegePlan("GrantDbmsAction", "DROP USER", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP USER ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "DROP USER", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DROP USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "DROP USER", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP USER", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ALTER USER ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "ALTER USER", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "ALTER USER", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ALTER USER ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "ALTER USER", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ALTER USER ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "ALTER USER", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "ALTER USER", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant user management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT USER MANAGEMENT ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "USER MANAGEMENT", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "USER MANAGEMENT", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny user management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY USER MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "USER MANAGEMENT", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke user management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE USER MANAGEMENT ON DBMS FROM $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "USER MANAGEMENT", Role("ROLE $role"),
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "USER MANAGEMENT", Role("ROLE $role"),
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // Database management privileges

  test("Grant create database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT CREATE DATABASE ON DBMS TO $role, editor", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "CREATE DATABASE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "CREATE DATABASE", Role("ROLE $role"),
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny create database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY CREATE DATABASE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "CREATE DATABASE", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke create database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE CREATE DATABASE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "CREATE DATABASE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "CREATE DATABASE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant drop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DROP DATABASE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "DROP DATABASE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "DROP DATABASE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny drop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DROP DATABASE ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "DROP DATABASE", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke drop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DROP DATABASE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "DROP DATABASE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "DROP DATABASE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant database management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT DATABASE MANAGEMENT ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "DATABASE MANAGEMENT", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "DATABASE MANAGEMENT", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny database management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY DATABASE MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "DATABASE MANAGEMENT", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke database management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE DATABASE MANAGEMENT ON DBMS FROM $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "DATABASE MANAGEMENT", Role("ROLE $role"),
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "DATABASE MANAGEMENT", Role("ROLE $role"),
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  // Privilege management privileges

  test("Grant show privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT SHOW PRIVILEGE ON DBMS TO $role, editor", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "SHOW PRIVILEGE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "SHOW PRIVILEGE", Role("ROLE $role"),
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny show privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY SHOW PRIVILEGE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "SHOW PRIVILEGE", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke show privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE SHOW PRIVILEGE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "SHOW PRIVILEGE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "SHOW PRIVILEGE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant assign privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ASSIGN PRIVILEGE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "ASSIGN PRIVILEGE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "ASSIGN PRIVILEGE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny assign privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ASSIGN PRIVILEGE ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "ASSIGN PRIVILEGE", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke assign privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ASSIGN PRIVILEGE ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "ASSIGN PRIVILEGE", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "ASSIGN PRIVILEGE", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant remove privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT REMOVE PRIVILEGE ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "REMOVE PRIVILEGE", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "REMOVE PRIVILEGE", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny remove privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY REMOVE PRIVILEGE ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "REMOVE PRIVILEGE", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke remove privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE REMOVE PRIVILEGE ON DBMS FROM $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "REMOVE PRIVILEGE", Role("ROLE $role"),
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "REMOVE PRIVILEGE", Role("ROLE $role"),
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant privilege management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT PRIVILEGE MANAGEMENT ON DBMS TO reader, $role", Map("role" -> "editor")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "PRIVILEGE MANAGEMENT", Role("ROLE $role"),
          dbmsPrivilegePlan("GrantDbmsAction", "PRIVILEGE MANAGEMENT", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny privilege management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY PRIVILEGE MANAGEMENT ON DBMS TO reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "PRIVILEGE MANAGEMENT", "reader",
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke privilege management") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE PRIVILEGE MANAGEMENT ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "PRIVILEGE MANAGEMENT", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "PRIVILEGE MANAGEMENT", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Grant all dbms privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ALL DBMS PRIVILEGES ON DBMS TO reader, editor").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("GrantDbmsAction", "ALL DBMS PRIVILEGES", "editor",
          dbmsPrivilegePlan("GrantDbmsAction", "ALL DBMS PRIVILEGES", "reader",
            assertDbmsAdminPlan("ASSIGN PRIVILEGE")
          )
        )
      ).toString
    )
  }

  test("Deny all dbms privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DENY ALL DBMS PRIVILEGES ON DBMS TO $role", Map("role" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("DenyDbmsAction", "ALL DBMS PRIVILEGES", Role("ROLE $role"),
          assertDbmsAdminPlan("ASSIGN PRIVILEGE")
        )
      ).toString
    )
  }

  test("Revoke all dbms privileges") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN REVOKE ALL DBMS PRIVILEGES ON DBMS FROM reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        dbmsPrivilegePlan("RevokeDbmsAction(DENIED)", "ALL DBMS PRIVILEGES", "reader",
          dbmsPrivilegePlan("RevokeDbmsAction(GRANTED)", "ALL DBMS PRIVILEGES", "reader",
            assertDbmsAdminPlan("REMOVE PRIVILEGE")
          )
        )
      ).toString
    )
  }
}
