/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.plandescription.Arguments.Role

class ManagementAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Database commands

  test("Show databases") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW DATABASES").executionPlanString()

    // Then
    plan should include(managementPlan("ShowDatabases").toString)
  }

  test(s"Show database $DEFAULT_DATABASE_NAME") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN SHOW DATABASE $DEFAULT_DATABASE_NAME").executionPlanString()

    // Then
    plan should include(managementPlan("ShowDatabase", Seq(databaseArg(DEFAULT_DATABASE_NAME))).toString)
  }

  test("Show default database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW DEFAULT DATABASE").executionPlanString()

    // Then
    plan should include(managementPlan("ShowDefaultDatabase").toString)
  }

  test("Create database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE DATABASE foo").executionPlanString()

    // Then
    plan should include(
      helperPlan("EnsureValidNumberOfDatabases",
        managementPlan("CreateDatabase", Seq(databaseArg("foo")),
          assertDbmsAdminPlan("CREATE DATABASE")
        )
      ).toString
    )
  }

  test("Create or replace database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE DATABASE foo").executionPlanString()

    // Then
    plan should include(
      helperPlan("EnsureValidNumberOfDatabases",
        managementPlan("CreateDatabase", Seq(databaseArg("foo")),
          managementPlan("DropDatabase", Seq(databaseArg("foo")),
            assertDbmsAdminPlan("CREATE DATABASE")
          )
        )
      ).toString
    )
  }

  test("Create database if not exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE DATABASE foo IF NOT EXISTS").executionPlanString()

    // Then
    plan should include(
      helperPlan("EnsureValidNumberOfDatabases",
        managementPlan("CreateDatabase", Seq(databaseArg("foo")),
          helperPlan("DoNothingIfExists(Database)", Seq(databaseArg("foo")),
            assertDbmsAdminPlan("CREATE DATABASE")
          )
        )
      ).toString
    )
  }

  test("Drop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN DROP DATABASE foo").executionPlanString()

    // Then
    plan should include(
      managementPlan("DropDatabase", Seq(databaseArg("foo")),
        helperPlan("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")),
          assertDbmsAdminPlan("DROP DATABASE")
        )
      ).toString
    )
  }

  test("Drop database if exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DROP DATABASE foo IF EXISTS").executionPlanString()

    // Then
    plan should include(
      managementPlan("DropDatabase", Seq(databaseArg("foo")),
        helperPlan("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")),
          helperPlan("DoNothingIfNotExists(Database)", Seq(databaseArg("foo")),
            assertDbmsAdminPlan("DROP DATABASE")
          )
        )
      ).toString
    )
  }

  test("Start database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // When
    val plan = execute("EXPLAIN START DATABASE foo").executionPlanString()

    // Then
    plan should include(
      managementPlan("StartDatabase", Seq(databaseArg("foo")),
        assertDatabaseAdminPlan("START", "foo")
      ).toString
    )
  }

  test("Stop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN STOP DATABASE foo").executionPlanString()

    // Then
    plan should include(
      managementPlan("StopDatabase", Seq(databaseArg("foo")),
        helperPlan("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")),
          assertDatabaseAdminPlan("STOP", "foo")
        )
      ).toString
    )
  }

  // User commands

  test("Show users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW USERS").executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowUsers",
        assertDbmsAdminPlan("SHOW USER")
      ).toString
    )
  }

  test("Create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE USER foo SET PASSWORD 'secret'").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateUser", Seq(userArg("foo")),
          assertDbmsAdminPlan("CREATE USER")
        )
      ).toString
    )
  }

  test("Create or replace user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE USER foo SET PASSWORD 'secret' CHANGE NOT REQUIRED").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateUser", Seq(userArg("foo")),
          managementPlan("DropUser", Seq(userArg("foo")),
            assertDbmsAdminPlan("CREATE USER")
          )
        )
      ).toString
    )
  }

  test("Create user if not exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE USER foo IF NOT EXISTS SET PASSWORD 'secret' SET STATUS SUSPENDED").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateUser", Seq(userArg("foo")),
          helperPlan("DoNothingIfExists(User)", Seq(userArg("foo")),
            assertDbmsAdminPlan("CREATE USER")
          )
        )
      ).toString
    )
  }

  test("Drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN DROP USER foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropUser", Seq(userArg("foo")),
          helperPlan("EnsureNodeExists(User)", Seq(userArg("foo")),
            assertDbmsAdminPlan("DROP USER")
          )
        )
      ).toString
    )
  }

  test("Drop user if exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DROP USER foo IF EXISTS").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropUser", Seq(userArg("foo")),
          helperPlan("DoNothingIfNotExists(User)", Seq(userArg("foo")),
            assertDbmsAdminPlan("DROP USER")
          )
        )
      ).toString
    )
  }

  test("Alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("AlterUser", Seq(userArg("foo")),
          assertDbmsAdminPlan("ALTER USER")
        )
      ).toString
    )
  }

  test("Alter user to suspended") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN ALTER USER foo SET STATUS SUSPENDED").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("AlterUser", Seq(userArg("foo")),
          helperPlan("AssertNotCurrentUser", Seq(userArg("foo")),
            assertDbmsAdminPlan("ALTER USER")
          )
        )
      ).toString
    )
  }

  test("Alter current user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secret'").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("AlterCurrentUserSetPassword")
      ).toString
    )
  }

  // Role commands

  test("Show roles") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW ROLES").executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowRoles",
        assertDbmsAdminPlan("SHOW ROLE")
      ).toString
    )
  }

  test("Show populated roles with users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW POPULATED ROLES WITH USERS").executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowRoles",
        assertDbmsAdminPlan("SHOW ROLE")
      ).toString
    )
  }

  test("Create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE ROLE foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateRole", Seq(roleArg("foo")),
          assertDbmsAdminPlan("CREATE ROLE")
        )
      ).toString
    )
  }

  test("Create or replace role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE ROLE foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateRole", Seq(roleArg("foo")),
          managementPlan("DropRole", Seq(roleArg("foo")),
            assertDbmsAdminPlan("CREATE ROLE")
          )
        )
      ).toString
    )
  }

  test("Create role if not exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE ROLE foo IF NOT EXISTS").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateRole", Seq(roleArg("foo")),
          helperPlan("DoNothingIfExists(Role)", Seq(roleArg("foo")),
            assertDbmsAdminPlan("CREATE ROLE")
          )
        )
      ).toString
    )
  }

  test("Create role as copy") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE ROLE foo AS COPY OF reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        helperPlan("CopyRolePrivileges(DENIED)", Seq(Role(s"FROM ROLE ${Prettifier.escapeName("reader")}"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
          helperPlan("CopyRolePrivileges(GRANTED)", Seq(Role(s"FROM ROLE ${Prettifier.escapeName("reader")}"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
            managementPlan("CreateRole", Seq(roleArg("foo")),
              helperPlan("RequireRole", Seq(roleArg("reader")),
                assertDbmsAdminPlan("CREATE ROLE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE ROLE foo")

    // When
    val plan = execute("EXPLAIN DROP ROLE foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropRole", Seq(roleArg("foo")),
          helperPlan("EnsureNodeExists(Role)", Seq(roleArg("foo")),
            assertDbmsAdminPlan("DROP ROLE")
          )
        )
      ).toString
    )
  }

  test("Drop role if exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DROP ROLE foo IF EXISTS").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropRole", Seq(roleArg("foo")),
          helperPlan("DoNothingIfNotExists(Role)", Seq(roleArg("foo")),
            assertDbmsAdminPlan("DROP ROLE")
          )
        )
      ).toString
    )
  }

  test("Grant role to user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ROLE reader TO neo4j").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("neo4j")),
          assertDbmsAdminPlan("ASSIGN ROLE")
        )
      ).toString
    )
  }

  test("Grant multiple roles to multiple users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN GRANT ROLE reader, editor, publisher TO neo4j, foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("publisher"), userPrivilegeArg("foo")),
          managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("editor"), userPrivilegeArg("foo")),
            managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("foo")),
              managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("publisher"), userPrivilegeArg("neo4j")),
                managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("editor"), userPrivilegeArg("neo4j")),
                  managementPlan("GrantRoleToUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("neo4j")),
                    assertDbmsAdminPlan("ASSIGN ROLE")
                  )
                )
              )
            )
          )
        )
      ).toString
    )
  }

  test("Revoke role from user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("GRANT ROLE reader TO neo4j")

    // When
    val plan = execute("EXPLAIN REVOKE ROLE reader FROM neo4j").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("neo4j")),
          assertDbmsAdminPlan("REMOVE ROLE")
        )
      ).toString
    )
  }

  test("Revoke multiple roles from multiple users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")
    execute("CREATE USER bar SET PASSWORD 'secret'")
    execute("GRANT ROLE reader, editor TO neo4j, foo, bar")

    // When
    val plan = execute("EXPLAIN REVOKE ROLE reader, editor FROM neo4j, foo, bar").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("editor"), userPrivilegeArg("bar")),
          managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("bar")),
            managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("editor"), userPrivilegeArg("foo")),
              managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("foo")),
                managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("editor"), userPrivilegeArg("neo4j")),
                  managementPlan("RevokeRoleFromUser", Seq(rolePrivilegeArg("reader"), userPrivilegeArg("neo4j")),
                    assertDbmsAdminPlan("REMOVE ROLE")
                  )
                )
              )
            )
          )
        )
      ).toString
    )
  }

  // SHOW _ PRIVILEGES

  // TODO
}
