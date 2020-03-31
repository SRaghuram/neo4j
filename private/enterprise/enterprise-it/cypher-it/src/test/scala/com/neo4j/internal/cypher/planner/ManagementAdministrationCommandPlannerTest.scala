/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.plandescription.Arguments.Database
import org.neo4j.cypher.internal.plandescription.Arguments.Role
import org.neo4j.cypher.internal.plandescription.Arguments.Scope
import org.neo4j.cypher.internal.plandescription.Arguments.User

class ManagementAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Database commands

  test("Show databases") {
    // When
    val plan = execute("EXPLAIN SHOW DATABASES").executionPlanString()

    // Then
    plan should include(managementPlan("ShowDatabases").toString)
  }

  test(s"Show database $DEFAULT_DATABASE_NAME") {
    // When
    val plan = execute(s"EXPLAIN SHOW DATABASE $DEFAULT_DATABASE_NAME").executionPlanString()

    // Then
    plan should include(managementPlan("ShowDatabase", Seq(databaseArg(DEFAULT_DATABASE_NAME))).toString)
  }

  test(s"Show database $DEFAULT_DATABASE_NAME with parameter") {
    // When
    val plan = execute("EXPLAIN SHOW DATABASE $db", Map("db" -> DEFAULT_DATABASE_NAME)).executionPlanString()

    // Then
    plan should include(managementPlan("ShowDatabase", Seq(Database("$db"))).toString)
  }

  test("Show default database") {
    // When
    val plan = execute("EXPLAIN SHOW DEFAULT DATABASE").executionPlanString()

    // Then
    plan should include(managementPlan("ShowDefaultDatabase").toString)
  }

  test("Create database") {
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

  test("Create database with parameter") {
    // When
    val plan = execute("EXPLAIN CREATE DATABASE $db", Map("db" -> "foo")).executionPlanString()

    // Then
    plan should include(
      helperPlan("EnsureValidNumberOfDatabases",
        managementPlan("CreateDatabase", Seq(Database("$db")),
          assertDbmsAdminPlan("CREATE DATABASE")
        )
      ).toString
    )
  }

  test("Create or replace database") {
    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE DATABASE foo").executionPlanString()

    // Then
    plan should include(
      helperPlan("EnsureValidNumberOfDatabases",
        managementPlan("CreateDatabase", Seq(databaseArg("foo")),
          managementPlan("DropDatabase", Seq(databaseArg("foo")),
            assertDbmsAdminPlan("DROP DATABASE", "CREATE DATABASE")
          )
        )
      ).toString
    )
  }

  test("Create database if not exists") {
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

  test("Drop database with parameter") {
    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN DROP DATABASE $db", Map("db" -> "foo")).executionPlanString()

    // Then
    plan should include(
      managementPlan("DropDatabase", Seq(Database("$db")),
        helperPlan("EnsureValidNonSystemDatabase", Seq(Database("$db")),
          assertDbmsAdminPlan("DROP DATABASE")
        )
      ).toString
    )
  }

  test("Drop database if exists") {
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
    // Given
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // When
    val plan = execute("EXPLAIN START DATABASE foo").executionPlanString()

    // Then
    plan should include(
      managementPlan("StartDatabase", Seq(databaseArg("foo")),
        assertDatabaseAdminPlan("START", databaseArg("foo"))
      ).toString
    )
  }

  test("Start database with parameter") {
    // Given
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // When
    val plan = execute("EXPLAIN START DATABASE $db", Map("db" -> "foo")).executionPlanString()

    // Then
    plan should include(
      managementPlan("StartDatabase", Seq(Database("$db")),
        assertDatabaseAdminPlan("START", Database("$db"))
      ).toString
    )
  }

  test("Stop database") {
    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN STOP DATABASE foo").executionPlanString()

    // Then
    plan should include(
      managementPlan("StopDatabase", Seq(databaseArg("foo")),
        helperPlan("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")),
          assertDatabaseAdminPlan("STOP", databaseArg("foo"))
        )
      ).toString
    )
  }

  test("Stop database with parameter") {
    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN STOP DATABASE $db", Map("db" -> "foo")).executionPlanString()

    // Then
    plan should include(
      managementPlan("StopDatabase", Seq(Database("$db")),
        helperPlan("EnsureValidNonSystemDatabase", Seq(Database("$db")),
          assertDatabaseAdminPlan("STOP", Database("$db"))
        )
      ).toString
    )
  }

  // User commands

  test("Show users") {
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

  test("Create user as parameter") {

    // When
    val plan = execute("EXPLAIN CREATE USER $foo SET PASSWORD 'secret'", Map("foo" -> "bar")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateUser", Seq(User("$foo")),
          assertDbmsAdminPlan("CREATE USER")
        )
      ).toString
    )
  }

  test("Create or replace user") {
    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE USER foo SET PASSWORD 'secret' CHANGE NOT REQUIRED").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateUser", Seq(userArg("foo")),
          managementPlan("DropUser", Seq(userArg("foo")),
            helperPlan("AssertNotCurrentUser", Seq(userArg("foo")),
              assertDbmsAdminPlan("DROP USER", "CREATE USER")
            )
          )
        )
      ).toString
    )
  }

  test("Create user if not exists") {
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
    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN DROP USER foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropUser", Seq(userArg("foo")),
          helperPlan("EnsureNodeExists(User)", Seq(userArg("foo")),
            helperPlan("AssertNotCurrentUser", Seq(userArg("foo")),
              assertDbmsAdminPlan("DROP USER")
            )
          )
        )
      ).toString
    )
  }

  test("Drop user as parameter") {
    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN DROP USER $foo", Map("foo" -> "foo")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropUser", Seq(User("$foo")),
          helperPlan("EnsureNodeExists(User)", Seq(User("$foo")),
            helperPlan("AssertNotCurrentUser", Seq(User("$foo")),
              assertDbmsAdminPlan("DROP USER")
            )
          )
        )
      ).toString
    )
  }

  test("Drop user if exists") {
    // When
    val plan = execute("EXPLAIN DROP USER foo IF EXISTS").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropUser", Seq(userArg("foo")),
          helperPlan("DoNothingIfNotExists(User)", Seq(userArg("foo")),
            helperPlan("AssertNotCurrentUser", Seq(userArg("foo")),
              assertDbmsAdminPlan("DROP USER")
            )
          )
        )
      ).toString
    )
  }

  test("Alter user") {
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

  test("Alter user as parameter") {
    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN ALTER USER $foo SET PASSWORD CHANGE NOT REQUIRED", Map("foo" -> "foo")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("AlterUser", Seq(User("$foo")),
          assertDbmsAdminPlan("ALTER USER")
        )
      ).toString
    )
  }

  test("Alter user to suspended") {
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

  test("Create role as parameter") {
    // When
    val plan = execute("EXPLAIN CREATE ROLE $foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateRole", Seq(Role("$foo")),
          assertDbmsAdminPlan("CREATE ROLE")
        )
      ).toString
    )
  }

  test("Create or replace role") {
    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE ROLE foo").executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("CreateRole", Seq(roleArg("foo")),
          managementPlan("DropRole", Seq(roleArg("foo")),
            assertDbmsAdminPlan("DROP ROLE", "CREATE ROLE")
          )
        )
      ).toString
    )
  }

  test("Create role if not exists") {
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

  test("Create role as copy with parameter") {
    // When
    val plan = execute("EXPLAIN CREATE ROLE foo AS COPY OF $bar", Map("bar" -> "reader")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        helperPlan("CopyRolePrivileges(DENIED)", Seq(Role("FROM ROLE $bar"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
          helperPlan("CopyRolePrivileges(GRANTED)", Seq(Role("FROM ROLE $bar"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
            managementPlan("CreateRole", Seq(roleArg("foo")),
              helperPlan("RequireRole", Seq(Role("$bar")),
                assertDbmsAdminPlan("CREATE ROLE")
              )
            )
          )
        )
      ).toString
    )
  }

  test("Create or replace role as copy") {
    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE ROLE foo AS COPY OF reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        helperPlan("CopyRolePrivileges(DENIED)", Seq(Role(s"FROM ROLE ${Prettifier.escapeName("reader")}"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
          helperPlan("CopyRolePrivileges(GRANTED)", Seq(Role(s"FROM ROLE ${Prettifier.escapeName("reader")}"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
            managementPlan("CreateRole", Seq(roleArg("foo")),
              helperPlan("RequireRole", Seq(roleArg("reader")),
                managementPlan("DropRole", Seq(roleArg("foo")),
                  assertDbmsAdminPlan("DROP ROLE", "CREATE ROLE")
                )
              )
            )
          )
        )
      ).toString
    )
  }

  test("Create role if not exists as copy") {
    // When
    val plan = execute("EXPLAIN CREATE ROLE foo IF NOT EXISTS AS COPY OF reader").executionPlanString()

    // Then
    plan should include(
      logPlan(
        helperPlan("CopyRolePrivileges(DENIED)", Seq(Role(s"FROM ROLE ${Prettifier.escapeName("reader")}"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
          helperPlan("CopyRolePrivileges(GRANTED)", Seq(Role(s"FROM ROLE ${Prettifier.escapeName("reader")}"), Role(s"TO ROLE ${Prettifier.escapeName("foo")}")),
            managementPlan("CreateRole", Seq(roleArg("foo")),
              helperPlan("RequireRole", Seq(roleArg("reader")),
                helperPlan("DoNothingIfExists(Role)", Seq(roleArg("foo")),
                  assertDbmsAdminPlan("CREATE ROLE")
                )
              )
            )
          )
        )
      ).toString
    )
  }

  test("Drop role") {
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

  test("Drop role as parameter") {
    // Given
    execute("CREATE ROLE foo")

    // When
    val plan = execute("EXPLAIN DROP ROLE $foo", Map("foo" -> "foo")).executionPlanString()

    // Then
    plan should include(
      logPlan(
        managementPlan("DropRole", Seq(Role("$foo")),
          helperPlan("EnsureNodeExists(Role)", Seq(Role("$foo")),
            assertDbmsAdminPlan("DROP ROLE")
          )
        )
      ).toString
    )
  }

  test("Drop role if exists") {
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

  test("Show all privileges") {
    // When
    val plan = execute("EXPLAIN SHOW PRIVILEGES").executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowPrivileges", Seq(Scope("ALL")),
        assertDbmsAdminPlan("SHOW PRIVILEGE")
      ).toString
    )
  }

  test("Show role privileges") {
    // When
    val plan = execute("EXPLAIN SHOW ROLE PUBLIC PRIVILEGES").executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowPrivileges", Seq(scopeArg("ROLE", PUBLIC)),
        assertDbmsAdminPlan("SHOW PRIVILEGE")
      ).toString
    )
  }

  test("Show role privileges with parameter") {
    // When
    val plan = execute("EXPLAIN SHOW ROLE $role PRIVILEGES", Map("role" -> "PUBLIC")).executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowPrivileges", Seq(Scope("ROLE $role")),
        assertDbmsAdminPlan("SHOW PRIVILEGE")
      ).toString
    )
  }

  test("Show user privileges") {
    // When
    val plan = execute("EXPLAIN SHOW USER neo4j PRIVILEGES").executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowPrivileges", Seq(scopeArg("USER", "neo4j")),
        assertDbmsAdminOrSelfPlan(userPrivilegeArg("neo4j"), "SHOW PRIVILEGE", "SHOW USER")
      ).toString
    )
  }

  test("Show user privileges with parameter") {
    // When
    val plan = execute("EXPLAIN SHOW USER $user PRIVILEGES", Map("user" -> "neo4j")).executionPlanString()

    // Then
    plan should include(
      managementPlan("ShowPrivileges", Seq(Scope("USER $user")),
        assertDbmsAdminOrSelfPlan(User("USER $user"), "SHOW PRIVILEGE", "SHOW USER")
      ).toString
    )
  }
}
