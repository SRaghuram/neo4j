/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.internal.plandescription.Arguments.{DatabaseAction, DbmsAction}
import org.neo4j.cypher.internal.plandescription.SingleChild

class ManagementAdministrationCommandPlannerTest extends AdministrationCommandPlannerTestBase {

  // Database commands

  test("Show databases") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW DATABASES").executionPlanString()

    // Then
    val expectedPlan = planDescription("ShowDatabases")
    plan should include(expectedPlan.toString)
  }

  test(s"Show database $DEFAULT_DATABASE_NAME") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute(s"EXPLAIN SHOW DATABASE $DEFAULT_DATABASE_NAME").executionPlanString()

    // Then
    val expectedPlan = planDescription("ShowDatabase", Seq(databaseArg(DEFAULT_DATABASE_NAME)))
    plan should include(expectedPlan.toString)
  }

  test("Show default database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW DEFAULT DATABASE").executionPlanString()

    // Then
    val expectedPlan = planDescription("ShowDefaultDatabase")
    plan should include(expectedPlan.toString)
  }

  test("Create database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE DATABASE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE DATABASE")))
    val createDatabase = planDescription("CreateDatabase", Seq(databaseArg("foo")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("EnsureValidNumberOfDatabases", children = SingleChild(createDatabase))

    plan should include(expectedPlan.toString)
  }

  test("Create or replace database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE DATABASE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE DATABASE")))
    val dropDatabase = planDescription("DropDatabase", Seq(databaseArg("foo")), SingleChild(assertAdmin))
    val createDatabase = planDescription("CreateDatabase", Seq(databaseArg("foo")), SingleChild(dropDatabase))
    val expectedPlan = planDescription("EnsureValidNumberOfDatabases", children = SingleChild(createDatabase))

    plan should include(expectedPlan.toString)
  }

  test("Create database if not exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE DATABASE foo IF NOT EXISTS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE DATABASE")))
    val ifExists = planDescription("DoNothingIfExists(Database)", Seq(databaseArg("foo")), SingleChild(assertAdmin))
    val createDatabase = planDescription("CreateDatabase", Seq(databaseArg("foo")), SingleChild(ifExists))
    val expectedPlan = planDescription("EnsureValidNumberOfDatabases", children = SingleChild(createDatabase))

    plan should include(expectedPlan.toString)
  }

  test("Drop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN DROP DATABASE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DROP DATABASE")))
    val nonSystem = planDescription("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("DropDatabase", Seq(databaseArg("foo")), SingleChild(nonSystem))

    plan should include(expectedPlan.toString)
  }

  test("Drop database if exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DROP DATABASE foo IF EXISTS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DROP DATABASE")))
    val ifNotExists = planDescription("DoNothingIfNotExists(Database)", Seq(databaseArg("foo")), SingleChild(assertAdmin))
    val nonSystem = planDescription("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")), SingleChild(ifNotExists))
    val expectedPlan = planDescription("DropDatabase", Seq(databaseArg("foo")), SingleChild(nonSystem))

    plan should include(expectedPlan.toString)
  }

  test("Start database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // When
    val plan = execute("EXPLAIN START DATABASE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDatabaseAdmin", Seq(DatabaseAction("START"), databaseArg("foo")))
    val expectedPlan = planDescription("StartDatabase", Seq(databaseArg("foo")), SingleChild(assertAdmin))

    plan should include(expectedPlan.toString)
  }

  test("Stop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE DATABASE foo")

    // When
    val plan = execute("EXPLAIN STOP DATABASE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDatabaseAdmin", Seq(DbmsAction("STOP"), databaseArg("foo")))
    val nonSystem = planDescription("EnsureValidNonSystemDatabase", Seq(databaseArg("foo")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("StopDatabase", Seq(databaseArg("foo")), SingleChild(nonSystem))

    plan should include(expectedPlan.toString)
  }

  // User commands

  test("Show users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW USERS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("SHOW USERS")))
    val expectedPlan = planDescription("ShowUsers", children = SingleChild(assertAdmin))
    plan should include(expectedPlan.toString)
  }

  test("Create user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE USER foo SET PASSWORD 'secret'").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE USER")))
    val createUser = planDescription("CreateUser", Seq(userArg("foo")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(createUser))
    plan should include(expectedPlan.toString)
  }

  test("Create or replace user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE USER foo SET PASSWORD 'secret' CHANGE NOT REQUIRED").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE USER")))
    val dropUser = planDescription("DropUser", Seq(userArg("foo")), SingleChild(assertAdmin))
    val createUser = planDescription("CreateUser", Seq(userArg("foo")), SingleChild(dropUser))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(createUser))
    plan should include(expectedPlan.toString)
  }

  test("Create user if not exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE USER foo IF NOT EXISTS SET PASSWORD 'secret' SET STATUS SUSPENDED").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE USER")))
    val ifExists = planDescription("DoNothingIfExists(User)", Seq(userArg("foo")), SingleChild(assertAdmin))
    val createUser = planDescription("CreateUser", Seq(userArg("foo")), SingleChild(ifExists))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(createUser))
    plan should include(expectedPlan.toString)
  }

  test("Drop user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN DROP USER foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DROP USER")))
    val ensureExists = planDescription("EnsureNodeExists(User)", Seq(userArg("foo")), SingleChild(assertAdmin))
    val dropUser = planDescription("DropUser", Seq(userArg("foo")), SingleChild(ensureExists))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(dropUser))
    plan should include(expectedPlan.toString)
  }

  test("Drop user if exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DROP USER foo IF EXISTS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DROP USER")))
    val ifNotExists = planDescription("DoNothingIfNotExists(User)", Seq(userArg("foo")), SingleChild(assertAdmin))
    val dropUser = planDescription("DropUser", Seq(userArg("foo")), SingleChild(ifNotExists))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(dropUser))
    plan should include(expectedPlan.toString)
  }

  test("Alter user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("ALTER USER")))
    val alterUser = planDescription("AlterUser", Seq(userArg("foo")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(alterUser))
    plan should include(expectedPlan.toString)
  }

  test("Alter user to suspended") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN ALTER USER foo SET STATUS SUSPENDED").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("ALTER USER")))
    val assertNotCurrent = planDescription("AssertNotCurrentUser", Seq(userArg("foo")), SingleChild(assertAdmin))
    val alterUser = planDescription("AlterUser", Seq(userArg("foo")), SingleChild(assertNotCurrent))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(alterUser))
    plan should include(expectedPlan.toString)
  }

  test("Alter current user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secret'").executionPlanString()

    // Then
    val alterCurrent = planDescription("AlterCurrentUserSetPassword")
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(alterCurrent))
    plan should include(expectedPlan.toString)
  }

  // Role commands

  test("Show roles") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW ROLES").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("SHOW ROLE")))
    val expectedPlan = planDescription("ShowRoles", children = SingleChild(assertAdmin))
    plan should include(expectedPlan.toString)
  }

  test("Show populated roles with users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN SHOW POPULATED ROLES WITH USERS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("SHOW ROLE")))
    val expectedPlan = planDescription("ShowRoles", children = SingleChild(assertAdmin))
    plan should include(expectedPlan.toString)
  }

  test("Create role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE ROLE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE ROLE")))
    val createRole = planDescription("CreateRole", Seq(roleArg("foo")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(createRole))
    plan should include(expectedPlan.toString)
  }

  test("Create or replace role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE OR REPLACE ROLE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE ROLE")))
    val dropRole = planDescription("DropRole", Seq(roleArg("foo")), SingleChild(assertAdmin))
    val createRole = planDescription("CreateRole", Seq(roleArg("foo")), SingleChild(dropRole))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(createRole))
    plan should include(expectedPlan.toString)
  }

  test("Create role if not exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE ROLE foo IF NOT EXISTS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE ROLE")))
    val ifExists = planDescription("DoNothingIfExists(Role)", Seq(roleArg("foo")), SingleChild(assertAdmin))
    val createRole = planDescription("CreateRole", Seq(roleArg("foo")), SingleChild(ifExists))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(createRole))
    plan should include(expectedPlan.toString)
  }

  test("Create role as copy") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN CREATE ROLE foo AS COPY OF reader").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("CREATE ROLE")))
    val requireRole = planDescription("RequireRole", Seq(roleArg("reader")), SingleChild(assertAdmin))
    val createRole = planDescription("CreateRole", Seq(roleArg("foo")), SingleChild(requireRole))
    val copyPrivilegeGranted = planDescription("CopyRolePrivileges(GRANTED)", Seq(roleArg("reader"), roleArg("foo")), SingleChild(createRole))
    val copyPrivilegeDenied = planDescription("CopyRolePrivileges(DENIED)", Seq(roleArg("reader"), roleArg("foo")), SingleChild(copyPrivilegeGranted))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(copyPrivilegeDenied))
    plan should include(expectedPlan.toString)
  }

  test("Drop role") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE ROLE foo")

    // When
    val plan = execute("EXPLAIN DROP ROLE foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DROP ROLE")))
    val checkFrozen = planDescription("CheckFrozenRole", Seq(roleArg("foo")), SingleChild(assertAdmin))
    val ensureExists = planDescription("EnsureNodeExists(Role)", Seq(userArg("foo")), SingleChild(checkFrozen))
    val dropRole = planDescription("DropRole", Seq(roleArg("foo")), SingleChild(ensureExists))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(dropRole))
    plan should include(expectedPlan.toString)
  }

  test("Drop role if exists") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN DROP ROLE foo IF EXISTS").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("DROP ROLE")))
    val checkFrozen = planDescription("CheckFrozenRole", Seq(roleArg("foo")), SingleChild(assertAdmin))
    val ifNotExists = planDescription("DoNothingIfNotExists(Role)", Seq(userArg("foo")), SingleChild(checkFrozen))
    val dropRole = planDescription("DropRole", Seq(roleArg("foo")), SingleChild(ifNotExists))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(dropRole))
    plan should include(expectedPlan.toString)
  }

  test("Grant role to user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // When
    val plan = execute("EXPLAIN GRANT ROLE reader TO neo4j").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("ASSIGN ROLE")))
    val grantRole = planDescription("GrantRoleToUser", Seq(roleArg("reader"), userArg("neo4j")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRole))
    plan should include(expectedPlan.toString)
  }

  test("Grant multiple roles to multiple users") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("CREATE USER foo SET PASSWORD 'secret'")

    // When
    val plan = execute("EXPLAIN GRANT ROLE reader, editor, publisher TO neo4j, foo").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("ASSIGN ROLE")))
    val grantRole1 = planDescription("GrantRoleToUser", Seq(roleArg("reader"), userArg("neo4j")), SingleChild(assertAdmin))
    val grantRole2 = planDescription("GrantRoleToUser", Seq(roleArg("editor"), userArg("neo4j")), SingleChild(grantRole1))
    val grantRole3 = planDescription("GrantRoleToUser", Seq(roleArg("publisher"), userArg("neo4j")), SingleChild(grantRole2))
    val grantRole4 = planDescription("GrantRoleToUser", Seq(roleArg("reader"), userArg("foo")), SingleChild(grantRole3))
    val grantRole5 = planDescription("GrantRoleToUser", Seq(roleArg("editor"), userArg("foo")), SingleChild(grantRole4))
    val grantRole6 = planDescription("GrantRoleToUser", Seq(roleArg("publisher"), userArg("foo")), SingleChild(grantRole5))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRole6))
    plan should include(expectedPlan.toString)
  }

  test("Revoke role from user") {
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Given
    execute("GRANT ROLE reader TO neo4j")

    // When
    val plan = execute("EXPLAIN REVOKE ROLE reader FROM neo4j").executionPlanString()

    // Then
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REMOVE ROLE")))
    val grantRole = planDescription("RevokeRoleFromUser", Seq(roleArg("reader"), userArg("neo4j")), SingleChild(assertAdmin))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRole))
    plan should include(expectedPlan.toString)
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
    val assertAdmin = planDescription("AssertDbmsAdmin", Seq(DbmsAction("REMOVE ROLE")))
    val grantRole1 = planDescription("RevokeRoleFromUser", Seq(roleArg("reader"), userArg("neo4j")), SingleChild(assertAdmin))
    val grantRole2 = planDescription("RevokeRoleFromUser", Seq(roleArg("editor"), userArg("neo4j")), SingleChild(grantRole1))
    val grantRole3 = planDescription("RevokeRoleFromUser", Seq(roleArg("reader"), userArg("foo")), SingleChild(grantRole2))
    val grantRole4 = planDescription("RevokeRoleFromUser", Seq(roleArg("editor"), userArg("foo")), SingleChild(grantRole3))
    val grantRole5 = planDescription("RevokeRoleFromUser", Seq(roleArg("reader"), userArg("bar")), SingleChild(grantRole4))
    val grantRole6 = planDescription("RevokeRoleFromUser", Seq(roleArg("editor"), userArg("bar")), SingleChild(grantRole5))
    val expectedPlan = planDescription("LogSystemCommand", children = SingleChild(grantRole6))
    plan should include(expectedPlan.toString)
  }
}
