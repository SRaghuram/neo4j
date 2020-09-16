/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import com.neo4j.internal.cypher.acceptance.AdministrationCommandAcceptanceTestBase
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME

class AdministrationCommandPlannerTest extends AdministrationCommandAcceptanceTestBase {

  override protected def initTest() {
    super.initTest()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  private val allPrivilegeCommandsWithMerge: Iterable[String] = allPrivilegeCommands ++ Seq("MERGE {prop} ON GRAPH foo")

  private val managementCommands = Seq(
    // Database commands
    s"SHOW DATABASE $DEFAULT_DATABASE_NAME",
    "SHOW DEFAULT DATABASE",
    "SHOW DATABASES YIELD name ORDER BY name SKIP 1 LIMIT 1 WHERE name='neo4j' RETURN name",
    "CREATE DATABASE foo.bar",
    "CREATE OR REPLACE DATABASE foo",
    "CREATE DATABASE foo IF NOT EXISTS",
    "DROP DATABASE $db",
    "DROP DATABASE foo IF EXISTS",
    "DROP DATABASE foo DUMP DATA",
    "START DATABASE foo",
    "STOP DATABASE foo",

    // User commands
    "SHOW USERS WHERE user ='bob'",
    "CREATE USER foo SET PASSWORD 'secret'",
    "CREATE USER foo SET ENCRYPTED PASSWORD 'password'",
    "CREATE OR REPLACE USER foo SET PASSWORD 'secret' CHANGE NOT REQUIRED",
    "CREATE USER foo IF NOT EXISTS SET PASSWORD 'secret' SET STATUS SUSPENDED",
    "DROP USER foo",
    "DROP USER foo IF EXISTS",
    "ALTER USER foo SET PLAINTEXT PASSWORD 'password' SET STATUS suspended",
    "ALTER USER foo SET ENCRYPTED PASSWORD 'password'",
    "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secret'",

    // Role commands
    "SHOW ROLES",
    "SHOW ROLES WITH USERS WHERE role = 'PUBLIC'",
    "SHOW POPULATED ROLES WITH USERS",
    "CREATE ROLE foo",
    "CREATE OR REPLACE ROLE foo",
    "CREATE ROLE foo IF NOT EXISTS",
    "CREATE ROLE foo AS COPY OF reader",
    "CREATE OR REPLACE ROLE foo AS COPY OF reader",
    "CREATE ROLE foo IF NOT EXISTS AS COPY OF reader",
    "DROP ROLE foo",
    "DROP ROLE foo IF EXISTS",
    "GRANT ROLE reader TO neo4j",
    "GRANT ROLE reader, editor, publisher TO neo4j, foo",
    "REVOKE ROLE reader FROM neo4j",
    "REVOKE ROLE reader, editor FROM neo4j, foo, bar",

    // SHOW _ PRIVILEGES
    "SHOW PRIVILEGES WHERE role = 'PUBLIC'",
    "SHOW ROLES reader, $role PRIVILEGES",
    "SHOW USER neo4j PRIVILEGES",
    "SHOW USER PRIVILEGES",
  )

  test("Should show correct plan for management commands") {
    managementCommands.foreach { command =>
      withClue(s"Command: $command ${System.lineSeparator()}${System.lineSeparator()}") {
        assertAdminCommandPlan(execute(s"EXPLAIN $command").executionPlanString())
      }
    }
  }

  test("Should show correct plan for grant commands") {
    allPrivilegeCommandsWithMerge.foreach { command =>
      withClue(s"Command: $command ${System.lineSeparator()}${System.lineSeparator()}") {
        assertAdminCommandPlan(execute(s"EXPLAIN GRANT $command TO reader,editor").executionPlanString())
      }
    }
  }

  test("Should show correct plan for deny commands") {
    allPrivilegeCommands.foreach { command =>
      withClue(s"Command: $command ${System.lineSeparator()}${System.lineSeparator()}") {
        assertAdminCommandPlan(execute(s"EXPLAIN DENY $command TO $$role", Map("role" -> "editor")).executionPlanString())
      }
    }
  }

  test("Should show correct plan for revoke commands") {
    allPrivilegeCommandsWithMerge.foreach { command =>
      withClue(s"Command: $command ${System.lineSeparator()}${System.lineSeparator()}") {
        assertAdminCommandPlan(execute(s"EXPLAIN REVOKE $command FROM reader").executionPlanString())
      }
    }
  }

  test("Should show correct plan for revoke grant commands") {
    allPrivilegeCommandsWithMerge.foreach { command =>
      withClue(s"Command: $command ${System.lineSeparator()}${System.lineSeparator()}") {
        assertAdminCommandPlan(execute(s"EXPLAIN REVOKE GRANT $command FROM reader").executionPlanString())
      }
    }
  }

  test("Should show correct plan for revoke deny commands") {
    allPrivilegeCommands.foreach { command =>
      withClue(s"Command: $command ${System.lineSeparator()}${System.lineSeparator()}") {
        assertAdminCommandPlan(execute(s"EXPLAIN REVOKE DENY $command FROM reader").executionPlanString())
      }
    }
  }

  private def assertAdminCommandPlan(plan: String): Unit = {
    val newLine = System.lineSeparator()
    val doubleNewLine = newLine + newLine

    plan should be(
      "Compiler CYPHER 4.2" + doubleNewLine +
        "Planner ADMINISTRATION" + doubleNewLine +
        "Runtime SYSTEM" + doubleNewLine +
        "Runtime version 4.2" + doubleNewLine +
        "+------------------------+" + newLine +
        "| Operator               |" + newLine +
        "+------------------------+" + newLine +
        "| +AdministrationCommand |" + newLine +
        "+------------------------+" + doubleNewLine +
        "Total database accesses: ?" + newLine
    )
  }
}
