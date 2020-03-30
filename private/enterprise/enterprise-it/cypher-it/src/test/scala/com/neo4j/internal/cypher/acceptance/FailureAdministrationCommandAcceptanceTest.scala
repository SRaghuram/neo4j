/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class FailureAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Tests for non-existing roles
  test("should give nothing when showing privileges for non-existing role") {
    // WHEN
    val resultFoo = execute("SHOW ROLE foo PRIVILEGES")

    // THEN
    resultFoo.toSet should be(Set.empty)

    // and an invalid (non-existing) one
    // WHEN
    val resultEmpty = execute("SHOW ROLE `` PRIVILEGES")

    // THEN
    resultEmpty.toSet should be(Set.empty)
  }

  test("should fail to grant privilege to non-existing role") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH * NODE A TO role")
      // THEN
    } should have message s"Failed to grant traversal privilege to role 'role': Role does not exist."
  }

  test("should fail to deny privilege to non-existing role") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DENY DROP USER ON DBMS TO $r", Map("r" -> "role"))
      // THEN
    } should have message s"Failed to deny drop_user privilege to role 'role': Role does not exist."
  }

  test("REVOKE from non-existing role should do nothing") {
    // WHEN
    execute(s"REVOKE WRITE ON GRAPH * FROM wrongRole")

    // THEN
    execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)
  }

  test("REVOKE GRANT from non-existing role should do nothing") {
    // WHEN
    execute("REVOKE GRANT START ON DEFAULT DATABASE FROM wrongRole")

    // THEN
    execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)
  }

  test("REVOKE DENY from non-existing role should do nothing") {
    // WHEN
    execute("REVOKE DENY INDEX MANAGEMENT ON DATABASE * FROM $r", Map("r" -> "wrongRole"))

    // THEN
    execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)
  }

  // Tests for non-existing databases

  test("should give nothing when showing a non-existing database") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List.empty)

    // and an invalid (non-existing) one
    // WHEN
    val result2 = execute("SHOW DATABASE ``")

    // THEN
    result2.toList should be(List.empty)
  }

  test("should fail to grant privilege on non-existing database") {
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("GRANT TRAVERSE ON GRAPH foo TO reader")
      // THEN
    } should have message "Failed to grant traversal privilege to role 'reader': Database 'foo' does not exist."
  }

  test("should fail to deny privilege on non-existing database") {
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DENY STOP ON DATABASE $db TO $role", Map("db" -> "foo", "role" -> "reader"))
      // THEN
    } should have message "Failed to deny stop_database privilege to role 'reader': Database 'foo' does not exist."
  }

  test("REVOKE on non-existing database should do nothing") {
    // WHEN
    execute("REVOKE WRITE ON GRAPH $db FROM editor", Map("db" -> "foo"))

    // THEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("REVOKE GRANT on non-existing database should do nothing") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT SHOW TRANSACTION (*) ON DATABASE * TO custom")

    // WHEN
    execute("REVOKE GRANT SHOW TRANSACTION (*) ON DATABASE foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(showTransaction("*")).database("*").role("custom").map))
  }

  test("REVOKE DENY on non-existing database should do nothing") {
    // WHEN
    execute("REVOKE DENY DROP CONSTRAINT ON DATABASE foo FROM publisher")

    // THEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  // Test for security commands not on system database
  test("should fail when not on system database") {
    // GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // Some different security commands
    Seq(
      ("SHOW DEFAULT DATABASE", "SHOW DEFAULT DATABASE"),
      ("CREATE USER foo SET PASSWORD 'bar'", "CREATE USER"),
      ("DROP ROLE reader", "DROP ROLE"),
      ("GRANT WRITE ON GRAPH * TO reader", "GRANT WRITE"),
      ("DENY USER MANAGEMENT ON DBMS TO editor", "DENY USER MANAGEMENT"),
      ("REVOKE ACCESS ON DEFAULT DATABASE FROM reader", "REVOKE ACCESS")
    ).foreach {
      case (query, command) =>
        withClue(s"$query on default database:") {
          the[DatabaseAdministrationException] thrownBy {
            // WHEN
            execute(query)
            // THEN
          } should have message
            s"This is an administration command and it should be executed against the system database: $command"
        }
    }

    // THEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }
}
