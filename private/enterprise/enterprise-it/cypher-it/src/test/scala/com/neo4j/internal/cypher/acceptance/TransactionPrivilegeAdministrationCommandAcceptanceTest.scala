/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class TransactionPrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT SHOW TRANSACTION (*) ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT SHOW TRANSACTION (*) ON DATABASE * FROM custom" -> 1,
      "DENY SHOW TRANSACTION (user) ON DATABASE * TO custom" -> 1,
      "REVOKE DENY SHOW TRANSACTION (user) ON DATABASE * FROM custom" -> 1,
      "GRANT SHOW TRANSACTION (user1,user2) ON DATABASE * TO custom" -> 2,
      "DENY SHOW TRANSACTION (user1,user2) ON DATABASE * TO custom" -> 2,
      "REVOKE SHOW TRANSACTION (user1,user2) ON DATABASE * FROM custom" -> 4,

      "GRANT TERMINATE TRANSACTION (*) ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT TERMINATE TRANSACTION (*) ON DATABASE * FROM custom" -> 1,
      "DENY TERMINATE TRANSACTION (user) ON DATABASE * TO custom" -> 1,
      "REVOKE DENY TERMINATE TRANSACTION (user) ON DATABASE * FROM custom" -> 1,
      "GRANT TERMINATE TRANSACTION (user1,user2) ON DATABASE * TO custom" -> 2,
      "DENY TERMINATE TRANSACTION (user1,user2) ON DATABASE * TO custom" -> 2,
      "REVOKE TERMINATE TRANSACTION (user1,user2) ON DATABASE * FROM custom" -> 4,

      "GRANT TRANSACTION MANAGEMENT ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT TRANSACTION MANAGEMENT ON DATABASE * FROM custom" -> 1,
      "DENY TRANSACTION MANAGEMENT ON DATABASE * TO custom" -> 1,
      "REVOKE DENY TRANSACTION MANAGEMENT ON DATABASE * FROM custom" -> 1,
      "GRANT TRANSACTION MANAGEMENT (*) ON DATABASE * TO custom" -> 1,
      "DENY TRANSACTION MANAGEMENT (*) ON DATABASE * TO custom" -> 1,
      "REVOKE TRANSACTION MANAGEMENT (*) ON DATABASE * FROM custom" -> 2,
      "GRANT TRANSACTION MANAGEMENT (user1,user2) ON DATABASE * TO custom" -> 2,
      "DENY TRANSACTION MANAGEMENT (user1,user2) ON DATABASE * TO custom" -> 2,
      "REVOKE TRANSACTION MANAGEMENT (user1,user2) ON DATABASE * FROM custom" -> 4
    ))
  }

  test("should grant show transaction privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT SHOW TRANSACTION ON DATABASE foo TO role")
    execute("GRANT SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      showTransaction("*").database("foo").role("role").map,
      showTransaction("user1").database(DEFAULT).role("role").map,
      showTransaction("user2").database(DEFAULT).role("role").map
    ))
  }

  test("should deny show transaction privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY SHOW TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      showTransaction("*", DENIED).database("foo").role("role").map,
      showTransaction("user1", DENIED).database(DEFAULT).role("role").map,
      showTransaction("user2", DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke show transaction privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT SHOW TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE SHOW TRANSACTION (*) ON DATABASE foo FROM role")
    execute("REVOKE SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to grant show transaction privilege to non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT SHOW TRANSACTION (*) ON DATABASE * TO role")
      // THEN
    } should have message "Failed to grant show_transaction privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to grant show transaction privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("GRANT SHOW TRANSACTION (*) ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to grant show_transaction privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant terminate transaction privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT TERMINATE TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      terminateTransaction("*").database("foo").role("role").map,
      terminateTransaction("user1").database(DEFAULT).role("role").map,
      terminateTransaction("user2").database(DEFAULT).role("role").map
    ))
  }

  test("should deny terminate transaction privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY TERMINATE TRANSACTION ON DATABASE foo TO role")
    execute("DENY TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      terminateTransaction("*", DENIED).database("foo").role("role").map,
      terminateTransaction("user1", DENIED).database(DEFAULT).role("role").map,
      terminateTransaction("user2", DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke terminate transaction privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT TERMINATE TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE TERMINATE TRANSACTION (*) ON DATABASE foo FROM role")
    execute("REVOKE TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to deny terminate transaction privilege to non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DENY TERMINATE TRANSACTION (*) ON DATABASE * TO role")
      // THEN
    } should have message "Failed to deny terminate_transaction privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to deny terminate transaction privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DENY TERMINATE TRANSACTION (*) ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to deny terminate_transaction privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant transaction management privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TRANSACTION ON DEFAULT DATABASE TO role")
    execute("GRANT TRANSACTION (user1,user2) ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      transaction("*").database("foo").role("role").map,
      transaction("*").database(DEFAULT).role("role").map,
      transaction("user1").role("role").map,
      transaction("user2").role("role").map
    ))
  }

  test("should deny transaction management privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY TRANSACTION ON DEFAULT DATABASE TO role")
    execute("DENY TRANSACTION (user1,user2) ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      transaction("*", DENIED).database("foo").role("role").map,
      transaction("*", DENIED).database(DEFAULT).role("role").map,
      transaction("user1", DENIED).role("role").map,
      transaction("user2", DENIED).role("role").map
    ))
  }

  test("should revoke transaction management privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY TRANSACTION ON DEFAULT DATABASE TO role")
    execute("GRANT TRANSACTION (user1) ON DATABASE * TO role")
    execute("DENY TRANSACTION (user1) ON DATABASE * TO role")

    // WHEN
    execute("REVOKE TRANSACTION ON DATABASE foo FROM role")
    execute("REVOKE TRANSACTION ON DEFAULT DATABASE FROM role")
    execute("REVOKE TRANSACTION (user1,user2) ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should do nothing when revoking transaction management privilege from non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")
    execute("GRANT TRANSACTION (*) ON DATABASE * TO role")

    // WHEN
    execute("REVOKE TRANSACTION (*) ON DATABASE * FROM wrongRole")
  }

  test("should do nothing when revoking transaction management privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")
    execute("CREATE DATABASE bar")
    execute("GRANT TRANSACTION (*) ON DATABASE bar TO role")

    // WHEN
    execute("REVOKE TRANSACTION (*) ON DATABASE foo FROM role")
  }
}
