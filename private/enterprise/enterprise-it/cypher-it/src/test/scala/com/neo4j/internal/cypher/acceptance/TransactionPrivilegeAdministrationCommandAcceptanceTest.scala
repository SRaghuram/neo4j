/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

class TransactionPrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
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
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT SHOW TRANSACTION ON DATABASE foo TO role")
    execute("GRANT SHOW TRANSACTION ON DATABASE $db TO role", Map("db" -> "bar"))
    execute("GRANT SHOW TRANSACTION (user1,$userParam) ON DEFAULT DATABASE TO role", Map("userParam" -> "user2"))

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(showTransaction("*")).database("foo").role("role").map,
      granted(showTransaction("*")).database("bar").role("role").map,
      granted(showTransaction("user1")).database(DEFAULT).role("role").map,
      granted(showTransaction("user2")).database(DEFAULT).role("role").map
    ))
  }

  test("should deny show transaction privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY SHOW TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE TO $r", Map("r" -> "role"))

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      denied(showTransaction("*")).database("foo").role("role").map,
      denied(showTransaction("user1")).database(DEFAULT).role("role").map,
      denied(showTransaction("user2")).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke show transaction privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT SHOW TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE SHOW TRANSACTION (*) ON DATABASE foo FROM role")
    execute("REVOKE SHOW TRANSACTION (user1,user2) ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(granted(transaction("*")).database("foo").role("role").map))
  }

  test("should grant terminate transaction privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT TERMINATE TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(terminateTransaction("*")).database("foo").role("role").map,
      granted(terminateTransaction("user1")).database(DEFAULT).role("role").map,
      granted(terminateTransaction("user2")).database(DEFAULT).role("role").map
    ))
  }

  test("should deny terminate transaction privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY TERMINATE TRANSACTION ON DATABASE foo TO role")
    execute("DENY TERMINATE TRANSACTION ON DATABASE $db TO role", Map("db" -> "bar"))
    execute("DENY TERMINATE TRANSACTION ($userParam,user2) ON DEFAULT DATABASE TO role", Map("userParam" -> "user1"))

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      denied(terminateTransaction("*")).database("foo").role("role").map,
      denied(terminateTransaction("*")).database("bar").role("role").map,
      denied(terminateTransaction("user1")).database(DEFAULT).role("role").map,
      denied(terminateTransaction("user2")).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke terminate transaction privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TERMINATE TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE TERMINATE TRANSACTION (*) ON DATABASE foo FROM role")
    execute("REVOKE TERMINATE TRANSACTION (user1,user2) ON DEFAULT DATABASE FROM $r", Map("r" -> "role"))

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(granted(transaction("*")).database("foo").role("role").map))
  }

  test("should grant transaction management privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TRANSACTION ON DEFAULT DATABASE TO $r", Map("r" -> "role"))
    execute("GRANT TRANSACTION (user1,user2) ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(transaction("*")).database("foo").role("role").map,
      granted(transaction("*")).database(DEFAULT).role("role").map,
      granted(transaction("user1")).role("role").map,
      granted(transaction("user2")).role("role").map
    ))
  }

  test("should deny transaction management privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY TRANSACTION (*) ON DATABASE foo TO role")
    execute("DENY TRANSACTION ON DEFAULT DATABASE TO role")
    execute("DENY TRANSACTION (user1,user2) ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      denied(transaction("*")).database("foo").role("role").map,
      denied(transaction("*")).database(DEFAULT).role("role").map,
      denied(transaction("user1")).role("role").map,
      denied(transaction("user2")).role("role").map
    ))
  }

  test("should revoke transaction management privilege") {
    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")
    execute("GRANT SHOW TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TERMINATE TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TRANSACTION (*) ON DATABASE foo TO role")
    execute("GRANT TRANSACTION ON DATABASE bar TO role")
    execute("DENY TRANSACTION ON DEFAULT DATABASE TO role")
    execute("GRANT TRANSACTION (user1) ON DATABASE * TO role")
    execute("DENY TRANSACTION (user1) ON DATABASE * TO role")

    // WHEN
    execute("REVOKE TRANSACTION ON DATABASE foo FROM role")
    execute("REVOKE TRANSACTION ON DATABASE $db FROM role", Map("db" -> "bar"))
    execute("REVOKE TRANSACTION ON DEFAULT DATABASE FROM role")
    execute("REVOKE TRANSACTION ($userParam1,$userParam2) ON DATABASE * FROM role", Map("userParam1" -> "user1", "userParam2" -> "user2"))

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(showTransaction("*")).database("foo").role("role").map,
      granted(terminateTransaction("*")).database("foo").role("role").map
    ))
  }

  test("should grant and revoke transaction privileges on multiple databases") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    transactionPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command ON DATABASE foo, bar TO role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            granted(action).database("foo").role("role").map,
            granted(action).database("bar").role("role").map,
          ))

          // WHEN
          execute(s"REVOKE GRANT $command ON DATABASE foo, bar FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

    test("should grant and revoke transaction privileges on multiple databases with parameter") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    transactionPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command ON DATABASE $$dbParam1, $$dbParam2 TO role", Map("dbParam1" -> "foo", "dbParam2" -> "bar"))

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            granted(action).database("foo").role("role").map,
            granted(action).database("bar").role("role").map,
          ))

          // WHEN
          execute(s"REVOKE GRANT $command ON DATABASE foo, bar FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should deny and revoke transaction privileges on multiple databases") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    transactionPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command ON DATABASE foo, bar TO role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            denied(action).database("foo").role("role").map,
            denied(action).database("bar").role("role").map,
          ))

          // WHEN
          execute(s"REVOKE DENY $command ON DATABASE foo, bar FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }
}
