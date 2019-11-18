/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.graphdb.security.AuthorizationViolationException

import scala.collection.Map

class MultiDatabasePrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT ACCESS ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT ACCESS ON DATABASE * FROM custom" -> 1,
      "DENY ACCESS ON DATABASE * TO custom" -> 1,
      "REVOKE DENY ACCESS ON DATABASE * FROM custom" -> 1,
      "GRANT ACCESS ON DATABASE * TO custom" -> 1,
      "DENY ACCESS ON DATABASE * TO custom" -> 1,
      "REVOKE ACCESS ON DATABASE * FROM custom" -> 2,

      "GRANT START ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT START ON DATABASE * FROM custom" -> 1,
      "DENY START ON DATABASE * TO custom" -> 1,
      "REVOKE DENY START ON DATABASE * FROM custom" -> 1,
      "GRANT START ON DATABASE * TO custom" -> 1,
      "DENY START ON DATABASE * TO custom" -> 1,
      "REVOKE START ON DATABASE * FROM custom" -> 2,

      "GRANT STOP ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT STOP ON DATABASE * FROM custom" -> 1,
      "DENY STOP ON DATABASE * TO custom" -> 1,
      "REVOKE DENY STOP ON DATABASE * FROM custom" -> 1,
      "GRANT STOP ON DATABASE * TO custom" -> 1,
      "DENY STOP ON DATABASE * TO custom" -> 1,
      "REVOKE STOP ON DATABASE * FROM custom" -> 2
    ))
  }

  test("should list start and stop database privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT START ON DATABASE foo TO role")
    execute("GRANT STOP ON DATABASE foo TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      startDatabase().database("foo").role("role").map,
      stopDatabase().database("foo").role("role").map
    ))

    // WHEN
    execute("REVOKE START ON DATABASE foo FROM role")
    execute("GRANT START ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      startDatabase().role("role").map,
      stopDatabase().database("foo").role("role").map
    ))

    // WHEN
    execute("DENY START ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      startDatabase().role("role").map,
      stopDatabase().database("foo").role("role").map,
      startDatabase("DENIED").database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE START ON DATABASE bar FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      startDatabase().role("role").map,
      stopDatabase().database("foo").role("role").map
    ))
  }

  test("should list access database privilege") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).role("custom").map
    ))

    // WHEN
    execute(s"DENY ACCESS ON DATABASE $SYSTEM_DATABASE_NAME TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).role("custom").map,
      access("DENIED").database(SYSTEM_DATABASE_NAME).role("custom").map
    ))

    // WHEN
    execute(s"REVOKE ACCESS ON DATABASE $SYSTEM_DATABASE_NAME FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).role("custom").map
    ))
  }

  // START DATABASE

  test("admin should be allowed to start database") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")

    // WHEN
    executeOnSystem("alice", "abc", "START DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("should fail to start database without privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "START DATABASE foo")
      // THEN
    } should have message "Permission denied."
  }

  test("should start database with privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("STOP DATABASE foo")
    execute("STOP DATABASE bar")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")
    execute("GRANT START ON DATABASE * TO role")

    // WHEN
    executeOnSystem("alice", "abc", "START DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db("bar", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )

    // WHEN
    executeOnSystem("alice", "abc", "START DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db("bar", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("should only start named database with privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("STOP DATABASE foo")
    execute("STOP DATABASE bar")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")
    execute("GRANT START ON DATABASE bar TO role")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "START DATABASE foo")
      // THEN
    } should have message "Permission denied."

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db("bar", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )

    // WHEN
    executeOnSystem("alice", "abc", "START DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db("bar", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("start database should not imply stop privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")
    execute("GRANT START ON DATABASE * TO role")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "STOP DATABASE foo")
      // THEN
    } should have message "Permission denied."

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("should only start database if not denied") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("STOP DATABASE foo")
    execute("STOP DATABASE bar")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")
    execute("DENY START ON DATABASE foo TO admin")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "START DATABASE foo")
      // THEN
    } should have message "Permission denied."

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db("bar", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )

    // WHEN
    executeOnSystem("alice", "abc", "START DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db("bar", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  // STOP DATABASE

  test("admin should be allowed to stop database") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")

    // WHEN
    executeOnSystem("alice", "abc", "STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("stop database should not imply start privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")
    execute("GRANT STOP ON DATABASE * TO role")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "START DATABASE foo")
      // THEN
    } should have message "Permission denied."

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("should fail to stop database without privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "STOP DATABASE foo")
      // THEN
    } should have message "Permission denied."
  }

  test("should stop database with privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")
    execute("GRANT STOP ON DATABASE * TO role")

    // WHEN
    executeOnSystem("alice", "abc", "STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db("bar", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )

    // WHEN
    executeOnSystem("alice", "abc", "STOP DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", offlineStatus),
      db("bar", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("should only stop named database with privilege") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO alice")
    execute("GRANT STOP ON DATABASE bar TO role")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "STOP DATABASE foo")
      // THEN
    } should have message "Permission denied."

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db("bar", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )

    // WHEN
    executeOnSystem("alice", "abc", "STOP DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db("bar", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  test("should only stop database if not denied") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")
    execute("GRANT ROLE admin TO alice")
    execute("DENY STOP ON DATABASE foo TO admin")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnSystem("alice", "abc", "STOP DATABASE foo")
      // THEN
    } should have message "Permission denied."

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db("bar", onlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )

    // WHEN
    executeOnSystem("alice", "abc", "STOP DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("foo", onlineStatus),
      db("bar", offlineStatus),
      db(SYSTEM_DATABASE_NAME))
    )
  }

  // ACCESS DATABASE

  test("should be able to access database with grant privilege") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n") should be(0)
  }

  test("should not be able to access database with deny privilege") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"DENY ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n") should be(0)
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  test("should not be able to access database without privilege") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n") should be(0)
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  private val onlineStatus = DatabaseStatus.Online.stringValue()
  private val offlineStatus = DatabaseStatus.Offline.stringValue()

  private def db(name: String, status: String = onlineStatus, default: Boolean = false) =
    Map("name" -> name,
      "address" -> "localhost:7687",
      "role" -> "standalone",
      "requestedStatus" -> status,
      "currentStatus" -> status,
      "error" -> "",
      "default" -> default)
}
