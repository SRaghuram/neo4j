/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.graphdb.QueryExecutionException
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
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  test("should not be able to access database with grant and deny privilege") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"DENY ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  test("should not be able to access database without privilege") {
    // GIVEN
    setupUserWithCustomRole(access = false)

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  // REDUCED ADMIN

  Seq(
    ("without traverse, read and write privileges", testAdminWithoutBasePrivileges _),
    ("with only user, role, database and access control privileges", testAdminWithoutAllRemovablePrivileges _)
  ).foreach {
    case (partialName, testMethod) =>
      test(s"Test role copied from admin $partialName") {
        // WHEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom AS COPY OF admin")
        execute("CREATE USER Alice SET PASSWORD 'oldSecret' CHANGE NOT REQUIRED")
        execute("GRANT ROLE custom TO Alice")

        // THEN
        testMethod("custom", 3)
      }

      test(s"Test admin $partialName") {
        // WHEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE USER Alice SET PASSWORD 'oldSecret' CHANGE NOT REQUIRED")
        execute("GRANT ROLE admin TO Alice")

        // THEN
        testMethod("admin", 2)
      }
  }

  private def testAdminWithoutAllRemovablePrivileges(role: String, populatedRoles: Int): Unit = {
    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute(s"REVOKE TRAVERSE ON GRAPH * FROM $role")
    execute(s"REVOKE READ {*} ON GRAPH * FROM $role")
    execute(s"REVOKE WRITE ON GRAPH * FROM $role")
    execute(s"DENY ALL ON DATABASE * TO $role") // have to deny since we can't revoke compound privileges
    execute(s"REVOKE DENY ACCESS ON DATABASE * FROM $role") // undo the deny from the line above

    // THEN
    testAlwaysAllowedForAdmin(populatedRoles)

    // create tokens
    the[QueryExecutionException] thrownBy {
      executeOnDefault("Alice", "secret", "CALL db.createLabel('Label')")
    } should have message s"'create_label' operations are not allowed for user 'Alice' with roles [$role] restricted to TOKEN_WRITE."

    // index management
    execute("CALL db.createLabel('Label')")
    execute("CALL db.createProperty('prop')")
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("Alice", "secret", "CREATE INDEX FOR (n:Label) ON (n.prop)")
    } should have message s"Schema operation 'create_index' is not allowed for user 'Alice' with roles [$role]."

    // constraint management
    execute("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT exists(n.prop)")
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("Alice", "secret", "DROP CONSTRAINT my_constraint")
    } should have message s"Schema operation 'drop_constraint' is not allowed for user 'Alice' with roles [$role]."

    // write
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("Alice", "secret", "CREATE (n:Label {prop: 'value'})")
    } should have message s"Write operations are not allowed for user 'Alice' with roles [$role]."

    // read/traverse
    execute("CREATE (n:Label {prop: 'value'})")
    executeOnDefault("Alice", "secret", "MATCH (n:Label) RETURN n.prop") should be(0)

    // stop database
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("Alice", "secret", "STOP DATABASE foo")
    } should have message "Permission denied."

    // start database
    execute("STOP DATABASE foo")
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("Alice", "secret", "START DATABASE foo")
    } should have message "Permission denied."
  }

  private def testAdminWithoutBasePrivileges(role: String, populatedRoles: Int): Unit = {
    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute(s"REVOKE TRAVERSE ON GRAPH * FROM $role")
    execute(s"REVOKE READ {*} ON GRAPH * FROM $role")
    execute(s"REVOKE WRITE ON GRAPH * FROM $role")

    // THEN
    testAlwaysAllowedForAdmin(populatedRoles)

    // create tokens
    executeOnDefault("Alice", "secret", "CALL db.createLabel('Label')")

    // index management
    executeOnDefault("Alice", "secret", "CREATE INDEX FOR (n:Label) ON (n.prop)") should be(0)
    graph.getMaybeIndex("Label", Seq("prop")).isDefined should be(true)

    // constraint management
    execute("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT exists(n.prop)")
    executeOnDefault("Alice", "secret", "DROP CONSTRAINT my_constraint") should be(0)
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)

    // write
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("Alice", "secret", "CREATE (n:Label {prop: 'value'})")
    } should have message s"Write operations are not allowed for user 'Alice' with roles [$role]."

    // read/traverse
    execute("CREATE (n:Label {prop: 'value'})")
    executeOnDefault("Alice", "secret", "MATCH (n:Label) RETURN n.prop") should be(0)

    // stop database
    executeOnSystem("Alice", "secret", "STOP DATABASE foo") should be(0)
    execute("SHOW DATABASE foo").toList should be(Seq(db("foo", offlineStatus)))

    // start database
    executeOnSystem("Alice", "secret", "START DATABASE foo") should be(0)
    execute("SHOW DATABASE foo").toList should be(Seq(db("foo", onlineStatus)))
  }

  private def testAlwaysAllowedForAdmin(populatedRoles: Int): Unit = {
    // create and alter users
    executeOnSystem("Alice", "oldSecret", "ALTER CURRENT USER SET PASSWORD FROM 'oldSecret' TO 'secret'")
    executeOnSystem("Alice", "secret", "CREATE USER Bob SET PASSWORD 'notSecret'")
    executeOnSystem("Alice", "secret", "ALTER USER Bob SET PASSWORD 'newSecret'")
    executeOnSystem("Alice", "secret", "SHOW USERS") should be(3)

    // create and granting roles
    executeOnSystem("Alice", "secret", "CREATE ROLE mine")
    executeOnSystem("Alice", "secret", "GRANT ROLE mine TO Bob")
    executeOnSystem("Alice", "secret", "SHOW POPULATED ROLES") should be(populatedRoles)

    // create dbs
    executeOnSystem("Alice", "secret", "CREATE DATABASE bar")
    executeOnSystem("Alice", "secret", "SHOW DATABASES") should be(4)

    // granting/denying/revoking privileges
    executeOnSystem("Alice", "secret", "GRANT ACCESS ON DATABASE bar TO mine")
    executeOnSystem("Alice", "secret", "DENY TRAVERSE ON GRAPH bar RELATIONSHIPS * TO mine")
    executeOnSystem("Alice", "secret", "SHOW ROLE mine PRIVILEGES") should be(2)
    executeOnSystem("Alice", "secret", "REVOKE GRANT ACCESS ON DATABASE bar FROM mine")
    executeOnSystem("Alice", "secret", "REVOKE TRAVERSE ON GRAPH bar FROM mine")

    // Revoking roles, dropping users/roles/dbs
    executeOnSystem("Alice", "secret", "REVOKE ROLE mine FROM Bob")
    executeOnSystem("Alice", "secret", "DROP ROLE mine")
    executeOnSystem("Alice", "secret", "DROP USER Bob")
    executeOnSystem("Alice", "secret", "DROP DATABASE bar")
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
