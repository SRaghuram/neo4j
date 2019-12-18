/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, default_database}
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException

class MultiDatabasePrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  private val default = "DEFAULT"

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    setup()
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
    setup()
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

    // WHEN
    execute("DENY STOP ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      startDatabase().role("role").map,
      stopDatabase().database("foo").role("role").map,
      stopDatabase("DENIED").database(default).role("role").map
    ))
  }

  test("should list access database privilege") {
    // GIVEN
    setup()
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

    // WHEN
    execute("DENY ACCESS ON DEFAULT DATABASE TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).role("custom").map,
      access("DENIED").database(default).role("custom").map
    ))

    // WHEN
    execute("REVOKE GRANT ACCESS ON DEFAULT DATABASE FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).role("custom").map,
      access("DENIED").database(default).role("custom").map
    ))

    // WHEN
    execute(s"REVOKE DENY ACCESS ON DATABASE $DEFAULT_DATABASE_NAME FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).role("custom").map,
      access("DENIED").database(default).role("custom").map
    ))
  }

  test("should list database privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT ACCESS ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().database(default).role("role").map
    ))

    // WHEN
    execute("REVOKE GRANT ACCESS ON DEFAULT DATABASE FROM role")
    execute("DENY START ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      startDatabase("DENIED").database(default).role("role").map
    ))

    // WHEN
    execute("REVOKE DENY START ON DEFAULT DATABASE FROM role")
    execute("GRANT STOP ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      stopDatabase().database(default).role("role").map
    ))

    // WHEN
    execute("REVOKE STOP ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  // START DATABASE

  test("admin should be allowed to start database") {
    setup()
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
    setup()
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
    setup()
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
    setup()
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
    setup()
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
    setup()
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

  test("should have start database privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role", access = false)
    execute(s"CREATE database $newDefaultDatabase")
    execute(s"STOP database $newDefaultDatabase")
    execute(s"STOP database $DEFAULT_DATABASE_NAME")

    // Confirm database status
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, offlineStatus, default = true)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, offlineStatus)))

    // WHEN: Grant on default database
    execute(s"GRANT START ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(startDatabase().database(default).role("role").map))

    // WHEN: Starting the databases
    executeOnSystem("alice", "abc", s"START DATABASE $DEFAULT_DATABASE_NAME")

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("alice", "abc", s"START DATABASE $newDefaultDatabase")
    } should have message "Permission denied."

    // THEN: new status on default
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, onlineStatus, default = true)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, offlineStatus)))

    // WHEN: switch default database and stop both databases
    config.set(default_database, newDefaultDatabase)
    restart(config)
    execute(s"STOP database $newDefaultDatabase")
    execute(s"STOP database $DEFAULT_DATABASE_NAME")

    // Confirm database status
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, offlineStatus)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, offlineStatus, default = true)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(startDatabase().database(default).role("role").map))

    // WHEN: Starting the databases
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("alice", "abc", s"START DATABASE $DEFAULT_DATABASE_NAME")
    } should have message "Permission denied."

    executeOnSystem("alice", "abc", s"START DATABASE $newDefaultDatabase")

    // THEN: new status on new default, but not the old
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, offlineStatus)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, onlineStatus, default = true)))
  }

  // STOP DATABASE

  test("admin should be allowed to stop database") {
    setup()
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
    setup()
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
    setup()
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
    setup()
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
    setup()
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
    setup()
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

  test("should have stop database privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role", access = false)
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm database status
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, onlineStatus, default = true)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, onlineStatus)))

    // WHEN: Grant on default database
    execute(s"GRANT STOP ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(stopDatabase().database(default).role("role").map))

    // WHEN: Stopping the databases
    executeOnSystem("alice", "abc", s"STOP DATABASE $DEFAULT_DATABASE_NAME")

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("alice", "abc", s"STOP DATABASE $newDefaultDatabase")
    } should have message "Permission denied."

    // THEN: new status on default
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, offlineStatus, default = true)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, onlineStatus)))

    // WHEN: switch default database and start both databases
    config.set(default_database, newDefaultDatabase)
    restart(config)
    execute(s"START database $newDefaultDatabase")
    execute(s"START database $DEFAULT_DATABASE_NAME")

    // Confirm database status
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, onlineStatus)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, onlineStatus, default = true)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(stopDatabase().database(default).role("role").map))

    // WHEN: Stopping the databases
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("alice", "abc", s"STOP DATABASE $DEFAULT_DATABASE_NAME")
    } should have message "Permission denied."

    executeOnSystem("alice", "abc", s"STOP DATABASE $newDefaultDatabase")

    // THEN: new status on new default, but not the old
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, onlineStatus)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, offlineStatus, default = true)))
  }

  // ACCESS DATABASE

  test("should be able to access database with grant privilege") {
    // GIVEN
    setup()
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
    setup()
    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"DENY ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  test("should not be able to access database without privilege") {
    // GIVEN
    setup()
    setupUserWithCustomRole(access = false)

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'joe' with roles [custom]."
  }

  test("should have access database privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role", access = false)
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm default database
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase)))

    // WHEN: Grant on default database
    execute(s"GRANT ACCESS ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(access().database(default).role("role").map))

    // WHEN & THEN: accessing the databases
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "MATCH (n) RETURN n") should be(0)

    the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'alice' with roles [role]."

    // WHEN: switch default database
    config.set(default_database, newDefaultDatabase)
    restart(config)

    // Confirm default database
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME)))
    execute(s"SHOW DATABASE $newDefaultDatabase").toSet should be(Set(db(newDefaultDatabase, default = true)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(access().database(default).role("role").map))

    // WHEN & THEN: accessing the databases
    the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "MATCH (n) RETURN n")
    } should have message "Database access is not allowed for user 'alice' with roles [role]."

    executeOn(newDefaultDatabase, "alice", "abc", "MATCH (n) RETURN n") should be(0)
  }

  // REDUCED ADMIN

  Seq(
    ("without traverse, read and write privileges", testAdminWithoutBasePrivileges _),
    ("with only user, role, database and access control privileges", testAdminWithoutAllRemovablePrivileges _)
  ).foreach {
    case (partialName, testMethod) =>
      test(s"Test role copied from admin $partialName") {
        // WHEN
        setup()
        execute("CREATE ROLE custom AS COPY OF admin")
        execute("CREATE USER Alice SET PASSWORD 'oldSecret' CHANGE NOT REQUIRED")
        execute("GRANT ROLE custom TO Alice")

        // THEN
        testMethod("custom", 3)
      }

      test(s"Test admin $partialName") {
        // WHEN
        setup()
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

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}

  // Use the default value instead of the new value in AdministrationCommandAcceptanceTestBase
  override def databaseConfig(): Map[Setting[_], Object] = Map()
}
