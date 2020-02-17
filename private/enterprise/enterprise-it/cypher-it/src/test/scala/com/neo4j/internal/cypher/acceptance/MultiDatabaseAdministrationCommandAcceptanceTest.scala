/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.default_database
import org.neo4j.dbms.api.DatabaseExistsException
import org.neo4j.dbms.api.DatabaseLimitReachedException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.DeadlockDetectedException
import org.neo4j.kernel.api.KernelTransaction

class MultiDatabaseAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    setup()

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "CREATE DATABASE foo" -> 1,
      "CREATE DATABASE foo2 IF NOT EXISTS" -> 1,
      "CREATE OR REPLACE DATABASE foo" -> 2,
      "CREATE OR REPLACE DATABASE foo3" -> 1,
      "STOP DATABASE foo" -> 1,
      "START DATABASE foo" -> 1,
      "DROP DATABASE foo" -> 1,
      "DROP DATABASE foo2 IF EXISTS" -> 1
    ))
  }

  test("should fail at startup when config setting for default database name is invalid") {
    // GIVEN
    val startOfError = "Error evaluating value for setting 'dbms.default_database'. "

    // Empty name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "")
      // THEN
    } should have message startOfError + "The provided database name is empty."

    // Starting on invalid characterN
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "_default")
      // THEN
    } should have message startOfError + "Database name '_default' is not starting with an ASCII alphabetic character."

    // Has prefix 'system'
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "system-mine")
      // THEN
    } should have message startOfError + "Database name 'system-mine' is invalid, due to the prefix 'system'."

    // Contains invalid characters
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "mydbwith_and%")
      // THEN
    } should have message startOfError + "Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes."

    // Too short name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "me")
      // THEN
    } should have message startOfError + "The provided database name must have a length between 3 and 63 characters."

    // Too long name
    val name = "ihaveallooootoflettersclearlymorethenishould-ihaveallooootoflettersclearlymorethenishould"
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, name)
      // THEN
    } should have message startOfError + "The provided database name must have a length between 3 and 63 characters."
  }

  // Tests for showing databases

  test(s"should show database $DEFAULT_DATABASE_NAME") {
    // GIVEN
    setup()

    // WHEN
    val result = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    result.toList should be(List(db(DEFAULT_DATABASE_NAME, default = true)))
  }

  test("should show custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(db("foo", default = true)))

    // WHEN
    val result2 = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    result2.toList should be(empty)
  }

  test("should show database for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE database foo")

    // WHEN
    val neoResult = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")
    val fooResult = execute("SHOW DATABASE foo")

    // THEN
    neoResult.toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    fooResult.toSet should be(Set(db("foo")))

    // GIVEN
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val neoResult2 = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")
    val fooResult2 = execute("SHOW DATABASE foo")

    // THEN
    neoResult2.toSet should be(Set(db(DEFAULT_DATABASE_NAME)))
    fooResult2.toSet should be(Set(db("foo", default = true)))
  }

  test("should start a stopped database when it becomes default") {
    // GIVEN
    val config = Config.defaults()
    setup(config)

    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toSet should be(Set(db("foo", status = offlineStatus)))

    // GIVEN
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DATABASE foo")

    // THEN
    // The new default database should be started when the system is restarted
    result2.toSet should be(Set(db("foo", default = true)))
  }

  test("should show database using mixed case name") {
    // GIVEN
    setup()
    execute("CREATE DATABASE FOO")

    // WHEN
    val result = execute("SHOW DATABASE FOO")

    // THEN
    result.toList should be(List(db("foo")))
  }

  test("should give nothing when showing a non-existing database") {
    // GIVEN
    setup()
    selectDatabase(SYSTEM_DATABASE_NAME)

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

  test("should fail when showing a database when not on system database") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW DATABASE"
  }

  test("should show default databases") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should show custom default and system databases") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db("foo", default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should show databases for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE database foo")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db("foo"), db(SYSTEM_DATABASE_NAME)))

    // GIVEN
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DATABASES")

    // THEN
    result2.toSet should be(Set(db(DEFAULT_DATABASE_NAME), db("foo", default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should fail when showing databases when not on system database") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW DATABASES")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW DATABASES"
  }

  test("should show default database") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(defaultDb(status = onlineStatus)))
  }

  test("should show custom default database using show default database command") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(defaultDb(name = "foo")))

    // WHEN
    val oldDefaultName = DEFAULT_DATABASE_NAME
    val result2 = execute(s"SHOW DATABASE $oldDefaultName")

    // THEN
    result2.toList should be(empty)
  }

  test("should show correct default database for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toSet should be(Set(defaultDb()))

    // GIVEN
    execute("CREATE database foo")
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DEFAULT DATABASE")

    // THEN
    result2.toSet should be(Set(defaultDb(name = "foo")))
  }

  test("should fail when showing default database when not on system database") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW DEFAULT DATABASE")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW DEFAULT DATABASE"
  }

  test("should fail when showing databases when credentials expired") {
    setup()
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("ALTER USER joe SET PASSWORD CHANGE REQUIRED")

    Seq(
      "SHOW DEFAULT DATABASE",
      "SHOW DATABASES",
      "SHOW DATABASE system"
    ).foreach {
      query =>
        the[AuthorizationViolationException] thrownBy {
          // WHEN
          executeOnSystem("joe", "soap", query)
          // THEN
        } should have message String.format("Permission denied." + PASSWORD_CHANGE_REQUIRED_MESSAGE)
    }
  }

  // Tests for creating databases

  test("should create database in systemdb") {
    setup()

    // WHEN
    execute("CREATE DATABASE `f.o-o123`")

    // THEN
    val result = execute("SHOW DATABASE `f.o-o123`")
    result.toList should be(List(db("f.o-o123")))
  }

  test("should create database using if not exists") {
    setup()

    // WHEN
    execute("CREATE DATABASE `f.o-o123` IF NOT EXISTS")

    // THEN
    val result = execute("SHOW DATABASE `f.o-o123`")
    result.toList should be(List(db("f.o-o123")))
  }

  test("should create database in systemdb when max number of databases is not reached") {

    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(3L))
    setup(config)
    execute("SHOW DATABASES").toList.size should equal(2)

    // WHEN
    execute("CREATE DATABASE `foo`")

    // THEN
    val result = execute("SHOW DATABASE `foo`")
    result.toList should be(List(db("foo")))
  }

  test("should fail to create database in systemdb when max number of databases is already reached") {

    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(2L))
    setup(config)
    execute("SHOW DATABASES").toList.size should equal(2)

    // WHEN
    the[DatabaseLimitReachedException] thrownBy {
      // WHEN
      execute("CREATE DATABASE `foo`")
      // THEN
    } should have message "Failed to create the specified database 'foo': The total limit of databases is already reached. " +
      "To create more you need to either drop databases or change the limit via the config setting 'dbms.max_databases'"

    // THEN
    execute("SHOW DATABASES").toList.size should equal(2)
  }

  test("should create database using mixed case name") {
    setup()

    // WHEN
    execute("CREATE DATABASE FoO")

    // THEN
    val result = execute("SHOW DATABASE fOo")
    result.toList should be(List(db("foo")))
  }

  test("should create default database on re-start after being dropped") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO joe")
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      read().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      traverse().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      access().database(DEFAULT).user("joe").role("PUBLIC").map
    ))

    // WHEN
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message s"$DEFAULT_DATABASE_NAME"

    // WHEN
    initSystemGraph(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B {name:'b'})")

    // THEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT).user("joe").role("PUBLIC").map
    ))
  }

  test("should have access on a created database") {
    // GIVEN
    setup()
    execute("CREATE USER baz SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO baz")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("baz", Seq("editor"), passwordChangeRequired = false))

    // WHEN
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo")))

    // THEN
    executeOn("foo", "baz", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should have no access on a re-created database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:A {name:'a'})")
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO joe")

    // WHEN
    execute("GRANT ACCESS ON DATABASE foo TO custom")
    execute("GRANT MATCH {*} ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      access().database("foo").user("joe").role("custom").map,
      read().node("*").database("foo").user("joe").role("custom").map,
      traverse().node("*").database("foo").user("joe").role("custom").map,
      access().database(DEFAULT).user("joe").role("PUBLIC").map
    ))
    executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name",
      resultHandler = (row, _) => row.get("n.name") should be("a")) should be(1)

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("DROP DATABASE foo")

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "foo"

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Database access is not allowed for user 'joe' with roles [PUBLIC, custom]."
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT).user("joe").role("PUBLIC").map
    ))
  }

  test("should have access on a re-created default database") {
    // GIVEN
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO joe")

    // WHEN
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      read().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      traverse().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      access().database(DEFAULT).user("joe").role("PUBLIC").map
    ))
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name",
      resultHandler = (row, _) => row.get("n.name") should be("a")) should be(1)

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message s"$DEFAULT_DATABASE_NAME"

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE DATABASE $DEFAULT_DATABASE_NAME")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B {name:'b'})")

    // THEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      access().database(DEFAULT).user("joe").role("PUBLIC").map
    ))
  }

  test("should fail when creating an already existing database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))

    the[DatabaseExistsException] thrownBy {
      // WHEN
      execute("CREATE DATABASE foo")
      // THEN
    } should have message "Failed to create the specified database 'foo': Database already exists."
  }

  test("should do nothing when creating an already existing database using if not exists") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))

    // WHEN
    execute("CREATE DATABASE foo IF NOT EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should do nothing when creating an already existing database using if not exists with max number of databases reached") {
    // GIVEN
    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(3L))
    setup(config)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))

    // WHEN
    execute("CREATE DATABASE foo IF NOT EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should fail when creating a database with invalid name") {
    setup()
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Empty name
    testCreateDbWithInvalidName("``", "The provided database name is empty.")

    // Starting on invalid character
    testCreateDbWithInvalidName("_default", "Database name '_default' is not starting with an ASCII alphabetic character.")

    // Has prefix 'system'
    testCreateDbWithInvalidName("`system-mine`", "Database name 'system-mine' is invalid, due to the prefix 'system'.")

    // Contains invalid characters
    testCreateDbWithInvalidName("`myDbWith_and%`",
      "Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.")

    // Too short name
    testCreateDbWithInvalidName("me", "The provided database name must have a length between 3 and 63 characters.")

    // Too long name
    val name = "ihaveallooootoflettersclearlymorethenishould_ihaveallooootoflettersclearlymorethenishould"
    testCreateDbWithInvalidName(name, "The provided database name must have a length between 3 and 63 characters.")
  }

  private def testCreateDbWithInvalidName(name: String, expectedErrorMessage: String): Unit = {

    val e = the[InvalidArgumentException] thrownBy {
      execute("CREATE DATABASE " + name)
    }

    e should have message expectedErrorMessage
    e.status().code().toString should be("Status.Code[Neo.ClientError.Statement.ArgumentError]")

  }

  test("should replace existing database (even with max number of databases reached)") {
    // GIVEN
    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(3L))
    setup(config)
    execute("SHOW DATABASES").toList.size should equal(2)

    // WHEN: creation
    execute("CREATE OR REPLACE DATABASE `foo`")
    execute("STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASE `foo`").toList should be(List(db("foo", status = offlineStatus)))
    execute("SHOW DATABASES").toList.size should equal(3)

    // WHEN: replacing
    execute("CREATE OR REPLACE DATABASE `foo`")

    // THEN
    execute("SHOW DATABASE `foo`").toList should be(List(db("foo")))
    execute("SHOW DATABASES").toList.size should equal(3)
  }

  test("should replace default database") {
    // GIVEN
    setup()
    execute("SHOW DATABASES").toList.size should equal(2)
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toList should be(List(db(DEFAULT_DATABASE_NAME, status = offlineStatus, default = true)))

    // WHEN
    execute(s"CREATE OR REPLACE DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toList should be(List(db(DEFAULT_DATABASE_NAME, default = true)))
    execute("SHOW DATABASES").toList.size should equal(2)
  }

  test("should fail to create database when max number of databases reached using replace") {
    // GIVEN
    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(2L))
    setup(config)
    execute("SHOW DATABASES").toList.size should equal(2)

    the[DatabaseLimitReachedException] thrownBy {
      // WHEN
      execute("CREATE OR REPLACE DATABASE `foo`")
      // THEN
    } should have message "Failed to create the specified database 'foo': The total limit of databases is already reached. " +
      "To create more you need to either drop databases or change the limit via the config setting 'dbms.max_databases'"

    // THEN
    execute("SHOW DATABASES").toList.size should equal(2)
    execute("SHOW DATABASE `foo`").toList should be(List.empty)
  }

  test("should get syntax exception when using both replace and if not exists") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE DATABASE foo IF NOT EXISTS")
    }

    // THEN
    exception.getMessage should include("Failed to create the specified database 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
  }

  test("should fail when creating a database when not on system database") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("CREATE DATABASE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: CREATE DATABASE"
  }

  // Tests for dropping databases

  test("should create and drop databases") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE DATABASE baz")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toList should contain allOf(db("foo"), db("bar"), db("baz"))

    // WHEN
    execute("DROP DATABASE baz") //online database
    execute("STOP DATABASE bar")
    execute("DROP DATABASE bar") //offline database

    // THEN
    val result2 = execute("SHOW DATABASES")
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should contain("foo")
    databaseNames should not contain allOf("bar", "baz")

    // WHEN
    execute("CREATE DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toList should contain allOf(db("foo"), db("bar"))
  }

  test("should drop existing database using if exists") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASE foo").toSet should be(Set(db("foo")))

    // WHEN
    execute("DROP DATABASE foo IF EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)
  }

  test("should drop database using mixed case name") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASES").toList should contain(db("foo"))

    // WHEN
    execute("DROP DATABASE FOO")

    // THEN
    val result2 = execute("SHOW DATABASES")
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should not contain "foo"
  }

  test("should have no access on a dropped database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("foo", Seq("editor"), passwordChangeRequired = false))
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true), db("baz"), db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute("DROP DATABASE baz")
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db(SYSTEM_DATABASE_NAME)))

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOn("baz", "foo", "bar", "MATCH (n) RETURN n")
    } should have message "baz"
  }

  test("should fail when dropping a non-existing database") {
    setup()

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Failed to delete the specified database 'foo': Database does not exist."

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE ``")
      // THEN
    } should have message "Failed to delete the specified database '': Database does not exist."

    // THEN
    execute("SHOW DATABASE ``").toSet should be(Set.empty)
  }

  test("should do nothing when dropping a non-existing database using if exists") {
    setup()

    // WHEN
    execute("DROP DATABASE foo IF EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)

    // and an invalid (non-existing) one

    // WHEN
    execute("DROP DATABASE `` IF EXISTS")

    // THEN
    execute("SHOW DATABASE ``").toSet should be(Set.empty)
  }

  test("should fail when dropping a dropped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")
    execute("SHOW DATABASE foo").toSet should be(Set.empty)

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Failed to delete the specified database 'foo': Database does not exist."

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)
  }

  test("should do nothing when dropping a dropped database using if exists") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")
    execute("SHOW DATABASE foo").toSet should be(Set.empty)

    // WHEN
    execute("DROP DATABASE foo IF EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)
  }

  test("should fail on dropping system database") {
    setup()

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"DROP DATABASE $SYSTEM_DATABASE_NAME")
      // THEN
    } should have message "Not allowed to delete system database."

    // THEN
    execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    // GIVEN
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"DROP DATABASE $SYSTEM_DATABASE_NAME IF EXISTS")
      // THEN
    } should have message "Not allowed to delete system database."

    // THEN
    execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))
  }

  test("should drop default database") {
    setup()
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute(s"CREATE DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should drop custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("SHOW DATABASES").toSet should be(Set(db("foo", default = true), db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("foo", default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should fail when dropping a database when not on system database") {
    // GIVEN
    setup()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: DROP DATABASE"
  }

  // Tests for starting databases

  test("should start database on create") {
    setup()

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))
  }

  test("should fail when starting a non-existing database") {
    setup()

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Failed to start the specified database 'foo': Database does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE ``")
      // THEN
    } should have message "Failed to start the specified database '': Database does not exist."
  }

  test("should fail when starting a dropped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Failed to start the specified database 'foo': Database does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should be able to start a started database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo")))
  }

  test("should re-start database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo"))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", status = offlineStatus))) // and stopped

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(db("foo")))
  }

  test("should re-start database using mixed case name") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo"))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", status = offlineStatus))) // and stopped

    // WHEN
    execute("START DATABASE FOO")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(db("foo")))
  }

  test("should have access on a re-started database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))
    execute("CREATE USER baz SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO baz")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("baz", Seq("editor"), passwordChangeRequired = false))

    the[DatabaseShutdownException] thrownBy {
      executeOn("foo", "baz", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("START DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo")))

    // THEN
    executeOn("foo", "baz", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should fail when starting a database when not on system database") {
    // GIVEN
    setup()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: START DATABASE"
  }

  // Tests for stopping databases

  test("should stop database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should stop database using mixed case name") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))

    // WHEN
    execute("STOP DATABASE FoO")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should have no access on a stopped database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("foo", Seq("editor"), passwordChangeRequired = false))
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, default = true),
      db("baz"),
      db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute("STOP DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz", status = offlineStatus)))

    // THEN
    the[DatabaseShutdownException] thrownBy {
      executeOn("baz", "foo", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("START DATABASE baz")

    // THEN
    executeOn("baz", "foo", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should fail when stopping a non-existing database") {
    setup()

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Failed to stop the specified database 'foo': Database does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE ``")
      // THEN
    } should have message "Failed to stop the specified database '': Database does not exist."
  }

  test("should fail when stopping a dropped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Failed to stop the specified database 'foo': Database does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should fail on stopping system database") {
    setup()

    // GIVEN
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"STOP DATABASE $SYSTEM_DATABASE_NAME")
      // THEN
    } should have message "Not allowed to stop system database."

    // WHEN
    val result = execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME")

    // THEN
    result.toSet should be(Set(db(SYSTEM_DATABASE_NAME)))
  }

  test("should stop default database") {
    // GIVEN
    setup()

    // WHEN
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, status = offlineStatus, default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should stop custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("foo", status = offlineStatus, default = true),
      db(SYSTEM_DATABASE_NAME)))
  }

  test("should have no access on a stopped default database") {
    // GIVEN
    setup()
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("foo", Seq("editor"), passwordChangeRequired = false))
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, status = offlineStatus, default = true)))

    // THEN
    the[DatabaseShutdownException] thrownBy {
      executeOnDefault("foo", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"START DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should be able to stop a stopped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo", status = offlineStatus)))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should fail when stopping a database when not on system database") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: STOP DATABASE"
  }

  test("should pass through deadlock exception") {
    val secondNodeLocked = new CountDownLatch(1)

    class StopDb2AndThenDb1 extends Runnable {
      override def run(): Unit = {
        val tx = graph.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
        tx.execute("STOP DATABASE db2")
        secondNodeLocked.countDown()
        tx.execute("STOP DATABASE db1")
      }
    }

    setup(defaultConfig)
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE db1")
    execute("CREATE DATABASE db2")

    val tx = graph.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    // take lock on db1 node
    tx.execute("STOP DATABASE db1")

    // take lock on db2 and wait for a lock on db1 in other transaction
    new Thread(new StopDb2AndThenDb1).start()

    // and wait for those locks to be pending
    secondNodeLocked.await(10, TimeUnit.SECONDS)
    Thread.sleep(1000)

    // try to take lock on db2
    assertThrows[DeadlockDetectedException] {
      tx.execute("STOP DATABASE db2")
    }
  }

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}

  // Use the default value instead of the new value in AdministrationCommandAcceptanceTestBase
  override def databaseConfig(): Map[Setting[_], Object] = Map()
}
