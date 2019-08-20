/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.io.File
import java.lang.Boolean.TRUE

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings
import com.neo4j.server.security.enterprise.systemgraph._
import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, default_database}
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.DatabaseAdministrationException
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.api.{DatabaseExistsException, DatabaseLimitReachedException, DatabaseNotFoundException}
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.SecureHasher
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor

import scala.collection.Map
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

//TODO: Reinstate ignored tests after database Id work is merged
class MultiDatabaseAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  private val onlineStatus = DatabaseStatus.Online.stringValue()
  private val offlineStatus = DatabaseStatus.Offline.stringValue()
  private val defaultConfig = Config.defaults( GraphDatabaseSettings.auth_enabled, TRUE )

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    setup(defaultConfig)

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "CREATE DATABASE foo" -> 1,
      "STOP DATABASE foo" -> 1,
      "START DATABASE foo" -> 1,
      "DROP DATABASE foo" -> 1
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
    setup(defaultConfig)

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
    result.toSet should be(Set(db("foo", offlineStatus)))

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
    setup(defaultConfig)
    execute("CREATE DATABASE FOO")

    // WHEN
    val result = execute("SHOW DATABASE FOO")

    // THEN
    result.toList should be(List(db("foo")))
  }

  test("should give nothing when showing a non-existing database") {
    // GIVEN
    setup(defaultConfig)
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
    setup(defaultConfig)
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
    setup(defaultConfig)

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
    setup(defaultConfig)
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
    setup( defaultConfig )

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(Map("name" -> DEFAULT_DATABASE_NAME, "status" -> onlineStatus)))
  }

  test("should show custom default database using show default database command") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))

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
    result.toSet should be(Set(Map("name" -> DEFAULT_DATABASE_NAME, "status" -> onlineStatus)))

    // GIVEN
    execute("CREATE database foo")
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DEFAULT DATABASE")

    // THEN
    result2.toSet should be(Set(Map("name" -> "foo", "status" -> onlineStatus)))
  }

  test("should fail when showing default database when not on system database") {
    setup(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    the [DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW DEFAULT DATABASE")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW DEFAULT DATABASE"
  }

  // Tests for creating databases

  test("should create database in systemdb") {
    setup(defaultConfig)

    // WHEN
    execute("CREATE DATABASE `f.o-o123`")

    // THEN
    val result = execute("SHOW DATABASE `f.o-o123`")
    result.toList should be(List(db("f.o-o123")))
  }

  test("should create database in systemdb when max number of databases is not reached") {

    val config = Config.defaults()
    config.set(CommercialEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(3L) )
    setup(config)
    execute("SHOW DATABASES").toList.size should equal(2)

    // WHEN
    execute("CREATE DATABASE `foo`")

    // THEN
    val result = execute("SHOW DATABASE `foo`")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))
  }

  test("should fail to create database in systemdb when max number of databases is already reached") {

    val config = Config.defaults()
    config.set(CommercialEditionSettings.maxNumberOfDatabases, java.lang.Long.valueOf(2L) )
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
    setup(defaultConfig)

    // WHEN
    execute("CREATE DATABASE FoO")

    // THEN
    val result = execute("SHOW DATABASE fOo")
    result.toList should be(List(db("foo")))
  }

  ignore("should create default database on re-start after being dropped") {
    // GIVEN
    setup(defaultConfig)
    execute("CREATE ROLE custom")
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO joe")
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      read().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      traverse().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map
    ))

    // WHEN
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    the[RuntimeException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message s"No such database: $DEFAULT_DATABASE_NAME"

    // WHEN
    initSystemGraph(defaultConfig)
    // TODO: Create is not yet working since the new clustering reconciler - could this be a test setup or a race condition?
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B {name:'b'})")

    // THEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Read operations are not allowed for user 'joe' with roles [custom]."
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set.empty)
  }

  test("should have access on a created database") {
    // GIVEN
    setup(defaultConfig)
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

  ignore("should have no access on a re-created database") {
    // GIVEN
    setup(defaultConfig)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:A {name:'a'})")
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO joe")

    // WHEN
    execute("GRANT MATCH {*} ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      read().node("*").database("foo").user("joe").role("custom").map,
      traverse().node("*").database("foo").user("joe").role("custom").map
    ))
    executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name",
      resultHandler = (row, _) => row.get("n.name") should be("a")) should be(1)

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("DROP DATABASE foo")

    // THEN
    the[RuntimeException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "No such database: foo"

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    // TODO: Create is not yet working since the new clustering reconciler - could this be a test setup or a race condition?
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Read operations are not allowed for user 'joe' with roles [custom]."
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set.empty)
  }

  ignore("should have no access on a re-created default database"){
    // GIVEN
    setup(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO joe")

    // WHEN
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set(
      read().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      traverse().node("*").database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map
    ))
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name",
      resultHandler = (row, _) => row.get("n.name") should be("a")) should be(1)

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    the[RuntimeException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message s"No such database: $DEFAULT_DATABASE_NAME"

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE DATABASE $DEFAULT_DATABASE_NAME")
    // TODO: Create is not yet working since the new clustering reconciler - could this be a test setup or a race condition?
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B {name:'b'})")

    // THEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true)))
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Read operations are not allowed for user 'joe' with roles [custom]."
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail when creating an already existing database") {
    setup(defaultConfig)

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

  test("should fail when creating a database with invalid name") {
    setup(defaultConfig)
    selectDatabase(SYSTEM_DATABASE_NAME)

    // Empty name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      execute("CREATE DATABASE ``")
      // THEN
    } should have message "The provided database name is empty."

    // Starting on invalid character
    the[IllegalArgumentException] thrownBy {
      // WHEN
      execute("CREATE DATABASE _default")
      // THEN
    } should have message "Database name '_default' is not starting with an ASCII alphabetic character."

    // Has prefix 'system'
    the[IllegalArgumentException] thrownBy {
      // WHEN
      execute("CREATE DATABASE `system-mine`")
      // THEN
    } should have message "Database name 'system-mine' is invalid, due to the prefix 'system'."

    // Contains invalid characters
    the[IllegalArgumentException] thrownBy {
      // WHEN
      execute("CREATE DATABASE `myDbWith_and%`")
      // THEN
    } should have message
      "Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes."

    // Too short name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      execute("CREATE DATABASE me")
      // THEN
    } should have message "The provided database name must have a length between 3 and 63 characters."

    // Too long name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      val name = "ihaveallooootoflettersclearlymorethenishould-ihaveallooootoflettersclearlymorethenishould"
      execute(s"CREATE DATABASE `$name`")
      // THEN
    } should have message "The provided database name must have a length between 3 and 63 characters."
  }

  test("should fail when creating a database when not on system database") {
    setup(defaultConfig)
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
    setup(defaultConfig)

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

  test("should drop database using mixed case name") {
    // GIVEN
    setup(defaultConfig)
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
    setup(defaultConfig)
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
    the[RuntimeException] thrownBy {
      executeOn("baz", "foo", "bar", "MATCH (n) RETURN n")
    } should have message "No such database: baz"
  }

  test("should fail when dropping a non-existing database") {
    setup(defaultConfig)

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Failed to drop the specified database 'foo': Database does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE ``")
      // THEN
    } should have message "Failed to drop the specified database '': Database does not exist."
  }

  test("should fail when dropping a dropped database") {
    setup(defaultConfig)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASES")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Failed to drop the specified database 'foo': Database does not exist."
  }

  test("should fail on dropping system database") {
    setup(defaultConfig)

    // GIVEN
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"DROP DATABASE $SYSTEM_DATABASE_NAME")
      // THEN
    } should have message "Not allowed to drop system database."

    // WHEN
    val result = execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME")

    // THEN
    result.toSet should be(Set(db(SYSTEM_DATABASE_NAME)))
  }

  test("should drop default database") {
    setup(defaultConfig)
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
    setup(defaultConfig)
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
    setup(defaultConfig)

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))
  }

  test("should fail when starting a non-existing database") {
    setup(defaultConfig)

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
    setup(defaultConfig)

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
    setup(defaultConfig)

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
    setup(defaultConfig)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo"))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", offlineStatus))) // and stopped

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(db("foo")))
  }

  test("should re-start database using mixed case name") {
    setup(defaultConfig)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo"))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", offlineStatus))) // and stopped

    // WHEN
    execute("START DATABASE FOO")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(db("foo")))
  }

  test("should have access on a re-started database") {
    // GIVEN
    setup(defaultConfig)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo", offlineStatus)))
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
    setup(defaultConfig)
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
    setup(defaultConfig)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", offlineStatus)))
  }

  test("should stop database using mixed case name") {
    setup(defaultConfig)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))

    // WHEN
    execute("STOP DATABASE FoO")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", offlineStatus)))
  }

  test("should have no access on a stopped database") {
    // GIVEN
    setup(defaultConfig)
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
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz", offlineStatus)))

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
    setup(defaultConfig)

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
    setup(defaultConfig)

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
    setup(defaultConfig)

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
    setup(defaultConfig)

    // WHEN
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, offlineStatus, default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should stop custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("foo", offlineStatus, default = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should have no access on a stopped default database") {
    // GIVEN
    setup(defaultConfig)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("foo", Seq("editor"), passwordChangeRequired = false))
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true), db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, offlineStatus, default = true)))

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
    setup(defaultConfig)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo", offlineStatus)))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", offlineStatus)))
  }

  test("should fail when stopping a database when not on system database") {
    setup(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: STOP DATABASE"
  }

  private def db(name: String, status: String = onlineStatus, default: Boolean = false) =
    Map("name" -> name, "status" -> status, "default" -> default)

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}

  private def setup(config: Config): Unit = {
    managementService = graphDatabaseFactory(new File("test")).impermanent().setConfig(config).setInternalLogProvider(logProvider).build()
    graphOps = managementService.database(SYSTEM_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
    databaseManager = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

    initSystemGraph(config)
  }

  private def initSystemGraph(config: Config): Unit = {
    val queryExecutor: ContextSwitchingSystemGraphQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager, threadToStatementContextBridge())
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(queryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new CommercialSystemGraphInitializer(databaseManager, config)
    val securityGraphInitializer = new EnterpriseSecurityGraphInitializer(systemGraphInitializer, queryExecutor, mock[Log], systemGraphOperations, importOptions, secureHasher)
    securityGraphInitializer.initializeSecurityGraph()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  private def threadToStatementContextBridge(): ThreadToStatementContextBridge = {
    graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }

  // Use the default value instead of the new value in DDLAcceptanceTestBase
  override def databaseConfig(): Map[Setting[_], Object] = Map()
}
