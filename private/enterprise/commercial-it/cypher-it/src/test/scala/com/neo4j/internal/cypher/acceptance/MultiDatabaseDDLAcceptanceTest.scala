/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.io.File

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings
import com.neo4j.server.security.enterprise.systemgraph._
import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME, default_database}
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.api.{DatabaseExistsException, DatabaseLimitReachedException, DatabaseNotFoundException}
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.config.{InvalidSettingException, Setting}
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.SecureHasher
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor

import scala.collection.Map

class MultiDatabaseDDLAcceptanceTest extends DDLAcceptanceTestBase {
  private val onlineStatus = DatabaseStatus.Online.stringValue()
  private val offlineStatus = DatabaseStatus.Offline.stringValue()
  private val defaultConfig = Config.defaults()
  private val databaseIdRepository = new TestDatabaseIdRepository()

  test("should fail at startup when config setting for default database name is invalid") {
    // GIVEN
    val config = Config.defaults()
    def startOfError( dbName: String ): String = s"Bad value '$dbName' for setting 'dbms.default_database': "

    // Empty name
    the[InvalidSettingException] thrownBy {
      // WHEN
      config.augment(default_database, "")
      // THEN
    } should have message (startOfError("") + "The provided database name is empty.")

    // Starting on invalid character
    the[InvalidSettingException] thrownBy {
      // WHEN
      config.augment(default_database, "_default")
      // THEN
    } should have message (startOfError("_default") + "Database name '_default' is not starting with an ASCII alphabetic character.")

    // Has prefix 'system'
    the[InvalidSettingException] thrownBy {
      // WHEN
      config.augment(default_database, "system-mine")
      // THEN
    } should have message (startOfError("system-mine") + "Database name 'system-mine' is invalid, due to the prefix 'system'.")

    // Contains invalid characters
    the[InvalidSettingException] thrownBy {
      // WHEN
      config.augment(default_database, "mydbwith_and%")
      // THEN
    } should have message (startOfError("mydbwith_and%") +
      "Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.")

    // Too short name
    the[InvalidSettingException] thrownBy {
      // WHEN
      config.augment(default_database, "me")
      // THEN
    } should have message (startOfError("me") + "The provided database name must have a length between 3 and 63 characters.")

    // Too long name
    val name = "ihaveallooootoflettersclearlymorethenishould-ihaveallooootoflettersclearlymorethenishould"
    the[InvalidSettingException] thrownBy {
      // WHEN
      config.augment(default_database, name)
      // THEN
    } should have message (startOfError(name) + "The provided database name must have a length between 3 and 63 characters.")
  }

  // Tests for showing databases

  test("should show database neo4j") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASE neo4j")

    // THEN
    result.toList should be(List(db("neo4j", default = true)))
  }

  test("should show custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(db("foo", default = true)))

    // WHEN
    val result2 = execute("SHOW DATABASE neo4j")

    // THEN
    result2.toList should be(empty)
  }

  test("should show database for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE database foo")

    // WHEN
    val neoResult = execute("SHOW DATABASE neo4j")
    val fooResult = execute("SHOW DATABASE foo")

    // THEN
    neoResult.toSet should be(Set(db("neo4j", default = true)))
    fooResult.toSet should be(Set(db("foo")))

    // GIVEN
    config.augment(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val neoResult2 = execute("SHOW DATABASE neo4j")
    val fooResult2 = execute("SHOW DATABASE foo")

    // THEN
    neoResult2.toSet should be(Set(db("neo4j")))
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
    config.augment(default_database, "foo")
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
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW DATABASE neo4j")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW DATABASE"
  }

  test("should show default databases") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db("neo4j", default = true), db("system")))
  }

  test("should show custom default and system databases") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db("foo", default = true), db("system")))
  }

  test("should show databases for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE database foo")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db("neo4j", default = true), db("foo"), db("system")))

    // GIVEN
    config.augment(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DATABASES")

    // THEN
    result2.toSet should be(Set(db("neo4j"), db("foo", default = true), db("system")))
  }

  test("should fail when showing databases when not on system database") {
    setup(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW DATABASES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW DATABASES"
  }

  test("should show default database") {
    // GIVEN
    setup( defaultConfig )

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j", "status" -> onlineStatus)))
  }

  test("should show custom default database using show default database command") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))

    // WHEN
    val result2 = execute("SHOW DATABASE neo4j")

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
    result.toSet should be(Set(Map("name" -> "neo4j", "status" -> onlineStatus)))

    // GIVEN
    execute("CREATE database foo")
    config.augment(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DEFAULT DATABASE")

    // THEN
    result2.toSet should be(Set(Map("name" -> "foo", "status" -> onlineStatus)))
  }

  test("should fail when showing default database when not on system database") {
    setup(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    the [DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW DEFAULT DATABASE")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW DEFAULT DATABASE"
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
    config.augment(CommercialEditionSettings.maxNumberOfDatabases, "3")
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
    config.augment(CommercialEditionSettings.maxNumberOfDatabases, "2")
    setup(config)
    execute("SHOW DATABASES").toList.size should equal(2)

    // WHEN
    the[DatabaseLimitReachedException] thrownBy {
      // WHEN
      execute("CREATE DATABASE `foo`")
      // THEN
    } should have message "The total limit of databases is already reached. To create more you need to either drop databases or change the limit via the config setting 'dbms.max_databases'"

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
    } should have message "The specified database 'foo' already exists."
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
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("CREATE DATABASE foo")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: CREATE DATABASE"
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
      db("neo4j", default = true), db("baz"), db("system")))

    // WHEN
    execute("DROP DATABASE baz")
    execute("SHOW DATABASES").toSet should be(Set(db("neo4j", default = true), db("system")))

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
    } should have message "Database 'foo' does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE ``")
      // THEN
    } should have message "Database '' does not exist."
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
    } should have message "Database 'foo' does not exist."
  }

  test("should fail on dropping system database") {
    setup(defaultConfig)

    // GIVEN
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("DROP DATABASE system")
      // THEN
    } should have message "Not allowed to drop system database."

    // WHEN
    val result = execute("SHOW DATABASE system")

    // THEN
    result.toSet should be(Set(db("system")))
  }

  test("should drop default database") {
    setup(defaultConfig)
    execute("SHOW DATABASES").toSet should be(Set(db("neo4j", default = true), db("system")))

    // WHEN
    execute("DROP DATABASE neo4j")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("system")))

    // WHEN
    execute("CREATE DATABASE neo4j")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("neo4j", default = true), db("system")))
  }

  test("should fail on dropping custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)
    execute("SHOW DATABASES").toSet should be(Set(db("foo", default = true), db("system")))

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("system")))
  }

  test("should fail when dropping a database when not on system database") {
    // GIVEN
    setup(defaultConfig)
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: DROP DATABASE"
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
    } should have message "Database 'foo' does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE ``")
      // THEN
    } should have message "Database '' does not exist."
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
    } should have message "Database 'foo' does not exist."

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

    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: START DATABASE"
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
      db("neo4j", default = true),
      db("baz"),
      db("system")))

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
    } should have message "Database 'foo' does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE ``")
      // THEN
    } should have message "Database '' does not exist."
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
    } should have message "Database 'foo' does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should fail on stopping system database") {
    setup(defaultConfig)

    // GIVEN
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("STOP DATABASE system")
      // THEN
    } should have message "Not allowed to stop system database."

    // WHEN
    val result = execute("SHOW DATABASE system")

    // THEN
    result.toSet should be(Set(db("system")))
  }

  test("should stop default database") {
    // GIVEN
    setup(defaultConfig)

    // WHEN
    execute("STOP DATABASE neo4j")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db("neo4j", offlineStatus, default = true), db("system")))
  }

  test("should stop custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("foo", offlineStatus, default = true), db("system")))
  }

  test("should have no access on a stopped default database") {
    // GIVEN
    setup(defaultConfig)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq("admin")),
      user("foo", Seq("editor"), passwordChangeRequired = false))
    execute("SHOW DATABASES").toSet should be(Set(db("neo4j", default = true), db("system")))

    // WHEN
    execute("STOP DATABASE neo4j")
    execute("SHOW DATABASE neo4j").toSet should be(Set(db("neo4j", offlineStatus, default = true)))

    // THEN
    the[DatabaseShutdownException] thrownBy {
      executeOnDefault("foo", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("START DATABASE neo4j")

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
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: STOP DATABASE"
  }

  private def db(name: String, status: String = onlineStatus, default: Boolean = false) =
    Map("name" -> name, "status" -> status, "default" -> default)

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}

  private def setup(config: Config): Unit = {
    managementService = graphDatabaseFactory(new File("test")).impermanent().setConfigRaw(config.getRaw).setInternalLogProvider(logProvider).build()
    graphOps = managementService.database(SYSTEM_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)

    initSystemGraph(config)
  }

  private def initSystemGraph(config: Config): Unit = {
    val queryExecutor: ContextSwitchingSystemGraphQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager, threadToStatementContextBridge(), databaseIdRepository)
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(queryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new CommercialSystemGraphInitializer(databaseManager, databaseIdRepository, config)
    val securityGraphInitializer = new EnterpriseSecurityGraphInitializer(systemGraphInitializer, queryExecutor, mock[Log], systemGraphOperations, importOptions, secureHasher)
    securityGraphInitializer.initializeSecurityGraph()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  private def threadToStatementContextBridge(): ThreadToStatementContextBridge = {
    graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }
}
