/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.systemgraph._
import org.neo4j.configuration.GraphDatabaseSettings.default_database
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.dbms.database.{DatabaseExistsException, DatabaseNotFoundException}
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.SecureHasher
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor
import org.parboiled.errors.ParserRuntimeException

class MultiDatabaseDDLAcceptanceTest extends DDLAcceptanceTestBase {
  private val onlineStatus = DatabaseStatus.Online.stringValue()
  private val offlineStatus = DatabaseStatus.Offline.stringValue()
  private val defaultConfig = Config.defaults()

  test("should list default database") {
    // GIVEN
    setup( defaultConfig )

    // WHEN
    val result = execute("SHOW DATABASE neo4j")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j", "status" -> onlineStatus, "default" -> true)))
  }

  test("should list custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> true)))

    // WHEN
    val result2 = execute("SHOW DATABASE neo4j")

    // THEN
    result2.toList should be(empty)
  }

  test("should give nothing when listing a non-existing database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

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

  test("should fail on listing a default database when not on system database") {
    the [IllegalStateException] thrownBy {
      // WHEN
      execute("SHOW DATABASE neo4j")
      // THEN
    } should have message "Trying to run `CATALOG SHOW DATABASE` against non-system database."
  }

  test("should list default databases") {
    // GIVEN
    setup( defaultConfig )

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(
      Map("name" -> "neo4j", "status" -> onlineStatus, "default" -> true),
      Map("name" -> "system", "status" -> onlineStatus, "default" -> false)))
  }

  test("should list custom default and system databases") {
    // GIVEN
    val config = Config.defaults()
    config.augment(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(
      Map("name" -> "foo", "status" -> onlineStatus, "default" -> true),
      Map("name" -> "system", "status" -> onlineStatus, "default" -> false)))
  }

  test("should fail on listing default databases when not on system database") {
    the [IllegalStateException] thrownBy {
      // WHEN
      execute("SHOW DATABASES")
      // THEN
    } should have message "Trying to run `CATALOG SHOW DATABASES` against non-system database."
  }

  test("should create database in systemdb") {
    setup( defaultConfig )

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))
  }

  test("should fail on creating an already existing database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))

    the [DatabaseExistsException] thrownBy {
      // WHEN
      execute("CREATE DATABASE foo")
      // THEN
    } should have message "The specified database 'foo' already exists."
  }

  test("should fail on creating a database with invalid name") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    val exception = the [ParserRuntimeException] thrownBy {
      // WHEN
      execute("CREATE DATABASE ``")
    }
    // THEN
    exception.getMessage should include("The provided database name is empty.")
  }

  test("should fail on creating a database when not on system database") {
    the [IllegalStateException] thrownBy {
      // WHEN
      execute("CREATE DATABASE foo")
      // THEN
    } should have message "Trying to run `CATALOG CREATE DATABASE` against non-system database."
  }

  test("should create and drop databases") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE DATABASE baz")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toList should contain allOf(
      Map("name" -> "foo", "status" -> onlineStatus, "default" -> false),
      Map("name" -> "bar", "status" -> onlineStatus, "default" -> false),
      Map("name" -> "baz", "status" -> onlineStatus, "default" -> false)
    )

    // WHEN
    execute("DROP DATABASE baz") //online database
    execute("STOP DATABASE bar")
    execute("DROP DATABASE bar") //offline database


    // THEN
    val result2 = execute("SHOW DATABASES")
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should contain("foo")
    databaseNames should not contain allOf("bar", "baz")
  }

  test("should fail on dropping a non-existing database") {
    setup( defaultConfig )

    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Database 'foo' does not exist."

    // and an invalid (non-existing) one
    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE ``")
      // THEN
    } should have message "Database '' does not exist."
  }

  test("should fail on dropping a dropped database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASES")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Database 'foo' does not exist."
  }

  test("should fail on dropping a database when not on system database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the [IllegalStateException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Trying to run `CATALOG DROP DATABASE` against non-system database."
  }

  test("should start database on create") {
    setup( defaultConfig )

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))
  }

  test("should fail on starting a non-existing database") {
    setup( defaultConfig )

    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Database 'foo' does not exist."

    // and an invalid (non-existing) one
    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE ``")
      // THEN
    } should have message "Database '' does not exist."
  }

  test("should fail on starting a dropped database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Database 'foo' does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should be able to start a started database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))
  }

  test("should re-start database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus, "default" -> false))) // and stopped

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))
  }

  test("should fail on starting a database when not on system database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the [IllegalStateException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Trying to run `CATALOG START DATABASE` against non-system database."
  }

  test("should stop database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus, "default" -> false)))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus, "default" -> false)))
  }

  test("should fail on stopping a non-existing database") {
    setup( defaultConfig )

    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Database 'foo' does not exist."

    // and an invalid (non-existing) one
    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE ``")
      // THEN
    } should have message "Database '' does not exist."
  }

  test("should fail on stopping a dropped database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the [DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Database 'foo' does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should fail on stopping a database when not on system database") {
    the [IllegalStateException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Trying to run `CATALOG STOP DATABASE` against non-system database."
  }

  test("should be able to stop a stopped database") {
    setup( defaultConfig )

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus, "default" -> false)))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus, "default" -> false)))
  }

  protected def setup(config: Config): Unit = {
    val queryExecutor: ContextSwitchingSystemGraphQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager, threadToStatementContextBridge())
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(queryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new SystemGraphInitializer(queryExecutor, systemGraphOperations, importOptions, secureHasher, mock[Log], config)
    val transactionEventListeners = graph.getDependencyResolver.resolveDependency(classOf[GlobalTransactionEventListeners])
    val systemListeners = transactionEventListeners.getDatabaseTransactionEventListeners(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    systemListeners.forEach(l => transactionEventListeners.unregisterTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, l))
    systemGraphInitializer.initializeSystemGraph()
    systemListeners.forEach(l => transactionEventListeners.registerTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, l))
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
  }
}
