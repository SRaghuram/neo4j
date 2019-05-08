/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.Optional

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.server.security.enterprise.systemgraph._
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher._
import org.neo4j.cypher.internal.DatabaseStatus
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.SecureHasher
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor

class MultiDatabaseCypherAcceptanceTest extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  private val onlineStatus = DatabaseStatus.Online.stringValue()
  private val offlineStatus = DatabaseStatus.Offline.stringValue()

  test("should list default database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW DATABASE neo4j")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j", "status" -> onlineStatus)))
  }

  test("should list default databases") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    val databaseNames: Set[String] = result.columnAs("name").toSet
    databaseNames should contain allOf("system", "neo4j")
  }

  test("should create database in systemdb") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))
  }

  test("should fail on creating already existing database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))

    try{
      // WHEN
      execute("CREATE DATABASE foo")

      fail("Expected error \"The specified database already exists.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("The specified database already exists.")
    }
  }

  test("should create and drop databases") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE DATABASE baz")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toList should contain allOf(
      Map("name" -> "foo", "status" -> onlineStatus),
      Map("name" -> "bar", "status" -> onlineStatus),
      Map("name" -> "baz", "status" -> onlineStatus)
    )

    // GIVEN
    execute("DROP DATABASE baz") //online database
    execute("STOP DATABASE bar")
    execute("DROP DATABASE bar") //offline database
    execute("DROP DATABASE bar") //deleted database

    // WHEN
    val result2 = execute("SHOW DATABASES")

    // THEN
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should contain("foo")
    databaseNames should not contain allOf("bar", "baz")
  }

  test("should fail on dropping non-existing database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try {
      // WHEN
      execute("DROP DATABASE foo")

      fail("Expected error \"Database 'foo' does not exist.\"")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Database 'foo' does not exist.")
    }
  }

  test("should fail on dropping dropped database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASES")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    try {
      // WHEN
      execute("DROP DATABASE foo")

      fail("Expected error \"Database 'foo' does not exist.\"")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Database 'foo' does not exist.")
    }
  }

  test("should start database on create") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))
  }

  test("should not be able to start non-existing database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try {
      // WHEN
      execute("START DATABASE foo")

      fail("Expected error \"Database 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Database 'foo' does not exist.")
    }

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should not be able to start a dropped database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    try{
    // WHEN
    execute("START DATABASE foo")

      fail("Expected error \"Database 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Database 'foo' does not exist.")
    }

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should be able to start a started database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))
  }

  test("should re-start database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus))) // and stopped

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))
  }

  test("should stop database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> onlineStatus)))

    // WHEN
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus)))
  }

  test("should not be able to stop non-existing database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try{
      // WHEN
      execute("STOP DATABASE foo")

      fail("Expected error \"Database 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Database 'foo' does not exist.")
    }

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should not be able to stop a dropped database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    try{
      // WHEN
      execute("STOP DATABASE foo")

      fail("Expected error \"Database 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should be("Database 'foo' does not exist.")
    }

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should be able to stop a stopped database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus)))

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> offlineStatus)))
  }

  protected override def initTest(): Unit = {
    super.initTest()
    val queryExecutor: ContextSwitchingSystemGraphQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager(), threadToStatementContextBridge())
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(queryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new SystemGraphInitializer(queryExecutor, systemGraphOperations, importOptions, secureHasher, mock[Log])
    val transactionEventListeners = graph.getDependencyResolver.resolveDependency(classOf[GlobalTransactionEventListeners])
    val systemListeners = transactionEventListeners.getDatabaseTransactionEventListeners(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    systemListeners.forEach(l => transactionEventListeners.unregisterTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, l))
    systemGraphInitializer.initializeSystemGraph()
    systemListeners.forEach(l => transactionEventListeners.registerTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, l))
  }

  private def databaseManager() = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

  private def threadToStatementContextBridge() = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

  private def selectDatabase(name: String): Unit = {
    val manager = databaseManager()
    val maybeCtx: Optional[DatabaseContext] = manager.getDatabaseContext(new DatabaseId(name))
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }
}
