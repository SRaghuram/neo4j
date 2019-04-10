/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.{Collections, Optional}

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.server.security.enterprise.auth.SecureHasher
import com.neo4j.server.security.enterprise.systemgraph._
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.logging.Log

class MultiDatabaseCypherAcceptanceTest
  extends ExecutionEngineFunSuite
    with CommercialGraphDatabaseTestSupport {

  test("should list default database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW DATABASE neo4j")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j", "status" -> "online")))
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
    result.toList should be(List(Map("name" -> "foo", "status" -> "online")))
  }

  test("should create and delete databases") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE DATABASE baz")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toList should contain allOf(
      Map("name" -> "foo", "status" -> "online"),
      Map("name" -> "bar", "status" -> "online"),
      Map("name" -> "baz", "status" -> "online")
    )

    // GIVEN
    execute("DROP DATABASE bar")

    // WHEN
    val result2 = execute("SHOW DATABASES")

    // THEN
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should contain allOf("foo", "baz")
    databaseNames should not contain "bar"
  }

  test("should start database on create") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> "online")))
  }

  test("should stop database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> "online")))

    // WHEN
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> "offline")))
  }

  test("should re-start database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(Map("name" -> "foo", "status" -> "online"))) // make sure it was started
    execute("STOP DATABASE foo")
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(Map("name" -> "foo", "status" -> "offline"))) // and stopped

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result3 = execute("SHOW DATABASE foo")
    result3.toList should be(List(Map("name" -> "foo", "status" -> "online")))
  }

  test("should not be able to start non existing database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("START DATABASE foo") // TODO: Shouldn't this throw?

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should not be able to stop non existing database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("STOP DATABASE foo") // TODO: Shouldn't this throw?

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should not be able to start a dropped database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("DROP DATABASE foo")

    // WHEN
    execute("START DATABASE foo") // TODO: Shouldn't this throw?

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should not be able to stop a dropped database") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("DROP DATABASE foo")

    // WHEN
    execute("STOP DATABASE foo") // TODO: Shouldn't this throw?

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  protected override def initTest(): Unit = {
    super.initTest()
    val queryExecutor: ContextSwitchingSystemGraphQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager(), "impermanent-db")
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(queryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new SystemGraphInitializer(queryExecutor, systemGraphOperations, importOptions, secureHasher, mock[Log])
    systemGraphInitializer.initializeSystemGraph()
  }

  private def databaseManager() = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

  private def selectDatabase(name: String): Unit = {
    val manager = databaseManager()
    val maybeCtx: Optional[DatabaseContext] = manager.getDatabaseContext(name)
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }
}
