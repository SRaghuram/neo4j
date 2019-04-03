/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.Optional

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}

class MultiDatabaseCypherAcceptanceTest
  extends ExecutionEngineFunSuite
    with CommercialGraphDatabaseTestSupport {

  test("should list default database") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW DATABASE neo4j")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j", "status" -> "started")))
  }

  test("should list default databases") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toList should be(List(Map("name" -> "system", "status" -> "started"), Map("name" -> "neo4j", "status" -> "started")))
  }

  test("should create database in systemdb") {
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(Map("name" -> "foo", "status" -> "started")))
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
      Map("name" -> "foo", "status" -> "started"),
      Map("name" -> "bar", "status" -> "started"),
      Map("name" -> "baz", "status" -> "started")
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

  private def databaseManager() = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

  private def selectDatabase(name: String): Unit = {
    val maybeCtx: Optional[DatabaseContext] = databaseManager().getDatabaseContext(name)
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }
}
