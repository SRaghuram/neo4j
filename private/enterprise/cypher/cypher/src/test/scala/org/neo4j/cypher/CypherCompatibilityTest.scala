/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.api.exceptions.Status

import scala.collection.JavaConverters._

class CypherCompatibilityTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport {

  private val QUERY = "MATCH (n:Label) RETURN n"
  private val QUERY_NOT_COMPILED = "MATCH (n:Movie)--(b), (a:A)--(c:C)--(d:D) RETURN count(*)"

  override protected def initTest(): Unit = ()

  override protected def stopTest(): Unit = ()

  test("should be able to switch between versions") {
    runWithConfig() {
      db =>
        db.execute(s"CYPHER 3.5 $QUERY").asScala.toList shouldBe empty
        db.execute(s"CYPHER 4.0 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should be able to switch between versions2") {
    runWithConfig() {
      db =>
        db.execute(s"CYPHER 3.5 $QUERY").asScala.toList shouldBe empty
        db.execute(s"CYPHER 4.0 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should be able to override config") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> "4.0") {
      db =>
        db.execute(s"CYPHER 3.5 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should be able to override config2") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> "3.5") {
      db =>
        db.execute(s"CYPHER 4.0 $QUERY").asScala.toList shouldBe empty
    }
  }

  test("should use default version by default") {
    runWithConfig() {
      db =>
        val result = db.execute(QUERY)
        result.asScala.toList shouldBe empty
        result.getExecutionPlanDescription.getArguments.get("version") should equal("CYPHER 4.0")
    }
  }

  test("should handle profile in compiled runtime") {
    runWithConfig() {
      db =>
        assertProfiled(db, "CYPHER 4.0 runtime=compiled PROFILE MATCH (n) RETURN n")
    }
  }

  test("should handle profile in interpreted runtime") {
    runWithConfig() {
      db =>
        assertProfiled(db, "CYPHER 3.5 runtime=interpreted PROFILE MATCH (n) RETURN n")
        assertProfiled(db, "CYPHER 4.0 runtime=interpreted PROFILE MATCH (n) RETURN n")
    }
  }

  test("should allow the use of explain in the supported compilers") {
    runWithConfig() {
      db =>
        assertExplained(db, "CYPHER 3.5 EXPLAIN MATCH (n) RETURN n")
        assertExplained(db, "CYPHER 4.0 EXPLAIN MATCH (n) RETURN n")
    }
  }

  test("should allow executing enterprise queries") {
    runWithConfig() {
      db =>
        assertVersionAndRuntime(db, "3.5", "slotted")
        assertVersionAndRuntime(db, "3.5", "compiled")
        assertVersionAndRuntime(db, "4.0", "slotted")
        assertVersionAndRuntime(db, "4.0", "compiled")
    }
  }

  test("should not fail if asked to execute query with runtime=compiled on simple query") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        db.execute("MATCH (n:Movie) RETURN n")
        db.execute("CYPHER runtime=compiled MATCH (n:Movie) RETURN n")
        shouldHaveNoWarnings(db.execute("EXPLAIN CYPHER runtime=compiled MATCH (n:Movie) RETURN n"))
    }
  }

  test("should fail if asked to execute query with runtime=compiled instead of falling back to interpreted if hint errors turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      db =>
        intercept[QueryExecutionException](
          db.execute(s"EXPLAIN CYPHER runtime=compiled $QUERY_NOT_COMPILED")
        ).getStatusCode should equal("Neo.ClientError.Statement.RuntimeUnsupportedError")
    }
  }

  test("should not fail if asked to execute query with runtime=compiled and instead fallback to interpreted and return a warning if hint errors turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "false") {
      db =>
        val result = db.execute(s"EXPLAIN CYPHER runtime=compiled $QUERY_NOT_COMPILED")
        shouldHaveWarning(result, Status.Statement.RuntimeUnsupportedWarning)
    }
  }

  test("should not fail if asked to execute query with runtime=compiled and instead fallback to interpreted and return a warning by default") {
    runWithConfig() {
      db =>
        val result = db.execute(s"EXPLAIN CYPHER runtime=compiled $QUERY_NOT_COMPILED")
        shouldHaveWarning(result, Status.Statement.RuntimeUnsupportedWarning)
    }
  }

  test("should not fail nor generate a warning if asked to execute query without specifying runtime, knowing that compiled is default but will fallback silently to interpreted") {
    runWithConfig() {
      db =>
        shouldHaveNoWarnings(db.execute(s"EXPLAIN $QUERY_NOT_COMPILED"))
    }
  }

  test("should not support old compilers") {
    runWithConfig() {
      db =>
        intercept[QueryExecutionException](db.execute("CYPHER 1.9 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.0 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.1 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.2 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 2.3 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 3.0 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 3.1 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 3.2 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 3.3 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 3.4 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        intercept[QueryExecutionException](db.execute("CYPHER 3.6 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
    }
  }

  test("should use settings without regard of case") {
    runWithConfig(GraphDatabaseSettings.cypher_runtime -> "slotted") {
      db =>
        db.execute(QUERY).getExecutionPlanDescription.toString should include("SLOTTED")
    }
  }

  private def assertProfiled(db: GraphDatabaseCypherService, q: String) {
    val result = db.execute(q)
    result.resultAsString()
    assert(result.getExecutionPlanDescription.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.getQueryExecutionType.requestedExecutionPlanDescription(), s"$q was not flagged for planDescription")
  }

  private def assertExplained(db: GraphDatabaseCypherService, q: String) {
    val result = db.execute(q)
    result.resultAsString()
    assert(!result.getExecutionPlanDescription.hasProfilerStatistics, s"$q was not explained as expected")
    assert(result.getQueryExecutionType.requestedExecutionPlanDescription(), s"$q was not flagged for planDescription")
  }

  private def assertVersionAndRuntime(db: GraphDatabaseCypherService, version: String, runtime: String): Unit = {
    val result = db.execute(s"CYPHER $version runtime=$runtime MATCH (n) RETURN n")
    result.getExecutionPlanDescription.getArguments.get("version") should be("CYPHER "+version)
    result.getExecutionPlanDescription.getArguments.get("runtime") should be(runtime.toUpperCase)
  }
}
