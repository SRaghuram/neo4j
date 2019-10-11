/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.{FALSE, TRUE}

import com.neo4j.cypher.RunWithConfigTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
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
        db.withTx( tx => {
          tx.execute(s"CYPHER 3.5 $QUERY").asScala.toList shouldBe empty
          tx.execute(s"CYPHER 4.0 $QUERY").asScala.toList shouldBe empty
        })
    }
  }

  test("should be able to switch between versions2") {
    runWithConfig() {
      db =>
        db.withTx(tx => {
          tx.execute(s"CYPHER 3.5 $QUERY").asScala.toList shouldBe empty
          tx.execute(s"CYPHER 4.0 $QUERY").asScala.toList shouldBe empty
        })
    }
  }

  test("should be able to override config") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> GraphDatabaseSettings.CypherParserVersion.V_40) {
      db =>
        db.withTx(tx => tx.execute(s"CYPHER 3.5 $QUERY").asScala.toList shouldBe empty)
    }
  }

  test("should be able to override config2") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> GraphDatabaseSettings.CypherParserVersion.V_35) {
      db =>
        db.withTx(tx => {
          tx.execute(s"CYPHER 4.0 $QUERY").asScala.toList shouldBe empty
        })
    }
  }

  test("should use default version by default") {
    runWithConfig() {
      db =>
        db.withTx(tx => {
          val result = tx.execute(QUERY)
          result.asScala.toList shouldBe empty
          result.getExecutionPlanDescription.getArguments.get("version") should equal("CYPHER 4.0")
        })
    }
  }

  test("should handle profile in compiled runtime") {
    runWithConfig() {
      db =>
        assertProfiled(db, "CYPHER 4.0 runtime=legacy_compiled PROFILE MATCH (n) RETURN n")
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
        assertVersionAndRuntime(db, "3.5", "legacy_compiled")
        assertVersionAndRuntime(db, "4.0", "slotted")
        assertVersionAndRuntime(db, "4.0", "legacy_compiled")
    }
  }

  test("should not fail if asked to execute query with runtime=legacy_compiled on simple query") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> TRUE) {
      db =>
        db.withTx(tx => {
          tx.execute("MATCH (n:Movie) RETURN n").close()
          tx.execute("CYPHER runtime=legacy_compiled MATCH (n:Movie) RETURN n").close()
          shouldHaveNoWarnings(tx.execute("EXPLAIN CYPHER runtime=legacy_compiled MATCH (n:Movie) RETURN n"))
        })
    }
  }

  test("should fail if asked to execute query with runtime=legacy_compiled instead of falling back to interpreted if hint errors turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> TRUE) {
      db =>
        db.withTx(tx => {
          intercept[QueryExecutionException](
            tx.execute(s"EXPLAIN CYPHER runtime=legacy_compiled $QUERY_NOT_COMPILED")
          ).getStatusCode should equal("Neo.ClientError.Statement.RuntimeUnsupportedError")
        })
    }
  }

  test("should not fail if asked to execute query with runtime=legacy_compiled and instead fallback to interpreted and return a warning if hint errors turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> FALSE) {
      db =>
        db.withTx(tx => {
          val result = tx.execute(s"EXPLAIN CYPHER runtime=legacy_compiled $QUERY_NOT_COMPILED")
          shouldHaveWarning(result, Status.Statement.RuntimeUnsupportedWarning)
        })
    }
  }

  test("should not fail if asked to execute query with runtime=legacy_compiled and instead fallback to interpreted and return a warning by default") {
    runWithConfig() {
      db =>
        db.withTx(tx => {
          val result = tx.execute(s"EXPLAIN CYPHER runtime=legacy_compiled $QUERY_NOT_COMPILED")
          shouldHaveWarning(result, Status.Statement.RuntimeUnsupportedWarning)
        })
    }
  }

  test("should not fail nor generate a warning if asked to execute query without specifying runtime, knowing that compiled is default but will fallback silently to interpreted") {
    runWithConfig() {
      db =>
        db.withTx(tx => {
          shouldHaveNoWarnings(tx.execute(s"EXPLAIN $QUERY_NOT_COMPILED"))
        })
    }
  }

  test("should not support old compilers") {
    runWithConfig() {
      db =>
        db.withTx(tx => {
          intercept[QueryExecutionException](tx.execute("CYPHER 1.9 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 2.0 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 2.1 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 2.2 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 2.3 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 3.0 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 3.1 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 3.2 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 3.3 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 3.4 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
          intercept[QueryExecutionException](tx.execute("CYPHER 3.6 MATCH (n) RETURN n")).getStatusCode should equal("Neo.ClientError.Statement.SyntaxError")
        })
    }
  }

  test("should use settings without regard of case") {
    runWithConfig(GraphDatabaseSettings.cypher_runtime -> GraphDatabaseSettings.CypherRuntime.SLOTTED) {
      db =>
        db.withTx(tx => {
          val result = tx.execute(QUERY)
          result.getExecutionPlanDescription.toString should include("SLOTTED")
          result.close()
        })
    }
  }

  private def assertProfiled(db: GraphDatabaseCypherService, q: String) {
    db.withTx(tx => {
      val result = tx.execute(q)
      result.resultAsString()
      assert(result.getExecutionPlanDescription.hasProfilerStatistics, s"$q was not profiled as expected")
      assert(result.getQueryExecutionType.requestedExecutionPlanDescription(), s"$q was not flagged for planDescription")
    })
  }

  private def assertExplained(db: GraphDatabaseCypherService, q: String) {
    db.withTx(tx => {
      val result = tx.execute(q)
      result.resultAsString()
      assert(!result.getExecutionPlanDescription.hasProfilerStatistics, s"$q was not explained as expected")
      assert(result.getQueryExecutionType.requestedExecutionPlanDescription(), s"$q was not flagged for planDescription")
    })
  }

  private def assertVersionAndRuntime(db: GraphDatabaseCypherService, version: String, runtime: String): Unit = {
    db.withTx(tx => {
      val result = tx.execute(s"CYPHER $version runtime=$runtime MATCH (n) RETURN n")
      result.getExecutionPlanDescription.getArguments.get("version") should be("CYPHER " + version)
      result.getExecutionPlanDescription.getArguments.get("runtime") should be(runtime.toUpperCase)
      result.close()
    })
  }
}
