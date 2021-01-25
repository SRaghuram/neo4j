/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherExpressionEngine.COMPILED
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.runtime.spec.CompiledExpressionsTestBase.COUNTER

object CompiledExpressionsTestBase {
  // This is necessary to avoid accidental caching of expressions when different implementations
  // of this test execute in parallel.
  val COUNTER = new AtomicLong()
}

abstract class CompiledExpressionsTestBase[CONTEXT <: EnterpriseRuntimeContext](edition: Edition[CONTEXT],
              runtime: CypherRuntime[CONTEXT]
            ) extends RuntimeTestSuite[CONTEXT](edition.copyWith(GraphDatabaseInternalSettings.cypher_expression_engine -> COMPILED), runtime) {

  private def newFilterQuery() =
    new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter(s"3*x+100*${COUNTER.getAndIncrement()} > 0")
      .nonFuseable()
      .projection("1 AS x")
      .argument()
      .build()

  test("should compile expression first time") {
    numberOfCompilationEvents(newFilterQuery()) shouldBe 1
  }

  test("should not recompile expression") {
    // given
    val filterQuery = newFilterQuery()
    execute(filterQuery, runtime)

    // then
    numberOfCompilationEvents(filterQuery) shouldBe 0
  }

  private def newProjectionQuery() =
    new LogicalQueryBuilder(this)
      .produceResults("y1", "y2")
      .projection(s"3*x+100*${COUNTER.getAndIncrement()} AS y1", "['hi', 'ho'][x] AS y2")
      .nonFuseable()
      .projection("1 AS x")
      .argument()
      .build()

  test("should compile projection first time") {
    // when
    numberOfCompilationEvents(newProjectionQuery()) shouldBe 1
  }

  test("should not recompile projection") {
    // given
    val projectionQuery = newProjectionQuery()
    execute(projectionQuery, runtime)

    // then
    numberOfCompilationEvents(projectionQuery) shouldBe 0
  }

  private def newAggregationQuery() =
    new LogicalQueryBuilder(this)
      .produceResults("y", "count")
      .aggregation(Seq(s"3*x+100*${COUNTER.getAndIncrement()} AS y"), Seq("count(*) AS count"))
      .nonFuseable()
      .projection("1 AS x")
      .argument()
      .build()

  test("should compile aggregation first time") {
    numberOfCompilationEvents(newAggregationQuery()) shouldBe 1
  }

  test("should not recompile aggregation") {
    // given
    val aggregationQuery = newAggregationQuery()
    execute(aggregationQuery, runtime)
    // then
    numberOfCompilationEvents(aggregationQuery) shouldBe 0
  }

  private def numberOfCompilationEvents(query: LogicalQuery): Int = {
    val (_, context) = buildPlanAndContext(query, runtime)
    context.compiledExpressionsContext.cachingExpressionCompilerTracer.asInstanceOf[TestCachingExpressionCompilerTracer].numberOfCompilationEvents
  }

  test("should never cache expression/projection/grouping which has late properties") {
    // given
    val propName = "unheardOfProperty"
    val someNumber = COUNTER.getAndIncrement() * 6841

    val query = new LogicalQueryBuilder(this)
      .produceResults("prop", "sum")
      .aggregation(Seq(s"n.$propName AS prop", "id(n) AS n", "id(x) AS x"), Seq("sum(x) AS sum"))
      .projection(s"n.$propName * $someNumber AS x")
      .filter(s"n.$propName > $someNumber")
      .nonFuseable()
      .allNodeScan("n")
      .build()

    val initialEvents = numberOfCompilationEvents(query)
    initialEvents should be >= 3 // pipelined has 4 because of mapper/reducer part of aggregation

    // when recompile then
    numberOfCompilationEvents(query) shouldBe initialEvents
  }

  test("should never cache expression/projection/grouping which has late labels") {
    // given
    val labelName = "UnheardLabel" + COUNTER.getAndIncrement()

    val query = new LogicalQueryBuilder(this)
      .produceResults("hasLabel", "count")
      .aggregation(Seq(s"n:$labelName AS hasLabel", "id(n) AS n", "id(x) AS x"), Seq("count(*) AS count"))
      .projection(s"n:$labelName IN [true, false] AS x")
      .filter(s"n:$labelName IN [true, false]")
      .nonFuseable()
      .allNodeScan("n")
      .build()

    val initialEvents = numberOfCompilationEvents(query)
    initialEvents should be >= 3 // pipelined has 4 because of mapper/reducer part of aggregation

    // when recompile then
    numberOfCompilationEvents(query) shouldBe initialEvents
  }

  test("should never cache expression/projection/grouping which has late relationship types") {
    // given
    val typeName = "UnheardType" + COUNTER.getAndIncrement()

    val query = new LogicalQueryBuilder(this)
      .produceResults("hasType", "count")
      .aggregation(Seq(s"r:$typeName AS hasType", "id(r) AS r", "id(x) AS x"), Seq("count(*) AS count"))
      .projection(s"r:$typeName IN [true, false] AS x")
      .filter(s"r:$typeName IN [true, false]")
      .nonFuseable()
      .expand("(n)-[r]->()")
      .allNodeScan("n")
      .build()

    val initialEvents = numberOfCompilationEvents(query)
    initialEvents should be >= 3 // pipelined has 4 because of mapper/reducer part of aggregation

    // when recompile then
    numberOfCompilationEvents(query) shouldBe initialEvents
  }
}
