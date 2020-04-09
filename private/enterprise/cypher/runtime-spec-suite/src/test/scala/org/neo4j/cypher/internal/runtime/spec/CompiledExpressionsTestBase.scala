/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.configuration.GraphDatabaseSettings.CypherExpressionEngine.COMPILED
import org.neo4j.configuration.GraphDatabaseSettings.cypher_expression_engine
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.DefaultExpressionCompilerBack

abstract class CompiledExpressionsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                      runtime: CypherRuntime[CONTEXT],
                                                                      sizeHint: Int
  ) extends RuntimeTestSuite[CONTEXT](edition.copyWith(cypher_expression_engine -> COMPILED), runtime) {

  private val filterQuery =
    new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("3*x+100*33 > 0")
      .nonFuseable()
      .projection("1 AS x")
      .argument()
      .build()

  test("should compile expression first time") {
    // given
    val before = DefaultExpressionCompilerBack.numberOfCompilationEvents()

    // when
    consume(execute(filterQuery, runtime))

    // then
    val after = DefaultExpressionCompilerBack.numberOfCompilationEvents()
    after shouldBe (before + 1)
  }

  test("should not recompile expression") {
    // given
    consume(execute(filterQuery, runtime))
    val before = DefaultExpressionCompilerBack.numberOfCompilationEvents()

    // when
    consume(execute(filterQuery, runtime))

    // then
    val after = DefaultExpressionCompilerBack.numberOfCompilationEvents()
    after shouldBe before
  }

  private val projectionQuery =
    new LogicalQueryBuilder(this)
      .produceResults("y1", "y2")
      .projection("3*x+100*33 AS y1", "['hi', 'ho'][x] AS y2")
      .nonFuseable()
      .projection("1 AS x")
      .argument()
      .build()

  test("should compile projection first time") {
    // given
    val before = DefaultExpressionCompilerBack.numberOfCompilationEvents()

    // when
    consume(execute(projectionQuery, runtime))

    // then
    val after = DefaultExpressionCompilerBack.numberOfCompilationEvents()
    after shouldBe (before + 1)
  }

  test("should not recompile projection") {
    // given
    consume(execute(projectionQuery, runtime))
    val before = DefaultExpressionCompilerBack.numberOfCompilationEvents()

    // when
    consume(execute(projectionQuery, runtime))

    // then
    val after = DefaultExpressionCompilerBack.numberOfCompilationEvents()
    after shouldBe before
  }

  private val aggregationQuery =
    new LogicalQueryBuilder(this)
      .produceResults("y", "count")
      .aggregation(Seq("3*x+100*33 AS y"), Seq("count(*) AS count"))
      .nonFuseable()
      .projection("1 AS x")
      .argument()
      .build()

  test("should compile aggregation first time") {
    // given
    val before = DefaultExpressionCompilerBack.numberOfCompilationEvents()

    // when
    consume(execute(aggregationQuery, runtime))

    // then
    val after = DefaultExpressionCompilerBack.numberOfCompilationEvents()
    after shouldBe (before + 1)
  }

  test("should not recompile aggregation") {
    // given
    consume(execute(aggregationQuery, runtime))
    val before = DefaultExpressionCompilerBack.numberOfCompilationEvents()

    // when
    consume(execute(aggregationQuery, runtime))

    // then
    val after = DefaultExpressionCompilerBack.numberOfCompilationEvents()
    after shouldBe before
  }
}
