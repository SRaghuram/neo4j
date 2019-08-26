/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.runtime.{InputDataStreamTestSupport, NoInput}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticFeature.{Cypher9Comparability, MultipleDatabases}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticState
import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, Statement}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{GeneratingNamer, IfNoParameter}
import org.neo4j.cypher.internal.{FullyParsedQuery, QueryOptions}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ExecutionEngineFullyParsedQueryTest
  extends ExecutionEngineFunSuite
    with CypherComparisonSupport
    with AstConstructionTestSupport
    with InputDataStreamTestSupport {

  test("InputDataStream gets forwarded to runtime") {
    val q = query(
      input(varFor("x")),
      return_(varFor("x").as("x"))
    )

    val result =
      execute(
        prepare(q),
        noParams,
        iteratorInput(Iterator(Array(1), Array(2), Array(3), Array(4)))
      ).toComparableResult
    result shouldEqual List(
      Map("x" -> 1),
      Map("x" -> 2),
      Map("x" -> 3),
      Map("x" -> 4),
    )
  }

  // Using these just to get a sample of queries
  LdbcQueries.LDBC_QUERIES.foreach { query =>

    test(query.name + " (fully parsed)") {
      execute(parse(query.createQuery), query.createParams, NoInput)
      for (q <- query.constraintQueries)
        execute(parse(q), noParams, NoInput)

      val result = execute(parse(query.query), query.params, NoInput).toComparableResult
      result shouldEqual query.expectedResult
    }

  }

  private def noParams: Map[String, Any] = Map.empty

  private val parsing = CompilationPhases.parsing(
    RewriterStepSequencer.newPlain,
    new GeneratingNamer()
  )

  private def parse(qs: String, options: QueryOptions = QueryOptions.default) =
    FullyParsedQuery(
      state = parsing.transform(
        InitialState(qs, None, PlannerNameFor(options.planner.name)),
        ContextHelper.create()
      ),
      options = options
    )

  private val semanticAnalysis =
    SemanticAnalysis(warn = true, Cypher9Comparability, MultipleDatabases).adds(BaseContains[SemanticState]) andThen
    AstRewriting(RewriterStepSequencer.newPlain, IfNoParameter, innerVariableNamer = new GeneratingNamer())

  private def prepare(query: Statement, options: QueryOptions = QueryOptions.default) =
    FullyParsedQuery(
      state = semanticAnalysis.transform(
        InitialState("", None, PlannerNameFor(options.planner.name))
          .withStatement(query),
        ContextHelper.create()
      ),
      options = options
    )
}
