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
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.GeneratingNamer
import org.neo4j.cypher.internal.{FullyParsedQuery, QueryOptions, RewindableExecutionResult}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ExecutionEngineFullyParsedQueryTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Using these just to get a sample of queries
  LdbcQueries.LDBC_QUERIES.foreach { query =>

    test(query.name + " (fully parsed)") {
      execute(parse(query.createQuery), query.createParams)
      for (q <- query.constraintQueries)
        execute(parse(q), noParams)

      val result = execute(parse(query.query), query.params).toComparableResult
      result shouldEqual query.expectedResult
    }

  }

  private def noParams: Map[String, Any] = Map.empty

  private def execute(fpq: FullyParsedQuery, params: Map[String, Any]): RewindableExecutionResult =
    execute(fpq, params, NoInput)

  private val pipeline = CompilationPhases.parsing(
    RewriterStepSequencer.newPlain,
    new GeneratingNamer()
  )

  private def parse(qs: String, options: QueryOptions = QueryOptions.default) =
    FullyParsedQuery(
      state = pipeline.transform(
        InitialState(qs, None, PlannerNameFor(options.planner.name)),
        ContextHelper.create()
      ),
      options = options
    )

}
