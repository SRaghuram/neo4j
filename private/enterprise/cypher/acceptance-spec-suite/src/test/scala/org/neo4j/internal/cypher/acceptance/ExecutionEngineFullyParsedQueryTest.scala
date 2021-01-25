/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.runtime.InputDataStreamTestSupport
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class ExecutionEngineFullyParsedQueryTest
  extends ExecutionEngineFunSuite
  with CypherComparisonSupport
  with AstConstructionTestSupport
  with InputDataStreamTestSupport
  with FullyParsedQueryTestSupport {

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

}
