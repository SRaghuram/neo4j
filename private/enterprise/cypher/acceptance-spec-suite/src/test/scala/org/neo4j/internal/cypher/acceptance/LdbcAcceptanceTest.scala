/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.internal.cypher.acceptance.LdbcQueries.LDBC_QUERIES
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

/**
 * Runs the 14 LDBC queries and checks so that the result is what is expected.
 */
class LdbcAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {


  LDBC_QUERIES.foreach { ldbcQuery =>
    test(ldbcQuery.name) {
      // given
      execute(ldbcQuery.createQuery, ldbcQuery.createParams)
      ldbcQuery.constraintQueries.foreach(query => execute(query))

      // when
      val result =
        executeWith(ldbcQuery.expectedToSucceedIn, ldbcQuery.query, params = ldbcQuery.params)

      //then
      result.toComparableResult should equal(ldbcQuery.expectedResult)
    }
  }

  test("LDBC query 12 should not get a bad plan because of lost precision in selectivity calculation") {
    // given
    val ldbcQuery = LdbcQueries.Query12
    execute(ldbcQuery.createQuery, ldbcQuery.createParams)
    ldbcQuery.constraintQueries.foreach(query => execute(query))

    val params: Map[String, Any] = Map("1" -> 0, "2" -> 1, "3" -> 10)

    val result =
    // when
      executeWith(ldbcQuery.expectedToSucceedIn, ldbcQuery.query, params = params)

    // no precision loss resulting in insane numbers
    all(collectEstimations(result.executionPlanDescription())) should be > 0.0
    all(collectEstimations(result.executionPlanDescription())) should be < 10.0
  }

  test("This LDBC query should work") {
    // given
    val ldbcQuery = """MATCH (knownTag:Tag {name:$2})
                      |MATCH (person:Person {id:$1})-[:KNOWS*1..2]-(friend)
                      |WHERE NOT person=friend
                      |WITH DISTINCT friend, knownTag
                      |MATCH (friend)<-[:POST_HAS_CREATOR]-(post)
                      |WHERE (post)-[:POST_HAS_TAG]->(knownTag)
                      |WITH post, knownTag
                      |MATCH (post)-[:POST_HAS_TAG]->(commonTag)
                      |WHERE NOT commonTag=knownTag
                      |WITH commonTag, count(post) AS postCount
                      |RETURN commonTag.name AS tagName, postCount
                      |ORDER BY postCount DESC, tagName ASC
                      |LIMIT $3""".stripMargin
    execute(LdbcQueries.Query4.createQuery, LdbcQueries.Query4.createParams)


    val params: Map[String, Any] = Map("1" -> 1, "2" ->  "tag1-ᚠさ丵פش", "3" -> 10)

    val result =
    // when
      executeWith(Configs.InterpretedAndSlottedAndPipelined, ldbcQuery, params = params)

    // then
    result should not be empty
  }

  private def collectEstimations(plan: InternalPlanDescription): Seq[Double] = {
    plan.arguments.collectFirst {
      case EstimatedRows(effectiveCardinality, _) => effectiveCardinality
    }.get +:
      plan.children.toIndexedSeq.flatMap(collectEstimations)
  }
}
