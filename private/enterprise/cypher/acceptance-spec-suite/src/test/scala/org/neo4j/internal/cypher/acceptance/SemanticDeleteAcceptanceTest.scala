/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.PatternGen
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.scalacheck.Gen
import org.scalacheck.Shrink

/*
 * Creates a random pattern, matches on it and deletes all variables
 *  - when done the database should be empty.
 */
class SemanticDeleteAcceptanceTest extends ExecutionEngineFunSuite with PatternGen {

  //we don't want scala check to shrink patterns here and leave things in the database
  implicit val dontShrink: Shrink[List[Element]] = Shrink(_ => Stream.empty)

  test("match and delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          val transaction = graphOps.beginTx()
          try {
            transaction.execute(s"CREATE $patternString").close()
            //delete
            val variables = findAllRelationshipNames(pattern) ++ findAllNodeNames(pattern)
            transaction.execute(s"MATCH $patternString DELETE ${variables.mkString(",")} RETURN count(*)").close()

            //now db should be empty
            RewindableExecutionResult(transaction.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
          } finally {
            transaction.close()
          }
        }
      }
    }
  }

  test("match and detach delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          val transaction = graphOps.beginTx()
          try {
            //update
            transaction.execute(s"CREATE $patternString").close()
            //delete
            val variables = findAllNodeNames(pattern)
            transaction.execute(s"MATCH $patternString DETACH DELETE ${variables.mkString(",")} RETURN count(*)").close()

            //now db should be empty
            RewindableExecutionResult(transaction.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
          } finally {
            transaction.close()
          }
        }
      }
    }
  }

  test("undirected match and delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          val transaction = graphOps.beginTx()
          try {
            //update
            transaction.execute(s"CREATE $patternString").close()
            //delete
            val variables = findAllRelationshipNames(pattern) ++ findAllNodeNames(pattern)
            val undirected = makeUndirected(pattern).map(_.string).mkString
            transaction.execute(s"MATCH $undirected DELETE ${variables.mkString(",")}").close()

            //now db should be empty
            RewindableExecutionResult(transaction.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
          } finally {
            transaction.close()
          }
        }
      }
    }
  }

  test("undirected match and detach delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          val transaction = graphOps.beginTx()
          try {
            //update
            transaction.execute(s"CREATE $patternString").close()
            //delete
            val variables = findAllNodeNames(pattern)
            val undirected = makeUndirected(pattern).map(_.string).mkString
            transaction.execute(s"MATCH $undirected DETACH DELETE ${variables.mkString(",")}").close()

            //now db should be empty
            RewindableExecutionResult(transaction.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
          } finally {
            transaction.close()
          }
        }
      }
    }
  }

  private def makeUndirected(elements: Seq[Element]): Seq[Element] = elements.map {
    case n: NodeWithRelationship => n.copy(rel = n.rel.withDirection(SemanticDirection.BOTH))
    case other => other
  }

  override protected def numberOfTestRuns: Int = 20

  override def relGen = Gen.oneOf(namedTypedRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen = Gen.oneOf(namedNodeGen, namedLabeledNodeGen, namedLabeledWithPropertiesNodeGen)

  override def relDirection = Gen.oneOf(SemanticDirection.INCOMING, SemanticDirection.OUTGOING)
}
