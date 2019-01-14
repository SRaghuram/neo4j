/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.{ExecutionEngineFunSuite, PatternGen}
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.scalacheck.{Gen, Shrink}

/*
 * Creates a random pattern, matches on it and deletes all variables
 *  - when done the database should be empty.
 */
class SemanticDeleteAcceptanceTest extends ExecutionEngineFunSuite with PatternGen {

  //we don't want scala check to shrink patterns here and leave things in the database
  implicit val dontShrink: Shrink[List[Element]] = Shrink(s => Stream.empty)

  test("match and delete random patterns") {

    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          graph.execute(s"CREATE $patternString")
          //delete
          val variables = findAllRelationshipNames(pattern) ++ findAllNodeNames(pattern)
          graph.execute(s"MATCH $patternString DELETE ${variables.mkString(",")} RETURN count(*)")

          //now db should be empty
          RewindableExecutionResult(graph.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
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
          //update
          graph.execute(s"CREATE $patternString")
          //delete
          val variables = findAllNodeNames(pattern)
          graph.execute(s"MATCH $patternString DETACH DELETE ${variables.mkString(",")} RETURN count(*)")

          //now db should be empty
          RewindableExecutionResult(graph.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
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
          //update
          graph.execute(s"CREATE $patternString")
          //delete
          val variables = findAllRelationshipNames(pattern) ++ findAllNodeNames(pattern)
          val undirected = makeUndirected(pattern).map(_.string).mkString
          graph.execute(s"MATCH $undirected DELETE ${variables.mkString(",")}")

          //now db should be empty
          RewindableExecutionResult(graph.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
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
          //update
          graph.execute(s"CREATE $patternString")
          //delete
          val variables = findAllNodeNames(pattern)
          val undirected = makeUndirected(pattern).map(_.string).mkString
          graph.execute(s"MATCH $undirected DETACH DELETE ${variables.mkString(",")}")

          //now db should be empty
          RewindableExecutionResult(graph.execute("MATCH () RETURN count(*) AS c")).toList should equal(List(Map("c" -> 0)))
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
