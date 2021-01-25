/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.PatternGen
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.graphdb.ResourceIterator
import org.scalacheck.Gen
import org.scalacheck.Shrink

/*
 * Tests create on random patterns.
 *  - makes sure that whatever pattern we create is returned when doing MATCH on pattern.
 */
class SemanticCreateAcceptanceTest extends ExecutionEngineFunSuite with PatternGen {

  //we don't want scala check to shrink patterns here and leave things in the database
  implicit val dontShrink: Shrink[List[Element]] = Shrink(_ => Stream.empty)

  test("create and match random patterns") {
    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)


      whenever(pattern.nonEmpty) {
        val patternString = pattern.map(_.string).mkString
        withClue(s"failing on pattern $patternString") {
          //update
          graph.withTx( tx => tx.execute(s"CREATE $patternString").close())

          //find created pattern (cannot return * since everything might be unnamed)
          graph.withTx( tx => {
            val result1 = tx.execute(s"MATCH $patternString RETURN 42")
            hasSingleRow(result1)

            val result2 = tx.execute(s"CYPHER runtime=interpreted MATCH $patternString RETURN 42")
            hasSingleRow(result2)
          })

          //clean up
          graph.withTx( tx => tx.execute(s"MATCH (n) DETACH DELETE n").close())
        }
      }
    }
  }

  private def hasSingleRow(in: ResourceIterator[_]) = {
    in.hasNext should equal(true)
    in.next()
    in.hasNext should equal(false)
  }

  override protected def numberOfTestRuns: Int = 20

  override def relGen = Gen.oneOf(typedRelGen, namedTypedRelGen, typedWithPropertiesRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen, labeledWithPropertiesNodeGen, namedLabeledWithPropertiesNodeGen)

  override def relDirection = Gen.oneOf(SemanticDirection.INCOMING, SemanticDirection.OUTGOING)
}
