/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

trait TestExhausted {
  def assertExhausted(): Unit
}

trait ReductionTestHelper extends CypherFunSuite {

  def getOracle(expectedInvocationsAndResults: Seq[(Array[Int], OracleResult)]): Oracle[Array[Int]] with TestExhausted = {
    var i = 0
    new Oracle[Array[Int]] with TestExhausted {
      override def apply(a: Array[Int]): OracleResult = {
        if (i >= expectedInvocationsAndResults.length) {
          fail(s"Oracle invoked too often. Argument was: ${a.toSeq}")
        }
        a should equal(expectedInvocationsAndResults(i)._1)
        val res = expectedInvocationsAndResults(i)._2
        i = i + 1
        res
      }

      def assertExhausted(): Unit = {
        if (i != expectedInvocationsAndResults.length) {
          fail(s"Oracle not invoked often enough. Next expected call: ${expectedInvocationsAndResults(i)._1.toSeq}")
        }
      }
    }
  }

}
