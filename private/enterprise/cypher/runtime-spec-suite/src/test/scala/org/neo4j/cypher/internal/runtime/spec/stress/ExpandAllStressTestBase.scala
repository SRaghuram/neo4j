/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition

abstract class ExpandAllStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime)
  with RHSOfApplyOneChildStressSuite
  with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.expand(s"($variable)-[:NEXT]->(next)"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          next <- (1 to 5).map(i => nodes((x.getId.toInt + i) % nodes.length))
        } yield Array(x, next),
      Seq("x", "next")
    )

  override def rhsOfApplyOperator(variable: String): RHSOfApplyOneChildTD =
    RHSOfApplyOneChildTD(
      _.expand(s"($variable)-[:NEXT]->(next)"),
      rowsComingIntoTheOperator =>
        for {
          Array(x, y) <- rowsComingIntoTheOperator
          next <- (1 to 5).map(i => nodes((y.getId.toInt + i) % nodes.length))
        } yield Array(x, y, next),
      Seq("x", "y", "next")
    )
}
