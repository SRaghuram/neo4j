/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition

import scala.util.Random

abstract class NodeByIdSeekStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime)
  with RHSOfApplyLeafStressSuite {

  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String): RHSOfApplyLeafTD = {
    val random = new Random(42)
    val seekNodes = (1 to 5).map(_ => nodes(random.nextInt(nodes.size)))
    RHSOfApplyLeafTD(
      _.nodeByIdSeek(variable, Set.empty, ids = seekNodes.map(_.getId): _*),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- seekNodes
        } yield Array(x, y)
    )
  }
}
