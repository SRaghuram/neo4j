/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext, MorselRuntime}

abstract class AllNodeScanStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with RHSOfApplyLeafStressSuite {

  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.allNodeScan(variable),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes
        } yield Array(x, y)
    )
}
