/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition

abstract class LabelScanStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime)
  with RHSOfApplyLeafStressSuite {

  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String): RHSOfApplyLeafTD =
    RHSOfApplyLeafTD(
      _.nodeByLabelScan(variable, "Label", IndexOrderNone),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes
        } yield Array(x, y)
    )
}
