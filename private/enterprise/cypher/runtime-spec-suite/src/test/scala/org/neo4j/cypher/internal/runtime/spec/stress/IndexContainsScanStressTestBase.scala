/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition

abstract class IndexContainsScanStressTestBase(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(edition, runtime)
  with RHSOfApplyLeafStressSuite {

  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String): RHSOfApplyLeafTD =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(text CONTAINS ???)", paramExpr = Some(function("toString", varFor(propArgument))), argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes.filter(n =>
            n.getProperty("text").asInstanceOf[String].contains(x.getId.toString))
        } yield Array(x, y)
    )
}
