/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext, MorselRuntime}

abstract class IndexContainsScanStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with RHSOfApplyLeafStressSuite {

  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(text CONTAINS ???)", paramExpr = Some(function("toString", varFor(propArgument))), argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes.filter(_.getProperty("text").asInstanceOf[String].contains(x.getId.toString))
        } yield Array(x, y)
    )
}
