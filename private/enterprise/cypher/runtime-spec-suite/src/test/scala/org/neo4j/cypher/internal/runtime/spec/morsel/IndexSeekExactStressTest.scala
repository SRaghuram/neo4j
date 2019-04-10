/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext, MorselRuntime}

abstract class IndexSeekExactStressTest(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with RHSOfApplyLeafStressSuite {

  override def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String) =
    RHSOfApplyLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop = ???)",
                          paramExpr = Some(varFor(propArgument)),
                          argumentIds = Set(propArgument)),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          y <- nodes.filter(_.getProperty("prop").asInstanceOf[Int] == x.getId)
        } yield Array(x, y)
    )

  override def rhsOfCartesianLeaf(variable: String) =
    RHSOfCartesianLeafTD(
      _.nodeIndexOperator(s"$variable:Label(prop = 10)"),
      () => nodes.filter(_.getProperty("prop").asInstanceOf[Int] == 10).map(Array(_))
    )
}
