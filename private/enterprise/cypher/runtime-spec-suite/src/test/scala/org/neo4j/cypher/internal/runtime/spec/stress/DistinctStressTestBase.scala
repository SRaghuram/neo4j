/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}
import org.neo4j.graphdb.Node

abstract class DistinctStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with RHSOfApplyOneChildStressSuite
    with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.distinct(s"$variable AS $variable"),
      rowsComingIntoTheOperator => {
        val xs: IndexedSeq[Node] = (for {Array(x) <- rowsComingIntoTheOperator} yield x).toIndexedSeq
        xs.distinct.map(Array(_))
      },
      Seq("x")
    )

  override def rhsOfApplyOperator(variable: String) =
    RHSOfApplyOneChildTD(
      _.distinct(s"$variable AS $variable"),
      rowsComingIntoTheOperator => {
        val rows = rowsComingIntoTheOperator.map(_.toSeq).toIndexedSeq
        rows.distinct.map(_.toArray)
      },
      Seq("x", "y")
    )
}
