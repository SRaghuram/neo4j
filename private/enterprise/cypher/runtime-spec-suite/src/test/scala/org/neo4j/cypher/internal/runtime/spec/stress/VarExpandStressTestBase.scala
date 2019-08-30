/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class VarExpandStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.expand(s"($variable)-[:NEXT*1..2]->(next)"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          hop <- (1 to 5) ++ // length 1 paths
                 (1 to 5).flatMap(hop1 => (1 to 5).map(_ + hop1)) // length 2 paths
        } yield Array(x, nodes((x.getId.toInt + hop) % nodes.length)),
      Seq("x", "next")
    )
}
