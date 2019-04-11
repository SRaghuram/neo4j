/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class UnwindStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.unwind(s"[$propVariable, 2 * $propVariable] AS i"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator
          f <- 1 to 2
        } yield Array(x.getId * f),
      Seq("i")
    )
}
