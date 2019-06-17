/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.cypher.internal.{CypherRuntime, EnterpriseRuntimeContext}

abstract class FilterStressTestBase(runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends ParallelStressSuite(runtime)
    with OnTopOfParallelInputStressTest {

  override def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD =
    OnTopOfParallelInputTD(
      _.filter("prop < 10"),
      rowsComingIntoTheOperator =>
        for {
          Array(x) <- rowsComingIntoTheOperator if x.getId < 10
        } yield Array(x),
      Seq("x")
    )
}
