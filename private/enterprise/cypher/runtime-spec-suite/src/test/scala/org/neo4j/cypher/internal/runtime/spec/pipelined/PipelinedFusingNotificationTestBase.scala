/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.compiler.CodeGenerationFailedNotification
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class PipelinedFusingNotificationTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                              runtime: CypherRuntime[CONTEXT]) extends RuntimeTestSuite(edition, runtime) {

  test("should generate notification if fusing fails") {
    given { nodeGraph(1) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("var0")
      //There is a limit of 65535 fields on a class, so by exceeding that limit
      //we trigger a compilation error
      .projection( (0 to 65535).map { i => s"$i AS var$i" }: _*)
      .allNodeScan("n")
      .build()

    val plan = buildPlan(logicalQuery, runtime)

    plan.notifications.map(_.getClass) should contain(classOf[CodeGenerationFailedNotification])
  }
}
