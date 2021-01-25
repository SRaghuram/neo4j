/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile

abstract class ProfileNoTimeTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                runtime: CypherRuntime[CONTEXT],
                                                                sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  // We always get OperatorProfile.NO_DATA for page cache hits and misses in Pipelined
  protected val NO_PROFILE = new OperatorProfile.ConstOperatorProfile(0, 0, 0, 0, 0, OperatorProfile.NO_DATA)

  test("should profile time at first operator if completely fused") {
    given { nodeGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be(OperatorProfile.NO_DATA) // produce results
    queryProfile.operatorProfile(1).time() should not be OperatorProfile.NO_DATA // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time at the first fused operator after boundary between fused and not-fused") {
    given { circleGraph(sizeHint, "X") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")       //FUSED
      .filter("true") //FUSED
      .expand("(x)-->(y)")    //FUSED
      .nonFuseable()
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be(OperatorProfile.NO_DATA) // produce results
    queryProfile.operatorProfile(1).time() should be(OperatorProfile.NO_DATA) // filter
    queryProfile.operatorProfile(2).time() should not be OperatorProfile.NO_DATA // expand
    queryProfile.operatorProfile(3).time() should not be OperatorProfile.NO_DATA // nonFuseable
    queryProfile.operatorProfile(4).time() should not be OperatorProfile.NO_DATA // labelscan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }
}