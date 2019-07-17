/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.{AllNodesScan, Argument, Ascending, Limit, NodeByLabelScan, NodeHashJoin, Optional, ProduceResult, Sort}
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PipelineBuilderTest extends CypherFunSuite {

  test("should plan all node scan") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, 0)
        .delegateToMorselBuffer(1)
        .pipeline(0, Seq(classOf[AllNodesScan], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should plan reduce") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .sort(Seq(Ascending("n"))).withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, 0)
        .delegateToMorselBuffer(1)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .argumentStateBuffer(2, 0)
        .pipeline(1, Seq(classOf[Sort], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(0).reducerOnRHS(0, 1, 0)
      start(graph).morselBuffer(1).reducer(0)
    }
  }

  test("should plan limit") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .limit(1)
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, 0)
        .delegateToMorselBuffer(1)
        .pipeline(0, Seq(classOf[AllNodesScan], classOf[Limit], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(0).canceller(0, 1, 0)
      start(graph).morselBuffer(1).canceller(0)
    }
  }

  test("should plan hash join") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .nodeHashJoin("n").withBreak()
      .|.nodeByLabelScan("n", "N").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, 0)
        .delegateToMorselBuffer(1)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .leftOfJoinBuffer(3, 0, 0, 1)
        .pipeline(2, Seq(classOf[NodeHashJoin], classOf[ProduceResult]), serial = true)
        .end

      start(graph)
        .applyBuffer(0)
        .delegateToMorselBuffer(2)
        .pipeline(1, Seq(classOf[NodeByLabelScan]))
        .rightOfJoinBuffer(3, 0)

      start(graph).applyBuffer(0).reducerOnRHS(0, 1, 0)
      start(graph).morselBuffer(1).reducer(0)
      start(graph).applyBuffer(0).reducerOnRHS(1, 1, 0)
      start(graph).morselBuffer(2).reducer(1)
    }
  }

  test("should plan apply") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .apply().withBreak()
      .|.nodeByLabelScan("m", "M").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, 0)
        .delegateToMorselBuffer(1)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, 2)
        .delegateToMorselBuffer(3)
        .pipeline(1, Seq(classOf[NodeByLabelScan], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should plan optional") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .apply().withBreak()
      .|.optional("n").withBreak()
      .|.argument("n").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, 0)
        .delegateToMorselBuffer(1)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, 2)
        .delegateToMorselBuffer(3)
        .pipeline(1, Seq(classOf[Argument]))
        .optionalBuffer(4, 2, 0)
        .pipeline(2, Seq(classOf[Optional], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(2).reducerOnRHS(0, 2, 2)
      start(graph).morselBuffer(3).reducer(0)
    }
  }
}
