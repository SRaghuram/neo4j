/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher.newGraph
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher.plan
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher.start
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PipelineBuilderTest extends CypherFunSuite {

  test("should plan all node scan") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 1)
        .delegateToMorselBuffer(1, 1)
        .pipeline(0, Seq(classOf[AllNodesScan], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should plan expand") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .expand("(n)-[r]->(m)").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 2)
        .delegateToMorselBuffer(1, 2)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .morselBuffer(2, 1)
        .pipeline(1, Seq(classOf[Expand], classOf[ProduceResult]), serial = true)
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
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 2)
        .delegateToMorselBuffer(1, 2)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .argumentStateBuffer(2, 0, 1)
        .pipeline(1, Seq(classOf[Sort], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(0).reducerOnRHS(0, 1, TopLevelArgument.SLOT_OFFSET)
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
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 2)
        .delegateToMorselBuffer(1, 2)
        .pipeline(0, Seq(classOf[AllNodesScan], classOf[Limit], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(0).canceller(0, 1, TopLevelArgument.SLOT_OFFSET)
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
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 3)
        .delegateToMorselBuffer(1, 3)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .leftOfJoinBuffer(id = 3, TopLevelArgument.SLOT_OFFSET, asmId = 0, planId = 1)

      start(graph)
        .applyBuffer(0)
        .delegateToMorselBuffer(2, 2)
        .pipeline(1, Seq(classOf[NodeByLabelScan]))
        .rightOfJoinBuffer(lhsId = 3, rhsId = 4, sourceId = 5, TopLevelArgument.SLOT_OFFSET, rhsAsmId = 1, planId = 1)
        .pipeline(2, Seq(classOf[NodeHashJoin], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(0).reducerOnRHS(0, 1, TopLevelArgument.SLOT_OFFSET)
      start(graph).morselBuffer(1).reducer(0)
      start(graph).applyBuffer(0).reducerOnRHS(1, 1, TopLevelArgument.SLOT_OFFSET)
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
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 3)
        .delegateToMorselBuffer(1, 3)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, 1, 1)
        .delegateToMorselBuffer(3, 2)
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
      val argumentSlotOffset = 1
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 4)
        .delegateToMorselBuffer(1, 4)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, argumentSlotOffset, 1)
        .delegateToMorselBuffer(3, 3)
        .pipeline(1, Seq(classOf[Argument]))
        .optionalBuffer(4, argumentSlotOffset, 0, 2)
        .pipeline(2, Seq(classOf[Optional], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(2).reducerOnRHS(0, 2, argumentSlotOffset)
      start(graph).morselBuffer(3).reducer(0)
    }
  }

  test("should plan cartesian product") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n", "m")
      .cartesianProduct().withBreak()
      .|.nodeByLabelScan("m", "M").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 3)
        .delegateToMorselBuffer(1, 3)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .attachBuffer(2, planId = 1, slots = SlotConfiguration.empty.newArgument(Id(1)).newLong("m", nullable = false, CTNode))
        .lhsJoinSinkForAttach(lhsSinkId = 5, lhsAsmId = 0, planId = 1) // returns attach buffer
        .delegateToApplyBuffer(3, 0, 1)
        .delegateToMorselBuffer(4, 2)

      start(graph)
        .morselBuffer(4)
        .pipeline(1, Seq(classOf[NodeByLabelScan]))
        .rightOfJoinBuffer(lhsId = 5, rhsId = 6, sourceId = 7, argumentSlotOffset = 0, rhsAsmId = 1, planId = 1)
        .pipeline(2, Seq(classOf[CartesianProduct], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(3).reducerOnRHS(0, 1, 0)
      start(graph).applyBuffer(3).reducerOnRHS(1, 1, 0)
      start(graph).morselBuffer(4).reducer(1)
    }
  }

  test("should plan union") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .union().withBreak()
      .|.nodeByLabelScan("n", "N").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 3)
        .delegateToMorselBuffer(1, 3)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .morselBuffer(3, 1)

      start(graph)
        .applyBuffer(0)
        .delegateToMorselBuffer(2, 2)
        .pipeline(1, Seq(classOf[NodeByLabelScan]))
        .morselBuffer(3)
        .pipeline(2, Seq(classOf[Union], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should plan union with reducers on RHS of apply with correct initialCount") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .apply().withBreak()
      .|.sort(Seq(Ascending("n"))).withBreak()
      .|.union().withBreak()
      .|.|.nodeByLabelScan("n", "N").withBreak()
      .|.argument("n").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 6)
        .delegateToMorselBuffer(1, 6)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, 1, 1)
        .delegateToMorselBuffer(3, 5)
        .pipeline(1, Seq(classOf[Argument]))
          .morselBuffer(5, 3)

      start(graph)
        .applyBuffer(2)
        .delegateToMorselBuffer(4, 4)
        .pipeline(2, Seq(classOf[NodeByLabelScan]))
        .morselBuffer(5)
        .pipeline(3, Seq(classOf[Union]))
        .argumentStateBuffer(6, 0, 2)
        .pipeline(4, Seq(classOf[Sort], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(2).reducerOnRHS(0, 2, 1, 2)
      start(graph).morselBuffer(3).reducer(0)
      start(graph).morselBuffer(4).reducer(0)
      start(graph).morselBuffer(5).reducer(0)
    }
  }
}
