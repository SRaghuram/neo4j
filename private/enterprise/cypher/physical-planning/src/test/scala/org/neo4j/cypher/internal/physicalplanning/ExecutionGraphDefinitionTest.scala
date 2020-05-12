/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher.newGraph
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher.plan
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinitionMatcher.start
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class ExecutionGraphDefinitionTest extends CypherFunSuite {

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
        .pipeline(0, Seq(classOf[AllNodesScan], classOf[Limit], classOf[ProduceResult]), serial = true, workLimiter = Some(ArgumentStateMapId(0)))
        .end

      start(graph).applyBuffer(0).canceller(0, 1, TopLevelArgument.SLOT_OFFSET)
      start(graph).morselBuffer(1).canceller(0)
    }
  }

  test("should plan distinct") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .distinct("n AS n")
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 2)
        .delegateToMorselBuffer(1, 2)
        .pipeline(0, Seq(classOf[AllNodesScan], classOf[Distinct], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(0).downstreamStateOnRHS(0, 1, TopLevelArgument.SLOT_OFFSET)
    }
  }

  test("should plan hash join") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .nodeHashJoin("n").withBreak()
      .|.nodeByLabelScan("n", "N", IndexOrderNone).withBreak()
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
      .|.nodeByLabelScan("m", "M", IndexOrderNone).withBreak()
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
        .argumentStreamBuffer(4, argumentSlotOffset, 0, 2)
        .pipeline(2, Seq(classOf[Optional], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(2).reducerOnRHS(0, 2, argumentSlotOffset)
      start(graph).morselBuffer(3).reducer(0)
    }
  }

  test("should plan anti") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .apply().withBreak()
      .|.anti().withBreak()
      .|.limit(1).withBreak()
      .|.filter("false")
      .|.argument("n").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {

      /*
      id  plan
      0   ProduceResult
      1   Apply
      2   Anti
      3   Limit
      4   Selection
      5   Argument
      6   AllNodeScan
      */

      val graph = newGraph
      val argumentSlotOffset = 1
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 6)
        .delegateToMorselBuffer(1, 6)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, argumentSlotOffset, 1)
        .delegateToMorselBuffer(3, 5)
        .pipeline(1, Seq(classOf[Argument], classOf[Selection], classOf[Limit]))
        .antiBuffer(4, argumentSlotOffset, 1, 2)
        .pipeline(2, Seq(classOf[Anti], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(2).reducerOnRHS(1, 2, argumentSlotOffset)
      start(graph).applyBuffer(2).canceller(0, 3, argumentSlotOffset)

      start(graph).morselBuffer(3).reducer(1, 2, argumentSlotOffset)
      start(graph).morselBuffer(3).canceller(0, 3, argumentSlotOffset)
    }
  }

  test("should plan cartesian product") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n", "m")
      .cartesianProduct().withBreak()
      .|.nodeByLabelScan("m", "M", IndexOrderNone).withBreak()
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
      .|.nodeByLabelScan("n", "N", IndexOrderNone).withBreak()
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

  test("should plan union with downstream states on RHS of apply with correct initialCount") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("n")
      .apply().withBreak()
      .|.sort(Seq(Ascending("n"))).withBreak()
      .|.limit(1)
      .|.distinct("n AS n")
      .|.union().withBreak()
      .|.|.nodeByLabelScan("n", "N", IndexOrderNone).withBreak()
      .|.argument("n").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph
      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 8)
        .delegateToMorselBuffer(1, 8)
        .pipeline(0, Seq(classOf[AllNodesScan]))
        .applyBuffer(2, 1, 1)
        .delegateToMorselBuffer(3, 7)
        .pipeline(1, Seq(classOf[Argument]))
        .morselBuffer(5, 5)

      start(graph)
        .applyBuffer(2)
        .delegateToMorselBuffer(4, 6)
        .pipeline(2, Seq(classOf[NodeByLabelScan]))
        .morselBuffer(5)
        .pipeline(3, Seq(classOf[Union], classOf[Distinct], classOf[Limit]))
        .argumentStateBuffer(6, 2, 2)
        .pipeline(4, Seq(classOf[Sort], classOf[ProduceResult]), serial = true)
        .end

      start(graph).applyBuffer(2).reducerOnRHS(2, 2, 1, 2)
      start(graph).applyBuffer(2).canceller(1, 3, 1, 2)
      start(graph).applyBuffer(2).downstreamStateOnRHS(0, 4, 1)
      start(graph).morselBuffer(3).reducer(2)
      start(graph).morselBuffer(3).canceller(1)
      start(graph).morselBuffer(4).reducer(2)
      start(graph).morselBuffer(4).canceller(1)
      start(graph).morselBuffer(5).reducer(2)
      start(graph).morselBuffer(5).canceller(1)
    }
  }

  test("should not fuse pipeline with single plan") {
    new ExecutionGraphDefinitionBuilder(operatorFuserFactory(classOf[AllNodesScan]))
      .produceResults("n")
      .expand("(n)-[r]->(m)").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 2)
        .delegateToMorselBuffer(1, 2)
        .pipeline(0, Seq(classOf[AllNodesScan]), fusedPlans = Seq.empty)
        .morselBuffer(2, 1)
        .pipeline(1, Seq(classOf[Expand], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should fuse full pipeline, ending in produce results") {
    new ExecutionGraphDefinitionBuilder(operatorFuserFactory(classOf[AllNodesScan], classOf[Selection], classOf[ProduceResult]))
      .produceResults("n")
      .filter("true")
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 2)
        .delegateToMorselBuffer(1, 2)
        .pipeline(0, Seq.empty, fusedPlans = Seq(classOf[AllNodesScan], classOf[Selection], classOf[ProduceResult]), serial = true)
        .endFused
    }
  }

  test("should fuse full pipeline, ending in aggregation") {
    new ExecutionGraphDefinitionBuilder(operatorFuserFactory(classOf[AllNodesScan], classOf[Selection], classOf[Aggregation]))
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count")).withBreak()
      .filter("true")
      .allNodeScan("n").withBreak()
      .build() should plan {
        val graph = newGraph
        start(graph)
          .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 3)
          .delegateToMorselBuffer(1, 3)
          .pipeline(0, Seq.empty, fusedPlans = Seq(classOf[AllNodesScan], classOf[Selection], classOf[Aggregation]))
          .argumentStateBuffer(2, 0, 1, fusedOutput = true)
          .pipeline(1, Seq(classOf[Aggregation], classOf[ProduceResult]), serial = true)
          .end

        start(graph).applyBuffer(0).reducerOnRHS(0, 1, TopLevelArgument.SLOT_OFFSET)
        start(graph).morselBuffer(1).reducer(0)
      }
  }

  test("should fuse partial pipeline, ending in produce results 1") {
    new ExecutionGraphDefinitionBuilder(operatorFuserFactory(classOf[AllNodesScan], classOf[Selection], classOf[ProduceResult]))
      .produceResults("x")
      .projection("n AS x") // not fusable
      .filter("true")
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 3)
        .delegateToMorselBuffer(1, 3)
        .pipeline(0, fusedPlans = Seq(classOf[AllNodesScan], classOf[Selection]), plans = Seq(classOf[Projection], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should fuse partial pipeline, ending in produce results 2") {
    new ExecutionGraphDefinitionBuilder(operatorFuserFactory(classOf[AllNodesScan], classOf[Selection], classOf[ProduceResult]))
      .produceResults("x")
      .filter("true")
      .projection("n AS x") // not fusable
      .filter("true")
      .allNodeScan("n").withBreak()
      .build() should plan {
      start(newGraph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 4)
        .delegateToMorselBuffer(1, 4)
        .pipeline(0, fusedPlans = Seq(classOf[AllNodesScan], classOf[Selection]), plans = Seq(classOf[Projection], classOf[Selection], classOf[ProduceResult]), serial = true)
        .end
    }
  }

  test("should propagate work limiter upstream") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("p")
      .expand("(o)-->(p)").withBreak()
      .limit(1)
      .expand("(m)-->(o)").withBreak()
      .expand("(n)-->(m)").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph

      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 5)
        .delegateToMorselBuffer(1, 5)
        .pipeline(0, Seq(classOf[AllNodesScan]), workLimiter = Some(ArgumentStateMapId(0)))
        .morselBuffer(2, 4)
        .pipeline(1, Seq(classOf[Expand]), workLimiter = Some(ArgumentStateMapId(0)))
        .morselBuffer(3, 3)
        .pipeline(2, Seq(classOf[Expand], classOf[Limit]), workLimiter = Some(ArgumentStateMapId(0)))
        .morselBuffer(4, 1)
        .pipeline(3, Seq(classOf[Expand], classOf[ProduceResult]), serial = true, workLimiter = None)
        .end

      start(graph).applyBuffer(0).canceller(0, 2, TopLevelArgument.SLOT_OFFSET)
      start(graph).morselBuffer(1).canceller(0)
      start(graph).morselBuffer(2).canceller(0)
      start(graph).morselBuffer(3).canceller(0)
    }
  }

  test("should handle multiple work limiters") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("p")
      .limit(1)
      .expand("(o)-->(p)").withBreak()
      .limit(10)
      .expand("(m)-->(o)").withBreak()
      .expand("(n)-->(m)").withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph

      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 6)
        .delegateToMorselBuffer(1, 6)
        .pipeline(0, Seq(classOf[AllNodesScan]), workLimiter = Some(ArgumentStateMapId(0)))
        .morselBuffer(2, 5)
        .pipeline(1, Seq(classOf[Expand]), workLimiter = Some(ArgumentStateMapId(0)))
        .morselBuffer(3, 4)
        .pipeline(2, Seq(classOf[Expand], classOf[Limit]), workLimiter = Some(ArgumentStateMapId(0)))
        .morselBuffer(4, 2)
        .pipeline(3, Seq(classOf[Expand],  classOf[Limit], classOf[ProduceResult]), serial = true, workLimiter = Some(ArgumentStateMapId(1)))
        .end

      start(graph).applyBuffer(0).canceller(0, 3, TopLevelArgument.SLOT_OFFSET)
      start(graph).applyBuffer(0).canceller(1, 1, TopLevelArgument.SLOT_OFFSET)
      start(graph).morselBuffer(1).canceller(0)
      start(graph).morselBuffer(1).canceller(1)
      start(graph).morselBuffer(2).canceller(0)
      start(graph).morselBuffer(2).canceller(1)
      start(graph).morselBuffer(3).canceller(0)
      start(graph).morselBuffer(3).canceller(1)
      start(graph).morselBuffer(4).canceller(1)
    }
  }

  test("should propagate top level limit only") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("m")
      .limit(1)
      .apply()
      .|.limit(10)
      .|.expand("(n)-->(m)")
      .|.argument().withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph

      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 6)
        .delegateToMorselBuffer(1, 6)
        .pipeline(0, Seq(classOf[AllNodesScan]), workLimiter = Some(ArgumentStateMapId(1)))
        .applyBuffer(2, 1, 2)
        .delegateToMorselBuffer(3, 5)
        .pipeline(1, Seq(classOf[Argument], classOf[Expand], classOf[Limit], classOf[Limit], classOf[ProduceResult]), serial = true, workLimiter = Some(ArgumentStateMapId(1)))
        .end

      start(graph).applyBuffer(0).canceller(1, 1, TopLevelArgument.SLOT_OFFSET)
      start(graph).applyBuffer(2).canceller(0, 3, 1)
      start(graph).morselBuffer(1).canceller(1)
      start(graph).morselBuffer(3).canceller(0)
      start(graph).morselBuffer(3).canceller(1)
    }
  }

  test("should not propagate limit under RHS of apply") {
    new ExecutionGraphDefinitionBuilder()
      .produceResults("m")
      .apply()
      .|.limit(10)
      .|.expand("(n)-->(m)")
      .|.argument().withBreak()
      .allNodeScan("n").withBreak()
      .build() should plan {
      val graph = newGraph

      start(graph)
        .applyBuffer(0, TopLevelArgument.SLOT_OFFSET, 5)
        .delegateToMorselBuffer(1, 5)
        .pipeline(0, Seq(classOf[AllNodesScan]), workLimiter = None)
        .applyBuffer(2, 1, 1)
        .delegateToMorselBuffer(3, 4)
        .pipeline(1, Seq(classOf[Argument], classOf[Expand], classOf[Limit], classOf[ProduceResult]), serial = true, workLimiter = None)
        .end

      start(graph).applyBuffer(2).canceller(0, 2, 1)
      start(graph).morselBuffer(3).canceller(0)
    }
  }

  def operatorFuserFactory(fusablePlans: Class[_ <: LogicalPlan]*): OperatorFuserFactory =
    new OperatorFuserFactory {
      val fusable = fusablePlans.toSet
      override def newOperatorFuser(headPlanId: Id,
                                    inputSlotConfiguration: SlotConfiguration): OperatorFuser =
        new OperatorFuser {
          val plans = new ArrayBuffer[LogicalPlan]
          override def fuseIn(plan: LogicalPlan): Boolean =
            if (fusable.contains(plan.getClass)) {
              plans += plan
              true
            } else {
              false
            }

          override def fuseIn(output: OutputDefinition): Boolean =
            output match {
              case ProduceResultOutput(p) => fuseIn(p)
              case ReduceOutput(_, _, p) => fuseIn(p)
              case _ => false
            }

          override def fusedPlans: IndexedSeq[LogicalPlan] = plans
        }
    }
}
