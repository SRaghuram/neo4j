/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v4_0.CreateNode
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.symbols._

//noinspection NameBooleanParameters
class SlotAllocationArgumentsTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val semanticTable = SemanticTable()

  test("zero size argument for single all nodes scan") {
    // given
    val plan = AllNodesScan("x", Set.empty)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 1
    arguments(plan.id) should equal(Size(0, 0))
  }

  test("zero size argument for only leaf operator") {
    // given
    val leaf = AllNodesScan("x", Set.empty)
    val expand = Expand(leaf, "x", INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val arguments = SlotAllocation.allocateSlots(expand, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 1
    arguments(leaf.id) should equal(Size(0, 0))
  }

  test("zero size argument for argument operator") {
    val argument = Argument(Set.empty)

    // when
    val arguments = SlotAllocation.allocateSlots(argument, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 1
    arguments(argument.id) should equal(Size(0, 0))
  }

  test("correct long size argument for rhs leaf") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 1, 0), rhs)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.id) should equal(Size(0, 0))
    arguments(rhs.id) should equal(Size(1, 0))
  }

  test("correct ref size argument for rhs leaf") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 0, 1), rhs)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.id) should equal(Size(0, 0))
    arguments(rhs.id) should equal(Size(0, 1))
  }

  test("correct size argument for more slots") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 17, 11), rhs)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.id) should equal(Size(0, 0))
    arguments(rhs.id) should equal(Size(17, 11))
  }

  test("apply right keeps rhs slots") {
    // given
    //        applyRight
    //         \        \
    //    applyRight    leaf3
    //    /        \
    // +1 long   +1 ref
    //    |         |
    //  leaf1     leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val leaf3 = leaf()
    val plan = applyRight(applyRight(pipe(leaf1, 1, 0), pipe(leaf2, 0, 1)), leaf3)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(leaf3.id) should equal(Size(1, 1))
  }

  test("apply left ignores rhs slots") {
    // given
    //          applyRight
    //         /          \
    //     applyLeft     leaf3
    //    /        \
    // +1 long   +1 ref
    //    |         |
    //  leaf1     leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val leaf3 = leaf()
    val plan = applyRight(applyLeft(pipe(leaf1, 1, 0), pipe(leaf2, 0, 1)), leaf3)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(leaf3.id) should equal(Size(1, 0))
  }

  test("apply left argument does not leak downstream slots") {
    // given
    //       +1 ref
    //         /
    //     applyLeft
    //    /        \
    // +1 long   leaf2
    //    |
    //  leaf1

    val leaf1 = leaf()
    val leaf2 = leaf()
    val plan = pipe(applyLeft(pipe(leaf1, 1, 0), leaf2), 0, 1)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 2
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
  }

  test("argument is passed over pipeline break") {
    // given
    //     applyRight
    //    /        \
    // +1 long   +1 ref
    //    |       --|--
    //  leaf1     breaker
    //              |
    //            leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val plan = applyRight(pipe(leaf1, 1, 0), pipe(break(leaf2), 0, 1))

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 2
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
  }

  test("Optional should record argument size") {
    // given
    //     applyRight
    //    /        \
    // +1 long   optional
    //    |         |
    //  leaf1     leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val optional = Optional(leaf2, Set.empty)
    val plan = applyRight(pipe(leaf1, 1, 0), optional)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(optional.id) should equal(Size(1, 0))
  }

  test("Distinct should retain argument slots") {
    // given
    //     applyRight
    //    /        \
    // +1 long   distinct
    // +2 ref       |
    //    |       +2 long
    //  leaf1     +2 ref
    //              |
    //            leaf2

    val leaf1 = leaf()
    val lhs = pipe(leaf1, 1, 2, "lhsLong", "lhsRef")
    val leaf2 = leaf()
    val rhs = pipe(leaf2, 2, 2, "rhsLong", "rhsRef")
    val distinct = Distinct(rhs, Map("rhsLong0" -> varFor("rhsLong0"), "rhsRef1" -> varFor("rhsRef1")))
    val plan = applyRight(lhs, distinct)

    // when
    val slots = SlotAllocation.allocateSlots(plan, semanticTable, PipelineBreakingPolicy.breakFor(leaf1, leaf2, distinct)).slotConfigurations

    // then
    slots(distinct.id) should be(SlotConfiguration.empty
                                   .newLong("lhsLong0", false, CTNode)
                                   .newLong("rhsLong0", false, CTNode)
                                   .newReference("lhsRef0", true, CTAny)
                                   .newReference("lhsRef1", true, CTAny)
                                   .newReference("rhsRef1", true, CTAny)
    )
  }

  test("Aggregation should retain argument slots") {
    // given
    //     applyRight
    //    /        \
    // +1 long   aggregation
    // +2 ref       |
    //    |       +2 long
    //  leaf1     +2 ref
    //              |
    //            leaf2

    val leaf1 = leaf()
    val lhs = pipe(leaf1, 1, 2, "lhsLong", "lhsRef")
    val leaf2 = leaf()
    val rhs = pipe(leaf2, 2, 2, "rhsLong", "rhsRef")
    val aggregation = Aggregation(rhs, Map("rhsLong0" -> varFor("rhsLong0")), Map("rhsRef1" -> varFor("rhsRef1")))
    val plan = applyRight(lhs, aggregation)

    // when
    val slots = SlotAllocation.allocateSlots(plan, semanticTable, PipelineBreakingPolicy.breakFor(leaf1, leaf2, aggregation)).slotConfigurations

    // then
    slots(aggregation.id) should be(SlotConfiguration.empty
                                   .newLong("lhsLong0", false, CTNode)
                                   .newLong("rhsLong0", false, CTNode)
                                   .newReference("lhsRef0", true, CTAny)
                                   .newReference("lhsRef1", true, CTAny)
                                   .newReference("rhsRef1", true, CTAny)
    )
  }

  private def leaf() = Argument(Set.empty)
  private def applyRight(lhs:LogicalPlan, rhs:LogicalPlan) = Apply(lhs, rhs)
  private def applyLeft(lhs:LogicalPlan, rhs:LogicalPlan) = SemiApply(lhs, rhs)
  private def break(source:LogicalPlan) = Eager(source)
  private def pipe(source:LogicalPlan, nLongs:Int, nRefs:Int, longPrefix: String = "long", refPrefix: String = "ref") = {
    var curr: LogicalPlan =
      Create(
        source,
        (0 until nLongs).map(i => CreateNode(longPrefix+i, Nil, None)),
        Nil
      )

    for ( i <- 0 until nRefs ) {
      curr = UnwindCollection(curr, refPrefix+i, listOf(literalInt(1)))
    }
    curr
  }
}
