/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.ir.{CreateNode, VarPatternLength}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.logical.{plans => logicalPlans}
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.breakFor
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.v4_0.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

//noinspection NameBooleanParameters
class SlotAllocationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  private val LABEL = labelName("label")
  private val semanticTable = SemanticTable()
  private val NO_EXPR_VARS = new AvailableExpressionVariables()

  test("only single allnodes scan") {
    // given
    val plan = AllNodesScan("x", Set.empty)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))
  }

  test("index seek without values") {
    // given
    val plan = IndexSeek("x:label2(prop = 42)", DoNotGetValue)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))
  }

  test("index seek with values") {
    // given
    val plan = IndexSeek("x:label2(prop = 42)", GetValue)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newCachedProperty(cachedNodeProp("x", "prop"))
    )
  }

  test("limit should not introduce slots") {
    // given
    val plan = logicalPlans.Limit(AllNodesScan("x", Set.empty), literalInt(1), DoNotIncludeTies)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(plan.id) should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))
  }

  test("single labelscan scan") {
    // given
    val plan = NodeByLabelScan("x", LABEL, Set.empty)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))
  }

  test("labelscan with filtering") {
    // given
    val leaf = NodeByLabelScan("x", LABEL, Set.empty)
    val filter = Selection(Seq(trueLiteral), leaf)

    // when
    val allocations = SlotAllocation.allocateSlots(filter, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(leaf.id) should equal(SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))
    allocations(filter.id) shouldBe theSameInstanceAs(allocations(leaf.id))
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = Expand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val allocations = SlotAllocation.allocateSlots(expand, semanticTable, breakFor(expand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.id)
    labelScanAllocations should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = false, CTRelationship),
        "z" -> LongSlot(2, nullable = false, CTNode)), numberOfLongs = 3, numberOfReferences = 0))
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = Expand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "x", "r", ExpandInto)

    // when
    val allocations = SlotAllocation.allocateSlots(expand, semanticTable, breakFor(expand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.id)
    labelScanAllocations should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode), "r" -> LongSlot(1, nullable = false,
        CTRelationship)), numberOfLongs = 2, numberOfReferences = 0))
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan("x", Set.empty)
    val plan = Optional(leaf)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(plan.id) should equal(SlotConfiguration(Map("x" -> LongSlot(0, nullable = true, CTNode)), 1, 0))
  }

  test("single node with optionalExpand ExpandAll") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = OptionalExpand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val allocations = SlotAllocation.allocateSlots(expand, semanticTable, breakFor(expand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.id)
    labelScanAllocations should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences =
        0))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = true, CTRelationship),
        "z" -> LongSlot(2, nullable = true, CTNode)
      ), numberOfLongs = 3, numberOfReferences = 0))
  }

  test("single node with optionalExpand ExpandInto") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = OptionalExpand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "x", "r", ExpandInto)

    // when
    val allocations = SlotAllocation.allocateSlots(expand, semanticTable, breakFor(expand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.id)
    labelScanAllocations should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = true, CTRelationship)
      ), numberOfLongs = 2, numberOfReferences = 0))
  }

  test("single node with var length expand") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val varLength = VarPatternLength(1, Some(15))
    val expand = VarExpand(allNodesScan, "x", SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty, "z", "r",
      varLength, ExpandAll, Some(VariablePredicate(exprVar(0, "r_NODES"), trueLiteral)), Some(VariablePredicate(exprVar(1, "r_EDGES"), trueLiteral)))

    // when
    val allocations = SlotAllocation.allocateSlots(expand, semanticTable, breakFor(expand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with two pipelines
    allocations should have size 2
    val allNodeScanAllocations = allocations(allNodesScan.id)
    allNodeScanAllocations should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    )

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newReference("r", nullable = false, CTList(CTRelationship))
        .newLong("z", nullable = false, CTNode)
    )
  }

  test("single node with var length expand into") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = Expand(allNodesScan, "x", SemanticDirection.OUTGOING, Seq.empty, "y", "r", ExpandAll)
    val varLength = VarPatternLength(1, Some(15))
    val varExpand = VarExpand(expand, "x", SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty, "y", "r2",
      varLength, ExpandInto, Some(VariablePredicate(exprVar(0, "r_NODES"), trueLiteral)), Some(VariablePredicate(exprVar(1, "r_EDGES"), trueLiteral)))

    // when
    val allocations = SlotAllocation.allocateSlots(varExpand, semanticTable, breakFor(expand, varExpand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with three pipelines
    allocations should have size 3
    val allNodeScanAllocations = allocations(allNodesScan.id)
    allNodeScanAllocations should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    )

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = false, CTRelationship)
        .newLong("y", nullable = false, CTNode)
    )

    val varExpandAllocations = allocations(varExpand.id)
    varExpandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = false, CTRelationship)
        .newLong("y", nullable = false, CTNode)
        .newReference("r2", nullable = false, CTList(CTRelationship))
    )
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val skip = logicalPlans.Skip(allNodesScan, literalInt(42))

    // when
    val allocations = SlotAllocation.allocateSlots(skip, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.id)
    labelScanAllocations should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(skip.id)
    expandAllocations shouldBe theSameInstanceAs(labelScanAllocations)
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan("x", LABEL, Set.empty)
    val rhs = IndexSeek("z:label2(prop = 42)", argumentIds = Set("x"))
    val apply = Apply(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(apply, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(lhs.id) should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))

    val rhsPipeline = allocations(rhs.id)

    rhsPipeline should equal(
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "z" -> LongSlot(1, nullable = false, CTNode)
      ), numberOfLongs = 2, numberOfReferences = 0))

    allocations(apply.id) shouldBe theSameInstanceAs(rhsPipeline)
  }

  test("aggregation used for distinct") {
    // given
    val leaf = NodeByLabelScan("x", LABEL, Set.empty)
    val distinct = Aggregation(leaf, Map("x" -> varFor("x")), Map.empty)

    // when
    val allocations = SlotAllocation.allocateSlots(distinct, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    val expected = SlotConfiguration.empty.newLong("x", false, CTNode)

    allocations should have size 2
    allocations(leaf.id) should equal(expected)
    allocations(distinct.id) should equal(expected)
  }

  test("optional travels through aggregation used for distinct") {
    // given OPTIONAL MATCH (x) RETURN DISTINCT x, x.propertyKey
    val leaf = NodeByLabelScan("x", LABEL, Set.empty)
    val optional = Optional(leaf)
    val distinct = Distinct(optional, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val allocations = SlotAllocation.allocateSlots(distinct, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    val expected =
      SlotConfiguration.empty
        .newLong("x", true, CTNode)
        .newReference("x.propertyKey", true, CTAny)

    allocations should have size 3
    allocations(leaf.id) should equal(expected)
    allocations(optional.id) should equal(expected)
    allocations(distinct.id) should equal(expected)
  }

  test("optional travels through aggregation") {
    // given OPTIONAL MATCH (x) RETURN x, x.propertyKey, count(*)
    val leaf = NodeByLabelScan("x", LABEL, Set.empty)
    val optional = Optional(leaf)
    val countStar = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"),
        "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map("count(*)" -> CountStar()(pos)))

    // when
    val allocations = SlotAllocation.allocateSlots(countStar, semanticTable, breakFor(countStar), NO_EXPR_VARS).slotConfigurations

    // then
    val leafExpected = SlotConfiguration.empty.newLong("x", true, CTNode)
    val aggrExpected =
      SlotConfiguration.empty
        .newLong("x", true, CTNode)
        .newReference("x.propertyKey", true, CTAny)
        .newReference("count(*)", true, CTAny)

    allocations should have size 3
    allocations(leaf.id) should equal(leafExpected)

    allocations(optional.id) should be theSameInstanceAs allocations(leaf.id)
    allocations(countStar.id) should equal(aggrExpected)
  }

  test("labelscan with projection") {
    // given
    val leaf = NodeByLabelScan("x", LABEL, Set.empty)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val allocations = SlotAllocation.allocateSlots(projection, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(leaf.id) should equal(SlotConfiguration(numberOfLongs = 1, numberOfReferences = 1, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "x.propertyKey" -> RefSlot(0, nullable = true, CTAny)
    )))
    allocations(projection.id) shouldBe theSameInstanceAs(allocations(leaf.id))
  }

  test("cartesian product") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
    val rhs = NodeByLabelScan("y", labelName("label2"), Set.empty)
    val Xproduct = CartesianProduct(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(Xproduct, semanticTable, breakFor(Xproduct), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(lhs.id) should equal(SlotConfiguration(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode)
    )))
    allocations(rhs.id) should equal(SlotConfiguration(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "y" -> LongSlot(0, nullable = false, CTNode)
    )))
    allocations(Xproduct.id) should equal(SlotConfiguration(numberOfLongs = 2, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "y" -> LongSlot(1, nullable = false, CTNode)
    )))

  }

  test("cartesian product should allocate lhs followed by rhs, in order") {
    def expand(n:Int): LogicalPlan =
      n match {
        case 1 => NodeByLabelScan("n1", labelName("label2"), Set.empty)
        case _ => Expand(expand(n-1), "n"+(n-1), SemanticDirection.INCOMING, Seq.empty, "n"+n, "r"+(n-1), ExpandAll)
      }
    val N = 10

    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
    val rhs = expand(N)
    val Xproduct = CartesianProduct(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(Xproduct, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size N+2

    val expectedPipelines =
      (1 until N).foldLeft(allocations(lhs.id))(
        (acc, i) =>
          acc
            .newLong("n"+i, false, CTNode)
            .newLong("r"+i, false, CTRelationship)
      ).newLong("n"+N, false, CTNode)

    allocations(Xproduct.id) should equal(expectedPipelines)
  }

  test("node hash join I") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
    val rhs = NodeByLabelScan("x", labelName("label2"), Set.empty)
    val hashJoin = NodeHashJoin(Set("x"), lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(hashJoin, semanticTable, breakFor(hashJoin), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(lhs.id) should equal(SlotConfiguration(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode)
    )))
    allocations(rhs.id) should equal(SlotConfiguration(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode)
    )))
    allocations(hashJoin.id) should equal(SlotConfiguration(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode)
    )))
  }

  test("node hash join II") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
    val lhsE = Expand(lhs, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val rhs = NodeByLabelScan("x", labelName("label2"), Set.empty)
    val rhsE = Expand(rhs, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r2", ExpandAll)

    val hashJoin = NodeHashJoin(Set("x"), lhsE, rhsE)

    // when
    val allocations = SlotAllocation.allocateSlots(hashJoin, semanticTable, breakFor(hashJoin), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5
    allocations(lhsE.id) should equal(SlotConfiguration(numberOfLongs = 3, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship),
      "y" -> LongSlot(2, nullable = false, CTNode)
    )))
    allocations(rhsE.id) should equal(SlotConfiguration(numberOfLongs = 3, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r2" -> LongSlot(1, nullable = false, CTRelationship),
      "z" -> LongSlot(2, nullable = false, CTNode)
    )))
    allocations(hashJoin.id) should equal(SlotConfiguration(numberOfLongs = 5, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship),
      "y" -> LongSlot(2, nullable = false, CTNode),
      "r2" -> LongSlot(3, nullable = false, CTRelationship),
      "z" -> LongSlot(4, nullable = false, CTNode)

    )))
  }

  test("node hash join III") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
    val lhsE = Expand(lhs, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val rhs = NodeByLabelScan("x", labelName("label2"), Set.empty)
    val rhsE = Expand(rhs, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r2", ExpandAll)

    val hashJoin = NodeHashJoin(Set("x", "y"), lhsE, rhsE)

    // when
    val allocations = SlotAllocation.allocateSlots(hashJoin, semanticTable, breakFor(hashJoin), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5 // One for each label-scan and expand, and one after the join
    allocations(lhsE.id) should equal(SlotConfiguration(numberOfLongs = 3, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship),
      "y" -> LongSlot(2, nullable = false, CTNode)
    )))
    allocations(rhsE.id) should equal(SlotConfiguration(numberOfLongs = 3, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r2" -> LongSlot(1, nullable = false, CTRelationship),
      "y" -> LongSlot(2, nullable = false, CTNode)
    )))
    allocations(hashJoin.id) should equal(SlotConfiguration(numberOfLongs = 4, numberOfReferences = 0, slots =
      Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship),
      "y" -> LongSlot(2, nullable = false, CTNode),
      "r2" -> LongSlot(3, nullable = false, CTRelationship)
    )))
  }

  test("joins should remember cached node properties from both sides") {
    // given
    val lhs = IndexSeek("x:L(lhsProp = 42)", GetValue)
    val rhs = IndexSeek("x:B(rhsProp = 42)", GetValue)

    val joins =
      List(
        CartesianProduct(lhs, rhs),
        NodeHashJoin(Set("x"), lhs, rhs),
        LeftOuterHashJoin(Set("x"), lhs, rhs),
        RightOuterHashJoin(Set("x"), lhs, rhs),
        ValueHashJoin(lhs, rhs, equals(varFor("x"), varFor("x")))
      )

    for (join <- joins) {
      // when
      val joinAllocations = SlotAllocation.allocateSlots(join, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

      // then
      joinAllocations(join.id) should be(
        SlotConfiguration.empty
          .newLong("x", false, CTNode)
          .newCachedProperty(cachedNodeProp("x", "lhsProp"))
          .newCachedProperty(cachedNodeProp("x", "rhsProp"))
      )
    }
  }

  test("joins should correctly handle cached node property argument") {
    // given
    val lhs = IndexSeek("x:L(lhsProp = 42)", GetValue)
    val rhs = IndexSeek("x:B(rhsProp = 42)", GetValue)
    val arg = IndexSeek("x:A(argProp = 42)", GetValue)

    val joins =
      List(
        CartesianProduct(lhs, rhs),
        NodeHashJoin(Set("x"), lhs, rhs),
        LeftOuterHashJoin(Set("x"), lhs, rhs),
        RightOuterHashJoin(Set("x"), lhs, rhs),
        logicalPlans.ValueHashJoin(lhs, rhs, equals(varFor("x"), varFor("x")))
      )

    for (join <- joins) {
      // when
      val plan = Apply(arg, join)
      val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

      // then
      allocations(plan.id) should be(
        SlotConfiguration.empty
          .newLong("x", false, CTNode)
          .newCachedProperty(cachedNodeProp("x", "argProp"))
          .newCachedProperty(cachedNodeProp("x", "lhsProp"))
          .newCachedProperty(cachedNodeProp("x", "rhsProp"))
      )
    }
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan("x", LABEL, Set.empty)
    val arg = Argument(Set("x"))
    val rhs = Expand(arg, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val apply = Apply(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(apply, semanticTable, breakFor(arg, rhs), NO_EXPR_VARS).slotConfigurations

    // then
    val lhsPipeline = SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode)),
      numberOfLongs = 1, numberOfReferences = 0)

    val rhsPipeline = SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "y" -> LongSlot(2, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship)
    ), numberOfLongs = 3, numberOfReferences = 0)

    allocations should have size 4
    allocations(arg.id) should equal(lhsPipeline)
    allocations(lhs.id) should equal(lhsPipeline)
    allocations(apply.id) should equal(rhsPipeline)
    allocations(rhs.id) should equal(rhsPipeline)
  }

  test("unwind and project") {
    // given UNWIND [1,2,3] as x RETURN x
    val leaf = Argument()
    val unwind = UnwindCollection(leaf, "x", listOfInt(1, 2, 3))
    val produceResult = logicalPlans.ProduceResult(unwind, Seq("x"))

    // when
    val allocations = SlotAllocation.allocateSlots(produceResult, semanticTable, breakFor(unwind), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(leaf.id) should equal(SlotConfiguration(Map.empty, 0, 0))


    allocations(unwind.id) should equal(SlotConfiguration(numberOfLongs = 0, numberOfReferences = 1, slots = Map(
      "x" -> RefSlot(0, nullable = true, CTAny)
    )))
    allocations(produceResult.id) shouldBe theSameInstanceAs(allocations(unwind.id))
  }

  test("unwind and project and sort") {
    // given UNWIND [1,2,3] as x RETURN x ORDER BY x
    val leaf = Argument()
    val unwind = UnwindCollection(leaf, "x", listOfInt(1, 2, 3))
    val sort = logicalPlans.Sort(unwind, List(Ascending("x")))
    val produceResult = logicalPlans.ProduceResult(sort, Seq("x"))

    // when
    val allocations = SlotAllocation.allocateSlots(produceResult, semanticTable, breakFor(unwind), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 4
    allocations(leaf.id) should equal(SlotConfiguration(Map.empty, 0, 0))


    val expectedPipeline = SlotConfiguration(numberOfLongs = 0, numberOfReferences = 1, slots = Map(
      "x" -> RefSlot(0, nullable = true, CTAny)
    ))
    allocations(unwind.id) should equal(expectedPipeline)
    allocations(sort.id) shouldBe theSameInstanceAs(allocations(unwind.id))
    allocations(produceResult.id) shouldBe theSameInstanceAs(allocations(unwind.id))
  }

  test("semi apply") {
    // MATCH (x) WHERE (x) -[:r]-> (y) ....
    testSemiApply(SemiApply(_,_))
  }

  test("anti semi apply") {
    // MATCH (x) WHERE NOT (x) -[:r]-> (y) ....
    testSemiApply(AntiSemiApply(_,_))
  }

  def testSemiApply(
                     semiApplyBuilder: (LogicalPlan, LogicalPlan) => AbstractSemiApply
                   ): Unit = {
    val lhs = NodeByLabelScan("x", LABEL, Set.empty)
    val arg = Argument(Set("x"))
    val rhs = Expand(arg, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)
    val semiApply = semiApplyBuilder(lhs, rhs)
    val allocations = SlotAllocation.allocateSlots(semiApply, semanticTable, breakFor(rhs, semiApply), NO_EXPR_VARS).slotConfigurations

    val lhsPipeline = SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode)),
      numberOfLongs = 1, numberOfReferences = 0)

    val argumentSide = lhsPipeline

    val rhsPipeline = SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "y" -> LongSlot(2, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship)
    ), numberOfLongs = 3, numberOfReferences = 0)

    allocations should have size 4
    allocations(semiApply.id) should equal(lhsPipeline)
    allocations(lhs.id) should equal(lhsPipeline)
    allocations(rhs.id) should equal(rhsPipeline)
    allocations(arg.id) should equal(argumentSide)
  }

  test("argument on two sides of Apply") {
    val arg1 = Argument()
    val arg2 = Argument()
    val pr1 = Projection(arg1, Map("x" -> literalInt(42)))
    val pr2 = Projection(arg2, Map("y" -> literalInt(666)))
    val apply = Apply(pr1, pr2)

    // when
    val allocations = SlotAllocation.allocateSlots(apply, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5
    val lhsPipeline = SlotConfiguration.empty.newReference("x", true, CTAny)
    val rhsPipeline =
      SlotConfiguration.empty
        .newReference("x", true, CTAny)
        .newReference("y", true, CTAny)

    allocations(arg1.id) should equal(lhsPipeline)
    allocations(pr1.id) should equal(lhsPipeline)
    allocations(arg2.id) should equal(rhsPipeline)
    allocations(pr2.id) should equal(rhsPipeline)
    allocations(apply.id) should equal(rhsPipeline)
  }

  test("should allocate aggregation") {
    // Given MATCH (x)-[r:R]->(y) RETURN x, x.prop, count(r.prop)
    val labelScan = NodeByLabelScan("x", LABEL, Set.empty)
    val expand = Expand(labelScan, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val grouping = Map(
      "x" -> varFor("x"),
      "x.prop" -> prop("x", "prop")
    )
    val aggregations = Map("count(r.prop)" -> count(prop("r", "prop")))
    val aggregation = Aggregation(expand, grouping, aggregations)

    // when
    val allocations = SlotAllocation.allocateSlots(aggregation, semanticTable, breakFor(expand, aggregation), NO_EXPR_VARS).slotConfigurations

    allocations should have size 3
    allocations(expand.id) should equal(
      SlotConfiguration.empty
        .newLong("x", false, CTNode)
        .newLong("r", false, CTRelationship)
        .newLong("y", false, CTNode))

    allocations(aggregation.id) should equal(
      SlotConfiguration.empty
        .newLong("x", false, CTNode)
        .newReference("x.prop", true, CTAny)
        .newReference("count(r.prop)", true, CTAny))
  }

  test("should allocate RollUpApply") {
    // Given RollUpApply with RHS ~= MATCH (x)-[r:R]->(y) WITH x, x.prop as prop, r ...

    // LHS
    val lhsLeaf = Argument()

    // RHS
    val labelScan = NodeByLabelScan("x", LABEL, Set.empty)
    val expand = Expand(labelScan, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)
    val projectionExpressions = Map(
      "x" -> varFor("x"),
      "prop" -> prop("x", "prop"),
      "r" -> varFor("r")
    )
    val rhsProjection = Projection(expand, projectionExpressions)

    // RollUpApply(LHS, RHS, ...)
    val rollUp =
      RollUpApply(lhsLeaf, rhsProjection, "c", "x", nullableVariables = Set("r", "y"))

    // when
    val allocations = SlotAllocation.allocateSlots(rollUp, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5
    allocations(rollUp.id) should equal(
      SlotConfiguration(Map(
        "c" -> RefSlot(0, nullable = false, CTList(CTAny))
      ), numberOfLongs = 0, numberOfReferences = 1)
    )
  }

  test("should handle UNION of two primitive nodes") {
    // given
    val lhs = AllNodesScan("x", Set.empty)
    val rhs = AllNodesScan("x", Set.empty)
    val plan = Union(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(plan.id) should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))
  }

  test("should handle UNION of one primitive relationship and one node") {
    // given MATCH (y)<-[x]-(z) UNION MATCH (x) (sort of)
    val allNodesScan = AllNodesScan("y", Set.empty)
    val lhs = Expand(allNodesScan, "y", SemanticDirection.INCOMING, Seq.empty, "z", "x", ExpandAll)
    val rhs = AllNodesScan("x", Set.empty)
    val plan = Union(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 4
    allocations(plan.id) should equal(
      SlotConfiguration(Map("x" -> RefSlot(0, nullable = false, CTAny)), 0, 1))
  }

  test("should handle UNION of projected variables") {
    val allNodesScan = AllNodesScan("x", Set.empty)
    val lhs = Projection(allNodesScan, Map("A" -> varFor("x")))
    val rhs = Projection(Argument(), Map("A" -> literalInt(42)))
    val plan = Union(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5
    allocations(plan.id) should equal(
      SlotConfiguration(Map("A" -> RefSlot(0, nullable = true, CTAny)), 0, 1))
  }

  test("should handle nested plan expression") {
    val nestedPlan = AllNodesScan("x", Set.empty)
    val argument = Argument()
    val plan = Projection(argument, Map("z" -> NestedPlanExpression(nestedPlan, literalString("foo"))(pos)))
    val availableExpressionVariables = new AvailableExpressionVariables
    availableExpressionVariables.set(nestedPlan.id, Seq.empty)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, availableExpressionVariables).slotConfigurations

    // then
    allocations should have size 3
    allocations(plan.id) should equal(
      SlotConfiguration(Map("z" -> RefSlot(0, nullable = true, CTAny)), 0, 1)
    )
    allocations(argument.id) should equal(allocations(plan.id))
    allocations(nestedPlan.id) should equal(
      SlotConfiguration(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0)
    )
  }

  test("foreach allocates on left hand side with integer list") {
    // given
    val lhs = NodeByLabelScan("x", LABEL, Set.empty)
    val argument = Argument()
    val list = listOfInt(1, 2, 3)
    val rhs = Create(argument, List(CreateNode("z", Seq.empty, None)), Nil)
    val foreach = ForeachApply(lhs, rhs, "i", list)

    val semanticTableWithList = SemanticTable(ASTAnnotationMap(list -> ExpressionTypeInfo(ListType(CTInteger), Some(ListType(CTAny)))))

    // when
    val allocations = SlotAllocation.allocateSlots(foreach, semanticTableWithList, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 4

    val lhsSlots = allocations(lhs.id)
    lhsSlots should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newReference("i", nullable = true, CTAny)
    )

    val rhsSlots = allocations(rhs.id)
    rhsSlots should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("z", nullable = false, CTNode)
        .newReference("i", nullable = true, CTAny)
    )

    allocations(foreach.id) shouldBe theSameInstanceAs(lhsSlots)
  }

  test("foreach allocates on left hand side with node list") {
    // given
    val lhs = NodeByLabelScan("x", LABEL, Set.empty)
    val argument = Argument()
    val list = listOf(varFor("x"))
    val rhs = Create(argument, List(CreateNode("z", Seq.empty, None)), Nil)
    val foreach = ForeachApply(lhs, rhs, "i", list)

    val semanticTableWithList = SemanticTable(ASTAnnotationMap(list -> ExpressionTypeInfo(ListType(CTNode), Some(ListType(CTNode)))))

    // when
    val allocations = SlotAllocation.allocateSlots(foreach, semanticTableWithList, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 4

    val lhsSlots = allocations(lhs.id)
    lhsSlots should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("i", nullable = true, CTNode)
    )

    val rhsSlots = allocations(rhs.id)
    rhsSlots should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("i", nullable = true, CTNode)
        .newLong("z", nullable = false, CTNode)
    )

    allocations(foreach.id) shouldBe theSameInstanceAs(lhsSlots)
  }

  def exprVar(offset: Int, name: String): ExpressionVariable = ExpressionVariable(offset, name)
}
