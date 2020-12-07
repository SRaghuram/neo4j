/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.breakFor
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection NameBooleanParameters
class SlotAllocationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  private val LABEL = labelName("label")
  private val semanticTable = SemanticTable()
  private val NO_EXPR_VARS = new AvailableExpressionVariables()
  private val leveragedOrders = new LeveragedOrders


  test("only single allnodes scan") {
    // given
    val plan = AllNodesScan("x", Set.empty)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
  }

  test("index seek without values") {
    // given
    val plan = IndexSeek("x:label2(prop = 42)", DoNotGetValue)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
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
    val plan = plans.Limit(AllNodesScan("x", Set.empty), literalInt(1))

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(plan.id) should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
  }

  test("single labelscan scan") {
    // given
    val plan = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 1
    allocations(plan.id) should equal(SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
  }

  test("labelscan with filtering") {
    // given
    val leaf = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val filter = Selection(Seq(trueLiteral), leaf)

    // when
    val allocations = SlotAllocation.allocateSlots(filter, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(leaf.id) should equal(SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
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
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = false, CTRelationship)
        .newLong("z", nullable = false, CTNode)
    )
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
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = false, CTRelationship)
    )
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan("x", Set.empty)
    val plan = Optional(leaf)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(plan.id) should equal(SlotConfiguration.empty
      .newLong("x", nullable = true, CTNode)
    )
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
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = true, CTRelationship)
        .newLong("z", nullable = true, CTNode)
    )
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
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = true, CTRelationship)
    )
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

  test("pruning var length expand with reference from-node") {
    // given
    val input = Input(Seq("x"))
    val expand = PruningVarExpand(input, "x", SemanticDirection.INCOMING, Seq.empty, "z", 1, 15)

    // when
    val allocations = SlotAllocation.allocateSlots(expand, semanticTable, breakFor(expand), NO_EXPR_VARS).slotConfigurations

    // then we'll end up with two pipelines
    allocations should have size 2
    val allNodeScanAllocations = allocations(input.id)
    allNodeScanAllocations should equal(
      SlotConfiguration.empty.newReference("x", nullable = true, CTAny)
    )

    val expandAllocations = allocations(expand.id)
    expandAllocations should equal(
      SlotConfiguration.empty
        .newReference("x", nullable = true, CTAny)
        .newLong("z", nullable = false, CTNode)
    )
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val skip = plans.Skip(allNodesScan, literalInt(42))

    // when
    val allocations = SlotAllocation.allocateSlots(skip, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.id)
    labelScanAllocations should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))

    val expandAllocations = allocations(skip.id)
    expandAllocations shouldBe theSameInstanceAs(labelScanAllocations)
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val rhs = IndexSeek("z:label2(prop = 42)", argumentIds = Set("x"))
    val apply = Apply(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(apply, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(lhs.id) should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))

    val rhsPipeline = allocations(rhs.id)

    rhsPipeline should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("z", nullable = false, CTNode)
)

    allocations(apply.id) shouldBe theSameInstanceAs(rhsPipeline)
  }

  test("aggregation used for distinct") {
    // given
    val leaf = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
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
    val leaf = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
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
    val leaf = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val optional = Optional(leaf)
    val countStar = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"),
        "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpressions = Map("count(*)" -> CountStar()(pos)))

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
    val leaf = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val allocations = SlotAllocation.allocateSlots(projection, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 2
    allocations(leaf.id) should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newReference("x.propertyKey", nullable = true, CTAny)
    )
    allocations(projection.id) shouldBe theSameInstanceAs(allocations(leaf.id))
  }

  test("cartesian product") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val rhs = NodeByLabelScan("y", labelName("label2"), Set.empty, IndexOrderNone)
    val Xproduct = CartesianProduct(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(Xproduct, semanticTable, breakFor(Xproduct), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(lhs.id) should equal(SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
    allocations(rhs.id) should equal(SlotConfiguration.empty.newLong("y", nullable = false, CTNode))
    allocations(Xproduct.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTNode)
    )

  }

  test("cartesian product should allocate lhs followed by rhs, in order") {
    def expand(n: Int): LogicalPlan =
      n match {
        case 1 => NodeByLabelScan("n1", labelName("label2"), Set.empty, IndexOrderNone)
        case _ => Expand(expand(n - 1), "n" + (n - 1), SemanticDirection.INCOMING, Seq.empty, "n" + n, "r" + (n - 1), ExpandAll)
      }
    val N = 10

    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val rhs = expand(N)
    val Xproduct = CartesianProduct(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(Xproduct, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size N + 2

    val expectedPipelines =
      (1 until N).foldLeft(allocations(lhs.id))(
        (acc, i) =>
          acc
            .newLong("n" + i, false, CTNode)
            .newLong("r" + i, false, CTRelationship)
      ).newLong("n" + N, false, CTNode)

    allocations(Xproduct.id) should equal(expectedPipelines)
  }

  test("node hash join I") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val rhs = NodeByLabelScan("x", labelName("label2"), Set.empty, IndexOrderNone)
    val hashJoin = NodeHashJoin(Set("x"), lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(hashJoin, semanticTable, breakFor(hashJoin), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(lhs.id) should equal(SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
    allocations(rhs.id) should equal(SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
    allocations(hashJoin.id) should equal(SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
  }

  test("most joins - with LHS & RHS aliases") {
    // given
    val lhs =
      Projection(
        Projection(
          NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone),
          Map("cLhs" -> literalInt(1))
        ),
        Map("cLhs2" -> varFor("cLhs"), "xLhs2" -> varFor("x"))
      )

    val rhs = Projection(
      Projection(
        NodeByLabelScan("x", labelName("label2"), Set.empty, IndexOrderNone),
        Map("cRhs" -> literalInt(1))
      ),
      Map("cRhs2" -> varFor("cRhs"), "xRhs2" -> varFor("x"))
    )

    val joins =
      List(
        CartesianProduct(lhs, rhs),
        NodeHashJoin(Set("x"), lhs, rhs),
        LeftOuterHashJoin(Set("x"), lhs, rhs),
        ValueHashJoin(lhs, rhs, equals(varFor("x"), varFor("x"))),
      )

    for (join <- joins) {
      withClue(s"operator[${join.getClass.getSimpleName}]:") {
        // when
        val allocations = SlotAllocation.allocateSlots(join, semanticTable, breakFor(join), NO_EXPR_VARS).slotConfigurations

        // then
        val expectedJoinSlotConfig = SlotConfiguration.empty
          .newLong("x", nullable = false, CTNode)
          .newReference("cLhs", nullable = true, CTAny)
          .addAlias("cLhs2", "cLhs")
          .addAlias("xLhs2", "x")
          .newReference("cRhs", nullable = true, CTAny)
          .addAlias("cRhs2", "cRhs")
          .addAlias("xRhs2", "x")

        allocations(join.id) should equal(expectedJoinSlotConfig)
      }
    }
  }

  test("right outer join - with LHS & RHS aliases") {
    // given
    val lhs =
      Projection(
        Projection(
          NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone),
          Map("cLhs" -> literalInt(1))
        ),
        Map("cLhs2" -> varFor("cLhs"), "xLhs2" -> varFor("x"))
      )

    val rhs = Projection(
      Projection(
        NodeByLabelScan("x", labelName("label2"), Set.empty, IndexOrderNone),
        Map("cRhs" -> literalInt(1))
      ),
      Map("cRhs2" -> varFor("cRhs"), "xRhs2" -> varFor("x"))
    )

    val join = RightOuterHashJoin(Set("x"), lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(join, semanticTable, breakFor(join), NO_EXPR_VARS).slotConfigurations

    // then
    val expectedJoinSlotConfig = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newReference("cRhs", nullable = true, CTAny)
      .addAlias("cRhs2", "cRhs")
      .addAlias("xRhs2", "x")
      .newReference("cLhs", nullable = true, CTAny)
      .addAlias("cLhs2", "cLhs")
      .addAlias("xLhs2", "x")

    allocations(join.id) should equal(expectedJoinSlotConfig)
  }

  test("node hash join II") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val lhsE = Expand(lhs, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val rhs = NodeByLabelScan("x", labelName("label2"), Set.empty, IndexOrderNone)
    val rhsE = Expand(rhs, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r2", ExpandAll)

    val hashJoin = NodeHashJoin(Set("x"), lhsE, rhsE)

    // when
    val allocations = SlotAllocation.allocateSlots(hashJoin, semanticTable, breakFor(hashJoin), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5
    allocations(lhsE.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)
    )
    allocations(rhsE.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r2", nullable = false, CTRelationship)
      .newLong("z", nullable = false, CTNode)
    )
    allocations(hashJoin.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)
      .newLong("r2", nullable = false, CTRelationship)
      .newLong("z", nullable = false, CTNode)
    )
  }

  test("node hash join III") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val lhsE = Expand(lhs, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val rhs = NodeByLabelScan("x", labelName("label2"), Set.empty, IndexOrderNone)
    val rhsE = Expand(rhs, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r2", ExpandAll)

    val hashJoin = NodeHashJoin(Set("x", "y"), lhsE, rhsE)

    // when
    val allocations = SlotAllocation.allocateSlots(hashJoin, semanticTable, breakFor(hashJoin), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5 // One for each label-scan and expand, and one after the join
    allocations(lhsE.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)
    )
    allocations(rhsE.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r2", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)
    )
    allocations(hashJoin.id) should equal(      SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)
      .newLong("r2", nullable = false, CTRelationship)
    )
  }

  test("left joins should remember cached node properties from both sides") {
    // given
    val lhs = IndexSeek("x:L(lhsProp = 42)", GetValue)
    val rhs = IndexSeek("x:B(rhsProp = 42)", GetValue)

    val leftJoins =
      List(
        CartesianProduct(lhs, rhs),
        NodeHashJoin(Set("x"), lhs, rhs),
        LeftOuterHashJoin(Set("x"), lhs, rhs),
        ValueHashJoin(lhs, rhs, equals(varFor("x"), varFor("x")))
      )

    for (join <- leftJoins) {
      withClue(s"operator[${join.getClass.getSimpleName}]:") {
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
  }

  test("right outer join should remember cached node properties from both sides") {
    // given
    val lhs = IndexSeek("x:L(lhsProp = 42)", GetValue)
    val rhs = IndexSeek("x:B(rhsProp = 42)", GetValue)
    val join = RightOuterHashJoin(Set("x"), lhs, rhs)

    // when
    val joinAllocations = SlotAllocation.allocateSlots(join, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    joinAllocations(join.id) should be(
      SlotConfiguration.empty
        .newLong("x", false, CTNode)
        .newCachedProperty(cachedNodeProp("x", "rhsProp"))
        .newCachedProperty(cachedNodeProp("x", "lhsProp"))
    )
  }

  test("left joins should correctly handle cached node property argument") {
    // given
    val lhs = IndexSeek("x:L(lhsProp = 42)", GetValue)
    val rhs = IndexSeek("x:B(rhsProp = 42)", GetValue)
    val arg = IndexSeek("x:A(argProp = 42)", GetValue)

    val joins =
      List(
        CartesianProduct(lhs, rhs),
        NodeHashJoin(Set("x"), lhs, rhs),
        LeftOuterHashJoin(Set("x"), lhs, rhs),
        plans.ValueHashJoin(lhs, rhs, equals(varFor("x"), varFor("x")))
      )

    for (join <- joins) {
      withClue(s"operator[${join.getClass.getSimpleName}]:") {
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
  }

  test("right outer join should correctly handle cached node property argument") {
    // given
    val lhs = IndexSeek("x:L(lhsProp = 42)", GetValue)
    val rhs = IndexSeek("x:B(rhsProp = 42)", GetValue)
    val arg = IndexSeek("x:A(argProp = 42)", GetValue)

    val join = RightOuterHashJoin(Set("x"), lhs, rhs)

    // when
    val plan = Apply(arg, join)
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations(plan.id) should be(
      SlotConfiguration.empty
        .newLong("x", false, CTNode)
        .newCachedProperty(cachedNodeProp("x", "argProp"))
        .newCachedProperty(cachedNodeProp("x", "rhsProp"))
        .newCachedProperty(cachedNodeProp("x", "lhsProp"))
    )
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val arg = Argument(Set("x"))
    val rhs = Expand(arg, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)

    val apply = Apply(lhs, rhs)

    // when
    val allocations = SlotAllocation.allocateSlots(apply, semanticTable, breakFor(arg, rhs), NO_EXPR_VARS).slotConfigurations

    // then
    val lhsPipeline = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val rhsPipeline =       SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)

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
    val produceResult = plans.ProduceResult(unwind, Seq("x"))

    // when
    val allocations = SlotAllocation.allocateSlots(produceResult, semanticTable, breakFor(unwind), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 3
    allocations(leaf.id) should equal(SlotConfiguration.empty)

    allocations(unwind.id) should equal(SlotConfiguration.empty.newReference("x", nullable = true, CTAny))
    allocations(produceResult.id) shouldBe theSameInstanceAs(allocations(unwind.id))
  }

  test("unwind and project and sort") {
    // given UNWIND [1,2,3] as x RETURN x ORDER BY x
    val leaf = Argument()
    val unwind = UnwindCollection(leaf, "x", listOfInt(1, 2, 3))
    val sort = plans.Sort(unwind, List(Ascending("x")))
    val produceResult = plans.ProduceResult(sort, Seq("x"))

    // when
    val allocations = SlotAllocation.allocateSlots(produceResult, semanticTable, breakFor(unwind), NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 4
    allocations(leaf.id) should equal(SlotConfiguration.empty)

    val expectedPipeline = SlotConfiguration.empty.newReference("x", nullable = true, CTAny)
    allocations (unwind.id) should equal(expectedPipeline)
    allocations(sort.id) shouldBe theSameInstanceAs(allocations(unwind.id))
    allocations(produceResult.id) shouldBe theSameInstanceAs(allocations(unwind.id))
  }

  test("semi apply") {
    // MATCH (x) WHERE (x) -[:r]-> (y) ....
    testSemiApply(SemiApply(_, _))
  }

  test("anti semi apply") {
    // MATCH (x) WHERE NOT (x) -[:r]-> (y) ....
    testSemiApply(AntiSemiApply(_, _))
  }

  def testSemiApply(
                     semiApplyBuilder: (LogicalPlan, LogicalPlan) => AbstractSemiApply
                   ): Unit = {
    val lhs = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val arg = Argument(Set("x"))
    val rhs = Expand(arg, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)
    val semiApply = semiApplyBuilder(lhs, rhs)
    val allocations = SlotAllocation.allocateSlots(semiApply, semanticTable, breakFor(rhs, semiApply), NO_EXPR_VARS).slotConfigurations

    val lhsPipeline = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val argumentSide = lhsPipeline

    val rhsPipeline =       SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("r", nullable = false, CTRelationship)
      .newLong("y", nullable = false, CTNode)

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
    val labelScan = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
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
    val labelScan = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
    val expand = Expand(labelScan, "x", SemanticDirection.INCOMING, Seq.empty, "y", "r", ExpandAll)
    val projectionExpressions = Map(
      "x" -> varFor("x"),
      "prop" -> prop("x", "prop"),
      "r" -> varFor("r")
    )
    val rhsProjection = Projection(expand, projectionExpressions)

    // RollUpApply(LHS, RHS, ...)
    val rollUp =
      RollUpApply(lhsLeaf, rhsProjection, "c", "x")

    // when
    val allocations = SlotAllocation.allocateSlots(rollUp, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS).slotConfigurations

    // then
    allocations should have size 5
    allocations(rollUp.id) should equal(
      SlotConfiguration.empty.newReference("c", nullable = false, CTList(CTAny)))
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
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode))
  }

  test("should handle UNION of primitive node with alias under apply") {
    // given
    val lhs = Projection(Argument(Set("x")), Map("y" -> varFor("x")))
    val rhs = Projection(Argument(Set("x")), Map("y" -> varFor("x")))
    val union = Union(lhs, rhs)
    val ans = AllNodesScan("x", Set.empty)
    val apply = Apply(ans, union)

    // when
    val allocations = SlotAllocation.allocateSlots(apply, semanticTable, BREAK_FOR_LEAFS, NO_EXPR_VARS, allocateArgumentSlots = true).slotConfigurations

    // then
    allocations should have size 7
    allocations(union.id) should equal(
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newArgument(apply.id)
        .newLong("y", nullable = false, CTNode)
    )
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
      SlotConfiguration.empty.newReference("x", nullable = false, CTAny))
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
      SlotConfiguration.empty.newReference("A", nullable = true, CTAny))
  }

  test("should handle nested plan expression") {
    val nestedPlan = AllNodesScan("x", Set.empty)
    val argument = Argument()
    val plan = Projection(argument, Map("z" -> NestedPlanExpression.collect(nestedPlan, literalString("foo"), literalString("foo"))(pos)))
    val availableExpressionVariables = new AvailableExpressionVariables
    availableExpressionVariables.set(nestedPlan.id, Seq.empty)

    // when
    val allocations = SlotAllocation.allocateSlots(plan, semanticTable, BREAK_FOR_LEAFS, availableExpressionVariables).slotConfigurations

    // then
    allocations should have size 3
    allocations(plan.id) should equal(
      SlotConfiguration.empty.newReference("z", nullable = true, CTAny)
    )
    allocations(argument.id) should equal(allocations(plan.id))
    allocations(nestedPlan.id) should equal(
      SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    )
  }

  test("foreach allocates on left hand side with integer list") {
    // given
    val lhs = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
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
    val lhs = NodeByLabelScan("x", LABEL, Set.empty, IndexOrderNone)
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
