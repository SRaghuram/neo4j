/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanner
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.FakeEntityTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FilterPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LimitPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTreeBuilder
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SkipPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UniqueIndexSeek
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedPrimitiveGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.expressions.NodeProperty
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedCommandProjection
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllNodesScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ArgumentSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CartesianProductSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateNodeSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.EagerSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ExpandAllSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ExpandIntoSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ForeachSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeIndexScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeIndexSeekSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodesByLabelScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OptionalExpandAllSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OptionalExpandIntoSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OptionalSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnwindSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeEntity
import org.neo4j.kernel.impl.util.ValueUtils.fromRelationshipEntity
import org.neo4j.values.storable.Values

//noinspection NameBooleanParameters
class SlottedPipeMapperTest extends CypherFunSuite with LogicalPlanningTestSupport2 with FakeEntityTestSupport {

  implicit private val table: SemanticTable = SemanticTable()

  private def build(beforeRewrite: LogicalPlan): Pipe = {
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(0))
    val physicalPlan = PhysicalPlanner.plan(tokenContext, beforeRewrite, table, SlottedPipelineBreakingPolicy)
    val converters = new ExpressionConverters(SlottedExpressionConverters(physicalPlan),
      CommunityExpressionConverter(TokenContext.EMPTY))

    val fallback = InterpretedPipeMapper(true, converters, tokenContext, mock[QueryIndexRegistrator])(table)
    val pipeBuilder = new SlottedPipeMapper(fallback, converters, tokenContext, physicalPlan, true, mock[QueryIndexRegistrator])(table)
    PipeTreeBuilder(pipeBuilder).build(physicalPlan.logicalPlan)
  }

  private val label = labelName("label")
  private val X_NODE_SLOTS = SlotConfiguration.empty.newLong("x", false, CTNode)

  test("only single allnodes scan") {
    // given
    val plan: AllNodesScan = AllNodesScan("x", Set.empty)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)()
    )
  }

  test("single all nodes scan with limit") {
    // given
    val plan = plans.Limit(AllNodesScan("x", Set.empty), literalInt(1), DoNotIncludeTies)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      LimitPipe(
        AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
        Literal(1)
      )()
    )
  }

  test("eagerize before create node") {
    val allNodeScan: AllNodesScan = AllNodesScan("x", Set.empty)
    val eager = Eager(allNodeScan)
    val createNode = Create(eager, List(CreateNode("z", Seq(label), None)), Nil)

    // when
    val pipe = build(createNode)

    // then
    val beforeEagerSlots = SlotConfiguration.empty
      .newLong("x", false, CTNode)

    val afterEagerSlots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newLong("z", false, CTNode)

    pipe should equal(
      CreateSlottedPipe(
        EagerSlottedPipe(
          AllNodesScanSlottedPipe("x", beforeEagerSlots, Size.zero)(),
          afterEagerSlots)(),
        Array(CreateNodeSlottedCommand(afterEagerSlots.getLongOffsetFor("z"), Seq(LazyLabel(label)), None)),
        IndexedSeq()
      )()
    )
  }

  test("create node") {
    val argument = Argument()
    val createNode = Create(argument, List(CreateNode("z", Seq(label), None)), Nil)

    // when
    val pipe = build(createNode)

    // then
    val slots = SlotConfiguration.empty.newLong("z", false, CTNode)
    pipe should equal(
      CreateSlottedPipe(
        ArgumentSlottedPipe(slots, Size.zero)(),
        Array(CreateNodeSlottedCommand(slots.getLongOffsetFor("z"), Seq(LazyLabel(label)), None)),
        IndexedSeq()
      )()
    )
  }

  test("single label scan") {
    val plan = NodeByLabelScan("x", label, Set.empty, IndexOrderNone)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      NodesByLabelScanSlottedPipe("x", LazyLabel(label), X_NODE_SLOTS, Size.zero, IndexOrderNone)()
    )
  }

  test("label scan with filtering") {
    val leaf = NodeByLabelScan("x", label, Set.empty, IndexOrderNone)
    val filter = Selection(Seq(trueLiteral), leaf)

    // when
    val pipe = build(filter)

    // then
    pipe should equal(
      FilterPipe(
        NodesByLabelScanSlottedPipe("x", LazyLabel(label), X_NODE_SLOTS, Size.zero, IndexOrderNone)(),
        predicates.True()
      )()
    )
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = Expand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(2, nullable = false, CTNode)
    pipe should equal(ExpandAllSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      xNodeSlot, rRelSlot.offset, zNodeSlot.offset,
      SemanticDirection.INCOMING,
      RelationshipTypes.empty,
      SlotConfiguration.empty
          .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)
          .newLong("r", rRelSlot.nullable, rRelSlot.typ)
          .newLong("z", zNodeSlot.nullable, zNodeSlot.typ)
    )())
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = Expand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "x", "r", ExpandInto)

    // when
    val pipe = build(expand)

    // then
    val nodeSlot = LongSlot(0, nullable = false, CTNode)
    val relSlot = LongSlot(1, nullable = false, CTRelationship)
    pipe should equal(ExpandIntoSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      nodeSlot, relSlot.offset, nodeSlot, SemanticDirection.INCOMING, RelationshipTypes.empty,
      SlotConfiguration.empty
        .newLong("x", nodeSlot.nullable, nodeSlot.typ)
        .newLong("r", relSlot.nullable, relSlot.typ)
    )())
  }

  test("single optional node with expand") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val optional = Optional(allNodesScan)
    val expand = Expand(optional, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = true, CTNode)
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(2, nullable = false, CTNode)
    val allNodeScanSlots = SlotConfiguration.empty
      .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)
    val expandSlots = SlotConfiguration.empty
      .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)
      .newLong("r", rRelSlot.nullable, rRelSlot.typ)
      .newLong("z", zNodeSlot.nullable, zNodeSlot.typ)
    pipe should equal(
      ExpandAllSlottedPipe(
        OptionalSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
          Array(xNodeSlot),
          allNodeScanSlots,
          Size.zero
        )(),
        xNodeSlot, rRelSlot.offset, zNodeSlot.offset,
        SemanticDirection.INCOMING,
        RelationshipTypes.empty,
        expandSlots
      )())
  }

  test("single optional node with expand into") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val optional = Optional(allNodesScan)
    val expand = Expand(optional, "x", SemanticDirection.INCOMING, Seq.empty, "x", "r", ExpandInto)

    // when
    val pipe = build(expand)

    // then
    val nodeSlot = LongSlot(0, nullable = true, CTNode)
    val relSlot = LongSlot(1, nullable = false, CTRelationship)
    val allNodeScanSlots = SlotConfiguration.empty
      .newLong("x", nodeSlot.nullable, nodeSlot.typ)
    val expandSlots =SlotConfiguration.empty
      .newLong("x", nodeSlot.nullable, nodeSlot.typ)
      .newLong("r", relSlot.nullable, relSlot.typ)

    pipe should equal(
      ExpandIntoSlottedPipe(
        OptionalSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
          Array(nodeSlot),
          allNodeScanSlots,
          Size.zero
        )(),
        nodeSlot, relSlot.offset, nodeSlot, SemanticDirection.INCOMING, RelationshipTypes.empty,
        expandSlots)()
    )
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan("x", Set.empty)
    val plan = Optional(leaf)

    // when
    val pipe = build(plan)
    val nodeSlot = LongSlot(0, nullable = true, CTNode)

    // then
    val expectedSlots = SlotConfiguration.empty
      .newLong("x", nodeSlot.nullable, nodeSlot.typ)
    pipe should equal(OptionalSlottedPipe(
      AllNodesScanSlottedPipe("x", expectedSlots, Size.zero)(),
      Array(nodeSlot),
      expectedSlots,
      Size.zero
    )())
  }

  test("optional refslot value") {
    // given
    val arg = Argument(Set.empty)
    val project = Projection(arg, Map("x" -> literalInt(1)))
    val plan = Optional(project)

    // when
    val pipe = build(plan)
    val refSlot = RefSlot(0, nullable = true, CTAny)

    // then
    val expectedSlots = SlotConfiguration.empty
      .newReference("x", refSlot.nullable, refSlot.typ)
    pipe should equal(OptionalSlottedPipe(
      ProjectionPipe(
        ArgumentSlottedPipe(expectedSlots, Size.zero)(),
        SlottedCommandProjection(Map(0 -> Literal(1)))
      )(),
      Array(refSlot),
      expectedSlots,
      Size.zero
    )())
  }

  test("single node with optionalExpand ExpandAll") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = OptionalExpand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandAllSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      X_NODE_SLOTS("x"), 1, 2, SemanticDirection.INCOMING, RelationshipTypes.empty,
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = true, CTRelationship)
        .newLong("z", nullable = true, CTNode),
      None
    )())
  }

  test("single node with optionalExpand ExpandInto") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = OptionalExpand(allNodesScan, "x", SemanticDirection.INCOMING, Seq.empty, "x", "r", ExpandInto)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandIntoSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      X_NODE_SLOTS("x"), 1, X_NODE_SLOTS("x"), SemanticDirection.INCOMING, RelationshipTypes.empty,
      SlotConfiguration.empty
        .newLong("x", nullable = false, CTNode)
        .newLong("r", nullable = true, CTRelationship),
      None
    )())
  }

  test("single node with varlength expand") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val varLength = VarPatternLength(1, Some(15))
    val expand = VarExpand(allNodesScan, "x", SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      "z", "r", varLength, ExpandAll)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val zNodeSlot = LongSlot(1, nullable = false, CTNode)
    val rRelSlot = RefSlot(0, nullable = false, CTList(CTRelationship))

    val allNodeScanSlots = SlotConfiguration.empty
      .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)

    val varExpandSlots = SlotConfiguration.empty
      .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)
      .newReference("r", rRelSlot.nullable, rRelSlot.typ)
      .newLong("z", zNodeSlot.nullable, zNodeSlot.typ)

    pipe should equal(VarLengthExpandSlottedPipe(
      AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
      xNodeSlot, rRelSlot.offset, zNodeSlot,
      SemanticDirection.INCOMING, SemanticDirection.INCOMING,
      RelationshipTypes.empty, varLength.min, varLength.max, shouldExpandAll = true,
      varExpandSlots,
      -1, -1,
      commands.predicates.True(), commands.predicates.True(), Size(1, 0)
    )())
  }

  test("single node with varlength expand into") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val expand = Expand(allNodesScan, "x", SemanticDirection.OUTGOING, Seq.empty, "z", "r", ExpandAll)
    val varLength = VarPatternLength(1, Some(15))
    val varExpand = VarExpand(expand, "x", SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      "z", "r2", varLength, ExpandInto)

    // when
    val pipe = build(varExpand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(2, nullable = false, CTNode)
    val r2RelSlot = RefSlot(0, nullable = false, CTList(CTRelationship))

    val allNodeScanSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)

    val expandSlots = SlotConfiguration.empty
      .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)
      .newLong("r", rRelSlot.nullable, rRelSlot.typ)
      .newLong("z", zNodeSlot.nullable, zNodeSlot.typ)

    val varExpandSlots = SlotConfiguration.empty
      .newLong("x", xNodeSlot.nullable, xNodeSlot.typ)
      .newLong("r", rRelSlot.nullable, rRelSlot.typ)
      .newLong("z", zNodeSlot.nullable, zNodeSlot.typ)
      .newReference("r2", r2RelSlot.nullable, r2RelSlot.typ)

    pipe should equal(
      VarLengthExpandSlottedPipe(
        ExpandAllSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
          xNodeSlot, rRelSlot.offset, zNodeSlot.offset,
          SemanticDirection.OUTGOING,
          RelationshipTypes.empty,
          expandSlots)(),
        xNodeSlot, r2RelSlot.offset, zNodeSlot,
        SemanticDirection.INCOMING, SemanticDirection.INCOMING,
        RelationshipTypes.empty, varLength.min, varLength.max, shouldExpandAll = false,
        varExpandSlots,
        VariablePredicates.NO_PREDICATE_OFFSET, // no node predicate
        VariablePredicates.NO_PREDICATE_OFFSET, // no relationship predicate
        commands.predicates.True(), commands.predicates.True(), Size(3, 0))()
    )
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val skip = plans.Skip(allNodesScan, literalInt(42))

    // when
    val pipe = build(skip)

    // then
    pipe should equal(SkipPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      commands.expressions.Literal(42)
    )())
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan("x", label, Set.empty, IndexOrderNone)
    val labelToken = LabelToken("label2", LabelId(0))
    val rhs = plans.IndexSeek("z:label2(prop = 42)", argumentIds = Set("x"))
    val apply = Apply(lhs, rhs)

    // when
    val pipe = build(apply)

    // then
    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), X_NODE_SLOTS, Size.zero, IndexOrderNone)(),
      NodeIndexSeekSlottedPipe("z", labelToken, Vector(SlottedIndexedProperty(0,None)), 0, SingleQueryExpression(commands.expressions.Literal(42)), org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeek,
        IndexOrderNone,
        SlotConfiguration.empty
          .newLong("x", false, CTNode)
          .newLong("z", false, CTNode),
        Size(1, 0))()
    )())
  }

  test("labelscan with projection") {
    // given
    val leaf = NodeByLabelScan("x", label, Set.empty, IndexOrderNone)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val pipe = build(projection)

    // then
    val slots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newReference("x.propertyKey", true, CTAny)

    pipe should equal(ProjectionPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), slots, Size.zero, IndexOrderNone)(),
      SlottedCommandProjection(Map(0 -> NodeProperty(slots("x.propertyKey").offset, 0)))
    )())
  }

  test("labelscan with projection and alias") {
    // given
    val leaf = NodeByLabelScan("x", label, Set.empty, IndexOrderNone)
    val projection = Projection(leaf, Map("A" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val pipe = build(projection)

    // then
    val slots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .addAlias("A", "x")
      .newReference("x.propertyKey", true, CTAny)

    pipe should equal(ProjectionPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), slots, Size.zero, IndexOrderNone)(),
      SlottedCommandProjection(Map(0 -> NodeProperty(slots("x.propertyKey").offset, 0)))
    )())
  }

  test("cartesian product") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val rhs = NodeByLabelScan("y", labelName("label2"), Set.empty, IndexOrderNone)
    val Xproduct = CartesianProduct(lhs, rhs)

    // when
    val pipe = build(Xproduct)

    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val rhsSlots = SlotConfiguration.empty.newLong("y", nullable = false, CTNode)
    val xProdSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTNode)

    // then
    pipe should equal(CartesianProductSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsSlots, Size.zero, IndexOrderNone)(),
      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsSlots, Size.zero, IndexOrderNone)(),
      lhsLongCount = 1, lhsRefCount = 0, xProdSlots, argumentSize = Size.zero)()
    )
  }

  test("foreach") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val rhs = NodeByLabelScan("y", LabelName("label2")(pos), Set.empty, IndexOrderNone) // This would be meaningless as a real-world example
    val foreach = ForeachApply(lhs, rhs, "z", listOfInt(1, 2))

    // when
    val pipe = build(foreach)

    // then
    val lhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newReference("z", nullable = true, CTAny)

    val rhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newReference("z", nullable = true, CTAny)
      .newLong("y", nullable = false, CTNode)

    pipe should equal(ForeachSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsSlots, Size.zero, IndexOrderNone)(),
      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsSlots, Size(nLongs = 1, nReferences = 1), IndexOrderNone)(),
      lhsSlots("z"),
      commands.expressions.ListLiteral(commands.expressions.Literal(1), commands.expressions.Literal(2)))()
    )
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty, IndexOrderNone)
    val arg = Argument(Set("x"))
    val rhs = Expand(arg, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    val apply = Apply(lhs, rhs)

    // when
    val pipe = build(apply)

    // then
    val lhsSlots = SlotConfiguration.empty.newLong("x", false, CTNode)

    val rhsSlots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newLong("r", false, CTRelationship)
      .newLong("z", false, CTNode)

    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel(labelName("label1")), lhsSlots, Size.zero, IndexOrderNone)(),
      ExpandAllSlottedPipe(
        ArgumentSlottedPipe(lhsSlots, Size(1, 0))(),
        rhsSlots("x"), 1, 2, SemanticDirection.INCOMING, RelationshipTypes.empty, rhsSlots
      )()
    )())
  }

  test("NodeIndexScan should yield a NodeIndexScanSlottedPipe") {
    // given
    val leaf = plans.IndexSeek("n:Awesome(prop)")

    // when
    val pipe = build(leaf)

    // then

    pipe should equal(
      NodeIndexScanSlottedPipe(
        "n",
        LabelToken("Awesome", LabelId(0)),
        Seq(SlottedIndexedProperty(0, None)),
        0,
        IndexOrderNone,
        SlotConfiguration.empty.newLong("n", false, CTNode), Size.zero)())
  }

  test("Should use NodeIndexUniqueSeek") {
    // given
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val seek = NodeUniqueIndexSeek("z", label, Seq.empty, seekExpression, Set("x"), IndexOrderNone)

    // when
    val pipe = build(seek)

    // then
    pipe should equal(
      NodeIndexSeekSlottedPipe("z", label, IndexedSeq.empty, 0, SingleQueryExpression(commands.expressions.Literal(42)), UniqueIndexSeek, IndexOrderNone,
        SlotConfiguration.empty.newLong("z", false, CTNode), Size.zero)()
    )
  }

  test("unwind and sort") {
    // given UNWIND [1,2,3] as x RETURN x ORDER BY x
    val xVar = varFor("x")
    val xVarName = xVar.name
    val leaf = Argument()
    val unwind = UnwindCollection(leaf, xVarName, listOfInt(1, 2, 3))
    val sort = Sort(unwind, List(plans.Ascending(xVarName)))

    // when
    val pipe = build(sort)

    // then
    val expectedSlots1 = SlotConfiguration.empty
    val xSlot = RefSlot(0, nullable = true, CTAny)
    val expectedSlots2 = SlotConfiguration.empty
      .newReference("x", xSlot.nullable, xSlot.typ)

    // We have to use mathPattern to ignore equality on the comparator, which does not implement equals in a sensible way.
    pipe should matchPattern {
      case SortPipe(
      UnwindSlottedPipe(
      ArgumentSlottedPipe(`expectedSlots1`, Size.zero),
      commands.expressions.ListLiteral(commands.expressions.Literal(1), commands.expressions.Literal(2), commands.expressions.Literal(3)), 0, `expectedSlots2`
      ), _) =>

    }
  }

  test("should have correct order for grouping columns") {
    // given
    val leaf = NodeByLabelScan("x", label, Set.empty, IndexOrderNone)
    val varLength = VarPatternLength(1, Some(15))
    val expand = VarExpand(leaf, "x", SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      "z", "r", varLength, ExpandAll)

    // must be  at least 6 grouping expressions to trigger slotted bug
    val groupingExpressions = Map("x1" -> varFor("x"), "z" -> varFor("z"), "x2" -> varFor("x"), "x3" -> varFor("x"), "x4" -> varFor("x"), "x5" -> varFor("x"))
    val plan = Aggregation(expand, groupingExpressions, aggregationExpression = Map.empty)

    // when
    val pipe = build(plan).asInstanceOf[EagerAggregationPipe]

    val tableFactory = pipe.tableFactory.asInstanceOf[SlottedPrimitiveGroupingAggTable.Factory]
    // then
    val offsetZ = tableFactory.writeGrouping(tableFactory.slots("z").offset)

    // x has id 0 and z has id 1
    for (i <- tableFactory.readGrouping.indices) {
      if (i != offsetZ) {
        tableFactory.readGrouping(i) should equal(0)
      } else {
        tableFactory.readGrouping(i) should equal(1)
      }
    }
  }

  test("should have correct order for join on many nodes") {
    // given

    def commonSubTree = {
      val leaf = NodeByLabelScan("node1", LabelName("label")(pos), Set.empty, IndexOrderNone)
      val expand1 = Expand(leaf, "node1", SemanticDirection.INCOMING, Seq.empty, "node2", "r")
      val expand2 = Expand(expand1, "node2", SemanticDirection.INCOMING, Seq.empty, "node3", "r")
      Expand(expand2, "node3", SemanticDirection.INCOMING, Seq.empty, "node4", "r")
    }

    val expand4a = Expand(commonSubTree, "node3", SemanticDirection.INCOMING, Seq.empty, "node7", "r")
    val expand5a = Expand(expand4a, "node4", SemanticDirection.INCOMING, Seq.empty, "node5", "r")
    val expand6a = Expand(expand5a, "node5", SemanticDirection.INCOMING, Seq.empty, "node6", "r")

    val expand4b = Expand(commonSubTree, "node4", SemanticDirection.OUTGOING, Seq.empty, "node6", "r")
    val expand5b = Expand(expand4b, "node6", SemanticDirection.OUTGOING, Seq.empty, "node5", "r")
    val expand6b = Expand(expand5b, "node6", SemanticDirection.OUTGOING, Seq.empty, "node8", "r")

    val nodes = Set("node1", "node2", "node3", "node4", "node5", "node6")
    val plan = NodeHashJoin(nodes, expand6a, expand6b)

    // when
    val pipe = build(plan).asInstanceOf[NodeHashJoinSlottedPipe]

    // then
    val lhsSlots = pipe.left.asInstanceOf[ExpandAllSlottedPipe].slots
    val rhsSlots = pipe.right.asInstanceOf[ExpandAllSlottedPipe].slots

    val lhsNodes = pipe.lhsOffsets.map(offset => lhsSlots.nameOfSlot(offset, longSlot = true).get)
    val rhsNodes = pipe.rhsOffsets.map(offset => rhsSlots.nameOfSlot(offset, longSlot = true).get)

    for(i <- 0 until 6) {
      lhsNodes(i) should equal(rhsNodes(i))
    }
    lhsNodes.toSet should be(nodes)
  }

  test("should compute union mapping with aliases if out defined for original") {
    // given
    val aProp = CachedProperty("a", varFor("a"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    val xProp = CachedProperty("x", varFor("x"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    val in = SlotConfiguration.empty
      .newLong("a", false, CTNode)
      .addAlias("aa", "a")
      .newReference("b", true, CTAny)
      .addAlias("bb", "b")
      .newLong("x", false, CTNode)
      .newReference("y", false, CTAny)
      .newCachedProperty(aProp)
      .newCachedProperty(xProp)

    val inRow = SlottedRow(in)
    inRow.setLongAt(0, 1) // a
    inRow.setLongAt(1, 2) // x
    inRow.setRefAt(0, Values.stringValue("b")) // b
    inRow.setRefAt(1, Values.stringValue("y")) // y
    inRow.setCachedPropertyAt(2, Values.stringValue("aprop")) // a.prop
    inRow.setCachedPropertyAt(3, Values.stringValue("xprop")) // x.prop

    val out = SlotConfiguration.empty
      .newLong("a", false, CTNode)
      .newReference("b", true, CTAny)
      .newCachedProperty(aProp)

    val mapping = SlottedPipeMapper.computeUnionRowMapping(in, out)

    // when
    val outRow = SlottedRow(out)
    mapping.mapRows(inRow, outRow, null)

    // then
    outRow.getLongAt(0) should equal(1) // a
    outRow.getRefAt(0) should equal(Values.stringValue("b")) // b
    outRow.getCachedPropertyAt(1) should equal(Values.stringValue("aprop")) // a.prop
  }

  test("should compute union mapping with aliases if out has separate slots for aliases") {
    // given
    val in = SlotConfiguration.empty
      .newLong("a", false, CTNode)
      .addAlias("aa", "a")
      .newReference("b", true, CTAny)
      .addAlias("bb", "b")

    val inRow = SlottedRow(in)
    inRow.setLongAt(0, 1) // a
    inRow.setRefAt(0, Values.stringValue("b")) // b

    val out = SlotConfiguration.empty
      .newLong("a", false, CTNode)
      .newLong("aa", false, CTNode)
      .newReference("b", true, CTAny)
      .newReference("bb", true, CTAny)

    val mapping = SlottedPipeMapper.computeUnionRowMapping(in, out)

    // when
    val outRow = SlottedRow(out)
    mapping.mapRows(inRow, outRow, null)

    // then
    outRow.getLongAt(0) should equal(1) // a
    outRow.getLongAt(1) should equal(1) // aa
    outRow.getRefAt(0) should equal(Values.stringValue("b")) // b
    outRow.getRefAt(1) should equal(Values.stringValue("b")) // bb
  }

  test("should compute union mapping with aliases if out defined for alias") {
    // given
    val aProp = CachedProperty("a", varFor("a"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    val in = SlotConfiguration.empty
      .newLong("a", false, CTNode)
      .addAlias("aa", "a")
      .newReference("b", true, CTAny)
      .addAlias("bb", "b")
      .newCachedProperty(aProp)

    val inRow = SlottedRow(in)
    inRow.setLongAt(0, 1) // a
    inRow.setRefAt(0, Values.stringValue("b")) // b
    inRow.setCachedPropertyAt(1, Values.stringValue("aprop")) // a.prop


    val aaProp = CachedProperty("a", varFor("aa"), PropertyKeyName("prop")(pos), NODE_TYPE)(pos)
    val out = SlotConfiguration.empty
      .newLong("aa", false, CTNode)
      .newReference("bb", true, CTAny)
      .newCachedProperty(aaProp)

    val mapping = SlottedPipeMapper.computeUnionRowMapping(in, out)

    // when
    val outRow = SlottedRow(out)
    mapping.mapRows(inRow, outRow, null)

    // then
    outRow.getLongAt(0) should equal(1) // aa
    outRow.getRefAt(0) should equal(Values.stringValue("b")) // bb
    outRow.getCachedPropertyAt(1) should equal(Values.stringValue("aprop")) // aa.prop
  }



  test("should compute union mapping with projecting a long slot to a ref slot") {
    // given
    val in = SlotConfiguration.empty
      .newLong("a", false, CTNode)
      .newLong("b", false, CTRelationship)

    val a = fromNodeEntity(new FakeNode {
      override def getId: Long = 1L
    })
    val b = fromRelationshipEntity(new FakeRel(null, null, null) {
      override def getId: Long = 2L
    })
    val queryState = {
      val queryContext = Mockito.mock(classOf[QueryContext])
      val nodeOps = Mockito.mock(classOf[NodeOperations])
      val relOps = Mockito.mock(classOf[RelationshipOperations])
      when(queryContext.nodeOps).thenReturn(nodeOps)
      when(queryContext.relationshipOps).thenReturn(relOps)
      when(nodeOps.getById(1)).thenReturn(a)
      when(relOps.getById(2)).thenReturn(b)
      QueryStateHelper.emptyWith(query = queryContext)
    }

    val inRow = SlottedRow(in)
    inRow.setLongAt(0, a.id()) // a
    inRow.setLongAt(1, b.id()) // b

    val out = SlotConfiguration.empty
      .newReference("a", false, CTNode)
      .newReference("b", false, CTRelationship)

    val mapping = SlottedPipeMapper.computeUnionRowMapping(in, out)

    // when
    val outRow = SlottedRow(out)
    mapping.mapRows(inRow, outRow, queryState)

    // then
    outRow.getRefAt(0) should equal(a)
    outRow.getRefAt(1) should equal(b)
  }
}
