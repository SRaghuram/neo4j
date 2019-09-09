/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.{CreateNode, VarPatternLength}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{InterpretedPipeMapper, commands}
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedPrimitiveGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.expressions.{NodeProperty, SlottedCommandProjection, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, LabelToken, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.util.LabelId
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTList, CTNode, CTRelationship}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

//noinspection NameBooleanParameters
class SlottedPipeMapperTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  implicit private val table: SemanticTable = SemanticTable()

  private def build(beforeRewrite: LogicalPlan): Pipe = {
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(0))
    val physicalPlan = PhysicalPlanner.plan(tokenContext, beforeRewrite, table, SlottedPipelineBreakingPolicy)
    val converters = new ExpressionConverters(SlottedExpressionConverters(physicalPlan),
                                                                          CommunityExpressionConverter(TokenContext.EMPTY))

    val fallback = InterpretedPipeMapper(true, converters, tokenContext, mock[QueryIndexRegistrator])(table)
    val pipeBuilder = new SlottedPipeMapper(fallback, converters, physicalPlan, true, mock[QueryIndexRegistrator])(table)
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
    val plan = NodeByLabelScan("x", label, Set.empty)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      NodesByLabelScanSlottedPipe("x", LazyLabel(label), X_NODE_SLOTS, Size.zero)()
    )
  }

  test("label scan with filtering") {
    val leaf = NodeByLabelScan("x", label, Set.empty)
    val filter = Selection(Seq(trueLiteral), leaf)

    // when
    val pipe = build(filter)

    // then
    pipe should equal(
      FilterPipe(
        NodesByLabelScanSlottedPipe("x", LazyLabel(label), X_NODE_SLOTS, Size.zero)(),
        predicates.True()
      )()
    )
    pipe.asInstanceOf[FilterPipe].predicate.owningPipe should equal(pipe)
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
      SlotConfiguration(Map(
        "x" -> xNodeSlot,
        "r" -> rRelSlot,
        "z" -> zNodeSlot), numberOfLongs = 3, numberOfReferences = 0)
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
      SlotConfiguration(Map("x" -> nodeSlot, "r" -> relSlot), numberOfLongs = 2, numberOfReferences = 0)
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
    val allNodeScanSlots = SlotConfiguration(Map("x" -> xNodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    val expandSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 3, numberOfReferences = 0)

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
    val allNodeScanSlots = SlotConfiguration(Map("x" -> nodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    val expandSlots = SlotConfiguration(Map("x" -> nodeSlot, "r" -> relSlot), numberOfLongs = 2, numberOfReferences = 0)

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
    val expectedSlots = SlotConfiguration(Map("x" -> nodeSlot), numberOfLongs = 1, numberOfReferences = 0)
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
    val expectedSlots = SlotConfiguration(Map("x" -> refSlot), numberOfLongs = 0, numberOfReferences = 1)
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
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = true, CTRelationship),
        "z" -> LongSlot(2, nullable = true, CTNode)), numberOfLongs = 3, numberOfReferences = 0),
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
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = true, CTRelationship)), numberOfLongs = 2, numberOfReferences = 0),
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

    val allNodeScanSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot), numberOfLongs = 1, numberOfReferences = 0)

    val varExpandSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 2, numberOfReferences = 1)

    pipe should equal(VarLengthExpandSlottedPipe(
      AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
      xNodeSlot, rRelSlot.offset, zNodeSlot,
      SemanticDirection.INCOMING, SemanticDirection.INCOMING,
      RelationshipTypes.empty, varLength.min, varLength.max, shouldExpandAll = true,
      varExpandSlots,
      -1, -1,
      commands.predicates.True(), commands.predicates.True(), Size(1, 0)
      )())
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].nodePredicate.owningPipe should equal(pipe)
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].relationshipPredicate.owningPipe should equal(pipe)
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

    val allNodeScanSlots =
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode)),
        numberOfLongs = 1, numberOfReferences = 0)

    val expandSlots =
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = false, CTRelationship),
        "z" -> LongSlot(2, nullable = false, CTNode)),
        numberOfLongs = 3, numberOfReferences = 0)

    val varExpandSlots =
      SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship),
      "z" -> LongSlot(2, nullable = false, CTNode),
      "r2" -> RefSlot(0, nullable = false, CTList(CTRelationship))),
      numberOfLongs = 3, numberOfReferences = 1)

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
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].nodePredicate.owningPipe should equal(pipe)
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].relationshipPredicate.owningPipe should equal(pipe)
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
    val lhs = NodeByLabelScan("x", label, Set.empty)
    val labelToken = LabelToken("label2", LabelId(0))
    val rhs = plans.IndexSeek("z:label2(prop = 42)", argumentIds = Set("x"))
    val apply = Apply(lhs, rhs)

    // when
    val pipe = build(apply)

    // then
    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), X_NODE_SLOTS, Size.zero)(),
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
    val leaf = NodeByLabelScan("x", label, Set.empty)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val pipe = build(projection)

    // then
    val slots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newReference("x.propertyKey", true, CTAny)

    pipe should equal(ProjectionPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), slots, Size.zero)(),
      SlottedCommandProjection(Map(0 -> NodeProperty(slots("x.propertyKey").offset, 0)))
    )())
  }

  test("labelscan with projection and alias") {
    // given
    val leaf = NodeByLabelScan("x", label, Set.empty)
    val projection = Projection(leaf, Map("A" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val pipe = build(projection)

    // then
    val slots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .addAlias("A", "x")
      .newReference("x.propertyKey", true, CTAny)

    pipe should equal(ProjectionPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), slots, Size.zero)(),
      SlottedCommandProjection(Map(0 -> NodeProperty(slots("x.propertyKey").offset, 0)))
    )())
  }
//
//  test("cartesian product") {
//    // given
//    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
//    val rhs = NodeByLabelScan("y", labelName("label2"), Set.empty)
//    val Xproduct = CartesianProduct(lhs, rhs)
//
//    // when
//    val pipe = build(Xproduct)
//
//    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
//    val rhsSlots = SlotConfiguration.empty.newLong("y", nullable = false, CTNode)
//    val xProdSlots = SlotConfiguration.empty
//      .newLong("x", nullable = false, CTNode)
//      .newLong("y", nullable = false, CTNode)
//
//    // then
//    pipe should equal(CartesianProductSlottedPipe(
//      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsSlots, Size.zero)(),
//      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsSlots, Size.zero)(),
//      lhsLongCount = 1, lhsRefCount = 0, xProdSlots, argumentSize = Size.zero)()
//    )
//  }

  test("foreach") {
    // given
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
    val rhs = NodeByLabelScan("y", LabelName("label2")(pos), Set.empty) // This would be meaningless as a real-world example
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
      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsSlots, Size.zero)(),
      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsSlots, Size(nLongs = 1, nReferences = 1))(),
      lhsSlots("z"),
      commands.expressions.ListLiteral(commands.expressions.Literal(1), commands.expressions.Literal(2)))()
    )
    pipe.asInstanceOf[ForeachSlottedPipe].expression.owningPipe should equal(pipe)
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan("x", labelName("label1"), Set.empty)
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
      NodesByLabelScanSlottedPipe("x", LazyLabel(labelName("label1")), lhsSlots, Size.zero)(),
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
    val expectedSlots2 = SlotConfiguration(numberOfLongs = 0, numberOfReferences = 1, slots = Map("x" -> xSlot))

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
    val leaf = NodeByLabelScan("x", label, Set.empty)
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
    val leaf = NodeByLabelScan("node1", LabelName("label")(pos), Set.empty)
    val expand1 = Expand(leaf, "node1", SemanticDirection.INCOMING, Seq.empty, "node2", "r")
    val expand2 = Expand(expand1, "node2", SemanticDirection.INCOMING, Seq.empty, "node3", "r")
    val expand3 = Expand(expand2, "node3", SemanticDirection.INCOMING, Seq.empty, "node4", "r")

    val expand4a = Expand(expand3, "node3", SemanticDirection.INCOMING, Seq.empty, "node7", "r")
    val expand5a = Expand(expand4a, "node4", SemanticDirection.INCOMING, Seq.empty, "node5", "r")
    val expand6a = Expand(expand5a, "node5", SemanticDirection.INCOMING, Seq.empty, "node6", "r")

    val expand4b = Expand(expand3, "node4", SemanticDirection.OUTGOING, Seq.empty, "node6", "r")
    val expand5b = Expand(expand4b, "node6", SemanticDirection.OUTGOING, Seq.empty, "node5", "r")
    val expand6b = Expand(expand5b, "node6", SemanticDirection.OUTGOING, Seq.empty, "node8", "r")

    val nodes = Set("node1", "node2", "node3", "node4", "node5", "node6")
    val plan = NodeHashJoin(nodes, expand6a, expand6b)

    // when
    val pipe = build(plan).asInstanceOf[NodeHashJoinSlottedPipe]

    // then
    val lhsSlots = pipe.left.asInstanceOf[ExpandAllSlottedPipe].slots
    val rhsSlots = pipe.right.asInstanceOf[ExpandAllSlottedPipe].slots

    val lhsNodes = pipe.lhsOffsets.map(offset => lhsSlots.nameOfLongSlot(offset).get)
    val rhsNodes = pipe.rhsOffsets.map(offset => rhsSlots.nameOfLongSlot(offset).get)

    for(i <- 0 until 6) {
      lhsNodes(i) should equal(rhsNodes(i))
    }
    lhsNodes.toSet should be(nodes)
  }
}
