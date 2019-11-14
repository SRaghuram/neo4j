/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.codegen.api.CodeGeneration.{ByteCodeGeneration, CodeSaver}
import org.neo4j.cypher.internal.ir.{LazyMode, StrictnessMode}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, ArgumentSizes, NestedPlanArgumentConfigurations, SlotConfigurations}
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.morsel.InterpretedPipesFallbackPolicy.INTERPRETED_PIPES_FALLBACK_DISABLED
import org.neo4j.cypher.internal.runtime.morsel.execution.{QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators._
import org.neo4j.cypher.internal.runtime.morsel.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.{ParameterMapping, ProcedureCallMode, QueryContext, QueryIndexRegistrator}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.util.attribution.{Id, SameId}
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.logging.NullLog
import org.scalatest.matchers.{BeMatcher, MatchResult}

import scala.collection.mutable

class FuseOperatorsTest extends CypherFunSuite with AstConstructionTestSupport  {
  private val theId = new Id(3)
  implicit val idGen: SameId = SameId(theId)

  test("should not fuse single plan pipelines") {
    //given
    val pipeline = allNodes("x")

    //when
    val compiled = fuse(pipeline)

    //then
    compiled.start should not be fused
    compiled.middleOperators shouldBe empty
    compiled.outputOperator shouldBe NoOutputOperator
  }

  test("should fuse full pipeline, ending in produce results") {
    // given
   val pipeline = allNodes("x") ~> filter(trueLiteral) ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators shouldBe empty
    compiled.outputOperator shouldBe NoOutputOperator
  }

  test("should fuse full pipeline, ending in aggregation") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> groupAggregation(Map("x" -> varFor("x")), Map("y"->countStar()))

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators shouldBe empty
    compiled.outputOperator shouldBe NoOutputOperator
  }

  test("should fuse full pipeline, ending in aggregation with no grouping") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> groupAggregation(Map.empty, Map("y"->countStar()))

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators shouldBe empty
    compiled.outputOperator shouldBe NoOutputOperator
  }

  test("should fuse partial pipelines, ending in produce results 1") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> dummy ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fuse partial pipelines, ending in aggregation 1") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> dummy ~> groupAggregation(Map("x" -> varFor("x")), Map("y"->countStar()))

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fuse partial pipelines, ending in aggregation with no grouping 1") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> dummy ~> groupAggregation(Map.empty, Map("y"->countStar()))

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fuse partial pipelines, ending in produce results 2") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> filter(trueLiteral) ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fuse partial pipelines, ending in aggregation 2") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> filter(trueLiteral) ~> groupAggregation(Map("x" -> varFor("x")), Map("y"->countStar()))

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fuse partial pipelines, ending in aggregation with no grouping 2") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> filter(trueLiteral) ~> groupAggregation(Map.empty, Map("y"->countStar()))

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fully chain fallback pipes") {
    // given
    val pipeline = optionalExpand("x", "r", "y") ~> dropResult ~> dropResult

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start should not be fused
    compiled.middleOperators should have size 0
    compiled.outputOperator shouldBe NoOutputOperator

    compiled.start shouldBe a[SlottedPipeHeadOperator]
    compiled.start.asInstanceOf[SlottedPipeHeadOperator].pipe shouldBe a[DropResultPipe]
    compiled.start.asInstanceOf[SlottedPipeHeadOperator].pipe.asInstanceOf[DropResultPipe].source shouldBe a[DropResultPipe]
    compiled.start.asInstanceOf[SlottedPipeHeadOperator].pipe.asInstanceOf[DropResultPipe].source.asInstanceOf[DropResultPipe]
      .source shouldBe a[NonFilteringOptionalExpandAllPipe]
    compiled.start.asInstanceOf[SlottedPipeHeadOperator].pipe.asInstanceOf[DropResultPipe].source.asInstanceOf[DropResultPipe]
      .source.asInstanceOf[NonFilteringOptionalExpandAllPipe].source shouldBe a[MorselFeedPipe]
  }

  test("should fully chain fallback pipes, ending in produce results") {
    // given
    val pipeline = optionalExpand("x", "r", "y") ~> dropResult ~> dropResult ~> dropResult ~> produceResult("x", "r", "y")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start should not be fused
    compiled.middleOperators should have size 0
    compiled.outputOperator should not be NoOutputOperator
    compiled.start shouldBe a[SlottedPipeHeadOperator]
  }

  test("should chain fallback pipes with interruption, ending in produce results") {
    // given
    val pipeline = optionalExpand("x", "r", "y") ~> dropResult ~> dummy ~> dropResult ~> dropResult ~> produceResult("x", "r", "y")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start should not be fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
    compiled.start shouldBe a[SlottedPipeHeadOperator]
  }

  test("should fuse partial pipelines, chain fallback pipes, ending in produce results") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dropResult ~> dropResult ~> dropResult ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 1
    compiled.outputOperator should not be NoOutputOperator
    compiled.middleOperators(0) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(0).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
  }

  test("should fuse partial pipelines, chain fallback pipes with interruption, ending in produce results") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dropResult ~> dropResult ~> dummy ~> dropResult ~> dropResult ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 3
    compiled.outputOperator should not be NoOutputOperator
    compiled.middleOperators(0) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(0).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
    compiled.middleOperators(1) shouldBe a[DummyMiddleOperator]
    compiled.middleOperators(2) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(2).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
  }

  test("should fuse partial pipelines, add middle, chain fallback pipes with interruption, ending in produce results") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> dropResult ~> dropResult ~> dummy ~> dropResult ~> dropResult ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 4
    compiled.outputOperator should not be NoOutputOperator

    compiled.middleOperators(0) shouldBe a[DummyMiddleOperator]
    compiled.middleOperators(1) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
    compiled.middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source shouldBe a[DropResultPipe]
    compiled.middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source.asInstanceOf[DropResultPipe]
      .source shouldBe a[MorselFeedPipe]
    compiled.middleOperators(2) shouldBe a[DummyMiddleOperator]
    compiled.middleOperators(3) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
    compiled.middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source shouldBe a[DropResultPipe]
    compiled.middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source.asInstanceOf[DropResultPipe]
      .source shouldBe a[MorselFeedPipe]
  }

  test("should fuse partial pipelines, add middle, chain fallback pipes with interruption, no output") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> dropResult ~> dropResult ~> dummy ~> dropResult ~> dropResult

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 4
    compiled.outputOperator shouldBe NoOutputOperator
    compiled.middleOperators(0) shouldBe a[DummyMiddleOperator]
    compiled.middleOperators(1) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
    compiled.middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source shouldBe a[DropResultPipe]
    compiled.middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source.asInstanceOf[DropResultPipe]
      .source shouldBe a[MorselFeedPipe]
    compiled.middleOperators(2) shouldBe a[DummyMiddleOperator]
    compiled.middleOperators(3) shouldBe a[SlottedPipeMiddleOperator]
    compiled.middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[DropResultPipe]
    compiled.middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source shouldBe a[DropResultPipe]
    compiled.middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[DropResultPipe].source.asInstanceOf[DropResultPipe]
      .source shouldBe a[MorselFeedPipe]
  }

  def notSupported = new PipelineBuilder(dummyLeaf)

  def allNodes(node: String): PipelineBuilder = {
    val builder = new PipelineBuilder(AllNodesScan(node, Set.empty))
    builder.addNode(node)
    builder
  }

  def filter(predicates: Expression*): LogicalPlan => LogicalPlan = Selection(predicates.toSeq, _)

  def dropResult: LogicalPlan => LogicalPlan = DropResult(_)

  def optionalExpand(from: String, relName: String, to: String): PipelineBuilder = {
    val plan = OptionalExpand(Argument(), from, SemanticDirection.OUTGOING, types = Seq.empty, to, relName, ExpandAll)
    val builder = new PipelineBuilder(plan)
    builder.addNode(from)
    builder.addNode(to)
    builder.addRelationship(relName)
    builder
  }

  def groupAggregation(groupings: Map[String, Expression],
                       aggregations: Map[String, Expression]): LogicalPlan => LogicalPlan = Aggregation(_, groupings, aggregations)

  def produceResult(out: String*): LogicalPlan => LogicalPlan = ProduceResult(_, out.toSeq)

  private val fusionPolicy = OperatorFusionPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = true)

  class PipelineBuilder(head: LogicalPlan) {
    private val slots = mutable.Map.empty[String, Slot]
    private var longCount = 0
    private var refCount = 0
    private var current = head
    private var bufferId = 0
    private val applyPlansOffsets = mutable.Map[Id, Int](Id(0) -> 0)
    val applyPlans = new ApplyPlans()
    val pipeline = new PipelineDefinitionBuild(PipelineId(3), head)
    if (fusionPolicy.canFuse(head)) {
      pipeline.fusedPlans += head
    }

    private def canFuse(plan: LogicalPlan): Boolean =
      pipeline.fusedPlans.nonEmpty && (pipeline.fusedPlans.last eq plan.lhs.get) && fusionPolicy.canFuse(plan)

    def ~>(f: LogicalPlan => LogicalPlan): PipelineBuilder = {
      current = f(current)
      applyPlans.set(current.id, Id(0))
      current match {
        case p: ProduceResult => pipeline.outputDefinition = ProduceResultOutput(p)
        case p: Aggregation => pipeline.outputDefinition = ReduceOutput(buffer(), p)
        case p =>
          if (canFuse(p)) {
            pipeline.fusedPlans += p
          } else {
            pipeline.middlePlans.append(p)
          }
      }
      this
    }

    private def buffer(): BufferId = {
      val buffer = BufferId(bufferId)
      bufferId += 1
      buffer
    }

    def addNode(node: String): Unit = {
      slots += node -> LongSlot(longCount, nullable = false, symbols.CTNode)
      longCount += 1
    }

    def addRelationship(relationship: String): Unit = {
      slots += relationship -> LongSlot(longCount, nullable = false, symbols.CTRelationship)
      longCount += 1
    }

    def addReference(ref: String): Unit = {
      slots += ref -> RefSlot(refCount, nullable = true, symbols.CTAny)
      refCount += 1
    }

    def slotConfiguration = new SlotConfiguration(slots, mutable.Map.empty, applyPlansOffsets, longCount, refCount)

  }
  private object fused extends BeMatcher[Operator] {

    override def apply(left: Operator): MatchResult =
      MatchResult(left.isInstanceOf[CompiledStreamingOperator],
                  s"Expected $left to have been fused", "")
  }

  private def fuse(pipelineBuilder: PipelineBuilder): ExecutablePipeline = {
    val physicalPlan = PhysicalPlan(null,
                                    0,
                                    new SlotConfigurations,
                                    new ArgumentSizes,
                                    pipelineBuilder.applyPlans,
                                    new NestedPlanArgumentConfigurations,
                                    new AvailableExpressionVariables,
                                    ParameterMapping.empty)

    physicalPlan.slotConfigurations.set(theId, pipelineBuilder.slotConfiguration)
    physicalPlan.argumentSizes.set(theId, Size.zero)
    val converter = new CompiledExpressionConverter(
      NullLog.getInstance(),
      physicalPlan,
      TokenContext.EMPTY,
      readOnly = false,
      codeGenerationMode = ByteCodeGeneration(new CodeSaver(false, false)),
      neverFail = false)

    val expressionConverters = new ExpressionConverters(converter,
                                                        SlottedExpressionConverters(physicalPlan),
                                                        CommunityExpressionConverter(TokenContext.EMPTY))

    val executionGraphDefinition = ExecutionGraphDefinition(physicalPlan, null, null, null, Map.empty)
    val operatorFactory = new DummyOperatorFactory(executionGraphDefinition, expressionConverters)
    val fuser = new FuseOperators(operatorFactory,
                                  tokenContext = TokenContext.EMPTY,
                                  parallelExecution = true,
                                  codeGenerationMode = ByteCodeGeneration(new CodeSaver(false, false)))
    val pipeline = PipelineDefinition(pipelineBuilder.pipeline.id,
                                      NO_PIPELINE,
                                      NO_PIPELINE,
                                      pipelineBuilder.pipeline.headPlan,
                                      pipelineBuilder.pipeline.fusedPlans,
                                      mock[BufferDefinition](RETURNS_DEEP_STUBS),
                                      pipelineBuilder.pipeline.outputDefinition,
                                      pipelineBuilder.pipeline.middlePlans,
                                      serial = false)
    fuser.compilePipeline(pipeline, false)._1
  }

  case class dummy(source: LogicalPlan) extends LogicalPlan(idGen) {

    override def lhs: Option[LogicalPlan] = Some(source)

    override def rhs: Option[LogicalPlan] = None

    override def availableSymbols: Set[String] = Set.empty

    override def strictness: StrictnessMode = LazyMode
  }

  case object dummyLeaf extends LogicalPlan(idGen) {

    override def lhs: Option[LogicalPlan] = None

    override def rhs: Option[LogicalPlan] = None

    override def availableSymbols: Set[String] = Set.empty

    override def strictness: StrictnessMode = LazyMode
  }

  class DummyOperatorFactory(executionGraphDefinition: ExecutionGraphDefinition,
                             converters: ExpressionConverters)
    extends OperatorFactory(executionGraphDefinition,
                            converters,
                            readOnly = true,
                            indexRegistrator = mock[QueryIndexRegistrator],
                            semanticTable = mock[SemanticTable],
                            INTERPRETED_PIPES_FALLBACK_DISABLED,
                            slottedPipeBuilder = Some(new DummySlottedPipeBuilder)) {

    override def create(plan: LogicalPlan,
                        inputBuffer: BufferDefinition): Operator =
      plan match {
        // Example of fallback plans
        case _: DropResult |
             _: OptionalExpand if slottedPipeBuilder.isDefined =>
          createSlottedPipeHeadOperator(plan)

        case _ =>
          mock[Operator](RETURNS_DEEP_STUBS)
      }

    override protected def createMiddleOrUpdateSlottedPipeChain(plan: LogicalPlan, maybeSlottedPipeOperatorToChainOnTo: Option[SlottedPipeOperator]): Option[MiddleOperator] = {
      plan match {
        // Example of fallback middle plan
        case _: DropResult if slottedPipeBuilder.isDefined =>
          createSlottedPipeMiddleOperator(plan, maybeSlottedPipeOperatorToChainOnTo)

        // Example of fallback plan that needs to be head
        case _: OptionalExpand if slottedPipeBuilder.isDefined =>
          throw new CantCompileQueryException(s"Morsel does not yet support using `$plan` as a fallback middle plan, use another runtime.")

        case _ =>
          Some(new DummyMiddleOperator)
      }
    }

    override def createProduceResults(plan: ProduceResult): ProduceResultOperator =
      mock[ProduceResultOperator](RETURNS_DEEP_STUBS)
  }

  class DummyMiddleOperator extends MiddleOperator {
    override def createTask(argumentStateCreator: ArgumentStateMapCreator, stateFactory: StateFactory, queryContext: QueryContext, state: QueryState, resources: QueryResources): OperatorTask = null
    override def workIdentity: WorkIdentity = WorkIdentityImpl(Id.INVALID_ID, "middle")
  }

  class DummySlottedPipeBuilder() extends PipeMapper {
    override def onLeaf(plan: LogicalPlan): Pipe = ???

    override def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = plan match {
      case _: DropResult =>
        DropResultPipe(source)(Id.INVALID_ID)

      case _: ProcedureCall =>
        ProcedureCallPipe(source,
          mock[ProcedureSignature](RETURNS_DEEP_STUBS),
          mock[ProcedureCallMode](RETURNS_DEEP_STUBS),
          Seq.empty,
          FlatMapAndAppendToRow,
          Seq.empty,
          Seq.empty
        )(Id.INVALID_ID)

      case OptionalExpand(_, from, dir, types, to, relName, mode, _) =>
        OptionalExpandAllPipe(source, from, relName, to, dir, RelationshipTypes.empty, None)(Id.INVALID_ID)

    }

    override def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = ???
  }
}
