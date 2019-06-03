/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.ir.{LazyMode, StrictnessMode}
import org.neo4j.cypher.internal.logical.plans.{AllNodesScan, LogicalPlan, ProduceResult, Selection}
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, ArgumentSizes, NestedPlanArgumentConfigurations, SlotConfigurations}
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefinitionBuild
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter
import org.neo4j.cypher.internal.runtime.morsel.operators._
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.{Id, SameId}
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
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

  test("should fuse full pipeline") {
    // given
   val pipeline = allNodes("x") ~> filter(trueLiteral) ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators shouldBe empty
    compiled.outputOperator shouldBe NoOutputOperator
  }

  test("should fuse partial pipelines") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> dummy ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  test("should fuse partial pipelines 2") {
    // given
    val pipeline = allNodes("x") ~> filter(trueLiteral) ~> dummy ~> filter(trueLiteral) ~> produceResult("x")

    // when
    val compiled = fuse(pipeline)

    // then
    compiled.start shouldBe fused
    compiled.middleOperators should have size 2
    compiled.outputOperator should not be NoOutputOperator
  }

  def notSupported = new PipelineBuilder(dummyLeaf)

  def allNodes(node: String): PipelineBuilder = {
    val builder = new PipelineBuilder(AllNodesScan(node, Set.empty))
    builder.addNode(node)
    builder
  }

  def filter(predicates: Expression*): LogicalPlan => LogicalPlan = Selection(predicates.toSeq, _)

  def produceResult(out: String*): LogicalPlan => LogicalPlan = ProduceResult(_, out.toSeq)

  class PipelineBuilder(head: LogicalPlan) {
    private val slots = mutable.Map.empty[String, Slot]
    private var longCount = 0
    private var refCount = 0
    private var current = head
    val pipeline = new PipelineDefinitionBuild(PipelineId(3), head)

    def ~>(f: LogicalPlan => LogicalPlan): PipelineBuilder = {
      current = f(current)
      current match {
        case p: ProduceResult => pipeline.outputDefinition = ProduceResultOutput(p)
        case p => pipeline.middlePlans.append(p)
      }
      this
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

    def slotConfiguration = SlotConfiguration(slots.toMap, longCount, refCount)

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
                                    new ApplyPlans,
                                    new NestedPlanArgumentConfigurations,
                                    new AvailableExpressionVariables,
                                    Map.empty)
    physicalPlan.slotConfigurations.set(theId, pipelineBuilder.slotConfiguration)
    physicalPlan.argumentSizes.set(theId, Size.zero)
    val converter = new CompiledExpressionConverter(NullLog.getInstance(), physicalPlan,
                                                    TokenContext.EMPTY, neverFail = false)

    val expressionConverters = new ExpressionConverters(converter)

    val executionGraphDefinition = ExecutionGraphDefinition(physicalPlan, null, null, null, Map.empty)
    val operatorFactory = new DummyOperatorFactory(executionGraphDefinition, expressionConverters)
    val fuser = new FuseOperators(operatorFactory,
                                  fusingEnabled = true,
                                  tokenContext = TokenContext.EMPTY)
    val pipeline = PipelineDefinition(pipelineBuilder.pipeline.id,
      pipelineBuilder.pipeline.headPlan,
      null,
      pipelineBuilder.pipeline.outputDefinition,
      pipelineBuilder.pipeline.middlePlans,
      serial = false)
    fuser.compilePipeline(pipeline)
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
                            queryIndexes = mock[QueryIndexes]) {

    override def create(plan: LogicalPlan,
                        inputBuffer: BufferDefinition): Operator =
      mock[Operator](RETURNS_DEEP_STUBS)

    override def createMiddle(plan: LogicalPlan): Option[MiddleOperator] =
      Some(mock[MiddleOperator](RETURNS_DEEP_STUBS))

    override def createProduceResults(plan: ProduceResult): ProduceResultOperator =
      mock[ProduceResultOperator](RETURNS_DEEP_STUBS)
  }
}
