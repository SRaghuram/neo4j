/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NonPipelined
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.NestedPlanArgumentConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.physicalplanning.RegularBufferVariant
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ShortestPathPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TestPipe
import org.neo4j.cypher.internal.runtime.pipelined.operators.AllNodeScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.FilterOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.MiddleOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselFeedPipe
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeHeadOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeMiddleOperator
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class OperatorFactoryTest extends CypherFunSuite with AstConstructionTestSupport  {
  private val theId = new Id(3)
  implicit val idGen: SameId = SameId(theId)

  private val input = BufferDefinition(BufferId(1), Id.INVALID_ID, ReadOnlyArray.empty, ReadOnlyArray.empty, RegularBufferVariant)(SlotConfiguration.empty)

  private val TEST_FALLBACK_POLICY = SLOTTED_FALLBACK_BY_CLASS(classOf[FindShortestPaths], Seq(classOf[NonPipelined], classOf[ProduceResult]))
  case class SLOTTED_FALLBACK_BY_CLASS(breaking: Class[_ <: LogicalPlan], nonBreaking: Seq[Class[_ <: LogicalPlan]]) extends InterpretedPipesFallbackPolicy {
    override def readOnly: Boolean = true
    override def breakOn(lp: LogicalPlan): Boolean = {
      val c = lp.getClass
      if (c == breaking) true
      else if (nonBreaking.contains(c)) false
      else throw InterpretedPipesFallbackPolicy.unsupported(c.getSimpleName, "Pipelined")
    }
  }

  test("should fully chain fallback pipes") {
    // given
    val pipeline = shortestPath("x", "r", "y") ~> nonPipelined ~> nonPipelined

    // when
    val (headOperator, middleOperators) = buildWithFactory(pipeline, TEST_FALLBACK_POLICY)

    // then
    middleOperators should have size 0

    headOperator shouldBe a[SlottedPipeHeadOperator]
    headOperator.asInstanceOf[SlottedPipeHeadOperator].pipe shouldBe a[TestPipe]
    headOperator.asInstanceOf[SlottedPipeHeadOperator].pipe.asInstanceOf[TestPipe].source shouldBe a[TestPipe]
    headOperator.asInstanceOf[SlottedPipeHeadOperator].pipe.asInstanceOf[TestPipe].source.asInstanceOf[TestPipe]
      .source shouldBe a[ShortestPathPipe]
    headOperator.asInstanceOf[SlottedPipeHeadOperator].pipe.asInstanceOf[TestPipe].source.asInstanceOf[TestPipe]
      .source.asInstanceOf[ShortestPathPipe].source shouldBe a[MorselFeedPipe]
  }

  test("should chain fallback pipes with interruption") {
    // given
    val pipeline = shortestPath("x", "r", "y") ~> nonPipelined ~> filter ~> nonPipelined ~> nonPipelined

    // when
    val (headOperator, middleOperators) = buildWithFactory(pipeline, TEST_FALLBACK_POLICY)

    // then
    headOperator shouldBe a[SlottedPipeHeadOperator]
    middleOperators should have size 2
  }

  test("should interpret partial pipelines, chain fallback pipes") {
    // given
    val pipeline = allNodes("x") ~> filter ~> nonPipelined ~> nonPipelined ~> nonPipelined

    // when
    val (headOperator, middleOperators) = buildWithFactory(pipeline, TEST_FALLBACK_POLICY)

    // then
    headOperator shouldBe a[AllNodeScanOperator]
    middleOperators should have size 2
    middleOperators(0) shouldBe a[FilterOperator]
    middleOperators(1) shouldBe a[SlottedPipeMiddleOperator]
    middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[TestPipe]
  }

  test("should interpret partial pipelines, chain fallback pipes with interruption") {
    // given
    val pipeline = allNodes("x") ~> filter ~> nonPipelined ~> nonPipelined ~> filter ~> nonPipelined ~> nonPipelined

    // when
    val (headOperator, middleOperators) = buildWithFactory(pipeline, TEST_FALLBACK_POLICY)

    // then
    headOperator shouldBe a[AllNodeScanOperator]
    middleOperators should have size 4

    middleOperators(0) shouldBe a[FilterOperator]
    middleOperators(1) shouldBe a[SlottedPipeMiddleOperator]
    middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[TestPipe]
    middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[TestPipe].source shouldBe a[TestPipe]
    middleOperators(1).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[TestPipe].source.asInstanceOf[TestPipe]
      .source shouldBe a[MorselFeedPipe]
    middleOperators(2) shouldBe a[FilterOperator]
    middleOperators(3) shouldBe a[SlottedPipeMiddleOperator]
    middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe shouldBe a[TestPipe]
    middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[TestPipe].source shouldBe a[TestPipe]
    middleOperators(3).asInstanceOf[SlottedPipeMiddleOperator].pipe.asInstanceOf[TestPipe].source.asInstanceOf[TestPipe]
      .source shouldBe a[MorselFeedPipe]
  }

  def allNodes(node: String): PipelineBuilder = {
    val builder = new PipelineBuilder(AllNodesScan(node, Set.empty))
    builder.addNode(node)
    builder
  }

  def nonPipelined: LogicalPlan => LogicalPlan = NonPipelined(_)

  def filter: LogicalPlan => LogicalPlan = Selection(Seq(trueLiteral), _)

  def shortestPath(from: String, relName: String, to: String): PipelineBuilder = {
    val min = 1
    val max = Some(3)
    val relTypes = Seq.empty
    val dir = SemanticDirection.OUTGOING
    val single = true
    val plan =
      FindShortestPaths(Argument(),
        ShortestPathPattern(None, PatternRelationship(relName, (from, to), dir, relTypes, VarPatternLength(min, max)), single)
        (ShortestPaths(RelationshipChain(
          NodePattern(Some(varFor(from)), Seq.empty, None)(pos), // labels and properties are not used at runtime
          RelationshipPattern(Some(varFor(relName)),
            relTypes,
            Some(Some(Range(Some(UnsignedDecimalIntegerLiteral(min.toString)(pos)), max.map(i => UnsignedDecimalIntegerLiteral(i.toString)(pos)))(pos))),
            None, // properties are not used at runtime
            dir
          )(pos),
          NodePattern(Some(varFor(to)), Seq.empty, None)(pos) // labels and properties are not used at runtime
        )(pos), single)(pos))
      )
    val builder = new PipelineBuilder(plan)
    builder.addNode(from)
    builder.addNode(to)
    builder.addRelationship(relName)
    builder
  }

  class PipelineBuilder(val headPlan: LogicalPlan) {
    private val slotConfig = SlotConfiguration.empty
    private var current = headPlan
    val middlePlans = new ArrayBuffer[LogicalPlan]
    val applyPlans = new ApplyPlans()

    def ~>(f: LogicalPlan => LogicalPlan): PipelineBuilder = {
      current = f(current)
      applyPlans.set(current.id, Id.INVALID_ID)
      middlePlans += current
      this
    }

    def addNode(node: String): Unit = {
      slotConfig.newLong(node, nullable = false, symbols.CTNode)
    }

    def addRelationship(relationship: String): Unit = {
      slotConfig.newLong(relationship, nullable = false, symbols.CTNode)
    }

    def addReference(ref: String): Unit = {
      slotConfig.newReference(ref, nullable = true, symbols.CTAny)
    }

    def slotConfiguration: SlotConfiguration = slotConfig
  }

  private def buildWithFactory(pipelineBuilder: PipelineBuilder, fallbackPolicy: InterpretedPipesFallbackPolicy): (Operator, Seq[MiddleOperator]) = {
    val readOnly = true
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
    val expressionConverters =
      new ExpressionConverters(
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(TokenContext.EMPTY)
      )

    val executionGraphDefinition = ExecutionGraphDefinition(physicalPlan, null, null, null, Map.empty)
    val indexRegistrator = mock[QueryIndexRegistrator]
    val semanticTable = mock[SemanticTable]
    val fallbackPipeMapper = InterpretedPipeMapper(readOnly, expressionConverters, mock[TokenContext], indexRegistrator)(semanticTable)
    val factory =
      new OperatorFactory(
        executionGraphDefinition,
        expressionConverters,
        readOnly,
        indexRegistrator,
        semanticTable,
        fallbackPolicy,
        slottedPipeBuilder = Some(fallbackPipeMapper),
        "Pipelined",
        parallelExecution = false,
       lenientCreateRelationship = false)

    val headOperator = factory.create(pipelineBuilder.headPlan, input)
    val middleOperators = factory.createMiddleOperators(pipelineBuilder.middlePlans, headOperator)
    (headOperator, middleOperators)
  }
}
