/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.OperatorFuser
import org.neo4j.cypher.internal.physicalplanning.OperatorFuserFactory
import org.neo4j.cypher.internal.physicalplanning.OutputDefinition
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.ProduceResultOutput
import org.neo4j.cypher.internal.physicalplanning.ReduceOutput
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.VariableNamer
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AggregatorFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorNoGroupingTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.BinaryOperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithMorselGenerator.compileOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithMorselTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.DelegateOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProduceResultOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

import scala.collection.mutable.ArrayBuffer

class TemplateOperatorFuserFactory(physicalPlan: PhysicalPlan,
                                   tokenContext: TokenContext,
                                   readOnly: Boolean,
                                   indexRegistrator: QueryIndexRegistrator,
                                   parallelExecution: Boolean,
                                   fusionOverPipelineEnabled: Boolean,
                                   codeGenerationMode: CodeGeneration.CodeGenerationMode) extends OperatorFuserFactory {
  override def newOperatorFuser(headPlanId: Id, inputSlotConfiguration: SlotConfiguration): OperatorFuser =
    new TemplateOperatorFuser(physicalPlan, tokenContext, readOnly, indexRegistrator, parallelExecution, fusionOverPipelineEnabled, codeGenerationMode, headPlanId, inputSlotConfiguration)
}

class TemplateOperatorFuser(val physicalPlan: PhysicalPlan,
                            val tokenContext: TokenContext,
                            val readOnly: Boolean,
                            indexRegistrator: QueryIndexRegistrator,
                            parallelExecution: Boolean,
                            fusionOverPipelineEnabled: Boolean,
                            codeGenerationMode: CodeGeneration.CodeGenerationMode,
                            headPlanId: Id,
                            inputSlotConfiguration: SlotConfiguration) extends TemplateOperators(readOnly, parallelExecution, fusionOverPipelineEnabled) with OperatorFuser {

  private val slots = physicalPlan.slotConfigurations(headPlanId)
  generateSlotAccessorFunctions(slots)

  private val namer = new VariableNamer

  private val templates = new ArrayBuffer[NewTemplate]
  private val _fusedPlans = new ArrayBuffer[LogicalPlan]

  def fusedPlans: IndexedSeq[LogicalPlan] = _fusedPlans

  def compile(executionGraphDefinition: ExecutionGraphDefinition, pipelineId: PipelineId): Operator = {

    val expressionCompiler =
      _fusedPlans.head match {
        case plan@(_: Union| _: ConditionalApply | _: AntiConditionalApply | _: SelectOrSemiApply | _: SelectOrAntiSemiApply) =>
          val leftSlots = physicalPlan.slotConfigurations(plan.lhs.get.id)
          val rightSlots = physicalPlan.slotConfigurations(plan.rhs.get.id)
          new BinaryOperatorExpressionCompiler(slots, inputSlotConfiguration, leftSlots, rightSlots, readOnly, namer)
        case _ =>
          new OperatorExpressionCompiler(slots, inputSlotConfiguration, readOnly, namer) // NOTE: We assume slots is the same within an entire pipeline
      }

    val innermost = new DelegateOperatorTaskTemplate()(expressionCompiler)
    var currentTemplate: OperatorTaskTemplate = innermost

    var argumentStates = new ArrayBuffer[(ArgumentStateMapId, ArgumentStateFactory[_ <: ArgumentState])]

    for ( fixTemplate <- templates.reverse ) {
      val ctx = TemplateContext(slots,
                                physicalPlan.slotConfigurations,
                                tokenContext,
                                indexRegistrator,
                                physicalPlan.argumentSizes,
                                executionGraphDefinition,
                                currentTemplate,
                                innermost,
                                expressionCompiler)
      val x = fixTemplate(ctx)
      argumentStates ++= x.argumentStateFactory
      currentTemplate = x.template
    }
    val workIdentity = WorkIdentity.fromFusedPlans(fusedPlans)
    val operatorTaskWithMorselTemplate = currentTemplate.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]
    try {
      compileOperator(operatorTaskWithMorselTemplate, workIdentity, argumentStates, codeGenerationMode, pipelineId)
    } catch {
      // In the case of a StackOverflowError we cannot recover correctly and abort fusing altogether.
      case e: StackOverflowError =>
        throw new CantCompileQueryException("Stack overflow caused operator compilation to fail", e)
    }
  }

  def fuseIn(plan: LogicalPlan): Boolean = {
    val newTemplate = createTemplate(plan, templates.isEmpty, physicalPlan.applyPlans(plan.id) == Id.INVALID_ID)
    if (newTemplate.isDefined) {
      templates += newTemplate.get
      _fusedPlans += plan
    }
    newTemplate.isDefined
  }

  def fuseIn(output: OutputDefinition): Boolean = {

    val aggregatorFactory = AggregatorFactory(physicalPlan)

    val maybePlanAndTemplate: Option[(LogicalPlan, NewTemplate)] =
      output match {
        case ProduceResultOutput(p) =>
          Some(p, (ctx: TemplateContext) => {
            ctx.innermost.shouldWriteToContext = false // No need to write if we have ProduceResult
            ctx.innermost.shouldCheckDemand = true // The produce pipeline should follow subscription demand for reactive result support
            new ProduceResultOperatorTaskTemplate(ctx.innermost, p.id, p.columns, slots)(ctx.expressionCompiler)
          })

        case ReduceOutput(_, argumentStateMapId, p@plans.Aggregation(_, groupingExpressions, aggregationExpressionsMap)) =>
          Some(p, (ctx: TemplateContext) => {
            def compileGroupingKey(astExpressions: Map[String, Expression],
                                   slots: SlotConfiguration,
                                   orderToLeverage: Seq[Expression]): () => IntermediateExpression = {
              val orderedGroupingExpressions = orderGroupingKeyExpressions(astExpressions, orderToLeverage)(slots).map(_._2)
              () => ctx.expressionCompiler.compileGroupingKey(orderedGroupingExpressions)
                .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $astExpressions"))
            }

            ctx.innermost.shouldWriteToContext = false // No need to write if we have Aggregation
            ctx.innermost.shouldCheckDemand = false // No need to check subscription demand when not in final pipeline
            ctx.innermost.shouldCheckOutputCounter = true // Use a simple counter of number of outputs to bound the work unit execution
            val applyPlanId = physicalPlan.applyPlans(p.id)
            val argumentSlotOffset = slots.getArgumentLongOffsetFor(applyPlanId)

            // To order the elements inside the computed grouping key correctly we use their slot offsets in the downstream pipeline slot configuration
            val outputSlots = physicalPlan.slotConfigurations(p.id)

            val aggregators = Array.newBuilder[Aggregator]
            val aggregationExpressions = Array.newBuilder[Expression]
            aggregationExpressionsMap.foreach {
              case (_, astExpression) =>
                val (aggregator, innerAstExpression) = aggregatorFactory.newAggregator(astExpression)
                aggregators += aggregator
                aggregationExpressions += innerAstExpression
            }
            val aggregationAstExpressions: Array[Expression] = aggregationExpressions.result()
            val aggregationExpressionsCreator = () => aggregationAstExpressions.map(e => ctx.compileExpression(e)())
            if (groupingExpressions.nonEmpty) {
              new AggregationMapperOperatorTaskTemplate(ctx.innermost,
                p.id,
                argumentSlotOffset,
                aggregators.result(),
                argumentStateMapId,
                aggregationExpressionsCreator,
                compileGroupingKey(groupingExpressions, outputSlots, orderToLeverage = Seq.empty),
                serialExecutionOnly)(ctx.expressionCompiler)
            } else {
              new AggregationMapperOperatorNoGroupingTaskTemplate(ctx.innermost,
                p.id,
                argumentSlotOffset,
                aggregators.result(),
                argumentStateMapId,
                aggregationExpressionsCreator,
                serialExecutionOnly)(ctx.expressionCompiler)
            }
          })
        case _ => None
      }

    maybePlanAndTemplate match {
      case Some((plan, fixTemplate)) =>
        templates += fixTemplate
        _fusedPlans += plan
        true
      case None =>
        false
    }
  }
}
