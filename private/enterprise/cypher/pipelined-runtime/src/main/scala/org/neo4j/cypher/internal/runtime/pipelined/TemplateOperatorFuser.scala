/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.OperatorFuser
import org.neo4j.cypher.internal.physicalplanning.OperatorFuserFactory
import org.neo4j.cypher.internal.physicalplanning.OutputDefinition
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.ProduceResultOutput
import org.neo4j.cypher.internal.physicalplanning.ReduceOutput
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperators.NewTemplate
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperators.TemplateContext
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AggregatorFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorNoGroupingTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProduceResultOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleArgumentAggregationMapperOperatorNoGroupingTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleArgumentAggregationMapperOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

import scala.collection.mutable.ArrayBuffer

class TemplateOperatorFuserFactory(physicalPlan: PhysicalPlan,
                                   readOnly: Boolean,
                                   parallelExecution: Boolean,
                                   fusionOverPipelineEnabled: Boolean) extends OperatorFuserFactory[NewTemplate] {
  override def newOperatorFuser(headPlanId: Id): OperatorFuser[NewTemplate] =
    new TemplateOperatorFuser(physicalPlan,
                              readOnly,
                              parallelExecution,
                              fusionOverPipelineEnabled,
                              headPlanId)
}

class TemplateOperatorFuser(physicalPlan: PhysicalPlan,
                            readOnly: Boolean,
                            parallelExecution: Boolean,
                            fusionOverPipelineEnabled: Boolean,
                            headPlanId: Id) extends TemplateOperators(readOnly, parallelExecution, fusionOverPipelineEnabled) with OperatorFuser[NewTemplate] {

  private val slots = physicalPlan.slotConfigurations(headPlanId)
  generateSlotAccessorFunctions(slots)

  private val _templates = new ArrayBuffer[NewTemplate]
  private val _fusedPlans = new ArrayBuffer[LogicalPlan]

  override def fusedPlans: IndexedSeq[LogicalPlan] = _fusedPlans

  override def templates: IndexedSeq[NewTemplate] = _templates

  override def fuseIn(plan: LogicalPlan): Boolean = {
    val newTemplate = createTemplate(plan, _templates.isEmpty, physicalPlan.applyPlans(plan.id) == Id.INVALID_ID)
    if (newTemplate.isDefined) {
      _templates += newTemplate.get
      _fusedPlans += plan
    }
    newTemplate.isDefined
  }

  override def fuseIn(output: OutputDefinition): Boolean = {

    val aggregatorFactory = AggregatorFactory(physicalPlan)

    val maybePlanAndTemplate: Option[(LogicalPlan, NewTemplate)] =
      output match {
        case ProduceResultOutput(p) =>
          Some((p, (ctx: TemplateContext) => {
            ctx.innermost.shouldWriteToContext = false // No need to write if we have ProduceResult
            ctx.innermost.shouldCheckDemand = true // The produce pipeline should follow subscription demand for reactive result support
            new ProduceResultOperatorTaskTemplate(ctx.innermost, p.id, p.columns, slots)(ctx.expressionCompiler)
          }))

        case ReduceOutput(_, argumentStateMapId, p@plans.Aggregation(_, groupingExpressions, aggregationExpressionsMap)) =>
          Some((p, (ctx: TemplateContext) => {
            def compileGroupingKey(astExpressions: Map[String, Expression],
                                   slots: SlotConfiguration,
                                   orderToLeverage: Seq[Expression]): () => IntermediateExpression = {
              val orderedGroupingExpressions = orderGroupingKeyExpressions(astExpressions, orderToLeverage)(slots).map(_._2)
              () => ctx.expressionCompiler.compileGroupingKey(orderedGroupingExpressions, p.id)
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
            val aggregationExpressions = Array.newBuilder[Array[Expression]]
            aggregationExpressionsMap.foreach {
              case (_, astExpression) =>
                val (aggregator, innerAstExpression) = aggregatorFactory.newAggregator(astExpression, parallelExecution)
                aggregators += aggregator
                aggregationExpressions += innerAstExpression
            }
            val aggregationAstExpressions: Array[Array[Expression]] = aggregationExpressions.result()
            if (groupingExpressions.nonEmpty) {
              if (aggregationAstExpressions.forall(_.length == 1)) {
                new SingleArgumentAggregationMapperOperatorTaskTemplate(ctx.innermost,
                  p.id,
                  argumentSlotOffset,
                  aggregators.result(),
                  argumentStateMapId,
                  () => aggregationAstExpressions.flatMap(a => a.map(e => ctx.compileExpression(e, p.id)())),
                  compileGroupingKey(groupingExpressions, outputSlots, orderToLeverage = Seq.empty),
                  serialExecutionOnly)(ctx.expressionCompiler)
              } else {
                new AggregationMapperOperatorTaskTemplate(ctx.innermost,
                  p.id,
                  argumentSlotOffset,
                  aggregators.result(),
                  argumentStateMapId,
                  () => aggregationAstExpressions.map(a => a.map(e => ctx.compileExpression(e, p.id)())),
                  compileGroupingKey(groupingExpressions, outputSlots, orderToLeverage = Seq.empty),
                  serialExecutionOnly)(ctx.expressionCompiler)
              }
            } else {
              if (aggregationAstExpressions.forall(_.length == 1)) {
                new SingleArgumentAggregationMapperOperatorNoGroupingTaskTemplate(ctx.innermost,
                  p.id,
                  argumentSlotOffset,
                  aggregators.result(),
                  argumentStateMapId,
                  () => aggregationAstExpressions.flatMap(a => a.map(e => ctx.compileExpression(e, p.id)())),
                  serialExecutionOnly)(ctx.expressionCompiler)
              } else {
                new AggregationMapperOperatorNoGroupingTaskTemplate(ctx.innermost,
                  p.id,
                  argumentSlotOffset,
                  aggregators.result(),
                  argumentStateMapId,
                  () => aggregationAstExpressions.map(a => a.map(e => ctx.compileExpression(e, p.id)())),
                  serialExecutionOnly)(ctx.expressionCompiler)
              }
            }
          }))
        case _ => None
      }

    maybePlanAndTemplate match {
      case Some((plan, fixTemplate)) =>
        _templates += fixTemplate
        _fusedPlans += plan
        true
      case None =>
        false
    }
  }
}
