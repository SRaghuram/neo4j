/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.CypherRuntime
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v4_0.runtime._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.executionplan.{ExecutionPlan => ExecutionPlan_V35}
import org.neo4j.cypher.internal.compiler.v4_0.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.compiler.v4_0.phases.LogicalPlanState
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{NestedPipeExpressions, PipeTreeBuilder}
import org.neo4j.cypher.internal.runtime.parallel.SchedulerTracer
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.vectorized.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.vectorized.{Dispatcher, Pipeline, PipelineBuilder}
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.util.InternalNotification

object MorselRuntime extends CypherRuntime[EnterpriseRuntimeContext] {

  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlan_V35 = {
    val (logicalPlan, physicalPlan) = rewritePlan(context, state.logicalPlan, state.semanticTable())

    val converters: ExpressionConverters = if (context.compileExpressions) {
      new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext),
        MorselExpressionConverters,
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    } else {
      new ExpressionConverters(
        MorselExpressionConverters,
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    }

    val queryIndexes = new QueryIndexes(context.schemaRead)

    // We can use lazy slotted pipes as a fallback for some missing operators. This also converts nested logical plans
    val (slottedPipeMapper: SlottedPipeMapper, logicalPlanWithConvertedNestedPlans: LogicalPlan) =
      createSlottedPipeFallback(state, context, logicalPlan, physicalPlan, converters, queryIndexes)

    val operatorBuilder = new PipelineBuilder(physicalPlan, converters, context.readOnly, queryIndexes, slottedPipeMapper)

    val operators = operatorBuilder.create(logicalPlanWithConvertedNestedPlans)
    val dispatcher = context.runtimeEnvironment.getDispatcher(context.debugOptions)
    val tracer = context.runtimeEnvironment.tracer
    val fieldNames = state.statement().returnColumns.toArray

    VectorizedExecutionPlan(operators,
                            physicalPlan.slotConfigurations,
                            queryIndexes,
                            logicalPlan,
                            fieldNames,
                            dispatcher,
                            tracer)
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan,
                          semanticTable: SemanticTable) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable)
    val slottedRewriter = new SlottedRewriter(context.tokenContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan)
  }

  private def createSlottedPipeFallback(state: LogicalPlanState,
                                        context: EnterpriseRuntimeContext,
                                        logicalPlan: LogicalPlan,
                                        physicalPlan: PhysicalPlan,
                                        converters: ExpressionConverters,
                                        queryIndexes: QueryIndexes): (SlottedPipeMapper, LogicalPlan) = {
    val slottedConverters = if (context.compileExpressions) {
      new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext),
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    } else {
      new ExpressionConverters(
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    }

    val interpretedPipeMapper = InterpretedPipeMapper(context.readOnly, converters, context.tokenContext, queryIndexes)(state.semanticTable)
    val slottedPipeMapper = new SlottedPipeMapper(interpretedPipeMapper, converters, physicalPlan, context.readOnly, queryIndexes)(state.semanticTable, context.tokenContext)
    val pipeTreeBuilder = PipeTreeBuilder(slottedPipeMapper)
    val logicalPlanWithConvertedNestedPlans = NestedPipeExpressions.build(pipeTreeBuilder, logicalPlan)
    (slottedPipeMapper, logicalPlanWithConvertedNestedPlans)
  }

  case class VectorizedExecutionPlan(operators: Pipeline,
                                     slots: SlotConfigurations,
                                     queryIndexes: QueryIndexes,
                                     logicalPlan: LogicalPlan,
                                     fieldNames: Array[String],
                                     dispatcher: Dispatcher,
                                     schedulerTracer: SchedulerTracer) extends ExecutionPlan_V35 {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue,
                     prePopulateResults: Boolean): RuntimeResult = {

      if (queryIndexes.hasLabelScan)
        queryContext.transactionalContext.dataRead.prepareForLabelScans()

      new VectorizedRuntimeResult(operators,
                                  queryIndexes.indexes.map(x => queryContext.transactionalContext.dataRead.indexReadSession(x)),
                                  logicalPlan,
                                  queryContext,
                                  params,
                                  fieldNames,
                                  dispatcher,
                                  schedulerTracer)
    }

    override def runtimeName: RuntimeName = MorselRuntimeName

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] = Set(ExperimentalFeatureNotification("use the morsel runtime at your own peril, " +
                                                                                                   "not recommended to be run on production systems"))
  }

  class VectorizedRuntimeResult(operators: Pipeline,
                                queryIndexes: Array[IndexReadSession],
                                logicalPlan: LogicalPlan,
                                queryContext: QueryContext,
                                params: MapValue,
                                override val fieldNames: Array[String],
                                dispatcher: Dispatcher,
                                schedulerTracer: SchedulerTracer) extends RuntimeResult {

    private var resultRequested = false

    override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {
      dispatcher.execute(operators, queryContext, params, schedulerTracer, queryIndexes)(visitor)
      resultRequested = true
    }

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def isIterable: Boolean = false

    override def asIterator(): ResourceIterator[java.util.Map[String, AnyRef]] =
      throw new UnsupportedOperationException("The Morsel runtime is not iterable")

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (!resultRequested) ConsumptionState.NOT_STARTED
      else ConsumptionState.EXHAUSTED

    override def close(): Unit = {}

    override def queryProfile(): QueryProfile = QueryProfile.NONE
  }

}
