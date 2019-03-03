/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CypherMorselRuntimeSchedulerOption.SingleThreaded
import org.neo4j.cypher.internal.compiler.v4_0.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{NestedPipeExpressions, PipeTreeBuilder}
import org.neo4j.cypher.internal.runtime.morsel.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.morsel.{Dispatcher, Pipeline, PipelineBuilder}
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.slotted.{SlottedPipeMapper, SlottedPipelineBreakingPolicy}
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{NaiveQuerySubscription, QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.internal.kernel.api.{CursorFactory, IndexReadSession}
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

object MorselRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def name: String = "morsel"

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext): ExecutionPlan = {
    val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                            query.logicalPlan,
                                            query.semanticTable,
                                            SlottedPipelineBreakingPolicy)

    val converters: ExpressionConverters = if (context.compileExpressions) {
      new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext),
        MorselExpressionConverters(physicalPlan),
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    } else {
      new ExpressionConverters(
        MorselExpressionConverters(physicalPlan),
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    }

    val queryIndexes = new QueryIndexes(context.schemaRead)

    // We can use lazy slotted pipes as a fallback for some missing operators. This also converts nested logical plans
    val (slottedPipeMapper: SlottedPipeMapper, logicalPlanWithConvertedNestedPlans: LogicalPlan) =
      createSlottedPipeFallback(query, context, physicalPlan, converters, queryIndexes)

    val operatorBuilder = new PipelineBuilder(physicalPlan, converters, query.readOnly, queryIndexes, slottedPipeMapper)

    val operators = operatorBuilder.create(logicalPlanWithConvertedNestedPlans)
    val dispatcher = context.runtimeEnvironment.getDispatcher(context.debugOptions)
    val tracer = context.runtimeEnvironment.tracer
    val fieldNames = query.resultColumns

    val maybeThreadSafeCursors = if (context.config.scheduler == SingleThreaded) None else Some(context.runtimeEnvironment.cursors)

    MorselExecutionPlan(operators,
                        physicalPlan.slotConfigurations,
                        queryIndexes,
                        physicalPlan.nExpressionSlots,
                        physicalPlan.logicalPlan,
                        fieldNames,
                        dispatcher,
                        tracer,
                        maybeThreadSafeCursors)
  }

  private def createSlottedPipeFallback(query: LogicalQuery,
                                        context: EnterpriseRuntimeContext,
                                        physicalPlan: PhysicalPlan,
                                        converters: ExpressionConverters,
                                        queryIndexes: QueryIndexes): (SlottedPipeMapper, LogicalPlan) = {
    val interpretedPipeMapper = InterpretedPipeMapper(query.readOnly, converters, context.tokenContext, queryIndexes)(query.semanticTable)
    val slottedPipeMapper = new SlottedPipeMapper(interpretedPipeMapper, converters, physicalPlan, query.readOnly, queryIndexes)(query.semanticTable, context.tokenContext)
    val pipeTreeBuilder = PipeTreeBuilder(slottedPipeMapper)
    val logicalPlanWithConvertedNestedPlans = NestedPipeExpressions.build(pipeTreeBuilder, physicalPlan.logicalPlan, physicalPlan.availableExpressionVariables)
    (slottedPipeMapper, logicalPlanWithConvertedNestedPlans)
  }

  case class MorselExecutionPlan(operators: Pipeline,
                                 slots: SlotConfigurations,
                                 queryIndexes: QueryIndexes,
                                 nExpressionSlots: Int,
                                 logicalPlan: LogicalPlan,
                                 fieldNames: Array[String],
                                 dispatcher: Dispatcher,
                                 schedulerTracer: SchedulerTracer,
                                 maybeThreadSafeCursors: Option[CursorFactory]) extends ExecutionPlan {

    override def threadSafeCursorFactory(debugOptions: Set[String]): Option[CursorFactory] =
      if (MorselOptions.singleThreaded(debugOptions)) None else maybeThreadSafeCursors

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     input: InputDataStream,
                     subscriber: QuerySubscriber): RuntimeResult = {

      if (queryIndexes.hasLabelScan)
        queryContext.transactionalContext.dataRead.prepareForLabelScans()

      new MorselRuntimeResult(operators,
                              queryIndexes.indexes.map(x => queryContext.transactionalContext.dataRead.indexReadSession(x)),
                              nExpressionSlots,
                              logicalPlan,
                              queryContext,
                              params,
                              fieldNames,
                              dispatcher,
                              schedulerTracer,
                              input, subscriber)
    }

    override def runtimeName: RuntimeName = MorselRuntimeName

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] = Set(ExperimentalFeatureNotification("use the morsel runtime at your own peril, " +
                                                                                                   "not recommended to be run on production systems"))
  }

  class MorselRuntimeResult(operators: Pipeline,
                            queryIndexes: Array[IndexReadSession],
                            nExpressionSlots: Int,
                            logicalPlan: LogicalPlan,
                            queryContext: QueryContext,
                            params: MapValue,
                            override val fieldNames: Array[String],
                            dispatcher: Dispatcher,
                            schedulerTracer: SchedulerTracer,
                            input: InputDataStream,
                            subscriber: QuerySubscriber) extends NaiveQuerySubscription(subscriber) {

    private var resultRequested = false

    override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {
      dispatcher.execute(operators, queryContext, params, schedulerTracer, queryIndexes, nExpressionSlots, input)(visitor)
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
