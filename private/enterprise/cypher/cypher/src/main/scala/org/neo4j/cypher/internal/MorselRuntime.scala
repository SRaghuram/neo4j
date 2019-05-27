/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.{ExecutionGraphDefinition, PhysicalPlanner, PipelineBuilder}
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.morsel.execution.QueryExecutor
import org.neo4j.cypher.internal.runtime.morsel.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.morsel.operators.CompiledQueryResultRecord
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutablePipeline, FuseOperators, MorselPipelineBreakingPolicy, OperatorFactory}
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, QueryResult, RuntimeResult}
import org.neo4j.graphdb
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object MorselRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def name: String = "morsel"

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext, username: String): ExecutionPlan = {
    DebugLog.log("MorselRuntime.compileToExecutable()")
    val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                            query.logicalPlan,
                                            query.semanticTable,
                                            MorselPipelineBreakingPolicy,
                                            allocateArgumentSlots = true)

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

    DebugLog.logDiff("PhysicalPlanner.plan")
    val executionGraphDefinition = PipelineBuilder.build(MorselPipelineBreakingPolicy, physicalPlan)
    val operatorFactory = new OperatorFactory(executionGraphDefinition, converters, true, queryIndexes)

    DebugLog.logDiff("PipelineBuilder")
    //=======================================================
    val fuseOperators = new FuseOperators(operatorFactory, context.config.fuseOperators, context.tokenContext)

    val executablePipelines =
      for (p <- executionGraphDefinition.pipelines) yield {
        fuseOperators.compilePipeline(p)
      }

    DebugLog.logDiff("FuseOperators")
    //=======================================================

    val executor = context.runtimeEnvironment.getQueryExecutor(context.debugOptions)

    MorselExecutionPlan(executablePipelines,
      executionGraphDefinition,
      queryIndexes,
      physicalPlan.nExpressionSlots,
      physicalPlan.logicalPlan,
      physicalPlan.parameterMapping,
      query.resultColumns,
      executor,
      context.runtimeEnvironment.tracer)
  }

  case class MorselExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                                 executionGraphDefinition: ExecutionGraphDefinition,
                                 queryIndexes: QueryIndexes,
                                 nExpressionSlots: Int,
                                 logicalPlan: LogicalPlan,
                                 parameterMapping: Map[String, Int],
                                 fieldNames: Array[String],
                                 queryExecutor: QueryExecutor,
                                 schedulerTracer: SchedulerTracer) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     inputDataStream: InputDataStream,
                     subscriber: QuerySubscriber): RuntimeResult = {
      if (queryIndexes.hasLabelScan)
        queryContext.transactionalContext.dataRead.prepareForLabelScans()

      new MorselRuntimeResult(executablePipelines,
        executionGraphDefinition,
        queryIndexes.indexes.map(x => queryContext.transactionalContext.dataRead.indexReadSession(x)),
        nExpressionSlots,
        prePopulateResults,
        inputDataStream,
        logicalPlan,
        queryContext,
        createParameterArray(params, parameterMapping),
        fieldNames,
        queryExecutor,
        schedulerTracer,
        subscriber: QuerySubscriber)
    }

    override def runtimeName: RuntimeName = MorselRuntimeName

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] = Set(ExperimentalFeatureNotification("use the morsel runtime at your own peril, " +
      "not recommended to be run on production systems"))
  }

  class MorselRuntimeResult(executablePipelines: IndexedSeq[ExecutablePipeline],
                            executionGraphDefinition: ExecutionGraphDefinition,
                            queryIndexes: Array[IndexReadSession],
                            nExpressionSlots: Int,
                            prePopulateResults: Boolean,
                            inputDataStream: InputDataStream,
                            logicalPlan: LogicalPlan,
                            queryContext: QueryContext,
                            params: Array[AnyValue],
                            override val fieldNames: Array[String],
                            queryExecutor: QueryExecutor,
                            schedulerTracer: SchedulerTracer,
                            subscriber: QuerySubscriber) extends RuntimeResult {
    private var resultRequested = false

    private var querySubscription: QuerySubscription = _

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (!resultRequested) ConsumptionState.NOT_STARTED
      else ConsumptionState.EXHAUSTED

    override def close(): Unit = {}

    override def queryProfile(): QueryProfile = QueryProfile.NONE

    override def request(numberOfRecords: Long): Unit = {
      if (querySubscription == null) {
        resultRequested = true
        querySubscription = queryExecutor.execute(
          executablePipelines,
          executionGraphDefinition,
          inputDataStream,
          queryContext,
          params,
          schedulerTracer,
          queryIndexes,
          nExpressionSlots,
          prePopulateResults,
          subscriber)
        //Only call onResult on first call
        subscriber.onResult(fieldNames.length)
      }

      querySubscription.request(numberOfRecords)
    }

    override def cancel(): Unit =
      querySubscription.cancel()

    override def await(): Boolean = {
      querySubscription.await()
    }
  }

}

class VisitSubscriber(visitor: QueryResultVisitor[_], numberOfFields: Int) extends QuerySubscriber {
  private val array: Array[AnyValue] = new Array[AnyValue](numberOfFields)
  private val results: QueryResult.Record = new CompiledQueryResultRecord(array)

  override def onResult(numberOfFields: Int): Unit = {
  }

  override def onRecord(): Unit = {}

  override def onField(offset: Int, value: AnyValue): Unit = {
    array(offset) = value
  }

  override def onRecordCompleted(): Unit = {
    visitor.visit(results)
  }

  override def onError(throwable: Throwable): Unit = {
    //errors are propagated via the QueryCompletionTracker
  }

  override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {

  }
}
