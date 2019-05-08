/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.{PhysicalPlanner, PipelineBuilder, StateDefinition}
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.morsel.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.zombie.execution.QueryExecutor
import org.neo4j.cypher.internal.runtime.zombie.operators.CompiledQueryResultRecord
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, QueryResult, RuntimeResult}
import org.neo4j.graphdb
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object ZombieRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def name: String = "zombie"

  val ENABLED = false

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext): ExecutionPlan = {
    val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
      query.logicalPlan,
      query.semanticTable,
      ZombiePipelineBreakingPolicy,
      allocateArgumentSlots = true)

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

    val stateDefinition = new StateDefinition(physicalPlan)
    val pipelineBuilder = new PipelineBuilder(ZombiePipelineBreakingPolicy, stateDefinition, physicalPlan.slotConfigurations)

    pipelineBuilder.build(physicalPlan.logicalPlan)
    val operatorFactory = new OperatorFactory(stateDefinition, converters, true, queryIndexes)

    //=======================================================
    val fuseOperators = new FuseOperators(operatorFactory, context.config.fuseOperators, context.tokenContext)

    val executablePipelines =
      for (p <- pipelineBuilder.pipelines) yield {
        fuseOperators.compilePipeline(p)
      }
    //=======================================================

    val executor = context.runtimeEnvironment.getQueryExecutor(context.debugOptions)

    ZombieExecutionPlan(executablePipelines,
      stateDefinition,
      queryIndexes,
      physicalPlan.nExpressionSlots,
      physicalPlan.logicalPlan,
      physicalPlan.parameterMapping,
      query.resultColumns,
      executor,
      context.runtimeEnvironment.tracer)
  }

  case class ZombieExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                                 stateDefinition: StateDefinition,
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

      new ZombieRuntimeResult(executablePipelines,
        stateDefinition,
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

  class ZombieRuntimeResult(executablePipelines: IndexedSeq[ExecutablePipeline],
                            stateDefinition: StateDefinition,
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

    override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {
      val executionHandle = queryExecutor.execute(executablePipelines,
                                                  stateDefinition,
                                                  inputDataStream,
                                                  queryContext,
                                                  params,
                                                  schedulerTracer,
                                                  queryIndexes,
                                                  nExpressionSlots,
                                                  prePopulateResults,
                                                  new VisitSubscriber(visitor, fieldNames.length))

      executionHandle.request(Long.MaxValue)
      resultRequested = true
      executionHandle.await()
    }

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def isIterable: Boolean = false

    override def asIterator(): ResourceIterator[java.util.Map[String, AnyRef]] =
      throw new UnsupportedOperationException("The Zombie runtime is not iterable")

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
          stateDefinition,
          inputDataStream,
          queryContext,
          params,
          schedulerTracer,
          queryIndexes,
          nExpressionSlots,
          prePopulateResults,
          subscriber)
      }
      querySubscription.request(numberOfRecords)
      subscriber.onResult(fieldNames.length)
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
