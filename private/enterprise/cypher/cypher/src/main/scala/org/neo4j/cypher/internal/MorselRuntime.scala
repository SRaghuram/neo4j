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
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.morsel.execution.{ProfiledQuerySubscription, QueryExecutor}
import org.neo4j.cypher.internal.runtime.morsel.expressions.MorselBlacklist
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.morsel.{ExecutablePipeline, FuseOperators, MorselPipelineBreakingPolicy, OperatorFactory}
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object MorselRuntime {
  val MORSEL = new MorselRuntime(false, "morsel")
  val PARALLEL = new MorselRuntime(true, "parallel")
}

class MorselRuntime(parallelExecution: Boolean,
                    override val name: String) extends CypherRuntime[EnterpriseRuntimeContext] {

  private val runtimeName = RuntimeName(name)

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext, securityContext: SecurityContext): ExecutionPlan = {
    DebugLog.log("MorselRuntime.compileToExecutable()")

    val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                            query.logicalPlan,
                                            query.semanticTable,
                                            MorselPipelineBreakingPolicy,
                                            context.config.transactionMaxMemory,
                                            allocateArgumentSlots = true)

    MorselBlacklist.throwOnUnsupportedPlan(query.logicalPlan, parallelExecution)

    val converters: ExpressionConverters = if (context.compileExpressions) {
      new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext),
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    } else {
      new ExpressionConverters(
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    }

    val queryIndexRegistrator = new QueryIndexRegistrator(context.schemaRead)

    DebugLog.logDiff("PhysicalPlanner.plan")
    val executionGraphDefinition = PipelineBuilder.build(MorselPipelineBreakingPolicy, physicalPlan)
    val operatorFactory = new OperatorFactory(executionGraphDefinition, converters, true, queryIndexRegistrator, query.semanticTable)

    DebugLog.logDiff("PipelineBuilder")
    //=======================================================
    val fuseOperators = new FuseOperators(operatorFactory, context.config.fuseOperators, context.tokenContext)

    val executablePipelines =
      for (p <- executionGraphDefinition.pipelines) yield {
        fuseOperators.compilePipeline(p)
      }

    DebugLog.logDiff("FuseOperators")
    //=======================================================

    val executor = context.runtimeEnvironment.getQueryExecutor(parallelExecution, context.debugOptions)

    val morselSize = selectMorselSize(query, context)

    MorselExecutionPlan(executablePipelines,
                        executionGraphDefinition,
                        queryIndexRegistrator.result(),
                        physicalPlan.nExpressionSlots,
                        physicalPlan.logicalPlan,
                        physicalPlan.parameterMapping,
                        query.resultColumns,
                        executor,
                        context.runtimeEnvironment.tracer,
                        morselSize)
  }

  private def selectMorselSize(query: LogicalQuery,
                               context: EnterpriseRuntimeContext): Int = {
    val maxCardinality = query.logicalPlan.flatten.map(plan => query.cardinalities.get(plan.id)).max
    val morselSize = if (maxCardinality.amount.toLong > context.config.morselSizeBig) context.config.morselSizeBig else context.config.morselSizeSmall
    morselSize
  }

  case class MorselExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                                 executionGraphDefinition: ExecutionGraphDefinition,
                                 queryIndexes: QueryIndexes,
                                 nExpressionSlots: Int,
                                 logicalPlan: LogicalPlan,
                                 parameterMapping: ParameterMapping,
                                 fieldNames: Array[String],
                                 queryExecutor: QueryExecutor,
                                 schedulerTracer: SchedulerTracer,
                                 morselSize: Int) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     inputDataStream: InputDataStream,
                     subscriber: QuerySubscriber): RuntimeResult = {

      new MorselRuntimeResult(executablePipelines,
                              executionGraphDefinition,
                              queryIndexes.initiateLabelAndSchemaIndexes(queryContext),
                              nExpressionSlots,
                              prePopulateResults,
                              inputDataStream,
                              logicalPlan,
                              queryContext,
                              createParameterArray(params, parameterMapping),
                              fieldNames,
                              queryExecutor,
                              schedulerTracer,
                              subscriber,
                              doProfile,
                              morselSize)
    }

    override def runtimeName: RuntimeName = MorselRuntime.this.runtimeName

    override def metadata: Seq[Argument] = Nil

    override def notifications: Set[InternalNotification] =
      if (parallelExecution)
        Set(ExperimentalFeatureNotification(
          "The parallel runtime is experimental and might suffer from instability and potentially correctness issues."))
      else Set.empty
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
                            subscriber: QuerySubscriber,
                            doProfile: Boolean,
                            morselSize: Int) extends RuntimeResult {

    private var querySubscription: QuerySubscription = _
    private var _queryProfile: QueryProfile = _

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (querySubscription == null) ConsumptionState.NOT_STARTED
      else ConsumptionState.EXHAUSTED

    override def close(): Unit = {}

    override def queryProfile(): QueryProfile = _queryProfile

    override def request(numberOfRecords: Long): Unit = {
      ensureQuerySubscription()
      querySubscription.request(numberOfRecords)
    }

    override def cancel(): Unit = {
      ensureQuerySubscription()
      querySubscription.cancel()
    }

    override def await(): Boolean = {
      ensureQuerySubscription()
      querySubscription.await()
    }

    private def ensureQuerySubscription(): Unit = {
      if (querySubscription == null) {
        val ProfiledQuerySubscription(sub, prof) = queryExecutor.execute(
          executablePipelines,
          executionGraphDefinition,
          inputDataStream,
          queryContext,
          params,
          schedulerTracer,
          queryIndexes,
          nExpressionSlots,
          prePopulateResults,
          subscriber,
          doProfile,
          morselSize)

        querySubscription = sub
        _queryProfile = prof

        //Only call onResult on first call
        subscriber.onResult(fieldNames.length)
      }
    }
  }

}
