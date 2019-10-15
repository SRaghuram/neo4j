/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import java.lang
import java.util.Optional

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.MorselRuntime.CODE_GEN_FAILED_MESSAGE
import org.neo4j.cypher.internal.compiler.{CodeGenerationFailedNotification, ExperimentalFeatureNotification}
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.morsel.execution.{ExecutionGraphSchedulingPolicy, LazyScheduling, ProfiledQuerySubscription, QueryExecutor}
import org.neo4j.cypher.internal.runtime.morsel.expressions.MorselBlacklist
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.v4_0.util.{CypherException, InternalNotification}
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.cypher.{CypherInterpretedPipesFallbackOption, CypherOperatorEngineOption}
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.{CursorFactory, IndexReadSession}
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscription}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object MorselRuntime {
  val MORSEL = new MorselRuntime(false, "morsel")
  val PARALLEL = new MorselRuntime(true, "parallel")

  val CODE_GEN_FAILED_MESSAGE = "Code generation failed. Retrying physical planning."
}

class MorselRuntime(parallelExecution: Boolean,
                    override val name: String) extends CypherRuntime[EnterpriseRuntimeContext] with DebugPrettyPrinter {

  private val runtimeName = RuntimeName(name)

  val ENABLE_DEBUG_PRINTS = false // NOTE: false toggles all debug prints off, overriding the individual settings below

  // Should we print query text and logical plan before we see any exceptions from execution plan building?
  // Setting this to true is useful if you want to see the query and logical plan while debugging a failure
  // Setting this to false is useful if you want to quickly spot the failure reason at the top of the output from tests
  val PRINT_PLAN_INFO_EARLY = false

  override val PRINT_QUERY_TEXT = true
  override val PRINT_LOGICAL_PLAN = true
  override val PRINT_REWRITTEN_LOGICAL_PLAN = true
  override val PRINT_PIPELINE_INFO = false
  override val PRINT_FAILURE_STACK_TRACE = true

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext, securityContext: SecurityContext): ExecutionPlan = {
    DebugLog.log("MorselRuntime.compileToExecutable()")

    if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
      printPlanInfo(query)
    }

    try {
      if (query.periodicCommitInfo.isDefined)
        throw new CantCompileQueryException("Periodic commit is not supported by Morsel runtime")

      val shouldFuseOperators = context.operatorEngine == CypherOperatorEngineOption.compiled
      val operatorFusionPolicy = OperatorFusionPolicy(shouldFuseOperators, fusionOverPipelinesEnabled = !parallelExecution)

      compilePlan(operatorFusionPolicy,
                  query,
                  context,
                  new QueryIndexRegistrator(context.schemaRead),
                  Set.empty)
    } catch {
      case e: CypherException if ENABLE_DEBUG_PRINTS =>
        printFailureStackTrace(e)
        if (!PRINT_PLAN_INFO_EARLY) {
          printPlanInfo(query)
        }
        throw e
    }
  }

  private def selectMorselSize(query: LogicalQuery,
                               context: EnterpriseRuntimeContext): Int = {
    val maxCardinality = query.logicalPlan.flatten.map(plan => query.cardinalities.get(plan.id)).max
    val morselSize = if (maxCardinality.amount.toLong > context.config.morselSizeBig) context.config.morselSizeBig else context.config.morselSizeSmall
    morselSize
  }

  /**
   * This tries to compile a plan, first with fusion enabled, second with fusion only inside a pipeline and third without fusion.
   */
  private def compilePlan(operatorFusionPolicy: OperatorFusionPolicy,
                          query: LogicalQuery,
                          context: EnterpriseRuntimeContext,
                          queryIndexRegistrator: QueryIndexRegistrator,
                          warnings: Set[InternalNotification]): MorselExecutionPlan = {
    val interpretedPipesFallbackPolicy = InterpretedPipesFallbackPolicy(context.interpretedPipesFallback, parallelExecution)
    val breakingPolicy = MorselPipelineBreakingPolicy(operatorFusionPolicy, interpretedPipesFallbackPolicy)

    val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                            query.logicalPlan,
                                            query.semanticTable,
                                            breakingPolicy,
                                            allocateArgumentSlots = true)

    if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
      printRewrittenPlanInfo(physicalPlan.logicalPlan)
    }

    val codeGenerationMode = CodeGeneration.CodeGenerationMode.fromDebugOptions(context.debugOptions)

    val converters: ExpressionConverters = if (context.compileExpressions) {
      new ExpressionConverters(
        new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext, query.readOnly, codeGenerationMode),
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    } else {
      new ExpressionConverters(
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(context.tokenContext))
    }

    MorselBlacklist.throwOnUnsupportedPlan(query.logicalPlan, parallelExecution)

    //=======================================================
    val slottedPipeBuilder =
      if (context.interpretedPipesFallback != CypherInterpretedPipesFallbackOption.disabled) {
        val slottedPipeBuilderFallback = InterpretedPipeMapper(query.readOnly, converters, context.tokenContext, queryIndexRegistrator)(query.semanticTable)
        Some(new SlottedPipeMapper(slottedPipeBuilderFallback, converters, physicalPlan, query.readOnly, queryIndexRegistrator)(query.semanticTable))
      }
      else
        None
    //=======================================================

    DebugLog.logDiff("PhysicalPlanner.plan")
    val executionGraphDefinition = PipelineBuilder.build(breakingPolicy, operatorFusionPolicy, physicalPlan)

    // Currently only interpreted pipes can do writes. Ask the policy if it is allowed.
    val readOnly = interpretedPipesFallbackPolicy.readOnly

    val operatorFactory = new OperatorFactory(executionGraphDefinition,
                                              converters,
                                              readOnly = readOnly,
                                              queryIndexRegistrator,
                                              query.semanticTable,
                                              interpretedPipesFallbackPolicy,
                                              slottedPipeBuilder)

    DebugLog.logDiff("PipelineBuilder")
    //=======================================================

    // Let scheduling policy compute static information
    val executionGraphSchedulingPolicy = LazyScheduling.executionGraphSchedulingPolicy(executionGraphDefinition)

    DebugLog.logDiff("Scheduling")
    //=======================================================
    val fuseOperators = new FuseOperators(operatorFactory, context.tokenContext, parallelExecution,
      codeGenerationMode)

    try {
      val executablePipelines: IndexedSeq[ExecutablePipeline] = fuseOperators.compilePipelines(executionGraphDefinition)
      DebugLog.logDiff("FuseOperators")
      //=======================================================

      val executor = context.runtimeEnvironment.getQueryExecutor(parallelExecution)

      val morselSize = selectMorselSize(query, context)

      val maybeThreadSafeExecutionResources =
        if (parallelExecution) {
          val resourceManagerFactory: ResourceManagerFactory = new ThreadSafeResourceManager(_)
          Some((context.runtimeEnvironment.cursors, resourceManagerFactory))
        }
        else {
          None
        }

      val metadata = CodeGenPlanDescriptionHelper.metadata(codeGenerationMode.saver)

      if (ENABLE_DEBUG_PRINTS) {
        if (!PRINT_PLAN_INFO_EARLY) {
          // Print after execution plan building to see any occurring exceptions first
          printPlanInfo(query)
          printRewrittenPlanInfo(physicalPlan.logicalPlan)
        }
        printPipe(physicalPlan.slotConfigurations)
      }

      new MorselExecutionPlan(executablePipelines,
                              executionGraphDefinition,
                              queryIndexRegistrator.result(),
                              physicalPlan.nExpressionSlots,
                              physicalPlan.logicalPlan,
                              physicalPlan.parameterMapping,
                              query.resultColumns,
                              executor,
                              context.runtimeEnvironment.tracer,
                              morselSize,
                              context.config.memoryTrackingController,
                              maybeThreadSafeExecutionResources,
                              metadata,
                              warnings,
                              executionGraphSchedulingPolicy)
    } catch {
      case e:Exception if operatorFusionPolicy.fusionEnabled =>
        // We failed to compile all the pipelines. Retry physical planning with fusing disabled.
        context.log.debug(CODE_GEN_FAILED_MESSAGE, e)
        DebugLog.log("Could not compile pipeline because of %s", e)

        val warning = CodeGenerationFailedNotification(e.getMessage)

        val nextOperatorFusionPolicy =
          if (operatorFusionPolicy.fusionOverPipelineEnabled)
            // Try again with fusion within pipeline enabled
            OperatorFusionPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = false)
          else
            // Try again with fusion disabled
            OperatorFusionPolicy(fusionEnabled = false, fusionOverPipelinesEnabled = false)

        compilePlan(nextOperatorFusionPolicy, query, context, queryIndexRegistrator, warnings + warning)
    }
  }

  class MorselExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                            executionGraphDefinition: ExecutionGraphDefinition,
                            queryIndexes: QueryIndexes,
                            nExpressionSlots: Int,
                            logicalPlan: LogicalPlan,
                            parameterMapping: ParameterMapping,
                            fieldNames: Array[String],
                            queryExecutor: QueryExecutor,
                            schedulerTracer: SchedulerTracer,
                            morselSize: Int,
                            memoryTrackingController: MemoryTrackingController,
                            maybeThreadSafeExecutionResources: Option[(CursorFactory, ResourceManagerFactory)],
                            override val metadata: Seq[Argument],
                            warnings: Set[InternalNotification],
                            executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode,
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
                              executionMode == ProfileMode,
                              morselSize,
                              memoryTrackingController.memoryTracking,
                              executionGraphSchedulingPolicy)
    }

    override def runtimeName: RuntimeName = MorselRuntime.this.runtimeName

    override def notifications: Set[InternalNotification] =
      if (parallelExecution)
        warnings + ExperimentalFeatureNotification(
          "The parallel runtime is experimental and might suffer from instability and potentially correctness issues.")
      else warnings

    override def threadSafeExecutionResources(): Option[(CursorFactory, ResourceManagerFactory)] = maybeThreadSafeExecutionResources
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
                            morselSize: Int,
                            memoryTracking: MemoryTracking,
                            executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy) extends RuntimeResult {

    private var querySubscription: QuerySubscription = _
    private var _queryProfile: QueryProfile = _
    private var _memoryTracker: QueryMemoryTracker = _

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def totalAllocatedMemory(): Optional[lang.Long] = {
      ensureQuerySubscription()
      _memoryTracker.totalAllocatedMemory
    }

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
        // Only call onResult on first call. Having this callback before execute()
        // ensure that we do not leave any inconsistent state around if onResult
        // throws an exception.
        subscriber.onResult(fieldNames.length)

        val ProfiledQuerySubscription(sub, prof, memTrack) = queryExecutor.execute(
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
          morselSize,
          memoryTracking,
          executionGraphSchedulingPolicy)

        querySubscription = sub
        _queryProfile = prof
        _memoryTracker = memTrack
      }
    }
  }

}
