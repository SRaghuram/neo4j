/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.CypherOperatorEngineOption
import org.neo4j.cypher.internal.PipelinedRuntime.CODE_GEN_FAILED_MESSAGE
import org.neo4j.cypher.internal.compiler.CodeGenerationFailedNotification
import org.neo4j.cypher.internal.compiler.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefiner
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanner
import org.neo4j.cypher.internal.physicalplanning.ProduceResultOutput
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MemoryTracking
import org.neo4j.cypher.internal.runtime.MemoryTrackingController
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.ThreadSafeResourceManager
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.pipelined.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.pipelined.FuseOperators
import org.neo4j.cypher.internal.runtime.pipelined.InterpretedPipesFallbackPolicy
import org.neo4j.cypher.internal.runtime.pipelined.OperatorFactory
import org.neo4j.cypher.internal.runtime.pipelined.PipelinedPipelineBreakingPolicy
import org.neo4j.cypher.internal.runtime.pipelined.execution.ExecutionGraphSchedulingPolicy
import org.neo4j.cypher.internal.runtime.pipelined.execution.LazyScheduling
import org.neo4j.cypher.internal.runtime.pipelined.execution.ProfiledQuerySubscription
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryExecutor
import org.neo4j.cypher.internal.runtime.pipelined.expressions.PipelinedBlacklist
import org.neo4j.cypher.internal.runtime.pipelined.rewriters.PipelinedPlanRewriter
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscription
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object PipelinedRuntime {
  val PIPELINED = new PipelinedRuntime(false, "pipelined")
  val PARALLEL = new PipelinedRuntime(true, "parallel")

  val CODE_GEN_FAILED_MESSAGE = "Code generation failed. Retrying physical planning."
}

class PipelinedRuntime private(parallelExecution: Boolean,
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

  private val rewriterSequencer: String => RewriterStepSequencer =
    if (Assertion.assertionsEnabled()) RewriterStepSequencer.newValidating else RewriterStepSequencer.newPlain

  private val optimizingRewriter = PipelinedPlanRewriter(rewriterSequencer)

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext, securityContext: SecurityContext): ExecutionPlan = {
    DebugLog.log("PipelinedRuntime.compileToExecutable()")

    if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
      printPlanInfo(query)
    }

    try {
      if (query.periodicCommitInfo.isDefined)
        throw new CantCompileQueryException("Periodic commit is not supported by Pipelined runtime")

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

  private def selectBatchSize(query: LogicalQuery,
                              context: EnterpriseRuntimeContext): Int = {
    val maxCardinality = query.logicalPlan.flatten.map(plan => query.cardinalities.get(plan.id)).max
    val batchSize = if (maxCardinality.amount.toLong > context.config.pipelinedBatchSizeBig) context.config.pipelinedBatchSizeBig else context.config.pipelinedBatchSizeSmall
    batchSize
  }

  /**
   * This tries to compile a plan, first with fusion enabled, second with fusion only inside a pipeline and third without fusion.
   */
  private def compilePlan(operatorFusionPolicy: OperatorFusionPolicy,
                          query: LogicalQuery,
                          context: EnterpriseRuntimeContext,
                          queryIndexRegistrator: QueryIndexRegistrator,
                          warnings: Set[InternalNotification]): PipelinedExecutionPlan = {
    val batchSize = selectBatchSize(query, context)
    val optimizedLogicalPlan = optimizingRewriter(query, batchSize)

    PipelinedBlacklist.throwOnUnsupportedPlan(optimizedLogicalPlan, parallelExecution, query.providedOrders)

    val interpretedPipesFallbackPolicy = InterpretedPipesFallbackPolicy(context.interpretedPipesFallback, parallelExecution)
    val breakingPolicy = PipelinedPipelineBreakingPolicy(operatorFusionPolicy, interpretedPipesFallbackPolicy)

    val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                            optimizedLogicalPlan,
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
    val executionGraphDefinition = ExecutionGraphDefiner.defineFrom(breakingPolicy, operatorFusionPolicy, physicalPlan)

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

      new PipelinedExecutionPlan(executablePipelines,
                                 executionGraphDefinition,
                                 queryIndexRegistrator.result(),
                                 physicalPlan.nExpressionSlots,
                                 physicalPlan.logicalPlan,
                                 physicalPlan.parameterMapping,
                                 query.resultColumns,
                                 executor,
                                 context.runtimeEnvironment.tracer,
                                 batchSize,
                                 context.config.memoryTrackingController,
                                 maybeThreadSafeExecutionResources,
                                 metadata,
                                 warnings,
                                 executionGraphSchedulingPolicy,
                                 optimizedLogicalPlan)
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

  class PipelinedExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                               executionGraphDefinition: ExecutionGraphDefinition,
                               queryIndexes: QueryIndexes,
                               nExpressionSlots: Int,
                               logicalPlan: LogicalPlan,
                               parameterMapping: ParameterMapping,
                               fieldNames: Array[String],
                               queryExecutor: QueryExecutor,
                               schedulerTracer: SchedulerTracer,
                               batchSize: Int,
                               memoryTrackingController: MemoryTrackingController,
                               maybeThreadSafeExecutionResources: Option[(CursorFactory, ResourceManagerFactory)],
                               override val metadata: Seq[Argument],
                               warnings: Set[InternalNotification],
                               executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy,
                               optimizedLogicalPlan: LogicalPlan) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     inputDataStream: InputDataStream,
                     subscriber: QuerySubscriber): RuntimeResult = {

      val doProfile = executionMode == ProfileMode

      new PipelinedRuntimeResult(executablePipelines,
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
        batchSize,
        memoryTrackingController.memoryTracking(doProfile),
        executionGraphSchedulingPolicy)
    }

    override def runtimeName: RuntimeName = PipelinedRuntime.this.runtimeName

    override def notifications: Set[InternalNotification] =
      if (parallelExecution)
        warnings + ExperimentalFeatureNotification(
          "The parallel runtime is experimental and might suffer from instability and potentially correctness issues.")
      else warnings

    override def threadSafeExecutionResources(): Option[(CursorFactory, ResourceManagerFactory)] = maybeThreadSafeExecutionResources

    override def operatorMetadata(plan: Id): Seq[Argument] = {
      val maybePipelineInfo = pipelineInfoForPlanInExecutionGraph(plan)
      maybePipelineInfo.toSeq
    }

    // TODO remove this
    override def rewrittenPlan: Option[LogicalPlan] = Some(optimizedLogicalPlan)

    private def pipelineInfoForPlanInExecutionGraph(planId: Id): Option[PipelineInfo] = {
      // If this proves to be too costly we could maintain a map structure
      executionGraphDefinition.pipelines.foreach { pipeline =>
        if (pipeline.fusedPlans.exists(_.id.x == planId.x)) {
          return Some(PipelineInfo(s"${pipeline.id.x}.0F"))
        }
        else if (pipeline.headPlan.id == planId) {
          return Some(PipelineInfo(s"${pipeline.id.x}.0"))
        }
        else {
          val middleIndex = pipeline.middlePlans.indexWhere(_.id.x == planId.x)
          if (middleIndex >= 0) {
            return Some(PipelineInfo(s"${pipeline.id.x}.${middleIndex + 1}"))
          } else {
            pipeline.outputDefinition match {
              case ProduceResultOutput(o) if o.id.x == planId.x =>
                val outputIndex = 1 + pipeline.middlePlans.size
                return Some(PipelineInfo(s"${pipeline.id.x}.$outputIndex"))

              case _ =>
                // Do nothing
            }
          }
        }
      }
      None
    }
  }

  class PipelinedRuntimeResult(executablePipelines: IndexedSeq[ExecutablePipeline],
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
                               batchSize: Int,
                               memoryTracking: MemoryTracking,
                               executionGraphSchedulingPolicy: ExecutionGraphSchedulingPolicy) extends RuntimeResult {

    private var querySubscription: QuerySubscription = _
    private var _queryProfile: QueryProfile = _
    private var _memoryTracker: QueryMemoryTracker = _

    override def queryStatistics(): runtime.QueryStatistics = queryContext.getOptStatistics.getOrElse(QueryStatistics())

    override def totalAllocatedMemory(): Long = {
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
          batchSize,
          memoryTracking,
          executionGraphSchedulingPolicy)

        querySubscription = sub
        _queryProfile = prof
        _memoryTracker = memTrack
      }
    }
  }

}
