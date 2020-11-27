/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.PipelinedRuntime.CODE_GEN_FAILED_MESSAGE
import org.neo4j.cypher.internal.compiler.CodeGenerationFailedNotification
import org.neo4j.cypher.internal.compiler.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.compiler.PushBatchedExecution
import org.neo4j.cypher.internal.config.MemoryTracking
import org.neo4j.cypher.internal.config.MemoryTrackingController
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefiner
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphVisualizer
import org.neo4j.cypher.internal.physicalplanning.FusedHead
import org.neo4j.cypher.internal.physicalplanning.InterpretedHead
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanner
import org.neo4j.cypher.internal.physicalplanning.ProduceResultOutput
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.rewrite.FusedPlanDescriptionArgumentRewriter
import org.neo4j.cypher.internal.plandescription.rewrite.InternalPlanDescriptionRewriter
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.ThreadSafeResourceManager
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpressions
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTreeBuilder
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ExecutablePipeline
import org.neo4j.cypher.internal.runtime.pipelined.InterpretedPipesFallbackPolicy
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorFactory
import org.neo4j.cypher.internal.runtime.pipelined.PipelineCompiler
import org.neo4j.cypher.internal.runtime.pipelined.PipelinedPipelineBreakingPolicy
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperatorPolicy
import org.neo4j.cypher.internal.runtime.pipelined.execution.ExecutingQuery
import org.neo4j.cypher.internal.runtime.pipelined.execution.ExecutionGraphSchedulingPolicy
import org.neo4j.cypher.internal.runtime.pipelined.execution.LazyScheduling
import org.neo4j.cypher.internal.runtime.pipelined.execution.ProfiledQuerySubscription
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryExecutor
import org.neo4j.cypher.internal.runtime.pipelined.expressions.PipelinedBlacklist
import org.neo4j.cypher.internal.runtime.pipelined.rewriters.pipelinedPrePhysicalPlanRewriter
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipelineBreakingPolicy
import org.neo4j.cypher.internal.runtime.slotted.expressions.CompiledExpressionConverter
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object PipelinedRuntime {
  val PIPELINED = new PipelinedRuntime(false, "Pipelined")
  val PARALLEL = new PipelinedRuntime(true, "Parallel")

  val CODE_GEN_FAILED_MESSAGE = "Code generation failed. Retrying physical planning."
}

class PipelinedRuntime private(parallelExecution: Boolean,
                               override val name: String) extends CypherRuntime[EnterpriseRuntimeContext] with DebugPrettyPrinter {

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = if (parallelExecution) Some(CypherRuntimeOption.parallel) else Some(CypherRuntimeOption.pipelined)

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

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext): ExecutionPlan = {
    DebugLog.log("PipelinedRuntime.compileToExecutable()")

    if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
      printPlanInfo(query)
    }

    try {
      if (query.periodicCommitInfo.isDefined)
        throw new CantCompileQueryException("Periodic commit is not supported by Pipelined runtime")

      val shouldFuseOperators = context.operatorEngine match {
        case CypherOperatorEngineOption.default |
             CypherOperatorEngineOption.compiled => true
        case _                                   => false
      }
      val operatorFusionPolicy = TemplateOperatorPolicy(shouldFuseOperators,
                                                        fusionOverPipelinesEnabled = !parallelExecution,
                                                        fusionOverPipelineLimit = context.config.operatorFusionOverPipelineLimit,
                                                        query.readOnly,
                                                        parallelExecution)

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


  /**
   * This tries to compile a plan, first with fusion enabled, second with fusion only inside a pipeline and third without fusion.
   */
  private def compilePlan(operatorFusionPolicy: OperatorFusionPolicy,
                          query: LogicalQuery,
                          context: EnterpriseRuntimeContext,
                          queryIndexRegistrator: QueryIndexRegistrator,
                          warnings: Set[InternalNotification]): ExecutionPlan = {
    val batchSize = PushBatchedExecution(context.config.pipelinedBatchSizeSmall, context.config.pipelinedBatchSizeBig)
      .selectBatchSize(query.logicalPlan, query.cardinalities)

    val logicalPlan = pipelinedPrePhysicalPlanRewriter(query, parallelExecution)

    PipelinedBlacklist.throwOnUnsupportedPlan(logicalPlan, parallelExecution, query.leveragedOrders, name)

    val interpretedPipesFallbackPolicy = InterpretedPipesFallbackPolicy(context.interpretedPipesFallback, parallelExecution, name)
    val breakingPolicy = PipelinedPipelineBreakingPolicy(
      operatorFusionPolicy,
      interpretedPipesFallbackPolicy,
      parallelExecution,
      nestedPlanBreakingPolicy = SlottedPipelineBreakingPolicy)

    var physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                            logicalPlan,
                                            query.semanticTable,
                                            breakingPolicy,
                                            context.config,
                                            allocateArgumentSlots = true)

    if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
      printRewrittenPlanInfo(physicalPlan.logicalPlan)
    }

    val codeGenerationMode = CodeGeneration.CodeGenerationMode.fromDebugOptions(context.debugOptions)

    val converters: ExpressionConverters = {
      val builder = Seq.newBuilder[ExpressionConverter]
      if (context.compileExpressions)
        builder += new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext, query.readOnly, codeGenerationMode, context.compiledExpressionsContext)
      builder += SlottedExpressionConverters(physicalPlan, Some(NoPipe))
      builder += CommunityExpressionConverter(context.tokenContext)
      new ExpressionConverters(builder.result():_*)
    }

    //=======================================================
    val pipeMapper =
      {
        val interpretedPipeMapper = InterpretedPipeMapper(query.readOnly, converters, context.tokenContext, queryIndexRegistrator)(query.semanticTable)
        new SlottedPipeMapper(interpretedPipeMapper, converters, physicalPlan, query.readOnly, queryIndexRegistrator)(query.semanticTable)
      }

    val pipeTreeBuilder = PipeTreeBuilder(pipeMapper)
    physicalPlan = physicalPlan.copy(logicalPlan = NestedPipeExpressions.build(pipeTreeBuilder,
                                                                               physicalPlan.logicalPlan,
                                                                               physicalPlan.availableExpressionVariables))

    DebugLog.logDiff("PhysicalPlanner.plan")
    //=======================================================

    val executionGraphDefinition =
      ExecutionGraphDefiner.defineFrom(
        breakingPolicy,
        operatorFusionPolicy.operatorFuserFactory(physicalPlan,
                                                  context.tokenContext,
                                                  query.readOnly,
                                                  query.doProfile,
                                                  queryIndexRegistrator,
                                                  parallelExecution,
                                                  codeGenerationMode,
                                                  context.config.lenientCreateRelationship),
        physicalPlan,
        converters,
        query.leveragedOrders)

    if (context.debugOptions.visualizePipelinesEnabled) {
      return ExecutionGraphVisualizer.getExecutionPlan(executionGraphDefinition)
    }

    // Currently only interpreted pipes can do writes. Ask the policy if it is allowed.
    val readOnly = interpretedPipesFallbackPolicy.readOnly

    val maybePipeMapper = if (context.interpretedPipesFallback != CypherInterpretedPipesFallbackOption.disabled) Some(pipeMapper) else None
    val operatorFactory = new OperatorFactory(executionGraphDefinition,
      converters,
      readOnly = readOnly,
      queryIndexRegistrator,
      query.semanticTable,
      interpretedPipesFallbackPolicy,
      maybePipeMapper,
      name,
      parallelExecution,
      context.config.lenientCreateRelationship)

    DebugLog.logDiff("ExecutionGraphDefiner")
    //=======================================================

    // Let scheduling policy compute static information
    val executionGraphSchedulingPolicy = LazyScheduling.executionGraphSchedulingPolicy(executionGraphDefinition)

    DebugLog.logDiff("Scheduling")
    //=======================================================
    val fuseOperators = new PipelineCompiler(operatorFactory, context.tokenContext, parallelExecution,
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
                                 logicalPlan)
    } catch {
      case e:Exception if operatorFusionPolicy.fusionEnabled =>
        // We failed to compile all the pipelines. Retry physical planning with fusing disabled.
        context.log.debug(CODE_GEN_FAILED_MESSAGE, e)
        DebugLog.log("Could not compile pipeline because of %s", e)
        val warning = CodeGenerationFailedNotification(e.getMessage)

        val nextOperatorFusionPolicy =
          if (operatorFusionPolicy.fusionOverPipelineEnabled)
          // Try again with fusion within pipeline enabled
            TemplateOperatorPolicy(fusionEnabled = true, fusionOverPipelinesEnabled = false, context.config.operatorFusionOverPipelineLimit, query.readOnly, parallelExecution)
          else
          // Try again with fusion disabled
            TemplateOperatorPolicy(fusionEnabled = false, fusionOverPipelinesEnabled = false, context.config.operatorFusionOverPipelineLimit, query.readOnly, parallelExecution)

        if (DebugSupport.PHYSICAL_PLANNING.enabled) {
          DebugSupport.PHYSICAL_PLANNING.log("Could not compile pipeline because of %s. Retrying with %s", e, nextOperatorFusionPolicy)
        }

        compilePlan(nextOperatorFusionPolicy, query, context, queryIndexRegistrator, warnings + warning)
    }
  }

  class PipelinedExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                               executionGraphDefinition: ExecutionGraphDefinition,
                               queryIndexes: QueryIndexes,
                               nExpressionSlots: Int,
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

    override def rewrittenPlan: Option[LogicalPlan] = Some(optimizedLogicalPlan)

    private def pipelineInfoForPlanInExecutionGraph(planId: Id): Option[PipelineInfo] = {
      // If this proves to be too costly we could maintain a map structure
      executionGraphDefinition.pipelines.foreach { pipeline =>
        pipeline.headPlan match {
          case InterpretedHead(plan) if plan.id == planId =>
            return Some(PipelineInfo(pipeline.id.x, fused = false))

          case FusedHead(fuser) if fuser.fusedPlans.exists(_.id.x == planId.x) =>
            return Some(PipelineInfo(pipeline.id.x, fused = true))

          case _ =>
            val middleIndex = pipeline.middlePlans.indexWhere(_.id.x == planId.x)
            if (middleIndex >= 0) {
              return Some(PipelineInfo(pipeline.id.x, fused = false))
            } else {
              pipeline.outputDefinition match {
                case ProduceResultOutput(o) if o.id.x == planId.x =>
                  return Some(PipelineInfo(pipeline.id.x, fused = false))

                case _ =>
                // Do nothing
              }
            }
        }
      }
      None
    }

    override val internalPlanDescriptionRewriter: Option[InternalPlanDescriptionRewriter] = Some(new FusedPlanDescriptionArgumentRewriter)
  }

  class PipelinedRuntimeResult(executablePipelines: IndexedSeq[ExecutablePipeline],
                               executionGraphDefinition: ExecutionGraphDefinition,
                               queryIndexes: Array[IndexReadSession],
                               nExpressionSlots: Int,
                               prePopulateResults: Boolean,
                               inputDataStream: InputDataStream,
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

    private var executingQuery: ExecutingQuery = _
    private var _queryProfile: QueryProfile = _
    private var _memoryTracker: QueryMemoryTracker = _
    private var _queryStatisticsTracker: MutableQueryStatistics = _

    override def queryStatistics(): QueryStatistics = _queryStatisticsTracker

    override def totalAllocatedMemory(): Long = {
      ensureQuerySubscription()
      _memoryTracker.totalAllocatedMemory
    }

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (executingQuery == null) ConsumptionState.NOT_STARTED
      else if (!executingQuery.hasSucceeded) ConsumptionState.HAS_MORE // Not exactly true, but EXHAUSTED would be more wrong.
      else ConsumptionState.EXHAUSTED

    override def close(): Unit = {}

    override def queryProfile(): QueryProfile = _queryProfile

    override def request(numberOfRecords: Long): Unit = {
      ensureQuerySubscription()
      executingQuery.request(numberOfRecords)
    }

    override def cancel(): Unit = {
      ensureQuerySubscription()
      executingQuery.cancel()
    }

    override def await(): Boolean = {
      ensureQuerySubscription()
      executingQuery.await()
    }

    private def ensureQuerySubscription(): Unit = {
      if (executingQuery == null) {
        // Only call onResult on first call. Having this callback before execute()
        // ensure that we do not leave any inconsistent state around if onResult
        // throws an exception.
        subscriber.onResult(fieldNames.length)

        val ProfiledQuerySubscription(sub, prof, memTrack, queryStatisticsTracker) = queryExecutor.execute(
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

        executingQuery = sub
        _queryProfile = prof
        _memoryTracker = memTrack
        _queryStatisticsTracker = queryStatisticsTracker
      }
    }
  }

  /**
   * Fake pipe used for now to allow using NestedPipeSlottedExpression from Pipelined.
   */
  object NoPipe extends Pipe {
    override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] =
      throw new IllegalStateException("Cannot use NoPipe to create results")
    override def id: Id = Id.INVALID_ID
  }
}
