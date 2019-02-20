/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.v4_0.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.{PipelineBuilder, SlotAllocation, SlottedRewriter, StateDefinition}
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.morsel.NO_TRANSACTION_BINDER
import org.neo4j.cypher.internal.runtime.morsel.expressions.MorselExpressionConverters
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.runtime.zombie.state.SingleThreadedStateBuilder
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext, QueryIndexes, QueryStatistics}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{NaiveQuerySubscription, QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

object ZombieRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def name: String = "zombie"

  val ENABLED = false

  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext): ExecutionPlan = {
    val (logicalPlan, physicalPlan) = rewritePlan(context, query.logicalPlan, query.semanticTable)

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

    val stateDefinition = new StateDefinition(physicalPlan)
    val pipelineBuilder = new PipelineBuilder(ZombiePipelineBreakingPolicy, stateDefinition, physicalPlan.slotConfigurations)

    pipelineBuilder.build(logicalPlan)
    val operatorFactory = new OperatorFactory(physicalPlan, converters, true, queryIndexes)

    val executablePipelines =
      for (p <- pipelineBuilder.pipelines) yield {
        val headOperator = operatorFactory.create(p.headPlan)
        val middleOperators = p.middlePlans.flatMap(operatorFactory.createMiddle)
        val produceResultOperator = p.produceResults.map(operatorFactory.createProduceResults)

        ExecutablePipeline(p.id,
                           headOperator,
                           middleOperators,
                           produceResultOperator,
                           physicalPlan.slotConfigurations(p.headPlan.id),
                           p.lhsRowBuffer,
                           p.output)
      }

    ZombieExecutionPlan(executablePipelines,
                        stateDefinition,
                        queryIndexes,
                        logicalPlan,
                        query.resultColumns,
                        new SingleThreadedQueryExecutor(NO_TRANSACTION_BINDER),
                        null)
  }

  private def rewritePlan(context: EnterpriseRuntimeContext,
                          beforeRewrite: LogicalPlan,
                          semanticTable: SemanticTable) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable, ZombiePipelineBreakingPolicy, allocateArgumentSlots = true)
    val slottedRewriter = new SlottedRewriter(context.tokenContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan)
  }

  case class ZombieExecutionPlan(executablePipelines: IndexedSeq[ExecutablePipeline],
                                 stateDefinition: StateDefinition,
                                 queryIndexes: QueryIndexes,
                                 logicalPlan: LogicalPlan,
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
                              SingleThreadedStateBuilder.build(stateDefinition, executablePipelines),
                              queryIndexes.indexes.map(x => queryContext.transactionalContext.dataRead.indexReadSession(x)),
                              inputDataStream,
                              logicalPlan,
                              queryContext,
                              params,
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
                            state: ExecutionState,
                            queryIndexes: Array[IndexReadSession],
                            inputDataStream: InputDataStream,
                            logicalPlan: LogicalPlan,
                            queryContext: QueryContext,
                            params: MapValue,
                            override val fieldNames: Array[String],
                            queryExecutor: QueryExecutor,
                            schedulerTracer: SchedulerTracer,
                            subscriber: QuerySubscriber) extends NaiveQuerySubscription(subscriber) {

    private var resultRequested = false

    override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {
      queryExecutor.execute(executablePipelines, state, inputDataStream, queryContext, params, schedulerTracer, queryIndexes, visitor)
      resultRequested = true
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
  }
}
