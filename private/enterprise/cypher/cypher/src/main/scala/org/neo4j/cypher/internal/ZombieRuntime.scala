/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.v4_0.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.physicalplanning.{PipelineBuilder, SlotAllocation, StateDefinition}
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext, QueryIndexes, QueryStatistics}
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.morsel.NO_TRANSACTION_BINDER
import org.neo4j.cypher.internal.runtime.scheduling.SchedulerTracer
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipelineBreakingPolicy
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue

object ZombieRuntime {
  def compileToExecutable(logicalQuery: LogicalQuery,
                          context: EnterpriseRuntimeContext,
                          logicalPlan: LogicalPlan,
                          physicalPlan: SlotAllocation.PhysicalPlan,
                          converters: ExpressionConverters,
                          queryIndexes: QueryIndexes): ExecutionPlan = {

    val stateDefinition = new StateDefinition
    val pipelineBuilder = new PipelineBuilder(SlottedPipelineBreakingPolicy, stateDefinition)

    pipelineBuilder.create(logicalPlan)
    val operatorFactory = new OperatorFactory(physicalPlan, converters, true, queryIndexes)

    val executablePipelines =
      for (p <- pipelineBuilder.pipelines) yield {
        val headOperator = operatorFactory.create(p.headPlan)
        val produceResultOperator = p.produceResults.map(operatorFactory.createProduceResults)

        ExecutablePipeline(p.id,
                           headOperator,
                           produceResultOperator,
                           physicalPlan.slotConfigurations(p.headPlan.id),
                           p.lhsRows,
                           p.output)
      }

    ZombieExecutionPlan(executablePipelines,
                        stateDefinition,
                        queryIndexes,
                        logicalPlan,
                        logicalQuery.resultColumns,
                        new SingleThreadedQueryExecutor(NO_TRANSACTION_BINDER),
                        null)
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
                     inputDataStream: InputDataStream): RuntimeResult = {

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
                              schedulerTracer)
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
                            schedulerTracer: SchedulerTracer) extends RuntimeResult {

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
