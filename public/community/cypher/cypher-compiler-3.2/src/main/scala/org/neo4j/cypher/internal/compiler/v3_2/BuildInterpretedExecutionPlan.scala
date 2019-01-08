/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PipeInfo, _}
import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v3_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LogicalPlanIdentificationBuilder
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.profiler.Profiler
import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, PlanContext, QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.frontend.v3_2.{PeriodicCommitInOpenTransactionException, PlannerName}
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_2.phases.{InternalNotificationLogger, Phase}

object BuildInterpretedExecutionPlan extends Phase[CompilerContext, CompilationState, CompilationState] {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: CompilationState, context: CompilerContext): CompilationState = {
    val logicalPlan = from.logicalPlan
    val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors)
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable, from.plannerName)
    val pipeInfo = executionPlanBuilder.build(from.periodicCommit, logicalPlan, idMap)(pipeBuildContext, context.planContext)
    val PipeInfo(pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = from.statement.returnColumns
    val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, context.typeConverter, logicalPlan, idMap)
    val func = getExecutionPlanFunction(periodicCommitInfo, from.queryText, updating, resultBuilderFactory, context.notificationLogger)

    val execPlan:ExecutionPlan = new InterpretedExecutionPlan(func,
                                                              logicalPlan,
                                                              pipe,
                                                              periodicCommitInfo.isDefined,
                                                              planner,
                                                              context.createFingerprintReference(fp),
                                                              context.config)

    from.copy(maybeExecutionPlan = Some(execPlan))
  }

  private def checkForNotifications(pipe: Pipe, planContext: PlanContext, config: CypherCompilerConfiguration): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_ (pipe))
  }

  private def getExecutionPlanFunction(periodicCommit: Option[PeriodicCommitInfo],
                                       queryId: AnyRef,
                                       updating: Boolean,
                                       resultBuilderFactory: ExecutionResultBuilderFactory,
                                       notificationLogger: InternalNotificationLogger):
  (QueryContext, ExecutionMode, Map[String, Any]) => InternalExecutionResult =
    (queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]) => {
      val builder = resultBuilderFactory.create()

      val profiling = planType == ProfileMode
      val builderContext = if (updating || profiling) new UpdateCountingQueryContext(queryContext) else queryContext

      builder.setQueryContext(builderContext)

      if (periodicCommit.isDefined) {
        if (!builderContext.transactionalContext.isTopLevelTx)
          throw new PeriodicCommitInOpenTransactionException()
        builder.setLoadCsvPeriodicCommitObserver(periodicCommit.get.batchRowCount)
      }

      if (profiling)
        builder.setPipeDecorator(new Profiler(queryContext.transactionalContext.databaseInfo))

      builder.build(planType, params, notificationLogger)
    }

  /**
    * Executable plan for a single cypher query. Warning, this class will get cached! Do not leak transaction objects
    * or other resources in here.
    */
  class InterpretedExecutionPlan(val executionPlanFunc: (QueryContext, ExecutionMode, Map[String, Any]) => InternalExecutionResult,
                                 val logicalPlan: LogicalPlan,
                                 val pipe: Pipe,
                                 override val isPeriodicCommit: Boolean,
                                 override val plannerUsed: PlannerName,
                                 val fingerprint: PlanFingerprintReference,
                                 val config: CypherCompilerConfiguration) extends ExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult =
      executionPlanFunc(queryContext, planType, params)

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics) = fingerprint.isStale(lastTxId, statistics)

    override def runtimeUsed = InterpretedRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] = checkForNotifications(pipe, planContext, config)

    override def plannedIndexUsage: Seq[IndexUsage] = logicalPlan.indexUsage
  }
}
