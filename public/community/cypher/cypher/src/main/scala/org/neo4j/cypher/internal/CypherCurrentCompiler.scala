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
package org.neo4j.cypher.internal

import org.neo4j.cypher._
import org.neo4j.cypher.exceptionHandler.runSafely
import org.neo4j.cypher.internal.NotificationWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v4_0.ExceptionTranslatingQueryContext
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.plandescription.{InternalPlanDescription, PlanDescriptionBuilder}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.result.{ClosingExecutionResult, ExplainExecutionResult, StandardInternalExecutionResult, _}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.{ExecutableQuery => _, _}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.{InternalNotification, TaskCloser}
import org.neo4j.graphdb.Notification
import org.neo4j.kernel.api.query.{CompilerInfo, SchemaIndexUsage}
import org.neo4j.kernel.impl.query.{QueryExecution, QueryExecutionMonitor, QuerySubscriber, TransactionalContext}
import org.neo4j.monitoring.Monitors
import org.neo4j.values.storable.{NoValue, TextValue}
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

/**
  * Composite [[Compiler]], which uses a [[CypherPlanner]] and [[CypherRuntime]] to compile
  * a preparsed query into a [[ExecutableQuery]].
  *
  * @param planner the planner
  * @param runtime the runtime
  * @param contextManager the runtime context manager
  * @param kernelMonitors monitors support
  * @tparam CONTEXT type of runtime context used
  */
case class CypherCurrentCompiler[CONTEXT <: RuntimeContext](planner: CypherPlanner,
                                                            runtime: CypherRuntime[CONTEXT],
                                                            contextManager: RuntimeContextManager[CONTEXT],
                                                            kernelMonitors: Monitors
                                                           ) extends org.neo4j.cypher.internal.Compiler {

  /**
    * Compile [[PreParsedQuery]] into [[ExecutableQuery]].
    *
    * @param preParsedQuery          pre-parsed query to convert
    * @param tracer                  compilation tracer to which events of the compilation process are reported
    * @param preParsingNotifications notifications from pre-parsing
    * @param transactionalContext    transactional context to use during compilation (in logical and physical planning)
    * @throws CypherException public cypher exceptions on compilation problems
    * @return a compiled and executable query
    */
  override def compile(preParsedQuery: PreParsedQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[Notification],
                       transactionalContext: TransactionalContext,
                       params: MapValue
                      ): ExecutableQuery = {

    def resolveParameterForManagementCommands(logicalPlan: LogicalPlan): LogicalPlan = {
      logicalPlan match {
        case c@CreateUser(_, _, Some(paramPassword), _, _) =>
          val paramString = params.get(paramPassword.name) match {
            case param: TextValue => param.stringValue()
            case NoValue.NO_VALUE => throw new ParameterNotFoundException("Expected parameter(s): " + paramPassword.name)
            case param => throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + param.getTypeName)
          }
          CreateUser(c.userName, Some(paramString), None, c.requirePasswordChange, c.suspended)(new SequentialIdGen(c.id.x + 1))
        case a@AlterUser(_, _, Some(paramPassword), _, _) =>
          val paramString = params.get(paramPassword.name) match {
            case param: TextValue => param.stringValue()
            case NoValue.NO_VALUE => throw new ParameterNotFoundException("Expected parameter(s): " + paramPassword.name)
            case param => throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + param.getTypeName)
          }
          AlterUser(a.userName, Some(paramString), None, a.requirePasswordChange, a.suspended)(new SequentialIdGen(a.id.x + 1))
        case _ => // Not a management command that needs resolving, do nothing
          logicalPlan
      }
    }

    val logicalPlanResult =
      planner.parseAndPlan(preParsedQuery, tracer, transactionalContext, params, runtime)  // we only pass in the runtime to be able to support checking against the correct CommandManagementRuntime

    val planState = logicalPlanResult.logicalPlanState
    val logicalPlan: LogicalPlan = resolveParameterForManagementCommands(planState.logicalPlan)
    val queryType = getQueryType(planState)

    val runtimeContext = contextManager.create(logicalPlanResult.plannerContext.planContext,
                                               transactionalContext.kernelTransaction().schemaRead(),
                                               logicalPlanResult.plannerContext.clock,
                                               logicalPlanResult.plannerContext.debugOptions,
                                               preParsedQuery.useCompiledExpressions)

    val logicalQuery = LogicalQuery(logicalPlan,
                                    planState.queryText,
                                    queryType == READ_ONLY,
                                    planState.statement().returnColumns.toArray,
                                    planState.semanticTable(),
                                    planState.planningAttributes.cardinalities,
                                    planState.hasLoadCSV,
                                    planState.maybePeriodicCommit.flatMap(_.map(x => PeriodicCommitInfo(x.batchSize))))

    val currentUser = transactionalContext.securityContext().subject().username()
    val executionPlan: ExecutionPlan = runtime.compileToExecutable(logicalQuery, runtimeContext, currentUser)

    new CypherExecutableQuery(
      logicalPlan,
      logicalQuery.readOnly,
      logicalPlanResult.logicalPlanState.planningAttributes.cardinalities,
      logicalPlanResult.logicalPlanState.planningAttributes.providedOrders,
      executionPlan,
      preParsingNotifications,
      logicalPlanResult.notifications,
      logicalPlanResult.reusability,
      logicalPlanResult.paramNames,
      logicalPlanResult.extractedParams,
      buildCompilerInfo(logicalPlan, planState.plannerName, executionPlan.runtimeName),
      planState.plannerName,
      preParsedQuery.version,
      queryType,
      logicalPlanResult.shouldBeCached)
  }

  private def buildCompilerInfo(logicalPlan: LogicalPlan,
                                plannerName: PlannerName,
                                runtimeName: RuntimeName): CompilerInfo =

    new CompilerInfo(plannerName.name, runtimeName.name, logicalPlan.indexUsage.map {
      case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => new SchemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
      case SchemaIndexScanUsage(identifier, labelId, label, propertyKeys) => new SchemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
    }.asJava)

  private def getQueryType(planState: LogicalPlanState): InternalQueryType = {
    // check system and procedure runtimes first, because if this is true solveds will be empty
    runtime match {
      case m:ManagementCommandRuntime if m.isApplicableManagementCommand(planState) =>
          DBMS
      case _ =>
        val procedureOrSchema = ProcedureCallOrSchemaCommandRuntime.queryType(planState.logicalPlan)
        if (procedureOrSchema.isDefined)
          procedureOrSchema.get
        else if (planState.planningAttributes.solveds(planState.logicalPlan.id).readOnly)
          READ_ONLY
        else if (columnNames(planState.logicalPlan).isEmpty)
          WRITE
        else
          READ_WRITE
    }
  }

  private def columnNames(logicalPlan: LogicalPlan): Array[String] =
    logicalPlan match {
      case produceResult: ProduceResult => produceResult.columns.toArray

      case _ => Array()
    }

  protected class CypherExecutableQuery(logicalPlan: LogicalPlan,
                                        readOnly: Boolean,
                                        cardinalities: Cardinalities,
                                        providedOrders: ProvidedOrders,
                                        executionPlan: ExecutionPlan,
                                        preParsingNotifications: Set[Notification],
                                        planningNotifications: Set[InternalNotification],
                                        reusabilityState: ReusabilityState,
                                        override val paramNames: Seq[String],
                                        override val extractedParams: MapValue,
                                        override val compilerInfo: CompilerInfo,
                                        plannerName: PlannerName,
                                        cypherVersion: CypherVersion,
                                        queryType: InternalQueryType,
                                        override val shouldBeCached: Boolean) extends ExecutableQuery {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
    private val resourceMonitor = kernelMonitors.newMonitor(classOf[ResourceMonitor])
    private val planDescriptionBuilder =
      new PlanDescriptionBuilder(logicalPlan,
        plannerName,
        cypherVersion,
        readOnly,
        cardinalities,
        providedOrders,
        executionPlan.runtimeName,
        executionPlan.metadata)

    private def getQueryContext(transactionalContext: TransactionalContext, debugOptions: Set[String]) = {
      val txContextWrapper = TransactionalContextWrapper(transactionalContext, executionPlan.threadSafeCursorFactory(debugOptions).orNull)
      val ctx = new TransactionBoundQueryContext(txContextWrapper,
                                                 new ResourceManager(resourceMonitor)
                                               )(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    override def execute(transactionalContext: TransactionalContext,
                         preParsedQuery: PreParsedQuery,
                         params: MapValue, prePopulateResults: Boolean,
                         subscriber: QuerySubscriber): QueryExecution = {


      val taskCloser = new TaskCloser
      val queryContext = getQueryContext(transactionalContext, preParsedQuery.debugOptions)
      taskCloser.addTask(queryContext.transactionalContext.close)
      taskCloser.addTask(queryContext.resources.close)
      runSafely {
        innerExecute(transactionalContext, preParsedQuery, taskCloser, queryContext, params, prePopulateResults,
                     subscriber)
      }( e => {
        subscriber.onError(e)
        taskCloser.close(false)
        new FailedExecutionResult(columnNames(logicalPlan), queryType, subscriber)
      })
    }

    private def innerExecute(transactionalContext: TransactionalContext,
                      preParsedQuery: PreParsedQuery,
                      taskCloser: TaskCloser,
                      queryContext: QueryContext,
                      params: MapValue, prePopulateResults: Boolean,
                      subscriber: QuerySubscriber): InternalExecutionResult = {

      val innerExecutionMode = preParsedQuery.executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.normal => NormalMode
      }

      val inner = if (innerExecutionMode == ExplainMode) {
        taskCloser.close(success = true)
        val columns = columnNames(logicalPlan)

        val allNotifications =
          preParsingNotifications ++ (planningNotifications ++ executionPlan.notifications)
            .map(asKernelNotification(Some(preParsedQuery.offset)))
        new ExplainExecutionResult(columns,
                               planDescriptionBuilder.explain(),
                               queryType, allNotifications, subscriber)
      } else {

        val doProfile = innerExecutionMode == ProfileMode
        val runtimeResult = executionPlan.run(queryContext, doProfile, params, prePopulateResults, NoInput, subscriber)

        taskCloser.addTask(_ => runtimeResult.close())

        new StandardInternalExecutionResult(queryContext,
                                            executionPlan.runtimeName,
                                            runtimeResult,
                                            taskCloser,
                                            queryType,
                                            innerExecutionMode,
                                            planDescriptionBuilder,
                                            subscriber)
        }
      ClosingExecutionResult.wrapAndInitiate(
        transactionalContext.executingQuery(),
        inner,
        runSafely,
        kernelMonitors.newMonitor(classOf[QueryExecutionMonitor]),
        subscriber
      )
    }

    override def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState = reusabilityState

    override def planDescription(): InternalPlanDescription = planDescriptionBuilder.explain()
  }

}
