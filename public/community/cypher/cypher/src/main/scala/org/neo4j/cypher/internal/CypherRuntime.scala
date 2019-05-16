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

import java.io.File
import java.time.Clock

import org.neo4j.cypher.internal.compiler.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.frontend.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.{CypherMorselRuntimeSchedulerOption, CypherRuntimeOption, RuntimeUnsupportedException, exceptionHandler}
import org.neo4j.internal.kernel.api.SchemaRead

import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * A cypher runtime. Compiles logical plans into a executable form, which can
  * be used directly to serve the query.
  */
trait CypherRuntime[-CONTEXT <: RuntimeContext] {

  /**
    * Compile a logical plan to an executable plan.
    *
    * WARNING: This code path is in the middle of a refactor and will be modified, changed and reworked.
    *
    * @param logicalQuery the logical query to compile
    * @param context the compilation context
    * @return the executable plan
    */
  def compileToExecutable(logicalQuery: LogicalQuery, context: CONTEXT): ExecutionPlan

  def name: String
}

/**
  * LogicalQuery contains all information about a planned query that is need for a runtime
  * to compile it to a ExecutionPlan.
  *
  * @param logicalPlan the logical plan
  * @param queryText the text representation of the query (only for debugging)
  * @param readOnly true if the query is read only
  * @param resultColumns names of the returned result columns
  * @param semanticTable semantic table with type information on the expressions in the query
  * @param cardinalities cardinalities (estimated rows) of all operators in the logical plan tree
  * @param hasLoadCSV a flag showing if the query contains a load csv, used for tracking line numbers
  * @param periodicCommitInfo periodic commit info if relevant
  */
case class LogicalQuery(logicalPlan: LogicalPlan,
                        queryText: String,
                        readOnly: Boolean,
                        resultColumns: Array[String],
                        semanticTable: SemanticTable,
                        cardinalities: Cardinalities,
                        hasLoadCSV: Boolean,
                        periodicCommitInfo: Option[PeriodicCommitInfo])

/**
  * Context in which the Runtime performs physical planning
  */
abstract class RuntimeContext {
  def tokenContext: TokenContext
  def schemaRead: SchemaRead
  def config: CypherRuntimeConfiguration
  def compileExpressions: Boolean
}

/**
  * Creator of runtime contexts.
  *
  * @tparam CONTEXT type of runtime context created
  */
trait RuntimeContextCreator[+CONTEXT <: RuntimeContext] {
  def create(tokenContext: TokenContext,
             schemaRead: SchemaRead,
             clock: Clock,
             debugOptions: Set[String],
             compileExpressions: Boolean
            ): CONTEXT
}

/**
  * Cypher runtime representing a user-selected runtime which is not supported.
  */
object UnknownRuntime extends CypherRuntime[RuntimeContext] {
  override def name: String = "unknown"

  override def compileToExecutable(logicalQuery: LogicalQuery, context: RuntimeContext): ExecutionPlan =
    throw new CantCompileQueryException()
}

/**
  * Composite cypher runtime, which attempts to compile using several different runtimes before giving up.
  *
  * In addition to attempting the provided runtimes, this runtime allways first attempt to compile using
  * `org.neo4j.cypher.internal.compatibility.ProcedureCallOrSchemaCommandRuntime`, in case the query
  * is a simple procedure call or schema command.
  *
  * @param runtimes the runtimes to attempt to compile with, in order of priority
  * @param requestedRuntime the requested runtime, used to provide error messages
  */
class FallbackRuntime[CONTEXT <: RuntimeContext](runtimes: Seq[CypherRuntime[CONTEXT]],
                                                 requestedRuntime: CypherRuntimeOption) extends CypherRuntime[CONTEXT] {

  override def name: String = "fallback"

  private def publicCannotCompile(originalException: Exception) = {
    val message = s"This version of Neo4j does not support requested runtime: ${requestedRuntime.name}"
    throw new RuntimeUnsupportedException(message, originalException)
  }

  override def compileToExecutable(logicalQuery: LogicalQuery, context: CONTEXT): ExecutionPlan = {
    var executionPlan: Try[ExecutionPlan] = Try(ProcedureCallOrSchemaCommandRuntime.compileToExecutable(logicalQuery, context))
    val logger = new RecordingNotificationLogger()
    for (runtime <- runtimes if executionPlan.isFailure) {
      executionPlan =
        Try(
          exceptionHandler.runSafely(
            runtime.compileToExecutable(logicalQuery, context)
          )
        )

      if (executionPlan.isFailure && requestedRuntime != CypherRuntimeOption.default)
        logger.log(RuntimeUnsupportedNotification)
    }
    val notifications = logger.notifications

    val plan = executionPlan.recover({
      case e: CantCompileQueryException => throw publicCannotCompile(e)
    }).get

    if (notifications.isEmpty) plan
    else ExecutionPlanWithNotifications(plan, notifications)
  }
}

case class CypherRuntimeConfiguration(workers: Int,
                                      scheduler: CypherMorselRuntimeSchedulerOption,
                                      morselSize: Int,
                                      schedulerTracing: SchedulerTracingConfiguration,
                                      waitTimeout: Duration,
                                      lenientCreateRelationship: Boolean,
                                      fuseOperators: Boolean)

sealed trait SchedulerTracingConfiguration
case object NoSchedulerTracing extends SchedulerTracingConfiguration
case object StdOutSchedulerTracing extends SchedulerTracingConfiguration
case class FileSchedulerTracing(file: File) extends SchedulerTracingConfiguration

case class ExecutionPlanWithNotifications(inner: ExecutionPlan, extraNotifications: Set[InternalNotification]) extends DelegatingExecutionPlan(inner) {

  override def notifications: Set[InternalNotification] = inner.notifications ++ extraNotifications
}
