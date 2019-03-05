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
package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility.v4_0.{WrappedMonitors => WrappedMonitorsv4_0}
import org.neo4j.cypher.internal.compiler.v4_0.{CypherPlanner => CypherPlannerv4_0, _}
import org.neo4j.cypher.internal.compiler.v4_0.phases.{LogicalPlanState, PlannerContext, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.idp._
import org.neo4j.cypher.internal.planner.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.{CypherPlannerOption, CypherUpdateStrategy}
import org.neo4j.helpers.collection.Pair
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

/**
  * Base planner.
  *
  * @tparam STATEMENT type of AST statement used by this planner.
  * @tparam PARSED_STATE type of the state which represents a parsed query.
  */
abstract class BasePlanner[STATEMENT <: AnyRef, PARSED_STATE <: AnyRef](
                                                config: CypherPlannerConfiguration,
                                                clock: Clock,
                                                kernelMonitors: KernelMonitors,
                                                log: Log,
                                                plannerOption: CypherPlannerOption,
                                                updateStrategy: CypherUpdateStrategy,
                                                txIdProvider: () => Long
                                               ) extends CachingPlanner[PARSED_STATE] {

  protected val logger: InfoLogger = new StringInfoLogger(log)
  protected val monitors: Monitors = WrappedMonitorsv4_0(kernelMonitors)

  protected val cacheTracer: CacheTracer[Pair[STATEMENT, ParameterTypeMap]] = monitors.newMonitor[CacheTracer[Pair[STATEMENT, ParameterTypeMap]]]("cypher4.0")

  override def parserCacheSize: Int = config.queryCacheSize

  protected val rewriterSequencer: String => RewriterStepSequencer = {
    import Assertion._
    import RewriterStepSequencer._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val contextCreator: PlannerContextCreator.type = PlannerContextCreator

  protected val plannerName: CostBasedPlannerName =
    plannerOption match {
      case CypherPlannerOption.default => CostBasedPlannerName.default
      case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerName
      case CypherPlannerOption.dp => DPPlannerName
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
    }

  protected val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  protected val planner: CypherPlannerv4_0[PlannerContext] =
    new CypherPlannerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
      maybeUpdateStrategy, contextCreator)

  protected def createQueryGraphSolver(): IDPQueryGraphSolver =
    plannerName match {
      case IDPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = config.idpMaxTableSize,
          iterationDurationLimit = config.idpIterationDuration
        )
        val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

      case DPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
    }

  protected val schemaStateKey: SchemaStateKey = SchemaStateKey.newKey()
  protected def checkForSchemaChanges(tcw: TransactionalContextWrapper): Unit =
    tcw.getOrCreateFromSchemaState(schemaStateKey, planCache.clear())

  protected val planCache: AstLogicalPlanCache[STATEMENT] =
    new AstLogicalPlanCache(config.queryCacheSize,
                            cacheTracer,
                            clock,
                            config.statsDivergenceCalculator,
                            txIdProvider)

  override def clearCaches(): Long = {
    Math.max(super.clearCaches(), planCache.clear())
  }

  protected def logStalePlanRemovalMonitor(log: InfoLogger): CacheTracer[STATEMENT] =
    new CacheTracer[STATEMENT] {
      override def queryCacheStale(key: STATEMENT, secondsSinceReplan: Int, metaData: String) {
        log.info(s"Discarded stale query from the query cache after $secondsSinceReplan seconds: $metaData")
      }

      override def queryCacheHit(queryKey: STATEMENT, metaData: String): Unit = {}
      override def queryCacheMiss(queryKey: STATEMENT, metaData: String): Unit = {}
      override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {}
      override def queryCacheRecompile(queryKey: STATEMENT, metaData: String): Unit = {}
    }

  protected def createReusabilityState(logicalPlanState: LogicalPlanState,
                                       planContext: PlanContext): ReusabilityState = {

    if (ProcedureCallOrSchemaCommandRuntime
      .logicalToExecutable
      .isDefinedAt(logicalPlanState.maybeLogicalPlan.get))
      FineToReuse
    else {
      val fp = PlanFingerprint.take(clock, planContext.txIdProvider, planContext.statistics)
      val fingerprint = new PlanFingerprintReference(fp)
      MaybeReusable(fingerprint)
    }
  }
}

trait CypherCacheFlushingMonitor {
  def cacheFlushDetected(sizeBeforeFlush: Long) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}
  def cacheMiss(key: T) {}
  def cacheDiscard(key: T, userKey: String, secondsSinceReplan: Int) {}
  def cacheRecompile(key: T) {}
}

trait CypherCacheMonitor[T] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor
trait AstCacheMonitor[STATEMENT <: AnyRef] extends CypherCacheMonitor[STATEMENT]

trait InfoLogger {
  def info(message: String)
}

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}
