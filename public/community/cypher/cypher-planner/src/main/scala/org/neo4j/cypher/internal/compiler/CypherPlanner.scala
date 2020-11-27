/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler

import java.time.Clock

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.planPipeLine
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.prepareForCaching
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.systemPipeLine
import org.neo4j.cypher.internal.compiler.phases.CypherCompatibilityVersion
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.debug.DebugPrinter
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.values.virtual.MapValue

case class CypherPlanner[Context <: PlannerContext](monitors: Monitors,
                                                    metricsFactory: MetricsFactory,
                                                    config: CypherPlannerConfiguration,
                                                    updateStrategy: UpdateStrategy,
                                                    clock: Clock,
                                                    contextCreation: ContextCreator[Context]) {

  def normalizeQuery(state: BaseState, context: Context): BaseState = prepareForCaching.transform(state, context)

  def planPreparedQuery(state: BaseState, context: Context): LogicalPlanState = {
    val pipeLine = if(config.planSystemCommands)
      systemPipeLine
    else if (context.debugOptions.toStringEnabled)
      planPipeLine() andThen DebugPrinter
    else
      planPipeLine()

    pipeLine.transform(state, context)
  }

  def parseQuery(queryText: String,
                 rawQueryText: String,
                 notificationLogger: InternalNotificationLogger,
                 plannerNameText: String = IDPPlannerName.name,
                 debugOptions: CypherDebugOptions,
                 offset: Option[InputPosition],
                 tracer: CompilationPhaseTracer,
                 innerVariableNamer: InnerVariableNamer,
                 params: MapValue,
                 compatibilityMode: CypherCompatibilityVersion): BaseState = {

    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, offset, plannerName)
    //TODO: these nulls are a short cut
    val context = contextCreation.create(tracer,
                                         notificationLogger,
                                         planContext = null,
                                         rawQueryText,
                                         debugOptions,
                                         executionModel = null,
                                         offset,
                                         monitors,
                                         metricsFactory,
                                         null,
                                         config,
                                         updateStrategy,
                                         clock,
                                         logicalPlanIdGen = null,
                                         evaluator = null,
                                         innerVariableNamer = innerVariableNamer,
                                         params )
    CompilationPhases.parsing(ParsingConfig(
      context.innerVariableNamer,
      compatibilityMode,
      parameterTypeMapping = context.getParameterValueTypeMapping,
      useJavaCCParser = config.useJavaCCParser
    )).transform(startState, context)
  }

}

object CypherPlannerConfiguration {
  def fromCypherConfiguration(config: CypherConfiguration, cfg: Config, planSystemCommands: Boolean): CypherPlannerConfiguration =
    CypherPlannerConfiguration(
      queryCacheSize = config.queryCacheSize,
      statsDivergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(config.statsDivergenceCalculator),
      useErrorsOverWarnings = config.useErrorsOverWarnings,
      idpMaxTableSize = config.idpMaxTableSize,
      idpIterationDuration = config.idpIterationDuration,
      errorIfShortestPathFallbackUsedAtRuntime = config.errorIfShortestPathFallbackUsedAtRuntime,
      errorIfShortestPathHasCommonNodesAtRuntime = config.errorIfShortestPathHasCommonNodesAtRuntime,
      legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping,
      csvBufferSize = config.csvBufferSize,
      nonIndexedLabelWarningThreshold = cfg.get(GraphDatabaseInternalSettings.query_non_indexed_label_warning_threshold).longValue(),
      planSystemCommands = planSystemCommands,
      useJavaCCParser = config.useJavaCCParser,
      pipelinedBatchSizeSmall = config.pipelinedBatchSizeSmall,
      pipelinedBatchSizeBig = config.pipelinedBatchSizeBig,
    )
}

case class CypherPlannerConfiguration(queryCacheSize: Int,
                                      statsDivergenceCalculator: StatsDivergenceCalculator,
                                      useErrorsOverWarnings: Boolean,
                                      idpMaxTableSize: Int,
                                      idpIterationDuration: Long,
                                      errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                      errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                                      legacyCsvQuoteEscaping: Boolean,
                                      csvBufferSize: Int,
                                      nonIndexedLabelWarningThreshold: Long,
                                      planSystemCommands: Boolean,
                                      useJavaCCParser: Boolean,
                                      pipelinedBatchSizeSmall: Int,
                                      pipelinedBatchSizeBig: Int)
