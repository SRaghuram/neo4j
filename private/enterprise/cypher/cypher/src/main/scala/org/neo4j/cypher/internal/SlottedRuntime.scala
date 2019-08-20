/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.CantCompileQueryException
import org.neo4j.cypher.internal.InterpretedRuntime.InterpretedExecutionPlan
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionResultBuilderFactory, SlottedPipeMapper, SlottedPipelineBreakingPolicy}
import org.neo4j.cypher.internal.v4_0.util.CypherException
import org.neo4j.internal.kernel.api.security.SecurityContext

object SlottedRuntime extends CypherRuntime[EnterpriseRuntimeContext] with DebugPrettyPrinter {
  override def name: String = "slotted"

  val ENABLE_DEBUG_PRINTS = false // NOTE: false toggles all debug prints off, overriding the individual settings below

  // Should we print query text and logical plan before we see any exceptions from execution plan building?
  // Setting this to true is useful if you want to see the query and logical plan while debugging a failure
  // Setting this to false is useful if you want to quickly spot the failure reason at the top of the output from tests
  val PRINT_PLAN_INFO_EARLY = true

  override val PRINT_QUERY_TEXT = true
  override val PRINT_LOGICAL_PLAN = true
  override val PRINT_REWRITTEN_LOGICAL_PLAN = true
  override val PRINT_PIPELINE_INFO = true
  override val PRINT_FAILURE_STACK_TRACE = true

  @throws[CantCompileQueryException]
  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext, securityContext: SecurityContext): ExecutionPlan = {
    try {
      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printPlanInfo(query)
      }

      val physicalPlan = PhysicalPlanner.plan(context.tokenContext,
                                              query.logicalPlan,
                                              query.semanticTable,
                                              SlottedPipelineBreakingPolicy,
                                              context.config.transactionMaxMemory)

      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printRewrittenPlanInfo(physicalPlan.logicalPlan)
      }

      val converters =
        if (context.compileExpressions) {
          new ExpressionConverters(
            new CompiledExpressionConverter(context.log, physicalPlan, context.tokenContext),
            SlottedExpressionConverters(physicalPlan),
            CommunityExpressionConverter(context.tokenContext))
        } else {
          new ExpressionConverters(
            SlottedExpressionConverters(physicalPlan),
            CommunityExpressionConverter(context.tokenContext))
        }

      val queryIndexRegistrator = new QueryIndexRegistrator(context.schemaRead)
      val fallback = InterpretedPipeMapper(query.readOnly, converters, context.tokenContext, queryIndexRegistrator)(query.semanticTable)
      val pipeBuilder = new SlottedPipeMapper(fallback, converters, physicalPlan, query.readOnly, queryIndexRegistrator)(query.semanticTable, context.tokenContext)
      val pipeTreeBuilder = PipeTreeBuilder(pipeBuilder)
      val logicalPlanWithConvertedNestedPlans = NestedPipeExpressions.build(pipeTreeBuilder, physicalPlan.logicalPlan, physicalPlan.availableExpressionVariables)
      val pipe = pipeTreeBuilder.build(logicalPlanWithConvertedNestedPlans)
      val columns = query.resultColumns
      val resultBuilderFactory =
        new SlottedExecutionResultBuilderFactory(pipe,
                                                 queryIndexRegistrator.result(),
                                                 physicalPlan.nExpressionSlots,
                                                 query.readOnly,
                                                 columns,
                                                 physicalPlan.logicalPlan,
                                                 physicalPlan.slotConfigurations,
                                                 physicalPlan.parameterMapping,
                                                 context.config.lenientCreateRelationship,
                                                 context.config.transactionMaxMemory,
                                                 query.hasLoadCSV)

      if (ENABLE_DEBUG_PRINTS) {
        if (!PRINT_PLAN_INFO_EARLY) {
          // Print after execution plan building to see any occurring exceptions first
          printPlanInfo(query)
          printRewrittenPlanInfo(physicalPlan.logicalPlan)
        }
        printPipe(physicalPlan.slotConfigurations, pipe)
      }

      new InterpretedExecutionPlan(
        query.periodicCommitInfo,
        resultBuilderFactory,
        SlottedRuntimeName,
        query.readOnly)
    }
    catch {
      case e: CypherException =>
        if (ENABLE_DEBUG_PRINTS) {
          printFailureStackTrace(e)
          if (!PRINT_PLAN_INFO_EARLY) {
            printPlanInfo(query)
          }
        }
        throw e
    }
  }
}
