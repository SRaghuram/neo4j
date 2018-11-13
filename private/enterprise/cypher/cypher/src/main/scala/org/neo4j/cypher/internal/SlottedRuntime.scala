/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.CypherRuntime
import org.neo4j.cypher.internal.compatibility.InterpretedRuntime.InterpretedExecutionPlan
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v4_0.runtime._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.executionplan.{PeriodicCommitInfo, ExecutionPlan => ExecutionPlan_V35}
import org.neo4j.cypher.internal.compiler.v4_0.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.expressions.{CompiledExpressionConverter, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionResultBuilderFactory, SlottedPipeMapper}
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.util.CypherException

object SlottedRuntime extends CypherRuntime[EnterpriseRuntimeContext] with DebugPrettyPrinter {

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
  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlan_V35 = {
    try {
      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printPlanInfo(state)
      }

      val (logicalPlan, physicalPlan) = rewritePlan(context, state.logicalPlan, state.semanticTable())

      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printRewrittenPlanInfo(logicalPlan)
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

      val queryIndexes = new QueryIndexes(context.schemaRead)
      val fallback = InterpretedPipeMapper(context.readOnly, converters, context.tokenContext, queryIndexes)(state.semanticTable)
      val pipeBuilder = new SlottedPipeMapper(fallback, converters, physicalPlan, context.readOnly, queryIndexes)(state.semanticTable, context.tokenContext)
      val pipeTreeBuilder = PipeTreeBuilder(pipeBuilder)
      val logicalPlanWithConvertedNestedPlans = NestedPipeExpressions.build(pipeTreeBuilder, logicalPlan)
      val pipe = pipeTreeBuilder.build(logicalPlanWithConvertedNestedPlans)
      val periodicCommitInfo = state.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
      val columns = state.statement().returnColumns
      val resultBuilderFactory =
        new SlottedExecutionResultBuilderFactory(pipe,
                                                 queryIndexes,
                                                 context.readOnly,
                                                 columns,
                                                 logicalPlan,
                                                 physicalPlan.slotConfigurations,
                                                 context.config.lenientCreateRelationship)

      if (ENABLE_DEBUG_PRINTS) {
        if (!PRINT_PLAN_INFO_EARLY) {
          // Print after execution plan building to see any occurring exceptions first
          printPlanInfo(state)
          printRewrittenPlanInfo(logicalPlan)
        }
        printPipe(physicalPlan.slotConfigurations, pipe)
      }

      new InterpretedExecutionPlan(
        periodicCommitInfo,
        resultBuilderFactory,
        SlottedRuntimeName,
        context.readOnly)
    }
    catch {
      case e: CypherException =>
        if (ENABLE_DEBUG_PRINTS) {
          printFailureStackTrace(e)
          if (!PRINT_PLAN_INFO_EARLY) {
            printPlanInfo(state)
          }
        }
        throw e
    }
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, PhysicalPlan) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable)
    val slottedRewriter = new SlottedRewriter(context.tokenContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan)
  }
}
