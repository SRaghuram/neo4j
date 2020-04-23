/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.FusedHead
import org.neo4j.cypher.internal.physicalplanning.InterpretedHead
import org.neo4j.cypher.internal.physicalplanning.NoOutput
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition
import org.neo4j.cypher.internal.planner.spi.TokenContext

@deprecated // IntelliJ hook for devs having a hard time remembering that FuseOperators doesn't exist anymore.
object FuseOperators

class PipelineCompiler(operatorFactory: OperatorFactory,
                       tokenContext: TokenContext,
                       parallelExecution: Boolean,
                       codeGenerationMode: CodeGeneration.CodeGenerationMode) {

  private val physicalPlan = operatorFactory.executionGraphDefinition.physicalPlan

  def compilePipelines(executionGraphDefinition: ExecutionGraphDefinition): IndexedSeq[ExecutablePipeline] = {
    // Compile pipelines from the end backwards/upstream, to track needsFilteringMorsel
    // (the previous pipelines will need a filtering morsel if its downstream pipeline has a work canceller
    val (executablePipelines, _, _) =
      executionGraphDefinition.pipelines.foldRight (IndexedSeq.empty[ExecutablePipeline], false, true) {
        case (p, (pipelines, needsFilteringMorsel, nextPipelineCanTrackTime)) =>
          val (executablePipeline, upstreamNeedsFilteringMorsel) =
            compilePipeline(executionGraphDefinition, p, needsFilteringMorsel, nextPipelineCanTrackTime)

          (executablePipeline +: pipelines, needsFilteringMorsel || upstreamNeedsFilteringMorsel, executablePipeline.startOperatorCanTrackTime)
      }
    executablePipelines
  }

  private def interpretedOperatorRequiresThisPipelineToUseFilteringMorsel(plan: LogicalPlan): Boolean = plan match {
    case _: Distinct | _: OrderedDistinct => true // Distinct calls ArgumentStateMap.filter
    case _: Limit => true // Limit (if not fused) calls ArgumentStateMap.filter
    case _: Skip => true // Skip (if not fused) calls ArgumentStateMap.filter
    case _ => false
  }

  private def requiresUpstreamPipelinesToUseFilteringMorsel(plan: LogicalPlan): Boolean = plan match {
    case _: Limit => true // All upstreams from LIMIT need filtering morsels
    case _: Skip => true // All upstreams from SKIP need filtering morsels
    case _ => false
  }

  /**
   * @param needsFilteringMorsel there is a downstream pipeline that requires the pipeline to use filtering morsels
   * @return the compiled pipeline and whether upstream pipelines need to use filtering morsels
   */
  def compilePipeline(executionGraphDefinition: ExecutionGraphDefinition,
                      p: PipelineDefinition,
                      needsFilteringMorsel: Boolean,
                      nextPipelineCanTrackTime: Boolean): (ExecutablePipeline, Boolean) = {

    // See if we need filtering morsels in upstream pipelines
    val upstreamsNeedsFilteringMorsel =
      needsFilteringMorsel ||
      p.middlePlans.exists(requiresUpstreamPipelinesToUseFilteringMorsel) ||
      (p.headPlan match {
        case FusedHead(fuser) => fuser.fusedPlans.exists(requiresUpstreamPipelinesToUseFilteringMorsel)
        case _ => false
      })

    val headOperator =
      p.headPlan match {
        case f: FusedHead =>
          f.operatorFuser.asInstanceOf[TemplateOperatorFuser].compile(executionGraphDefinition)
        case InterpretedHead(plan) =>
          operatorFactory.create(plan, p.inputBuffer)
      }

    //For a fully fused pipeline that includes the output we don't need to allocate an output morsel
    val needsMorsel = p.outputDefinition != NoOutput

    // Check if there are any unhandled middle operators that causes this pipeline (or upstream pipelines in case it has a WorkCanceller)
    // require the use of FilteringMorselExecutionContext.
    val thisNeedsFilteringMorsel = needsFilteringMorsel || p.middlePlans.exists(interpretedOperatorRequiresThisPipelineToUseFilteringMorsel)

    val middleOperators = operatorFactory.createMiddleOperators(p.middlePlans, headOperator)
    (ExecutablePipeline(p.id,
                        p.lhs,
                        p.rhs,
                        headOperator,
                        middleOperators,
                        p.serial,
                        physicalPlan.slotConfigurations(p.headPlan.id),
                        p.inputBuffer,
                        operatorFactory.createOutput(p.outputDefinition, nextPipelineCanTrackTime),
                        p.workLimiter,
                        needsMorsel,
                        thisNeedsFilteringMorsel),
     upstreamsNeedsFilteringMorsel)
  }
}
