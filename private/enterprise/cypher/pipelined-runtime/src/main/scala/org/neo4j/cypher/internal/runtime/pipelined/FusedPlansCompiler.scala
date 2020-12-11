/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.PipelineId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.compiled.expressions.VariableNamer
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperators.NewTemplate
import org.neo4j.cypher.internal.runtime.pipelined.TemplateOperators.TemplateContext
import org.neo4j.cypher.internal.runtime.pipelined.operators.BinaryOperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithMorselGenerator.compileOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DelegateOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

import scala.collection.mutable.ArrayBuffer

/**
 * Compiles templates into a single Operator.
 */
class FusedPlansCompiler(readOnly: Boolean,
                         doProfile: Boolean,
                         headPlanId: Id,
                         inputSlotConfiguration: SlotConfiguration,
                         lenientCreateRelationship: Boolean,
                         fusedPlans: IndexedSeq[LogicalPlan],
                         templates: IndexedSeq[NewTemplate],
                        ) {

  private val namer = new VariableNamer

  def compile(executionGraphDefinition: ExecutionGraphDefinition,
              pipelineId: PipelineId,
              tokenContext: TokenContext,
              indexRegistrator: QueryIndexRegistrator,
              codeGenerationMode: CodeGeneration.CodeGenerationMode,
             ): Operator = {
    val physicalPlan = executionGraphDefinition.physicalPlan
    val slots = physicalPlan.slotConfigurations(headPlanId)

    val expressionCompiler =
      fusedPlans.head match {
        case plan@(_: Union | _: ConditionalApply | _: AntiConditionalApply | _: SelectOrSemiApply | _: SelectOrAntiSemiApply) =>
          val leftSlots = physicalPlan.slotConfigurations(plan.lhs.get.id)
          val rightSlots = physicalPlan.slotConfigurations(plan.rhs.get.id)
          new BinaryOperatorExpressionCompiler(slots, inputSlotConfiguration, leftSlots, rightSlots, readOnly, namer)
        case _ =>
          new OperatorExpressionCompiler(slots, inputSlotConfiguration, readOnly, namer) // NOTE: We assume slots is the same within an entire pipeline
      }

    val innermost = new DelegateOperatorTaskTemplate()(expressionCompiler)
    var currentTemplate: OperatorTaskTemplate = innermost

    var argumentStates = new ArrayBuffer[ArgumentStateDescriptor]

    for (fixTemplate <- templates.reverse) {
      val ctx = TemplateContext(slots,
        physicalPlan.slotConfigurations,
        tokenContext,
        indexRegistrator,
        physicalPlan.argumentSizes,
        executionGraphDefinition,
        currentTemplate,
        innermost,
        expressionCompiler,
        lenientCreateRelationship)
      val x = fixTemplate(ctx)
      x.template.setProfile(doProfile)
      argumentStates ++= x.argumentStateFactory
      currentTemplate = x.template
    }
    val workIdentity = WorkIdentity.fromFusedPlans(fusedPlans)
    try {
      compileOperator(currentTemplate, workIdentity, argumentStates, codeGenerationMode, pipelineId)
    } catch {
      // In the case of a StackOverflowError we cannot recover correctly and abort fusing altogether.
      case e: StackOverflowError =>
        throw new CantCompileQueryException("Stack overflow caused operator compilation to fail", e)
    }
  }
}
