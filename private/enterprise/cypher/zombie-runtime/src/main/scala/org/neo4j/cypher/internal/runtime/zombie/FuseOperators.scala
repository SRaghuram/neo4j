/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.Pipeline
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.Pipeline.dprintln
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.FuseOperators.FUSE_LIMIT
import org.neo4j.cypher.internal.runtime.zombie.operators.{SingleThreadedAllNodeScanTaskTemplate, _}

class FuseOperators(operatorFactory: OperatorFactory,
                    fusingEnabled: Boolean,
                    tokenContext: TokenContext) {

  private val physicalPlan = operatorFactory.stateDefinition.physicalPlan

  def compilePipeline(p: Pipeline): ExecutablePipeline = {
    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, unhandledMiddlePlans, unhandledOutputPlan) =
      if (fusingEnabled) fuseOperators(p.headPlan, p.middlePlans, p.outputPlan)
      else (None, p.middlePlans, p.outputPlan)

    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan, p.inputBuffer))
    val middleOperators = unhandledMiddlePlans.flatMap(operatorFactory.createMiddle).toArray
    ExecutablePipeline(p.id,
                       headOperator,
                       middleOperators,
                       None,
                       p.outputBuffer.flatMap(_.workOnPut),
                       p.serial,
                       physicalPlan.slotConfigurations(p.headPlan.id),
                       p.inputBuffer,
                       operatorFactory.createOutput(unhandledOutputPlan, p.outputBuffer))
  }

  private def fuseOperators(headPlan: LogicalPlan, middlePlans: Seq[LogicalPlan], outputPlan: Option[LogicalPlan]): (Option[Operator], Seq[LogicalPlan], Option[LogicalPlan]) = {

    val id = headPlan.id
    val slots = physicalPlan.slotConfigurations(id)
    val namer = new VariableNamer
    val expressionCompiler = new OperatorExpressionCompiler(slots, namer) // NOTE: We assume slots is the same within an entire pipeline
    generateSlotAccessorFunctions(slots)

    // Fold plans in reverse to build-up code generation templates with inner templates
    // Then generate create the taskFactory from the headPlan template
    // Some stateless operators cannot be fused, e.g. SortPreOperator
    // Return these to be built as separate operators

    // E.g. HeadPlan, Seq(MiddlePlan1, MiddlePlan2)
    // MiddlePlan2 -> Template1(innermostTemplate)
    // MiddlePlan1 -> Template2(inner=Template1)
    // HeadPlan    -> Template3(inner=Template2)
    val innermostTemplate = new DelegateOperatorTaskTemplate()(expressionCompiler)

    val (innerTemplate, initFusedPlans, unhandledOutputPlan) =
      outputPlan match {
        case Some(p: ProduceResult) =>
          innermostTemplate.shouldWriteToContext = false // No need to write if we have ProduceResult
          val template = new ProduceResultOperatorTaskTemplate(innermostTemplate, p.columns, slots)(expressionCompiler)
          (template, List(p), None)

        case unhandled =>
          (innermostTemplate, List.empty[LogicalPlan], unhandled)
      }

    val reversePlans = (headPlan +: middlePlans).reverse

    val fusedPipeline =
      reversePlans.foldLeft(FusionPlan(innerTemplate, outputPlan.toList, List.empty, None)) {
        case (acc, nextPlan) => nextPlan match {

          case plans.AllNodesScan(nodeVariableName, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)
            val newTemplate =
              new SingleThreadedAllNodeScanTaskTemplate(acc.template,
                                                        innermostTemplate,
                                                        nodeVariableName,
                                                        slots.getLongOffsetFor(nodeVariableName),
                                                        argumentSize)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plans.Expand(_, fromName, dir, types, to, relName, ExpandAll) =>
            val fromOffset = slots.getLongOffsetFor(fromName)
            val relOffset = slots.getLongOffsetFor(relName)
            val toOffset = slots.getLongOffsetFor(to)
            val tokensOrNames = types.map(r => tokenContext.getOptRelTypeId(r.name) match {
                case Some(token) => Left(token)
                case None => Right(r.name)
              }
            )

            val typeTokens = tokensOrNames.collect {
              case Left(token: Int) => token
            }
            val missingTypes = tokensOrNames.collect {
              case Right(name: String) => name
            }
            val newTemplate = new ExpandAllOperatorTaskTemplate(acc.template,
                                                                innermostTemplate,
                                                                fromOffset,
                                                                relOffset,
                                                                toOffset,
                                                                dir,
                                                                typeTokens.toArray,
                                                                missingTypes.toArray)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plans.Selection(predicate, _) =>
            val compiledPredicate = () => expressionCompiler.intermediateCompileExpression(predicate).getOrElse(
              return (None, middlePlans, acc.unhandledOutputPlan)
            )
            acc.copy(
              template = new FilterOperatorTemplate(acc.template, compiledPredicate),
              fusedPlans = nextPlan :: acc.fusedPlans)

          case _ =>
            // We cannot handle this plan. Start over from scratch (discard any previously fused plans)
            acc.copy(
              template = innermostTemplate,
              fusedPlans = List.empty,
              unhandledPlans = nextPlan :: acc.fusedPlans.filterNot(_.isInstanceOf[ProduceResult]):::acc.unhandledPlans,
              unhandledOutputPlan = outputPlan)
        }
      }

    // Did we find any sequence of operators that we can fuse with the headPlan?
    if (fusedPipeline.fusedPlans.length < FUSE_LIMIT) {
      (None, middlePlans, outputPlan)
    } else {
      // Yes! Generate a class and an operator with a task factory that produces tasks based on the generated class
      dprintln(() => s"@@@ Fused plans ${fusedPipeline.fusedPlans.map(_.getClass.getSimpleName)}")

      val workIdentity = WorkIdentity.fromFusedPlans(fusedPipeline.fusedPlans)
      val operatorTaskWithMorselTemplate = fusedPipeline.template.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]

      val taskFactory = ContinuableOperatorTaskWithMorselGenerator.compileOperator(operatorTaskWithMorselTemplate)
      (Some(new CompiledStreamingOperator(workIdentity, taskFactory)), fusedPipeline.unhandledPlans, fusedPipeline.unhandledProduceResult)
    }
  }
}

object FuseOperators {
  private val FUSE_LIMIT = 2
}

case class FusionPlan(template: OperatorTaskTemplate,
                      fusedPlans: List[LogicalPlan],
                      unhandledPlans: List[LogicalPlan] = List.empty,
                      unhandledOutputPlan: Option[LogicalPlan] = None)
