/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.{PhysicalPlan, Pipeline}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.morsel.Pipeline.dprintln
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.FuseOperators.FUSE_LIMIT
import org.neo4j.cypher.internal.runtime.zombie.operators._

class FuseOperators(operatorFactory: OperatorFactory,
                    physicalPlan: PhysicalPlan,
                    converters: ExpressionConverters,
                    readOnly: Boolean,
                    queryIndexes: QueryIndexes,
                    fusingEnabled: Boolean,
                    tokenContext: TokenContext) {

  def compilePipeline(p: Pipeline): ExecutablePipeline = {
    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, unhandledMiddlePlans, unhandledProduceResult) =
      if (fusingEnabled) fuseOperators(p.headPlan, p.middlePlans, p.produceResults)
      else (None, p.middlePlans, p.produceResults)

    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan, p.inputBuffer))
    val middleOperators = unhandledMiddlePlans.flatMap(operatorFactory.createMiddle).toArray
    val produceResultOperator = unhandledProduceResult.map(operatorFactory.createProduceResults)
    ExecutablePipeline(p.id,
      headOperator,
      middleOperators,
      produceResultOperator,
      p.serial,
      physicalPlan.slotConfigurations(p.headPlan.id),
      p.inputBuffer,
      p.outputBuffer)
  }

  private def fuseOperators(headPlan: LogicalPlan, middlePlans: Seq[LogicalPlan], produceResult: Option[ProduceResult]): (Option[Operator], Seq[LogicalPlan], Option[ProduceResult]) = {

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

    val innerTemplate = produceResult.map(p => {
      innermostTemplate.shouldWriteToContext = false // No need to write if we have ProduceResult
      new ProduceResultOperatorTaskTemplate(innermostTemplate, p.columns, slots)(expressionCompiler)
    }).getOrElse(innermostTemplate)

    val reversePlans = middlePlans.foldLeft(List(headPlan))((acc, p) => p :: acc)

    //noinspection VariablePatternShadow
    val (operatorTaskTemplate, fusedPlans, unhandledPlans, unhandledProduceResult) =
      reversePlans.foldLeft(
        // Accumulator start values:
        // operatorTaskTemplate             , fusedPlans,                            , unhandledPlans         , unhandledProduceResult
        (innerTemplate: OperatorTaskTemplate, produceResult.toList: List[LogicalPlan], List.empty[LogicalPlan], None: Option[ProduceResult])
      ) {
        case ((innerTemplate, fusedPlans, unhandledPlans, unhandledProduceResult), p) => p match {

          case plans.AllNodesScan(nodeVariableName, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)
            (new SingleThreadedAllNodeScanTaskTemplate(innerTemplate, innermostTemplate, nodeVariableName, slots.getLongOffsetFor(nodeVariableName), argumentSize)(expressionCompiler),
              p :: fusedPlans, unhandledPlans, unhandledProduceResult)

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
            (new ExpandAllOperatorTaskTemplate(innerTemplate,
                                               innermostTemplate,
                                               fromOffset,
                                               relOffset,
                                               toOffset,
                                               dir,
                                               typeTokens.toArray,
                                               missingTypes.toArray)(expressionCompiler),
              p :: fusedPlans, unhandledPlans, unhandledProduceResult)

          case plans.Selection(predicate, _) =>
            val compiledPredicate = () => expressionCompiler.intermediateCompileExpression(predicate).getOrElse(
              return (None, middlePlans, unhandledProduceResult)
            )
            (new FilterOperatorTemplate(innerTemplate, compiledPredicate), p :: fusedPlans, unhandledPlans, unhandledProduceResult)

          case _ =>
            // We cannot handle this plan. Start over from scratch (discard any previously fused plans)
            (innermostTemplate, List.empty, p :: unhandledPlans, produceResult)
        }
      }

    // Did we find any sequence of operators that we can fuse with the headPlan?
    if (fusedPlans.length < FUSE_LIMIT) {
      (None, middlePlans, produceResult)
    } else {
      // Yes! Generate a class and an operator with a task factory that produces tasks based on the generated class
      dprintln(() => s"@@@ Fused plans ${fusedPlans.map(_.getClass.getSimpleName)}")

      val workIdentity = WorkIdentity.fromFusedPlans(fusedPlans)
      val operatorTaskWithMorselTemplate = operatorTaskTemplate.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]

      val taskFactory = ContinuableOperatorTaskWithMorselGenerator.compileOperator(operatorTaskWithMorselTemplate)
      (Some(new CompiledStreamingOperator(workIdentity, taskFactory)), unhandledPlans, unhandledProduceResult)
    }
  }
}

object FuseOperators {
  private val FUSE_LIMIT = 2
}
