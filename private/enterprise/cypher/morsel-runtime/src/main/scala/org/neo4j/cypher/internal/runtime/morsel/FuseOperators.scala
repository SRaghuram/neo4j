/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.FuseOperators.FUSE_LIMIT
import org.neo4j.cypher.internal.runtime.morsel.operators.{OperatorTaskTemplate, SingleThreadedAllNodeScanTaskTemplate, _}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.v4_0.expressions.{ASTCachedProperty, ListLiteral}
import org.neo4j.cypher.internal.v4_0.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.v4_0.util._

class FuseOperators(operatorFactory: OperatorFactory,
                    fusingEnabled: Boolean,
                    tokenContext: TokenContext) {

  private val physicalPlan = operatorFactory.executionGraphDefinition.physicalPlan

  def compilePipeline(p: PipelineDefinition): ExecutablePipeline = {
    // Fused operators do not support Cached properties for now
    val cannotFuse = p.treeExists { case _:ASTCachedProperty => true }

    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, unhandledMiddlePlans, unhandledOutput) =
      if (fusingEnabled && !cannotFuse) fuseOperators(p.headPlan, p.middlePlans, p.outputDefinition)
      else (None, p.middlePlans, p.outputDefinition)

    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan, p.inputBuffer))
    val middleOperators = unhandledMiddlePlans.flatMap(operatorFactory.createMiddle).toArray
    ExecutablePipeline(p.id,
                       headOperator,
                       middleOperators,
                       p.serial,
                       physicalPlan.slotConfigurations(p.headPlan.id),
                       p.inputBuffer,
                       operatorFactory.createOutput(unhandledOutput))
  }

  private def fuseOperators(headPlan: LogicalPlan,
                            middlePlans: Seq[LogicalPlan],
                            output: OutputDefinition): (Option[Operator], Seq[LogicalPlan], OutputDefinition) = {

    val id = headPlan.id
    val slots = physicalPlan.slotConfigurations(id)
    val namer = new VariableNamer
    val expressionCompiler = new OperatorExpressionCompiler(slots, namer) // NOTE: We assume slots is the same within an entire pipeline
    def compile(astExpression: org.neo4j.cypher.internal.v4_0.expressions.Expression): () => IntermediateExpression =
      () => expressionCompiler.intermediateCompileExpression(astExpression).getOrElse(throw new CantCompileQueryException)
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

    val (innerTemplate, initFusedPlans, initUnhandledOutput) =
      output match {
        case ProduceResultOutput(p) =>
          innermostTemplate.shouldWriteToContext = false // No need to write if we have ProduceResult
          val template = new ProduceResultOperatorTaskTemplate(innermostTemplate, p.id, p.columns, slots)(expressionCompiler)
          (template, List(p), NoOutput)

        case unhandled =>
          (innermostTemplate, List.empty[LogicalPlan], unhandled)
      }

    val reversePlans = (headPlan +: middlePlans).reverse

    def cantHandle(acc: FusionPlan,
                   nextPlan: LogicalPlan) = {
      // We cannot handle this plan. Start over from scratch (discard any previously fused plans)
      innermostTemplate.shouldWriteToContext = true
      acc.copy(
        template = innermostTemplate,
        fusedPlans = List.empty,
        unhandledPlans = nextPlan :: acc.fusedPlans.filterNot(_.isInstanceOf[ProduceResult]) ::: acc.unhandledPlans,
        unhandledOutput = output)
    }

    val fusedPipeline =
      reversePlans.foldLeft(FusionPlan(innerTemplate, initFusedPlans, List.empty, initUnhandledOutput)) {
        case (acc, nextPlan) => nextPlan match {

          case plan@plans.AllNodesScan(nodeVariableName, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)
            val newTemplate =
              new SingleThreadedAllNodeScanTaskTemplate(acc.template,
                                                        plan.id,
                                                        innermostTemplate,
                                                        nodeVariableName,
                                                        slots.getLongOffsetFor(nodeVariableName),
                                                        argumentSize)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.NodeByLabelScan(node, label, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)
            val maybeToken = tokenContext.getOptLabelId(label.name)
            val newTemplate = new SingleThreadedLabelScanTaskTemplate(
              acc.template,
              plan.id,
              innermostTemplate,
              node,
              slots.getLongOffsetFor(node),
              label.name,
              maybeToken,
              argumentSize)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.NodeIndexSeek(node, label, properties, valueExpr, _,  _)  =>
            val argumentSize = physicalPlan.argumentSizes(id)

            valueExpr match {
              //MATCH (n:L) WHERE n.prop = 1337
              case SingleQueryExpression(expr) if operatorFactory.readOnly =>
                assert(properties.length == 1)
                val newTemplate = new SingleQueryExactNodeIndexSeekTaskTemplate(acc.template,
                                                                                plan.id,
                                                                                innermostTemplate,
                                                                                node,
                                                                                slots.getLongOffsetFor(node),
                                                                                SlottedIndexedProperty(node,
                                                                                                       properties.head,
                                                                                                       slots),
                                                                                compile(expr),
                                                                                operatorFactory.queryIndexes.registerQueryIndex(label, properties.head),
                                                                                argumentSize)(expressionCompiler)
                acc.copy(
                  template = newTemplate,
                  fusedPlans = nextPlan :: acc.fusedPlans)

              //MATCH (n:L) WHERE n.prop = 1337 OR n.prop = 42
              case ManyQueryExpression(expr) =>
                assert(properties.length == 1)
                val newTemplate = new ManyQueriesExactNodeIndexSeekTaskTemplate(acc.template,
                                                                                plan.id,
                                                                                innermostTemplate,
                                                                                node,
                                                                                slots.getLongOffsetFor(node),
                                                                                SlottedIndexedProperty(node,
                                                                                                       properties.head,
                                                                                                       slots),
                                                                                compile(expr),
                                                                                operatorFactory.queryIndexes.registerQueryIndex(label, properties.head),
                                                                                argumentSize)(expressionCompiler)
                acc.copy(
                  template = newTemplate,
                  fusedPlans = nextPlan :: acc.fusedPlans)


              case _ => cantHandle(acc, nextPlan)
            }

          case plan@plans.NodeByIdSeek(node, nodeIds, _) => {
            val newTemplate = nodeIds match {
              case SingleSeekableArg(expr) =>
                new SingleNodeByIdSeekTaskTemplate(acc.template,
                                                   plan.id,
                                                   innermostTemplate,
                                                   node,
                                                   slots.getLongOffsetFor(node),
                                                   expr, physicalPlan.argumentSizes(id))(expressionCompiler)

              case ManySeekableArgs(expr) => expr match {
                case coll: ListLiteral =>
                  ZeroOneOrMany(coll.expressions) match {
                    case Zero => OperatorTaskTemplate.empty(plan.id)
                    case One(value) => new SingleNodeByIdSeekTaskTemplate(acc.template,
                                                                          plan.id,
                                                                          innermostTemplate,
                                                                          node,
                                                                          slots.getLongOffsetFor(node),
                                                                          value,
                                                                          physicalPlan.argumentSizes(id))(expressionCompiler)
                    case Many(_) => new ManyNodeByIdsSeekTaskTemplate(acc.template,
                                                                      plan.id,
                                                                      innermostTemplate,
                                                                      node,
                                                                      slots.getLongOffsetFor(node),
                                                                      expr,
                                                                      physicalPlan.argumentSizes(id))(expressionCompiler)
                  }

                case _ => new ManyNodeByIdsSeekTaskTemplate(acc.template,
                                                            plan.id,
                                                            innermostTemplate,
                                                            node,
                                                            slots.getLongOffsetFor(node),
                                                            expr,
                                                            physicalPlan.argumentSizes(id))(expressionCompiler)
              }
            }

            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)
          }

          case plan@plans.DirectedRelationshipByIdSeek(relationship, relIds, from, to, _) => {
            val newTemplate = relIds match {
              case SingleSeekableArg(expr) =>
                new SingleDirectedRelationshipByIdSeekTaskTemplate(acc.template,
                                                   plan.id,
                                                   innermostTemplate,
                                                   slots.getLongOffsetFor(relationship),
                                                   slots.getLongOffsetFor(from),
                                                   slots.getLongOffsetFor(to),
                                                   expr, physicalPlan.argumentSizes(id))(expressionCompiler)

              case ManySeekableArgs(expr) => expr match {
                case coll: ListLiteral =>
                  ZeroOneOrMany(coll.expressions) match {
                    case Zero => OperatorTaskTemplate.empty(plan.id)
                    case One(value) => new SingleDirectedRelationshipByIdSeekTaskTemplate(acc.template,
                                                                                          plan.id,
                                                                                          innermostTemplate,
                                                                                          slots.getLongOffsetFor(
                                                                                            relationship),
                                                                                          slots.getLongOffsetFor(from),
                                                                                          slots.getLongOffsetFor(to),
                                                                                          value, physicalPlan
                                                                                            .argumentSizes(id))(
                      expressionCompiler)
                    case Many(_) =>
                      new ManyDirectedRelationshipByIdsSeekTaskTemplate(acc.template,
                                                                        plan.id,
                                                                        innermostTemplate,
                                                                        slots.getLongOffsetFor(relationship),
                                                                        slots.getLongOffsetFor(from),
                                                                        slots.getLongOffsetFor(to),
                                                                        expr, physicalPlan.argumentSizes(id))(
                        expressionCompiler)
                  }

                case _ =>
                  new ManyDirectedRelationshipByIdsSeekTaskTemplate(acc.template,
                                                                    plan.id,
                                                                    innermostTemplate,
                                                                    slots.getLongOffsetFor(relationship),
                                                                    slots.getLongOffsetFor(from),
                                                                    slots.getLongOffsetFor(to),
                                                                    expr, physicalPlan.argumentSizes(id))(expressionCompiler)
              }
            }

            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)
          }

          case plan@plans.Expand(_, fromName, dir, types, to, relName, ExpandAll) =>
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
                                                                plan.id,
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

          case plan@plans.Selection(predicate, _) =>
            acc.copy(
              template = new FilterOperatorTemplate(acc.template, plan.id, compile(predicate)),
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.Input(nodes, variables, nullable) =>
            val newTemplate = new InputOperatorTemplate(acc.template, plan.id, innermostTemplate, nodes.map(v => slots.getLongOffsetFor(v)),
                                                        variables.map(v => slots.getReferenceOffsetFor(v)), nullable)(expressionCompiler)
            acc.copy(template = newTemplate,
                     fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.UnwindCollection(_, variable, collection) =>
            val offset = slots.get(variable) match {
              case Some(RefSlot(idx, _, _)) => idx
              case Some(slot) =>
                throw new InternalException(s"$slot cannot be used for UNWIND")
              case None =>
                throw new InternalException("No slot found for UNWIND")
            }
            val newTemplate = new UnwindOperatorTaskTemplate(
              acc.template,
              plan.id,
              innermostTemplate,
              collection,
              offset)(expressionCompiler)

            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case _ =>
            cantHandle(acc, nextPlan)
        }
      }

    // Did we find any sequence of operators that we can fuse with the headPlan?
    if (fusedPipeline.fusedPlans.length < FUSE_LIMIT) {
      (None, middlePlans, output)
    } else {
      val workIdentity = WorkIdentity.fromFusedPlans(fusedPipeline.fusedPlans)
      val operatorTaskWithMorselTemplate = fusedPipeline.template.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]

      try {
        val taskFactory = ContinuableOperatorTaskWithMorselGenerator.compileOperator(operatorTaskWithMorselTemplate)
        (Some(new CompiledStreamingOperator(workIdentity, taskFactory)), fusedPipeline.unhandledPlans, fusedPipeline.unhandledOutput)
      } catch {
        case _: CantCompileQueryException =>
          (None, middlePlans, output)
      }
    }
  }
}

object FuseOperators {
  private val FUSE_LIMIT = 2
}

case class FusionPlan(template: OperatorTaskTemplate,
                      fusedPlans: List[LogicalPlan],
                      unhandledPlans: List[LogicalPlan] = List.empty,
                      unhandledOutput: OutputDefinition = NoOutput)
