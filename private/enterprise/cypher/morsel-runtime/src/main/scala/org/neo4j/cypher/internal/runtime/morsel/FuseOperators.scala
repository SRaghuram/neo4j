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
import org.neo4j.cypher.internal.physicalplanning.{OutputDefinition, RefSlot, _}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.FuseOperators.FUSE_LIMIT
import org.neo4j.cypher.internal.runtime.morsel.aggregators.{Aggregator, AggregatorFactory}
import org.neo4j.cypher.internal.runtime.morsel.operators.{Operator, OperatorTaskTemplate, SingleThreadedAllNodeScanTaskTemplate, _}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.v4_0.expressions.{ASTCachedProperty, Expression, LabelToken, ListLiteral}
import org.neo4j.cypher.internal.v4_0.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.v4_0.util._

class FuseOperators(operatorFactory: OperatorFactory,
                    fusingEnabled: Boolean,
                    tokenContext: TokenContext) {

  private val physicalPlan = operatorFactory.executionGraphDefinition.physicalPlan
  private val aggregatorFactory = AggregatorFactory(physicalPlan)

  def compilePipeline(p: PipelineDefinition): ExecutablePipeline = {
    // Fused operators do not support Cached properties for now
    val cannotFuse = p.treeExists { case _:ASTCachedProperty => true }

    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, unhandledMiddlePlans, unhandledOutput) =
      if (fusingEnabled && !cannotFuse) fuseOperators(p.headPlan, p.middlePlans, p.outputDefinition)
      else (None, p.middlePlans, p.outputDefinition)

    //For a fully fused pipeline that includes ProduceResult or Aggregation we don't need to allocate an output morsel
    val needsMorsel = (p.outputDefinition, maybeHeadOperator, unhandledMiddlePlans) match {
      case (_:ProduceResultOutput, Some(_), Seq()) => false
      case (ReduceOutput(_,plans.Aggregation(_,groupingExpressions,_)), Some(_), Seq()) if groupingExpressions.nonEmpty => false
      case _ => true
    }
    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan, p.inputBuffer))
    val middleOperators = unhandledMiddlePlans.flatMap(operatorFactory.createMiddle).toArray
    ExecutablePipeline(p.id,
                       headOperator,
                       middleOperators,
                       p.serial,
                       physicalPlan.slotConfigurations(p.headPlan.id),
                       p.inputBuffer,
                       operatorFactory.createOutput(unhandledOutput),
                       needsMorsel)
  }

  private def fuseOperators(headPlan: LogicalPlan,
                            middlePlans: Seq[LogicalPlan],
                            output: OutputDefinition): (Option[Operator], Seq[LogicalPlan], OutputDefinition) = {

    // TODO ===================================================================================================================================================
    //      obviously a filthy hack.
    //      for some reason the slots needed by aggregation are not available in start operator.
    //      this merges all pipeline operator slots configurations, rather than using start operator slot configuration.
    def getSlots: SlotConfiguration = {
      val slotsHead = physicalPlan.slotConfigurations(headPlan.id)
      output match {
        case ReduceOutput(_, p@Aggregation(_, groupingExpressions, _)) if groupingExpressions.nonEmpty =>
          val slotsAll = slotsHead.copy()
          val nonHeadPlanIds = middlePlans.map(_.id) :+ p.id
          nonHeadPlanIds.foreach(id => {
            val operatorSlots = physicalPlan.slotConfigurations(id)
            operatorSlots.foreachSlot[Unit](
              slotInfo => {
                slotsAll.get(slotInfo._1) match {
                  case None =>
                    slotsAll.add(slotInfo._1, slotInfo._2)
                  case _ =>
                }
              },
              cachedSlotInfo => {
                slotsAll.getCachedPropertySlot(cachedSlotInfo._1) match {
                  case None =>
                    throw new RuntimeException(s"Surprising! Cached property slot missing from later operator of same pipeline. Missing slot: $cachedSlotInfo")
                }
              })
          })
          slotsAll
        case _ =>
          slotsHead
      }
    }
    // TODO ===================================================================================================================================================

    val id = headPlan.id
    val slots = getSlots
    val namer = new VariableNamer
    val expressionCompiler = new OperatorExpressionCompiler(slots, namer) // NOTE: We assume slots are the same within an entire pipeline // TODO <-- not true
    def compile(astExpression: org.neo4j.cypher.internal.v4_0.expressions.Expression): () => IntermediateExpression =
      () => expressionCompiler.intermediateCompileExpression(astExpression).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $astExpression"))

    def compileGroupingKey(astExpressions: Map[String, org.neo4j.cypher.internal.v4_0.expressions.Expression]): () => IntermediateExpression =
      () => expressionCompiler.intermediateCompileGrouping(astExpressions)
                              .map(_.computeKey)
                              .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $astExpressions"))

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

        case reduceOutput@ReduceOutput(bufferId, p) =>
          p match {
            case plans.Aggregation(_, groupingExpressions, aggregationExpressionsMap) if groupingExpressions.nonEmpty =>

              innermostTemplate.shouldWriteToContext = false // No need to write if we have Aggregation
              innermostTemplate.shouldCheckDemand = false    // No need to check subscription demand when not in final pipeline
              val argumentDepth = physicalPlan.applyPlans(id)
              val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)

              val aggregators = Array.newBuilder[Aggregator]
              val aggregationExpressions = Array.newBuilder[Expression]
              aggregationExpressionsMap.foreach {
                case (key, astExpression) =>
                  val (aggregator, innerAstExpression) = aggregatorFactory.newAggregator(astExpression)
                  aggregators += aggregator
                  aggregationExpressions += innerAstExpression
              }

              /*
              TODO the grouping expression seems to be generated inline in the operator, but also a separate class is created and never used. why?
                   e.g.,
                   package org.neo4j.codegen;
                   /** Generated by org.neo4j.codegen.source.SourceCodeGenerator */
                   public class Expression0 extends java.lang.Object
                       implements org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledGroupingExpression
               */

              val aggregationAstExpressions: Array[Expression] = aggregationExpressions.result()
              val template = new AggregationMapperOperatorTaskTemplate(innermostTemplate,
                                                                       p.id,
                                                                       argumentSlotOffset,
                                                                       aggregators.result(),
                                                                       bufferId,
                                                                       aggregationExpressionsCreator = () => aggregationAstExpressions.map(e => compile(e)()),
                                                                       groupingKeyExpressionCreator = compileGroupingKey(groupingExpressions),
                                                                       aggregationAstExpressions)(expressionCompiler)
              (template, List(p), NoOutput)

            case _ =>
              (innermostTemplate, List.empty[LogicalPlan], reduceOutput)
          }
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
        // TODO what is happening here, shouldn't Aggregation be kept now too?
        unhandledPlans = nextPlan :: acc.fusedPlans.filterNot(_.isInstanceOf[ProduceResult]) ::: acc.unhandledPlans,
        unhandledOutput = output)
    }
    def indexSeek(node: String,
                  label: LabelToken,
                  properties: Seq[IndexedProperty],
                  valueExpr: QueryExpression[Expression],
                  unique: Boolean,
                  plan: LogicalPlan,
                  acc: FusionPlan): Option[InputLoopTaskTemplate] = {
      val needsLockingUnique = !operatorFactory.readOnly && unique
      valueExpr match {
        case SingleQueryExpression(expr) if !needsLockingUnique =>
          assert(properties.length == 1)
          Some(new SingleQueryExactNodeIndexSeekTaskTemplate(acc.template,
                                                             plan.id,
                                                             innermostTemplate,
                                                             node,
                                                             slots.getLongOffsetFor(node),
                                                             SlottedIndexedProperty(node,
                                                                                    properties.head,
                                                                                    slots),
                                                             compile(expr),
                                                             operatorFactory.indexRegistrator.registerQueryIndex(label, properties),
                                                             physicalPlan.argumentSizes(id))(expressionCompiler))


        //MATCH (n:L) WHERE n.prop = 1337 OR n.prop = 42
        case ManyQueryExpression(expr) if !needsLockingUnique =>
          assert(properties.length == 1)
          Some(new ManyQueriesExactNodeIndexSeekTaskTemplate(acc.template,
                                                             plan.id,
                                                             innermostTemplate,
                                                             node,
                                                             slots.getLongOffsetFor(node),
                                                             SlottedIndexedProperty(node,
                                                                                    properties.head,
                                                                                    slots),
                                                             compile(expr),
                                                             operatorFactory.indexRegistrator.registerQueryIndex(label, properties),
                                                             physicalPlan.argumentSizes(id))(expressionCompiler))

        case _ => None

      }
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

          case plan@plans.NodeIndexScan(node, label, properties, _, indexOrder) =>
            val newTemplate = new NodeIndexScanTaskTemplate(acc.template,
                                                            plan.id,
                                                            innermostTemplate,
                                                            slots.getLongOffsetFor(node),
                                                            properties.map(SlottedIndexedProperty(node, _, slots)).toArray,
                                                            operatorFactory.indexRegistrator.registerQueryIndex(label, properties),
                                                            asKernelIndexOrder(indexOrder),
                                                            physicalPlan.argumentSizes(id))(expressionCompiler)

            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.NodeUniqueIndexSeek(node, label, properties, valueExpr, _, _) if operatorFactory.readOnly =>
            indexSeek(node, label, properties, valueExpr, unique = true, plan, acc) match {
              case Some(seek) =>
                acc.copy(template = seek, fusedPlans = nextPlan :: acc.fusedPlans)
              case None => cantHandle(acc, nextPlan)
            }

          case plan@plans.NodeIndexSeek(node, label, properties, valueExpr, _, _) =>
            indexSeek(node, label, properties, valueExpr, unique = false, plan, acc) match {
              case Some(seek) =>
                acc.copy(template = seek, fusedPlans = nextPlan :: acc.fusedPlans)
              case None => cantHandle(acc, nextPlan)
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
            val newTemplate = RelationshipByIdSeekOperator.taskTemplate(isDirected = true,
                                                                        acc.template,
                                                                        plan.id,
                                                                        innermostTemplate,
                                                                        slots.getLongOffsetFor(relationship),
                                                                        slots.getLongOffsetFor(from),
                                                                        slots.getLongOffsetFor(to),
                                                                        relIds,
                                                                        physicalPlan.argumentSizes(id),
                                                                        expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)
          }

          case plan@plans.UndirectedRelationshipByIdSeek(relationship, relIds, from, to, _) => {
            val newTemplate = RelationshipByIdSeekOperator.taskTemplate(isDirected = false,
                                                                        acc.template,
                                                                        plan.id,
                                                                        innermostTemplate,
                                                                        slots.getLongOffsetFor(relationship),
                                                                        slots.getLongOffsetFor(from),
                                                                        slots.getLongOffsetFor(to),
                                                                        relIds,
                                                                        physicalPlan.argumentSizes(id),
                                                                        expressionCompiler)
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

          case plan@plans.Projection(_, projections) =>
            acc.copy(
              template = new ProjectOperatorTemplate(acc.template, plan.id, projections)(expressionCompiler),
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

          case plan@plans.NodeCountFromCountStore(name, labels, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)
            val labelTokenOrNames = labels.map(_.map(labelName => tokenContext.getOptLabelId(labelName.name) match {
              case None => Left(labelName.name)
              case Some(token) => Right(token)
            }))
            val newTemplate = new NodeCountFromCountStoreOperatorTemplate(
              acc.template,
              plan.id,
              innermostTemplate,
              slots.getReferenceOffsetFor(name),
              labelTokenOrNames,
              argumentSize)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.RelationshipCountFromCountStore(name, startLabel, typeNames, endLabel, _) =>
            val argumentSize = physicalPlan.argumentSizes(id)

            val startNameOrToken = startLabel.map(l => tokenContext.getOptLabelId(l.name) match {
              case None => Left(l.name)
              case Some(token) => Right(token)
            })
            val endNameOrToken = endLabel.map(l => tokenContext.getOptLabelId(l.name) match {
              case None => Left(l.name)
              case Some(token) => Right(token)
            })
            val typeNamesOrTokens = typeNames.map(t => tokenContext.getOptRelTypeId(t.name) match {
              case None => Left(t.name)
              case Some(token) => Right(token)
            })

            val newTemplate = new RelationshipCountFromCountStoreOperatorTemplate(
              acc.template,
              plan.id,
              innermostTemplate,
              slots.getReferenceOffsetFor(name),
              startNameOrToken,
              typeNamesOrTokens,
              endNameOrToken,
              argumentSize)(expressionCompiler)
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
