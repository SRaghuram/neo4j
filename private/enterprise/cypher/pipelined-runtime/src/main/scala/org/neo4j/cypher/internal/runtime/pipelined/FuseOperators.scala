/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.codegen.api.{CodeGeneration, IntermediateRepresentation}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.physicalplanning.{OutputDefinition, RefSlot, _}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.pipelined.FuseOperators.FUSE_LIMIT
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.{Aggregator, AggregatorFactory}
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates._
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelLimitOperatorTaskTemplate.SerialTopLevelLimitStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.{Operator, OperatorTaskTemplate, SingleThreadedAllNodeScanTaskTemplate, _}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, LabelToken, ListLiteral}
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id.INVALID_ID
import org.neo4j.exceptions.{CantCompileQueryException, InternalException}
import org.neo4j.internal.schema.IndexOrder

class FuseOperators(operatorFactory: OperatorFactory,
                    tokenContext: TokenContext,
                    parallelExecution: Boolean,
                    codeGenerationMode: CodeGeneration.CodeGenerationMode) {

  private val physicalPlan = operatorFactory.executionGraphDefinition.physicalPlan
  private val aggregatorFactory = AggregatorFactory(physicalPlan)

  def compilePipelines(executionGraphDefinition: ExecutionGraphDefinition): IndexedSeq[ExecutablePipeline] = {
    // Compile pipelines from the end backwards/upstream, to track needsFilteringMorsel
    // (the previous pipelines will need a filtering morsel if its downstream pipeline has a work canceller
    val (executablePipelines, _) =
      executionGraphDefinition.pipelines.foldRight(IndexedSeq.empty[ExecutablePipeline], false) {
        case (p, (pipelines, needsFilteringMorsel)) =>
          val (executablePipeline, upstreamNeedsFilteringMorsel) =
            compilePipeline(p, needsFilteringMorsel)

          (executablePipeline +: pipelines, needsFilteringMorsel || upstreamNeedsFilteringMorsel)
      }
    executablePipelines
  }

  private def interpretedOperatorRequiresThisPipelineToUseFilteringMorsel(plan: LogicalPlan): Boolean = plan match {
    case _:Distinct => true // Distinct calls ArgumentStateMap.filter
    case _:Limit => true // Limit (if not fused) calls ArgumentStateMap.filter
    case _ => false
  }

  private def requiresUpstreamPipelinesToUseFilteringMorsel(plan: LogicalPlan): Boolean = plan match {
    case _:Limit => true // All upstreams from LIMIT need filtering morsels
    case _ => false
  }

  /**
   * @param needsFilteringMorsel there is a downstream pipeline that requires the pipeline to use filtering morsels
   * @return the compiled pipeline and whether upstream pipelines need to use filtering morsels
   */
  def compilePipeline(p: PipelineDefinition, needsFilteringMorsel: Boolean): (ExecutablePipeline, Boolean) = {

    // See if we need filtering morsels in upstream pipelines
    val upstreamsNeedsFilteringMorsel = p.middlePlans.exists(requiresUpstreamPipelinesToUseFilteringMorsel) ||
      p.fusedPlans.exists(requiresUpstreamPipelinesToUseFilteringMorsel) ||
      needsFilteringMorsel

    // First, try to fuse as many middle operators as possible into the head operator
    val (maybeHeadOperator, unhandledMiddlePlans, unhandledOutput) =
      if (p.fusedPlans.nonEmpty) fuseOperators(p)
      else (None, p.middlePlans, p.outputDefinition)

    //For a fully fused pipeline that includes ProduceResult or Aggregation we don't need to allocate an output morsel
    val needsMorsel = (p.outputDefinition, maybeHeadOperator, unhandledMiddlePlans) match {
      case (_:ProduceResultOutput, Some(_), Seq()) => false
      case (ReduceOutput(_,plans.Aggregation(_,_,_)), Some(_), Seq()) => false
      case _ => true
    }

    // Check if there are any unhandled middle operators that causes this pipeline (or upstream pipelines in case it has a WorkCanceller)
    // require the use of FilteringMorselExecutionContext.
    val thisNeedsFilteringMorsel = needsFilteringMorsel || unhandledMiddlePlans.exists(interpretedOperatorRequiresThisPipelineToUseFilteringMorsel)

    val headOperator = maybeHeadOperator.getOrElse(operatorFactory.create(p.headPlan, p.inputBuffer))
    val middleOperators = operatorFactory.createMiddleOperators(unhandledMiddlePlans, headOperator)
    (ExecutablePipeline(p.id,
                        p.lhs,
                        p.rhs,
                        headOperator,
                        middleOperators,
                        p.serial,
                        physicalPlan.slotConfigurations(p.headPlan.id),
                        p.inputBuffer,
                        operatorFactory.createOutput(unhandledOutput),
                        needsMorsel,
                        thisNeedsFilteringMorsel),
     upstreamsNeedsFilteringMorsel)
  }

  private def fuseOperators(pipeline: PipelineDefinition): (Option[Operator], Seq[LogicalPlan], OutputDefinition) = {
    val inputSlotConfiguration = pipeline.inputBuffer.bufferSlotConfiguration
    val headPlan = pipeline.headPlan
    val middlePlans = pipeline.middlePlans
    val output = pipeline.outputDefinition
    val id = headPlan.id
    val slots = physicalPlan.slotConfigurations(headPlan.id) // getSlots

    val namer = new VariableNamer
    val expressionCompiler = new OperatorExpressionCompiler(slots, inputSlotConfiguration, operatorFactory.readOnly, codeGenerationMode, namer) // NOTE: We assume slots is the same within an entire pipeline

    def compileExpression(astExpression: org.neo4j.cypher.internal.v4_0.expressions.Expression): () => IntermediateExpression =
      () => expressionCompiler.intermediateCompileExpression(astExpression)
                              .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $astExpression"))

    def compileGroupingKey(astExpressions: Map[String, Expression],
                           slots: SlotConfiguration,
                           orderToLeverage: Seq[Expression]): () => IntermediateExpression = {
      val orderedGroupingExpressions = orderGroupingKeyExpressions(astExpressions, orderToLeverage)(slots).map(_._2)
      () => expressionCompiler.intermediateCompileGroupingKey(orderedGroupingExpressions)
                              .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $astExpressions"))
    }

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

    val (innerTemplate, initFusedPlans, initUnhandledOutput) = {
      //if we have any middle plan it means we can't fuse all the way to the output operator
      if (middlePlans.nonEmpty) {
        (innermostTemplate, List.empty[LogicalPlan], output)
      } else {
        output match {
          case ProduceResultOutput(p) =>
            innermostTemplate.shouldWriteToContext = false // No need to write if we have ProduceResult
            innermostTemplate.shouldCheckDemand = true // The produce pipeline should follow subscription demand for reactive result support
          val template = new ProduceResultOperatorTaskTemplate(innermostTemplate, p.id, p.columns, slots)(
            expressionCompiler)
            (template, List(p), NoOutput)

          case ReduceOutput(bufferId, p@plans.Aggregation(_, groupingExpressions,
                                                          aggregationExpressionsMap)) =>
            innermostTemplate.shouldWriteToContext = false // No need to write if we have Aggregation
            innermostTemplate.shouldCheckDemand = false // No need to check subscription demand when not in final pipeline
            innermostTemplate.shouldCheckOutputCounter = true // Use a simple counter of number of outputs to bound the work unit execution
            val applyPlanId = physicalPlan.applyPlans(p.id)
            val argumentSlotOffset = slots.getArgumentLongOffsetFor(applyPlanId)

            // To order the elements inside the computed grouping key correctly we use their slot offsets in the downstream pipeline slot configuration
            val outputSlots = physicalPlan.slotConfigurations(p.id)

          val aggregators = Array.newBuilder[Aggregator]
          val aggregationExpressions = Array.newBuilder[Expression]
          aggregationExpressionsMap.foreach {
            case (_, astExpression) =>
              val (aggregator, innerAstExpression) = aggregatorFactory.newAggregator(astExpression)
              aggregators += aggregator
              aggregationExpressions += innerAstExpression
          }
          val aggregationAstExpressions: Array[Expression] = aggregationExpressions.result()
          val aggregationExpressionsCreator = () => aggregationAstExpressions.map(e => compileExpression(e)())
          val template = if (groupingExpressions.nonEmpty) {
            new AggregationMapperOperatorTaskTemplate(innermostTemplate,
                                                      p.id,
                                                      argumentSlotOffset,
                                                      aggregators.result(),
                                                      bufferId,
                                                      aggregationExpressionsCreator,
                                                        groupingKeyExpressionCreator = compileGroupingKey(groupingExpressions, outputSlots, orderToLeverage = Seq.empty),
                                                        aggregationAstExpressions)(expressionCompiler)
            } else {
            new AggregationMapperOperatorNoGroupingTaskTemplate(innermostTemplate,
                                                                p.id,
                                                                argumentSlotOffset,
                                                                aggregators.result(),
                                                                bufferId,
                                                                aggregationExpressionsCreator,
                                                                aggregationAstExpressions)(expressionCompiler)
          }
          (template, List(p), NoOutput)

        case unhandled =>
          (innermostTemplate, List.empty[LogicalPlan], unhandled)}
      }
    }

    val reversePlans = pipeline.fusedPlans.reverse

    def cantHandle(acc: FusionPlan,
                   nextPlan: LogicalPlan) = {
      val outputPlan = initFusedPlans match {
        case Seq(plan) => plan
        case _ => null
      }
      // We cannot handle this plan. Start over from scratch (discard any previously fused plans)
      innermostTemplate.reset()
      acc.copy(
        template = innermostTemplate,
        fusedPlans = List.empty,
        argumentStates = List.empty,
        unhandledPlans = nextPlan :: acc.fusedPlans.filterNot(_ eq outputPlan) ::: acc.unhandledPlans,
        unhandledOutput = output)
    }

    def indexSeek(node: String,
                  label: LabelToken,
                  properties: Seq[IndexedProperty],
                  valueExpr: QueryExpression[Expression],
                  order: IndexOrder,
                  unique: Boolean,
                  plan: LogicalPlan,
                  acc: FusionPlan): Option[InputLoopTaskTemplate] = {
      val needsLockingUnique = !operatorFactory.readOnly && unique
      val property = SlottedIndexedProperty(node, properties.head, slots)
      valueExpr match {
        case SingleQueryExpression(expr) if !needsLockingUnique =>
          assert(properties.length == 1)
          Some(new SingleExactSeekQueryNodeIndexSeekTaskTemplate(acc.template,
                                                        plan.id,
                                                        innermostTemplate,
                                                        slots.getLongOffsetFor(node),
                                                        property,
                                                        compileExpression(expr),
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
                                                             property,
                                                             compileExpression(expr),
                                                             operatorFactory.indexRegistrator.registerQueryIndex(label, properties),
                                                             physicalPlan.argumentSizes(id))(expressionCompiler))

        case RangeQueryExpression(rangeWrapper) if !needsLockingUnique=>
          assert(properties.length == 1)
          //NOTE: So far we only support fusing of single-bound inequalities. Not sure if it ever makes sense to have
          //multiple bounds
          (rangeWrapper match {
            case InequalitySeekRangeWrapper(RangeLessThan(Last(bound))) =>
              Some((Seq(compileExpression(bound.endPoint)), (in: Seq[IntermediateRepresentation]) => lessThanSeek(
                property.propertyKeyId, bound.isInclusive, in.head)))

            case InequalitySeekRangeWrapper(RangeGreaterThan(Last(bound))) =>
              Some((Seq(compileExpression(bound.endPoint)), (in: Seq[IntermediateRepresentation]) => greaterThanSeek(
                property.propertyKeyId, bound.isInclusive, in.head)))

            case InequalitySeekRangeWrapper(
            RangeBetween(RangeGreaterThan(Last(greaterThan)), RangeLessThan(Last(lessThan)))) =>
              Some((Seq(compileExpression(greaterThan.endPoint), compileExpression(lessThan.endPoint)),
                     (in: Seq[IntermediateRepresentation]) =>
                       rangeBetweenSeek(property.propertyKeyId, greaterThan.isInclusive, in.head, lessThan.isInclusive,
                                        in.tail.head)))
            case _ => None

          }).map {
            case (generateSeekValues, generatePredicate) =>
              new SingleRangeSeekQueryNodeIndexSeekTaskTemplate(acc.template,
                                                                plan.id,
                                                                innermostTemplate,
                                                                slots.getLongOffsetFor(node),
                                                                property,
                                                                generateSeekValues,
                                                                generatePredicate,
                                                                operatorFactory.indexRegistrator.registerQueryIndex(label, properties),
                                                                order,
                                                                physicalPlan.argumentSizes(id))(expressionCompiler)
          }

        case _ => None

      }
    }

    // These conditions can be checked to see if we can specialize some simple cases
    def hasNoNestedArguments(plan: LogicalPlan): Boolean = physicalPlan.applyPlans(plan.id) == INVALID_ID
    val serialExecutionOnly: Boolean = !parallelExecution || pipeline.serial

    val fusedPipeline =
      reversePlans.foldLeft(FusionPlan(innerTemplate, initFusedPlans, List.empty, middlePlans.toList, initUnhandledOutput)) {
        case (acc, nextPlan) => nextPlan match {

          case plan@plans.Argument(_) =>
            val newTemplate =
              new ArgumentOperatorTaskTemplate(acc.template,
                                               plan.id,
                                               innermostTemplate,
                                               physicalPlan.argumentSizes(id))(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

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

          case plan@plans.NodeIndexContainsScan(node, label, property, seekExpression, _, indexOrder) =>
            val newTemplate = new NodeIndexStringSearchScanTaskTemplate(acc.template,
                                                                        plan.id,
                                                                        innermostTemplate,
                                                                        slots.getLongOffsetFor(node),
                                                                        SlottedIndexedProperty(node, property, slots),
                                                                        operatorFactory.indexRegistrator.registerQueryIndex(label, property),
                                                                        asKernelIndexOrder(indexOrder),
                                                                        compileExpression(seekExpression),
                                                                        stringContainsScan,
                                                                        physicalPlan.argumentSizes(id))(expressionCompiler)

            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.NodeIndexEndsWithScan(node, label, property, seekExpression, _, indexOrder) =>
            val newTemplate = new NodeIndexStringSearchScanTaskTemplate(acc.template,
                                                                        plan.id,
                                                                        innermostTemplate,
                                                                        slots.getLongOffsetFor(node),
                                                                        SlottedIndexedProperty(node, property, slots),
                                                                        operatorFactory.indexRegistrator.registerQueryIndex(label, property),
                                                                        asKernelIndexOrder(indexOrder),
                                                                        compileExpression(seekExpression),
                                                                        stringEndsWithScan,
                                                                        physicalPlan.argumentSizes(id))(expressionCompiler)

            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.NodeUniqueIndexSeek(node, label, properties, valueExpr, _, order) if operatorFactory.readOnly =>
            indexSeek(node, label, properties, valueExpr, asKernelIndexOrder(order), unique = true, plan, acc) match {
              case Some(seek) =>
                acc.copy(template = seek, fusedPlans = nextPlan :: acc.fusedPlans)
              case None => cantHandle(acc, nextPlan)
            }

          case plan@plans.NodeIndexSeek(node, label, properties, valueExpr, _, order) =>
            indexSeek(node, label, properties, valueExpr, asKernelIndexOrder(order), unique = false, plan, acc) match {
              case Some(seek) =>
                acc.copy(template = seek, fusedPlans = nextPlan :: acc.fusedPlans)
              case None => cantHandle(acc, nextPlan)
            }

          case plan@plans.NodeByIdSeek(node, nodeIds, _) =>
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

          case plan@plans.DirectedRelationshipByIdSeek(relationship, relIds, from, to, _) =>
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

          case plan@plans.UndirectedRelationshipByIdSeek(relationship, relIds, from, to, _) =>
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

          case plan@plans.Expand(_, fromName, dir, types, to, relName, ExpandAll) =>
            val fromSlot = slots(fromName)
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
                                                                plan eq headPlan,
                                                                fromSlot,
                                                                relOffset,
                                                                toOffset,
                                                                dir,
                                                                typeTokens.toArray,
                                                                missingTypes.toArray)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case  plan@plans.VarExpand(_,
                               fromName,
                               dir,
                               projectedDir,
                               types,
                               toName,
                               relName,
                               length,
                               mode,
                               nodePredicate,
                               relationshipPredicate) =>

            val fromSlot = slots(fromName)
            val relOffset = slots.getReferenceOffsetFor(relName)
            val toSlot = slots(toName)
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
            val tempNodeOffset = expressionSlotForPredicate(nodePredicate)
            val tempRelationshipOffset = expressionSlotForPredicate(relationshipPredicate)
            val newTemplate = new VarExpandOperatorTaskTemplate(acc.template,
                                                                plan.id,
                                                                innermostTemplate,
                                                                plan eq headPlan,
                                                                fromSlot,
                                                                relOffset,
                                                                toSlot,
                                                                dir,
                                                                projectedDir,
                                                                typeTokens.toArray,
                                                                missingTypes.toArray,
                                                                length.min,
                                                                length.max.getOrElse(Int.MaxValue),
                                                                mode == ExpandAll,
                                                                tempNodeOffset,
                                                                tempRelationshipOffset,
                                                                nodePredicate,
                                                                relationshipPredicate)(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.Selection(predicate, _) =>
            acc.copy(
              template = new FilterOperatorTemplate(acc.template, plan.id, compileExpression(predicate))(expressionCompiler),
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.Projection(_, projections) =>
            acc.copy(
              template = new ProjectOperatorTemplate(acc.template, plan.id, projections)(expressionCompiler),
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.CacheProperties(_, properties) =>
            acc.copy(
              template = new CachePropertiesOperatorTemplate(acc.template, plan.id, properties.toSeq)(expressionCompiler),
              fusedPlans = nextPlan :: acc.fusedPlans)

          case plan@plans.Input(nodes, relationships, variables, nullable) =>
            val newTemplate = new InputOperatorTemplate(acc.template, plan.id, innermostTemplate,
                                                        nodes.map(v => slots.getLongOffsetFor(v)).toArray,
                                                        relationships.map(v => slots.getLongOffsetFor(v)).toArray,
                                                        variables.map(v => slots.getReferenceOffsetFor(v)).toArray, nullable)(expressionCompiler)
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
              plan eq headPlan,
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

          // Special case for limit when not nested under an apply and with serial execution
          case plan@Limit(_, countExpression, DoNotIncludeTies) if hasNoNestedArguments(plan) && serialExecutionOnly =>
            val argumentStateMapId = operatorFactory.executionGraphDefinition.findArgumentStateMapForPlan(plan.id)
            val newTemplate = new SerialTopLevelLimitOperatorTaskTemplate(acc.template,
                                                                          plan.id,
                                                                          innermostTemplate,
                                                                          argumentStateMapId,
                                                                          compileExpression(countExpression))(expressionCompiler)
            acc.copy(
              template = newTemplate,
              fusedPlans = nextPlan :: acc.fusedPlans,
              argumentStates = (argumentStateMapId, SerialTopLevelLimitStateFactory) :: acc.argumentStates
            )

          case _ =>
            cantHandle(acc, nextPlan)
        }
      }

    // Did we find any sequence of operators that we can fuse with the headPlan?
    //might have failed and should now be a middle plan
    if (fusedPipeline.fusedPlans.length < FUSE_LIMIT) {
      (None, pipeline.fusedPlans.tail ++ middlePlans, output)
    } else {
      val workIdentity = WorkIdentity.fromFusedPlans(fusedPipeline.fusedPlans)
      val operatorTaskWithMorselTemplate = fusedPipeline.template.asInstanceOf[ContinuableOperatorTaskWithMorselTemplate]

      try {
        val compiledOperator = ContinuableOperatorTaskWithMorselGenerator.compileOperator(operatorTaskWithMorselTemplate, workIdentity, fusedPipeline.argumentStates, codeGenerationMode)
        (Some(compiledOperator), fusedPipeline.unhandledPlans, fusedPipeline.unhandledOutput)
      } catch {
        case _: CantCompileQueryException =>
          (None, pipeline.fusedPlans.tail ++ middlePlans, output)
      }
    }
  }
}

object FuseOperators {
  private val FUSE_LIMIT = 2
}

final case class FusionPlan(template: OperatorTaskTemplate,
                            fusedPlans: List[LogicalPlan],
                            argumentStates: List[(ArgumentStateMapId, ArgumentStateFactory[_ <: ArgumentState])] = List.empty,
                            unhandledPlans: List[LogicalPlan] = List.empty,
                            unhandledOutput: OutputDefinition = NoOutput)
