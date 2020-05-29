/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRange
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorFactory.getExpandProperties
import org.neo4j.cypher.internal.runtime.pipelined.operators.ArgumentOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.BinaryOperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.CachePropertiesOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.CompositeNodeIndexSeekTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ConditionalOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.DelegateOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandIntoOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.FilterOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.InputOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyNodeByIdsSeekTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ManyQueriesNodeIndexSeekTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeCountFromCountStoreOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexScanTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexStringSearchScanTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.exactSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.existsSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.greaterThanSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.lessThanSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.manyExactSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.multipleGreaterThanSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.multipleLessThanSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.multipleRangeBetweenSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.pointDistanceSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.rangeBetweenSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.stringContainsScan
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.stringEndsWithScan
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.stringPrefixSeek
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalExpandAllOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalExpandIntoOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProjectOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.RelationshipByIdSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.RelationshipCountFromCountStoreOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SeekExpression
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialLimitOnRhsOfApplyOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctPrimitiveOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctSinglePrimitiveOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelLimitOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelLimitOperatorTaskTemplate.SerialLimitStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelSkipOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelSkipOperatorTaskTemplate.SerialTopLevelSkipStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleExactSeekQueryNodeIndexSeekTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleNodeByIdSeekTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleRangeSeekQueryNodeIndexSeekTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleThreadedAllNodeScanTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.SingleThreadedLabelScanTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.UnionOperatorTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.UnwindOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandOperatorTaskTemplate
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.DistinctAllPrimitive
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.DistinctWithReferences
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.findDistinctPhysicalOp
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.util.Many
import org.neo4j.cypher.internal.util.One
import org.neo4j.cypher.internal.util.Zero
import org.neo4j.cypher.internal.util.ZeroOneOrMany
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.schema.IndexOrder

abstract class TemplateOperators(readOnly: Boolean, parallelExecution: Boolean, fuseOverPipelines: Boolean) {

  case class TemplateAndArgumentStateFactory(template: OperatorTaskTemplate, argumentStateFactory: Option[(ArgumentStateMapId, ArgumentStateFactory[_ <: ArgumentState])])
  type NewTemplate = TemplateContext => TemplateAndArgumentStateFactory

  // NOTE: These implicits are used to reduce the boilerplate in the template match cases, which improves readability
  //       They allow for example omitting Some(), and omitting TemplateAndArgumentStateFactory(_, None).
  //       Their order might be important.
  protected implicit def injectMissingArgumentStateFactory(operator: OperatorTaskTemplate): TemplateAndArgumentStateFactory =
    TemplateAndArgumentStateFactory(operator, None)
  protected implicit def injectMissingOptionAndArgumentStateFactory(operator: TemplateContext => OperatorTaskTemplate): Option[NewTemplate] =
    Some((ctx: TemplateContext) => TemplateAndArgumentStateFactory(operator(ctx), None))
  protected implicit def injectMissingOption(operator: NewTemplate): Option[NewTemplate] = Some(operator)

  // NOTE: Ideally this flag would also be true for any serial pipelines in the parallel runtime, but
  //       since we make the call on whether to fuse or not as we walk the logical plan, we do not yet
  //       know whether the pipeline will be executed in serial (this is only known once we've seen the
  //       last plan currently (ProduceResults)).
  val serialExecutionOnly: Boolean = !parallelExecution

  case class TemplateContext(slots: SlotConfiguration,
                             slotConfigurations: SlotConfigurations,
                             tokenContext: TokenContext,
                             indexRegistrator: QueryIndexRegistrator,
                             argumentSizes: ArgumentSizes,
                             executionGraphDefinition: ExecutionGraphDefinition,
                             inner: OperatorTaskTemplate,
                             innermost: DelegateOperatorTaskTemplate,
                             expressionCompiler: OperatorExpressionCompiler) {

    def compileExpression(astExpression: Expression): () => IntermediateExpression =
      () => expressionCompiler.compileExpression(astExpression)
        .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $astExpression"))
  }

  protected def createTemplate(plan: LogicalPlan,
                               isHeadOperator: Boolean,
                               hasNoNestedArguments: Boolean): Option[NewTemplate] = {

    val template: Option[NewTemplate] =
      plan match {
        case plan@plans.Argument(_) if isHeadOperator =>
          ctx: TemplateContext =>
            new ArgumentOperatorTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        case plan@plans.AllNodesScan(nodeVariableName, _) =>
          ctx: TemplateContext =>
            new SingleThreadedAllNodeScanTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              nodeVariableName,
              ctx.slots.getLongOffsetFor(nodeVariableName),
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        case plan@plans.NodeByLabelScan(node, label, _, indexOrder) =>
          ctx: TemplateContext =>
            val maybeToken = ctx.tokenContext.getOptLabelId(label.name)
            new SingleThreadedLabelScanTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              node,
              ctx.slots.getLongOffsetFor(node),
              label.name,
              maybeToken,
              ctx.argumentSizes(plan.id),
              asKernelIndexOrder(indexOrder))(ctx.expressionCompiler)

        case plan@plans.NodeIndexScan(node, label, properties, _, indexOrder) =>
          ctx: TemplateContext =>
            new NodeIndexScanTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              node,
              ctx.slots.getLongOffsetFor(node),
              properties.map(SlottedIndexedProperty(node, _, ctx.slots)).toArray,
              ctx.indexRegistrator.registerQueryIndex(label, properties),
              asKernelIndexOrder(indexOrder),
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        case plan@plans.NodeIndexContainsScan(node, label, property, seekExpression, _, indexOrder) =>
          ctx: TemplateContext =>
            new NodeIndexStringSearchScanTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              node,
              ctx.slots.getLongOffsetFor(node),
              SlottedIndexedProperty(node, property, ctx.slots),
              ctx.indexRegistrator.registerQueryIndex(label, property),
              asKernelIndexOrder(indexOrder),
              ctx.compileExpression(seekExpression),
              stringContainsScan,
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        case plan@plans.NodeIndexEndsWithScan(node, label, property, seekExpression, _, indexOrder) =>
          ctx: TemplateContext =>
            new NodeIndexStringSearchScanTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              node,
              ctx.slots.getLongOffsetFor(node),
              SlottedIndexedProperty(node, property, ctx.slots),
              ctx.indexRegistrator.registerQueryIndex(label, property),
              asKernelIndexOrder(indexOrder),
              ctx.compileExpression(seekExpression),
              stringEndsWithScan,
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        case plan@plans.NodeUniqueIndexSeek(node, label, properties, valueExpr, _, order) if readOnly =>
            indexSeek(node, label, properties, valueExpr, asKernelIndexOrder(order), unique = true, plan)

        case plan@plans.NodeIndexSeek(node, label, properties, valueExpr, _, order) =>
            indexSeek(node, label, properties, valueExpr, asKernelIndexOrder(order), unique = false, plan)

        case plan@plans.NodeByIdSeek(node, nodeIds, _) =>
          ctx: TemplateContext =>
            nodeIds match {
              case SingleSeekableArg(expr) =>
                new SingleNodeByIdSeekTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  node,
                  ctx.slots.getLongOffsetFor(node),
                  expr,
                  ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

              case ManySeekableArgs(expr) => expr match {
                case coll: ListLiteral =>
                  ZeroOneOrMany(coll.expressions) match {
                    case Zero => OperatorTaskTemplate.empty(plan.id)
                    case One(value) => new SingleNodeByIdSeekTaskTemplate(ctx.inner,
                      plan.id,
                      ctx.innermost,
                      node,
                      ctx.slots.getLongOffsetFor(node),
                      value,
                      ctx.argumentSizes(plan.id))(ctx.expressionCompiler)
                    case Many(_) => new ManyNodeByIdsSeekTaskTemplate(ctx.inner,
                      plan.id,
                      ctx.innermost,
                      node,
                      ctx.slots.getLongOffsetFor(node),
                      expr,
                      ctx.argumentSizes(plan.id))(ctx.expressionCompiler)
                  }

                case _ => new ManyNodeByIdsSeekTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  node,
                  ctx.slots.getLongOffsetFor(node),
                  expr,
                  ctx.argumentSizes(plan.id))(ctx.expressionCompiler)
              }
            }

        case plan@plans.DirectedRelationshipByIdSeek(relationship, relIds, from, to, _) =>
          ctx: TemplateContext =>
            RelationshipByIdSeekOperator.taskTemplate(isDirected = true,
              ctx.inner,
              plan.id,
              ctx.innermost,
              ctx.slots.getLongOffsetFor(relationship),
              ctx.slots.getLongOffsetFor(from),
              ctx.slots.getLongOffsetFor(to),
              relIds,
              ctx.argumentSizes(plan.id),
              ctx.expressionCompiler)

        case plan@plans.UndirectedRelationshipByIdSeek(relationship, relIds, from, to, _) =>
          ctx: TemplateContext =>
            RelationshipByIdSeekOperator.taskTemplate(isDirected = false,
              ctx.inner,
              plan.id,
              ctx.innermost,
              ctx.slots.getLongOffsetFor(relationship),
              ctx.slots.getLongOffsetFor(from),
              ctx.slots.getLongOffsetFor(to),
              relIds,
              ctx.argumentSizes(plan.id),
              ctx.expressionCompiler)

        case plan@plans.Expand(_, fromName, dir, types, to, relName, mode, expandProperties) if isHeadOperator || fuseOverPipelines =>
          ctx: TemplateContext =>
            val fromSlot = ctx.slots(fromName)
            val relOffset = ctx.slots.getLongOffsetFor(relName)
            val toSlot = ctx.slots(to)
            val tokensOrNames = types.map(r => ctx.tokenContext.getOptRelTypeId(r.name) match {
              case Some(token) => Left(token)
              case None => Right(r.name)})

            val typeTokens = tokensOrNames.collect {
              case Left(token: Int) => token
            }
            val missingTypes = tokensOrNames.collect {
              case Right(name: String) => name
            }
            val (nodePropsToCache, relPropsToCache) = getExpandProperties(ctx.slots, ctx.tokenContext, expandProperties)

            mode match {
              case ExpandAll =>
                new ExpandAllOperatorTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  isHeadOperator,
                  fromName,
                  fromSlot,
                  relName,
                  relOffset,
                  toSlot.offset,
                  dir,
                  typeTokens.toArray,
                  missingTypes.toArray,
                  nodePropsToCache,
                  relPropsToCache)(ctx.expressionCompiler)
              case ExpandInto =>
                new ExpandIntoOperatorTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  isHeadOperator,
                  fromSlot,
                  relName,
                  relOffset,
                  toSlot,
                  dir,
                  typeTokens.toArray,
                  missingTypes.toArray)(ctx.expressionCompiler)
            }

        case plan@plans.OptionalExpand(_, fromName, dir, types, to, relName, mode, maybePredicate, expandProperties) if isHeadOperator || fuseOverPipelines =>
          ctx: TemplateContext =>
            val fromSlot = ctx.slots(fromName)
            val relOffset = ctx.slots.getLongOffsetFor(relName)
            val toSlot = ctx.slots(to)
            val tokensOrNames = types.map(r => ctx.tokenContext.getOptRelTypeId(r.name) match {
              case Some(token) => Left(token)
              case None => Right(r.name)
            })

            val typeTokens = tokensOrNames.collect {
              case Left(token: Int) => token
            }
            val missingTypes = tokensOrNames.collect {
              case Right(name: String) => name
            }

            mode match {
              case ExpandAll =>
                val (nodePropsToCache, relPropsToCache) = getExpandProperties(ctx.slots, ctx.tokenContext, expandProperties)
                new OptionalExpandAllOperatorTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  isHeadOperator,
                  fromName,
                  fromSlot,
                  relName,
                  relOffset,
                  toSlot.offset,
                  dir,
                  typeTokens.toArray,
                  missingTypes.toArray,
                  maybePredicate.map(ctx.compileExpression),
                  nodePropsToCache,
                  relPropsToCache)(ctx.expressionCompiler)
              case ExpandInto =>
                new OptionalExpandIntoOperatorTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  isHeadOperator,
                  fromSlot,
                  relName,
                  relOffset,
                  toSlot,
                  dir,
                  typeTokens.toArray,
                  missingTypes.toArray,
                  maybePredicate.map(ctx.compileExpression))(ctx.expressionCompiler)
            }

        case plan@plans.VarExpand(_,
                                  fromName,
                                  dir,
                                  projectedDir,
                                  types,
                                  toName,
                                  relName,
                                  length,
                                  mode,
                                  nodePredicate,
                                  relationshipPredicate) if isHeadOperator || noPredicate(nodePredicate, relationshipPredicate) && fuseOverPipelines =>
          ctx: TemplateContext =>

            val fromSlot = ctx.slots(fromName)
            val relOffset = ctx.slots.getReferenceOffsetFor(relName)
            val toSlot = ctx.slots(toName)
            val tokensOrNames =
              types.map(
                r => ctx.tokenContext.getOptRelTypeId(r.name) match {
                  case Some(token) => Left(token)
                  case None => Right(r.name)
                })

            val typeTokens = tokensOrNames.collect {
              case Left(token: Int) => token
            }
            val missingTypes = tokensOrNames.collect {
              case Right(name: String) => name
            }
            val tempNodeOffset = expressionSlotForPredicate(nodePredicate)
            val tempRelationshipOffset = expressionSlotForPredicate(relationshipPredicate)
            new VarExpandOperatorTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              isHeadOperator,
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
              relationshipPredicate)(ctx.expressionCompiler)

        case plan@plans.Selection(predicate, _) =>
          ctx: TemplateContext =>
            new FilterOperatorTemplate(ctx.inner, plan.id, ctx.compileExpression(predicate))(ctx.expressionCompiler)

        case plan@plans.Projection(_, projections) =>
          ctx: TemplateContext =>
            new ProjectOperatorTemplate(ctx.inner, plan.id, projections)(ctx.expressionCompiler)

        case plan@plans.CacheProperties(_, properties) =>
          ctx: TemplateContext =>
            new CachePropertiesOperatorTemplate(ctx.inner, plan.id, properties.toSeq)(ctx.expressionCompiler)

        case plan@plans.Input(nodes, relationships, variables, nullable) =>
          ctx: TemplateContext =>
            new InputOperatorTemplate(ctx.inner, plan.id, ctx.innermost,
              nodes.map(v => ctx.slots.getLongOffsetFor(v)).toArray,
              relationships.map(v => ctx.slots.getLongOffsetFor(v)).toArray,
              variables.map(v => ctx.slots.getReferenceOffsetFor(v)).toArray, nullable)(ctx.expressionCompiler)

        case plan@plans.UnwindCollection(_, variable, collection) if isHeadOperator || fuseOverPipelines =>
          ctx: TemplateContext =>
            val offset = ctx.slots.get(variable) match {
              case Some(RefSlot(idx, _, _)) => idx
              case Some(slot) =>
                throw new InternalException(s"$slot cannot be used for UNWIND")
              case None =>
                throw new InternalException("No slot found for UNWIND")
            }
            new UnwindOperatorTaskTemplate(
              ctx.inner,
              plan.id,
              ctx.innermost,
              isHeadOperator,
              collection,
              offset)(ctx.expressionCompiler)

        case plan@plans.NodeCountFromCountStore(name, labels, _) =>
          ctx: TemplateContext =>
            val labelTokenOrNames = labels.map(_.map(labelName => ctx.tokenContext.getOptLabelId(labelName.name) match {
              case None => Left(labelName.name)
              case Some(token) => Right(token)
            }))
            new NodeCountFromCountStoreOperatorTemplate(
              ctx.inner,
              plan.id,
              ctx.innermost,
              ctx.slots.getReferenceOffsetFor(name),
              labelTokenOrNames,
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        case plan@plans.RelationshipCountFromCountStore(name, startLabel, typeNames, endLabel, _) =>
          ctx: TemplateContext =>
            val startNameOrToken = startLabel.map(l => ctx.tokenContext.getOptLabelId(l.name) match {
              case None => Left(l.name)
              case Some(token) => Right(token)
            })
            val endNameOrToken = endLabel.map(l => ctx.tokenContext.getOptLabelId(l.name) match {
              case None => Left(l.name)
              case Some(token) => Right(token)
            })
            val typeNamesOrTokens = typeNames.map(t => ctx.tokenContext.getOptRelTypeId(t.name) match {
              case None => Left(t.name)
              case Some(token) => Right(token)
            })

            new RelationshipCountFromCountStoreOperatorTemplate(
              ctx.inner,
              plan.id,
              ctx.innermost,
              ctx.slots.getReferenceOffsetFor(name),
              startNameOrToken,
              typeNamesOrTokens,
              endNameOrToken,
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

        // Special case for limit when not nested under an apply and with serial execution
        case plan@Distinct(_, grouping) if hasNoNestedArguments && serialExecutionOnly =>
          ctx: TemplateContext =>
            val physicalDistinctOp = findDistinctPhysicalOp(grouping, Seq.empty)

            val argumentStateMapId = ctx.executionGraphDefinition.findArgumentStateMapForPlan(plan.id)
            val groupMapping = SlottedExpressionConverters.orderGroupingKeyExpressions(grouping, Seq.empty)(ctx.slots).map {
              case (k, e, _) => ctx.slots(k) -> e
            }
            physicalDistinctOp match {
              case DistinctAllPrimitive(offsets, _) if offsets.size == 1 =>
                val (toSlot, expression) = groupMapping.head
                TemplateAndArgumentStateFactory(
                  new SerialTopLevelDistinctSinglePrimitiveOperatorTaskTemplate(ctx.inner,
                    plan.id,
                    argumentStateMapId,
                    toSlot,
                    offsets.head,
                    ctx.compileExpression(expression))(ctx.expressionCompiler),
                  Some(argumentStateMapId -> SerialTopLevelDistinctSinglePrimitiveOperatorTaskTemplate.DistinctStateFactory))
              case DistinctAllPrimitive(offsets, _) =>
                TemplateAndArgumentStateFactory(
                  new SerialTopLevelDistinctPrimitiveOperatorTaskTemplate(ctx.inner,
                    plan.id,
                    argumentStateMapId,
                    offsets.sorted.toArray,
                    groupMapping)(ctx.expressionCompiler),
                  Some(argumentStateMapId -> SerialTopLevelDistinctOperatorTaskTemplate.DistinctStateFactory))

              case DistinctWithReferences =>
                TemplateAndArgumentStateFactory(
                  new SerialTopLevelDistinctOperatorTaskTemplate(ctx.inner,
                    plan.id,
                    argumentStateMapId,
                    groupMapping)(ctx.expressionCompiler),
                  Some(argumentStateMapId -> SerialTopLevelDistinctOperatorTaskTemplate.DistinctStateFactory))
            }

        // Special case for limit with serial execution
        case plan@Limit(_, countExpression, DoNotIncludeTies) if serialExecutionOnly =>
          ctx: TemplateContext =>
            val argumentStateMapId = ctx.executionGraphDefinition.findArgumentStateMapForPlan(plan.id)
            if (hasNoNestedArguments) {
              ctx.innermost.shouldCheckBreak = true
              TemplateAndArgumentStateFactory(
                new SerialTopLevelLimitOperatorTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  argumentStateMapId,
                  ctx.compileExpression(countExpression))(ctx.expressionCompiler),
                Some(argumentStateMapId -> SerialLimitStateFactory)
              )
            } else {
              ctx.innermost.limits += argumentStateMapId
              TemplateAndArgumentStateFactory(
                new SerialLimitOnRhsOfApplyOperatorTaskTemplate(ctx.inner,
                  plan.id,
                  ctx.innermost,
                  argumentStateMapId,
                  ctx.compileExpression(countExpression))(ctx.expressionCompiler),
                Some(argumentStateMapId -> SerialLimitStateFactory)
              )
            }

        // Special case for skip when not nested under an apply and with serial execution
        case plan@Skip(_, countExpression) if hasNoNestedArguments && serialExecutionOnly =>
          ctx: TemplateContext =>
            val argumentStateMapId = ctx.executionGraphDefinition.findArgumentStateMapForPlan(plan.id)
            TemplateAndArgumentStateFactory(
              new SerialTopLevelSkipOperatorTaskTemplate(ctx.inner,
                plan.id,
                ctx.innermost,
                argumentStateMapId,
                ctx.compileExpression(countExpression))(ctx.expressionCompiler),
              Some(argumentStateMapId -> SerialTopLevelSkipStateFactory)
            )

        case plan@plans.Union(lhs ,rhs) if isHeadOperator =>
          ctx: TemplateContext =>
            val lhsSlots = ctx.slotConfigurations(lhs.id)
            val rhsSlots = ctx.slotConfigurations(rhs.id)
            val slots = ctx.slotConfigurations(plan.id)
            new UnionOperatorTemplate(ctx.inner,
                plan.id,
              ctx.innermost,
                lhsSlots,
                rhsSlots,
                SlottedPipeMapper.computeUnionSlotMappings(lhsSlots, slots),
                SlottedPipeMapper.computeUnionSlotMappings(rhsSlots, slots))(ctx.expressionCompiler.asInstanceOf[BinaryOperatorExpressionCompiler])

        case plan@(_: plans.ConditionalApply |
                   _: plans.AntiConditionalApply |
                   _: plans.SelectOrSemiApply |
                   _: plans.SelectOrAntiSemiApply ) if isHeadOperator =>
          ctx: TemplateContext =>
            val lhsSlots = ctx.slotConfigurations(plan.lhs.get.id)
            val rhsSlots = ctx.slotConfigurations(plan.rhs.get.id)
            new ConditionalOperatorTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              lhsSlots,
              rhsSlots
            )(ctx.expressionCompiler.asInstanceOf[BinaryOperatorExpressionCompiler])

        case _ =>
          None
      }

    template
  }

  private def noPredicate(nodePredicate: Option[VariablePredicate],
                          relationshipPredicate: Option[VariablePredicate]): Boolean = {
    !(nodePredicate.isDefined || relationshipPredicate.isDefined)
  }

  //We support multiple bounds such as a < 3 AND a < 5, this is quite esoteric and forces us to do stuff at runtime
  //hence we have specialized versions for the more common case of single bound ranges, i.e. a < 7, a >= 10, or 10 <= a < 12
  def computeInequalityRange(ctx: TemplateContext, inequality: InequalitySeekRange[Expression], property: IndexedProperty): SeekExpression = inequality match {
    case RangeLessThan(bounds) =>
      val (expressions, inclusive) = bounds.map(e => (ctx.compileExpression(e.endPoint), e.isInclusive)).toIndexedSeq.unzip
      val call = if (expressions.size == 1) {
        (in: Seq[IntermediateRepresentation]) => lessThanSeek(property.propertyKeyId, inclusive.head, in.head)
      } else {
        (in: Seq[IntermediateRepresentation]) => multipleLessThanSeek(property.propertyKeyId, in, inclusive)
      }SeekExpression(expressions, call, single = true)

    case RangeGreaterThan(bounds) =>
      val (expressions, inclusive) = bounds.map(e => (ctx.compileExpression(e.endPoint), e.isInclusive)).toIndexedSeq.unzip
      val call = if (expressions.size == 1) {
        (in: Seq[IntermediateRepresentation]) => greaterThanSeek(property.propertyKeyId, inclusive.head, in.head)
      } else {
        (in: Seq[IntermediateRepresentation]) => multipleGreaterThanSeek(property.propertyKeyId, in, inclusive)
      }SeekExpression(expressions, call, single = true)

    case RangeBetween(RangeGreaterThan(gtBounds), RangeLessThan(ltBounds)) =>
      val (gtExpressions, gtInclusive) = gtBounds.map(e => (ctx.compileExpression(e.endPoint), e.isInclusive)).toIndexedSeq.unzip
      val (ltExpressions, ltInclusive) = ltBounds.map(e => (ctx.compileExpression(e.endPoint), e.isInclusive)).toIndexedSeq.unzip

      val call = if (gtExpressions.size == 1 && ltExpressions.size == 1) {
        in: Seq[IntermediateRepresentation] =>
          rangeBetweenSeek(property.propertyKeyId, gtInclusive.head, in.head, ltInclusive.head, in.tail.head)
      } else {

        in: Seq[IntermediateRepresentation] =>
          multipleRangeBetweenSeek(property.propertyKeyId,
            gtInclusive,
            in.take(gtExpressions.size),
            ltInclusive,
            in.drop(gtExpressions.size))
      }
      SeekExpression(gtExpressions ++ ltExpressions, call, single = true)
  }

  def computeRangeExpression(rangeWrapper: Expression,
                             property: IndexedProperty): Option[TemplateContext => SeekExpression] =
    rangeWrapper match {
      case InequalitySeekRangeWrapper(inner) =>
        Some((ctx: TemplateContext) => computeInequalityRange(ctx, inner, property))

      case PrefixSeekRangeWrapper(range) =>
        Some((ctx: TemplateContext) =>
            SeekExpression(
              Seq(ctx.compileExpression(range.prefix)),
              in => stringPrefixSeek(property.propertyKeyId, in.head),
              single = true))

      case PointDistanceSeekRangeWrapper(range) =>
        Some((ctx: TemplateContext) =>
            SeekExpression(
              Seq(ctx.compileExpression(range.point), ctx.compileExpression(range.distance)),
              in => pointDistanceSeek(property.propertyKeyId, in.head, in.tail.head, range.inclusive)
            ))

      case _ => None
    }

  def computeCompositeQueries(query: QueryExpression[Expression], property: IndexedProperty): Option[TemplateContext => SeekExpression]  =
    query match {
      case SingleQueryExpression(inner) =>
        Some(
          (ctx: TemplateContext) =>
            SeekExpression(Seq(ctx.compileExpression(inner)),
                           in => arrayOf[IndexQuery](exactSeek(property.propertyKeyId, in.head))))

      case ManyQueryExpression(expr) =>
        Some(
          (ctx: TemplateContext) =>
            SeekExpression(Seq(ctx.compileExpression(expr)),
                           in => manyExactSeek(property.propertyKeyId, in.head)))

      case RangeQueryExpression(rangeWrapper) =>
        computeRangeExpression(rangeWrapper, property).map(
          maybeSeek => {
            ctx: TemplateContext => {
              maybeSeek(ctx) match {
                case seek if seek.single => seek.copy(generatePredicate = in => arrayOf[IndexQuery](seek.generatePredicate(in)))
                case seek => seek
              }
            }
          })

      case ExistenceQueryExpression() =>
        Some(
          (ctx: TemplateContext) =>
            SeekExpression(Seq.empty,
                           _ => arrayOf[IndexQuery](existsSeek(property.propertyKeyId))))

      case CompositeQueryExpression(_) =>
        throw new InternalException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

      case _ => None
    }

  private def indexSeek(node: String,
                        label: LabelToken,
                        properties: Seq[IndexedProperty],
                        valueExpr: QueryExpression[Expression],
                        order: IndexOrder,
                        unique: Boolean,
                        plan: LogicalPlan): Option[NewTemplate] = {
    val needsLockingUnique = !readOnly && unique
    valueExpr match {
      case SingleQueryExpression(expr) if !needsLockingUnique =>
        require(properties.length == 1)
        ctx: TemplateContext =>
          val slottedIndexedProperties = properties.map(SlottedIndexedProperty(node, _, ctx.slots))
          val property = slottedIndexedProperties.head
          new SingleExactSeekQueryNodeIndexSeekTaskTemplate(ctx.inner,
            plan.id,
            ctx.innermost,
            node,
            ctx.slots.getLongOffsetFor(node),
            property,
            ctx.compileExpression(expr),
            ctx.indexRegistrator.registerQueryIndex(label, properties),
            ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

      //MATCH (n:L) WHERE n.prop = 1337 OR n.prop = 42
      case ManyQueryExpression(expr) if !needsLockingUnique =>
        require(properties.length == 1)
        ctx: TemplateContext =>
          val slottedIndexedProperties = properties.map(SlottedIndexedProperty(node, _, ctx.slots))
          val property = slottedIndexedProperties.head
          new ManyQueriesNodeIndexSeekTaskTemplate(ctx.inner,
            plan.id,
            ctx.innermost,
            ctx.slots.getLongOffsetFor(node),
            property,
            SeekExpression(Seq(ctx.compileExpression(expr)), in => manyExactSeek(property.propertyKeyId, in.head)),
            ctx.indexRegistrator.registerQueryIndex(label, properties),
            order,
            ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

      case RangeQueryExpression(rangeWrapper) if !needsLockingUnique =>
        require(properties.length == 1)
        computeRangeExpression(rangeWrapper, properties.head).map(
          seek => {
            ctx: TemplateContext => {
              val slottedIndexedProperties = properties.map(SlottedIndexedProperty(node, _, ctx.slots))
              val property = slottedIndexedProperties.head
              seek(ctx) match {
                case seek if seek.single =>
                  new SingleRangeSeekQueryNodeIndexSeekTaskTemplate(ctx.inner,
                    plan.id,
                    ctx.innermost,
                    node,
                    ctx.slots.getLongOffsetFor(node),
                    property,
                    seek,
                    ctx.indexRegistrator.registerQueryIndex(label, properties),
                    order,
                    ctx.argumentSizes(plan.id))(ctx.expressionCompiler)

                case seek =>
                  new ManyQueriesNodeIndexSeekTaskTemplate(ctx.inner,
                    plan.id,
                    ctx.innermost,
                    ctx.slots.getLongOffsetFor(node),
                    property,
                    seek,
                    ctx.indexRegistrator.registerQueryIndex(label, properties),
                    order,
                    ctx.argumentSizes(plan.id))(ctx.expressionCompiler)
              }
            }
          }
        )

      case CompositeQueryExpression(parts) if !needsLockingUnique =>
        require(parts.lengthCompare(properties.length) == 0)
        val predicates = parts.zip(properties).flatMap {
          case (e, p) => computeCompositeQueries(e, p)
        }

        if (predicates.size != properties.length) {
          None
        } else {
          ctx: TemplateContext =>
            val slottedIndexedProperties = properties.map(SlottedIndexedProperty(node, _, ctx.slots))
            new CompositeNodeIndexSeekTaskTemplate(ctx.inner,
              plan.id,
              ctx.innermost,
              ctx.slots.getLongOffsetFor(node),
              slottedIndexedProperties,
              predicates.map(predicate => predicate(ctx)),
              ctx.indexRegistrator.registerQueryIndex(label, properties),
              order,
              ctx.argumentSizes(plan.id))(ctx.expressionCompiler)
        }

      case ExistenceQueryExpression() =>
        throw new InternalException("An ExistenceQueryExpression shouldn't be found outside of a CompositeQueryExpression")

      case _ => None

    }
  }
}
