/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.ExpandCursorProperties
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateBufferVariant
import org.neo4j.cypher.internal.physicalplanning.ArgumentStreamBufferVariant
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.LHSAccumulatingRHSStreamingBufferVariant
import org.neo4j.cypher.internal.physicalplanning.MorselArgumentStateBufferOutput
import org.neo4j.cypher.internal.physicalplanning.MorselBufferOutput
import org.neo4j.cypher.internal.physicalplanning.NoOutput
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy.OPERATOR_FUSION_DISABLED
import org.neo4j.cypher.internal.physicalplanning.OutputDefinition
import org.neo4j.cypher.internal.physicalplanning.ProduceResultOutput
import org.neo4j.cypher.internal.physicalplanning.ReduceOutput
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.isRefSlotAndNotAlias
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DropResultPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekModeFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LockingUniqueIndexSeek
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.OperatorFactory.getExpandProperties
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AggregatorFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationOperatorNoGrouping
import org.neo4j.cypher.internal.runtime.pipelined.operators.AllNodeScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.AllOrderedAggregationOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.AllOrderedDistinctOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.AntiOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ArgumentOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.CachePropertiesOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.CartesianProductOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ConditionalApplyOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DirectedRelationshipByIdSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctPrimitiveOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandIntoOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.FilterOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.InputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.LabelScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.LimitOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.MiddleOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselArgumentStateBufferOutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselBufferOutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselFeedPipe
import org.neo4j.cypher.internal.runtime.pipelined.operators.MorselSorting
import org.neo4j.cypher.internal.runtime.pipelined.operators.MultiNodeIndexSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NoOutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeByIdSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeCountFromCountStoreOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeHashJoinSingleNodeOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexContainsScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexEndsWithScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeIndexSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NodeRightOuterHashJoinOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.NonFuseableOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalExpandAllOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalExpandIntoOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OrderedAggregationOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OrderedDistinctOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OutputOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.PartialSortOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.PartialTopOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProcedureCallMiddleOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProcedureCallOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProduceResultOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProjectOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.RelationshipCountFromCountStoreOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SkipOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeHeadOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeMiddleOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SortMergeOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.SortPreOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.TopOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.UndirectedRelationshipByIdSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.UnionOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.UnwindOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ValueHashJoinOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandOperator
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityMutableDescription
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityMutableDescriptionImpl
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.DistinctAllPrimitive
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.DistinctWithReferences
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.computeSlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionsForResult
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.findDistinctPhysicalOp
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.partitionGroupingExpressions
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.translateColumnOrder
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPropertyKeys
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.schema.IndexOrder

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * Responsible for a mapping from LogicalPlans to Operators.
 */
class OperatorFactory(val executionGraphDefinition: ExecutionGraphDefinition,
                      val converters: ExpressionConverters,
                      val readOnly: Boolean,
                      val indexRegistrator: QueryIndexRegistrator,
                      semanticTable: SemanticTable,
                      tokenContext: TokenContext,
                      val interpretedPipesFallbackPolicy: InterpretedPipesFallbackPolicy,
                      val slottedPipeBuilder: Option[PipeMapper],
                      runtimeName: String,
                      parallelExecution: Boolean) {

  private val physicalPlan = executionGraphDefinition.physicalPlan
  private val aggregatorFactory = AggregatorFactory(physicalPlan)

  // When determining if an interpreted pipe fallback operator can be used as a middle operator we need breakOn() to answer with fusion disabled
  private val breakingPolicyForInterpretedPipesFallback = PipelinedPipelineBreakingPolicy(OPERATOR_FUSION_DISABLED, interpretedPipesFallbackPolicy, parallelExecution)

  def create(plan: LogicalPlan, inputBuffer: BufferDefinition): Operator = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Input(nodes, relationships, variables, _) =>
        new InputOperator(WorkIdentity.fromPlan(plan),
          nodes.map(v => slots.getLongOffsetFor(v)).toArray,
          relationships.map(v => slots.getLongOffsetFor(v)).toArray,
          variables.map(v => slots.getReferenceOffsetFor(v)).toArray)

      case plans.AllNodesScan(column, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new AllNodeScanOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          argumentSize)

      case plans.NodeByLabelScan(column, label, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        indexRegistrator.registerLabelScan()
        new LabelScanOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          LazyLabel(label)(semanticTable),
          argumentSize,
          asKernelIndexOrder(indexOrder))

      case plans.NodeIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        if (indexSeekMode == LockingUniqueIndexSeek) {
          throw new CantCompileQueryException(s"$runtimeName does not yet support the plans including `NodeUniqueIndexSeek(Locking)`, use another runtime.")
        }

        new NodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
          indexRegistrator.registerQueryIndex(label, properties),
          asKernelIndexOrder(indexOrder),
          argumentSize,
          valueExpr.map(converters.toCommandExpression(id, _)),
          indexSeekMode)

      case plans.NodeUniqueIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
          indexRegistrator.registerQueryIndex(label, properties),
          asKernelIndexOrder(indexOrder),
          argumentSize,
          valueExpr.map(converters.toCommandExpression(id, _)),
          indexSeekMode)

      case plans.MultiNodeIndexSeek(nodeIndexSeeks) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val nodeIndexSeekParameters: Seq[NodeIndexSeekParameters] = nodeIndexSeeks.map { p =>
            val columnOffset = slots.getLongOffsetFor(p.idName)
            val slottedIndexProperties = p.properties.map(SlottedIndexedProperty(p.idName, _, slots)).toArray
            val queryIndex = indexRegistrator.registerQueryIndex(p.label, p.properties)
            val kernelIndexOrder = asKernelIndexOrder(p.indexOrder)
            val valueExpression = p.valueExpr.map(converters.toCommandExpression(id, _))
            val indexSeekMode = IndexSeekModeFactory(unique = p.isInstanceOf[NodeUniqueIndexSeek], readOnly = readOnly).fromQueryExpression(p.valueExpr)
            NodeIndexSeekParameters(columnOffset, slottedIndexProperties, queryIndex, kernelIndexOrder, valueExpression, indexSeekMode)
        }

        new MultiNodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
                                       argumentSize,
                                       nodeIndexSeekParameters)

      case plans.NodeIndexScan(column, labelToken, properties, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new NodeIndexScanOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
          indexRegistrator.registerQueryIndex(labelToken, properties),
          asKernelIndexOrder(indexOrder),
          argumentSize)

      case plans.NodeIndexContainsScan(column, labelToken, property, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new NodeIndexContainsScanOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          SlottedIndexedProperty(column, property, slots),
          indexRegistrator.registerQueryIndex(labelToken, property),
          asKernelIndexOrder(indexOrder),
          converters.toCommandExpression(id, valueExpr),
          argumentSize)

      case plans.NodeIndexEndsWithScan(column, labelToken, property, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new NodeIndexEndsWithScanOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          SlottedIndexedProperty(column, property, slots),
          indexRegistrator.registerQueryIndex(labelToken, property),
          asKernelIndexOrder(indexOrder),
          converters.toCommandExpression(id, valueExpr),
          argumentSize)

      case plans.NodeByIdSeek(column, nodeIds, _) =>
        new NodeByIdSeekOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          converters.toCommandSeekArgs(id, nodeIds),
          physicalPlan.argumentSizes(id))

      case plans.DirectedRelationshipByIdSeek(column, relIds, startNode, endNode, _) =>
        new DirectedRelationshipByIdSeekOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          slots.getLongOffsetFor(startNode),
          slots.getLongOffsetFor(endNode),
          converters.toCommandSeekArgs(id, relIds),
          physicalPlan.argumentSizes(id))

      case plans.UndirectedRelationshipByIdSeek(column, relIds, startNode, endNode, _) =>
        new UndirectedRelationshipByIdSeekOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          slots.getLongOffsetFor(startNode),
          slots.getLongOffsetFor(endNode),
          converters.toCommandSeekArgs(id, relIds),
          physicalPlan.argumentSizes(id))

      case plans.NodeCountFromCountStore(idName, labelNames, _) =>
        val labels = labelNames.map(label => label.map(LazyLabel(_)(semanticTable)))
        new NodeCountFromCountStoreOperator(WorkIdentity.fromPlan(plan),
          slots.getReferenceOffsetFor(idName),
          labels,
          physicalPlan.argumentSizes(id))

      case plans.RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, _) =>
        val maybeStartLabel = startLabel.map(label => LazyLabel(label)(semanticTable))
        val relationshipTypes = RelationshipTypes(typeNames.toArray)(semanticTable)
        val maybeEndLabel = endLabel.map(label => LazyLabel(label)(semanticTable))
        new RelationshipCountFromCountStoreOperator(WorkIdentity.fromPlan(plan),
          slots.getReferenceOffsetFor(idName),
          maybeStartLabel,
          relationshipTypes,
          maybeEndLabel,
          physicalPlan.argumentSizes(id))

      case plans.Expand(_, fromName, dir, types, to, relName, plans.ExpandAll, expandProperties) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(to)
        val lazyTypes = RelationshipTypes(types.toArray)(semanticTable)
        val (nodePropsToCache, relPropsToCache) = getExpandProperties(slots, tokenContext, expandProperties)
        new ExpandAllOperator(WorkIdentity.fromPlan(plan),
          fromSlot,
          relOffset,
          toOffset,
          dir,
          lazyTypes,
          nodePropsToCache,
          relPropsToCache)

      case plans.Expand(_, fromName, dir, types, to, relName, plans.ExpandInto, _) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toSlot = slots(to)
        val lazyTypes = RelationshipTypes(types.toArray)(semanticTable)
        new ExpandIntoOperator(WorkIdentity.fromPlan(plan),
          fromSlot,
          relOffset,
          toSlot,
          dir,
          lazyTypes)(id)

      case plans.VarExpand(_,
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
        val lazyTypes = RelationshipTypes(types.toArray)(semanticTable)

        val tempNodeOffset = expressionSlotForPredicate(nodePredicate)
        val tempRelationshipOffset = expressionSlotForPredicate(relationshipPredicate)

        new VarExpandOperator(WorkIdentity.fromPlan(plan),
          fromSlot,
          relOffset,
          toSlot,
          dir,
          projectedDir,
          lazyTypes,
          length.min,
          length.max.getOrElse(Int.MaxValue),
          mode == plans.ExpandAll,
          tempNodeOffset,
          tempRelationshipOffset,
          nodePredicate.map(x => converters.toCommandExpression(id, x.predicate)).getOrElse(True()),
          relationshipPredicate.map(x => converters.toCommandExpression(id, x.predicate)).getOrElse(True()))

      case plans.OptionalExpand(_, fromName, dir, types, to, relName, plans.ExpandAll, maybePredicate, expandProperties) =>
        val (nodePropsToCache, relPropsToCache) = getExpandProperties(slots, tokenContext, expandProperties)
        new OptionalExpandAllOperator(WorkIdentity.fromPlan(plan),
          slots(fromName),
          slots.getLongOffsetFor(relName),
          slots.getLongOffsetFor(to),
          dir,
          RelationshipTypes(types.toArray)(semanticTable),
          maybePredicate.map(converters.toCommandExpression(id, _)),
          nodePropsToCache,
          relPropsToCache)

      case plans.OptionalExpand(_, fromName, dir, types, to, relName, plans.ExpandInto, maybePredicate, _) =>
        new OptionalExpandIntoOperator(WorkIdentity.fromPlan(plan),
          slots(fromName),
          slots.getLongOffsetFor(relName),
          slots(to),
          dir,
          RelationshipTypes(types.toArray)(semanticTable),
          maybePredicate.map(converters.toCommandExpression(id, _)))(id)

      case plans.Optional(source, protectedSymbols) =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStreamBufferVariant].argumentStateMapId
        val nullableKeys = source.availableSymbols -- protectedSymbols
        val nullableSlots: Array[Slot] = nullableKeys.map(k => slots.get(k).get).toArray
        val argumentSize = physicalPlan.argumentSizes(id)

        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)

        new OptionalOperator(WorkIdentity.fromPlan(plan), argumentStateMapId, argumentSlotOffset, nullableSlots, slots, argumentSize)(id)

      case _: plans.Anti =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStreamBufferVariant].argumentStateMapId
        val argumentSize = physicalPlan.argumentSizes(id)

        val argumentDepth = physicalPlan.applyPlans(id)
        new AntiOperator(WorkIdentity.fromPlan(plan), argumentStateMapId, argumentSize)(id)

      case joinPlan: plans.NodeHashJoin =>

        val slotConfigs = physicalPlan.slotConfigurations
        val argumentSize = physicalPlan.argumentSizes(id)
        val nodes = joinPlan.nodes.toArray

        val lhsOffsets = KeyOffsets.create(slots, nodes)
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val rhsOffsets = KeyOffsets.create(rhsSlots, nodes)

        val rhsSlotMappings = computeSlotMappings(rhsSlots, argumentSize, slots)

        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        if (lhsOffsets.isSingle) {
          new NodeHashJoinSingleNodeOperator(
            WorkIdentity.fromPlan(plan),
            buffer.lhsArgumentStateMapId,
            buffer.rhsArgumentStateMapId,
            lhsOffsets.asSingle,
            rhsOffsets.asSingle,
            rhsSlotMappings)(id)
        } else {
          new NodeHashJoinOperator(
            WorkIdentity.fromPlan(plan),
            buffer.lhsArgumentStateMapId,
            buffer.rhsArgumentStateMapId,
            lhsOffsets,
            rhsOffsets,
            rhsSlotMappings)(id)
        }

      case joinPlan: plans.RightOuterHashJoin =>

        val slotConfigs = physicalPlan.slotConfigurations
        val argumentSize = physicalPlan.argumentSizes(id)
        val keyVariables = joinPlan.nodes.toArray

        val lhsSlots = slotConfigs(joinPlan.left.id)
        val rhsSlots = slotConfigs(joinPlan.right.id)

        val lhsKeyOffsets = KeyOffsets.create(lhsSlots, keyVariables)
        val rhsKeyOffsets = KeyOffsets.create(rhsSlots, keyVariables)

        val lhsSlotMappings = computeSlotMappings(lhsSlots, argumentSize, slots)

        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        new NodeRightOuterHashJoinOperator(
          WorkIdentity.fromPlan(plan),
          buffer.lhsArgumentStateMapId,
          buffer.rhsArgumentStateMapId,
          lhsKeyOffsets,
          rhsKeyOffsets,
          lhsSlots,
          lhsSlotMappings)(id)

      case _: ConditionalApply |
           _: AntiConditionalApply |
           _: SelectOrSemiApply |
           _: SelectOrAntiSemiApply =>
        new ConditionalApplyOperator(WorkIdentity.fromPlan(plan),
          physicalPlan.slotConfigurations.get(plan.lhs.get.id),
          physicalPlan.slotConfigurations.get(plan.rhs.get.id))

      case joinPlan: plans.ValueHashJoin =>

        val slotConfigs = physicalPlan.slotConfigurations
        val argumentSize = physicalPlan.argumentSizes(id)
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        val lhsExpression = converters.toCommandExpression(id, joinPlan.join.lhs)
        val rhsExpression = converters.toCommandExpression(id, joinPlan.join.rhs)
        val rhsSlotMappings = computeSlotMappings(rhsSlots, argumentSize, slots)

        new ValueHashJoinOperator(WorkIdentity.fromPlan(plan),
                                  buffer.lhsArgumentStateMapId,
                                  buffer.rhsArgumentStateMapId,
                                  slots,
                                  lhsExpression,
                                  rhsExpression,
                                  rhsSlotMappings)(id)


      case _: plans.CartesianProduct =>
        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        new CartesianProductOperator(
          WorkIdentity.fromPlan(plan),
          buffer.lhsArgumentStateMapId,
          buffer.rhsArgumentStateMapId,
          physicalPlan.argumentSizes(id)
        )(id)

      case plans.UnwindCollection(_, variable, collection) =>
        val offset = slots.get(variable) match {
          case Some(RefSlot(idx, _, _)) => idx
          case Some(slot) =>
            throw new InternalException(s"$slot cannot be used for UNWIND")
          case None =>
            throw new InternalException("No slot found for UNWIND")
        }
        val runtimeExpression = converters.toCommandExpression(id, collection)
        new UnwindOperator(WorkIdentity.fromPlan(plan), runtimeExpression, offset)

      case plans.Sort(_, sortItems) =>
        val comparator = MorselSorting.createComparator(sortItems.map(translateColumnOrder(slots, _)))
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        new SortMergeOperator(
          argumentStateMapId,
          WorkIdentity.fromPlan(plan),
          comparator)(id)

      case plans.PartialSort(_, alreadySortedPrefix, stillToSortSuffix) =>
        val prefixComparator = MorselSorting.createComparator(alreadySortedPrefix.map(translateColumnOrder(slots, _)))
        val suffixComparator = MorselSorting.createComparator(stillToSortSuffix.map(translateColumnOrder(slots, _)))
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        new PartialSortOperator(
          argumentStateMapId,
          WorkIdentity.fromPlan(plan),
          prefixComparator,
          suffixComparator)(id)

      case plans.Top(_, sortItems, limit) =>
        val ordering = sortItems.map(translateColumnOrder(slots, _))
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        TopOperator(WorkIdentity.fromPlan(plan),
          ordering,
          converters.toCommandExpression(id, limit)).reducer(argumentStateMapId, id)

      case plans.PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit) =>
        val prefixComparator = MorselSorting.createComparator(alreadySortedPrefix.map(translateColumnOrder(slots, _)))
        val suffixComparator = MorselSorting.createComparator(stillToSortSuffix.map(translateColumnOrder(slots, _)))
        val Seq(bufferAsmId, workCancellerAsmId) = executionGraphDefinition.argumentStateMaps.toSeq.collect {
          case argumentStateDef if argumentStateDef.planId == id => argumentStateDef.id
        }

        val applyPlanId = physicalPlan.applyPlans(id)
        val argumentSlotOffset = slots.getArgumentLongOffsetFor(applyPlanId)

        new PartialTopOperator(
          bufferAsmId,
          workCancellerAsmId,
          argumentSlotOffset,
          WorkIdentity.fromPlan(plan),
          prefixComparator,
          suffixComparator,
          converters.toCommandExpression(id, limit),
          id)

      case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId

        val aggregators = Array.newBuilder[Aggregator]
        val outputSlots = Array.newBuilder[Int]
        aggregationExpression.foreach {
          case (key, astExpression) =>
            val outputSlot = slots(key)
            val (aggregator, _) = aggregatorFactory.newAggregator(astExpression)
            aggregators += aggregator
            outputSlots += outputSlot.offset
        }
        AggregationOperatorNoGrouping(WorkIdentity.fromPlan(plan),
          aggregators.result())
          .reducer(argumentStateMapId,
            outputSlots.result(), id)

      case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)

        val aggregators = Array.newBuilder[Aggregator]
        val outputSlots = Array.newBuilder[Int]
        aggregationExpression.foreach {
          case (key, astExpression) =>
            val outputSlot = slots(key)
            val (aggregator, _) = aggregatorFactory.newAggregator(astExpression)
            aggregators += aggregator
            outputSlots += outputSlot.offset
        }

        AggregationOperator(WorkIdentity.fromPlan(plan), aggregators.result(), groupings)
          .reducer(argumentStateMapId, outputSlots.result(), id)

      case plans.OrderedAggregation(_, groupingExpressions, aggregationExpression, orderToLeverage) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        val applyPlanId = physicalPlan.applyPlans(id)
        val argumentSlotOffset = slots.getArgumentLongOffsetFor(applyPlanId)

        val aggExpressions = Array.newBuilder[AggregationExpression]
        val outputSlots = Array.newBuilder[Int]
        aggregationExpression.foreach {
          case (key, astExpression) =>
            val outputSlot = slots(key)
            outputSlots += outputSlot.offset
            aggExpressions += converters.toCommandExpression(id, astExpression).asInstanceOf[AggregationExpression]
        }

        val (orderedGroupingColumns, unorderedGroupingColumns) = partitionGroupingExpressions(converters, groupingExpressions, orderToLeverage, id)
        if (unorderedGroupingColumns.isEmpty) {
          new AllOrderedAggregationOperator(
            argumentStateMapId,
            WorkIdentity.fromPlan(plan),
            aggExpressions.result(),
            orderedGroupingColumns,
            outputSlots.result(),
            physicalPlan.argumentSizes(id)
          )(id)
        } else {
          new OrderedAggregationOperator(
            argumentStateMapId,
            argumentSlotOffset,
            WorkIdentity.fromPlan(plan),
            aggExpressions.result(),
            orderedGroupingColumns,
            unorderedGroupingColumns,
            outputSlots.result(),
            physicalPlan.argumentSizes(id)
          )(id)
        }

      case plan: plans.ProduceResult => createProduceResults(plan)

      case _: plans.Argument =>
        new ArgumentOperator(WorkIdentity.fromPlan(plan),
          physicalPlan.argumentSizes(id))

      case plans.Union(lhs, rhs) =>
        val lhsSlots = physicalPlan.slotConfigurations(lhs.id)
        val rhsSlots = physicalPlan.slotConfigurations(rhs.id)
        new UnionOperator(
          WorkIdentity.fromPlan(plan),
          lhsSlots,
          rhsSlots,
          SlottedPipeMapper.computeUnionRowMapping(lhsSlots, slots),
          SlottedPipeMapper.computeUnionRowMapping(rhsSlots, slots)
        )

      case plans.ProcedureCall(_, call@ResolvedCall(signature, callArguments, _, _, _)) =>
        new ProcedureCallOperator(
          WorkIdentity.fromPlan(plan),
          signature,
          ProcedureCallMode.fromAccessMode(signature.accessMode),
          argumentsForProcedureCall(id, signature, callArguments),
          call.callResults.map(r => r.outputName).toArray,
          call.callResultIndices.map {
            case (k, (n, _)) => (k, slots(n).offset)
          }.toArray)

      case _ if slottedPipeBuilder.isDefined =>
        // Validate that we support fallback for this plan (throws CantCompileQueryException otherwise)
        interpretedPipesFallbackPolicy.breakOn(plan)
        createSlottedPipeHeadOperator(plan)

      case _ =>
        throw new CantCompileQueryException(s"$runtimeName does not yet support the plans including `$plan`, use another runtime.")
    }
  }

  def createMiddleOperators(middlePlans: Seq[LogicalPlan], headOperator: Operator): Array[MiddleOperator] = {
    val maybeSlottedPipeOperatorToChainOnTo: Option[SlottedPipeOperator] = headOperator match {
      case op: SlottedPipeOperator =>
        Some(op)
      case _ =>
        None
    }

    val middleOperatorBuilder = new ArrayBuffer[MiddleOperator]
    middlePlans.foldLeft(middleOperatorBuilder -> maybeSlottedPipeOperatorToChainOnTo)(createMiddleFoldFunction)
    val middleOperators = middleOperatorBuilder.result.toArray
    middleOperators
  }

  // To be used with foldLeft only
  // The accumulator is a tuple of
  // _1 a mutating buffer of middle operators
  // _2 an optional slotted pipe operator to chain sub-sequent slotted pipes to
  private def createMiddleFoldFunction(acc: (ArrayBuffer[MiddleOperator], Option[SlottedPipeOperator]), plan: LogicalPlan): (ArrayBuffer[MiddleOperator], Option[SlottedPipeOperator]) = {
    val (middleOperators, maybeSlottedPipeOperatorToChainOnTo) = acc

    val maybeNewOperator =
      createMiddleOrUpdateSlottedPipeChain(plan, maybeSlottedPipeOperatorToChainOnTo)

    // Add the new middle operator (unless it was chained on to an existing slotted pipe)
    middleOperators ++= maybeNewOperator

    // Determine what to do with the slotted pipe chain
    maybeNewOperator match {
      case Some(spo: SlottedPipeOperator) if maybeSlottedPipeOperatorToChainOnTo.isEmpty =>
        // We have a new slotted pipe operator to chain on to
        (middleOperators, Some(spo))

      case Some(mo) if maybeSlottedPipeOperatorToChainOnTo.isDefined =>
        // We have a new normal pipelined operator, the existing slotted pipe chain is now broken
        (middleOperators, None)

      case _ =>
        // Nothing changes in this regard, keep going with or without a slotted pipe chain
        acc
    }
  }

  // Returns Some new middle operator or None if the existing slotted pipe chain has been updated instead
  private def createMiddleOrUpdateSlottedPipeChain(plan: LogicalPlan, maybeSlottedPipeOperatorToChainOnTo: Option[SlottedPipeOperator]): Option[MiddleOperator] = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Selection(predicate, _) =>
        Some(new FilterOperator(WorkIdentity.fromPlan(plan), converters.toCommandExpression(id, predicate)))

      case plans.NonFuseable(_) =>
        Some(new NonFuseableOperator(WorkIdentity.fromPlan(plan)))

      case plans.Limit(_, count, DoNotIncludeTies) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        Some(new LimitOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), converters.toCommandExpression(id, count)))

      case plans.Skip(_, count) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        Some(new SkipOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), converters.toCommandExpression(id, count)))

      case plans.Distinct(_, groupingExpressions) =>
        val physicalDistinctOp = findDistinctPhysicalOp(groupingExpressions, Seq.empty)
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)

        physicalDistinctOp match {
          case DistinctAllPrimitive(offsets, _) if offsets.size == 1 =>
            val (toSlot, expression) = groupingExpressions.head
            val runtimeExpression = converters.toCommandExpression(id, expression)
            Some(new DistinctSinglePrimitiveOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), slots(toSlot), offsets.head, runtimeExpression)(id))
          case DistinctAllPrimitive(offsets, _) =>
            Some(new DistinctPrimitiveOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), offsets.sorted.toArray, groupings)(id))
          case DistinctWithReferences =>
            Some(new DistinctOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), groupings)(id))
        }

      case plans.OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        if (groupingExpressions.values.forall(orderToLeverage.contains)) {
          val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)
          Some(new AllOrderedDistinctOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), groupings)(id))
        } else {
          val (orderedGroupingExpressions, unorderedGroupingExpressions) = groupingExpressions.partition { case (_,v) => orderToLeverage.contains(v) }
          val orderedGroupingColumns = converters.toGroupingExpression(id, orderedGroupingExpressions, orderToLeverage)
          val unorderedGroupingColumns = converters.toGroupingExpression(id, unorderedGroupingExpressions, orderToLeverage)

          Some(new OrderedDistinctOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), orderedGroupingColumns, unorderedGroupingColumns)(id))
        }

      case plans.Projection(_, expressions) =>
        val toProject = expressions collect {
          case (k, e) if isRefSlotAndNotAlias(slots, k) => k -> e
        }
        val projectionOps: CommandProjection = converters.toCommandProjection(id, toProject)
        Some(new ProjectOperator(WorkIdentity.fromPlan(plan), projectionOps))

      case plans.CacheProperties(_, properties) =>
        val propertyOps = properties.toArray.map(converters.toCommandExpression(id, _))
        Some(new CachePropertiesOperator(WorkIdentity.fromPlan(plan), propertyOps))

      case _: plans.Argument => None

      case plans.ProcedureCall(_, call@ResolvedCall(signature, callArguments, _, _, _)) =>
        val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)
        Some(new ProcedureCallMiddleOperator(
          WorkIdentity.fromPlan(plan),
          signature,
          callMode,
          argumentsForProcedureCall(id, signature, callArguments),
          call.callResults.map(r => r.outputName).toArray
        ))

      case _ if slottedPipeBuilder.isDefined =>
        // Validate that we support fallback for this plan (throws CantCompileQueryException)
        interpretedPipesFallbackPolicy.breakOn(plan)
        if (breakingPolicyForInterpretedPipesFallback.breakOn(plan, physicalPlan.applyPlans(id))) {
          // Plan is supported, but only as a head plan
          throw new CantCompileQueryException(s"$runtimeName does not yet support using `$plan` as a fallback middle plan, use another runtime.")
        }
        createSlottedPipeMiddleOperator(plan, maybeSlottedPipeOperatorToChainOnTo)

      case _ =>
        throw new CantCompileQueryException(s"$runtimeName does not yet support using `$plan` as a middle plan, use another runtime.")
    }
  }

  def createProduceResults(plan: plans.ProduceResult): ProduceResultOperator = {
    val slots = physicalPlan.slotConfigurations(plan.id)
    val runtimeColumns = createProjectionsForResult(plan.columns, slots)
    new ProduceResultOperator(WorkIdentity.fromPlan(plan),
      slots,
      runtimeColumns)
  }

  def createOutput(outputDefinition: OutputDefinition, nextPipelineCanTrackTime: Boolean): OutputOperator = {

    outputDefinition match {
      case NoOutput => NoOutputOperator
      case MorselBufferOutput(bufferId, planId) => MorselBufferOutputOperator(bufferId, planId, nextPipelineCanTrackTime)
      case MorselArgumentStateBufferOutput(bufferId, argumentSlotOffset, planId) => MorselArgumentStateBufferOutputOperator(bufferId, argumentSlotOffset, planId, nextPipelineCanTrackTime)
      case ProduceResultOutput(p) => createProduceResults(p)
      case ReduceOutput(bufferId, argumentStateMapId, plan) =>
        val id = plan.id
        val slots = physicalPlan.slotConfigurations(id)
        generateSlotAccessorFunctions(slots)
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)

        plan match {
          case plans.Sort(_, sortItems) =>
            val ordering = sortItems.map(translateColumnOrder(slots, _))
            new SortPreOperator(WorkIdentity.fromPlan(plan, "Pre"), argumentSlotOffset, bufferId, ordering)

          case plans.Top(_, sortItems, limit) =>
            val ordering = sortItems.map(translateColumnOrder(slots, _))
            TopOperator(WorkIdentity.fromPlan(plan, "Pre"),
              ordering,
              converters.toCommandExpression(id, limit)).mapper(argumentSlotOffset, bufferId)

          case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
            val (aggregators, expressions) = buildAggregators(id, aggregationExpression)
            AggregationOperatorNoGrouping(WorkIdentity.fromPlan(plan, "Pre"), aggregators)
              .mapper(argumentSlotOffset, argumentStateMapId, expressions, id)

          case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
            val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)
            val (aggregators, expressions) = buildAggregators(id, aggregationExpression)

            AggregationOperator(WorkIdentity.fromPlan(plan, "Pre"), aggregators, groupings)
              .mapper(argumentSlotOffset, argumentStateMapId, expressions, id)

        }
    }
  }

  private def buildAggregators(operatorId: Id, aggregationExpression: Map[String, org.neo4j.cypher.internal.expressions.Expression]): (Array[Aggregator], Array[Expression]) = {
    val aggregators = Array.newBuilder[Aggregator]
    val expressions = Array.newBuilder[Expression]
    aggregationExpression.foreach {
      case (_, astExpression) =>
        val (aggregator, expression) = aggregatorFactory.newAggregator(astExpression)
        aggregators += aggregator
        expressions += converters.toCommandExpression(operatorId, expression)
    }
    (aggregators.result(), expressions.result())
  }

  private def workIdentityDescriptionForPipe(pipe: Pipe): String = {
    @tailrec
    def collectPipeNames(pipe: Pipe, acc: List[String]): List[String] = {
      val pipeName = pipe.getClass.getSimpleName
      pipe match {
        case p: PipeWithSource =>
          collectPipeNames(p.getSource, pipeName :: acc)
        case p: DropResultPipe => // Special case that does not implement PipeWithSource
          collectPipeNames(p.source, pipeName :: acc)
        case _ =>
          acc
      }
    }

    val pipeNames = collectPipeNames(pipe, Nil)
    pipeNames.mkString("(", "->", ")")
  }

  private def workIdentityFromSlottedPipePlan(opName: String, plan: LogicalPlan, pipe: Pipe): WorkIdentityMutableDescription = {
    val prefix = s"$opName[${plan.getClass.getSimpleName}]"
    val pipeDescription = workIdentityDescriptionForPipe(pipe)
    WorkIdentityMutableDescriptionImpl(plan.id, prefix, pipeDescription)
  }

  private def createSlottedPipeHeadOperator(plan: LogicalPlan): Operator = {
    val feedPipe = MorselFeedPipe()(Id.INVALID_ID)
    val pipe = slottedPipeBuilder.get.onOneChildPlan(plan, feedPipe)
    val workIdentity = workIdentityFromSlottedPipePlan("SlottedPipeHead", plan, pipe)
    new SlottedPipeHeadOperator(workIdentity, pipe)
  }

  private def createSlottedPipeMiddleOperator(plan: LogicalPlan, maybeSlottedPipeOperatorToChainOnTo: Option[SlottedPipeOperator]): Option[MiddleOperator] = {
    maybeSlottedPipeOperatorToChainOnTo match {
      case Some(slottedPipeOperator) =>
        // We chain the new pipe to the existing pipe in the previous operator
        val chainedPipe = slottedPipeBuilder.get.onOneChildPlan(plan, slottedPipeOperator.pipe)
        slottedPipeOperator.setPipe(chainedPipe)
        // Recompute work identity description
        slottedPipeOperator.workIdentity.updateDescription(workIdentityDescriptionForPipe(chainedPipe))
        None

      case None =>
        val feedPipe = MorselFeedPipe()(Id.INVALID_ID)
        val pipe = slottedPipeBuilder.get.onOneChildPlan(plan, feedPipe)
        val workIdentity = workIdentityFromSlottedPipePlan("SlottedPipeMiddle", plan, pipe)
        Some(new SlottedPipeMiddleOperator(workIdentity, pipe))
    }
  }

  private def argumentsForProcedureCall(id: Id,
                                        signature: ProcedureSignature,
                                        callArguments: Seq[org.neo4j.cypher.internal.expressions.Expression]): Seq[Expression] = {
    callArguments.map(Some(_)).zipAll(signature.inputSignature.map(_.default), None, None).map {
      case (given, default) => given.map(converters.toCommandExpression(id, _)).getOrElse(Literal(default.get))
    }
  }
}

object OperatorFactory {
  def getExpandProperties(slots: SlotConfiguration,
                          tokenContext: TokenContext,
                          expandProperties: Option[ExpandCursorProperties]): (Option[SlottedPropertyKeys], Option[SlottedPropertyKeys]) = {
    val (nodePropsToCache, relPropsToCache) = expandProperties match {
      case Some(rp) => (
        if (rp.nodeProperties.isEmpty) None else Some(SlottedPropertyKeys.resolve(rp.nodeProperties, slots, tokenContext)),
        if (rp.relProperties.isEmpty) None else Some(SlottedPropertyKeys.resolve(rp.relProperties, slots, tokenContext)))
      case None => (None, None)
    }
    (nodePropsToCache, relPropsToCache)
  }
}

case class NodeIndexSeekParameters(nodeSlotOffset: Int,
                                   slottedIndexProperties: Array[SlottedIndexedProperty],
                                   queryIndex: Int,
                                   kernelIndexOrder: IndexOrder,
                                   valueExpression: QueryExpression[Expression],
                                   indexSeekMode: IndexSeekMode)
