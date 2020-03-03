/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateBufferVariant
import org.neo4j.cypher.internal.physicalplanning.BufferDefinition
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.physicalplanning.LHSAccumulatingRHSStreamingBufferVariant
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.MorselArgumentStateBufferOutput
import org.neo4j.cypher.internal.physicalplanning.MorselBufferOutput
import org.neo4j.cypher.internal.physicalplanning.NoOutput
import org.neo4j.cypher.internal.physicalplanning.OperatorFusionPolicy.OPERATOR_FUSION_DISABLED
import org.neo4j.cypher.internal.physicalplanning.OptionalBufferVariant
import org.neo4j.cypher.internal.physicalplanning.OutputDefinition
import org.neo4j.cypher.internal.physicalplanning.ProduceResultOutput
import org.neo4j.cypher.internal.physicalplanning.ReduceOutput
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.isRefSlotAndNotAlias
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
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
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AggregatorFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationOperatorNoGrouping
import org.neo4j.cypher.internal.runtime.pipelined.operators.AllNodeScanOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ArgumentOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.CachePropertiesOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.CartesianProductOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DirectedRelationshipByIdSeekOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.Operator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalExpandAllOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalExpandIntoOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.OutputOperator
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.UnwindOperator
import org.neo4j.cypher.internal.runtime.pipelined.operators.VarExpandOperator
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityMutableDescription
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityMutableDescriptionImpl
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionsForResult
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.translateColumnOrder
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
                      val interpretedPipesFallbackPolicy: InterpretedPipesFallbackPolicy,
                      val slottedPipeBuilder: Option[PipeMapper]) {

  private val physicalPlan = executionGraphDefinition.physicalPlan
  private val aggregatorFactory = AggregatorFactory(physicalPlan)

  // When determining if an interpreted pipe fallback operator can be used as a middle operator we need breakOn() to answer with fusion disabled
  private val breakingPolicyForInterpretedPipesFallback = PipelinedPipelineBreakingPolicy(OPERATOR_FUSION_DISABLED, interpretedPipesFallbackPolicy)

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

      case plans.NodeByLabelScan(column, label, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        indexRegistrator.registerLabelScan()
        new LabelScanOperator(WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          LazyLabel(label)(semanticTable),
          argumentSize)

      case plans.NodeIndexSeek(column, label, properties, valueExpr, _,  indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        if (indexSeekMode == LockingUniqueIndexSeek) {
          throw new CantCompileQueryException("Pipelined does not yet support the plans including `NodeUniqueIndexSeek(Locking)`, use another runtime.")
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

      case plans.Expand(_, fromName, dir, types, to, relName, plans.ExpandAll) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(to)
        val lazyTypes = RelationshipTypes(types.toArray)(semanticTable)
        new ExpandAllOperator(WorkIdentity.fromPlan(plan),
          fromSlot,
          relOffset,
          toOffset,
          dir,
          lazyTypes)

      case plans.Expand(_, fromName, dir, types, to, relName, plans.ExpandInto) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toSlot = slots(to)
        val lazyTypes = RelationshipTypes(types.toArray)(semanticTable)
        new ExpandIntoOperator(WorkIdentity.fromPlan(plan),
          fromSlot,
          relOffset,
          toSlot,
          dir,
          lazyTypes)

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

      case plans.OptionalExpand(_, fromName, dir, types, to, relName, plans.ExpandAll, maybePredicate) =>
        new OptionalExpandAllOperator(WorkIdentity.fromPlan(plan),
          slots(fromName),
          slots.getLongOffsetFor(relName),
          slots.getLongOffsetFor(to),
          dir,
          RelationshipTypes(types.toArray)(semanticTable),
          maybePredicate.map(converters.toCommandExpression(id, _)))

      case plans.OptionalExpand(_, fromName, dir, types, to, relName, plans.ExpandInto, maybePredicate) =>
        new OptionalExpandIntoOperator(WorkIdentity.fromPlan(plan),
          slots(fromName),
          slots.getLongOffsetFor(relName),
          slots(to),
          dir,
          RelationshipTypes(types.toArray)(semanticTable),
          maybePredicate.map(converters.toCommandExpression(id, _)))

      case plans.Optional(source, protectedSymbols) =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[OptionalBufferVariant].argumentStateMapId
        val nullableKeys = source.availableSymbols -- protectedSymbols
        val nullableSlots: Array[Slot] = nullableKeys.map(k => slots.get(k).get).toArray
        val argumentSize = physicalPlan.argumentSizes(plan.id)

        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)

        new OptionalOperator(WorkIdentity.fromPlan(plan), argumentStateMapId, argumentSlotOffset, nullableSlots, slots, argumentSize)(plan.id)

      case joinPlan:plans.NodeHashJoin =>

        val slotConfigs = physicalPlan.slotConfigurations
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val nodes = joinPlan.nodes.toArray
        val lhsOffsets: Array[Int] = nodes.map(k => slots.getLongOffsetFor(k))
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val rhsOffsets: Array[Int] = nodes.map(k => rhsSlots.getLongOffsetFor(k))
        val copyLongsFromRHS = Array.newBuilder[(Int,Int)]
        val copyRefsFromRHS = Array.newBuilder[(Int,Int)]
        val copyCachedPropertiesFromRHS = Array.newBuilder[(Int,Int)]

        // When executing the HashJoin, the LHS will be copied to the first slots in the produced row, and any additional RHS columns that are not
        // part of the join comparison
        rhsSlots.foreachSlotAndAliasesOrdered({
          case SlotWithKeyAndAliases(VariableSlotKey(key), LongSlot(offset, _, _), _) if offset >= argumentSize.nLongs =>
            copyLongsFromRHS += ((offset, slots.getLongOffsetFor(key)))
          case SlotWithKeyAndAliases(VariableSlotKey(key), RefSlot(offset, _, _), _) if offset >= argumentSize.nReferences =>
            copyRefsFromRHS += ((offset, slots.getReferenceOffsetFor(key)))
          case SlotWithKeyAndAliases(_: VariableSlotKey, _, _) => // do nothing, already added by lhs
          case SlotWithKeyAndAliases(_: ApplyPlanSlotKey, _, _) => // do nothing, already added by lhs
          case SlotWithKeyAndAliases(CachedPropertySlotKey(cnp), refSlot, _) =>
            val offset = refSlot.offset
            if (offset >= argumentSize.nReferences)
              copyCachedPropertiesFromRHS += offset -> slots.getCachedPropertyOffsetFor(cnp)
        })

        val longsToCopy = copyLongsFromRHS.result()
        val refsToCopy = copyRefsFromRHS.result()
        val cachedPropertiesToCopy = copyCachedPropertiesFromRHS.result()

        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        if (lhsOffsets.length == 1) {
          new NodeHashJoinSingleNodeOperator(
            WorkIdentity.fromPlan(plan),
            buffer.lhsArgumentStateMapId,
            buffer.rhsArgumentStateMapId,
            lhsOffsets(0),
            rhsOffsets(0),
            slots,
            longsToCopy,
            refsToCopy,
            cachedPropertiesToCopy)(id)
        } else {
          new NodeHashJoinOperator(
            WorkIdentity.fromPlan(plan),
            buffer.lhsArgumentStateMapId,
            buffer.rhsArgumentStateMapId,
            lhsOffsets,
            rhsOffsets,
            slots,
            longsToCopy,
            refsToCopy,
            cachedPropertiesToCopy)(id)
        }

      case _: plans.CartesianProduct =>
        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        new CartesianProductOperator(
          WorkIdentity.fromPlan(plan),
          buffer.lhsArgumentStateMapId,
          buffer.rhsArgumentStateMapId,
          slots,
          physicalPlan.argumentSizes(plan.id)
        )(plan.id)

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
        val ordering = sortItems.map(translateColumnOrder(slots, _))
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        new SortMergeOperator(argumentStateMapId,
          WorkIdentity.fromPlan(plan),
          ordering,
          argumentSlot)(plan.id)

      case plans.Top(_, sortItems, limit) =>
        val ordering = sortItems.map(translateColumnOrder(slots, _))
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        TopOperator(WorkIdentity.fromPlan(plan),
          ordering,
          converters.toCommandExpression(plan.id, limit)).reducer(argumentStateMapId, id)

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

      case plan: plans.ProduceResult => createProduceResults(plan)

      case _: plans.Argument =>
        new ArgumentOperator(WorkIdentity.fromPlan(plan),
          physicalPlan.argumentSizes(id))

      case _ if slottedPipeBuilder.isDefined =>
        // Validate that we support fallback for this plan (throws CantCompileQueryException otherwise)
        interpretedPipesFallbackPolicy.breakOn(plan)
        createSlottedPipeHeadOperator(plan)

      case _ =>
        throw new CantCompileQueryException(s"Pipelined does not yet support the plans including `$plan`, use another runtime.")
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
    middlePlans.foldLeft(middleOperatorBuilder, maybeSlottedPipeOperatorToChainOnTo)(createMiddleFoldFunction)
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

  // Overridden by test class
  // Returns Some new middle operator or None if the existing slotted pipe chain has been updated instead
  protected def createMiddleOrUpdateSlottedPipeChain(plan: LogicalPlan, maybeSlottedPipeOperatorToChainOnTo: Option[SlottedPipeOperator]): Option[MiddleOperator] = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Selection(predicate, _) =>
        Some(new FilterOperator(WorkIdentity.fromPlan(plan), converters.toCommandExpression(id, predicate)))

      case plans.Limit(_, count, DoNotIncludeTies) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        Some(new LimitOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), converters.toCommandExpression(plan.id, count)))

      case plans.Skip(_, count) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        Some(new SkipOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), converters.toCommandExpression(plan.id, count)))

      case plans.Distinct(_, groupingExpressions) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)
        Some(new DistinctOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), groupings)(id))

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

      case _ if slottedPipeBuilder.isDefined =>
        // Validate that we support fallback for this plan (throws CantCompileQueryException)
        interpretedPipesFallbackPolicy.breakOn(plan)
        if (breakingPolicyForInterpretedPipesFallback.breakOn(plan)) {
          // Plan is supported, but only as a head plan
          throw new CantCompileQueryException(s"Pipelined does not yet support using `$plan` as a fallback middle plan, use another runtime.")
        }
        createSlottedPipeMiddleOperator(plan, maybeSlottedPipeOperatorToChainOnTo)

      case _ =>
        throw new CantCompileQueryException(s"Pipelined does not yet support using `$plan` as a middle plan, use another runtime.")
    }
  }

  def createProduceResults(plan: plans.ProduceResult): ProduceResultOperator = {
    val slots = physicalPlan.slotConfigurations(plan.id)
    val runtimeColumns = createProjectionsForResult(plan.columns, slots)
    new ProduceResultOperator(WorkIdentity.fromPlan(plan),
      slots,
      runtimeColumns)
  }

  def createOutput(outputDefinition: OutputDefinition, nextPipelineFused: Boolean): OutputOperator = {

    outputDefinition match {
      case NoOutput => NoOutputOperator
      case MorselBufferOutput(bufferId, planId) => MorselBufferOutputOperator(bufferId, planId, nextPipelineFused)
      case MorselArgumentStateBufferOutput(bufferId, argumentSlotOffset, planId) => MorselArgumentStateBufferOutputOperator(bufferId, argumentSlotOffset, planId, nextPipelineFused)
      case ProduceResultOutput(p) => createProduceResults(p)
      case ReduceOutput(bufferId, plan) =>
        val id = plan.id
        val slots = physicalPlan.slotConfigurations(id)
        generateSlotAccessorFunctions(slots)
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)

        plan match {
          case plans.Sort(_, sortItems) =>
            val ordering = sortItems.map(translateColumnOrder(slots, _))
            new SortPreOperator(WorkIdentity.fromPlan(plan, "Pre"), argumentSlot, bufferId, ordering)

          case plans.Top(_, sortItems, limit) =>
            val ordering = sortItems.map(translateColumnOrder(slots, _))
            TopOperator(WorkIdentity.fromPlan(plan, "Pre"),
              ordering,
              converters.toCommandExpression(plan.id, limit)).mapper(argumentSlot, bufferId)

          case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
            val aggregators = Array.newBuilder[Aggregator]
            val expressions = Array.newBuilder[Expression]
            aggregationExpression.foreach {
              case (key, astExpression) =>
                val (aggregator, expression) = aggregatorFactory.newAggregator(astExpression)
                aggregators += aggregator
                expressions += converters.toCommandExpression(id, expression)
            }

            AggregationOperatorNoGrouping(WorkIdentity.fromPlan(plan, "Pre"),
              aggregators.result())
              .mapper(argumentSlot,
                bufferId,
                expressions.result())

          case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
            val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)

            val aggregators = Array.newBuilder[Aggregator]
            val expressions = Array.newBuilder[Expression]
            aggregationExpression.foreach {
              case (key, astExpression) =>
                val (aggregator, expression) = aggregatorFactory.newAggregator(astExpression)
                aggregators += aggregator
                expressions += converters.toCommandExpression(id, expression)
            }

            AggregationOperator(WorkIdentity.fromPlan(plan, "Pre"), aggregators.result(), groupings)
              .mapper(argumentSlot, bufferId, expressions.result())

        }
    }
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

  protected def createSlottedPipeHeadOperator(plan: LogicalPlan): Operator = {
    val feedPipe = MorselFeedPipe()(Id.INVALID_ID)
    val pipe = slottedPipeBuilder.get.onOneChildPlan(plan, feedPipe)
    val workIdentity = workIdentityFromSlottedPipePlan("SlottedPipeHead", plan, pipe)
    new SlottedPipeHeadOperator(workIdentity, pipe)
  }

  protected def createSlottedPipeMiddleOperator(plan: LogicalPlan, maybeSlottedPipeOperatorToChainOnTo: Option[SlottedPipeOperator]): Option[MiddleOperator] = {
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

}

case class NodeIndexSeekParameters(nodeSlotOffset: Int,
                                   slottedIndexProperties: Array[SlottedIndexedProperty],
                                   queryIndex: Int,
                                   kernelIndexOrder: IndexOrder,
                                   valueExpression: QueryExpression[Expression],
                                   indexSeekMode: IndexSeekMode)
