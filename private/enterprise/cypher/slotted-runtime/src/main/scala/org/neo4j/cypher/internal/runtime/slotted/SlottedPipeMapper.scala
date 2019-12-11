/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AbstractSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CrossApply
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DropResult
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IncludeTies
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MergeCreateNode
import org.neo4j.cypher.internal.logical.plans.MergeCreateRelationship
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.isRefSlotAndNotAlias
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckVariable
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipFromSlot
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DropResultPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EagerAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.EmptyResultPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekModeFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedAggregationPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Top1Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Top1WithTiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TopNPipe
import org.neo4j.cypher.internal.runtime.slotted
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionForIdentifier
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionsForResult
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.translateColumnOrder
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedNonGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedOrderedGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedOrderedNonGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedPrimitiveGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllNodesScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllOrderedDistinctSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllOrderedDistinctSlottedPrimitivePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ArgumentSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CartesianProductSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ConditionalApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateNodeSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateRelationshipSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CrossApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedSinglePrimitivePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.EagerSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ExpandAllSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ExpandIntoSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ForeachSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.MergeCreateNodeSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.MergeCreateRelationshipSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedSingleNodePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeIndexScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeIndexSeekSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodesByLabelScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OptionalExpandAllSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OptionalExpandIntoSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OptionalSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OrderedDistinctSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OrderedDistinctSlottedPrimitivePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.OrderedDistinctSlottedSinglePrimitivePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ProduceResultSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.RollUpApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe.RowMapping
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnwindSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ValueHashJoinSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SlottedPipeMapper(fallback: PipeMapper,
                        expressionConverters: ExpressionConverters,
                        physicalPlan: PhysicalPlan,
                        readOnly: Boolean,
                        indexRegistrator: QueryIndexRegistrator)
                       (implicit semanticTable: SemanticTable)
  extends PipeMapper {

  override def onLeaf(plan: LogicalPlan): Pipe = {

    val id = plan.id
    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)
    val slots = physicalPlan.slotConfigurations(id)
    val argumentSize = physicalPlan.argumentSizes(id)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case AllNodesScan(column, _) =>
        AllNodesScanSlottedPipe(column, slots, argumentSize)(id)

      case NodeIndexScan(column, label, properties, _, indexOrder) =>
        NodeIndexScanSlottedPipe(column, label, properties.map(SlottedIndexedProperty(column, _, slots)),
          indexRegistrator.registerQueryIndex(label, properties), indexOrder, slots, argumentSize)(id)

      case NodeIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, properties.map(SlottedIndexedProperty(column, _, slots)).toIndexedSeq,
          indexRegistrator.registerQueryIndex(label, properties), valueExpr.map(convertExpressions), indexSeekMode, indexOrder, slots, argumentSize)(id)

      case NodeUniqueIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, properties.map(SlottedIndexedProperty(column, _, slots)).toIndexedSeq,
          indexRegistrator.registerQueryIndex(label, properties), valueExpr.map(convertExpressions), indexSeekMode, indexOrder, slots, argumentSize)(id = id)

      case NodeByLabelScan(column, label, _) =>
        indexRegistrator.registerLabelScan()
        NodesByLabelScanSlottedPipe(column, LazyLabel(label), slots, argumentSize)(id)

      case _: Argument =>
        ArgumentSlottedPipe(slots, argumentSize)(id)

      // Currently used for testing only
      case _: MultiNodeIndexSeek =>
        throw new CantCompileQueryException(s"Slotted runtime does not support $plan")

      case _ =>
        fallback.onLeaf(plan)
    }
    pipe.executionContextFactory = SlottedExecutionContextFactory(slots)
    pipe
  }

  override def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = {

    val id = plan.id
    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case ProduceResult(_, columns) =>
        val runtimeColumns = createProjectionsForResult(columns, slots)
        ProduceResultSlottedPipe(source, runtimeColumns)(id)

      case Expand(_, from, dir, types, to, relName, ExpandAll) =>
        val fromSlot = slots(from)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(to)
        ExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, RelationshipTypes(types.toArray), slots)(id)

      case Expand(_, from, dir, types, to, relName, ExpandInto) =>
        val fromSlot = slots(from)
        val relOffset = slots.getLongOffsetFor(relName)
        val toSlot = slots(to)
        ExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, RelationshipTypes(types.toArray), slots)(id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandAll, predicate) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(toName)
        OptionalExpandAllSlottedPipe(source, fromSlot, relOffset, toOffset, dir, RelationshipTypes(types.toArray), slots,
          predicate.map(convertExpressions))(id)

      case OptionalExpand(_, fromName, dir, types, toName, relName, ExpandInto, predicate) =>
        val fromSlot = slots(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toSlot = slots(toName)

        OptionalExpandIntoSlottedPipe(source, fromSlot, relOffset, toSlot, dir, RelationshipTypes(types.toArray), slots,
          predicate.map(convertExpressions))(id)

      case VarExpand(sourcePlan,
      fromName,
      dir,
      projectedDir,
      types,
      toName,
      relName,
      VarPatternLength(min, max),
      expansionMode,
      nodePredicate,
      relationshipPredicate) =>
        val shouldExpandAll = expansionMode match {
          case ExpandAll => true
          case ExpandInto => false
        }
        val fromSlot = slots(fromName)
        val relOffset = slots.getReferenceOffsetFor(relName)
        val toSlot = slots(toName)

        // The node/relationship predicates are evaluated on the source pipeline, not the produced one
        val sourceSlots = physicalPlan.slotConfigurations(sourcePlan.id)
        val tempNodeOffset = expressionSlotForPredicate(nodePredicate)
        val tempRelationshipOffset = expressionSlotForPredicate(relationshipPredicate)
        val argumentSize = SlotConfiguration.Size(sourceSlots.numberOfLongs, sourceSlots.numberOfReferences)
        VarLengthExpandSlottedPipe(source, fromSlot, relOffset, toSlot, dir, projectedDir, RelationshipTypes(types.toArray), min,
          max, shouldExpandAll, slots,
          tempNodeOffset = tempNodeOffset,
          tempRelationshipOffset = tempRelationshipOffset,
          nodePredicate = nodePredicate.map(x => expressionConverters.toCommandExpression(id, x.predicate)).getOrElse(True()),
          relationshipPredicate = relationshipPredicate.map(x => expressionConverters.toCommandExpression(id, x.predicate)).getOrElse(True()),
          argumentSize = argumentSize)(id)

      case Optional(inner, symbols) =>
        val nullableKeys = inner.availableSymbols -- symbols
        val nullableSlots: Array[Slot] = nullableKeys.map(k => slots.get(k).get).toArray
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        OptionalSlottedPipe(source, nullableSlots, slots, argumentSize)(id)

      case Projection(_, expressions) =>
        val toProject = expressions collect {
          case (k, e) if isRefSlotAndNotAlias(slots, k) => k -> e
        }
        ProjectionPipe(source, expressionConverters.toCommandProjection(id, toProject))(id)

      case Create(_, nodes, relationships) =>
        CreateSlottedPipe(
          source,
          nodes.map(n =>
            CreateNodeSlottedCommand(
              slots.getLongOffsetFor(n.idName),
              n.labels.map(LazyLabel.apply),
              n.properties.map(convertExpressions)
            )
          ).toIndexedSeq,
          relationships.map(r =>
            CreateRelationshipSlottedCommand(
              slots.getLongOffsetFor(r.idName),
              SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(r.startNode)),
              LazyType(r.relType.name),
              SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(r.endNode)),
              r.properties.map(convertExpressions),
              r.idName, r.startNode, r.endNode
            )
          ).toIndexedSeq
        )(id)

      case MergeCreateNode(_, idName, labels, properties) =>
        MergeCreateNodeSlottedPipe(
          source,
          CreateNodeSlottedCommand(
            slots.getLongOffsetFor(idName),
            labels.map(LazyLabel.apply),
            properties.map(convertExpressions)
          )
        )(id)

      case MergeCreateRelationship(_, idName, startNode, relType, endNode, properties) =>
        MergeCreateRelationshipSlottedPipe(
          source,
          CreateRelationshipSlottedCommand(
            slots.getLongOffsetFor(idName),
            SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(startNode)),
            LazyType(relType.name),
            SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor(slots(endNode)),
            properties.map(convertExpressions),
            idName, startNode, endNode
          )
        )(id)

      case EmptyResult(_) =>
        EmptyResultPipe(source)(id)

      case DropResult(_) =>
        DropResultPipe(source)(id)

      case UnwindCollection(_, name, expression) =>
        val offset = slots.getReferenceOffsetFor(name)
        UnwindSlottedPipe(source, convertExpressions(expression), offset, slots)(id)

      case Aggregation(_, groupingExpressions, aggregationExpression) =>
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            slots.getReferenceOffsetFor(key) -> convertExpressions(expression).asInstanceOf[AggregationExpression]
        }

        val keys = groupingExpressions.keys.toArray

        val longSlotGroupingValues = keys.collect {
          case key if slots(key).isLongSlot => groupingExpressions(key) match {
            case NodeFromSlot(offset, _) => offset
            case RelationshipFromSlot(offset, _) => offset
            case NullCheckVariable(_, NodeFromSlot(offset, _)) => offset
            case NullCheckVariable(_, RelationshipFromSlot(offset, _)) => offset
            case x => throw new InternalException(s"Cannot build slotted aggregation pipe. Unexpected grouping expression: $x")
          }
        }

        val longSlotGroupingKeys: Array[Int] = keys.collect {
          case x if slots(x).isLongSlot => slots(x).offset
        }

        // Choose the right kind of aggregation table factory based on what grouping columns we have
        val tableFactory =
          if (groupingExpressions.isEmpty) {
            SlottedNonGroupingAggTable.Factory(slots, aggregation)
          } else if (longSlotGroupingValues.length == groupingExpressions.size &&
            longSlotGroupingValues.length == longSlotGroupingKeys.length) {
            // If we are able to use primitive for all incoming and outgoing grouping columns, we can use the more effective
            // Primitive table that leverages that the fact that grouping can be done a single array of longs
            SlottedPrimitiveGroupingAggTable.Factory(slots, longSlotGroupingValues, longSlotGroupingKeys, aggregation)
          } else {
            SlottedGroupingAggTable.Factory(slots, expressionConverters.toGroupingExpression(id, groupingExpressions, Seq.empty), aggregation)
          }
        EagerAggregationPipe(source, tableFactory)(id)

      case OrderedAggregation(_, groupingExpressions, aggregationExpression, orderToLeverage) =>
        val aggregation = aggregationExpression.map {
          case (key, expression) =>
            slots.getReferenceOffsetFor(key) -> convertExpressions(expression).asInstanceOf[AggregationExpression]
        }

        val (orderedGroupingExpressions, unorderedGroupingExpressions) = groupingExpressions.partition { case (_,v) => orderToLeverage.contains(v) }
        val orderedGroupingColumns = expressionConverters.toGroupingExpression(id, orderedGroupingExpressions, orderToLeverage)
        val unorderedGroupingColumns = expressionConverters.toGroupingExpression(id, unorderedGroupingExpressions, orderToLeverage)

        val tableFactory =
          if (unorderedGroupingColumns.isEmpty) {
            SlottedOrderedNonGroupingAggTable.Factory(slots, orderedGroupingColumns, aggregation)
          } else {
            SlottedOrderedGroupingAggTable.Factory(slots, orderedGroupingColumns, unorderedGroupingColumns, aggregation)
          }
        OrderedAggregationPipe(source, tableFactory)(id = id)

      case Distinct(_, groupingExpressions) =>
        chooseDistinctPipe(groupingExpressions, Seq.empty, slots, source, id)

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        chooseDistinctPipe(groupingExpressions, orderToLeverage, slots, source, id)

      case Top(_, sortItems, _) if sortItems.isEmpty => source

      case Top(_, sortItems, SignedDecimalIntegerLiteral("1")) =>
        Top1Pipe(source, SlottedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder(slots, _))))(id = id)

      case Top(_, sortItems, limit) =>
        TopNPipe(source, convertExpressions(limit),
          SlottedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder(slots, _))))(id = id)

      case Limit(_, count, IncludeTies) =>
        (source, count) match {
          case (SortPipe(inner, comparator), SignedDecimalIntegerLiteral("1")) =>
            Top1WithTiesPipe(inner, comparator)(id = id)

          case _ => throw new InternalException("Including ties is only supported for very specific plans")
        }

      // Pipes that do not themselves read/write slots should be fine to use the fallback (non-slot aware pipes)
      case _: Selection |
           _: Limit |
           _: ErrorPlan |
           _: Skip =>
        fallback.onOneChildPlan(plan, source)

      case Sort(_, sortItems) =>
        SortPipe(source, SlottedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder(slots, _))))(id = id)

      case Eager(_) =>
        EagerSlottedPipe(source, slots)(id)

      case _: DeleteNode |
           _: DeleteRelationship |
           _: DeletePath |
           _: DeleteExpression |
           _: DetachDeleteNode |
           _: DetachDeletePath |
           _: DetachDeleteExpression =>
        fallback.onOneChildPlan(plan, source)

      case _: SetLabels |
           _: SetNodeProperty |
           _: SetNodePropertiesFromMap |
           _: SetRelationshipProperty |
           _: SetRelationshipPropertiesFromMap |
           _: SetProperty |
           _: SetPropertiesFromMap |
           _: RemoveLabels =>
        fallback.onOneChildPlan(plan, source)

      case _: LockNodes =>
        fallback.onOneChildPlan(plan, source)

      case _ =>
        fallback.onOneChildPlan(plan, source)
    }
    pipe.executionContextFactory = SlottedExecutionContextFactory(slots)
    pipe
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {

    val slotConfigs = physicalPlan.slotConfigurations
    val id = plan.id
    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)
    val slots = slotConfigs(id)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case Apply(_, _) =>
        ApplySlottedPipe(lhs, rhs)(id)

      case CrossApply(lhsPlan, rhsPlan) =>
        val rhsArgumentSize = physicalPlan.argumentSizes(rhsPlan.leftmostLeaf.id)
        val lhsSlots = slotConfigs(lhsPlan.id)
        CrossApplySlottedPipe(lhs, rhs, lhsSlots.numberOfLongs, lhsSlots.numberOfReferences, slots, rhsArgumentSize)(id)

      case _: AbstractSemiApply |
           _: AbstractSelectOrSemiApply =>
        fallback.onTwoChildPlan(plan, lhs, rhs)

      case RollUpApply(_, rhsPlan, collectionName, identifierToCollect, nullables) =>
        val rhsSlots = slotConfigs(rhsPlan.id)
        val identifierToCollectExpression = createProjectionForIdentifier(rhsSlots)(identifierToCollect)
        val collectionRefSlotOffset = slots.getReferenceOffsetFor(collectionName)
        RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset, identifierToCollectExpression,
          nullables, slots)(id = id)

      case _: CartesianProduct =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val lhsPlan = plan.lhs.get
        val lhsSlots = slotConfigs(lhsPlan.id)

        // Verify the assumption that the only shared slots we have are arguments which are identical on both lhs and rhs.
        // This assumption enables us to use array copy within CartesianProductSlottedPipe.
        checkOnlyWhenAssertionsAreEnabled(verifyOnlyArgumentsAreSharedSlots(plan, physicalPlan))

        CartesianProductSlottedPipe(lhs, rhs, lhsSlots.numberOfLongs, lhsSlots.numberOfReferences, slots, argumentSize)(id)

      case joinPlan: NodeHashJoin =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val nodes = joinPlan.nodes.toArray // Make sure that leftNodes and rightNodes have the same order
        val leftNodes: Array[Int] = nodes.map(k => slots.getLongOffsetFor(k))
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val rightNodes: Array[Int] = nodes.map(k => rhsSlots.getLongOffsetFor(k))

        // Verify the assumption that the argument slots are the same on both sides
        checkOnlyWhenAssertionsAreEnabled(verifyArgumentsAreTheSameOnBothSides(plan, physicalPlan))
        val (longsToCopy, refsToCopy, cachedPropertiesToCopy) = computeSlotsToCopy(rhsSlots, argumentSize, slots)

        if (leftNodes.length == 1) {
          NodeHashJoinSlottedSingleNodePipe(leftNodes(0), rightNodes(0), lhs, rhs, slots, longsToCopy, refsToCopy, cachedPropertiesToCopy)(id)
        } else {
          NodeHashJoinSlottedPipe(leftNodes, rightNodes, lhs, rhs, slots, longsToCopy, refsToCopy, cachedPropertiesToCopy)(id)
        }

      case ValueHashJoin(lhsPlan, _, Equals(lhsAstExp, rhsAstExp)) =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val lhsCmdExp = convertExpressions(lhsAstExp)
        val rhsCmdExp = convertExpressions(rhsAstExp)
        val lhsSlots = slotConfigs(lhsPlan.id)
        val longOffset = lhsSlots.numberOfLongs
        val refOffset = lhsSlots.numberOfReferences

        // Verify the assumption that the only shared slots we have are arguments which are identical on both lhs and rhs.
        // This assumption enables us to use array copy within ValueHashJoin.
        checkOnlyWhenAssertionsAreEnabled(verifyOnlyArgumentsAreSharedSlots(plan, physicalPlan))

        ValueHashJoinSlottedPipe(lhsCmdExp, rhsCmdExp, lhs, rhs, slots, longOffset, refOffset, argumentSize)(id)

      case ConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => slots.get(idName) match {
          case Some(_: LongSlot) => true
          case Some(_: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => slots.getLongOffsetFor(e))
        val refOffsets = refIds.map(e => slots.getReferenceOffsetFor(e))
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, negated = false, slots)(id)

      case AntiConditionalApply(_, _, items) =>
        val (longIds , refIds) = items.partition(idName => slots.get(idName) match {
          case Some(_: LongSlot) => true
          case Some(_: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => slots.getLongOffsetFor(e))
        val refOffsets = refIds.map(e => slots.getReferenceOffsetFor(e))
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets, refOffsets, negated = true, slots)(id)

      case ForeachApply(_, _, variable, expression) =>
        val innerVariableSlot = slots.get(variable).getOrElse(throw new InternalException(s"Foreach variable '$variable' has no slot"))
        ForeachSlottedPipe(lhs, rhs, innerVariableSlot, convertExpressions(expression))(id)

      case Union(_, _) =>
        val lhsSlots = slotConfigs(lhs.id)
        val rhsSlots = slotConfigs(rhs.id)
        UnionSlottedPipe(lhs,
          rhs,
          slots,
          SlottedPipeMapper.computeUnionMapping(lhsSlots, slots),
          SlottedPipeMapper.computeUnionMapping(rhsSlots, slots))(id = id)

      case _: AssertSameNode =>
        fallback.onTwoChildPlan(plan, lhs, rhs)

      case _ =>
        fallback.onTwoChildPlan(plan, lhs, rhs)
    }
    pipe.executionContextFactory = SlottedExecutionContextFactory(slots)
    pipe
  }

  private def computeSlotsToCopy(rhsSlots: SlotConfiguration, argumentSize: SlotConfiguration.Size, slots: SlotConfiguration): (Array[(Int, Int)], Array[(Int, Int)], Array[(Int, Int)]) = {
    val copyLongsFromRHS: mutable.Builder[(Int, Int), ArrayBuffer[(Int, Int)]] = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]
    val copyRefsFromRHS = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]
    val copyCachedPropertiesFromRHS = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]

    // When executing, the LHS will be copied to the first slots in the produced row, and any additional RHS columns that are not
    // part of the join comparison
    rhsSlots.foreachSlotOrdered({
      case (VariableSlotKey(key), LongSlot(offset, _, _)) if offset >= argumentSize.nLongs =>
        copyLongsFromRHS += ((offset, slots.getLongOffsetFor(key)))
      case (VariableSlotKey(key), RefSlot(offset, _, _)) if offset >= argumentSize.nReferences =>
        copyRefsFromRHS += ((offset, slots.getReferenceOffsetFor(key)))
      case (_: VariableSlotKey, _) => // do nothing, already added by lhs
      case (CachedPropertySlotKey(cnp), _) =>
        val offset = rhsSlots.getCachedPropertyOffsetFor(cnp)
        if (offset >= argumentSize.nReferences) {
          copyCachedPropertiesFromRHS += offset -> slots.getCachedPropertyOffsetFor(cnp)
        }
    })

    (copyLongsFromRHS.result().toArray, copyRefsFromRHS.result().toArray, copyCachedPropertiesFromRHS.result().toArray)
  }


  private def chooseDistinctPipe(groupingExpressions: Map[String, internal.expressions.Expression],
                                 orderToLeverage: Seq[internal.expressions.Expression],
                                 slots: SlotConfiguration,
                                 source: Pipe,
                                 id: Id): Pipe = {

    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)

    /**
     * We use these objects to figure out:
     * a) can we use the primitive distinct pipe?
     * b) if we can, what offsets are interesting
     */
    trait DistinctPhysicalOp {
      def addExpression(e: internal.expressions.Expression, ordered: Boolean): DistinctPhysicalOp
    }

    case class AllPrimitive(offsets: Seq[Int], orderedOffsets: Seq[Int]) extends DistinctPhysicalOp {
      override def addExpression(e: internal.expressions.Expression, ordered: Boolean): DistinctPhysicalOp = e match {
        case v: NodeFromSlot =>
          val oo = if (ordered) orderedOffsets :+ v.offset else orderedOffsets
          AllPrimitive(offsets :+ v.offset, oo)
        case v: RelationshipFromSlot =>
          val oo = if (ordered) orderedOffsets :+ v.offset else orderedOffsets
          AllPrimitive(offsets :+ v.offset, oo)
        case _ =>
          References
      }
    }

    object References extends DistinctPhysicalOp {
      override def addExpression(e: internal.expressions.Expression, ordered: Boolean): DistinctPhysicalOp = References
    }

    val runtimeProjections: Map[Slot, commands.expressions.Expression] = groupingExpressions.map {
      case (key, expression) =>
        slots(key) -> convertExpressions(expression)
    }

    val physicalDistinctOp = groupingExpressions.foldLeft[DistinctPhysicalOp](AllPrimitive(Seq.empty, Seq.empty)) {
      case (acc: DistinctPhysicalOp, (_, expression)) =>
        acc.addExpression(expression, orderToLeverage.contains(expression))
    }

    physicalDistinctOp match {
      case AllPrimitive(offsets, orderedOffsets) if offsets.size == 1 && orderedOffsets.isEmpty =>
        val (toSlot, runtimeExpression) = runtimeProjections.head
        DistinctSlottedSinglePrimitivePipe(source, slots, toSlot, offsets.head, runtimeExpression)(id)

      case AllPrimitive(offsets, orderedOffsets) if offsets.size == 1 && orderedOffsets == offsets =>
        val (toSlot, runtimeExpression) = runtimeProjections.head
        OrderedDistinctSlottedSinglePrimitivePipe(source, slots, toSlot, offsets.head, runtimeExpression)(id)

      case AllPrimitive(offsets, orderedOffsets) =>
        if (orderToLeverage.isEmpty) {
          DistinctSlottedPrimitivePipe(
            source,
            slots,
            offsets.sorted.toArray,
            expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        } else if (orderedOffsets == offsets) {
          AllOrderedDistinctSlottedPrimitivePipe(
            source,
            slots,
            offsets.sorted.toArray,
            expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        } else {
          OrderedDistinctSlottedPrimitivePipe(
            source,
            slots,
            offsets.sorted.toArray,
            orderedOffsets.sorted.toArray,
            expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        }

      case References =>
        if (orderToLeverage.isEmpty) {
          DistinctSlottedPipe(source, slots, expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        } else if (groupingExpressions.values.forall(orderToLeverage.contains)) {
          AllOrderedDistinctSlottedPipe(source, slots, expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        } else {
          OrderedDistinctSlottedPipe(source, slots, expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        }
    }
  }

  // Verifies the assumption that all shared slots are arguments with slot offsets within the first argument size number of slots
  // and the number of shared slots are identical to the argument size.
  private def verifyOnlyArgumentsAreSharedSlots(plan: LogicalPlan, physicalPlan: PhysicalPlan): Boolean = {
    val argumentSize = physicalPlan.argumentSizes(plan.id)
    val lhsPlan = plan.lhs.get
    val rhsPlan = plan.rhs.get
    val lhsSlots = physicalPlan.slotConfigurations(lhsPlan.id)
    val rhsSlots = physicalPlan.slotConfigurations(rhsPlan.id)
    val sharedSlots =
      rhsSlots.filterSlots({
        case (VariableSlotKey(k), _) =>  lhsSlots.get(k).isDefined
        case (CachedPropertySlotKey(k), slot) => slot.offset < argumentSize.nReferences && lhsSlots.hasCachedPropertySlot(k)
      })

    val (sharedLongSlots, sharedRefSlots) = sharedSlots.partition(_.isLongSlot)

    def checkSharedSlots(slots: Seq[Slot], expectedSlots: Int): Boolean = {
      val sorted = slots.sortBy(_.offset)
      var prevOffset = -1
      for (slot <- sorted) {
        if (slot.offset == prevOffset ||      // if we have aliases for the same slot, we will get it again
          slot.offset == prevOffset + 1) {  // otherwise we expect the next shared slot to sit at the next offset
          prevOffset = slot.offset
        } else {
          return false
        }
      }
      prevOffset + 1 == expectedSlots
    }

    val longSlotsOk = checkSharedSlots(sharedLongSlots.toSeq, argumentSize.nLongs)
    val refSlotsOk = checkSharedSlots(sharedRefSlots.toSeq, argumentSize.nReferences)

    if (!longSlotsOk || !refSlotsOk) {
      val longSlotsMessage = if (longSlotsOk) "" else s"#long arguments=${argumentSize.nLongs} shared long slots: $sharedLongSlots "
      val refSlotsMessage = if (refSlotsOk) "" else s"#ref arguments=${argumentSize.nReferences} shared ref slots: $sharedRefSlots "
      throw new InternalException(s"Unexpected slot configuration. Shared slots not only within argument size: $longSlotsMessage$refSlotsMessage")
    }

    true
  }

  private def verifyArgumentsAreTheSameOnBothSides(plan: LogicalPlan, physicalPlan: PhysicalPlan): Boolean = {
    val argumentSize = physicalPlan.argumentSizes(plan.id)
    val lhsPlan = plan.lhs.get
    val rhsPlan = plan.rhs.get
    val lhsSlots = physicalPlan.slotConfigurations(lhsPlan.id)
    val rhsSlots = physicalPlan.slotConfigurations(rhsPlan.id)

    val lhsArgLongSlots = mutable.ArrayBuffer.empty[(String, Slot)]
    val lhsArgRefSlots = mutable.ArrayBuffer.empty[(String, Slot)]
    val rhsArgLongSlots = mutable.ArrayBuffer.empty[(String, Slot)]
    val rhsArgRefSlots = mutable.ArrayBuffer.empty[(String, Slot)]
    lhsSlots.foreachSlot({
      case (VariableSlotKey(key), slot)  =>
        if (slot.isLongSlot && slot.offset < argumentSize.nLongs) {
          lhsArgLongSlots += (key -> slot)
        }  else if (!slot.isLongSlot && slot.offset < argumentSize.nReferences) {
          lhsArgRefSlots += (key -> slot)
        }
      case (CachedPropertySlotKey(key), slot)  =>
        if (slot.offset < argumentSize.nReferences) {
          lhsArgRefSlots += (key.asCanonicalStringVal -> slot)
        }
    })
    rhsSlots.foreachSlot({
      case (VariableSlotKey(key), slot)  =>
        if (slot.isLongSlot && slot.offset < argumentSize.nLongs) {
          rhsArgLongSlots += (key -> slot)
        }  else if (!slot.isLongSlot && slot.offset < argumentSize.nReferences) {
          rhsArgRefSlots += (key -> slot)
        }
      case (CachedPropertySlotKey(key), slot) =>
        if (slot.offset < argumentSize.nReferences) {
          rhsArgRefSlots += (key.asCanonicalStringVal -> slot)
        }
    })

    def sameSlotsInOrder(a: Seq[(String, Slot)], b: Seq[(String, Slot)]): Boolean =
      a.sortBy(_._1).zip(b.sortBy(_._1)) forall {
        case ((k1, slot1), (k2, slot2)) =>
          k1 == k2 && slot1.offset == slot2.offset && slot1.isTypeCompatibleWith(slot2)
      }

    val longSlotsOk = lhsArgLongSlots.size == rhsArgLongSlots.size && sameSlotsInOrder(lhsArgLongSlots, rhsArgLongSlots)
    val refSlotsOk = lhsArgRefSlots.size == rhsArgRefSlots.size && sameSlotsInOrder(lhsArgRefSlots, rhsArgRefSlots)

    if (!longSlotsOk || !refSlotsOk) {
      val longSlotsMessage = if (longSlotsOk) "" else s"#long arguments=${argumentSize.nLongs} lhs: $lhsArgLongSlots rhs: $rhsArgLongSlots "
      val refSlotsMessage = if (refSlotsOk) "" else s"#ref arguments=${argumentSize.nReferences} lhs: $lhsArgRefSlots rhs: $rhsArgRefSlots "
      throw new InternalException(s"Unexpected slot configuration. Arguments differ between lhs and rhs: $longSlotsMessage$refSlotsMessage")
    }
    true
  }
}

object SlottedPipeMapper {

  def createProjectionsForResult(columns: Seq[String], slots: SlotConfiguration): Seq[(String, Expression)] = {
    val runtimeColumns: Seq[(String, commands.expressions.Expression)] =
      columns.map(createProjectionForIdentifier(slots))
    runtimeColumns
  }

  private def createProjectionForIdentifier(slots: SlotConfiguration)(identifier: String): (String, Expression) = {
    val slot = slots.get(identifier).getOrElse(
      throw new InternalException(s"Did not find `$identifier` in the slot configuration")
    )
    identifier -> SlottedPipeMapper.projectSlotExpression(slot)
  }

  private def projectSlotExpression(slot: Slot): commands.expressions.Expression = slot match {
    case LongSlot(offset, false, CTNode) =>
      slotted.expressions.NodeFromSlot(offset)
    case LongSlot(offset, true, CTNode) =>
      slotted.expressions.NullCheck(offset, slotted.expressions.NodeFromSlot(offset))
    case LongSlot(offset, false, CTRelationship) =>
      slotted.expressions.RelationshipFromSlot(offset)
    case LongSlot(offset, true, CTRelationship) =>
      slotted.expressions.NullCheck(offset, slotted.expressions.RelationshipFromSlot(offset))

    case RefSlot(offset, _, _) =>
      slotted.expressions.ReferenceFromSlot(offset)

    case _ =>
      throw new InternalException(s"Do not know how to project $slot")
  }

  //compute mapping from incoming to outgoing pipe line, the slot order may differ
  //between the output and the input (lhs and rhs) and it may be the case that
  //we have a reference slot in the output but a long slot on one of the inputs,
  //e.g. MATCH (n) RETURN n UNION RETURN 42 AS n
  def computeUnionMapping(in: SlotConfiguration, out: SlotConfiguration): RowMapping = {
    val mapSlots: Iterable[(ExecutionContext, ExecutionContext, QueryState) => Unit] = in.mapSlot(
      onVariable = {
        case (k, inSlot: LongSlot) =>
          out.get(k) match {
            // The output does not have a slot for this, so we don't need to copy
            case None => (_, _, _) => ()
            case Some(l: LongSlot) =>
              (in, out, _) => out.setLongAt(l.offset, in.getLongAt(inSlot.offset))
            case Some(r: RefSlot) =>
              //here we must map the long slot to a reference slot
              val projectionExpression = projectSlotExpression(inSlot) // Pre-compute projection expression
              (in, out, state) => out.setRefAt(r.offset, projectionExpression(in, state))
          }
        case (k, inSlot: RefSlot) =>
          // This means out must be a ref slot as well, if it exists, otherwise slot allocation was wrong
          out.get(k) match {
            // The output does not have a slot for this, so we don't need to copy
            case None => (_, _, _) => ()
            case Some(l: LongSlot) =>
              throw new IllegalStateException(s"Expected Union output slot to be a refslot but was: $l")
            case Some(r: RefSlot) =>
              (in, out, _) => out.setRefAt(r.offset, in.getRefAt(inSlot.offset))
          }
      }, onCachedProperty = {
        case (cachedProp, inRefSlot) =>
          out.getCachedPropertySlot(cachedProp) match {
            case Some(outRefSlot) =>
              // Copy the cached property if the output has it as well
              (in, out, _) => out.setCachedPropertyAt(outRefSlot.offset, in.getCachedPropertyAt(inRefSlot.offset))
            case None =>
              // Otherwise do nothing
              (_, _, _) => ()
          }
      })
    //Apply all transformations
    (incoming: ExecutionContext, outgoing: ExecutionContext, state: QueryState) => {
      mapSlots.foreach(f => f(incoming, outgoing, state))
    }
  }

  def translateColumnOrder(slots: SlotConfiguration, s: plans.ColumnOrder): slotted.ColumnOrder = s match {
    case plans.Ascending(name) =>
      slots.get(name) match {
        case Some(slot) => slotted.Ascending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
    case plans.Descending(name) =>
      slots.get(name) match {
        case Some(slot) => slotted.Descending(slot)
        case None => throw new InternalException(s"Did not find `$name` in the pipeline information")
      }
  }
}
