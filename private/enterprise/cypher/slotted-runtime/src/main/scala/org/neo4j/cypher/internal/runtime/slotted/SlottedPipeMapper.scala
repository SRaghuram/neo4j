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
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
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
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
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
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.isRefSlotAndNotAlias
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckVariable
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipFromSlot
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialSortPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTop1Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTop1WithTiesPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialTopNPipe
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
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.DistinctAllPrimitive
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.DistinctWithReferences
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.computeSlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionForIdentifier
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.createProjectionsForResult
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.findDistinctPhysicalOp
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.partitionGroupingExpressions
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.symbolsToSlots
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.translateColumnOrder
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedNonGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedOrderedGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedOrderedNonGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.aggregation.SlottedPrimitiveGroupingAggTable
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllNodesScanSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllOrderedDistinctSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.AllOrderedDistinctSlottedPrimitivePipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.AntiConditionalApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ArgumentSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CartesianProductSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ConditionalApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateNodeSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateRelationshipSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateSlottedPipe
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
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
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
import org.neo4j.cypher.internal.runtime.slotted.pipes.SelectOrSemiApplySlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnwindSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.ValueHashJoinSlottedPipe
import org.neo4j.cypher.internal.runtime.slotted.pipes.VarLengthExpandSlottedPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException

import scala.collection.mutable

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
        AllNodesScanSlottedPipe(column, slots)(id)

      case NodeIndexScan(column, label, properties, _, indexOrder) =>
        NodeIndexScanSlottedPipe(column, label, properties.map(SlottedIndexedProperty(column, _, slots)),
          indexRegistrator.registerQueryIndex(label, properties), indexOrder, slots)(id)

      case NodeIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, properties.map(SlottedIndexedProperty(column, _, slots)).toIndexedSeq,
          indexRegistrator.registerQueryIndex(label, properties), valueExpr.map(convertExpressions), indexSeekMode, indexOrder, slots)(id)

      case NodeUniqueIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekSlottedPipe(column, label, properties.map(SlottedIndexedProperty(column, _, slots)).toIndexedSeq,
          indexRegistrator.registerQueryIndex(label, properties), valueExpr.map(convertExpressions), indexSeekMode, indexOrder, slots)(id = id)

      case NodeByLabelScan(column, label, _, indexOrder) =>
        indexRegistrator.registerLabelScan()
        NodesByLabelScanSlottedPipe(column, LazyLabel(label), slots, indexOrder)(id)

      case _: Argument =>
        ArgumentSlottedPipe()(id)

      // Currently used for testing only
      case _: MultiNodeIndexSeek =>
        throw new CantCompileQueryException(s"Slotted runtime does not support $plan")

      case _ =>
        fallback.onLeaf(plan)
    }
    pipe.rowFactory = SlottedCypherRowFactory(slots, argumentSize)
    pipe
  }

  override def onOneChildPlan(plan: LogicalPlan, source: Pipe): Pipe = {

    val id = plan.id
    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)
    val slots = physicalPlan.slotConfigurations(id)
    // some operators will overwrite this value
    var argumentSize = physicalPlan.argumentSizes.getOrElse(id, SlotConfiguration.Size.zero)
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
        argumentSize = SlotConfiguration.Size(sourceSlots.numberOfLongs, sourceSlots.numberOfReferences)
        VarLengthExpandSlottedPipe(source, fromSlot, relOffset, toSlot, dir, projectedDir, RelationshipTypes(types.toArray), min,
          max, shouldExpandAll, slots,
          tempNodeOffset = tempNodeOffset,
          tempRelationshipOffset = tempRelationshipOffset,
          nodePredicate = nodePredicate.map(x => expressionConverters.toCommandExpression(id, x.predicate)).getOrElse(True()),
          relationshipPredicate = relationshipPredicate.map(x => expressionConverters.toCommandExpression(id, x.predicate)).getOrElse(True()),
          argumentSize = argumentSize)(id)

      case Optional(inner, symbols) =>
        val nullableSlots = symbolsToSlots(inner.availableSymbols -- symbols, slots)
        OptionalSlottedPipe(source, nullableSlots)(id)

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

        val (orderedGroupingColumns, unorderedGroupingColumns) = partitionGroupingExpressions(expressionConverters, groupingExpressions, orderToLeverage, id)
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

      case PartialTop(_, _, stillToSortSuffix, _, _) if stillToSortSuffix.isEmpty => source

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, SignedDecimalIntegerLiteral("1"), _) =>
        PartialTop1Pipe(source, SlottedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder(slots, _)).toList),
          SlottedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder(slots, _)).toList))(id = id)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, skipSortingPrefixLength) =>
        PartialTopNPipe(source,
          convertExpressions(limit),
          skipSortingPrefixLength.map(convertExpressions),
          SlottedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder(slots, _)).toList),
          SlottedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder(slots, _)).toList))(id = id)

      case Limit(_, count, IncludeTies) =>
        (source, count) match {
          case (SortPipe(inner, comparator), SignedDecimalIntegerLiteral("1")) =>
            Top1WithTiesPipe(inner, comparator)(id = id)
          case (PartialSortPipe(inner, prefixComparator, suffixComparator, None), SignedDecimalIntegerLiteral("1")) =>
            PartialTop1WithTiesPipe(inner, prefixComparator, suffixComparator)(id = id)

          case _ => throw new InternalException("Including ties is only supported for very specific plans")
        }

      // Pipes that do not themselves read/write slots should be fine to use the fallback (non-slot aware pipes)
      case _: Selection |
           _: Limit |
           _: ErrorPlan |
           _: Skip |
           _: NonFuseable =>
        fallback.onOneChildPlan(plan, source)

      case Sort(_, sortItems) =>
        SortPipe(source, SlottedExecutionContextOrdering.asComparator(sortItems.map(translateColumnOrder(slots, _))))(id = id)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, skipSortingPrefixLength) =>
        PartialSortPipe(source,
          SlottedExecutionContextOrdering.asComparator(alreadySortedPrefix.map(translateColumnOrder(slots, _))),
          SlottedExecutionContextOrdering.asComparator(stillToSortSuffix.map(translateColumnOrder(slots, _))),
          skipSortingPrefixLength.map(convertExpressions)
        )(id = id)

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
    pipe.rowFactory = SlottedCypherRowFactory(slots, argumentSize)
    pipe
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {

    val slotConfigs = physicalPlan.slotConfigurations
    val id = plan.id
    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)
    val slots = slotConfigs(id)
    // some plans (e.g. Apply) have no argument size attribute set
    val argumentSize = physicalPlan.argumentSizes.getOrElse(id, SlotConfiguration.Size.zero)
    generateSlotAccessorFunctions(slots)

    val pipe = plan match {
      case Apply(_, _) =>
        ApplySlottedPipe(lhs, rhs)(id)

      case RollUpApply(_, rhsPlan, collectionName, identifierToCollect) =>
        val rhsSlots = slotConfigs(rhsPlan.id)
        val identifierToCollectExpression = createProjectionForIdentifier(rhsSlots)(identifierToCollect)
        val collectionRefSlotOffset = slots.getReferenceOffsetFor(collectionName)
        RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset, identifierToCollectExpression, slots)(id = id)

      case _: CartesianProduct =>
        val lhsPlan = plan.lhs.get
        val lhsSlots = slotConfigs(lhsPlan.id)

        // Verify the assumption that the only shared slots we have are arguments which are identical on both lhs and rhs.
        // This assumption enables us to use array copy within CartesianProductSlottedPipe.
        checkOnlyWhenAssertionsAreEnabled(verifyOnlyArgumentsAreSharedSlots(plan, physicalPlan))

        CartesianProductSlottedPipe(lhs, rhs, lhsSlots.numberOfLongs, lhsSlots.numberOfReferences, slots, argumentSize)(id)

      case joinPlan: NodeHashJoin =>
        val nodes = joinPlan.nodes.toArray // Make sure that leftNodes and rightNodes have the same order
        val leftNodes = KeyOffsets.create(slots, nodes)
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val rightNodes = KeyOffsets.create(rhsSlots, nodes)

        // Verify the assumption that the argument slots are the same on both sides
        checkOnlyWhenAssertionsAreEnabled(verifyArgumentsAreTheSameOnBothSides(plan, physicalPlan))
        val rhsSlotMappings = computeSlotMappings(rhsSlots, argumentSize, slots)

        if (leftNodes.isSingle) {
          NodeHashJoinSlottedSingleNodePipe(leftNodes.asSingle, rightNodes.asSingle, lhs, rhs, slots, rhsSlotMappings)(id)
        } else {
          NodeHashJoinSlottedPipe(leftNodes, rightNodes, lhs, rhs, slots, rhsSlotMappings)(id)
        }

      case ValueHashJoin(lhsPlan, rhsPlan, Equals(lhsAstExp, rhsAstExp)) =>
        val lhsCmdExp = convertExpressions(lhsAstExp)
        val rhsCmdExp = convertExpressions(rhsAstExp)
        val rhsSlots = slotConfigs(rhsPlan.id)

        // Verify the assumption that the only shared slots we have are arguments which are identical on both lhs and rhs.
        // This assumption enables us to use array copy within ValueHashJoin.
        checkOnlyWhenAssertionsAreEnabled(verifyArgumentsAreTheSameOnBothSides(plan, physicalPlan))
        val rhsSlotMappings = computeSlotMappings(rhsSlots, argumentSize, slots)

        ValueHashJoinSlottedPipe(lhsCmdExp, rhsCmdExp, lhs, rhs, slots, rhsSlotMappings)(id)

      case ConditionalApply(left, right, items) =>
        val (longIds , refIds) = items.partition(idName => slots.get(idName) match {
          case Some(_: LongSlot) => true
          case Some(_: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => slots.getLongOffsetFor(e))
        val refOffsets = refIds.map(e => slots.getReferenceOffsetFor(e))
        val nullableSlots = symbolsToSlots(right.availableSymbols -- left.availableSymbols, slots)
        ConditionalApplySlottedPipe(lhs, rhs, longOffsets.toArray, refOffsets.toArray, slots, nullableSlots)(id)

      case AntiConditionalApply(left, right, items) =>
        val (longIds , refIds) = items.partition(idName => slots.get(idName) match {
          case Some(_: LongSlot) => true
          case Some(_: RefSlot) => false
          case _ => throw new InternalException("We expect only an existing LongSlot or RefSlot here")
        })
        val longOffsets = longIds.map(e => slots.getLongOffsetFor(e))
        val refOffsets = refIds.map(e => slots.getReferenceOffsetFor(e))
        val nullableSlots = symbolsToSlots(right.availableSymbols -- left.availableSymbols, slots)
        AntiConditionalApplySlottedPipe(lhs, rhs, longOffsets.toArray, refOffsets.toArray, slots, nullableSlots)(id)

      case ForeachApply(_, _, variable, expression) =>
        val innerVariableSlot = slots.get(variable).getOrElse(throw new InternalException(s"Foreach variable '$variable' has no slot"))
        ForeachSlottedPipe(lhs, rhs, innerVariableSlot, convertExpressions(expression))(id)

      case SelectOrSemiApply(_, _, expression) =>
        SelectOrSemiApplySlottedPipe(lhs, rhs, expressionConverters.toCommandExpression(id, expression), negated = false, slots)(id)

      case SelectOrAntiSemiApply(_, _, expression) =>
        SelectOrSemiApplySlottedPipe(lhs, rhs, expressionConverters.toCommandExpression(id, expression), negated = true, slots)(id)

      case Union(_, _) =>
        val lhsSlots = slotConfigs(lhs.id)
        val rhsSlots = slotConfigs(rhs.id)
        UnionSlottedPipe(lhs,
          rhs,
          slots,
          SlottedPipeMapper.computeUnionRowMapping(lhsSlots, slots),
          SlottedPipeMapper.computeUnionRowMapping(rhsSlots, slots))(id = id)

      case _: AssertSameNode =>
        fallback.onTwoChildPlan(plan, lhs, rhs)

      case _ =>
        fallback.onTwoChildPlan(plan, lhs, rhs)
    }
    pipe.rowFactory = SlottedCypherRowFactory(slots, argumentSize)
    pipe
  }

  private def chooseDistinctPipe(groupingExpressions: Map[String, internal.expressions.Expression],
                                 orderToLeverage: Seq[internal.expressions.Expression],
                                 slots: SlotConfiguration,
                                 source: Pipe,
                                 id: Id): Pipe = {
    val convertExpressions = (e: internal.expressions.Expression) => expressionConverters.toCommandExpression(id, e)

    val runtimeProjections: Map[Slot, commands.expressions.Expression] = groupingExpressions.map {
      case (key, expression) =>
        slots(key) -> convertExpressions(expression)
    }

    val physicalDistinctOp = findDistinctPhysicalOp(groupingExpressions, orderToLeverage)

    physicalDistinctOp match {
      case DistinctAllPrimitive(offsets, orderedOffsets) if offsets.size == 1 && orderedOffsets.isEmpty =>
        val (toSlot, runtimeExpression) = runtimeProjections.head
        DistinctSlottedSinglePrimitivePipe(source, slots, toSlot, offsets.head, runtimeExpression)(id)

      case DistinctAllPrimitive(offsets, orderedOffsets) if offsets.size == 1 && orderedOffsets == offsets =>
        val (toSlot, runtimeExpression) = runtimeProjections.head
        OrderedDistinctSlottedSinglePrimitivePipe(source, slots, toSlot, offsets.head, runtimeExpression)(id)

      case DistinctAllPrimitive(offsets, orderedOffsets) =>
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
            orderedOffsets.sorted.toArray,
            offsets.filterNot(orderedOffsets.contains(_)).sorted.toArray,
            expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        }

      case DistinctWithReferences =>
        if (orderToLeverage.isEmpty) {
          DistinctSlottedPipe(source, slots, expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        } else if (groupingExpressions.values.forall(orderToLeverage.contains)) {
          AllOrderedDistinctSlottedPipe(source, slots, expressionConverters.toGroupingExpression(id, groupingExpressions, orderToLeverage))(id)
        } else {
          val (ordered, unordered) = partitionGroupingExpressions(expressionConverters, groupingExpressions, orderToLeverage, id)
          OrderedDistinctSlottedPipe(source, slots, ordered, unordered)(id)
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
    lhsSlots.foreachSlotAndAliases({
      case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases)  =>
        if (slot.isLongSlot && slot.offset < argumentSize.nLongs) {
          lhsArgLongSlots += (key -> slot)
          aliases.foreach(alias => lhsArgLongSlots += (alias -> slot))
        }  else if (!slot.isLongSlot && slot.offset < argumentSize.nReferences) {
          lhsArgRefSlots += (key -> slot)
          aliases.foreach(alias => lhsArgRefSlots += (alias -> slot))
        }
      case SlotWithKeyAndAliases(CachedPropertySlotKey(key), slot, _)  =>
        if (slot.offset < argumentSize.nReferences) {
          lhsArgRefSlots += (key.asCanonicalStringVal -> slot)
        }
    })
    rhsSlots.foreachSlotAndAliases({
      case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases)  =>
        if (slot.isLongSlot && slot.offset < argumentSize.nLongs) {
          rhsArgLongSlots += (key -> slot)
          aliases.foreach(alias => rhsArgLongSlots += (alias -> slot))
        }  else if (!slot.isLongSlot && slot.offset < argumentSize.nReferences) {
          rhsArgRefSlots += (key -> slot)
          aliases.foreach(alias => rhsArgRefSlots += (alias -> slot))
        }
      case SlotWithKeyAndAliases(CachedPropertySlotKey(key), slot, _) =>
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

  case class SlotMappings(
    longMappings: Array[(Int, Int)],
    refMappings: Array[(Int, Int)],
    cachedPropertyMappings: Array[(Int, Int)],
  )

  /** Computes slot mappings from one slot configuration to another
   *
   * Given the values:
   *                  0,    1, 2, 3, 4, 5, 6      0, 1,  2,  3,  4
   *  fromSlots = [arg1, arg2, a, b, c]       [arg3, k, p1,  m, p2]
   *  toSlots =   [arg1, arg2, a, c, d, e, b] [arg3, k,  m, p1, p2]
   *  argumentSize.nLongs = 2
   *  argumentSize.nReferences = 1
   *
   * it produces the output:
   *  longMappings           2->2, 3->6, 4->3
   *  refMappings            1->1, 3->2
   *  cachedPropertyMappings 2->3, 4->4
   * */
  def computeSlotMappings(fromSlots: SlotConfiguration, argumentSize: SlotConfiguration.Size, toSlots: SlotConfiguration): SlotMappings = {
    val longMappings = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]
    val refMappings = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]
    val cachedPropertyMappings = collection.mutable.ArrayBuffer.newBuilder[(Int,Int)]

    fromSlots.foreachSlotAndAliasesOrdered({
      case SlotWithKeyAndAliases(VariableSlotKey(key), LongSlot(offset, _, _), _) if offset >= argumentSize.nLongs =>
        longMappings += ((offset, toSlots.getLongOffsetFor(key)))
      case SlotWithKeyAndAliases(VariableSlotKey(key), RefSlot(offset, _, _), _) if offset >= argumentSize.nReferences =>
        refMappings += ((offset, toSlots.getReferenceOffsetFor(key)))
      case SlotWithKeyAndAliases(_: VariableSlotKey, _, _) => // do nothing, part of arguments
      case SlotWithKeyAndAliases(_: ApplyPlanSlotKey, _, _) => // do nothing, part of arguments
      case SlotWithKeyAndAliases(CachedPropertySlotKey(cnp), _, _) =>
        val offset = fromSlots.getCachedPropertyOffsetFor(cnp)
        if (offset >= argumentSize.nReferences) {
          cachedPropertyMappings += offset -> toSlots.getCachedPropertyOffsetFor(cnp)
        }
    })

    SlotMappings(longMappings.result().toArray, refMappings.result().toArray, cachedPropertyMappings.result().toArray)
  }

  /**
   * We use these objects to figure out:
   * a) can we use the primitive distinct pipe?
   * b) if we can, what offsets are interesting
   */
  sealed trait DistinctPhysicalOp {
    def addExpression(e: internal.expressions.Expression, ordered: Boolean): DistinctPhysicalOp
  }

  case class DistinctAllPrimitive(offsets: Seq[Int], orderedOffsets: Seq[Int]) extends DistinctPhysicalOp {
    override def addExpression(e: internal.expressions.Expression, ordered: Boolean): DistinctPhysicalOp = e match {
      case v: NodeFromSlot =>
        val oo = if (ordered) orderedOffsets :+ v.offset else orderedOffsets
        DistinctAllPrimitive(offsets :+ v.offset, oo)
      case v: RelationshipFromSlot =>
        val oo = if (ordered) orderedOffsets :+ v.offset else orderedOffsets
        DistinctAllPrimitive(offsets :+ v.offset, oo)
      case _ =>
        DistinctWithReferences
    }
  }

  case object DistinctWithReferences extends DistinctPhysicalOp {
    override def addExpression(e: internal.expressions.Expression, ordered: Boolean): DistinctPhysicalOp = DistinctWithReferences
  }

  def findDistinctPhysicalOp(groupingExpressions: Map[String, internal.expressions.Expression], orderToLeverage: Seq[internal.expressions.Expression]): DistinctPhysicalOp = {
    groupingExpressions.foldLeft[DistinctPhysicalOp](DistinctAllPrimitive(Seq.empty, Seq.empty)) {
      case (acc: DistinctPhysicalOp, (_, expression)) =>
        acc.addExpression(expression, orderToLeverage.contains(expression))
    }
  }

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

  /**
   * A mapping from one input slot configuration to the output slot configuration that dictates what to copy in a Union.
   */
  sealed trait UnionSlotMapping
  case class CopyLongSlot(sourceOffset: Int, targetOffset: Int) extends UnionSlotMapping
  case class CopyRefSlot(sourceOffset: Int, targetOffset: Int) extends UnionSlotMapping
  case class CopyCachedProperty(sourceOffset: Int, targetOffset: Int) extends UnionSlotMapping
  case class ProjectLongToRefSlot(sourceSlot: LongSlot, targetOffset: Int) extends UnionSlotMapping

  /**
   * A [[UnionSlotMapping]] is a function that actually performs the copying.
   */
  trait RowMapping extends {
    def mapRows(incoming: CypherRow, outgoing: CypherRow, state: QueryState): Unit
  }

  /**
   * compute mapping from incoming to outgoing pipeline, the slot order may differ
   * between the output and the input (lhs and rhs) and it may be the case that
   * we have a reference slot in the output but a long slot on one of the inputs,
   * e.g. MATCH (n) RETURN n UNION RETURN 42 AS n
   */
  def computeUnionSlotMappings(in: SlotConfiguration, out: SlotConfiguration): Iterable[UnionSlotMapping] = {
    in.mapSlotsDoNotSkipAliases {
      case (VariableSlotKey(k), inLongSlot: LongSlot) =>
        out.get(k).map {
          case l: LongSlot => CopyLongSlot(inLongSlot.offset, l.offset)
          case r: RefSlot => ProjectLongToRefSlot(inLongSlot, r.offset)
        }
      case (VariableSlotKey(k), inSlot: RefSlot) =>
        // This means out must be a ref slot as well, if it exists, otherwise slot allocation was wrong
        out.get(k).map {
          case l: LongSlot => throw new IllegalStateException(s"Expected Union output slot to be a refslot but was: $l")
          case r: RefSlot => CopyRefSlot(inSlot.offset, r.offset)
        }
      case (CachedPropertySlotKey(cachedProp), inRefSlot) =>
        out.getCachedPropertySlot(cachedProp).map {
          outRefSlot => CopyCachedProperty(inRefSlot.offset, outRefSlot.offset)
        }
      case (ApplyPlanSlotKey(id), slot) =>
        out.getArgumentSlot(id).map {
          outArgumentSlot => CopyLongSlot(slot.offset, outArgumentSlot.offset)
        }
    }.flatten
  }

  /**
   * Compute the [[RowMapping]] from [[UnionSlotMapping]]s, which can be then applied to Rows at runtime.
   */
  def computeUnionRowMapping(in: SlotConfiguration, out: SlotConfiguration): RowMapping = {
    val mappings = computeUnionSlotMappings(in, out)

    // Collect all 4 types of mappings
    case class ProjectExpressionToRefSlot(expression: Expression, targetOffset: Int)
    val copyLongSlots = mappings.collect { case c: CopyLongSlot => c}.toArray.sortBy(_.targetOffset)
    val copyRefSlots = mappings.collect { case c: CopyRefSlot => c}.toArray.sortBy(_.targetOffset)
    val copyCachedProperties = mappings.collect { case c: CopyCachedProperty => c }.toArray.sortBy(_.targetOffset)
    val projectExpressionToRefSlots = mappings.collect {
      case c: ProjectLongToRefSlot =>
        // Pre-compute projection expression
        val projectionExpression = projectSlotExpression(c.sourceSlot)
        ProjectExpressionToRefSlot(projectionExpression, c.targetOffset)
    }.toArray.sortBy(_.targetOffset)

    //Apply all transformations
    (in, out, state) => {
      var i = 0
      while (i < copyLongSlots.length) {
        val x = copyLongSlots(i)
        out.setLongAt(x.targetOffset, in.getLongAt(x.sourceOffset))
        i += 1
      }
      i = 0
      while (i < copyRefSlots.length) {
        val x = copyRefSlots(i)
        out.setRefAt(x.targetOffset, in.getRefAt(x.sourceOffset))
        i += 1
      }
      i = 0
      while (i < copyCachedProperties.length) {
        val x = copyCachedProperties(i)
        out.setCachedPropertyAt(x.targetOffset, in.getCachedPropertyAt(x.sourceOffset))
        i += 1
      }
      i = 0
      while (i < projectExpressionToRefSlots.length) {
        val x = projectExpressionToRefSlots(i)
        out.setRefAt(x.targetOffset, x.expression(in, state))
        i += 1
      }
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

  def partitionGroupingExpressions(expressionConverters: ExpressionConverters,
                                   groupingExpressions: Map[String, internal.expressions.Expression],
                                   orderToLeverage: Seq[internal.expressions.Expression],
                                   id: Id): (GroupingExpression, GroupingExpression) = {
    val (orderedGroupingExpressions, unorderedGroupingExpressions) = groupingExpressions.partition { case (_, v) => orderToLeverage.contains(v) }
    val orderedGroupingColumns = expressionConverters.toGroupingExpression(id, orderedGroupingExpressions, orderToLeverage)
    val unorderedGroupingColumns = expressionConverters.toGroupingExpression(id, unorderedGroupingExpressions, orderToLeverage)
    (orderedGroupingColumns, unorderedGroupingColumns)
  }

  def symbolsToSlots(symbols: Set[String], slotConfiguration: SlotConfiguration): Array[Slot] =
    symbols.map(k => slotConfiguration.get(k).get).toArray
}
