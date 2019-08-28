/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.{DoNotIncludeTies, ExpandAll, LogicalPlan}
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.VariablePredicates.expressionSlotForPredicate
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlottedIndexedProperty, _}
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.morsel.aggregators.{Aggregator, AggregatorFactory}
import org.neo4j.cypher.internal.runtime.morsel.operators._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.{createProjectionsForResult, translateColumnOrder}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.exceptions.{CantCompileQueryException, InternalException}

/**
  * Responsible for a mapping from LogicalPlans to Operators.
  */
class OperatorFactory(val executionGraphDefinition: ExecutionGraphDefinition,
                      val converters: ExpressionConverters,
                      val readOnly: Boolean,
                      val indexRegistrator: QueryIndexRegistrator,
                      semanticTable: SemanticTable) {

  private val physicalPlan = executionGraphDefinition.physicalPlan
  private val aggregatorFactory = AggregatorFactory(physicalPlan)

  def create(plan: LogicalPlan, inputBuffer: BufferDefinition): Operator = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Input(nodes, variables, _) =>
        new InputOperator(WorkIdentity.fromPlan(plan),
                          nodes.map(v => slots.getLongOffsetFor(v)).toArray,
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
          throw new CantCompileQueryException("Morsel does not yet support the plans including `NodeUniqueIndexSeek(Locking)`, use another runtime.")
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
                              mode == ExpandAll,
                              tempNodeOffset,
                              tempRelationshipOffset,
                              nodePredicate.map(x => converters.toCommandExpression(id, x.predicate)).getOrElse(True()),
                              relationshipPredicate.map(x => converters.toCommandExpression(id, x.predicate)).getOrElse(True()))

      case plans.Optional(source, protectedSymbols) =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[OptionalBufferVariant].argumentStateMapId
        val nullableKeys = source.availableSymbols -- protectedSymbols
        val nullableSlots: Array[Slot] = nullableKeys.map(k => slots.get(k).get).toArray
        val argumentSize = physicalPlan.argumentSizes(plan.id)

        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)

        new OptionalOperator(WorkIdentity.fromPlan(plan), argumentStateMapId, argumentSlotOffset, nullableSlots, slots, argumentSize)

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
        rhsSlots.foreachSlotOrdered({
          case (key, LongSlot(offset, _, _)) if offset >= argumentSize.nLongs =>
            copyLongsFromRHS += ((offset, slots.getLongOffsetFor(key)))
          case (key, RefSlot(offset, _, _)) if offset >= argumentSize.nReferences =>
            copyRefsFromRHS += ((offset, slots.getReferenceOffsetFor(key)))
          case _ => // do nothing, already added by lhs
        }, { cnp =>
          val offset = rhsSlots.getCachedPropertyOffsetFor(cnp)
          if (offset >= argumentSize.nReferences)
            copyCachedPropertiesFromRHS += offset -> slots.getCachedPropertyOffsetFor(cnp)
        })

        val longsToCopy = copyLongsFromRHS.result()
        val refsToCopy = copyRefsFromRHS.result()
        val cachedPropertiesToCopy = copyCachedPropertiesFromRHS.result()

        val buffer = inputBuffer.variant.asInstanceOf[LHSAccumulatingRHSStreamingBufferVariant]
        new NodeHashJoinOperator(
          WorkIdentity.fromPlan(plan),
          buffer.lhsArgumentStateMapId,
          buffer.rhsArgumentStateMapId,
          lhsOffsets,
          rhsOffsets,
          slots,
          longsToCopy,
          refsToCopy,
          cachedPropertiesToCopy)

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
                              argumentSlot)

      case plans.Top(_, sortItems, limit) =>
        val ordering = sortItems.map(translateColumnOrder(slots, _))
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        new TopMergeOperator(argumentStateMapId,
                             WorkIdentity.fromPlan(plan),
                             ordering,
                             argumentSlot,
                             converters.toCommandExpression(plan.id, limit))

      case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId

        val aggregators = Array.newBuilder[Aggregator]
        val outputSlots = Array.newBuilder[Int]
        aggregationExpression.foreach {
          case (key, astExpression) =>
            val outputSlot = slots.get(key).get
            val (aggregator, _) = aggregatorFactory.newAggregator(astExpression)
            aggregators += aggregator
            outputSlots += outputSlot.offset
        }
        AggregationOperatorNoGrouping(WorkIdentity.fromPlan(plan),
                                      aggregators.result())
            .reducer(argumentStateMapId,
                     outputSlots.result())

      case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
        val argumentStateMapId = inputBuffer.variant.asInstanceOf[ArgumentStateBufferVariant].argumentStateMapId
        val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)

        val aggregators = Array.newBuilder[Aggregator]
        val outputSlots = Array.newBuilder[Int]
        aggregationExpression.foreach {
          case (key, astExpression) =>
            val outputSlot = slots.get(key).get
            val (aggregator, _) = aggregatorFactory.newAggregator(astExpression)
            aggregators += aggregator
            outputSlots += outputSlot.offset
        }

        AggregationOperator(WorkIdentity.fromPlan(plan), aggregators.result(), groupings)
          .reducer(argumentStateMapId, outputSlots.result())

      case plan: plans.ProduceResult => createProduceResults(plan)

      case _: plans.Argument =>
        new ArgumentOperator(WorkIdentity.fromPlan(plan),
                             physicalPlan.argumentSizes(id))
    }
  }

  def createMiddle(plan: LogicalPlan): Option[MiddleOperator] = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Selection(predicate, _) =>
        Some(new FilterOperator(WorkIdentity.fromPlan(plan), converters.toCommandExpression(id, predicate)))

      case plans.Limit(_, count, DoNotIncludeTies) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        Some(new LimitOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), converters.toCommandExpression(plan.id, count)))

      case plans.Distinct(_, groupingExpressions) =>
        val argumentStateMapId = executionGraphDefinition.findArgumentStateMapForPlan(id)
        val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)
        Some(new DistinctOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), groupings))

      case plans.Projection(_, expressions) =>
        val projectionOps: CommandProjection = converters.toCommandProjection(id, expressions)
        Some(new ProjectOperator(WorkIdentity.fromPlan(plan), projectionOps))

      case plans.CacheProperties(_, properties) =>
        val propertyOps = properties.toArray.map(converters.toCommandExpression(id, _))
        Some(new CachePropertiesOperator(WorkIdentity.fromPlan(plan), propertyOps))

      case _: plans.Argument => None
    }
  }

  def createProduceResults(plan: plans.ProduceResult): ProduceResultOperator = {
    val slots = physicalPlan.slotConfigurations(plan.id)
    val runtimeColumns = createProjectionsForResult(plan.columns, slots)
    new ProduceResultOperator(WorkIdentity.fromPlan(plan),
                              slots,
                              runtimeColumns)
  }

  def createOutput(outputDefinition: OutputDefinition): OutputOperator = {

    outputDefinition match {
      case NoOutput => NoOutputOperator
      case MorselBufferOutput(bufferId, planId) => MorselBufferOutputOperator(bufferId, planId)
      case MorselArgumentStateBufferOutput(bufferId, argumentSlotOffset, planId) => MorselArgumentStateBufferOutputOperator(bufferId, argumentSlotOffset, planId)
      case ProduceResultOutput(p) => createProduceResults(p)
      case ReduceOutput(bufferId, plan) =>
        val id = plan.id
        val slots = physicalPlan.slotConfigurations(id)
        generateSlotAccessorFunctions(slots)

        plan match {
          case plans.Sort(_, sortItems) =>
            val argumentDepth = physicalPlan.applyPlans(id)
            val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
            val ordering = sortItems.map(translateColumnOrder(slots, _))
            new SortPreOperator(WorkIdentity.fromPlan(plan), argumentSlot, bufferId, ordering)

          case plans.Top(_, sortItems, limit) =>
            val argumentDepth = physicalPlan.applyPlans(id)
            val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
            val ordering = sortItems.map(translateColumnOrder(slots, _))
            new SortPreOperator(WorkIdentity.fromPlan(plan), argumentSlot, bufferId, ordering)

          case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
            val argumentDepth = physicalPlan.applyPlans(id)
            val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)

            val aggregators = Array.newBuilder[Aggregator]
            val expressions = Array.newBuilder[Expression]
            aggregationExpression.foreach {
              case (key, astExpression) =>
                val (aggregator, expression) = aggregatorFactory.newAggregator(astExpression)
                aggregators += aggregator
                expressions += converters.toCommandExpression(id, expression)
              }

            AggregationOperatorNoGrouping(WorkIdentity.fromPlan(plan),
                                          aggregators.result())
              .mapper(argumentSlotOffset,
                      bufferId,
                      expressions.result())

          case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
            val argumentDepth = physicalPlan.applyPlans(id)
            val argumentSlotOffset = slots.getArgumentLongOffsetFor(argumentDepth)
            val groupings = converters.toGroupingExpression(id, groupingExpressions, Seq.empty)

            val aggregators = Array.newBuilder[Aggregator]
            val expressions = Array.newBuilder[Expression]
            aggregationExpression.foreach {
              case (key, astExpression) =>
                val (aggregator, expression) = aggregatorFactory.newAggregator(astExpression)
                aggregators += aggregator
                expressions += converters.toCommandExpression(id, expression)
            }

            AggregationOperator(WorkIdentity.fromPlan(plan), aggregators.result(), groupings)
              .mapper(argumentSlotOffset, bufferId, expressions.result())

        }
    }
  }
}
