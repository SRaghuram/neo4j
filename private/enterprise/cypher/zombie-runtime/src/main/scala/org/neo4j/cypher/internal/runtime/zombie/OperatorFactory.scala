/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, SlottedIndexedProperty, _}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.{createProjectionsForResult, translateColumnOrder}
import org.neo4j.cypher.internal.runtime.zombie.operators._
import org.neo4j.cypher.internal.runtime.{QueryIndexes, slotted}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.symbols.CTInteger
import org.neo4j.internal.kernel

/**
  * Responsible for a mapping from LogicalPlans to Operators.
  */
class OperatorFactory(val stateDefinition: StateDefinition,
                      val converters: ExpressionConverters,
                      val readOnly: Boolean,
                      val queryIndexes: QueryIndexes) {
  private val physicalPlan = stateDefinition.physicalPlan

  def create(plan: LogicalPlan, inputBuffer: BufferDefinition): Operator = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

    plan match {
      case plans.Input(nodes, variables, _) =>
        new InputOperator(WorkIdentity.fromPlan(plan),
                          nodes.map(v => slots.getLongOffsetFor(v)),
                          variables.map(v => slots.getReferenceOffsetFor(v)))

      case plans.AllNodesScan(column, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new AllNodeScanOperator(WorkIdentity.fromPlan(plan),
                                slots.getLongOffsetFor(column),
                                argumentSize)

      case plans.NodeByLabelScan(column, label, _) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        queryIndexes.registerLabelScan()
        new LabelScanOperator(WorkIdentity.fromPlan(plan),
                              slots.getLongOffsetFor(column),
                              LazyLabel(label)(SemanticTable()),
                              argumentSize)

      case plans.NodeIndexSeek(column, label, properties, valueExpr, _,  indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
                                  slots.getLongOffsetFor(column),
                                  label,
                                  properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
                                  queryIndexes.registerQueryIndex(label, properties),
                                  indexOrder,
                                  argumentSize,
                                  valueExpr.map(converters.toCommandExpression(id, _)),
                                  indexSeekMode)

      case plans.NodeUniqueIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(WorkIdentity.fromPlan(plan),
                                  slots.getLongOffsetFor(column),
                                  label,
                                  properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
                                  queryIndexes.registerQueryIndex(label, properties),
                                  indexOrder,
                                  argumentSize,
                                  valueExpr.map(converters.toCommandExpression(id, _)),
                                  indexSeekMode)

      case plans.NodeIndexScan(column, labelToken, properties, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new NodeIndexScanOperator(WorkIdentity.fromPlan(plan),
                                  slots.getLongOffsetFor(column),
                                  properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
                                  queryIndexes.registerQueryIndex(labelToken, properties),
                                  asKernelIndexOrder(indexOrder),
                                  argumentSize)

      case plans.NodeIndexContainsScan(column, labelToken, property, valueExpr, _, indexOrder) =>
        val argumentSize = physicalPlan.argumentSizes(id)
        new NodeIndexContainsScanOperator(WorkIdentity.fromPlan(plan),
                                          slots.getLongOffsetFor(column),
                                          SlottedIndexedProperty(column, property, slots),
                                          queryIndexes.registerQueryIndex(labelToken, property),
                                          asKernelIndexOrder(indexOrder),
                                          converters.toCommandExpression(id, valueExpr),
                                          argumentSize)

      case plans.Expand(lhs, fromName, dir, types, to, relName, plans.ExpandAll) =>
        val fromOffset = slots.getLongOffsetFor(fromName)
        val relOffset = slots.getLongOffsetFor(relName)
        val toOffset = slots.getLongOffsetFor(to)
        val lazyTypes = RelationshipTypes(types.toArray)(SemanticTable())
        new ExpandAllOperator(WorkIdentity.fromPlan(plan),
                              fromOffset,
                              relOffset,
                              toOffset,
                              dir,
                              lazyTypes)

      case joinPlan:plans.NodeHashJoin =>

        val slotConfigs = physicalPlan.slotConfigurations
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val lhsOffsets: Array[Int] = joinPlan.nodes.map(k => slots.getLongOffsetFor(k)).toArray
        val rhsSlots = slotConfigs(joinPlan.right.id)
        val rhsOffsets: Array[Int] = joinPlan.nodes.map(k => rhsSlots.getLongOffsetFor(k)).toArray
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
          val offset = rhsSlots.getCachedNodePropertyOffsetFor(cnp)
          if (offset >= argumentSize.nReferences)
            copyCachedPropertiesFromRHS += offset -> slots.getCachedNodePropertyOffsetFor(cnp)
        })

        val longsToCopy = copyLongsFromRHS.result()
        val refsToCopy = copyRefsFromRHS.result()
        val cachedPropertiesToCopy = copyCachedPropertiesFromRHS.result()

        val buffer = inputBuffer.asInstanceOf[LHSAccumulatingRHSStreamingBufferDefinition]
        new NodeHashJoinOperator(
          WorkIdentity.fromPlan(plan),
          buffer.lhsargumentStateMapId,
          buffer.rhsargumentStateMapId,
          lhsOffsets,
          rhsOffsets,
          slots,
          longsToCopy,
          refsToCopy,
          cachedPropertiesToCopy)

      case plans.UnwindCollection(src, variable, collection) =>
        val offset = slots.get(variable) match {
          case Some(RefSlot(idx, _, _)) => idx
          case _ =>
            throw new InternalException("Weird slot found for UNWIND")
        }
        val runtimeExpression = converters.toCommandExpression(id, collection)
        new UnwindOperator(WorkIdentity.fromPlan(plan), runtimeExpression, offset)

      case plans.Sort(_, sortItems) =>
        val ordering = sortItems.map(translateColumnOrder(slots, _))
        val argumentDepth = physicalPlan.applyPlans(id)
        val argumentSlot = slots.getArgumentLongOffsetFor(argumentDepth)
        val argumentStateMapId = inputBuffer.asInstanceOf[ArgumentStateBufferDefinition].argumentStateMapId
        new SortMergeOperator(argumentStateMapId,
                              WorkIdentity.fromPlan(plan),
                              ordering,
                              argumentSlot)

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

      case plans.Limit(_, count, ties) =>
        val argumentStateMapId = stateDefinition.findArgumentStateMapForPlan(id)
        Some(new LimitOperator(argumentStateMapId, WorkIdentity.fromPlan(plan), converters.toCommandExpression(plan.id, count)))

      case plans.Projection(_, expressions) =>
        val projectionOps = expressions.map {
          case (key, e) => slots(key) -> converters.toCommandExpression(id, e)
        }
        Some(new ProjectOperator(WorkIdentity.fromPlan(plan), projectionOps))

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

  private def asKernelIndexOrder(indexOrder: plans.IndexOrder): kernel.api.IndexOrder =
    indexOrder match {
      case plans.IndexOrderAscending => kernel.api.IndexOrder.ASCENDING
      case plans.IndexOrderDescending => kernel.api.IndexOrder.DESCENDING
      case plans.IndexOrderNone => kernel.api.IndexOrder.NONE
    }
}
