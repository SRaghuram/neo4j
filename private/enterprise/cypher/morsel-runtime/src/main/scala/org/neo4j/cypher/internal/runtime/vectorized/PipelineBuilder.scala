/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.RefSlot
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlottedIndexedProperty
import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekModeFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.translateColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized.expressions.AggregationExpressionOperator
import org.neo4j.cypher.internal.runtime.vectorized.operators._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._

class PipelineBuilder(physicalPlan: PhysicalPlan, converters: ExpressionConverters, readOnly: Boolean, queryIndexes: QueryIndexes)
  extends TreeBuilder[Pipeline] {

  override def create(plan: LogicalPlan): Pipeline = {
    val pipeline: Pipeline = super.create(plan)
    pipeline.construct
  }

  override protected def onLeaf(plan: LogicalPlan, source: Option[Pipeline]): Pipeline = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    val argumentSize = physicalPlan.argumentSizes(id)

    val thisOp = plan match {
      case plans.AllNodesScan(column, _) =>
        new AllNodeScanOperator(
          slots.getLongOffsetFor(column),
          argumentSize)

      case plans.NodeByLabelScan(column, label, _) =>
        new LabelScanOperator(
          slots.getLongOffsetFor(column),
          LazyLabel(label)(SemanticTable()),
          argumentSize)

      case plans.NodeIndexScan(column, labelToken, property, _, _) =>
        new NodeIndexScanOperator(
          slots.getLongOffsetFor(column),
          labelToken.nameId.id,
          SlottedIndexedProperty(column, property, slots),
          queryIndexes.registerQueryIndex(labelToken, property),
          argumentSize)

      case NodeIndexContainsScan(column, labelToken, property, valueExpr, _, indexOrder) =>
        new NodeIndexContainsScanOperator(
          slots.getLongOffsetFor(column),
          labelToken.nameId.id,
          SlottedIndexedProperty(column, property, slots),
          converters.toCommandExpression(id, valueExpr),
          argumentSize)

      case plans.NodeIndexSeek(column, label, properties, valueExpr, _,  indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(
          slots.getLongOffsetFor(column),
          label,
          properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
          queryIndexes.registerQueryIndex(label, properties),
          indexOrder,
          argumentSize,
          valueExpr.map(converters.toCommandExpression(id, _)),
          indexSeekMode)

      case plans.NodeUniqueIndexSeek(column, label, properties, valueExpr, _, indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = true, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(
          slots.getLongOffsetFor(column),
          label,
          properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
          queryIndexes.registerQueryIndex(label, properties),
          indexOrder,
          argumentSize,
          valueExpr.map(converters.toCommandExpression(id, _)),
          indexSeekMode)

      case plans.Argument(_) =>
        new ArgumentOperator(argumentSize)

      case p =>
        throw new CantCompileQueryException(s"$p not supported in morsel runtime")
    }

    new StreamingPipeline(thisOp, slots, source)
  }

  override protected def onOneChildPlan(plan: LogicalPlan, source: Pipeline): Pipeline = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)

      val thisOp = plan match {
        case plans.ProduceResult(_, columns) =>
          new ProduceResultOperator(slots, columns.toArray)

        case plans.Selection(predicate, _) =>
          new FilterOperator(converters.toCommandPredicate(id, predicate))

        case plans.Expand(lhs, fromName, dir, types, to, relName, ExpandAll) =>
          val fromOffset = slots.getLongOffsetFor(fromName)
          val relOffset = slots.getLongOffsetFor(relName)
          val toOffset = slots.getLongOffsetFor(to)
          val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
          new ExpandAllOperator(fromOffset, relOffset, toOffset, dir, lazyTypes)

        case plans.Projection(_, expressions) =>
          val projectionOps = expressions.map {
            case (key, e) => slots(key) -> converters.toCommandExpression(id, e)
          }
          new ProjectOperator(projectionOps)

        case plans.Sort(_, sortItems) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val preSorting = new PreSortOperator(ordering)
          source.addOperator(preSorting)
          new MergeSortOperator(ordering)

        case Top(_, sortItems, limit) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val countExpression = converters.toCommandExpression(id, limit)
          val preTop = new PreSortOperator(ordering, Some(countExpression))
          source.addOperator(preTop)
          new MergeSortOperator(ordering, Some(countExpression))

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
          val aggregations = aggregationExpression.map {
            case (key, expression) =>
              val currentSlot = slots.get(key).get
              //we need to make room for storing aggregation value in
              //source slot
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              AggregationOffsets(source.slots.getReferenceOffsetFor(key), currentSlot.offset,
                                 converters.toCommandExpression(id, expression).asInstanceOf[AggregationExpressionOperator])
          }.toArray

          //add mapper to source
          source.addOperator(new AggregationMapperOperatorNoGrouping(aggregations))
          new AggregationReduceOperatorNoGrouping(aggregations)

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
          val groupings = groupingExpressions.map {
            case (key, expression) =>
              val currentSlot = slots(key)
              //we need to make room for storing grouping value in source slot
              if (currentSlot.isLongSlot)
                source.slots.newLong(key, currentSlot.nullable, currentSlot.typ)
              else
                source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              GroupingOffsets(source.slots(key), currentSlot, converters.toCommandExpression(id, expression))
          }.toArray

          val aggregations = aggregationExpression.map {
            case (key, expression) =>
              val currentSlot = slots.get(key).get
              //we need to make room for storing aggregation value in
              //source slot
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              AggregationOffsets(source.slots.getReferenceOffsetFor(key), currentSlot.offset,
                                 converters.toCommandExpression(id, expression).asInstanceOf[AggregationExpressionOperator])
          }.toArray

          //add mapper to source
          source.addOperator(new AggregationMapperOperator(aggregations, groupings))
          new AggregationReduceOperator(aggregations, groupings)

        case plans.UnwindCollection(src, variable, collection) =>
          val offset = slots.get(variable) match {
            case Some(RefSlot(idx, _, _)) => idx
            case _ =>
              throw new InternalException("Weird slot found for UNWIND")
          }
          val runtimeExpression = converters.toCommandExpression(id, collection)
          new UnwindOperator(runtimeExpression, offset)

        case p => throw new CantCompileQueryException(s"$p not supported in morsel runtime")
      }

    thisOp match {
      case so: StreamingOperator =>
        new StreamingPipeline(so, slots, Some(source))
      case mo: StatelessOperator =>
        source.addOperator(mo)
        source
      case ro: EagerReduceOperator =>
        new EagerReducePipeline(ro, slots, Some(source))
      case ro: LazyReduceOperator =>
        new LazyReducePipeline(ro, slots, Some(source))
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan, lhs: Pipeline): Option[Pipeline]= {
    plan match {
      case _: plans.Apply =>
        // Connect the lhs pipeline to the rhs pipeline (the return value here will be used as source to the rhs pipeline)
        Some(lhs)

      case _ =>
        None
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = {
    val slots = physicalPlan.slotConfigurations(plan.id)

    plan match {
      case _: plans.Apply =>
        rhs

      case p =>
        throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
    }
  }
}

object IsPipelineBreaker {
  def apply(plan: LogicalPlan): Boolean = {
    plan match {
      case _ => true
    }
  }
}
