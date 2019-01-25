/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.physical_planning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.physical_planning.SlotConfigurationUtils.generateSlotAccessorFunctions
import org.neo4j.cypher.internal.physical_planning.{RefSlot, SlottedIndexedProperty}
import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.QueryIndexes
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.morsel.expressions.AggregationExpressionOperator
import org.neo4j.cypher.internal.runtime.morsel.operators._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.{createProjectionsForResult, translateColumnOrder}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.{IndexOrder => KernelIndexOrder}

class PipelineBuilder(physicalPlan: PhysicalPlan,
                      converters: ExpressionConverters,
                      readOnly: Boolean,
                      queryIndexes: QueryIndexes,
                      fallbackPipeMapper: PipeMapper)
  extends TreeBuilder[Pipeline] {

  override def create(plan: LogicalPlan): Pipeline = {
    if (!readOnly)
      throw new CantCompileQueryException("Write queries are not supported in the morsel runtime")

    val pipeline: Pipeline = super.create(plan)
    pipeline.construct
  }

  override protected def onLeaf(plan: LogicalPlan, source: Option[Pipeline]): Pipeline = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    val argumentSize = physicalPlan.argumentSizes(id)
    generateSlotAccessorFunctions(slots)

    val thisOp = plan match {
      case plans.AllNodesScan(column, _) =>
        new AllNodeScanOperator(
          WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          argumentSize)

      case plans.NodeByLabelScan(column, label, _) =>
        queryIndexes.registerLabelScan()
        new LabelScanOperator(
          WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          LazyLabel(label)(SemanticTable()),
          argumentSize)

      case plans.NodeIndexScan(column, labelToken, property, _, indexOrder) =>
        new NodeIndexScanOperator(
          WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          labelToken.nameId.id,
          SlottedIndexedProperty(column, property, slots),
          queryIndexes.registerQueryIndex(labelToken, property),
          asKernelIndexOrder(indexOrder),
          argumentSize)

      case NodeIndexContainsScan(column, labelToken, property, valueExpr, _, indexOrder) =>
        new NodeIndexContainsScanOperator(
          WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          labelToken.nameId.id,
          SlottedIndexedProperty(column, property, slots),
          queryIndexes.registerQueryIndex(labelToken, property),
          asKernelIndexOrder(indexOrder),
          converters.toCommandExpression(id, valueExpr),
          argumentSize)

      case plans.NodeIndexSeek(column, label, properties, valueExpr, _,  indexOrder) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        new NodeIndexSeekOperator(
          WorkIdentity.fromPlan(plan),
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
          WorkIdentity.fromPlan(plan),
          slots.getLongOffsetFor(column),
          label,
          properties.map(SlottedIndexedProperty(column, _, slots)).toArray,
          queryIndexes.registerQueryIndex(label, properties),
          indexOrder,
          argumentSize,
          valueExpr.map(converters.toCommandExpression(id, _)),
          indexSeekMode)

      case plans.Argument(_) =>
        new ArgumentOperator(WorkIdentity.fromPlan(plan), argumentSize)

      case plans.Input(variables) =>
        new InputOperator(WorkIdentity.fromPlan(plan), variables.map(v => slots.getReferenceOffsetFor(v)))

      case p: plans.LazyLogicalPlan =>
        val pipe = fallbackPipeMapper.onLeaf(plan)
        new LazySlottedPipeLeafOperator(WorkIdentity.fromPlan(plan), pipe, argumentSize)

      case p =>
        throw new CantCompileQueryException(s"$p not supported in morsel runtime")
    }

    thisOp match {
      case co: InitialComposableOperator[_] =>
        new StreamingComposablePipeline(co, slots, source)

      case so: StreamingOperator =>
        new StreamingPipeline(so, slots, source)
    }
  }

  override protected def onOneChildPlan(plan: LogicalPlan, source: Pipeline): Pipeline = {
    val id = plan.id
    val slots = physicalPlan.slotConfigurations(id)
    generateSlotAccessorFunctions(slots)

      val thisOp = plan match {
        case plans.ProduceResult(_, columns) =>
          val runtimeColumns = createProjectionsForResult(columns, slots)
          new ProduceResultOperator(WorkIdentity.fromPlan(plan), slots, runtimeColumns)

        case plans.Selection(predicate, _) =>
          new FilterOperator(WorkIdentity.fromPlan(plan), converters.toCommandPredicate(id, predicate))

        case plans.Expand(lhs, fromName, dir, types, to, relName, ExpandAll) =>
          val fromOffset = slots.getLongOffsetFor(fromName)
          val relOffset = slots.getLongOffsetFor(relName)
          val toOffset = slots.getLongOffsetFor(to)
          val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
          new ExpandAllOperator(WorkIdentity.fromPlan(plan), fromOffset, relOffset, toOffset, dir, lazyTypes)

        case plans.Projection(_, expressions) =>
          val projectionOps = expressions.map {
            case (key, e) => slots(key) -> converters.toCommandExpression(id, e)
          }
          new ProjectOperator(WorkIdentity.fromPlan(plan), projectionOps)

        case plans.Sort(_, sortItems) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val preSorting = new PreSortOperator(WorkIdentity.fromPlan(plan), ordering)
          source.addOperator(preSorting)
          new MergeSortOperator(WorkIdentity.fromPlan(plan), ordering)

        case Top(_, sortItems, limit) =>
          val ordering = sortItems.map(translateColumnOrder(slots, _))
          val countExpression = converters.toCommandExpression(id, limit)
          val preTop = new PreSortOperator(WorkIdentity.fromPlan(plan), ordering, Some(countExpression))
          source.addOperator(preTop)
          new MergeSortOperator(WorkIdentity.fromPlan(plan), ordering, Some(countExpression))

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) if groupingExpressions.isEmpty =>
          val aggregations = aggregationExpression.map {
            case (key, expression) =>
              val currentSlot = slots.get(key).get
              //we need to make room for storing aggregation value in
              //source slot
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              val morselAggExpression = converters.toCommandExpression(id, expression) match {
                case e:AggregationExpressionOperator => e
                case _ => throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
              }
              AggregationOffsets(source.slots.getReferenceOffsetFor(key), currentSlot.offset, morselAggExpression)
          }.toArray

          //add mapper to source
          source.addOperator(new AggregationMapperOperatorNoGrouping(WorkIdentity.fromPlan(plan), aggregations))
          new AggregationReduceOperatorNoGrouping(WorkIdentity.fromPlan(plan), aggregations)

        case plans.Aggregation(_, groupingExpressions, aggregationExpression) =>
          groupingExpressions.keys.foreach { key =>
            val currentSlot = slots(key)
            //we need to make room for storing grouping value in source slot
            if (currentSlot.isLongSlot)
              source.slots.newLong(key, currentSlot.nullable, currentSlot.typ)
            else
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
          }

          val aggregations = aggregationExpression.map {
            case (key, expression) =>
              val currentSlot = slots.get(key).get
              //we need to make room for storing aggregation value in
              //source slot
              source.slots.newReference(key, currentSlot.nullable, currentSlot.typ)
              val morselAggExpression = converters.toCommandExpression(id, expression) match {
                case e:AggregationExpressionOperator => e
                case _ => throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
              }
              AggregationOffsets(source.slots.getReferenceOffsetFor(key), currentSlot.offset, morselAggExpression)
          }.toArray

          //add mapper to source
          val groupings = converters.toGroupingExpression(id, groupingExpressions)
          source.addOperator(new AggregationMapperOperator(WorkIdentity.fromPlan(plan), aggregations, groupings))
          new AggregationReduceOperator(WorkIdentity.fromPlan(plan), aggregations, groupings)

        case plans.UnwindCollection(src, variable, collection) =>
          val offset = slots.get(variable) match {
            case Some(RefSlot(idx, _, _)) => idx
            case _ =>
              throw new InternalException("Weird slot found for UNWIND")
          }
          val runtimeExpression = converters.toCommandExpression(id, collection)
          new UnwindOperator(WorkIdentity.fromPlan(plan), runtimeExpression, offset)

        // Plans that are not supported by slotted pipe fallback (to be supported it needs to be lazy and stateless)
        case _: plans.Limit | // Limit keeps state (remaining counter) between iterator.next() calls, so we cannot re-create iterator
             _: plans.Optional | // Optional pipe relies on a pull-based dataflow and needs a different solution for push
             _: plans.Skip | // Skip pipe eagerly drops n rows upfront which does not work with feed pipe
             _: plans.Eager | // We do not support eager plans since the resulting iterators cannot be recreated and fed a single input row at a time
             _: plans.EmptyResult | // Eagerly exhausts the source iterator
             _: plans.Distinct | // Even though the Distinct pipe is not really eager it still keeps state
             _: plans.LoadCSV | // Not verified to be thread safe
             _: plans.ProcedureCall => // Even READ_ONLY Procedures are not allowed because they will/might access the
                                       // transaction via Core API reads, which is not thread safe because of the transaction
                                       // bound CursorFactory.
          throw new CantCompileQueryException(s"$plan not supported in morsel runtime")

        // Fallback to use a lazy slotted pipe
        case p =>
          source match {
            case s: StreamingComposablePipeline[_] if s.start.isInstanceOf[LazySlottedPipeStreamingOperator] && !s.hasAdditionalOperators =>
              val pipeConstructor: Pipe => Pipe =
                (sourcePipe: Pipe) =>
                  fallbackPipeMapper.onOneChildPlan(plan, sourcePipe)
              new LazySlottedPipeComposableOperator(WorkIdentity.fromPlan(plan), pipeConstructor)

            case _ =>
              val feedPipe = FeedPipe(source.slots)()
              val pipe = fallbackPipeMapper.onOneChildPlan(plan, feedPipe)
              new LazySlottedPipeOneChildOperator(WorkIdentity.fromPlan(plan), pipe)
          }
      }

    thisOp match {
      case co: ComposableOperator[_] =>
        source.asInstanceOf[StreamingComposablePipeline[_]].addComposableOperator(co, slots)
        source
      case co: StreamingOperator with InitialComposableOperator[_] =>
        new StreamingComposablePipeline(co, slots, Some(source))
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
    generateSlotAccessorFunctions(slots)

    plan match {
      case _: plans.Apply =>
        rhs

      case _: plans.CartesianProduct =>
        val argumentSize = physicalPlan.argumentSizes(plan.id)
        val operator = new CartesianProductOperator(WorkIdentity.fromPlan(plan), argumentSize)
        new StreamingMergePipeline(operator, slots, lhs, rhs)

      case p =>
        throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
    }
  }

  private def asKernelIndexOrder(indexOrder: IndexOrder): api.IndexOrder = indexOrder match {
    case IndexOrderAscending => KernelIndexOrder.ASCENDING
    case IndexOrderDescending => KernelIndexOrder.DESCENDING
    case IndexOrderNone => KernelIndexOrder.NONE
  }
}

object IsPipelineBreaker {
  def apply(plan: LogicalPlan): Boolean = {
    plan match {
      case _ => true
    }
  }
}
