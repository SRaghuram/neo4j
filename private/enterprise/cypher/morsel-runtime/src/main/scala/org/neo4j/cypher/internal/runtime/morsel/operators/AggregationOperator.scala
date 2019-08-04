/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.{MemoryTracker, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.aggregators.{Aggregator, AvgAggregator, CollectAggregator, CountAggregator, CountStarAggregator, MaxAggregator, MinAggregator, Reducer, SumAggregator, Updater}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator, PerArgument}
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.morsel.{ArgumentStateMapCreator, ExecutionState, OperatorExpressionCompiler}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.v4_0.util.SyntaxException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import scala.collection.JavaConverters._
import org.neo4j.values.AnyValue
import org.neo4j.cypher.internal.v4_0.expressions.{Expression => AstExpression}

import scala.collection.mutable.ArrayBuffer

/**
  * General purpose aggregation operator, supporting clauses like
  *
  * {{{
  *   WITH key1, key2, key3, sum(..) AS aggr1, count(..) AS aggr2, avg(..) AS aggr3
  * }}}
  *
  * The implementation composes an [[AggregationMapperOperator]], an [[AggregatingAccumulator]] and an [[AggregationReduceOperator]].
  */
case class AggregationOperator(workIdentity: WorkIdentity,
                               aggregations: Array[Aggregator],
                               groupings: GroupingExpression) {

  type AggPreMap = java.util.LinkedHashMap[groupings.KeyType, Array[Updater]]

  private val newUpdaters: java.util.function.Function[ groupings.KeyType, Array[Updater]] =
    (_: groupings.KeyType) => aggregations.map(_.newUpdater)

  def mapper(argumentSlotOffset: Int,
             outputBufferId: BufferId,
             expressionValues: Array[Expression]) =
    new AggregationMapperOperator(argumentSlotOffset, outputBufferId, expressionValues)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int]) =
    new AggregationReduceOperator(argumentStateMapId, reducerOutputSlots)

  /**
    * Pre-operator for aggregations with grouping. This performs local aggregation of the
    * data in a single morsel at a time, before putting these local aggregations into the
    * [[ExecutionState]] buffer which perform the final global aggregation.
    */
  class AggregationMapperOperator(argumentSlotOffset: Int,
                                  outputBufferId: BufferId,
                                  expressionValues: Array[Expression]) extends OutputOperator {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState,
                             pipelineId: PipelineId): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[AggPreMap]]](pipelineId, outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[AggPreMap]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      override def prepareOutput(morsel: MorselExecutionContext,
                                 context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreAggregatedOutput = {

        val queryState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)

        val preAggregated = ArgumentStateMap.map(argumentSlotOffset,
                                             morsel,
                                             preAggregate(queryState))

        new PreAggregatedOutput(preAggregated, sink)
      }

      private def preAggregate(queryState: OldQueryState)
                              (morsel: MorselExecutionContext): AggPreMap = {

        val result = new AggPreMap()

        //loop over the entire morsel view and apply the aggregation
        while (morsel.isValidRow) {
          val groupingValue = groupings.computeGroupingKey(morsel, queryState)
          val updaters = result.computeIfAbsent(groupingValue, newUpdaters)
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(morsel, queryState)
            updaters(i).update(value)
            i += 1
          }
          morsel.moveToNextRow()
        }
        result
      }
    }

    class PreAggregatedOutput(preAggregated: IndexedSeq[PerArgument[AggPreMap]],
                              sink: Sink[IndexedSeq[PerArgument[AggPreMap]]]) extends PreparedOutput {
      override def produce(): Unit = sink.put(preAggregated)
    }
  }

  /**
    * Accumulator that compacts input data using some [[Reducer]]s.
    */
  abstract class AggregatingAccumulator extends MorselAccumulator[AggPreMap] {
    /**
      * Return the result of the reducer.
      */
    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]]
  }

  class StandardAggregatingAccumulator(override val argumentRowId: Long,
                                       aggregators: Array[Aggregator],
                                       override val argumentRowIdsForReducers: Array[Long],
                                       memoryTracker: MemoryTracker) extends AggregatingAccumulator {

    val reducerMap = new java.util.LinkedHashMap[groupings.KeyType, Array[Reducer]]

    override def update(data: AggPreMap): Unit = {
     val iterator = data.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val reducers = reducerMap.computeIfAbsent(entry.getKey, key => aggregators.map(_.newStandardReducer(memoryTracker)))
        memoryTracker.checkMemoryRequirement(reducerMap.keySet().asScala.toList.map(_.estimatedHeapUsage).sum)

        var i = 0
        while (i < reducers.length) {
          reducers(i).update(entry.getValue()(i))
          i += 1
        }
      }
    }

    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]] = reducerMap.entrySet().iterator()
  }

  class ConcurrentAggregatingAccumulator(override val argumentRowId: Long,
                                         aggregators: Array[Aggregator],
                                         override val argumentRowIdsForReducers: Array[Long]) extends AggregatingAccumulator {

    val reducerMap = new ConcurrentHashMap[groupings.KeyType, Array[Reducer]]

    override def update(data: AggPreMap): Unit = {
      val iterator = data.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val reducers = reducerMap.computeIfAbsent(entry.getKey, key => aggregators.map(_.newConcurrentReducer))
          var i = 0
          while (i < reducers.length) {
            reducers(i).update(entry.getValue()(i))
            i += 1
          }
      }
    }

    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]] = reducerMap.entrySet().iterator()
  }

  object AggregatingAccumulator {

    class Factory(aggregators: Array[Aggregator], memoryTracker: MemoryTracker) extends ArgumentStateFactory[AggregatingAccumulator] {
      override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): AggregatingAccumulator =
        new StandardAggregatingAccumulator(argumentRowId, aggregators, argumentRowIdsForReducers, memoryTracker)

      override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): AggregatingAccumulator =
        new ConcurrentAggregatingAccumulator(argumentRowId, aggregators, argumentRowIdsForReducers)
    }
  }

  /**
    * Operator which streams aggregated data, built by [[AggregationMapperOperator]] and [[AggregatingAccumulator]].
    */
  class AggregationReduceOperator(val argumentStateMapId: ArgumentStateMapId,
                                  reducerOutputSlots: Array[Int])
    extends Operator
      with ReduceOperatorState[AggPreMap, AggregatingAccumulator] {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator, stateFactory: StateFactory): ReduceOperatorState[AggPreMap, AggregatingAccumulator] = {
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations, stateFactory.memoryTracker))
      this
    }

    override def nextTasks(queryContext: QueryContext,
                           state: QueryState,
                           input: AggregatingAccumulator,
                           resources: QueryResources
                          ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[AggPreMap, AggregatingAccumulator]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulator: AggregatingAccumulator)
      extends ContinuableOperatorTaskWithAccumulator[AggPreMap, AggregatingAccumulator] {

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      private val resultIterator = accumulator.result()

      override def operate(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Unit = {

        while (resultIterator.hasNext && outputRow.isValidRow) {
          val entry = resultIterator.next()
          val key = entry.getKey
          val reducers = entry.getValue

          groupings.project(outputRow, key)
          var i = 0
          while (i < aggregations.length) {
            outputRow.setRefAt(reducerOutputSlots(i), reducers(i).result)
            i += 1
          }
          outputRow.moveToNextRow()
        }
        outputRow.finishedWriting()
      }

      override def canContinue: Boolean = resultIterator.hasNext
    }
  }
}

class AggregationMapperOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                            override val id: Id,
                                            argumentSlotOffset: Int,
                                            aggregators: Array[Aggregator],
                                            outputBufferId: BufferId,
                                            aggregationExpressionsCreator : () => Array[IntermediateExpression],
                                            groupingKeyExpressionCreator: () => IntermediateExpression,
                                            aggregationExpressions: Array[AstExpression])
                                           (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  import OperatorCodeGenHelperTemplates._
  import org.neo4j.codegen.api.IntermediateRepresentation._

  type AggMap = java.util.LinkedHashMap[AnyValue, Array[Updater]]
  type AggOut = scala.collection.mutable.ArrayBuffer[PerArgument[AggMap]]

  // TODO profiling events?

  override def toString: String = "AggregationMapperOperatorTaskTemplate"

  private def createAggregators(): IntermediateRepresentation = {
    val newAggregators = aggregators.map {
      case CountStarAggregator() => newInstance(constructor[CountStarAggregator])
      case CountAggregator() => newInstance(constructor[CountAggregator])
      case SumAggregator() => newInstance(constructor[SumAggregator])
      case AvgAggregator() => newInstance(constructor[AvgAggregator])
      case MaxAggregator() => newInstance(constructor[MaxAggregator])
      case MinAggregator() => newInstance(constructor[MinAggregator])
      case CollectAggregator() => newInstance(constructor[CollectAggregator])
      case aggregator =>
        throw new SyntaxException(s"Unexpected Aggregator: ${aggregator.getClass.getName}")
    }
    arrayOf[Aggregator](newAggregators: _ *)
  }

  private def createUpdaters(): IntermediateRepresentation = {
    val newUpdaters = aggregators.indices.map(i =>
      invoke(arrayLoad(load(aggregatorsVar), i), method[Aggregator, Updater]("newUpdater"))
    )
    arrayOf[Updater](newUpdaters: _ *)
  }

  private val perArgsField: Field = field[AggOut](codeGen.namer.nextVariableName())
  private val sinkField: Field = field[Sink[IndexedSeq[PerArgument[AggMap]]]](codeGen.namer.nextVariableName())
  private val bufferIdField: Field = field[Int](codeGen.namer.nextVariableName())
  private val collectionHelperField: Field = field[ScalaCollectionHelper](codeGen.namer.nextVariableName())

  private val aggregatorsVar = variable[Array[Aggregator]](codeGen.namer.nextVariableName(), createAggregators())
  private val argVar = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private val aggPreMapVar = variable[AggMap](codeGen.namer.nextVariableName(), constant(null))

  private var compiledAggregationExpressions: Array[IntermediateExpression] = _
  private var compiledGroupingExpression: IntermediateExpression = _

  // constructor
  override def genInit: IntermediateRepresentation = {
    block(
      setField(perArgsField, newInstance(constructor[AggOut])),
      setField(bufferIdField, constant(outputBufferId.x)),
      setField(collectionHelperField, newInstance(constructor[ScalaCollectionHelper])),
      inner.genInit
    )
  }

  // this operates on a single row only
  override def genOperate: IntermediateRepresentation = {
    if (null == compiledAggregationExpressions) {
      compiledAggregationExpressions = aggregationExpressionsCreator()
      compiledGroupingExpression = groupingKeyExpressionCreator()
    }

    /**
      *
      * // this is the final result: a list of pre-aggregations, one per argument
      * val perArgs = new ArrayList<PerArgument<AggPreMap>>()
      *
      * // last seen argument
      * long arg = -1
      *
      * // map of key <--> aggregations
      * val aggPreMap = null
      *
      * {{{
      *
      *   // ----- track when argument changes -----
      *
      *   val currentArg = getFromLongSlot(argumentSlotOffset)
      *   if (currentArg != arg) {
      *     arg = currentArg
      *     aggPreMap = new AggPreMap()
      *     val perArg = new PerArgument<AggPreMap>(arg, aggPreMap)
      *     perArgs.add(perArg)
      *   }
      *
      *   // ----- create updaters group for for each new grouping -----
      *
      *   val groupingValue: AnyValue = {compiledGroupingExpression.ir}
      *   val updaters: Updater[] = aggPreMap.get(groupingValue)
      *   if (updaters == null){
      *    updaters = new Updater[]{ aggregations[0].newUpdater,
      *                              aggregations[1].newUpdater,
      *                              ...
      *                              aggregations[n-1].newUpdater}
      *     aggPreMap.put(groupingValue, updaters)
      *   }
      *
      *   // ----- aggregate -----
      *
      *   {
      *     updaters[0].update(aggregationExpression[0]())
      *     updaters[1].update(aggregationExpression[1]())
      *     ...
      *     updaters[n-1].update(aggregationExpression[n-1]())
      *   }
      * }}}
      *
      */

    // argument of current morsel row
    val currentArg = codeGen.namer.nextVariableName()
    val groupingValue = codeGen.namer.nextVariableName()
    val updaters = codeGen.namer.nextVariableName()

    block(

      /*
       * val currentArg = getFromLongSlot(argumentSlotOffset)
       * if (currentArg != arg) {
       *   arg = currentArg
       *   aggPreMap = new AggPreMap()
       *   perArgs.add(new PerArgument<AggPreMap>(arg, aggPreMap))
       * }
       */
      declareAndAssign(typeRefOf[Long], currentArg, codeGen.getLongAt(argumentSlotOffset)),
      condition(notEqual(load(currentArg), load(argVar)))(
        block(
          assign(argVar, load(currentArg)),
          assign(aggPreMapVar, newInstance(constructor[AggMap])),
          invokeSideEffect(loadField(collectionHelperField),
                           method[ScalaCollectionHelper, Unit, PerArgument[AggMap]]("add"),
                           loadField(perArgsField),
                           newInstance(constructor[PerArgument[AggMap], Long, AggMap], load(argVar), load(aggPreMapVar))),
        )),

      /*
       * val groupingValue: AnyValue = {compiledGroupingExpression.ir}
       * val updaters: Updater[] = aggPreMap.get(groupingValue)
       * if (updaters == null){
       *  updaters = new Updater[]{ aggregations[0].newUpdater,
       *                            aggregations[1].newUpdater,
       *                            ...
       *                            aggregations[n-1].newUpdater}
       *   aggPreMap.put(groupingValue, updaters)
       * }
       */
      declareAndAssign(typeRefOf[AnyValue],
        groupingValue,
        compiledGroupingExpression.ir),
      declareAndAssign(typeRefOf[Array[Updater]],
                       updaters,
                       invoke(load(aggPreMapVar), method[AggMap, Array[Updater], AnyValue]("get"), load(groupingValue))),
      condition(isNull(load(updaters)))(
        block(
          assign(updaters, createUpdaters()),
          invokeSideEffect(load(aggPreMapVar),
                           method[AggMap, Array[Updater], AnyValue, Array[Updater]]("put"),
                           load(groupingValue),
                           load(updaters)),
        )
      ),

      /*
       * updaters[0].update(aggregationExpression[0]())
       * updaters[1].update(aggregationExpression[1]())
       * ...
       * updaters[n-1].update(aggregationExpression[n-1]())
       */
      block(
        compiledAggregationExpressions.indices.map(i => {
          invokeSideEffect(arrayLoad(load(updaters), i), method[Updater, Unit, AnyValue]("update"), compiledAggregationExpressions(i).ir)
        }): _ *
      ),

      inner.genOperateWithExpressions
    )
  }

  override protected def genCreateState: IntermediateRepresentation = {
    block(
      setField(sinkField,
               invoke(EXECUTION_STATE,
                      method[ExecutionState, PipelineId, BufferId, Sink[IndexedSeq[PerArgument[AggMap]]]]("getSink"),
                      PIPELINE_ID,
                      loadField(bufferIdField)))
    )
  }

  override protected def genProduce: IntermediateRepresentation = {
    block(
      invokeSideEffect(loadField(sinkField),
                       method[Sink[AggOut], AggOut, Unit]("put"),
                       loadField(perArgsField)),
      // currently impossible for owning task to continue, as there is no output morsel
      setField(perArgsField, constant(null))
    )
  }

  override def genOutputBuffer: Option[IntermediateRepresentation] = Some(loadField(bufferIdField))

  override def genFields: Seq[Field] = Seq(perArgsField, sinkField, bufferIdField, collectionHelperField) ++ inner.genFields

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, aggPreMapVar) ++ inner.genLocalVariables

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions ++ Seq(compiledGroupingExpression)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}

class ScalaCollectionHelper {
  def add[T](seq: ArrayBuffer[T], element: T): Unit = seq += element
}
