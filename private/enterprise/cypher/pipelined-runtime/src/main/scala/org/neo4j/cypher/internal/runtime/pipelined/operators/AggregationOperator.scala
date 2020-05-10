/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.ConcurrentHashMap
import java.util.function

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.add
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.lessThan
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregators
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectAllAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountStarAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.MaxAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.MinAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Reducer
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.SumAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.SumDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Updater
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createAggregators
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createUpdaters
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.EXECUTION_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_RESOURCES
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.setMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.SyntaxException
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.values.AnyValue

import scala.collection.mutable
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

  private val newUpdaters: MemoryTracker => java.util.function.Function[ groupings.KeyType, Array[Updater]] =
    (memoryTracker: MemoryTracker) => (_: groupings.KeyType) => aggregations.map(_.newUpdater(memoryTracker))

  def mapper(argumentSlotOffset: Int,
             outputBufferId: BufferId,
             expressionValues: Array[Expression],
             operatorId: Id) =
    new AggregationMapperOperator(argumentSlotOffset, outputBufferId, expressionValues)(operatorId)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int],
              operatorId: Id) =
    new AggregationReduceOperator(argumentStateMapId, reducerOutputSlots)(operatorId)

  /**
   * Pre-operator for aggregations with grouping. This performs local aggregation of the
   * data in a single morsel at a time, before putting these local aggregations into the
   * [[ExecutionState]] buffer which perform the final global aggregation.
   */
  class AggregationMapperOperator(argumentSlotOffset: Int,
                                  outputBufferId: BufferId,
                                  expressionValues: Array[Expression])
                                 (val id: Id = Id.INVALID_ID)
    extends OutputOperator {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      new State(executionState.getSink[IndexedSeq[PerArgument[AggPreMap]]](outputBufferId), memoryTracker)
    }

    class State(sink: Sink[IndexedSeq[PerArgument[AggPreMap]]], memoryTracker: MemoryTracker) extends OutputOperatorState {
      private val newUpdatersWithMemoryTracker: function.Function[groupings.KeyType, Array[Updater]] = newUpdaters(memoryTracker)

      override def trackTime: Boolean = true

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      override def prepareOutput(morsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreAggregatedOutput = {
        // TODO: TBD Is it really necessary to have individually tracking and closeable updaters or is it enough to have a scoped memory tracker here (and in AggregationMapperOperatorNoGrouping)?

        val queryState = state.queryStateForExpressionEvaluation(resources)

        val preAggregated = ArgumentStateMap.map(argumentSlotOffset,
          morsel,
          preAggregate(queryState))

        new PreAggregatedOutput(preAggregated, sink)
      }

      private def preAggregate(queryState: QueryState)
                              (morsel: Morsel): AggPreMap = {

        val result = new AggPreMap()

        //loop over the entire morsel view and apply the aggregation
        val readCursor = morsel.readCursor()
        while (readCursor.next()) {
          val groupingValue = groupings.computeGroupingKey(readCursor, queryState)
          val updaters = result.computeIfAbsent(groupingValue, newUpdatersWithMemoryTracker)
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(readCursor, queryState)
            updaters(i).update(value)
            i += 1
          }
        }
        result
      }
    }

    class PreAggregatedOutput(preAggregated: IndexedSeq[PerArgument[AggPreMap]],
                              sink: Sink[IndexedSeq[PerArgument[AggPreMap]]]) extends PreparedOutput {
      override def produce(resources: QueryResources): Unit = sink.put(preAggregated, resources)

      override def close(): Unit = {
        var i = 0
        while (i < preAggregated.size) {
          val perArgument = preAggregated(i)
          perArgument.value.forEach((_, updaters) => {
            var j = 0
            while (j < updaters.length) {
              updaters(j).close()
              j += 1
            }
          })
          i += 1
        }
        super.close()
      }
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

    def argumentRow: MorselRow
  }

  class StandardAggregatingAccumulator(override val argumentRowId: Long,
                                       aggregators: Array[Aggregator],
                                       override val argumentRowIdsForReducers: Array[Long],
                                       override val argumentRow: MorselRow,
                                       memoryTracker: MemoryTracker) extends AggregatingAccumulator {

    val reducerMap = new java.util.LinkedHashMap[groupings.KeyType, Array[Reducer]] // TODO: Use a heap tracking ordered map
    val scopedMemoryTracker = new ScopedMemoryTracker(memoryTracker)

    override def update(data: AggPreMap, resources: QueryResources): Unit = {
      val iterator = data.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val reducers = reducerMap.computeIfAbsent(entry.getKey, _ => {
          // Note: this allocation is currently never de-allocated
          scopedMemoryTracker.allocateHeap(entry.getKey.estimatedHeapUsage())
          aggregators.map(_.newStandardReducer(memoryTracker))
        })

        var i = 0
        while (i < reducers.length) {
          reducers(i).update(entry.getValue()(i))
          i += 1
        }
      }
    }

    def result(): java.util.Iterator[java.util.Map.Entry[groupings.KeyType, Array[Reducer]]] = reducerMap.entrySet().iterator()

    override def close(): Unit = {
      reducerMap.forEach((_, reducers) => {
        var i = 0
        while(i < reducers.length) {
          reducers(i).close()
          i += 1
        }
      })
      scopedMemoryTracker.close()
      super.close()
    }
  }

  class ConcurrentAggregatingAccumulator(override val argumentRowId: Long,
                                         aggregators: Array[Aggregator],
                                         override val argumentRowIdsForReducers: Array[Long],
                                         override val argumentRow: MorselRow) extends AggregatingAccumulator {

    val reducerMap = new ConcurrentHashMap[groupings.KeyType, Array[Reducer]]

    override def update(data: AggPreMap, resources: QueryResources): Unit = {
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
      override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): AggregatingAccumulator =
        new StandardAggregatingAccumulator(argumentRowId, aggregators, argumentRowIdsForReducers, argumentMorsel.snapshot(), memoryTracker)

      override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): AggregatingAccumulator =
        new ConcurrentAggregatingAccumulator(argumentRowId, aggregators, argumentRowIdsForReducers, argumentMorsel.snapshot())
    }
  }

  /**
   * Operator which streams aggregated data, built by [[AggregationMapperOperator]] and [[AggregatingAccumulator]].
   */
  class AggregationReduceOperator(val argumentStateMapId: ArgumentStateMapId,
                                  reducerOutputSlots: Array[Int])
                                 (val id: Id = Id.INVALID_ID)
    extends Operator
    with ReduceOperatorState[AggPreMap, AggregatingAccumulator] {

    override def accumulatorsPerTask(morselSize: Int): Int = 1

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): ReduceOperatorState[AggPreMap, AggregatingAccumulator] = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations, memoryTracker))
      this
    }

    override def nextTasks(state: PipelinedQueryState, input: IndexedSeq[AggregatingAccumulator], resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[AggPreMap, AggregatingAccumulator]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[AggregatingAccumulator])
      extends ContinuableOperatorTaskWithAccumulators[AggPreMap, AggregatingAccumulator] {

      AssertMacros.checkOnlyWhenAssertionsAreEnabled(accumulators.size == 1)
      private val accumulator = accumulators.head

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      private val resultIterator = accumulator.result()

      override def operate(outputMorsel: Morsel,
                           state: PipelinedQueryState,
                           resources: QueryResources): Unit = {

        val outputCursor = outputMorsel.fullCursor(onFirstRow = true)
        while (resultIterator.hasNext && outputCursor.onValidRow()) {
          val entry = resultIterator.next()
          val key = entry.getKey
          val reducers = entry.getValue

          outputCursor.copyFrom(accumulator.argumentRow)
          groupings.project(outputCursor, key)
          var i = 0
          while (i < aggregations.length) {
            outputCursor.setRefAt(reducerOutputSlots(i), reducers(i).result)
            i += 1
          }
          outputCursor.next()
        }
        outputCursor.truncate()
      }

      override def canContinue: Boolean = resultIterator.hasNext
    }
  }
}

object AggregationMapperOperatorTaskTemplate {
  def createAggregators(aggregators: Array[Aggregator]): IntermediateRepresentation = {
    val newAggregators = aggregators.map {
      case CountStarAggregator => getStatic[Aggregators,Aggregator]("COUNT_STAR")
      case CountAggregator => getStatic[Aggregators,Aggregator]("COUNT")
      case CountDistinctAggregator => getStatic[Aggregators,Aggregator]("COUNT_DISTINCT")
      case SumAggregator => getStatic[Aggregators,Aggregator]("SUM")
      case SumDistinctAggregator => getStatic[Aggregators,Aggregator]("SUM_DISTINCT")
      case AvgAggregator => getStatic[Aggregators,Aggregator]("AVG")
      case AvgDistinctAggregator => getStatic[Aggregators,Aggregator]("AVG_DISTINCT")
      case MaxAggregator => getStatic[Aggregators,Aggregator]("MAX")
      case MinAggregator => getStatic[Aggregators,Aggregator]("MIN")
      case CollectAggregator => getStatic[Aggregators,Aggregator]("COLLECT")
      case CollectAllAggregator => getStatic[Aggregators,Aggregator]("COLLECT_ALL")
      case CollectDistinctAggregator => getStatic[Aggregators,Aggregator]("COLLECT_DISTINCT")
      case aggregator =>
        throw new SyntaxException(s"Unexpected Aggregator: ${aggregator.getClass.getName}")
    }
    arrayOf[Aggregator](newAggregators: _ *)
  }

  def createUpdaters(aggregators: Array[Aggregator], aggregatorsVar: IntermediateRepresentation, memoryTracker: IntermediateRepresentation): IntermediateRepresentation = {
    val newUpdaters = aggregators.indices.map(i =>
      invoke(arrayLoad(aggregatorsVar, i), method[Aggregator, Updater, MemoryTracker]("newUpdater"), memoryTracker)
    )
    arrayOf[Updater](newUpdaters: _ *)
  }
}
class AggregationMapperOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                            override val id: Id,
                                            argumentSlotOffset: Int,
                                            aggregators: Array[Aggregator],
                                            outputBufferId: BufferId,
                                            aggregationExpressionsCreator : () => Array[IntermediateExpression],
                                            groupingKeyExpressionCreator: () => IntermediateExpression,
                                            aggregationExpressions: Array[expressions.Expression])
                                           (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  type Agg = Array[Any]
  type AggMap = java.util.LinkedHashMap[AnyValue, Agg]
  type AggOut = scala.collection.mutable.ArrayBuffer[PerArgument[AggMap]]

  override def toString: String = "AggregationMapperOperatorTaskTemplate"

  private val perArgsField: Field = field[AggOut](codeGen.namer.nextVariableName("perArgs"))
  private val sinkField: Field = field[Sink[IndexedSeq[PerArgument[AggMap]]]](codeGen.namer.nextVariableName("sink"))
  private val bufferIdField: Field = field[Int](codeGen.namer.nextVariableName("bufferId"))
  private val memoryTrackerField = field[MemoryTracker](codeGen.namer.nextVariableName("memoryTracker"))

  private val aggregatorsVar = variable[Array[Aggregator]](codeGen.namer.nextVariableName("aggregators"), createAggregators(aggregators))
  private val argVar = variable[Long](codeGen.namer.nextVariableName("arg"), constant(-1L))
  private val aggPreMapVar = variable[AggMap](codeGen.namer.nextVariableName("aggPreMap"), constant(null))

  private var compiledAggregationExpressions: Array[IntermediateExpression] = _
  private var compiledGroupingExpression: IntermediateExpression = _

  // constructor
  override def genInit: IntermediateRepresentation = {
    block(
      setField(perArgsField, newInstance(constructor[AggOut])),
      setField(bufferIdField, constant(outputBufferId.x)),
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
     *   // ----- create updaters group for each new grouping -----
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
      declareAndAssign(typeRefOf[Long], currentArg, codeGen.getArgumentAt(argumentSlotOffset)),
      condition(notEqual(load(currentArg), load(argVar)))(
        block(
          assign(argVar, load(currentArg)),
          assign(aggPreMapVar, newInstance(constructor[AggMap])),
          invokeSideEffect(loadField(perArgsField),
            method[ArrayBuffer[_], ArrayBuffer[_], Any]("$plus$eq"),
            newInstance(constructor[PerArgument[AggMap], Long, Any], load(argVar), load(aggPreMapVar)))
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
      declareAndAssign(typeRefOf[Array[Any]],
        updaters,
        invoke(load(aggPreMapVar), method[AggMap, Any, Any]("get"), load(groupingValue))),
      condition(isNull(load(updaters)))(
        block(
          assign(updaters, createUpdaters(aggregators, load(aggregatorsVar), loadField(memoryTrackerField))),
          invokeSideEffect(load(aggPreMapVar),
            method[AggMap, Any, Any, Any]("put"),
            load(groupingValue),
            load(updaters))
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
          invokeSideEffect(arrayLoad(cast[Array[Updater]](load(updaters)), i), method[Updater, Unit, AnyValue]("update"),
            nullCheckIfRequired(compiledAggregationExpressions(i)))
        }): _ *
      ),

      inner.genOperateWithExpressions
    )
  }

  override def genCreateState: IntermediateRepresentation = {
    block(
      setMemoryTracker(memoryTrackerField, id.x),
      setField(sinkField,
        invoke(EXECUTION_STATE,
          method[ExecutionState, Sink[_], Int]("getSinkInt"),
          loadField(bufferIdField))),
      inner.genCreateState
    )
  }

  override protected def genProduce: IntermediateRepresentation = {
    block(
      invokeSideEffect(loadField(sinkField),
        method[Sink[_], Unit, Any, QueryResources]("put"),
        loadField(perArgsField), QUERY_RESOURCES),
      setField(perArgsField, newInstance(constructor[AggOut]))
    )
  }

  override def genCloseOutput: IntermediateRepresentation = {
    /*
     * Close updaters and reset prepared output (perArgsField) for the next output batch
     * {
     *   var i = 0
     *   while (i < preAggregated.size) {
     *     val aggMap = preAggregated(i).value
     *     val aggMapIter = aggMap.value().iterator()
     *     while (aggMapIter.hasNext) {
     *       val updaters = aggMapIter.next()
     *       updaters[0].close()
     *       updaters[1].close()
     *       ...
     *       updaters[n-1].close()
     *     }
     *     i += 1
     *   }
     *   perArgs = new AggOut()
     * }
     */
    val i = codeGen.namer.nextVariableName()
    val iSize = codeGen.namer.nextVariableName()
    val aggMap = codeGen.namer.nextVariableName("aggMap")
    val aggMapIter = codeGen.namer.nextVariableName("aggMapIter")
    val updaters = codeGen.namer.nextVariableName("updaters")

    block(
      block(
        declareAndAssign(typeRefOf[Int], i, constant(0)),
        declareAndAssign(typeRefOf[Int], iSize, invoke(loadField(perArgsField), method[AggOut, Int]("size"))),
        declare(typeRefOf[AggMap], aggMap),
        declare(typeRefOf[java.util.Iterator[Agg]], aggMapIter),
        loop(lessThan(load(i), load(iSize)))(block(
          assign(aggMap, cast[AggMap](invoke(cast[PerArgument[AggMap]](invoke(loadField(perArgsField),
                                                                              method[mutable.ResizableArray[Any], Any, Int]("apply"),
                                                                              load(i))),
                                             method[PerArgument[Any], Any]("value")))),
          assign(aggMapIter, invoke(invoke(load(aggMap), method[AggMap, java.util.Collection[Agg]]("values")),
                                    method[java.util.Collection[Agg], java.util.Iterator[Array[Any]]]("iterator"))),
          loop(invoke(load(aggMapIter), method[java.util.Iterator[Array[Any]], Boolean]("hasNext")))(block(
            declareAndAssign(typeRefOf[Array[Updater]], updaters, cast[Array[Updater]](invoke(load(aggMapIter),
                                                                                              method[java.util.Iterator[Any], Any]("next")))),
            block(
              compiledAggregationExpressions.indices.map(i => {
                invokeSideEffect(arrayLoad(load(updaters), i), method[Updater, Unit]("close"))
              }): _ *
            )
          )),
          assign(i, add(load(i), constant(1)))
        ))
      ),
      setField(perArgsField, newInstance(constructor[AggOut])),
      inner.genCloseOutput
    )
  }

  override def genOutputBuffer: Option[IntermediateRepresentation] = Some(loadField(bufferIdField))

  override def genFields: Seq[Field] = Seq(perArgsField, sinkField, bufferIdField, memoryTrackerField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, aggPreMapVar)

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions ++ Seq(compiledGroupingExpression)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
