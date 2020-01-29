/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.ConcurrentHashMap

<<<<<<< HEAD
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.cast
=======
>>>>>>> da402acfd95... Don't attribute any time to fused operators
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.method
<<<<<<< HEAD
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
=======
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.PipelineId
>>>>>>> da402acfd95... Don't attribute any time to fused operators
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
<<<<<<< HEAD
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregators
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountStarAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.MaxAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.MinAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Reducer
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.SumAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Updater
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createAggregators
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createUpdaters
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.EXECUTION_STATE
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.util.attribution.Id
=======
import org.neo4j.cypher.internal.runtime.pipelined.aggregators._
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.v4_0.expressions.{Expression => AstExpression}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
>>>>>>> da402acfd95... Don't attribute any time to fused operators
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

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
                                  expressionValues: Array[Expression]) extends OutputOperator {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[AggPreMap]]](outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[AggPreMap]]]) extends OutputOperatorState {

      override def trackTime: Boolean = true

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      override def prepareOutput(morsel: MorselExecutionContext,
                                 context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreAggregatedOutput = {

        val queryState = new SlottedQueryState(context,
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

      private def preAggregate(queryState: SlottedQueryState)
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
                                       memoryTracker: QueryMemoryTracker,
                                       operatorId: Id) extends AggregatingAccumulator {

    val reducerMap = new java.util.LinkedHashMap[groupings.KeyType, Array[Reducer]]

    override def update(data: AggPreMap): Unit = {
      val iterator = data.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val reducers = reducerMap.computeIfAbsent(entry.getKey, _ => {
          // Note: this allocation is currently never de-allocated
          memoryTracker.allocated(entry.getKey, operatorId.x)
          aggregators.map(_.newStandardReducer(memoryTracker, operatorId))
        })

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

    class Factory(aggregators: Array[Aggregator], memoryTracker: QueryMemoryTracker, operatorId: Id) extends ArgumentStateFactory[AggregatingAccumulator] {
      override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): AggregatingAccumulator =
        new StandardAggregatingAccumulator(argumentRowId, aggregators, argumentRowIdsForReducers, memoryTracker, operatorId)

      override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): AggregatingAccumulator =
        new ConcurrentAggregatingAccumulator(argumentRowId, aggregators, argumentRowIdsForReducers)
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

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             queryContext: QueryContext,
                             state: QueryState,
                             resources: QueryResources): ReduceOperatorState[AggPreMap, AggregatingAccumulator] = {
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations, stateFactory.memoryTracker, id))
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

object AggregationMapperOperatorTaskTemplate {
  def createAggregators(aggregators: Array[Aggregator]): IntermediateRepresentation = {
    val newAggregators = aggregators.map {
      case CountStarAggregator => getStatic[Aggregators,Aggregator]("COUNT_STAR")
      case CountAggregator => getStatic[Aggregators,Aggregator]("COUNT")
      case CountDistinctAggregator => getStatic[Aggregators,Aggregator]("COUNT_DISTINCT")
      case SumAggregator => getStatic[Aggregators,Aggregator]("SUM")
      case AvgAggregator => getStatic[Aggregators,Aggregator]("AVG")
      case MaxAggregator => getStatic[Aggregators,Aggregator]("MAX")
      case MinAggregator => getStatic[Aggregators,Aggregator]("MIN")
      case CollectAggregator => getStatic[Aggregators,Aggregator]("COLLECT")
      case aggregator =>
        throw new SyntaxException(s"Unexpected Aggregator: ${aggregator.getClass.getName}")
    }
    arrayOf[Aggregator](newAggregators: _ *)
  }

  def createUpdaters(aggregators: Array[Aggregator], aggregatorsVar: IntermediateRepresentation): IntermediateRepresentation = {
    val newUpdaters = aggregators.indices.map(i =>
      invoke(arrayLoad(aggregatorsVar, i), method[Aggregator, Updater]("newUpdater"))
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
<<<<<<< HEAD
=======
  import org.neo4j.codegen.api.IntermediateRepresentation._
  import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate._
  import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates._
>>>>>>> da402acfd95... Don't attribute any time to fused operators

  type AggMap = java.util.LinkedHashMap[AnyValue, Array[Any]]
  type AggOut = scala.collection.mutable.ArrayBuffer[PerArgument[AggMap]]

  override def toString: String = "AggregationMapperOperatorTaskTemplate"

  private val perArgsField: Field = field[AggOut](codeGen.namer.nextVariableName())
  private val sinkField: Field = field[Sink[IndexedSeq[PerArgument[AggMap]]]](codeGen.namer.nextVariableName())
  private val bufferIdField: Field = field[Int](codeGen.namer.nextVariableName())

  private val aggregatorsVar = variable[Array[Aggregator]](codeGen.namer.nextVariableName(), createAggregators(aggregators))
  private val argVar = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private val aggPreMapVar = variable[AggMap](codeGen.namer.nextVariableName(), constant(null))

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
          assign(updaters, createUpdaters(aggregators, load(aggregatorsVar))),
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

  override protected def genCreateState: IntermediateRepresentation = {
    block(
      setField(sinkField,
        invoke(EXECUTION_STATE,
          method[ExecutionState, Sink[_], Int]("getSinkInt"),
          loadField(bufferIdField)))
    )
  }

  override protected def genProduce: IntermediateRepresentation = {
    block(
      invokeSideEffect(loadField(sinkField),
        method[Sink[_], Unit, Any]("put"),
        loadField(perArgsField)),
      setField(perArgsField, newInstance(constructor[AggOut]))
    )
  }

  override def genOutputBuffer: Option[IntermediateRepresentation] = Some(loadField(bufferIdField))

  override def genFields: Seq[Field] = Seq(perArgsField, sinkField, bufferIdField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, aggPreMapVar)

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions ++ Seq(compiledGroupingExpression)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
