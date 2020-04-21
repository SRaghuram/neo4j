/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AggregatingAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Updater
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createAggregators
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createUpdaters
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.EXECUTION_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.setMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer

case class AggregationOperatorNoGrouping(workIdentity: WorkIdentity,
                                         aggregations: Array[Aggregator]) {

  def mapper(argumentSlotOffset: Int,
             outputBufferId: BufferId,
             expressionValues: Array[Expression],
             operatorId: Id) =
    new AggregationMapperOperatorNoGrouping(workIdentity,
      argumentSlotOffset,
      outputBufferId,
      aggregations,
      expressionValues)(operatorId)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int],
              operatorId: Id) =
    new AggregationReduceOperatorNoGrouping(argumentStateMapId,
      workIdentity,
      aggregations,
      reducerOutputSlots)(operatorId)

  // =========== THE MAPPER ============

  /**
   * Pre-operator for aggregations with no grouping. This performs local aggregation of the
   * data in a single morsel at a time, before putting these local aggregations into the
   * [[ExecutionState]] buffer which perform the final global aggregation.
   */
  class AggregationMapperOperatorNoGrouping(val workIdentity: WorkIdentity,
                                            argumentSlotOffset: Int,
                                            outputBufferId: BufferId,
                                            aggregations: Array[Aggregator],
                                            expressionValues: Array[Expression])
                                           (val id: Id = Id.INVALID_ID)
    extends OutputOperator {

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      new State(executionState.getSink[IndexedSeq[PerArgument[Array[Updater]]]](outputBufferId), memoryTracker)
    }

    class State(sink: Sink[IndexedSeq[PerArgument[Array[Updater]]]], memoryTracker: MemoryTracker) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = AggregationMapperOperatorNoGrouping.this.workIdentity

      override def trackTime: Boolean = true

      override def prepareOutput(outputMorsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreAggregatedOutput = {

        val queryState = state.queryStateForExpressionEvaluation(resources)

        val preAggregated = ArgumentStateMap.map(argumentSlotOffset,
          outputMorsel,
          preAggregate(queryState))

        new PreAggregatedOutput(preAggregated, sink)
      }

      private def preAggregate(queryState: QueryState)(morsel: Morsel): Array[Updater] = {
        val updaters = aggregations.map(_.newUpdater(memoryTracker))
        //loop over the entire morsel view and apply the aggregation
        val cursor = morsel.readCursor()
        while (cursor.next()) {
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(cursor, queryState)
            updaters(i).update(value)
            i += 1
          }
        }
        updaters
      }
    }

    class PreAggregatedOutput(preAggregated: IndexedSeq[PerArgument[Array[Updater]]],
                              sink: Sink[IndexedSeq[PerArgument[Array[Updater]]]]) extends PreparedOutput {
      override def produce(): Unit = sink.put(preAggregated)

      override def close(): Unit = {
        var i = 0
        while (i < preAggregated.size) {
          val perArgument = preAggregated(i)
          val updaters = perArgument.value
          var j = 0
          while (j < updaters.length) {
            updaters(j).close()
            j += 1
          }
          i += 1
        }
        super.close()
      }
    }
  }

  // =========== THE REDUCER ============

  /**
   * Operator which streams aggregated data, built by [[AggregationMapperOperatorNoGrouping]] and [[AggregatingAccumulator]].
   */
  class AggregationReduceOperatorNoGrouping(val argumentStateMapId: ArgumentStateMapId,
                                            val workIdentity: WorkIdentity,
                                            aggregations: Array[Aggregator],
                                            reducerOutputSlots: Array[Int])
                                           (val id: Id = Id.INVALID_ID)
    extends Operator
    with ReduceOperatorState[Array[Updater], AggregatingAccumulator] {

    override def accumulatorsPerTask(morselSize: Int): Int = morselSize

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): ReduceOperatorState[Array[Updater], AggregatingAccumulator] = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations, memoryTracker))
      this
    }

    override def nextTasks(state: PipelinedQueryState, input: IndexedSeq[AggregatingAccumulator], resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Array[Updater], AggregatingAccumulator]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[AggregatingAccumulator]) extends ContinuableOperatorTaskWithAccumulators[Array[Updater], AggregatingAccumulator] {

      override def workIdentity: WorkIdentity = AggregationReduceOperatorNoGrouping.this.workIdentity

      override def operate(outputMorsel: Morsel,
                           state: PipelinedQueryState,
                           resources: QueryResources): Unit = {

        val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
        val iter = accumulators.iterator
        while (iter.hasNext) { // guaranteed to be < morsel size
          val accumulator = iter.next()
          outputCursor.copyFrom(accumulator.argumentRow)
          var i = 0
          while (i < aggregations.length) {
            outputCursor.setRefAt(reducerOutputSlots(i), accumulator.result(i))
            i += 1
          }
          outputCursor.next()
        }
        outputCursor.truncate()
      }

      // This operator will never continue since it will always write a single row
      override def canContinue: Boolean = false
    }
  }
}

class AggregationMapperOperatorNoGroupingTaskTemplate(val inner: OperatorTaskTemplate,
                                                      override val id: Id,
                                                      argumentSlotOffset: Int,
                                                      aggregators: Array[Aggregator],
                                                      outputBufferId: BufferId,
                                                      aggregationExpressionsCreator: () => Array[IntermediateExpression],
                                                      aggregationExpressions: Array[expressions.Expression])
                                                     (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  type Agg = Array[Any]
  type AggOut = scala.collection.mutable.ArrayBuffer[PerArgument[Agg]]

  override def toString: String = "AggregationMapperNoGroupingOperatorTaskTemplate"

  private val perArgsField: Field = field[AggOut](codeGen.namer.nextVariableName())
  private val sinkField: Field = field[Sink[IndexedSeq[PerArgument[Agg]]]](codeGen.namer.nextVariableName())
  private val bufferIdField: Field = field[Int](codeGen.namer.nextVariableName())
  private val memoryTrackerField = field[MemoryTracker](codeGen.namer.nextVariableName("memoryTracker"))

  private val aggregatorsVar = variable[Array[Aggregator]](codeGen.namer.nextVariableName(), createAggregators(aggregators))
  private val argVar = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private val updatersVar = variable[Agg](codeGen.namer.nextVariableName(), constant(null))

  private var compiledAggregationExpressions: Array[IntermediateExpression] = _

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
    }

    /**
     *
     * // this is the final result: a list of pre-aggregations, one per argument
     * val perArgs = new ArrayList<PerArgument<Updater[]>>()
     *
     * // last seen argument
     * long arg = -1
     *
     * {{{
     *
     *   // ----- track when argument changes & create updaters group -----
     *
     *   val currentArg = getFromLongSlot(argumentSlotOffset)
     *   if (currentArg != arg) {
     *     arg = currentArg
     *     updaters = new Updater[]{ aggregations[0].newUpdater,
     *                               aggregations[1].newUpdater,
     *                               ...
     *                               aggregations[n-1].newUpdater}
     *     val perArg = new PerArgument<Updater[]>(arg, updaters)
     *     perArgs.add(perArg)
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

    block(

      /*
       * val currentArg = getFromLongSlot(argumentSlotOffset)
       * if (currentArg != arg) {
       *   arg = currentArg
       *   updaters = new Updater[]{ aggregations[0].newUpdater,
       *                             aggregations[1].newUpdater,
       *                             ...
       *                             aggregations[n-1].newUpdater}
       *   perArgs.add(new PerArgument<Updater[]>(arg, updaters))
       * }
       */
      declareAndAssign(typeRefOf[Long], currentArg, codeGen.getArgumentAt(argumentSlotOffset)),
      condition(notEqual(load(currentArg), load(argVar)))(
        block(
          assign(argVar, load(currentArg)),
          assign(updatersVar, createUpdaters(aggregators, load(aggregatorsVar), loadField(memoryTrackerField))),
          invokeSideEffect(loadField(perArgsField),
            method[ArrayBuffer[_], ArrayBuffer[_], Any]("$plus$eq"),
            newInstance(constructor[PerArgument[Agg], Long, Any], load(argVar), load(updatersVar)))
        )),

      /*
       * updaters[0].update(aggregationExpression[0]())
       * updaters[1].update(aggregationExpression[1]())
       * ...
       * updaters[n-1].update(aggregationExpression[n-1]())
       */
      block(
        compiledAggregationExpressions.indices.map(i => {
          invokeSideEffect(arrayLoad(cast[Array[Updater]](load(updatersVar)), i), method[Updater, Unit, AnyValue]("update"),
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
        method[Sink[_], Unit, Any]("put"),
        loadField(perArgsField)),
      setField(perArgsField, newInstance(constructor[AggOut]))
    )
  }

  override def genCloseOutput: IntermediateRepresentation = {
    // TODO: Close updaters!
    noop()
  }

  override def genOutputBuffer: Option[IntermediateRepresentation] = Some(loadField(bufferIdField))

  override def genFields: Seq[Field] = Seq(perArgsField, sinkField, bufferIdField, memoryTrackerField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, updatersVar)

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
