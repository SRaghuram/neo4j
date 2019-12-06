/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.{AggregatingAccumulator, Aggregator, Updater}
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.pipelined.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.pipelined.{ArgumentStateMapCreator, ExecutionState, OperatorExpressionCompiler}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.expressions.{Expression => AstExpression}
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer

case class AggregationOperatorNoGrouping(workIdentity: WorkIdentity,
                                         aggregations: Array[Aggregator]) {

  def mapper(argumentSlotOffset: Int,
             outputBufferId: BufferId,
             expressionValues: Array[Expression]) =
    new AggregationMapperOperatorNoGrouping(workIdentity,
                                            argumentSlotOffset,
                                            outputBufferId,
                                            aggregations,
                                            expressionValues)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int]) =
    new AggregationReduceOperatorNoGrouping(argumentStateMapId,
                                            workIdentity,
                                            aggregations,
                                            reducerOutputSlots)

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
                                            expressionValues: Array[Expression]) extends OutputOperator {

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState,
                             pipelineId: PipelineId): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[Array[Updater]]]](pipelineId, outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[Array[Updater]]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = AggregationMapperOperatorNoGrouping.this.workIdentity

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

      private def preAggregate(queryState: OldQueryState)(morsel: MorselExecutionContext): Array[Updater] = {
        val updaters = aggregations.map(_.newUpdater)
        //loop over the entire morsel view and apply the aggregation
        while (morsel.isValidRow) {
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(morsel, queryState)
            updaters(i).update(value)
            i += 1
          }
          morsel.moveToNextRow()
        }
        updaters
      }
    }

    class PreAggregatedOutput(preAggregated: IndexedSeq[PerArgument[Array[Updater]]],
                              sink: Sink[IndexedSeq[PerArgument[Array[Updater]]]]) extends PreparedOutput {
      override def produce(): Unit = sink.put(preAggregated)
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
    extends Operator
      with ReduceOperatorState[Array[Updater], AggregatingAccumulator] {

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             queryContext: QueryContext,
                             state: QueryState,
                             resources: QueryResources): ReduceOperatorState[Array[Updater], AggregatingAccumulator] = {
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations, stateFactory.memoryTracker))
      this
    }

    override def nextTasks(queryContext: QueryContext,
                           state: QueryState,
                           input: AggregatingAccumulator,
                           resources: QueryResources
                          ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[Array[Updater], AggregatingAccumulator]] = {
      Array(new OTask(input))
    }

    class OTask(override val accumulator: AggregatingAccumulator) extends ContinuableOperatorTaskWithAccumulator[Array[Updater], AggregatingAccumulator] {

      override def workIdentity: WorkIdentity = AggregationReduceOperatorNoGrouping.this.workIdentity

      override def operate(outputRow: MorselExecutionContext,
                           context: QueryContext,
                           state: QueryState,
                           resources: QueryResources): Unit = {

        var i = 0
        while (i < aggregations.length) {
          outputRow.setRefAt(reducerOutputSlots(i), accumulator.result(i))
          i += 1
        }
        outputRow.moveToNextRow()
        outputRow.finishedWriting()
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
                                                      aggregationExpressions: Array[AstExpression])
                                                     (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  import OperatorCodeGenHelperTemplates._
  import org.neo4j.codegen.api.IntermediateRepresentation._
  import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate._

  type Agg = Array[Any]
  type AggOut = scala.collection.mutable.ArrayBuffer[PerArgument[Agg]]

  override def toString: String = "AggregationMapperNoGroupingOperatorTaskTemplate"

  private val perArgsField: Field = field[AggOut](codeGen.namer.nextVariableName())
  private val sinkField: Field = field[Sink[IndexedSeq[PerArgument[Agg]]]](codeGen.namer.nextVariableName())
  private val bufferIdField: Field = field[Int](codeGen.namer.nextVariableName())

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
          assign(updatersVar, createUpdaters(aggregators, load(aggregatorsVar))),
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

  override protected def genCreateState: IntermediateRepresentation = {
    block(
      setField(sinkField,
               invoke(EXECUTION_STATE,
                      method[ExecutionState, Sink[_], Int, Int]("getSinkInt"),
                      PIPELINE_ID,
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

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, updatersVar)

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
