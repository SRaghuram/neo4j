/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
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
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createAggregators
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_RESOURCES
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.AggregatedRow
import org.neo4j.cypher.internal.runtime.pipelined.state.AggregatedRowAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.AggregatedRowUpdaters
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

case class AggregationOperatorNoGrouping(workIdentity: WorkIdentity,
                                         aggregations: Array[Aggregator]) {

  def mapper(argumentSlotOffset: Int,
             argumentStateMapId: ArgumentStateMapId,
             expressionValues: Array[Array[Expression]],
             operatorId: Id) =
    new AggregationMapperOperatorNoGrouping(workIdentity,
      argumentSlotOffset,
      argumentStateMapId,
      aggregations,
      expressionValues)(operatorId)

  def mapper(argumentSlotOffset: Int,
             argumentStateMapId: ArgumentStateMapId,
             expressionValues: Array[Expression],
             operatorId: Id) =
    new SingleAggregationMapperOperatorNoGrouping(workIdentity,
      argumentSlotOffset,
      argumentStateMapId,
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
   *
   * NOTE: should only be used for aggregation functions taking a single argument (or no argument)
   */
  class SingleAggregationMapperOperatorNoGrouping(val workIdentity: WorkIdentity,
                                            argumentSlotOffset: Int,
                                            argumentStateMapId: ArgumentStateMapId,
                                            aggregations: Array[Aggregator],
                                            expressionValues: Array[Expression])
                                           (val id: Id = Id.INVALID_ID)
    extends OutputOperator {

    override def outputBuffer: Option[BufferId] = None

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
      new State(executionState.argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[AggregatedRowAccumulator]])
    }

    class State(aggregatorRows: ArgumentStateMap[AggregatedRowAccumulator]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = SingleAggregationMapperOperatorNoGrouping.this.workIdentity

      override def trackTime: Boolean = true

      override def prepareOutput(outputMorsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {

        val queryState = state.queryStateForExpressionEvaluation(resources)

        var argumentRowId = -1L
        var update: AggregatedRowUpdaters = null
        val readCursor = outputMorsel.readCursor()
        while (readCursor.next()) {
          val arg = ArgumentSlots.getArgumentAt(readCursor, argumentSlotOffset)
          if (arg != argumentRowId) {
            if (update != null) {
              update.applyUpdates()
            }
            update = aggregatorRows.peek(arg).aggregatorRow.updaters(resources.workerId)
            argumentRowId = arg
          }

          var i = 0
          while (i < aggregations.length) {
            update.initialize(i, queryState)
            val value = expressionValues(i)(readCursor, queryState)
            update.addUpdate(i, value)
            i += 1
          }
        }

        if (update != null) {
          update.applyUpdates()
        }

        NoOutputOperator
      }
    }
  }

  /**
   * Pre-operator for aggregations with no grouping. This performs local aggregation of the
   * data in a single morsel at a time, before putting these local aggregations into the
   * [[ExecutionState]] buffer which perform the final global aggregation.
   */
  class AggregationMapperOperatorNoGrouping(val workIdentity: WorkIdentity,
                                            argumentSlotOffset: Int,
                                            argumentStateMapId: ArgumentStateMapId,
                                            aggregations: Array[Aggregator],
                                            expressionValues: Array[Array[Expression]])
                                           (val id: Id = Id.INVALID_ID)
    extends OutputOperator {

    override def outputBuffer: Option[BufferId] = None

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
      new State(executionState.argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[AggregatedRowAccumulator]])
    }

    class State(aggregatorRows: ArgumentStateMap[AggregatedRowAccumulator]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = AggregationMapperOperatorNoGrouping.this.workIdentity

      override def trackTime: Boolean = true

      override def prepareOutput(outputMorsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {

        val queryState = state.queryStateForExpressionEvaluation(resources)

        var argumentRowId = -1L
        var update: AggregatedRowUpdaters = null
        val readCursor = outputMorsel.readCursor()
        while (readCursor.next()) {
          val arg = ArgumentSlots.getArgumentAt(readCursor, argumentSlotOffset)
          if (arg != argumentRowId) {
            if (update != null) {
              update.applyUpdates()
            }
            update = aggregatorRows.peek(arg).aggregatorRow.updaters(resources.workerId)
            argumentRowId = arg
          }

          var i = 0
          while (i < aggregations.length) {
            update.initialize(i, queryState)
            val arguments = expressionValues(i)
            val input = new Array[AnyValue](arguments.length)
            var j = 0
            while (j < arguments.length) {
              input(j) = arguments(j)(readCursor, queryState)
              j += 1
            }
            update.addUpdate(i, input)
            i += 1
          }
        }

        if (update != null) {
          update.applyUpdates()
        }

        NoOutputOperator
      }
    }
  }

  // =========== THE REDUCER ============

  /**
   * Operator which streams aggregated data, built by [[AggregationMapperOperatorNoGrouping]] and [[AggregatedRowAccumulator]].
   */
  class AggregationReduceOperatorNoGrouping(val argumentStateMapId: ArgumentStateMapId,
                                            val workIdentity: WorkIdentity,
                                            aggregations: Array[Aggregator],
                                            reducerOutputSlots: Array[Int])
                                           (val id: Id = Id.INVALID_ID)
    extends Operator
    with AccumulatorsInputOperatorState[AnyRef, AggregatedRowAccumulator] {

    override def accumulatorsPerTask(morselSize: Int): Int = morselSize

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): AccumulatorsInputOperatorState[AnyRef, AggregatedRowAccumulator] = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatedRowAccumulator.Factory(aggregations, memoryTracker, state.numberOfWorkers))
      this
    }

    override def nextTasks(input: IndexedSeq[AggregatedRowAccumulator],
                           resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[AnyRef, AggregatedRowAccumulator]] = {
      singletonIndexedSeq(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[AggregatedRowAccumulator])
      extends ContinuableOperatorTaskWithAccumulators[AnyRef, AggregatedRowAccumulator] {

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
            outputCursor.setRefAt(reducerOutputSlots(i), accumulator.aggregatorRow.result(i))
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

abstract class BaseAggregationMapperOperatorNoGroupingTaskTemplate(val inner: OperatorTaskTemplate,
                                                                   override val id: Id,
                                                                   argumentSlotOffset: Int,
                                                                   aggregators: Array[Aggregator],
                                                                   argumentStateMapId: ArgumentStateMapId,
                                                                   serialExecutionOnly: Boolean = false,
                                                                   codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  override protected def isHead: Boolean = false

  override def toString: String = "AggregationMapperNoGroupingOperatorTaskTemplate"

  private val needToApplyUpdates = !Aggregator.allDirect(aggregators) || !serialExecutionOnly

  private val asmField: Field =
    field[ArgumentStateMap[AggregatedRowAccumulator]](codeGen.namer.nextVariableName("aggregatedRows"),
      argumentStateMap[AggregatedRowAccumulator](argumentStateMapId))

  private val aggregatorsVar = variable[Array[Aggregator]](codeGen.namer.nextVariableName("aggregators"), createAggregators(aggregators))
  private val argVar = variable[Long](codeGen.namer.nextVariableName("arg"), constant(-1L))
  protected val updatersVar: LocalVariable = variable[AggregatedRowUpdaters](codeGen.namer.nextVariableName("updaters"), constant(null))
  private val workerIdVar = variable[Int](codeGen.namer.nextVariableName("workerId"), constant(-1))

  protected def setupAggregation(): Unit
  protected def addUpdates: IntermediateRepresentation

  // constructor
  override def genInit: IntermediateRepresentation = inner.genInit

  override def genOperateEnter: IntermediateRepresentation = {
    block(
      assign(workerIdVar, invoke(QUERY_RESOURCES, method[QueryResources, Int]("workerId"))),
      super.genOperateEnter)
  }

  // this operates on a single row only
  override def genOperate: IntermediateRepresentation = {
    setupAggregation()

    /**
     * // last seen argument
     * long arg = -1
     *
     * // updaters for the current aggregated row
     * val updaters = null
     *
     * {{{
     *
     *   // ----- track when argument changes & create updaters group -----
     *
     *   val currentArg = getFromLongSlot(argumentSlotOffset)
     *   if (currentArg != arg) {
     *     arg = currentArg
     *     if (updaters != null) {
     *       updaters.applyUpdates()
     *     }
     *     updaters = aggregatedRows.peek(arg).aggregatorRow.updaters(workerId)
     *   }
     *
     *   // ----- aggregate -----
     *
     *   {
     *     updaters.updater(0).add(aggregationExpression[0]())
     *     updaters.updater(1).add(aggregationExpression[1]())
     *     ...
     *     updaters.updater(n-1).add(aggregationExpression[n-1]())
     *   }
     * }}}
     *
     */

    // argument of current morsel row
    val currentArg = codeGen.namer.nextVariableName()

    block(
      /*
       * if (scopedMemoryTracker == null) {
       *   scopedMemoryTracker = new ScopedMemoryTracker(memoryTracker)
       * }
       */

      /*
       * val currentArg = getFromLongSlot(argumentSlotOffset)
       * if (currentArg != arg) {
       *   arg = currentArg
       *   if (updaters != null) {   // ---
       *     updaters.applyUpdates() // If all aggregators are direct we skip this code
       *   }                         // ---
       *   updaters = aggregatedRows.peek(arg).aggregatorRow.updaters(workerId)
       * }
       */
      declareAndAssign(typeRefOf[Long], currentArg, codeGen.getArgumentAt(argumentSlotOffset)),
      condition(notEqual(load(currentArg), load(argVar)))(
        block(
          assign(argVar, load(currentArg)),
          genApplyUpdates,
          assign(updatersVar,
            invoke(
              invoke(
                OperatorCodeGenHelperTemplates.peekState[AggregatedRowAccumulator](loadField(asmField), load(argVar)),
                method[AggregatedRowAccumulator, AggregatedRow]("aggregatorRow")),
              method[AggregatedRow, AggregatedRowUpdaters, Int]("updaters"),
              load(workerIdVar)
            ))
        )),

      /*
       * updaters.updater(0).add(aggregationExpression[0]())
       * updaters.updater(1).add(aggregationExpression[1]())
       * ...
       * updaters.updater(n-1).add(aggregationExpression[n-1]())
       */
      addUpdates
      ,

      inner.genOperateWithExpressions
    )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      genApplyUpdates,
      super.genOperateExit
    )
  }

  private def genApplyUpdates: IntermediateRepresentation = {
    if (needToApplyUpdates) {
      condition(IntermediateRepresentation.isNotNull(load(updatersVar)))(
        invokeSideEffect(load(updatersVar), method[AggregatedRowUpdaters, Unit]("applyUpdates"))
      )
    } else {
      noop()
    }
  }

  override def genOutputBuffer: Option[IntermediateRepresentation] = None

  override def genFields: Seq[Field] = Seq(asmField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, updatersVar, workerIdVar)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}

class SingleArgumentAggregationMapperOperatorNoGroupingTaskTemplate(inner: OperatorTaskTemplate,
                                                                    id: Id,
                                                                    argumentSlotOffset: Int,
                                                                    aggregators: Array[Aggregator],
                                                                    argumentStateMapId: ArgumentStateMapId,
                                                                    aggregationExpressionsCreator: () => Array[IntermediateExpression],
                                                                    serialExecutionOnly: Boolean = false)
                                                                   (protected val codeGen: OperatorExpressionCompiler)
  extends BaseAggregationMapperOperatorNoGroupingTaskTemplate(inner, id, argumentSlotOffset, aggregators, argumentStateMapId, serialExecutionOnly, codeGen) {

  override def toString: String = "AggregationMapperNoGroupingOperatorTaskTemplate"

  private var compiledAggregationExpressions: Array[IntermediateExpression] = _

  override protected def setupAggregation(): Unit = {
    if (null == compiledAggregationExpressions) {
      compiledAggregationExpressions = aggregationExpressionsCreator()
    }
  }

  override protected def addUpdates: IntermediateRepresentation = block(
    compiledAggregationExpressions.indices.map(i => {
      block(
        invokeSideEffect(load(updatersVar), method[AggregatedRowUpdaters, Unit, Int, QueryState]("initialize"),constant(i), QUERY_STATE),
      invokeSideEffect(load(updatersVar), method[AggregatedRowUpdaters, Unit, Int, AnyValue]("addUpdate"),
        constant(i),
        nullCheckIfRequired(compiledAggregationExpressions(i)))
      )
    }): _ *
  )

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions.toSeq
}

class AggregationMapperOperatorNoGroupingTaskTemplate(inner: OperatorTaskTemplate,
                                                      id: Id,
                                                      argumentSlotOffset: Int,
                                                      aggregators: Array[Aggregator],
                                                      argumentStateMapId: ArgumentStateMapId,
                                                      aggregationExpressionsCreator: () => Array[Array[IntermediateExpression]],
                                                      serialExecutionOnly: Boolean = false)
                                                     (protected val codeGen: OperatorExpressionCompiler)
  extends BaseAggregationMapperOperatorNoGroupingTaskTemplate(inner, id, argumentSlotOffset, aggregators, argumentStateMapId, serialExecutionOnly, codeGen) {

  override def toString: String = "AggregationMapperNoGroupingOperatorTaskTemplate"

  private var compiledAggregationExpressions: Array[Array[IntermediateExpression]] = _

  override protected def setupAggregation(): Unit = {
    if (null == compiledAggregationExpressions) {
      compiledAggregationExpressions = aggregationExpressionsCreator()
    }
  }

  override protected def addUpdates: IntermediateRepresentation = block(
    compiledAggregationExpressions.indices.map(i => {
      block(
        invokeSideEffect(load(updatersVar), method[AggregatedRowUpdaters, Unit, Int, QueryState]("initialize"),constant(i), QUERY_STATE),
      invokeSideEffect(load(updatersVar), method[AggregatedRowUpdaters, Unit, Int, Array[AnyValue]]("addUpdate"),
        constant(i),
        arrayOf[AnyValue](compiledAggregationExpressions(i).map(e => nullCheckIfRequired(e)): _*))
      )
    }): _ *
  )

  override def genExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions.flatten.toSeq
}
