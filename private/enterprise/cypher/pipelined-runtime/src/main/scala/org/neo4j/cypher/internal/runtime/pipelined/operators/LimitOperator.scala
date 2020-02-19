/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThan
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.TopLevelArgument
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.LimitOperator.LimitState
import org.neo4j.cypher.internal.runtime.pipelined.operators.LimitOperator.evaluateCountValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_COUNTER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_ROW
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SHOULD_BREAK
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRows
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelLimitOperatorTaskTemplate.SerialTopLevelLimitState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue

object LimitOperator {
  def evaluateCountValue(queryContext: QueryContext,
                         state: QueryState,
                         resources: QueryResources,
                         countExpression: Expression): Long = {
    val queryState = new SlottedQueryState(queryContext,
      resources = null,
      params = state.params,
      resources.expressionCursors,
      Array.empty[IndexReadSession],
      resources.expressionVariables(state.nExpressionSlots),
      state.subscriber,
      NoMemoryTracker)

    val countValue = countExpression(CypherRow.empty, queryState)
    evaluateCountValue(countValue)
  }

  def evaluateCountValue(countValue: AnyValue): Long = {
    val limitNumber = NumericHelper.asNumber(countValue)
    if (limitNumber.isInstanceOf[FloatingPointValue]) {
      val limit = limitNumber.doubleValue()
      throw new InvalidArgumentException(s"LIMIT: Invalid input. '$limit' is not a valid value. Must be a non-negative integer.")
    }
    val limit = limitNumber.longValue()

    if (limit < 0) {
      throw new InvalidArgumentException(s"LIMIT: Invalid input. '$limit' is not a valid value. Must be a non-negative integer.")
    }
    limit
  }

  class LimitStateFactory(count: Long) extends ArgumentStateFactory[LimitState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): LimitState =
      new StandardLimitState(argumentRowId, count, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): LimitState =
      new ConcurrentLimitState(argumentRowId, count, argumentRowIdsForReducers)
  }

  /**
   * Query-wide row count for the rows from one argumentRowId.
   */
  abstract class LimitState extends WorkCanceller {
    def reserve(wanted: Long): Long
  }

  class StandardLimitState(override val argumentRowId: Long,
                           countTotal: Long,
                           override val argumentRowIdsForReducers: Array[Long]) extends LimitState {
    private var countLeft = countTotal

    override def reserve(wanted: Long): Long = {
      val got = math.min(countLeft, wanted)
      countLeft -= got
      got
    }

    override def isCancelled: Boolean = countLeft == 0

    override def toString: String = s"StandardLimitState($argumentRowId, countLeft=$countLeft)"
  }

  class ConcurrentLimitState(override val argumentRowId: Long,
                             countTotal: Long,
                             override val argumentRowIdsForReducers: Array[Long]) extends LimitState {
    private val countLeft = new AtomicLong(countTotal)

    def reserve(wanted: Long): Long = {
      if (countLeft.get() <= 0) {
        0L
      } else {
        val newCountLeft = countLeft.addAndGet(-wanted)
        if (newCountLeft >= 0) {
          wanted
        } else {
          math.max(0L, wanted + newCountLeft)
        }
      }
    }

    override def isCancelled: Boolean = countLeft.get() <= 0

    override def toString: String = s"ConcurrentLimitState($argumentRowId, countLeft=${countLeft.get()})"
  }
}

/**
 * Limit the number of rows to `countExpression` per argument.
 */
class LimitOperator(argumentStateMapId: ArgumentStateMapId,
                    val workIdentity: WorkIdentity,
                    countExpression: Expression) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator, stateFactory: StateFactory, queryContext: QueryContext, state: QueryState, resources: QueryResources): OperatorTask = {
    val limit = evaluateCountValue(queryContext, state, resources, countExpression)
    new LimitOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
      new LimitOperator.LimitStateFactory(limit)))
  }

  class LimitOperatorTask(argumentStateMap: ArgumentStateMap[LimitState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = LimitOperator.this.workIdentity

    override def operate(output: MorselCypherRow,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      argumentStateMap.filter[FilterState](output,
        (rowCount, nRows) => new FilterState(rowCount.reserve(nRows)),
        (x, _) => x.next())
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  }

  /**
   * Filter state for the rows from one argumentRowId within one morsel.
   */
  class FilterState(var countLeft: Long) {
    def next(): Boolean = {
      if (countLeft > 0) {
        countLeft -= 1
        true
      } else
        false
    }
  }

}

/**
 * This is a (common) special case used when not nested under an apply, so we do not need to worry about the argument state map.
 * It also needs to be run either single threaded execution or in a serial pipeline (i.e. the final produce pipeline) in parallel execution,
 * since it does not synchronize the limit count between tasks.
 */
class SerialTopLevelLimitOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                              override val id: Id,
                                              innermost: DelegateOperatorTaskTemplate,
                                              argumentStateMapId: ArgumentStateMapId,
                                              generateCountExpression: () => IntermediateExpression)
                                             (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {


  private var countExpression: IntermediateExpression = _
  private val countLeftVar: LocalVariable = variable[Long](codeGen.namer.nextVariableName("countLeft"), constant(0L))
  private val reservedVar: LocalVariable = variable[Long](codeGen.namer.nextVariableName("reserved"), constant(0L))
  private val limitStateField = field[SerialTopLevelLimitState](codeGen.namer.nextVariableName("limitState"),
    // Get the limit operator state from the ArgumentStateMaps that is passed to the constructor
    // We do not generate any checks or error handling code, so the runtime compiler is responsible for this fitting together perfectly
    cast[SerialTopLevelLimitState](
      invoke(
        invoke(load(ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER.name), method[ArgumentStateMaps, ArgumentStateMap[_ <: ArgumentState], Int]("applyByIntId"),
          constant(argumentStateMapId.x)),
        method[ArgumentStateMap[_ <: ArgumentState], ArgumentState, Long]("peek"),
        constant(TopLevelArgument.VALUE)
      )
    )
  )

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genExpressions: Seq[IntermediateExpression] = Seq(countExpression)

  override def genOperateEnter: IntermediateRepresentation = {
    if (countExpression == null) {
      countExpression = generateCountExpression()
    }

    val howMuchToReserve: IntermediateRepresentation =
      if (innermost.shouldWriteToContext) {
        // Use the available output morsel rows to determine our maximum chunk of the total limit
        cast[Long](invoke(OUTPUT_ROW, method[MorselCypherRow, Int]("getValidRows")))
      } else if (innermost.shouldCheckOutputCounter) {
        // Use the output counter to determine our maximum chunk of the total limit
        cast[Long](load(OUTPUT_COUNTER))
      } else {
        // We do not seem to have any bound on the output of this task (i.e. we are the final produce result pipeline task)
        // Reserve as much as we can get
        constant(Long.MaxValue)
      }

    // Initialize the limit state
    // NOTE: We would typically prefer to do this in the constructor, but that is called using reflection, and the error handling
    // does not work so well when exceptions are thrown from evaluateCountValue (which can be expected due to it performing user error checking!)
    block(
      condition(invoke(loadField(limitStateField), method[SerialTopLevelLimitState, Boolean]("isUninitialized")))(
        invoke(loadField(limitStateField), method[SerialTopLevelLimitState, Unit, Long]("initialize"),
          invokeStatic(method[LimitOperator, Long, AnyValue]("evaluateCountValue"), countExpression.ir))
      ),
      assign(reservedVar, invoke(loadField(limitStateField), method[LimitState, Long, Long]("reserve"), howMuchToReserve)),
      assign(countLeftVar, load(reservedVar)),
      inner.genOperateEnter
    )
  }

  override def genOperate: IntermediateRepresentation = {
    block(
      condition(greaterThan(load(countLeftVar), constant(0L))) (
        block(
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
              assign(countLeftVar, subtract(load(countLeftVar), constant(1L)))))
        ),
      doIfInnerCantContinue(
        condition(equal(load(countLeftVar), constant(0L)))(
          setField(SHOULD_BREAK, constant(true)))
        )
      )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      invoke(loadField(limitStateField), method[SerialTopLevelLimitState, Unit, Long]("update"), subtract(load(reservedVar), load(countLeftVar))),
      profileRows(id, cast[Int](subtract(load(reservedVar), load(countLeftVar)))),
      condition(greaterThan(load(reservedVar), constant(0L)))(
        setField(SHOULD_BREAK, constant(false))
        ),
      inner.genOperateExit
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq(countLeftVar, reservedVar)

  override def genFields: Seq[Field] = Seq(limitStateField)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}

object SerialTopLevelLimitOperatorTaskTemplate {

  /**
   * This LimitState is intended for use with `SerialTopLevelLimitOperatorTaskTemplate`.
   *
   * The SerialTopLevelLimitState (x) while be used in the following way from compiled code:
   *
   * {{{
   *   def operate() {
   *     if (x.isUnitialized) x.setCount(<THE_LIMIT_COUNT>) // setCount happens once
   *     val reserved = x.reserve(<RESERVE_NBR>) // we don't know how many rows we'll get yet, so just
   *                                             // reserve some number. Note that reserve itself does
   *                                             // not modify the state, that only happens on update.
   *
   *     val countLeft = reserved
   *     ...
   *       countLeft -= 1
   *     ...
   *
   *     x.update(reserved - countLeft) // Before returning we update the state.
   *   }
   * }}}
   */
  abstract class SerialTopLevelLimitState extends LimitState {

    protected def getCount: Long
    protected def setCount(count: Long): Unit

    // True iff initialize has never been called
    def isUninitialized: Boolean = getCount == -1L

    // Initialize the count. Intended to be called only once.
    def initialize(count: Long): Unit = {
      Preconditions.checkState(isUninitialized, "Can only call initialize once")
      setCount(count)
    }

    // Update this state with the number of rows that passed the limit.
    def update(usedCount: Long): Unit = {
      Preconditions.checkState(usedCount >= 0, "Can not have used a negative number of rows")
      val newCount = getCount - usedCount
      Preconditions.checkState(newCount >= 0, "Used more rows than had count left")
      setCount(newCount)
    }

    override def reserve(wanted: Long): Long = {
      val count = getCount
      Preconditions.checkState(count != -1, "SerialTopLevelLimitState has not been initialized")
      math.min(count, wanted)
    }

    override def isCancelled: Boolean = getCount == 0
  }

  // This is used by fused limit in a serial pipeline, i.e. only safe to use in single-threaded execution or by a serial pipeline in parallel execution
  object SerialTopLevelLimitStateFactory extends ArgumentStateFactory[SerialTopLevelLimitState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): SerialTopLevelLimitState =
      new StandardSerialTopLevelLimitState(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): SerialTopLevelLimitState =
    // NOTE: This is actually _not_ threadsafe and only safe to use in a serial pipeline!
      new VolatileSerialTopLevelLimitState(argumentRowId, argumentRowIdsForReducers)
  }

  class StandardSerialTopLevelLimitState(override val argumentRowId: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends SerialTopLevelLimitState {

    private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"StandardSerialTopLevelLimitState($argumentRowId, countLeft=$countLeft)"
  }

  /**
   * The SerialTopLevelLimitState intended for use with `SerialTopLevelLimitOperatorTaskTemplate` when used
   * in `ParallelRuntime`. It provides thread-safe calls of `isCancelled`, while all other methods have
   * to be accessed in serial.
   */
  class VolatileSerialTopLevelLimitState(override val argumentRowId: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends SerialTopLevelLimitState {

    @volatile private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"VolatileSerialTopLevelLimitState($argumentRowId, countLeft=$countLeft)"
  }
}
