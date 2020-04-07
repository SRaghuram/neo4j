/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue

object CountingState {
  def evaluateCountValue(state: PipelinedQueryState,
                         resources: QueryResources,
                         countExpression: Expression): Long = {
    val queryState = state.queryStateForExpressionEvaluation(resources)

    val countValue = countExpression(CypherRow.empty, queryState)
    evaluateCountValue(countValue)
  }

  def evaluateCountValue(countValue: AnyValue): Long = {
    val countNumber = NumericHelper.asNumber(countValue)
    if (countNumber.isInstanceOf[FloatingPointValue]) {
      val count = countNumber.doubleValue()
      throw new InvalidArgumentException(s"Invalid input. '$count' is not a valid value. Must be a non-negative integer.")
    }
    val count = countNumber.longValue()

    if (count < 0) {
      throw new InvalidArgumentException(s"Invalid input. '$count' is not a valid value. Must be a non-negative integer.")
    }
    count
  }

  abstract class StandardCountingState(override val argumentRowId: Long,
                                       countTotal: Long,
                                       override val argumentRowIdsForReducers: Array[Long]) extends CountingState {
    protected var countLeft: Long = countTotal

    override def reserve(wanted: Int): Int = {
      val got = math.min(countLeft, wanted)
      countLeft -= got
      got.asInstanceOf[Int]//safe since got is always < wanted
    }
    override def getCount: Long = countLeft
    override def toString: String = s"StandardCountingState($argumentRowId, countLeft=$countLeft)"
  }

  abstract class ConcurrentCountingState(override val argumentRowId: Long,
                                         countTotal: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends CountingState {
    private val countLeft = new AtomicLong(countTotal)

    def reserve(wanted: Int): Int = {
      if (countLeft.get() <= 0) {
        0
      } else {
        val newCountLeft = countLeft.addAndGet(-wanted)
        if (newCountLeft >= 0) {
          wanted
        } else {
          math.max(0, wanted + newCountLeft).asInstanceOf[Int]//always safe cast since result is always in [0, wanted)
        }
      }
    }
    override def getCount: Long = countLeft.get()

    override def toString: String = s"ConcurrentCountingState($argumentRowId, countLeft=${countLeft.get()})"
  }
}

/**
  * This CountingState is intended for use with serial top level generated codegen templates.
  *
  * For example the SerialTopLevelLimitState (x) while be used in the following way from compiled code:
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
abstract class SerialCountingState extends CountingState {

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
  def update(usedCount: Int): Unit = {
    Preconditions.checkState(usedCount >= 0, "Can not have used a negative number of rows")
    val newCount = getCount - usedCount
    Preconditions.checkState(newCount >= 0, "Used more rows than had count left")
    setCount(newCount)
  }

  override def reserve(wanted: Int): Int = {
    val count = getCount
    Preconditions.checkState(count != -1, "SerialTopLevelLimitState has not been initialized")
    math.min(count, wanted).asInstanceOf[Int]//safe cast since result is <= wanted
  }
}

/**
  * Query-wide row count for the rows from one argumentRowId.
  */
abstract class CountingState extends ArgumentState {
  def reserve(wanted: Int): Int

  protected def getCount: Long
}

/**
  * This is a (common) special case used when not nested under an apply, so we do not need to worry about the argument state map.
  * It also needs to be run either single threaded execution or in a serial pipeline (i.e. the final produce pipeline) in parallel execution,
  * since it does not synchronize the skip count between tasks.
  */
abstract class SerialTopLevelCountingOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                          override val id: Id,
                                                          innermost: DelegateOperatorTaskTemplate,
                                                          argumentStateMapId: ArgumentStateMapId,
                                                          generateCountExpression: () => IntermediateExpression,
                                                          protected val codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {


  private var countExpression: IntermediateExpression = _
  protected val countLeftVar: LocalVariable = variable[Int](codeGen.namer.nextVariableName("countLeft"), constant(0))
  protected val reservedVar: LocalVariable = variable[Int](codeGen.namer.nextVariableName("reserved"), constant(0))
  protected val countingStateField: InstanceField = field[SerialCountingState](
    codeGen.namer.nextVariableName("countState"),
    // Get the skip operator state from the ArgumentStateMaps that is passed to the constructor
    // We do not generate any checks or error handling code, so the runtime compiler is responsible for this fitting together perfectly
    peekState[SerialCountingState](argumentStateMapId))

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genExpressions: Seq[IntermediateExpression] = Seq(countExpression)

  override def genOperateEnter: IntermediateRepresentation = {
    if (countExpression == null) {
      countExpression = generateCountExpression()
    }

    // Initialize the state
    // NOTE: We would typically prefer to do this in the constructor, but that is called using reflection, and the error handling
    // does not work so well when exceptions are thrown from evaluateCountValue (which can be expected due to it performing user error checking!)
    block(
      condition(invoke(loadField(countingStateField), method[SerialCountingState, Boolean]("isUninitialized")))(
        invoke(loadField(countingStateField), method[SerialCountingState, Unit, Long]("initialize"),
               invokeStatic(method[CountingState, Long, AnyValue]("evaluateCountValue"), nullCheckIfRequired(countExpression)))
        ),
      assign(reservedVar, invoke(loadField(countingStateField), method[CountingState, Int, Int]("reserve"), howMuchToReserve)),
      assign(countLeftVar, load(reservedVar)),
      inner.genOperateEnter
      )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      invoke(loadField(countingStateField), method[SerialCountingState, Unit, Int]("update"),
        subtract(load(reservedVar), load(countLeftVar))),
      inner.genOperateExit)
  }

  protected def howMuchToReserve: IntermediateRepresentation = constant(Int.MaxValue)

  override def genLocalVariables: Seq[LocalVariable] = Seq(countLeftVar, reservedVar)

  override def genFields: Seq[Field] = Seq(countingStateField)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
