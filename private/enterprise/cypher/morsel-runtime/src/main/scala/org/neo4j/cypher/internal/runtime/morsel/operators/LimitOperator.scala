/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, TopLevelArgument}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.LimitOperator.{LazyLimitState, LimitState, LimitStateFactory, evaluateCountValue}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates._
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory, ArgumentStateMaps, WorkCanceller}
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue

object LimitOperator extends NumericHelper {
  def evaluateCountValue(countValue: AnyValue): Long = {
    val limitNumber = asNumber(countValue)
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
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LimitState =
      new StandardLimitState(argumentRowId, count, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LimitState =
      new ConcurrentLimitState(argumentRowId, count, argumentRowIdsForReducers)
  }

  // This is used by fused limit in a serial pipeline, i.e. only safe to use in single-threaded execution or by a serial pipeline in parallel execution
  object LazyLimitStateFactory extends ArgumentStateFactory[LazyLimitState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LazyLimitState =
      new LazyStandardLimitState(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LazyLimitState =
      // NOTE: This is actually _not_ threadsafe and only safe to use in a serial pipeline!
      new LazyStandardLimitState(argumentRowId, argumentRowIdsForReducers)
  }

  /**
   * Query-wide row count for the rows from one argumentRowId.
   */
  abstract class LimitState extends WorkCanceller {
    def reserve(wanted: Long): Long
  }

  /**
   * A LimitState that does not need to be initialized in the constructor
   * It is initialized by calling setCount()
   */
  abstract class LazyLimitState extends LimitState {
    def setCount(count: Long): Unit // Initialize the count. Intended to be called only once.
    def isUninitialized: Boolean // True iff setCount has never been called
    def unreserve(unusedCount: Long): Unit // A reserved count that was not fully consumed can be handed back to the state
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

  class LazyStandardLimitState(override val argumentRowId: Long,
                               override val argumentRowIdsForReducers: Array[Long]) extends LazyLimitState {

    private var countLeft: Long = -1L

    override def isUninitialized: Boolean = countLeft == -1L

    override def setCount(count: Long): Unit = {
      AssertionRunner.runUnderAssertion{() => countLeft == -1}
      countLeft = count
    }

    override def reserve(wanted: Long): Long = {
      AssertionRunner.runUnderAssertion{() => countLeft != -1}
      val got = math.min(countLeft, wanted)
      countLeft -= got
      got
    }

    override def unreserve(unusedCount: Long): Unit = {
      assert(unusedCount >= 0)
      countLeft += unusedCount
      AssertionRunner.runUnderAssertion{() => countLeft >= 0}
    }

    override def isCancelled: Boolean = countLeft == 0

    override def toString: String = s"LazyStandardLimitState($argumentRowId, countLeft=$countLeft)"
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
                    countExpression: Expression) extends MiddleOperator with NumericHelper {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator, stateFactory: StateFactory, queryContext: QueryContext, state: QueryState, resources: QueryResources): OperatorTask = {

    val queryState = new OldQueryState(queryContext,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots),
                                       state.subscriber,
                                       NoMemoryTracker)
    val countValue: AnyValue = countExpression(ExecutionContext.empty, queryState)
    val limit = evaluateCountValue(countValue)
    new LimitOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
                                                                      new LimitStateFactory(limit)))
  }

  class LimitOperatorTask(argumentStateMap: ArgumentStateMap[LimitState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = LimitOperator.this.workIdentity

    override def operate(output: MorselExecutionContext,
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
                                             (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  private var countExpression: IntermediateExpression = _
  private val countLeftVar: LocalVariable = variable[Long](codeGen.namer.nextVariableName() + "_countLeft", constant(0L))
  private val reservedVar: LocalVariable = variable[Long](codeGen.namer.nextVariableName() + "_reserved", constant(0L))
  private val limitStateField = field[LazyLimitState](codeGen.namer.nextVariableName() + "_limitState",
    // Get the limit operator state from the ArgumentStateMaps that is passed to the constructor
    // We do not generate any checks or error handling code, so the runtime compiler is responsible for this fitting together perfectly
    cast[LazyLimitState](
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
        cast[Long](invoke(OUTPUT_ROW, method[MorselExecutionContext, Int]("getValidRows")))
      } else if (innermost.shouldCheckOutputCounter) {
        // Use the output counter to determine our maximum chunk of the total limit
        load(OUTPUT_COUNTER)
      } else {
        // We do not seem to have any bound on the output of this task (i.e. we are the final produce result pipeline task)
        // Reserve as much as we can get
        constant(Long.MaxValue)
      }

    // Initialize the limit state
    // NOTE: We would typically prefer to do this in the constructor, but that is called using reflection, and the error handling
    // does not work so well when exceptions are thrown from evaluateCountValue (which can be expected due to it performing user error checking!)
    block(
      condition(invoke(loadField(limitStateField), method[LazyLimitState, Boolean]("isUninitialized")))(
        invoke(loadField(limitStateField), method[LazyLimitState, Unit, Long]("setCount"),
          invokeStatic(method[LimitOperator, Long, AnyValue]("evaluateCountValue"), countExpression.ir))
      ),
      assign(reservedVar, invoke(loadField(limitStateField), method[LimitState, Long, Long]("reserve"), howMuchToReserve)),
      assign(countLeftVar, load(reservedVar)),
      inner.genOperateEnter
    )
  }

  override def genOperate: IntermediateRepresentation = {
    block(
      condition(lessThanOrEqual(load(countLeftVar), constant(0L))) (
        break(OUTER_LOOP_LABEL_NAME)
      ),
      assign(countLeftVar, subtract(load(countLeftVar), constant(1L))),
      inner.genOperateWithExpressions
    )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      ifElse(greaterThan(load(countLeftVar), constant(0L)))(
        block(
          invoke(loadField(limitStateField), method[LazyLimitState, Unit, Long]("unreserve"), load(countLeftVar)),
          profileRows(id, cast[Int](subtract(load(reservedVar), load(countLeftVar))))
        )
      )(
        profileRows(id, cast[Int](load(reservedVar)))
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