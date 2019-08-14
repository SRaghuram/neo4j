/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, InstanceField, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.LimitOperator.evaluateCountValue
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.OUTER_LOOP_LABEL_NAME
import org.neo4j.cypher.internal.runtime.morsel.state.{ArgumentStateMap, StateFactory}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, WorkCanceller}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
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

  class LimitStateFactory(count: Long) extends ArgumentStateFactory[LimitState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LimitState =
      new StandardLimitState(argumentRowId, count, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LimitState =
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

    def reserve(wanted: Long): Long = {
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
 * This is a (common) special case used when not nested under an apply, so we do not need to worry about the argument state map.
 * It also needs to be run either single threaded execution or in a serial pipeline (i.e. the final produce pipeline) in parallel execution,
 * since it does not synchronize the limit count between tasks.
 */
class SimpleLimitOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                      override val id: Id,
                                      generateCountExpression: () => IntermediateExpression)
                                     (codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  private final val COUNT_NOT_INITIALIZED: Long = -2L

  private var countExpression: IntermediateExpression = _
  private val countLeft: InstanceField = field[Long](codeGen.namer.nextVariableName(), constant(COUNT_NOT_INITIALIZED))

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genExpressions: Seq[IntermediateExpression] = Seq(countExpression)

  override def genOperateEnter: IntermediateRepresentation = {
    if (countExpression == null) {
      countExpression = generateCountExpression()
    }
    // Initialize the counter
    // We would prefer to do this in the constructor, but that is called using reflection, and the error handling
    // does not work so well when exceptions are thrown from evaluateCountValue
    block(
      condition(equal(loadField(countLeft), constant(COUNT_NOT_INITIALIZED)))(
        setField(countLeft, invokeStatic(method[LimitOperator, Long, AnyValue]("evaluateCountValue"), countExpression.ir))
      ),
      inner.genOperateEnter
    )
  }

  override def genOperate: IntermediateRepresentation = {
    block(
      condition(lessThanOrEqual(loadField(countLeft), constant(0L))) (
        break(OUTER_LOOP_LABEL_NAME)
      ),
      setField(countLeft, subtract(loadField(countLeft), constant(1L))),
      inner.genOperateWithExpressions
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genFields: Seq[Field] = Seq(countLeft)

  override def genCanContinue: Option[IntermediateRepresentation] = {
    val canContinue = greaterThan(loadField(countLeft), constant(0L))
    inner.genCanContinue.map(and(_, canContinue)).orElse(Some(canContinue))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  // Since we do not create an actual entry in the argument state map, we need to override this
  // method to prevent blowing up in the scheduler
  override def genFilterCancelledArguments: Option[IntermediateRepresentation] = {
    Some(constant(false))
  }
}