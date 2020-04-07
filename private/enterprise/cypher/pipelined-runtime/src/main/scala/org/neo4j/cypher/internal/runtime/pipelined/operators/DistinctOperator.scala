/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateGroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.MEMORY_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SET_MEMORY_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctOperatorTaskTemplate.SerialTopLevelDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.values.AnyValue

/**
 * The distinct operator, for Cypher like
 *
 *   MATCH ...
 *   WITH DISTINCT a, b
 *   ...
 *
 * or
 *
 *   UNWIND someList AS x
 *   RETURN DISTINCT x
 */

trait DistinctOperatorState extends ArgumentState {
  def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean
}

class DistinctOperatorTask[S <: DistinctOperatorState](argumentStateMap: ArgumentStateMap[S], val workIdentity: WorkIdentity) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => distinctState,
      (distinctState, row) => distinctState.filterOrProject(row, queryState))
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

class DistinctOperator(argumentStateMapId: ArgumentStateMapId,
                       val workIdentity: WorkIdentity,
                       groupings: GroupingExpression)
                      (val id: Id = Id.INVALID_ID) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources): OperatorTask =
    new DistinctOperatorTask(
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new DistinctStateFactory(stateFactory.memoryTracker), ordered = false),
      workIdentity)

  class DistinctStateFactory(memoryTracker: QueryMemoryTracker) extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState =
      new DistinctState(argumentRowId, new util.HashSet[groupings.KeyType](), argumentRowIdsForReducers, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState =
      new DistinctState(argumentRowId, ConcurrentHashMap.newKeySet[groupings.KeyType](), argumentRowIdsForReducers, memoryTracker)

    override def completeOnConstruction: Boolean = true
  }

  class DistinctState(override val argumentRowId: Long,
                      seen: util.Set[groupings.KeyType],
                      override val argumentRowIdsForReducers: Array[Long],
                      memoryTracker: QueryMemoryTracker) extends DistinctOperatorState {

    override def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean = {
      val groupingKey = groupings.computeGroupingKey(row, queryState)
      if (seen.add(groupingKey)) {
        // Note: this allocation is currently never de-allocated
        memoryTracker.allocated(groupingKey, id.x)
        groupings.project(row, groupingKey)
        true
      } else {
        false
      }
    }
    override def toString: String = s"DistinctState($argumentRowId, concurrent=${seen.getClass.getPackageName.contains("concurrent")})"
  }
}

class SerialTopLevelDistinctOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                 override val id: Id,
                                                 argumentStateMapId: ArgumentStateMapId,
                                                 groupings: Seq[(Slot, Expression)])
                                                (protected val codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {

  private var groupingExpression: IntermediateGroupingExpression = _

  private val distinctStateField = field[SerialTopLevelDistinctState](codeGen.namer.nextVariableName("distinctState"),
                                                                      peekState[SerialTopLevelDistinctState](argumentStateMapId))

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override protected def genOperate: IntermediateRepresentation = {
    val compiled = groupings.map {
      case (s, e) =>
        s -> codeGen.intermediateCompileExpression(e).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $e"))
    }
    //note that this key ir read later also by project
    val keyVar = codeGen.namer.nextVariableName("groupingKey")
    groupingExpression = codeGen.intermediateCompileGroupingExpression(compiled, keyVar)

    /**
      * val key = [computeKey]
      * if (distinctState.distinct(key, memoryTracker)) {
      *   [project key]
      *   <<inner.generate>>
      * }
      */
    block(
      declareAndAssign(typeRefOf[AnyValue], keyVar, nullCheckIfRequired(groupingExpression.computeKey)),
      condition(or(innerCanContinue,
                   invoke(loadField(distinctStateField),
                          method[SerialTopLevelDistinctState, Boolean, AnyValue, QueryMemoryTracker]("distinct"),
                          load(keyVar), loadField(MEMORY_TRACKER)))) {
        block(
          profileRow(id),
          nullCheckIfRequired(groupingExpression.projectKey),
          inner.genOperateWithExpressions
        )
      })
  }

  override def genCreateState: IntermediateRepresentation = block(SET_MEMORY_TRACKER, inner.genCreateState)

  override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)

  override def genFields: Seq[Field] = Seq(distinctStateField, MEMORY_TRACKER)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

object SerialTopLevelDistinctOperatorTaskTemplate {
  val NO_MEMORY_TRACKING: QueryMemoryTracker = NoMemoryTracker

  class DistinctStateFactory(id: Id) extends ArgumentStateFactory[SerialTopLevelDistinctState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long]): SerialTopLevelDistinctState =
      new SerialTopLevelDistinctState(argumentRowId,
                                      argumentRowIdsForReducers,
                                      new util.HashSet[AnyValue], id)

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): SerialTopLevelDistinctState =
      new SerialTopLevelDistinctState(argumentRowId, argumentRowIdsForReducers, ConcurrentHashMap.newKeySet[AnyValue](), id)
  }

  class SerialTopLevelDistinctState(override val argumentRowId: Long,
                                    override val argumentRowIdsForReducers: Array[Long],
                                    seen: util.Set[AnyValue],
                                    id: Id) extends ArgumentState {
    def distinct(groupingKey: AnyValue, memoryTracker: QueryMemoryTracker): Boolean = {
      if (seen.add(groupingKey)) {
        memoryTracker.allocated(groupingKey, id.x)
        true
      } else false
    }
    override def toString: String = s"SerialTopLevelDistinctState($argumentRowId, concurrent=${seen.getClass.getPackageName.contains("concurrent")})"
  }
}
