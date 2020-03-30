/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.ConcurrentHashMap

import org.eclipse.collections.impl.factory.Sets
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.setMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctOperatorTaskTemplate.SerialTopLevelDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.memory.MemoryTracker
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
                      (val id: Id = Id.INVALID_ID) extends MemoryTrackingMiddleOperator(id.x) {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources,
                          memoryTracker: MemoryTracker): OperatorTask = {
    new DistinctOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId, new DistinctStateFactory(memoryTracker)), workIdentity)
  }

  class DistinctStateFactory(memoryTracker: MemoryTracker) extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState = {
      // TODO: Use HeapTrackingCollections.newSet()
      val seenSet = Sets.mutable.empty[groupings.KeyType]()
      val seen = (key: groupings.KeyType) => seenSet.add(key)
      new DistinctState(argumentRowId, seen, argumentRowIdsForReducers, memoryTracker)
    }

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState = {
      val seenSet = ConcurrentHashMap.newKeySet[groupings.KeyType]()
      val seen = (key: groupings.KeyType) => seenSet.add(key)
      new DistinctState(argumentRowId, seen, argumentRowIdsForReducers, memoryTracker)
    }

    override def completeOnConstruction: Boolean = true
  }

  class DistinctState(override val argumentRowId: Long,
                      seen: groupings.KeyType => Boolean,
                      override val argumentRowIdsForReducers: Array[Long],
                      memoryTracker: MemoryTracker) extends DistinctOperatorState {

    override def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean = {
      val groupingKey = groupings.computeGroupingKey(row, queryState)
      if (seen(groupingKey)) {
        // Note: this allocation is currently never de-allocated
        memoryTracker.allocateHeap(groupingKey.estimatedHeapUsage())
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
  private val memoryTrackerField = field[MemoryTracker](codeGen.namer.nextVariableName("memoryTracker"))

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override protected def genOperate: IntermediateRepresentation = {
    val compiled = groupings.map {
      case (s, e) =>
        s -> codeGen.compileExpression(e).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $e"))
    }
    //note that this key ir read later also by project
    val keyVar = codeGen.namer.nextVariableName("groupingKey")
    groupingExpression = codeGen.compileGroupingExpression(compiled, keyVar)

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
                          method[SerialTopLevelDistinctState, Boolean, AnyValue]("distinct"),
                          load(keyVar)))) {
        block(
          profileRow(id),
          nullCheckIfRequired(groupingExpression.projectKey),
          inner.genOperateWithExpressions
        )
      })
  }

  override def genCreateState: IntermediateRepresentation = block(
    setMemoryTracker(memoryTrackerField, id.x),
    invoke(loadField(distinctStateField),
           method[SerialTopLevelDistinctState, Unit, MemoryTracker]("setMemoryTracker"),
           loadField(memoryTrackerField)),
    inner.genCreateState)

  override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)

  override def genFields: Seq[Field] = Seq(distinctStateField, memoryTrackerField)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

object SerialTopLevelDistinctOperatorTaskTemplate {
  class DistinctStateFactory(id: Id) extends ArgumentStateFactory[SerialTopLevelDistinctState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long]): SerialTopLevelDistinctState = {
      // TODO: Use HeapTrackingCollections.newSet()
      val seenSet = Sets.mutable.empty[AnyValue]()
      val seen = (key: AnyValue) => seenSet.add(key)
      new SerialTopLevelDistinctState(argumentRowId,
                                      argumentRowIdsForReducers,
                                      seen, id)
    }

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): SerialTopLevelDistinctState = {
      val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()
      val seen = (key: AnyValue) => seenSet.add(key)
      new SerialTopLevelDistinctState(argumentRowId, argumentRowIdsForReducers, seen, id)
    }
  }

  class SerialTopLevelDistinctState(override val argumentRowId: Long,
                                    override val argumentRowIdsForReducers: Array[Long],
                                    seen: AnyValue => Boolean,
                                    id: Id) extends ArgumentState {
    private var memoryTracker: MemoryTracker = _

    def setMemoryTracker(memoryTracker: MemoryTracker): Unit =
      this.memoryTracker = memoryTracker

    def distinct(groupingKey: AnyValue): Boolean = {
      if (seen(groupingKey)) {
        memoryTracker.allocateHeap(groupingKey.estimatedHeapUsage())
        true
      } else false
    }
    override def toString: String = s"SerialTopLevelDistinctState($argumentRowId, concurrent=${seen.getClass.getPackageName.contains("concurrent")})"
  }
}
