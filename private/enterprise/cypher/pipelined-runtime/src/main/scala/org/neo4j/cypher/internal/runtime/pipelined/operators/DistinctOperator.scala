/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

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
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateGroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.ConcurrentDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.StandardDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.setMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.kernel.impl.util.collection.DistinctSet
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

class DistinctOperatorTask[S <: DistinctState](argumentStateMap: ArgumentStateMap[S],
                                               val workIdentity: WorkIdentity,
                                               groupings: GroupingExpression) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => distinctState,
      (distinctState, row) => {
        val groupingKey = groupings.computeGroupingKey(row, queryState)
        if (distinctState.seen(groupingKey)) {
          groupings.project(row, groupingKey)
          true
        } else {
          false
        }
      })
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
    new DistinctOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId, new DistinctStateFactory(memoryTracker)), workIdentity, groupings)
  }
}

object DistinctOperator {

  class DistinctStateFactory(memoryTracker: MemoryTracker) extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState = {
      new StandardDistinctState(argumentRowId, argumentRowIdsForReducers, memoryTracker)
    }

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState = {
      new ConcurrentDistinctState(argumentRowId, argumentRowIdsForReducers)
    }

    override def completeOnConstruction: Boolean = true
  }

  trait DistinctState extends ArgumentState {
    def seen(key: AnyValue): Boolean
    /**
     * We need this method instead of having the memoryTracked as a constructor parameter because the Factory
     * needs be statically known for the fused code. This allows us to share the DistinctState between fused and non-fused code.
     */
    def setMemoryTracker(memoryTracker: MemoryTracker): Unit
  }

  class StandardDistinctState(override val argumentRowId: Long, override val argumentRowIdsForReducers: Array[Long])
    extends DistinctState {

    def this(argumentRowId: Long, argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker) = {
      this(argumentRowId, argumentRowIdsForReducers)
      setMemoryTracker(memoryTracker)
    }

    private var seenSet: DistinctSet[AnyValue] = _

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {
      if (seenSet == null) {
        seenSet = DistinctSet.createDistinctSet[AnyValue](memoryTracker)
      }
    }

    override def seen(key: AnyValue): Boolean =
      seenSet.add(key)

    override def close(): Unit = {
      seenSet.close()
      super.close()
    }

    override def toString: String = s"StandardDistinctState($argumentRowId)"
  }

  class ConcurrentDistinctState(override val argumentRowId: Long, override val argumentRowIdsForReducers: Array[Long])
    extends DistinctState {

    private val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {}

    override def seen(key: AnyValue): Boolean =
      seenSet.add(key)

    override def toString: String = s"ConcurrentDistinctState($argumentRowId)"
  }
}

class SerialTopLevelDistinctOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                 override val id: Id,
                                                 argumentStateMapId: ArgumentStateMapId,
                                                 groupings: Seq[(Slot, Expression)])
                                                (protected val codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {

  private var groupingExpression: IntermediateGroupingExpression = _

  private val distinctStateField = field[DistinctState](codeGen.namer.nextVariableName("distinctState"),
                                                                      peekState[DistinctState](argumentStateMapId))
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
      * if (distinctState.seen(key)) {
      *   [project key]
      *   <<inner.generate>>
      * }
      */
    block(
      declareAndAssign(typeRefOf[AnyValue], keyVar, nullCheckIfRequired(groupingExpression.computeKey)),
      condition(or(innerCanContinue,
                   invoke(loadField(distinctStateField),
                          method[DistinctState, Boolean, AnyValue]("seen"),
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
           method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"),
           loadField(memoryTrackerField)),
    inner.genCreateState)

  override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)

  override def genFields: Seq[Field] = Seq(distinctStateField, memoryTrackerField)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

object SerialTopLevelDistinctOperatorTaskTemplate {
  object DistinctStateFactory extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long]): DistinctState = {
      new StandardDistinctState(argumentRowId, argumentRowIdsForReducers)
    }

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): DistinctState = {
      new ConcurrentDistinctState(argumentRowId, argumentRowIdsForReducers)
    }
  }
}
