/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThan
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.evaluateCountValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_COUNTER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_MORSEL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgument
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRows
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.FilterStateWithIsLast
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker

/**
 * Limit the number of rows to `countExpression` per argument.
 */
class ExhaustiveLimitOperator(argumentStateMapId: ArgumentStateMapId,
                    val workIdentity: WorkIdentity,
                    countExpression: Expression)(val id: Id = Id.INVALID_ID) extends MemoryTrackingMiddleOperator(id.x) {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources,
                          memoryTracker: MemoryTracker): OperatorTask = {
    val limit = evaluateCountValue(state, resources, countExpression)
    new ExhaustiveLimitOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
      new SkipOperator.SkipStateFactory(limit), memoryTracker))
  }

  class ExhaustiveLimitOperatorTask(argumentStateMap: ArgumentStateMap[CountingState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = ExhaustiveLimitOperator.this.workIdentity

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {

      argumentStateMap.filterWithSideEffect[FilterState](outputMorsel,
        (rowCount, nRows) => {
          FilterStateWithIsLast(new FilterState(rowCount.reserve(nRows)), isLast = false)
        },
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
      } else {
        false
      }
    }
  }

}

class SerialTopLevelExhaustiveLimitOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                        id: Id,
                                                        innermost: DelegateOperatorTaskTemplate,
                                                        argumentStateMapId: ArgumentStateMapId,
                                                        generateCountExpression: () => IntermediateExpression)
                                                       (codeGen: OperatorExpressionCompiler)
  extends SerialTopLevelCountingOperatorTaskTemplate(inner, id, innermost, argumentStateMapId, generateCountExpression, codeGen) {

  override def genLocalVariables: Seq[LocalVariable] = super.genLocalVariables

  override def genOperate: IntermediateRepresentation = {
    condition(greaterThan(load(countLeftVar), constant(0)))(
      block(
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          assign(countLeftVar, subtract(load(countLeftVar), constant(1)))))
    )
  }

  override protected def howMuchToReserve: IntermediateRepresentation = {
    if (innermost.shouldWriteToContext) {
      // Use the available output morsel rows to determine our maximum chunk of the total limit
      invoke(OUTPUT_MORSEL, method[Morsel, Int]("numberOfRows"))
    } else if (innermost.shouldCheckOutputCounter) {
      // Use the output counter to determine our maximum chunk of the total limit
      load(OUTPUT_COUNTER)
    } else {
      // We do not seem to have any bound on the output of this task (i.e. we are the final produce result pipeline task)
      // Reserve as much as we can get
      constant(Int.MaxValue)
    }
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      profileRows(id, cast[Long](subtract(load(reservedVar), load(countLeftVar))), doProfile),
      super.genOperateExit
    )
  }

  override protected def isHead: Boolean = false
}

/**
 * Assumes that this is run on the RHS of an Apply.
 */
class SerialExhaustiveLimitOnRhsOfApplyOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                            id: Id,
                                                            argumentStateMapId: ArgumentStateMapId,
                                                            generateCountExpression: () => IntermediateExpression)
                                                           (codeGen: OperatorExpressionCompiler)
  extends SerialCountingOperatorOnRhsOfApplyOperatorTaskTemplate(inner, id, argumentStateMapId, generateCountExpression, codeGen) {

  override protected def beginOperate: IntermediateRepresentation =
    declareAndAssign(typeRefOf[Long], argumentVarName(argumentStateMapId), getArgument(argumentStateMapId))

  override protected def innerOperate: IntermediateRepresentation = {
    block(
      condition(greaterThan(load(countLeftVar), constant(0)))(
        block(
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
            block(
              profileRow(id, doProfile),
              assign(countLeftVar, subtract(load(countLeftVar), constant(1)))
            )
          ))
      )
    )
  }

  override protected def isHead: Boolean = false
}
