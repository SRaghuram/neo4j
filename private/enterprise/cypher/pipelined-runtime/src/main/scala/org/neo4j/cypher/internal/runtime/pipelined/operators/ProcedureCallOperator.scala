/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.ProcedureCallOperator.createProcedureCallContext
import org.neo4j.cypher.internal.runtime.pipelined.procedures.Procedures
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.values.AnyValue

class ProcedureCallOperator(val workIdentity: WorkIdentity,
                            signature: ProcedureSignature,
                            callMode: ProcedureCallMode,
                            argExprs: Seq[Expression],
                            originalVariables: Array[String],
                            indexToOffset: Array[(Int, Int)])
  extends StreamingOperator with ListSupport {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new ProcedureCallTask(inputMorsel.nextCopy, workIdentity, signature, callMode, argExprs, originalVariables, indexToOffset))
  }
}

object ProcedureCallOperator {
  def createProcedureCallContext(qtx: QueryContext, originalVariables: Array[String]): ProcedureCallContext = {
    // getting the original name of the yielded variable
    val databaseId = qtx.transactionalContext.databaseId
    new ProcedureCallContext(originalVariables, true, databaseId.name(), databaseId.isSystemDatabase)
  }
}

class ProcedureCallMiddleOperator(val workIdentity: WorkIdentity,
                                  signature: ProcedureSignature,
                                  callMode: ProcedureCallMode,
                                  argExprs: Seq[Expression],
                                  originalVariables: Array[String]) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    val readCursor = morsel.readCursor()
    while (readCursor.next()) {
      val argValues = argExprs.map(arg => arg(readCursor, queryState)).toArray
      callMode.callProcedure(state.query, signature.id, argValues, createProcedureCallContext(state.query, originalVariables))
    }
  }
}

class ProcedureCallTask(inputMorsel: Morsel,
                        val workIdentity: WorkIdentity,
                        signature: ProcedureSignature,
                        callMode: ProcedureCallMode,
                        argExprs: Seq[Expression],
                        originalVariables: Array[String],
                        indexToOffset: Array[(Int, Int)]) extends InputLoopTask(inputMorsel) {
  protected var iterator: Iterator[Array[AnyValue]] = _

  override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    initExecutionContext.copyFrom(inputCursor, inputMorsel.longsPerRow, inputMorsel.refsPerRow)
    val argValues = argExprs.map(arg => arg(initExecutionContext, queryState)).toArray
    iterator = callMode.callProcedure(state.query, signature.id, argValues, createProcedureCallContext(state.query, originalVariables))
    true
  }

  override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
    while (outputRow.onValidRow() && iterator.hasNext) {
      val thisValue = iterator.next()
      outputRow.copyFrom(inputCursor)
      var i = 0
      while (i < indexToOffset.length) {
        val (index, offset) = indexToOffset(i)
        outputRow.setRefAt(offset, thisValue(index))
        i += 1
      }
      outputRow.next()
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

  override protected def closeInnerLoop(resources: QueryResources): Unit = {
    iterator = null
  }

  override def canContinue: Boolean = iterator != null || inputCursor.onValidRow()
}




