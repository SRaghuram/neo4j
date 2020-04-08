/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue

class UnwindOperator(val workIdentity: WorkIdentity,
                     collection: Expression,
                     offset: Int)
  extends StreamingOperator with ListSupport {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  class OTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = UnwindOperator.this.workIdentity

    private var unwoundValues: java.util.Iterator[AnyValue] = _

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {

      val queryState = state.queryStateForExpressionEvaluation(resources)

      initExecutionContext.copyFrom(inputCursor, inputMorsel.longsPerRow, inputMorsel.refsPerRow)
      val value = collection(initExecutionContext, queryState)
      unwoundValues = makeTraversable(value).iterator
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      while (unwoundValues.hasNext && outputRow.onValidRow) {
        val thisValue = unwoundValues.next()
        outputRow.copyFrom(inputCursor)
        outputRow.setRefAt(offset, thisValue)
        outputRow.next()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      unwoundValues = null
    }

    override def canContinue: Boolean = unwoundValues != null || inputCursor.onValidRow()
  }
}

class UnwindOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                 id: Id,
                                 innermost: DelegateOperatorTaskTemplate,
                                 isHead: Boolean,
                                 rawListExpression: expressions.Expression,
                                 offset: Int)
                                (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen, isHead) {


  private val cursorField = field[IteratorCursor](codeGen.namer.nextVariableName())
  private var listExpression: IntermediateExpression = _

  override final def scopeId: String = "unwind" + id.x

  override def genMoreFields: Seq[Field] = Seq(cursorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(listExpression)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (listExpression == null) {
      listExpression = codeGen.compileExpression(rawListExpression).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $rawListExpression"))
    }

    /**
     * {{{
     *   this.cursor = IteratorCursor(asList([expression]).iterator())
     *   this.canContinue = this.cursor.next()
     *   true
     * }}}
     */
    block(
      setField(cursorField,
        invokeStatic(method[IteratorCursor, IteratorCursor, java.util.Iterator[_]]("apply"),
          invoke(
            invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"),
              nullCheckIfRequired(listExpression)),
            method[ListValue, java.util.Iterator[AnyValue]]("iterator")))),
      setField(canContinue, profilingCursorNext[IteratorCursor](loadField(cursorField), id)),
      constant(true)
    )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     setRefAt(offset, cursor.value)
     *     << inner.genOperate >>
     *     this.canContinue = cursor.next()
     *   }
     * }}}
     */
    block(
      loop(and(innermost.predicate, loadField(canContinue)))(
        block(
          codeGen.copyFromInput(codeGen.inputSlotConfiguration.numberOfLongs,
            codeGen.inputSlotConfiguration.numberOfReferences),
          codeGen.setRefAt(offset,
            invoke(loadField(cursorField), method[IteratorCursor, AnyValue]("value"))),
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[IteratorCursor](loadField(cursorField), id))),
          endInnerLoop
        )
      )
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    noop()
  }
}



