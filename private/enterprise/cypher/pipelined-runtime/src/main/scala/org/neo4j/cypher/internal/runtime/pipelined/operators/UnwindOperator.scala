/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => InterpretedQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ListSupport, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue

class UnwindOperator(val workIdentity: WorkIdentity,
                     collection: Expression,
                     offset: Int)
  extends StreamingOperator with ListSupport {

  override protected def nextTasks(context: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = UnwindOperator.this.workIdentity

    private var unwoundValues: java.util.Iterator[AnyValue] = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {

      val queryState = new InterpretedQueryState(context,
                                                 resources = null,
                                                 params = state.params,
                                                 resources.expressionCursors,
                                                 Array.empty[IndexReadSession],
                                                 resources.expressionVariables(state.nExpressionSlots),
                                                 state.subscriber,
                                                 NoMemoryTracker)

      initExecutionContext.copyFrom(inputMorsel, inputMorsel.getLongsPerRow, inputMorsel.getRefsPerRow)
      val value = collection(initExecutionContext, queryState)
      unwoundValues = makeTraversable(value).iterator
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {
      while (unwoundValues.hasNext && outputRow.isValidRow) {
        val thisValue = unwoundValues.next()
        outputRow.copyFrom(inputMorsel)
        outputRow.setRefAt(offset, thisValue)
        outputRow.moveToNextRow()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      unwoundValues = null
    }

    override def canContinue: Boolean = unwoundValues != null || inputMorsel.isValidRow
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

  import OperatorCodeGenHelperTemplates._

  private val cursorField = field[IteratorCursor](codeGen.namer.nextVariableName())
  private var listExpression: IntermediateExpression = _

  override final def scopeId: String = "unwind" + id.x

  override def genMoreFields: Seq[Field] = Seq(cursorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(listExpression)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    listExpression = codeGen.intermediateCompileExpression(rawListExpression).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $rawListExpression"))

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
          doIfInnerCantContinue(setField(canContinue, profilingCursorNext[IteratorCursor](loadField(cursorField), id))),
          endInnerLoop
          )
        )
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    noop()
  }
}



