/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => InterpretedQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ListSupport, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue

class UnwindOperator(val workIdentity: WorkIdentity,
                     collection: Expression,
                     offset: Int)
  extends StreamingOperator with ListSupport {

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
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
                                                 state.subscriber)

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
                                 rawListExpression: expressions.Expression,
                                 offset: Int)
                                (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val iteratorField = field[java.util.Iterator[AnyValue]](codeGen.namer.nextVariableName())
  private var listExpression: IntermediateExpression = _

  override def genMoreFields: Seq[Field] = Seq(iteratorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(listExpression)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    listExpression = codeGen.intermediateCompileExpression(rawListExpression).getOrElse(throw new CantCompileQueryException())

    /**
      * {{{
      *   this.iterator = asList([expression]).iterator()
      *   this.canContinue = iterator.hasNext
      *   canContinue
      * }}}
      */
    block(
      setField(iteratorField, invoke(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), nullCheckIfRequired(listExpression)),
        method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
      setField(canContinue, invoke(loadField(iteratorField), method[java.util.Iterator[_], Boolean]("hasNext"))),
      loadField(canContinue)
      )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     setRefAt(offset, iterator.next())
      *     << inner.genOperate >>
      *     this.canContinue = iterator.hasNext()
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        if (innermost.shouldWriteToContext) {
          invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, MorselExecutionContext]("copyFrom"), loadField(INPUT_MORSEL))
        } else {
          noop()
        },
        codeGen.setRefAt(offset, cast[AnyValue](invoke(loadField(iteratorField), method[java.util.Iterator[_], Object]("next")))),
        profileRow(id),
        inner.genOperate,
        setField(canContinue, invoke(loadField(iteratorField), method[java.util.Iterator[_], Boolean]("hasNext"))),
        )
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    noop()
  }
}

