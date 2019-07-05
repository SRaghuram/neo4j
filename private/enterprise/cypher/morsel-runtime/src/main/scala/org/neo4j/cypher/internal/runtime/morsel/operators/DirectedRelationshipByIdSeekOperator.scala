/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable, Method}
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.DirectedRelationshipByIdSeekOperator.{asId, asIdMethod}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.virtual.ListValue

class DirectedRelationshipByIdSeekOperator(val workIdentity: WorkIdentity,
                                           relationship: Int,
                                           startNode: Int,
                                           endNode: Int,
                                           relId: SeekArgs,
                                           argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

      IndexedSeq(new DirectedRelationshipByIdTask(inputMorsel.nextCopy))
  }


  class DirectedRelationshipByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = " DirectedRelationshipByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _
    private var cursor: RelationshipScanCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      cursor = resources.cursorPools.relationshipScanCursorPool.allocate()
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots),
                                         state.subscriber)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = relId.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = DirectedRelationshipByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && ids.hasNext) {
        val nextId = asId(ids.next())
        val read = context.transactionalContext.dataRead
        if (nextId >= 0L) {
          read.singleRelationship(nextId, cursor)
          if (cursor.next()) {
            outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
            outputRow.setLongAt(relationship, nextId)
            outputRow.setLongAt(startNode, cursor.sourceNodeReference())
            outputRow.setLongAt(endNode, cursor.targetNodeReference())
            outputRow.moveToNextRow()
          }
        }
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.relationshipScanCursorPool.free(cursor)
      cursor = null
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit =  {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }
  }
}

object DirectedRelationshipByIdSeekOperator {
  def asId(value: AnyValue): Long = value match {
    case d:IntegralValue => d.longValue()
    case _ => -1L
  }

  val asIdMethod: Method = method[DirectedRelationshipByIdSeekOperator, Long, AnyValue]("asId")
}

class SingleDirectedRelationshipByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                     id: Id,
                                                     innermost: DelegateOperatorTaskTemplate,
                                                     relationshipOffset: Int,
                                                     fromOffset: Int,
                                                     toOffset: Int,
                                                     relIdExpr: Expression,
                                                     argumentSize: SlotConfiguration.Size)
                                    (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val idVariable = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))
  private val cursor = field[RelationshipScanCursor](codeGen.namer.nextVariableName())
  private var relId: IntermediateExpression= _

  override def genMoreFields: Seq[Field] = Seq(cursor)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, idVariable)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(cursor)))(
        invokeSideEffect(loadField(cursor), method[RelationshipScanCursor, Unit, KernelReadTracer]("setTracer"), event)
        ),
      inner.genSetExecutionEvent(event)
      )

  override def genExpressions: Seq[IntermediateExpression] = Seq(relId)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    relId = codeGen.intermediateCompileExpression(relIdExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $relIdExpr"))

    /**
      * {{{
      *   this.cursor = resources.cursorPools.relationshipScanCursorPool.allocate()
      *   id = asId([relExpresssion])
      *   if (id >= 0) read.singleRelationship(id, cursor)
      *   this.canContinue = id >= 0 && cursor.next
      *   this.canContinue
      * }}}
      */
    block(
      allocateAndTraceCursor(cursor, executionEventField, ALLOCATE_REL_SCAN_CURSOR),
      assign(idVariable, invokeStatic(asIdMethod, nullCheckIfRequired(relId))),
      condition(greaterThanOrEqual(load(idVariable), constant(0L))) {
        singleRelationship(load(idVariable), loadField(cursor)) },
      setField(canContinue, and(greaterThanOrEqual(load(idVariable), constant(0L)), cursorNext[RelationshipScanCursor](loadField(cursor)))),
      loadField(canContinue)
      )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     ...
      *     setLongAt(relOffset, id)
      *     setLongAt(fromOffset, cursor.sourceNodeReference)
      *     setLongAt(toOffset, cursor.targetNodeReference)
      *     << inner.genOperate >>
      *     this.canContinue = false
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        if (innermost.shouldWriteToContext && (argumentSize.nLongs > 0 || argumentSize.nReferences > 0)) {
          invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
                           loadField(INPUT_MORSEL), constant(argumentSize.nLongs), constant(argumentSize.nReferences))
        } else {
          noop()
        },
        codeGen.setLongAt(relationshipOffset, load(idVariable)),
        codeGen.setLongAt(fromOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
        codeGen.setLongAt(toOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
        profileRow(id),
        inner.genOperateWithExpressions,
        setField(canContinue, constant(false)))
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   resources.cursorPools.relationshipScanCursorPool.free(cursor)
      *   cursor = null
      * }}}
      */
    block(
      freeCursor[RelationshipScanCursor](loadField(cursor), RelScanCursorPool),
      setField(cursor, constant(null))
      )
  }
}

class ManyDirectedRelationshipByIdsSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                    id: Id,
                                                    innermost: DelegateOperatorTaskTemplate,
                                                    relationshipOffset: Int,
                                                    fromOffset: Int,
                                                    toOffset: Int,
                                                    relIdsExpr: Expression,
                                                    argumentSize: SlotConfiguration.Size)
                                   (codeGen: OperatorExpressionCompiler) extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val idIterator = field[java.util.Iterator[AnyValue]](codeGen.namer.nextVariableName())
  private var relIds: IntermediateExpression= _
  private val cursor = field[RelationshipScanCursor](codeGen.namer.nextVariableName())

  override def genMoreFields: Seq[Field] = Seq(idIterator,cursor)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(cursor)))(
        invokeSideEffect(loadField(cursor), method[RelationshipScanCursor, Unit, KernelReadTracer]("setTracer"), event)
        ),
      inner.genSetExecutionEvent(event)
      )

  override def genExpressions: Seq[IntermediateExpression] = Seq(relIds)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    relIds = codeGen.intermediateCompileExpression(relIdsExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $relIdsExpr"))

    /**
      * {{{
      *   this.cursor = resources.cursorPools.relationshipScanCursorPool.allocate()
      *   this.idIterator = ((ListValue) <<reldIdsExpr>>)).iterator()
      *   this.canContinue = idIterator.hasNext
      *   this.canContinue
      * }}}
      */
    block(
      setField(cursor, ALLOCATE_REL_SCAN_CURSOR),
      setField(idIterator,
               invoke(cast[ListValue](nullCheckIfRequired(relIds)), method[ListValue, util.Iterator[AnyValue]]("iterator"))),
      setField(canContinue, invoke(loadField(idIterator), method[util.Iterator[_], Boolean]("hasNext"))),
      loadField(canContinue))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    val idVariable = codeGen.namer.nextVariableName()

    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     val id = asId(idIterator.next())
      *     if (id >= 0) read.singleRelationship(id, cursor)
      *
      *     if (id >= 0 && cursor.next()) {
      *       setLongAt(relOffset, id)
      *       setLongAt(fromOffset, cursor.sourceNodeReference)
      *       setLongAt(toOffset, cursor.targetNodeReference)
      *       << inner.genOperate >>
      *      }
      *      this.canContinue = itIterator.hasNext()
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        declareAndAssign(typeRefOf[Long], idVariable,
                         invokeStatic(asIdMethod, cast[AnyValue](
                           invoke(loadField(idIterator),
                                  method[java.util.Iterator[AnyValue], Object]("next"))))),
        condition(greaterThanOrEqual(load(idVariable), constant(0L))) {
          singleRelationship(load(idVariable), loadField(cursor)) },

        condition(and(greaterThanOrEqual(load(idVariable), constant(0L)), cursorNext[RelationshipScanCursor](loadField(cursor))))(
          block(
            if (innermost.shouldWriteToContext && (argumentSize.nLongs > 0 || argumentSize.nReferences > 0)) {
              invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
                               loadField(INPUT_MORSEL), constant(argumentSize.nLongs),
                               constant(argumentSize.nReferences))
            } else {
              noop()
            },
            codeGen.setLongAt(relationshipOffset, load(idVariable)),
            codeGen.setLongAt(fromOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            codeGen.setLongAt(toOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            profileRow(id),
            inner.genOperateWithExpressions
            )),
        setField(canContinue, invoke(loadField(idIterator), method[util.Iterator[_], Boolean]("hasNext"))))
      )
  }

  /**
    * {{{
    *   resources.cursorPools.relationshipScanCursorPool.free(cursor)
    *   cursor = null
    * }}}
    */
  override protected def genCloseInnerLoop: IntermediateRepresentation = block(
    freeCursor[RelationshipScanCursor](loadField(cursor), RelScanCursorPool),
    setField(cursor, constant(null))
    )
}
