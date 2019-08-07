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
import org.neo4j.cypher.internal.runtime.morsel.operators.UndirectedRelationshipByIdSeekOperator.{asId, asIdMethod}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.virtual.ListValue

class UndirectedRelationshipByIdSeekOperator(val workIdentity: WorkIdentity,
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

      IndexedSeq(new UndirectedRelationshipByIdTask(inputMorsel.nextCopy))
  }

  class UndirectedRelationshipByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "UndirectedRelationshipByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _
    private var cursor: RelationshipScanCursor = _
    private var progress = true

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
                                           state.subscriber,
                                           NoMemoryTracker)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = relId.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = UndirectedRelationshipByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.isValidRow && (!progress || ids.hasNext)) {
        if (progress) {
          val nextId = asId(ids.next())
          if (nextId >= 0L) {
            context.transactionalContext.dataRead.singleRelationship(nextId, cursor)
            if (cursor.next()) {
              outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
              outputRow.setLongAt(relationship, nextId)
              outputRow.setLongAt(startNode, cursor.sourceNodeReference())
              outputRow.setLongAt(endNode, cursor.targetNodeReference())
              outputRow.moveToNextRow()
              progress = false
            }
          }
        } else {
          outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(relationship, cursor.relationshipReference())
          outputRow.setLongAt(startNode, cursor.targetNodeReference())
          outputRow.setLongAt(endNode, cursor.sourceNodeReference())
          outputRow.moveToNextRow()
          progress = true
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

object UndirectedRelationshipByIdSeekOperator {
  def asId(value: AnyValue): Long = value match {
    case d:IntegralValue => d.longValue()
    case _ => -1L
  }

  val asIdMethod: Method = method[UndirectedRelationshipByIdSeekOperator, Long, AnyValue]("asId")
}

class SingleUndirectedRelationshipByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
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
  private val forwardDirection = field[Boolean](codeGen.namer.nextVariableName(), constant(true))
  private var relId: IntermediateExpression= _

  override def genMoreFields: Seq[Field] = Seq(cursor, forwardDirection)

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
      *     if (this.forwardDirection) {
      *       setLongAt(fromOffset, cursor.sourceNodeReference)
      *       setLongAt(toOffset, cursor.targetNodeReference)
      *       this.forwardDirection = false
      *     } else {
      *       setLongAt(fromOffset, cursor.targetNodeReference)
      *       setLongAt(toOffset, cursor.sourceNodeReference)
      *       this.forwardDirection = true
      *     }
      *     << inner.genOperate >>
      *     this.canContinue = !forwardDirection
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
        ifElse(loadField(forwardDirection))(block(
          codeGen.setLongAt(fromOffset,
                            invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
          codeGen.setLongAt(toOffset,
                            invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
          setField(forwardDirection, constant(false))))(block(
          codeGen.setLongAt(fromOffset,
                            invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
          codeGen.setLongAt(toOffset,
                            invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
          setField(forwardDirection, constant(true)))),
        profileRow(id),
        inner.genOperateWithExpressions,
        setField(canContinue, not(loadField(forwardDirection))))
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

class ManyUndirectedRelationshipByIdsSeekTaskTemplate(inner: OperatorTaskTemplate,
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
  private val progressCursor = field[Boolean](codeGen.namer.nextVariableName(), constant(true))

  override def genMoreFields: Seq[Field] = Seq(idIterator,cursor, progressCursor)

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
      setField(canContinue, or(not(loadField(progressCursor)), invoke(loadField(idIterator), method[util.Iterator[_], Boolean]("hasNext")))),
      loadField(canContinue))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    val idVariable = codeGen.namer.nextVariableName()

    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     val id = if (this.progressCursor) asId(idIterator.next()) else cursor.relationshipReference()
      *     if (id >= 0) read.singleRelationship(id, cursor)
      *
      *     if (id >= 0 && (!this.progressCursor || cursor.next())) {
      *       setLongAt(relOffset, id)
      *       if (this.progressCursor) {
      *         setLongAt(fromOffset, cursor.sourceNodeReference)
      *         setLongAt(toOffset, cursor.targetNodeReference)
      *         this.progressCursor = false
      *       } else {
      *       setLongAt(fromOffset, cursor.targetNodeReference)
      *         setLongAt(toOffset, cursor.sourceNodeReference)
      *         this.progressCursor = true
      *       }
      *       << inner.genOperate >>
      *      }
      *      this.canContinue = !this.progressCursor || idIterator.hasNext()
      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        declareAndAssign(typeRefOf[Long], idVariable,
                         ternary(loadField(progressCursor),
                         invokeStatic(asIdMethod, cast[AnyValue](
                           invoke(loadField(idIterator),
                                  method[java.util.Iterator[AnyValue], Object]("next")))),
                                 invoke(loadField(cursor), method[RelationshipScanCursor, Long]("relationshipReference")))),
        condition(greaterThanOrEqual(load(idVariable), constant(0L))) {
          singleRelationship(load(idVariable), loadField(cursor)) },
        condition(and(greaterThanOrEqual(load(idVariable), constant(0L)),
                      or(not(loadField(progressCursor)), cursorNext[RelationshipScanCursor](loadField(cursor)))))(
          block(
            if (innermost.shouldWriteToContext && (argumentSize.nLongs > 0 || argumentSize.nReferences > 0)) {
              invokeSideEffect(OUTPUT_ROW, method[MorselExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
                               loadField(INPUT_MORSEL), constant(argumentSize.nLongs),
                               constant(argumentSize.nReferences))
            } else {
              noop()
            },
              codeGen.setLongAt(relationshipOffset, load(idVariable)),
            ifElse(loadField(progressCursor))(block(
              codeGen.setLongAt(fromOffset,
                                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
              codeGen.setLongAt(toOffset,
                                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
              setField(progressCursor, constant(false))))(block(
              codeGen.setLongAt(fromOffset,
                                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
              codeGen.setLongAt(toOffset,
                                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
              setField(progressCursor, constant(true)))),
            profileRow(id),
            inner.genOperateWithExpressions
            )),
        setField(canContinue, or(not(loadField(progressCursor)), invoke(loadField(idIterator), method[util.Iterator[_], Boolean]("hasNext")))))
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