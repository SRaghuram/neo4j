/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThanOrEqual
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ALLOCATE_REL_SCAN_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.RelScanCursorPool
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.allocateAndTraceCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.freeCursor
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleRelationship
import org.neo4j.cypher.internal.runtime.pipelined.operators.RelationshipByIdSeekOperator.asIdMethod
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.Many
import org.neo4j.cypher.internal.util.One
import org.neo4j.cypher.internal.util.Zero
import org.neo4j.cypher.internal.util.ZeroOneOrMany
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue

abstract class RelationshipByIdSeekOperator(val workIdentity: WorkIdentity,
                                            relationship: Int,
                                            startNode: Int,
                                            endNode: Int,
                                            relId: SeekArgs,
                                            argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  abstract class RelationshipByIdTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    protected var ids: java.util.Iterator[AnyValue] = _
    protected var cursor: RelationshipScanCursor = _

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      cursor = resources.cursorPools.relationshipScanCursorPool.allocateAndTrace()
      val queryState = state.queryStateForExpressionEvaluation(resources)
      initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      ids = relId.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = RelationshipByIdSeekOperator.this.workIdentity

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

object RelationshipByIdSeekOperator {

  val asIdMethod: Method = method[NumericHelper, Long, AnyValue]("asLongEntityIdPrimitive")


  def taskTemplate(isDirected: Boolean,
                   inner: OperatorTaskTemplate,
                   id: Id,
                   innermost: DelegateOperatorTaskTemplate,
                   relationshipOffset: Int,
                   fromOffset: Int,
                   toOffset: Int,
                   relIds: SeekableArgs,
                   argumentSize: SlotConfiguration.Size,
                   codeGen: OperatorExpressionCompiler): OperatorTaskTemplate = {


    def factory(expr: Expression, isSingle: Boolean) = (isDirected, isSingle) match {
      case (true, true) => new SingleDirectedRelationshipByIdSeekTaskTemplate(inner,
        id,
        innermost,
        relationshipOffset,
        fromOffset,
        toOffset,
        expr,
        argumentSize)(codeGen)
      case (true, false) => new ManyDirectedRelationshipByIdsSeekTaskTemplate(inner,
        id,
        innermost,
        relationshipOffset,
        fromOffset,
        toOffset,
        expr,
        argumentSize)(codeGen)
      case (false, true) => new SingleUndirectedRelationshipByIdSeekTaskTemplate(inner,
        id,
        innermost,
        relationshipOffset,
        fromOffset,
        toOffset,
        expr,
        argumentSize)(codeGen)
      case (false, false) => new ManyUndirectedRelationshipByIdsSeekTaskTemplate(inner,
        id,
        innermost,
        relationshipOffset,
        fromOffset,
        toOffset,
        expr,
        argumentSize)(codeGen)
    }

    relIds match {
      case SingleSeekableArg(expr) => factory(expr, isSingle = true)
      case ManySeekableArgs(expr) => expr match {
        case coll: ListLiteral =>
          ZeroOneOrMany(coll.expressions) match {
            case Zero => OperatorTaskTemplate.empty(id)
            case One(value) => factory(value, isSingle = true)
            case Many(_) => factory(expr, isSingle = false)
          }
        case _ => factory(expr, isSingle = false)
      }
    }
  }


}

class DirectedRelationshipByIdSeekOperator(workIdentity: WorkIdentity,
                                           relationship: Int,
                                           startNode: Int,
                                           endNode: Int,
                                           relId: SeekArgs,
                                           argumentSize: SlotConfiguration.Size)
  extends RelationshipByIdSeekOperator(workIdentity, relationship, startNode, endNode, relId, argumentSize) {

  override def toString: String = "DirectedRelationshipByIdTask"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new RelationshipByIdTask(inputMorsel.nextCopy) {
      override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
        while (outputRow.onValidRow() && ids.hasNext) {
          val nextId = NumericHelper.asLongEntityIdPrimitive(ids.next())
          val read = state.queryContext.transactionalContext.dataRead
          if (nextId >= 0L) {
            read.singleRelationship(nextId, cursor)
            if (cursor.next()) {
              outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
              outputRow.setLongAt(relationship, nextId)
              outputRow.setLongAt(startNode, cursor.sourceNodeReference())
              outputRow.setLongAt(endNode, cursor.targetNodeReference())
              outputRow.next()
            }
          }
        }
      }
    })
  }

}

class UndirectedRelationshipByIdSeekOperator(workIdentity: WorkIdentity,
                                             relationship: Int,
                                             startNode: Int,
                                             endNode: Int,
                                             relId: SeekArgs,
                                             argumentSize: SlotConfiguration.Size)
  extends RelationshipByIdSeekOperator(workIdentity, relationship, startNode, endNode, relId, argumentSize) {

  override def toString: String = "UndirectedRelationshipByIdTask"
  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new RelationshipByIdTask(inputMorsel.nextCopy) {

      /**
       * For an undirected seek we write two rows for each time we progress the cursor, (start) -> (end) and (end) -> (start).
       * When this flag is `true` we will progress the cursor and then write (start) -> (end) and when it is `false` we
       * will not progress the cursor and just write (end) -> (start)
       */
      private var forwardDirection = true

      override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
        while (outputRow.onValidRow && (!forwardDirection || ids.hasNext)) {
          if (forwardDirection) {
            val nextId = NumericHelper.asLongEntityIdPrimitive(ids.next())
            if (nextId >= 0L) {
              state.queryContext.transactionalContext.dataRead.singleRelationship(nextId, cursor)
              if (cursor.next()) {
                outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
                outputRow.setLongAt(relationship, nextId)
                outputRow.setLongAt(startNode, cursor.sourceNodeReference())
                outputRow.setLongAt(endNode, cursor.targetNodeReference())
                outputRow.next()
                forwardDirection = false
              }
            }
          } else {
            outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
            outputRow.setLongAt(relationship, cursor.relationshipReference())
            outputRow.setLongAt(startNode, cursor.targetNodeReference())
            outputRow.setLongAt(endNode, cursor.sourceNodeReference())
            outputRow.next()
            forwardDirection = true
          }
        }
      }
    })
  }
}

abstract class RelationshipByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                id: Id,
                                                innermost: DelegateOperatorTaskTemplate,
                                                codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {


  protected val cursor: InstanceField = field[RelationshipScanCursor](codeGen.namer.nextVariableName())
  protected var relationshipExpression: IntermediateExpression= _

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(cursor)))(
        invokeSideEffect(loadField(cursor), method[RelationshipScanCursor, Unit, KernelReadTracer]("setTracer"), event)
      ),
      inner.genSetExecutionEvent(event)
    )

  override def genExpressions: Seq[IntermediateExpression] = Seq(relationshipExpression)


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

abstract class SingleRelationshipByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                      id: Id,
                                                      innermost: DelegateOperatorTaskTemplate,
                                                      relIdExpr: Expression,
                                                      codeGen: OperatorExpressionCompiler)
  extends RelationshipByIdSeekTaskTemplate(inner,
    id,
    innermost,
    codeGen) {

  protected val idVariable: LocalVariable = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, idVariable)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (relationshipExpression == null) {
      relationshipExpression = codeGen.compileExpression(relIdExpr)
        .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $relIdExpr"))
    }

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
      assign(idVariable, invokeStatic(asIdMethod, nullCheckIfRequired(relationshipExpression))),
      condition(greaterThanOrEqual(load(idVariable), constant(0L))) {
        singleRelationship(load(idVariable), loadField(cursor))
      },
      setField(canContinue, and(greaterThanOrEqual(load(idVariable), constant(0L)),
        cursorNext[RelationshipScanCursor](loadField(cursor)))),
      loadField(canContinue)
    )
  }
}


class SingleDirectedRelationshipByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                     id: Id,
                                                     innermost: DelegateOperatorTaskTemplate,
                                                     relationshipOffset: Int,
                                                     fromOffset: Int,
                                                     toOffset: Int,
                                                     relIdExpr: Expression,
                                                     argumentSize: SlotConfiguration.Size)
                                                    (codeGen: OperatorExpressionCompiler)
  extends SingleRelationshipByIdSeekTaskTemplate(inner,
    id,
    innermost,
    relIdExpr,
    codeGen) {


  override def genMoreFields: Seq[Field] = Seq(cursor)

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
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(relationshipOffset, load(idVariable)),
        codeGen.setLongAt(fromOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
        codeGen.setLongAt(toOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(profileRow(id)),
        innermost.setUnlessPastLimit(canContinue, constant(false)))
    )
  }
}

class SingleUndirectedRelationshipByIdSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                       id: Id,
                                                       innermost: DelegateOperatorTaskTemplate,
                                                       relationshipOffset: Int,
                                                       fromOffset: Int,
                                                       toOffset: Int,
                                                       relIdExpr: Expression,
                                                       argumentSize: SlotConfiguration.Size)
                                                      (codeGen: OperatorExpressionCompiler)
  extends SingleRelationshipByIdSeekTaskTemplate(inner,
    id,
    innermost,
    relIdExpr,
    codeGen) {


  /**
   * For an undirected seek we write two rows for each time we progress the cursor, (start) -> (end) and (end) -> (start).
   * When this flag is `true` we will progress the cursor and then write (start) -> (end) and when it is `false` we
   * will not progress the cursor and just write (end) -> (start)
   */
  private val forwardDirection: Field = field[Boolean](codeGen.namer.nextVariableName(), constant(true))

  override def genMoreFields: Seq[Field] = Seq(cursor, forwardDirection)

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     setLongAt(relOffset, cursor.relationshipReference)
     *     if (this.forwardDirection) {
     *       setLongAt(fromOffset, cursor.sourceNodeReference)
     *       setLongAt(toOffset, cursor.targetNodeReference)
     *     } else {
     *       setLongAt(fromOffset, cursor.targetNodeReference)
     *       setLongAt(toOffset, cursor.sourceNodeReference)
     *     }
     *     << inner.genOperate >>
     *     this.forwardDirection = !this.forwardDirection
     *     this.canContinue = !forwardDirection
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(relationshipOffset,
          invoke(loadField(cursor), method[RelationshipScanCursor, Long]("relationshipReference"))),
        ifElse(loadField(forwardDirection)) {
          //this is the on true block of if {} else {}
          block(
            codeGen.setLongAt(fromOffset,
              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            codeGen.setLongAt(toOffset,
              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference")))
          )
        } {
          //else block of if {} else {}
          block(
            codeGen.setLongAt(fromOffset,
              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            codeGen.setLongAt(toOffset,
              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference")))
          )
        },
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          block(
            profileRow(id),
            setField(forwardDirection, not(loadField(forwardDirection)))
          )
        ),
        innermost.setUnlessPastLimit(canContinue, not(loadField(forwardDirection)))))
  }
}

abstract class ManyRelationshipByIdsSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                     id: Id,
                                                     innermost: DelegateOperatorTaskTemplate,
                                                     relIdsExpr: Expression,
                                                     codeGen: OperatorExpressionCompiler)
  extends RelationshipByIdSeekTaskTemplate(inner,
    id,
    innermost,
    codeGen) {


  protected val idCursor: InstanceField = field[IteratorCursor](codeGen.namer.nextVariableName())

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    if (relationshipExpression == null) {
      relationshipExpression = codeGen.compileExpression(relIdsExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $relIdsExpr"))
    }

    /**
     * {{{
     *   this.cursor = resources.cursorPools.relationshipScanCursorPool.allocate()
     *   this.idCursor = IteratorCursor(((ListValue) <<reldIdsExpr>>)).iterator())
     *   this.canContinue = idCursor.next
     *   this.canContinue
     * }}}
     */
    block(
      setField(cursor, ALLOCATE_REL_SCAN_CURSOR),
      setField(idCursor,
        invokeStatic(method[IteratorCursor, IteratorCursor, java.util.Iterator[_]]("apply"),
          invoke(cast[ListValue](nullCheckIfRequired(relationshipExpression)),
            method[ListValue, util.Iterator[AnyValue]]("iterator")))),
      setField(canContinue, cursorNext[IteratorCursor](loadField(idCursor))),
      loadField(canContinue))
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
                                                   (codeGen: OperatorExpressionCompiler)
  extends ManyRelationshipByIdsSeekTaskTemplate(inner,
    id,
    innermost,
    relIdsExpr,
    codeGen) {

  override def genMoreFields: Seq[Field] = Seq(cursor, idCursor)

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
            invoke(loadField(idCursor),
              method[IteratorCursor, AnyValue]("value"))))),
        condition(greaterThanOrEqual(load(idVariable), constant(0L))) {
          singleRelationship(load(idVariable), loadField(cursor)) },

        condition(and(greaterThanOrEqual(load(idVariable), constant(0L)), cursorNext[RelationshipScanCursor](loadField(cursor))))(
          block(
            codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
            codeGen.setLongAt(relationshipOffset, load(idVariable)),
            codeGen.setLongAt(fromOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            codeGen.setLongAt(toOffset, invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            inner.genOperateWithExpressions,
            doIfInnerCantContinue(profileRow(id))
          )),
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, cursorNext[IteratorCursor](loadField(idCursor)))))
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
                                                     (codeGen: OperatorExpressionCompiler)
  extends ManyRelationshipByIdsSeekTaskTemplate(inner,
    id,
    innermost,
    relIdsExpr,
    codeGen) {


  /**
   * For an undirected seek we write two rows for each time we progress the cursor, (start) -> (end) and (end) -> (start).
   * When this flag is `true` we will progress the cursor and then write (start) -> (end) and when it is `false` we
   * will not progress the cursor and just write (end) -> (start)
   */
  private val forwardDirection: Field = field[Boolean](codeGen.namer.nextVariableName(), constant(true))
  override def genMoreFields: Seq[Field] = Seq(idCursor, cursor, forwardDirection)

  override protected def genInnerLoop: IntermediateRepresentation = {
    val idVariable = codeGen.namer.nextVariableName()

    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     val id = if (this.forwardDirection) asId(idCursor.value()) else cursor.relationshipReference()
     *     if (id >= 0) read.singleRelationship(id, cursor)
     *
     *     if (id >= 0 && (!this.forwardDirection || cursor.next())) {
     *       setLongAt(relOffset, id)
     *       if (this.forwardDirection) {
     *         setLongAt(fromOffset, cursor.sourceNodeReference)
     *         setLongAt(toOffset, cursor.targetNodeReference)
     *       } else {
     *         setLongAt(fromOffset, cursor.targetNodeReference)
     *         setLongAt(toOffset, cursor.sourceNodeReference)
     *       }
     *       << inner.genOperate >>
     *       this.forwardDirection = !this.forwardDirection
     *      }
     *      this.canContinue = !this.forwardDirection || idIterator.hasNext()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        declareAndAssign(typeRefOf[Long], idVariable,
          ternary(loadField(forwardDirection),
            invokeStatic(asIdMethod, cast[AnyValue](
              invoke(loadField(idCursor),
                method[IteratorCursor, AnyValue]("value")))),
            invoke(loadField(cursor), method[RelationshipScanCursor, Long]("relationshipReference")))),
        condition(greaterThanOrEqual(load(idVariable), constant(0L))) {
          singleRelationship(load(idVariable), loadField(cursor)) },
        condition(and(greaterThanOrEqual(load(idVariable), constant(0L)),
         cursorNext[RelationshipScanCursor](loadField(cursor))))(
          block(
            codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
            codeGen.setLongAt(relationshipOffset, load(idVariable)),
            ifElse(loadField(forwardDirection))(block(
              codeGen.setLongAt(fromOffset,
                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
              codeGen.setLongAt(toOffset,
                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            ))(block(
              codeGen.setLongAt(fromOffset,
                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
              codeGen.setLongAt(toOffset,
                invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            )),
            inner.genOperateWithExpressions,
            doIfInnerCantContinue(
              block(
                profileRow(id),
                setField(forwardDirection, not(loadField(forwardDirection))),
              )
            )
          )),
        doIfInnerCantContinue(
          block(
            innermost.setUnlessPastLimit(canContinue,
            or(not(loadField(forwardDirection)), cursorNext[IteratorCursor](loadField(idCursor)))))),
        )
    )
  }
}
