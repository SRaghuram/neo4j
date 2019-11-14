/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.logical.plans.{ManySeekableArgs, SeekableArgs, SingleSeekableArg}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{PipelinedExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.RelationshipByIdSeekOperator.asIdMethod
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, ListLiteral}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.{Many, One, Zero, ZeroOneOrMany}
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue

abstract class RelationshipByIdSeekOperator(val workIdentity: WorkIdentity,
                                   relationship: Int,
                                   startNode: Int,
                                   endNode: Int,
                                   relId: SeekArgs,
                                   argumentSize: SlotConfiguration.Size) extends StreamingOperator {



  abstract class RelationshipByIdTask(val inputMorsel: PipelinedExecutionContext) extends InputLoopTask {


    protected var ids: java.util.Iterator[AnyValue] = _
    protected var cursor: RelationshipScanCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      cursor = resources.cursorPools.relationshipScanCursorPool.allocateAndTrace()
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

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    IndexedSeq(new RelationshipByIdTask(inputMorsel.nextCopy) {
      override protected def innerLoop(outputRow: PipelinedExecutionContext, context: QueryContext, state: QueryState): Unit = {
        while (outputRow.isValidRow && ids.hasNext) {
          val nextId = NumericHelper.asLongEntityIdPrimitive(ids.next())
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

  /**
    * For an undirected seek we write two rows for each time we progress the cursor, (start) -> (end) and (end) -> (start).
    * When this flag is `true` we will progress the cursor and then write (start) -> (end) and when it is `false` we
    * will not progress the cursor and just write (end) -> (start)
    */
  private var forwardDirection = true

  override def toString: String = "UndirectedRelationshipByIdTask"
  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    IndexedSeq(new RelationshipByIdTask(inputMorsel.nextCopy) {
      override protected def innerLoop(outputRow: PipelinedExecutionContext, context: QueryContext, state: QueryState): Unit = {
        while (outputRow.isValidRow && (!forwardDirection || ids.hasNext)) {
          if (forwardDirection) {
            val nextId = NumericHelper.asLongEntityIdPrimitive(ids.next())
            if (nextId >= 0L) {
              context.transactionalContext.dataRead.singleRelationship(nextId, cursor)
              if (cursor.next()) {
                outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
                outputRow.setLongAt(relationship, nextId)
                outputRow.setLongAt(startNode, cursor.sourceNodeReference())
                outputRow.setLongAt(endNode, cursor.targetNodeReference())
                outputRow.moveToNextRow()
                forwardDirection = false
              }
            }
          } else {
            outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
            outputRow.setLongAt(relationship, cursor.relationshipReference())
            outputRow.setLongAt(startNode, cursor.targetNodeReference())
            outputRow.setLongAt(endNode, cursor.sourceNodeReference())
            outputRow.moveToNextRow()
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
                                                relationshipOffset: Int,
                                                fromOffset: Int,
                                                toOffset: Int,
                                                relIdExpr: Expression,
                                                argumentSize: SlotConfiguration.Size,
                                                codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

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
                                                      relationshipOffset: Int,
                                                      fromOffset: Int,
                                                      toOffset: Int,
                                                      relIdExpr: Expression,
                                                      argumentSize: SlotConfiguration.Size,
                                                      codeGen: OperatorExpressionCompiler)
  extends RelationshipByIdSeekTaskTemplate(inner,
                                           id,
                                           innermost,
                                           relationshipOffset,
                                           fromOffset,
                                           toOffset,
                                           relIdExpr,
                                           argumentSize,
                                           codeGen) {
  import OperatorCodeGenHelperTemplates._

  protected val idVariable: LocalVariable = variable[Long](codeGen.namer.nextVariableName(), constant(-1L))

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V, idVariable)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    relationshipExpression = codeGen.intermediateCompileExpression(relIdExpr)
      .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $relIdExpr"))

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
  extends SingleRelationshipByIdSeekTaskTemplate(inner, id, innermost, relationshipOffset, fromOffset, toOffset,
                                                 relIdExpr, argumentSize, codeGen) {

  import OperatorCodeGenHelperTemplates._

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
        setField(canContinue, constant(false)))
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
  extends SingleRelationshipByIdSeekTaskTemplate(inner, id, innermost, relationshipOffset, fromOffset, toOffset,
                                                 relIdExpr, argumentSize, codeGen) {

  import OperatorCodeGenHelperTemplates._

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
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(relationshipOffset, load(idVariable)),
        ifElse(loadField(forwardDirection)) {
          //this is the on true block of if {} else {}
          block(
            codeGen.setLongAt(fromOffset,
                              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            codeGen.setLongAt(toOffset,
                              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            setField(forwardDirection, constant(false)))
        } {
          //else block of if {} else {}
          block(
            codeGen.setLongAt(fromOffset,
                              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            codeGen.setLongAt(toOffset,
                              invoke(loadField(cursor), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            setField(forwardDirection, constant(true)))
        },
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(profileRow(id)),
        setField(canContinue, not(loadField(forwardDirection)))))
  }
}

abstract class ManyRelationshipByIdsSeekTaskTemplate(inner: OperatorTaskTemplate,
                                                    id: Id,
                                                    innermost: DelegateOperatorTaskTemplate,
                                                    relationshipOffset: Int,
                                                    fromOffset: Int,
                                                    toOffset: Int,
                                                    relIdsExpr: Expression,
                                                    argumentSize: SlotConfiguration.Size,
                                                    codeGen: OperatorExpressionCompiler)
  extends RelationshipByIdSeekTaskTemplate(inner,
                                           id,
                                           innermost,
                                           relationshipOffset,
                                           fromOffset,
                                           toOffset,
                                           relIdsExpr,
                                           argumentSize,
                                           codeGen) {

  import OperatorCodeGenHelperTemplates._

  protected val idCursor: InstanceField = field[IteratorCursor](codeGen.namer.nextVariableName())

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    relationshipExpression = codeGen.intermediateCompileExpression(relIdsExpr).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $relIdsExpr"))

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
                                                relationshipOffset,
                                                fromOffset,
                                                toOffset,
                                                relIdsExpr,
                                                argumentSize,
                                                codeGen) {
  import OperatorCodeGenHelperTemplates._

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
        doIfInnerCantContinue(setField(canContinue, cursorNext[IteratorCursor](loadField(idCursor)))))
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
                                                relationshipOffset,
                                                fromOffset,
                                                toOffset,
                                                relIdsExpr,
                                                argumentSize,
                                                codeGen) {

  import OperatorCodeGenHelperTemplates._

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
      *         this.forwardDirection = false
      *       } else {
      *         setLongAt(fromOffset, cursor.targetNodeReference)
      *         setLongAt(toOffset, cursor.sourceNodeReference)
      *         this.forwardDirection = true
      *       }
      *       << inner.genOperate >>
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
                      or(not(loadField(forwardDirection)), cursorNext[RelationshipScanCursor](loadField(cursor)))))(
          block(
            codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
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
            inner.genOperateWithExpressions,
            doIfInnerCantContinue(profileRow(id))
            )),
        doIfInnerCantContinue(
          setField(canContinue,
                   or(not(loadField(forwardDirection)), cursorNext[IteratorCursor](loadField(idCursor))))))
      )
  }
}
