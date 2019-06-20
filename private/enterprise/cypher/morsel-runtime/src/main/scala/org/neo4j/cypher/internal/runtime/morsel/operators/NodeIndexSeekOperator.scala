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
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.ManyQueriesExactNodeIndexSeekTaskTemplate.{nextMethod, queryIteratorMethod}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext, makeValueNeoSafe}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.ListValue

class NodeIndexSeekOperator(val workIdentity: WorkIdentity,
                            offset: Int,
                            properties: Array[SlottedIndexedProperty],
                            queryIndexId: Int,
                            indexOrder: IndexOrder,
                            argumentSize: SlotConfiguration.Size,
                            override val valueExpr: QueryExpression[Expression],
                            override val indexMode: IndexSeekMode = IndexSeek)
  extends StreamingOperator with NodeIndexSeeker {

  private val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)
  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    IndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId)

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = NodeIndexSeekOperator.this.workIdentity

    private var nodeCursors: Iterator[Seq[IndexQuery]] = _
    private var nodeCursor: NodeValueIndexCursor = _
    private var exactSeekValues: Array[Value] = _

    // INPUT LOOP TASK

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {

      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots))
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      nodeCursors = indexQueries(queryState, initExecutionContext)
      nodeCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      val read = context.transactionalContext.transaction.dataRead()
      while (outputRow.isValidRow && next(state, read)) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, nodeCursor.nodeReference())
        var i = 0
        while (i < indexPropertyIndices.length) {
          val indexPropertyIndex = indexPropertyIndices(i)
          val value =
            if (exactSeekValues != null) exactSeekValues(indexPropertyIndex)
            else nodeCursor.propertyValue(indexPropertyIndex)

          outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
          i += 1
        }
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      nodeCursors = null
      resources.cursorPools.nodeValueIndexCursorPool.free(nodeCursor)
      nodeCursor = null
    }

    // HELPERS

    private def next(state: QueryState, read: Read): Boolean = {
      while (true) {
        if (nodeCursor != null && nodeCursor.next()) {
          return true
        } else if (nodeCursors.hasNext) {
          val indexQuery = nodeCursors.next()
          seek(state.queryIndexes(queryIndexId), nodeCursor, read, indexQuery)
        } else {
          return false
        }
      }
      throw new IllegalStateException("Unreachable code")
    }

    private def seek[RESULT <: AnyRef](index: IndexReadSession,
                                       nodeCursor: NodeValueIndexCursor,
                                       read: Read,
                                       predicates: Seq[IndexQuery]): Unit = {

      val impossiblePredicate =
        predicates.exists {
          case p: ExactPredicate => p.value() == Values.NO_VALUE
          case _: IndexQuery.ExistsPredicate if predicates.length > 1 => false
          case p: IndexQuery =>
            !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        }

      if (impossiblePredicate) {
        return // leave cursor un-initialized/empty
      }

      // We don't need property values from the index for an exact seek
      exactSeekValues =
        if (needsValues && predicates.forall(_.isInstanceOf[ExactPredicate]))
          predicates.map(_.asInstanceOf[ExactPredicate].value()).toArray
        else
          null

      val needsValuesFromIndexSeek = exactSeekValues == null && needsValues
      read.nodeIndexSeek(index, nodeCursor, indexOrder, needsValuesFromIndexSeek, predicates: _*)
    }
  }

  // index seek
  protected def indexQueries[RESULT <: AnyRef](state: OldQueryState,
                                               baseContext: ExecutionContext): Iterator[Seq[IndexQuery]] =
    indexMode match {
      case _: ExactSeek |
           _: SeekByRange =>
        computeIndexQueries(state, baseContext).toIterator

      case LockingUniqueIndexSeek =>
        throw new UnsupportedOperationException("not implemented")
    }
}

class NodeWithValues(val nodeId: Long, val values: Array[Value])

/**
  * Code generation template for index seeks of the form `MATCH (n:L) WHERE n.prop = 42`
  */
class SingleQueryExactNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                                id: Id,
                                                innermost: DelegateOperatorTaskTemplate,
                                                nodeVarName: String,
                                                offset: Int,
                                                property: SlottedIndexedProperty,
                                                rawExpression: expressions.Expression,
                                                queryIndexId: Int,
                                                argumentSize: SlotConfiguration.Size)
                                               (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._
  private var query: IntermediateExpression = _
  private val queryVariable = codeGen.namer.nextVariableName()

  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())

  override def genFields: Seq[Field] = {
    (super.genFields ++ query.fields :+ nodeIndexCursorField ) ++ inner.genFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    query.variables ++ inner.genLocalVariables :+ CURSOR_POOL_V
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val compiled = codeGen.intermediateCompileExpression(rawExpression).getOrElse(throw new CantCompileQueryException())
    query = compiled.copy(ir = asStorableValue(compiled.ir))
    val hasInnerLoopVar = codeGen.namer.nextVariableName()

    /**
      * {{{
      *   val query = asStorable([query predicate])
      *   val hasInnerLoop = query != NO_VALUE
      *   if (hasInnerLoop) {
      *     nodeIndexCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
      *     context.transactionalContext.dataRead.nodeIndexSeek(indexReadSession(queryIndexId),
      *                                                         nodeIndexCursor,
      *                                                         IndexQuery.exact(prop, query))
      *     this.canContinue = nodeLabelCursor.next()
      *    }
      *    hasInnerLoop
      * }}}
      */
    block(
      declareAndAssign(typeRefOf[Value], queryVariable, query.ir),
      declareAndAssign(typeRefOf[Boolean], hasInnerLoopVar, notEqual(load(queryVariable), noValue)),
      condition(load(hasInnerLoopVar))(
        block(
          setField(nodeIndexCursorField, ALLOCATE_NODE_INDEX_CURSOR),
          nodeIndexSeek(indexReadSession(queryIndexId), loadField(nodeIndexCursorField),
                        exactSeek(property.propertyKeyId, load(queryVariable))),
          setField(canContinue, cursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField)))
          )),
      load(hasInnerLoopVar)
      )
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     ...
      *     << inner.genOperate >>
      *     setLongAt(offset, nodeIndexCursor.nodeReference())
      *     setCachedPropertyAt(offset, value)
      *     this.canContinue = this.nodeIndexCursor.next()
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
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        property.maybeCachedNodePropertySlot.map(codeGen.setCachedPropertyAt(_, load(queryVariable))).getOrElse(noop()),
        profileRow(id),
        inner.genOperate,
        setField(canContinue, cursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField)))
        )
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   resources.cursorPools.nodeValueIndexCursorPool.free(nodeIndexCursor)
      *   nodeIndexCursor = null
      * }}}
      */
    block(
      freeCursor[NodeValueIndexCursor](loadField(nodeIndexCursorField), NodeValueIndexCursorPool),
      setField(nodeIndexCursorField, constant(null))
      )
  }
}

/**
  * Code generation template for index seeks of the form `MATCH (n:L) WHERE n.prop = 1 OR n.prop = 2 OR n.prop =...`
  * Generates code for first doing an exact search of the first predicate and when the result is exhausted
  * it moves on to the next predicate until all predicates have been visited.
  */
class ManyQueriesExactNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                                id: Id,
                                                innermost: DelegateOperatorTaskTemplate,
                                                nodeVarName: String,
                                                offset: Int,
                                                property: SlottedIndexedProperty,
                                                rawListExpression: expressions.Expression,
                                                queryIndexId: Int,
                                                argumentSize: SlotConfiguration.Size)
                                               (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._
  private var queries: IntermediateExpression = _
  private val queryVariable = codeGen.namer.nextVariableName()

  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val queryIteratorField = field[ExactPredicateIterator](codeGen.namer.nextVariableName())

  override def genFields: Seq[Field] = {
    (super.genFields ++ queries.fields :+ nodeIndexCursorField :+ queryIteratorField) ++ inner.genFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    queries.variables ++ inner.genLocalVariables :+ CURSOR_POOL_V
  }


  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val compiled = codeGen.intermediateCompileExpression(rawListExpression).getOrElse(throw new CantCompileQueryException())
    queries = compiled.copy(ir = asListValue(compiled.ir))
    /**
      * {{{
      *   val queries = asListValue([query predicate])
      *   queryIterator = queryIterator(property, queries)
      *   nodeIndexCursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
      *   this.canContinue = next(indexReadSession(queryIndexId), nodeIndexCursor, queryIterator, read)
      *   true
      * }}}
      */
    block(
      declareAndAssign(typeRefOf[ListValue], queryVariable, queries.ir),
      setField(queryIteratorField,
               invokeStatic(queryIteratorMethod, constant(property.propertyKeyId), load(queryVariable))),
      setField(nodeIndexCursorField, ALLOCATE_NODE_INDEX_CURSOR),
      setField(canContinue,
               invokeStatic(nextMethod,
                            indexReadSession(queryIndexId),
                            loadField(nodeIndexCursorField),
                            loadField(queryIteratorField),
                            loadField(DATA_READ))),
      constant(true))
  }

  override protected def genInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     ...
      *     << inner.genOperate >>
      *     setLongAt(offset, nodeIndexCursor.nodeReference())
      *     setCachedPropertyAt(offset, queryIterator.current)//only if applicable
      *     this.canContinue = next(indexReadSession(queryIndexId), nodeIndexCursor, queryIterator, read)
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
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        property.maybeCachedNodePropertySlot.map(codeGen.setCachedPropertyAt(_,
                                                                             invoke(
                                                                               loadField(queryIteratorField),
                                                                               method[ExactPredicateIterator, Value](
                                                                                 "current")))).getOrElse(noop()),
        profileRow(id),
        inner.genOperate,
        setField(canContinue,
                 invokeStatic(nextMethod,
                              indexReadSession(queryIndexId),
                              loadField(nodeIndexCursorField),
                              loadField(queryIteratorField),
                              loadField(DATA_READ)))
        )
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   resources.cursorPools.nodeValueIndexCursorPool.free(nodeIndexCursor)
      *   nodeIndexCursor = null
      * }}}
      */
    block(
      freeCursor[NodeValueIndexCursor](loadField(nodeIndexCursorField), NodeValueIndexCursorPool),
      setField(nodeIndexCursorField, constant(null))
      )
  }
}

/**
  * These are helper methods used by the generated code
  */
object ManyQueriesExactNodeIndexSeekTaskTemplate {

  /**
    * Exhausts the cursor and when that is done picks the next query and initializes the cursor for that query.
    * Continues until the cursor has been exhausted for all provided queries.
    * @return `true` if there is data otherwise `false`
    */
  def next(index: IndexReadSession,
           cursor: NodeValueIndexCursor,
           queries: ExactPredicateIterator,
           read: Read): Boolean = {
    while (true) {
      if (cursor.next()) {
        return true
      }
      else if (queries.hasNext) {
        var continue = true
        while (continue) {
          val indexQuery = queries.next()
          if (!(indexQuery.value() eq Values.NO_VALUE)) {
            read.nodeIndexSeek(index, cursor, IndexOrder.NONE, false, indexQuery)
            continue = false
          } else {
            continue = queries.hasNext
          }
        }
      } else {
        return false
      }
    }
    throw new IllegalStateException("Unreachable code")
  }

  /**
    * Creates a specilized iterator over the queries for the seek
    */
  def queryIterator(propertyKey: Int, list: ListValue) = new ExactPredicateIterator(propertyKey, list)

  val nextMethod: Method =
    method[ManyQueriesExactNodeIndexSeekTaskTemplate,
           Boolean,
           IndexReadSession,
           NodeValueIndexCursor,
           ExactPredicateIterator,
           Read]("next")

  val queryIteratorMethod: Method =
    method[ManyQueriesExactNodeIndexSeekTaskTemplate, ExactPredicateIterator, Int, ListValue]("queryIterator")
}

class ExactPredicateIterator(propertyKey: Int, list: ListValue) {
  private val inner: util.Iterator[AnyValue] = list.distinct.iterator()
  private var _current :Value = _

  def hasNext: Boolean = inner.hasNext

  def next(): ExactPredicate = {
    _current = makeValueNeoSafe(inner.next)
    IndexQuery.exact(propertyKey, _current)
  }

  def current: Value = _current
}
