/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api._
import org.neo4j.values.storable.{Value, Values}

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
          case p: IndexQuery.ExactPredicate => p.value() == Values.NO_VALUE
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


class SingleQueryExactNodeIndexSeekTaskTemplate(override val inner: OperatorTaskTemplate,
                                                id: Id,
                                                innermost: DelegateOperatorTaskTemplate,
                                                nodeVarName: String,
                                                offset: Int,
                                                property: SlottedIndexedProperty,
                                                rawExpression: expressions.Expression,
                                                queryIndexId: Int,
                                                indexOrder: IndexOrder,
                                                argumentSize: SlotConfiguration.Size)
                                               (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._
  private var query: IntermediateExpression = _

  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())

  override def genFields: Seq[Field] = {
    (super.genFields ++ query.fields :+ nodeIndexCursorField ) ++ inner.genFields
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    query.variables ++ inner.genLocalVariables :+ CURSOR_POOL_V
  }

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val compiled = codeGen.intermediateCompileExpression(rawExpression).getOrElse(throw new CantCompileQueryException())
    query = compiled.copy(ir = asValue(compiled.ir))
    val input = invokeStatic(method[IndexQuery, ExactPredicate, Int, Object]("exact"), constant(property.propertyKeyId),
                             asValue(query.ir))
    block(
      setField(nodeIndexCursorField, ALLOCATE_NODE_INDEX_CURSOR),
      nodeIndexSeek(indexReadSession(queryIndexId), loadField(nodeIndexCursorField), indexOrder, input),
      setField(canContinue, cursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField))),
      constant(true))
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
        property.maybeCachedNodePropertySlot.map(codeGen.setCachedPropertyAt(_, query.ir)).getOrElse(noop()),
        profileRow(id),
        inner.genOperate,
        setField(canContinue, cursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField)))
        )
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   resources.cursorPools.nodeValueIndexCursorPool.free(nodeLabelCursor)
      *   nodeValueCursor = null
      * }}}
      */
    block(
      freeCursor[NodeValueIndexCursor](loadField(nodeIndexCursorField), NodeValueIndexCursorPool),
      setField(nodeIndexCursorField, constant(null))
      )
  }
}
