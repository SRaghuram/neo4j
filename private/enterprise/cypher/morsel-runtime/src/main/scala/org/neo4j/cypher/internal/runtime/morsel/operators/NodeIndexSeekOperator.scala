/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.LabelToken
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api._
import org.neo4j.values.storable.{Value, Values}

class NodeIndexSeekOperator(val workIdentity: WorkIdentity,
                            offset: Int,
                            label: LabelToken,
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

    private var nodeCursors: Iterator[Seq[IndexQuery]] = _
    private var nodeCursor: NodeValueIndexCursor = _
    private var exactSeekValues: Array[Value] = _

    // INPUT LOOP TASK

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {

      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots))
      nodeCursors = indexQueries(queryState, inputMorsel)
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
