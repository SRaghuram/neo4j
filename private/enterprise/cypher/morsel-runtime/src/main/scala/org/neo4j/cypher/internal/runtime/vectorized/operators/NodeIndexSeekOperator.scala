/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexSeek, IndexSeekMode, NodeIndexSeeker, QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.v4_0.logical.plans.{IndexOrder, QueryExpression}
import org.neo4j.internal.kernel.api._
import org.neo4j.values.storable.Value
import org.neo4j.cypher.internal.v4_0.expressions.LabelToken

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

  override def init(context: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): ContinuableOperatorTask = {
    new OTask(inputMorsel)
  }

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId)

  class OTask(val inputRow: MorselExecutionContext) extends StreamingContinuableOperatorTask {

    private var nodeCursors: Iterator[NodeValueIndexCursor] = _
    private var nodeCursor: NodeValueIndexCursor = _

    private def next(): Boolean = {
      while (true) {
        if (nodeCursor != null && nodeCursor.next())
          return true
        else if (nodeCursors.hasNext)
          nodeCursor = nodeCursors.next()
        else {
          return false
        }
      }
      false // because scala compiler doesn't realize that this line is unreachable
    }

    override protected def initializeInnerLoop(inputRow: MorselExecutionContext,
                                               context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession])
      nodeCursors = indexSeek(queryState, state.queryIndexes(queryIndexId), needsValues, indexOrder, inputRow)
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.hasMoreRows && next()) {
        outputRow.setLongAt(offset, nodeCursor.nodeReference())
        var i = 0
        while (i < indexPropertyIndices.length) {
          outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), nodeCursor.propertyValue(indexPropertyIndices(i)))
          i += 1
        }
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      nodeCursors.foreach(cursor =>
        resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      )
      nodeCursors = null
      nodeCursor = null
    }
  }
}

class NodeWithValues(val nodeId: Long, val values: Array[Value])
