/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexIteratorBase
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor

/**
 * Provides helper methods for slotted index pipes that get nodes together with actual property values.
 */
trait IndexSlottedPipeWithValues extends Pipe {

  // Offset of the long slot of node variable
  val offset: Int
  // the indices of the index properties where we will get values
  val indexPropertyIndices: Array[Int]
  // the offsets of the cached node property slots where we will set values
  val indexPropertySlotOffsets: Array[Int]

  class SlottedNodeIndexIterator(state: QueryState,
                                 cursor: NodeValueIndexCursor
                                ) extends IndexIteratorBase[CypherRow](cursor) {

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {
        val slottedContext = state.newRowWithArgument(rowFactory)
        slottedContext.setLongAt(offset, cursor.nodeReference())
        var i = 0
        while (i < indexPropertyIndices.length) {
          val value = cursor.propertyValue(indexPropertyIndices(i))
          slottedContext.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
          i += 1
        }
        slottedContext
      } else null
    }
  }

  class SlottedRelationshipIndexIterator(state: QueryState,
                                         startOffset: Int,
                                         endOffset: Int,
                                         cursor: RelationshipValueIndexCursor
                                        ) extends IndexIteratorBase[CypherRow](cursor) {

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {
        val slottedContext = state.newRowWithArgument(rowFactory)
        slottedContext.setLongAt(offset, cursor.relationshipReference())
        //NOTE: sourceNodeReference and targetNodeReference is not implemented yet on the cursor
        val internalCursor = state.cursors.relationshipScanCursor
        state.query.singleRelationship(cursor.relationshipReference(), internalCursor)
        internalCursor.next()
        slottedContext.setLongAt(startOffset, internalCursor.sourceNodeReference())
        slottedContext.setLongAt(endOffset, internalCursor.targetNodeReference())
        var i = 0
        while (i < indexPropertyIndices.length) {
          val value = cursor.propertyValue(indexPropertyIndices(i))
          slottedContext.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
          i += 1
        }
        slottedContext
      } else null
    }
  }

  class SlottedUndirectedRelationshipIndexIterator(state: QueryState,
                                                   startOffset: Int,
                                                   endOffset: Int,
                                                   cursor: RelationshipValueIndexCursor
                                                  ) extends IndexIteratorBase[CypherRow](cursor) {

    private var emitSibling: Boolean = false
    private var lastRelationship: Long = -1L
    private var lastStart: Long = -1L
    private var lastEnd: Long = -1L

    override protected def fetchNext(): CypherRow = {
      val newContext = if (emitSibling) {
        emitSibling = false
        val slottedContext = state.newRowWithArgument(rowFactory)
        slottedContext.setLongAt(offset, lastRelationship)
        slottedContext.setLongAt(startOffset, lastEnd)
        slottedContext.setLongAt(endOffset, lastStart)
        slottedContext
      } else if (cursor.next()) {
        emitSibling = true
        val slottedContext = state.newRowWithArgument(rowFactory)
        //NOTE: sourceNodeReference and targetNodeReference is not implemented yet on the cursor
        val internalCursor = state.cursors.relationshipScanCursor
        state.query.singleRelationship(cursor.relationshipReference(), internalCursor)
        internalCursor.next()
        lastRelationship = internalCursor.relationshipReference()
        lastStart = internalCursor.sourceNodeReference()
        lastEnd = internalCursor.targetNodeReference()
        slottedContext.setLongAt(offset, lastRelationship)
        slottedContext.setLongAt(startOffset, lastStart)
        slottedContext.setLongAt(endOffset, lastEnd)
        slottedContext
      } else {
        null
      }

      if (newContext != null) {
        var i = 0
        while (i < indexPropertyIndices.length) {
          val value = cursor.propertyValue(indexPropertyIndices(i))
          newContext.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
          i += 1
        }
      }
      newContext
    }
  }

}
