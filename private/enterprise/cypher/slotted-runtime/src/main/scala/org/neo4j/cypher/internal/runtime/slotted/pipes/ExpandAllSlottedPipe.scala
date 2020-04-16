/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPropertyKeys
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections
import org.neo4j.values.storable.Value

import scala.collection.mutable

case class ExpandAllSlottedPipe(source: Pipe,
                                fromSlot: Slot,
                                relOffset: Int,
                                toOffset: Int,
                                dir: SemanticDirection,
                                types: RelationshipTypes,
                                slots: SlotConfiguration,
                                nodePropsToRead: Option[SlottedPropertyKeys] = None,
                                relsPropsToRead: Option[SlottedPropertyKeys] = None)
                               (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(fromSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {
    input.flatMap {
      inputRow: CypherRow =>
        val fromNode = getFromNodeFunction.applyAsLong(inputRow)

        if (NullChecker.entityIsNull(fromNode)) {
          Iterator.empty
        } else {
          val nodeCursor = state.query.nodeCursor()
          val relCursor = state.query.traversalCursor()
          try {
            val read = state.query.transactionalContext.dataRead
            read.singleNode(fromNode, nodeCursor)
            if (!nodeCursor.next()) {
              Iterator.empty
            } else {
              val nodePropsToCache = getNodePropertiesToCache(nodeCursor, state.cursors.propertyCursor, state.query)
              val selectionCursor = dir match {
                case OUTGOING => RelationshipSelections.outgoingCursor(relCursor, nodeCursor, types.types(state.query))
                case INCOMING => RelationshipSelections.incomingCursor(relCursor, nodeCursor, types.types(state.query))
                case BOTH => RelationshipSelections.allCursor(relCursor, nodeCursor, types.types(state.query))
              }
              state.query.resources.trace(selectionCursor)
              new Iterator[CypherRow] {
                private var initialized = false
                private var hasMore = false

                private def fetchNext(): Boolean =
                  if (selectionCursor.next()) {
                    true
                  } else {
                    selectionCursor.close()
                    false
                  }

                override def hasNext: Boolean = {
                  if (!initialized) {
                    hasMore = fetchNext()
                    initialized = true
                  }

                  hasMore
                }

                override def next(): CypherRow = {
                  if (!hasNext) {
                    selectionCursor.close()
                    Iterator.empty.next()
                  }

                  val outputRow = SlottedRow(slots)
                  inputRow.copyTo(outputRow)
                  outputRow.setLongAt(relOffset, selectionCursor.relationshipReference())
                  outputRow.setLongAt(toOffset, selectionCursor.otherNodeReference())
                  nodePropsToCache.foreach {
                    case (offset, value) => outputRow.setCachedPropertyAt(offset, value)
                  }
                  cacheRelationshipProperties(relCursor, state.cursors.propertyCursor, outputRow, state.query)

                  hasMore = fetchNext()
                  outputRow
                }
              }
            }
          } finally {
            nodeCursor.close()
          }
        }
    }
  }

  private def getNodePropertiesToCache(nodeCursor: NodeCursor,
                                       propertyCursor: PropertyCursor,
                                       queryContext: QueryContext): Seq[(Int, Value)] = {
    nodePropsToRead.map(p => {
      nodeCursor.properties(propertyCursor)
      val props = mutable.ArrayBuffer.empty[(Int, Value)]
      while (propertyCursor.next() && p.accept(queryContext, propertyCursor.propertyKey())) {
        props += (p.offset -> propertyCursor.propertyValue())
      }
      props
    }).getOrElse(Seq.empty)
  }

  private def cacheRelationshipProperties(relationships: RelationshipTraversalCursor,
                                          propertyCursor: PropertyCursor,
                                          outputRow: CypherRow, queryContext: QueryContext): Unit = {
    relsPropsToRead.foreach(p => {
      relationships.properties(propertyCursor)
      while (propertyCursor.next() && p.accept(queryContext, propertyCursor.propertyKey())) {
        outputRow.setCachedPropertyAt(p.offset, propertyCursor.propertyValue())
      }
    })
  }
}
