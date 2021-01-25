/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class UndirectedRelationshipTypeScanSlottedPipe(relOffset: Int, fromOffset: Int, typ: LazyType, toOffset: Int)
                                                    (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val typeId = typ.getId(state.query)
    if (typeId == LazyType.UNKNOWN) {
      ClosingIterator.empty
    } else {
      new UndirectedIterator(relOffset, typeId, fromOffset, toOffset, rowFactory, state)
    }
  }
}

private class UndirectedIterator(relOffset: Int,
                                 relToken: Int,
                                 fromOffset: Int,
                                 toOffset: Int,
                                 rowFactory: CypherRowFactory,
                                 state: QueryState) extends ClosingIterator[CypherRow] {
  private var emitSibling = false
  private var lastRelationship: Long = -1L
  private var lastStart: Long = -1L
  private var lastEnd: Long = -1L

  private val query = state.query
  private val relIterator = query.getRelationshipsByType(relToken)

  def next(): CypherRow = {
    val context = state.newRowWithArgument(rowFactory)
    if (emitSibling) {
      emitSibling = false
      context.setLongAt(relOffset, lastRelationship)
      context.setLongAt(fromOffset, lastEnd)
      context.setLongAt(toOffset, lastStart)
    } else {
      emitSibling = true
      lastRelationship = relIterator.next()
      val relationship = query.relationshipById(lastRelationship)
      lastStart = relationship.startNode().id()
      lastEnd = relationship.endNode().id()
      context.setLongAt(relOffset, lastRelationship)
      context.setLongAt(fromOffset, lastStart)
      context.setLongAt(toOffset, lastEnd)
    }
    context
  }

  override protected[this] def closeMore(): Unit = {
    relIterator.close()
  }
  override protected[this] def innerHasNext: Boolean = emitSibling || relIterator.hasNext
}
