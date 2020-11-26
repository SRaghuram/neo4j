/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class DirectedRelationshipTypeScanSlottedPipe(relOffset: Int, fromOffset: Int, typ: LazyType, toOffset: Int)
                                                  (val id: Id = Id.INVALID_ID) extends Pipe {


  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {

    val typeId = typ.getId(state.query)
    if (typeId == LazyType.UNKNOWN) ClosingIterator.empty
    else {
      PrimitiveLongHelper.map(state.query.getRelationshipsByType(typeId), { relId =>
        val context = state.newRowWithArgument(rowFactory)
        val relationship = state.query.relationshipById(relId)
        context.setLongAt(relOffset, relId)
        context.setLongAt(fromOffset, relationship.startNode().id())
        context.setLongAt(toOffset, relationship.endNode().id())
        context
      })
    }
  }
}
