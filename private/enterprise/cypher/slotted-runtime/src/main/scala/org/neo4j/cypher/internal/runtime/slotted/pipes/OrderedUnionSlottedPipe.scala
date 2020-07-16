/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PeekingIterator
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OrderedUnionPipe.OrderedUnionIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.RowMapping
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe.mapRow
import org.neo4j.cypher.internal.util.attribution.Id

import java.util.Comparator

case class OrderedUnionSlottedPipe(lhs: Pipe,
                                   rhs: Pipe,
                                   slots: SlotConfiguration,
                                   lhsMapping: RowMapping,
                                   rhsMapping: RowMapping,
                                   comparator: Comparator[ReadableRow])
                                  (val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val leftRows = new PeekingIterator(lhs.createResults(state).map(mapRow(slots, lhsMapping, _, state)))
    val rightRows = new PeekingIterator(rhs.createResults(state).map(mapRow(slots, rhsMapping, _, state)))
    new OrderedUnionIterator[CypherRow](leftRows, rightRows, comparator)
  }
}
