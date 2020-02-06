/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE

case class ValueHashJoinSlottedPipe(leftSide: Expression,
                                    rightSide: Expression,
                                    left: Pipe,
                                    right: Pipe,
                                    slots: SlotConfiguration,
                                    longOffset: Int,
                                    refsOffset: Int,
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID)
  extends AbstractHashJoinPipe[AnyValue, Expression](left, right, slots) {

  leftSide.registerOwningPipe(this)
  rightSide.registerOwningPipe(this)

  override def computeKey(context: CypherRow, keyColumns: Expression, queryState: QueryState): Option[AnyValue] = {
    val value = keyColumns.apply(context, queryState)
    if (value eq NO_VALUE)
      None
    else
      Some(value)
  }

  override def copyDataFromRhs(newRow: SlottedRow, rhs: CypherRow): Unit =
    rhs.copyTo(newRow, sourceLongOffset = argumentSize.nLongs, sourceRefOffset = argumentSize.nReferences, targetLongOffset = longOffset, targetRefOffset = refsOffset)
}
