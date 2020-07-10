/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE

import scala.collection.JavaConverters.asScalaIteratorConverter

case class ValueHashJoinSlottedPipe(leftSide: Expression,
                                    rightSide: Expression,
                                    left: Pipe,
                                    right: Pipe,
                                    slots: SlotConfiguration,
                                    rhsSlotMappings: SlotMappings)
                                   (val id: Id = Id.INVALID_ID)
  extends AbstractHashJoinPipe[AnyValue](left, right) {

  private val rhsLongMappings: Array[(Int, Int)] = rhsSlotMappings.longMappings
  private val rhsRefMappings: Array[(Int, Int)] = rhsSlotMappings.refMappings
  private val rhsCachedPropertyMappings: Array[(Int, Int)] = rhsSlotMappings.cachedPropertyMappings

  override def probeInput(rhsIterator: ClosingIterator[CypherRow],
                          state: QueryState,
                          table: ProbeTable[AnyValue, CypherRow]): ClosingIterator[SlottedRow] = {
    val result = for {
      rhs <- rhsIterator
      joinKey <- computeKey(rhs, rightSide, state).toIterator
      lhs <- table.get(joinKey).asScala
    } yield {
      val newRow = SlottedRow(slots)
      newRow.copyAllFrom(lhs)
      NodeHashJoinSlottedPipe.copyDataFromRow(rhsLongMappings, rhsRefMappings, rhsCachedPropertyMappings, newRow, rhs)
      newRow
    }
    result.closing(table)
  }

  override def buildProbeTable(input: ClosingIterator[CypherRow], queryState: QueryState): collection.ProbeTable[AnyValue, CypherRow] = {
    val table = collection.ProbeTable.createProbeTable[AnyValue, CypherRow](queryState.memoryTracker.memoryTrackerForOperator(id.x))

    for {context <- input
         joinKey <- computeKey(context, leftSide, queryState)} {
      table.put(joinKey, context)
    }

    table
  }

  private def computeKey(context: CypherRow, keyColumns: Expression, queryState: QueryState): Option[AnyValue] = {
    val value = keyColumns.apply(context, queryState)
    if (value eq NO_VALUE) {
      None
    } else {
      Some(value)
    }}
  }
