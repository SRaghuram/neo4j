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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import scala.collection.JavaConverters.asScalaIteratorConverter


case class ValueHashJoinSlottedPipe(leftSide: Expression,
                                    rightSide: Expression,
                                    left: Pipe,
                                    right: Pipe,
                                    slots: SlotConfiguration,
                                    longsToCopy: Array[(Int, Int)],
                                    refsToCopy: Array[(Int, Int)],
                                    cachedPropertiesToCopy: Array[(Int, Int)])
                                   (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(left) {
  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {

    if (!input.hasNext) {
      return Iterator.empty
    }
    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty) {
      return Iterator.empty
    }
    val table = buildProbeTable(input, state)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty) {
      return Iterator.empty
    }
    val result = for {rhs: CypherRow <- rhsIterator
                      joinKey <- computeKey(rhs, rightSide, state)}
      yield {
        val matchesFromLhs = table.get(joinKey)

        matchesFromLhs.asScala.map { lhs =>
          val newRow = SlottedRow(slots)
          lhs.copyTo(newRow)
          NodeHashJoinSlottedPipe.copyDataFromRhs(longsToCopy, refsToCopy, cachedPropertiesToCopy, newRow, rhs)
          newRow
        }
      }

    result.flatten
  }

  private def buildProbeTable(input: Iterator[CypherRow], queryState: QueryState): collection.ProbeTable[AnyValue, CypherRow] = {
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
