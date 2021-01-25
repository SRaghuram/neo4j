/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue

object HashJoinSlottedPipeTestHelper extends CypherFunSuite with SlottedPipeTestHelper {

  abstract class Row {
    val l: Longs
    val r: Refs
  }

  case class RowR(refs: AnyValue*) extends Row {
    val r = Refs(refs: _*)
    val l = Longs()
  }

  case class RowL(longs: Long*) extends Row {
    val r = Refs()
    val l = Longs(longs: _*)
  }

  case class RowRL(l: Longs, r: Refs) extends Row

  case class Longs(l: Long*)

  case class Refs(l: AnyValue*)

  def mockPipeFor(slots: SlotConfiguration, rows: Row*): Pipe = {
    val p = mock[Pipe]
    when(p.createResults(any())).thenAnswer((_: InvocationOnMock) => {
      ClosingIterator(rows.toIterator.map { row =>
        val createdRow = SlottedRow(slots)
        row.l.l.zipWithIndex foreach {
          case (v, idx) => createdRow.setLongAt(idx, v)
        }
        row.r.l.zipWithIndex foreach {
          case (v, idx) => createdRow.setRefAt(idx, v)
        }
        createdRow
      })
    })
    p
  }
}
