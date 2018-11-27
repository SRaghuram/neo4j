/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ListSupport}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

import scala.annotation.tailrec
import scala.collection.JavaConverters._

case class UnwindSlottedPipe(source: Pipe,
                             collection: Expression,
                             offset: Int,
                             slots: SlotConfiguration)
                            (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with ListSupport {
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new UnwindIterator(input, state)

  private class UnwindIterator(input: Iterator[ExecutionContext], state: QueryState) extends Iterator[ExecutionContext] {
    private var currentInputRow: ExecutionContext = _
    private var unwindIterator: Iterator[AnyValue] = _
    private var nextItem: SlottedExecutionContext = _

    prefetch()

    override def hasNext: Boolean = nextItem != null

    override def next(): ExecutionContext =
      if (hasNext) {
        val ret = nextItem
        prefetch()
        ret
      } else {
        Iterator.empty.next() // Fail nicely
      }

    @tailrec
    private def prefetch() {
      nextItem = null
      if (unwindIterator != null && unwindIterator.hasNext) {
        nextItem = SlottedExecutionContext(slots)
        currentInputRow.copyTo(nextItem)
        nextItem.setRefAt(offset, unwindIterator.next())
      } else {
        if (input.hasNext) {
          currentInputRow = input.next()
          val value: AnyValue = collection(currentInputRow, state)
          unwindIterator = makeTraversable(value).iterator.asScala
          prefetch()
        }
      }
    }
  }
}
