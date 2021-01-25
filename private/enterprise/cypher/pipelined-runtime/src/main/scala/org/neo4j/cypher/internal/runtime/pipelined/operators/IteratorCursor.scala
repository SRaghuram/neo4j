/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.values.AnyValue

/**
 * Helper class that turns an iterator into a cursor.
 *
 * @param iterator the iterator over the data
 */
class IteratorCursor(iterator: java.util.Iterator[AnyValue]) {

  private[this] var _value: AnyValue = _

  def next(): Boolean = {
    if (iterator.hasNext) {
      _value = iterator.next()
      true
    } else {
      false
    }
  }

  def value: AnyValue = _value
}

object IteratorCursor {
  def apply(iterator: java.util.Iterator[AnyValue]): IteratorCursor = new IteratorCursor(iterator)
}
