/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExecutionContextFactory
import org.neo4j.values.AnyValue

case class SlottedExecutionContextFactory(slots: SlotConfiguration) extends ExecutionContextFactory {

  override def newExecutionContext(): CypherRow =
    SlottedRow(slots)

  override def copyWith(row: ReadableRow): CypherRow = {
    val newCtx = SlottedRow(slots)
    newCtx.copyAllFrom(row)
    newCtx
  }

  override def copyWith(row: ReadableRow, newEntries: Seq[(String, AnyValue)]): CypherRow = {
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(row)
    for ((key,value) <- newEntries) {
      newCopy.set(key, value)
    }
    newCopy
  }

  override def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow = {
    val newCtx = SlottedRow(slots)
    newCtx.copyAllFrom(row)
    newCtx.set(key, value)
    newCtx
  }

  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = {
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(row)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy
  }

  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): CypherRow = {
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(row)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy.set(key3, value3)
    newCopy
  }
}
