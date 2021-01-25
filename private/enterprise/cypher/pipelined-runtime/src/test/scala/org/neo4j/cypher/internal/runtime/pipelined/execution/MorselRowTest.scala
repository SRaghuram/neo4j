/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class MorselRowTest extends CypherFunSuite {

  test("should estimate heap usage of views correctly") {
    // GIVEN
    val morselSize = 4
    val slots =
      SlotConfiguration.empty
        .newLong("n", nullable = false, CTNode)
        .newLong("m", nullable = false, CTNode)
        .newReference("x", nullable = true, CTAny)
        .newReference("y", nullable = true, CTAny)

    // WHEN
    val morsel = MorselFactory.allocate(slots, morselSize)
    // set values
    val intVal = Values.intValue(0)
    val cursor = morsel.writeCursor(true)
    for (_ <- 0 until morselSize) {
      cursor.setLongAt(0, 0L)
      cursor.setLongAt(1, 0L)
      cursor.setRefAt(0, intVal)
      cursor.setRefAt(1, intVal)
      cursor.next()
    }

    // THEN
    val expectedSizeOfLong = java.lang.Long.BYTES * 2
    val expectedSizeOfRef = (intVal.estimatedHeapUsage() + org.neo4j.memory.HeapEstimator.OBJECT_REFERENCE_BYTES) * 2
    val expectedSizeOfOneRow = expectedSizeOfLong + expectedSizeOfRef
    morsel.estimatedHeapUsage should be(expectedSizeOfOneRow * morselSize + Morsel.INSTANCE_SIZE)

    // 1 row views
    for (i <- 0 until morselSize) {
      val view = morsel.view(i, i + 1)
      view.estimatedHeapUsage should be(expectedSizeOfOneRow + Morsel.INSTANCE_SIZE)
    }
    // 2 row views
    for (i <- 0 until morselSize by 2) {
      val view = morsel.view(i, i + 2)
      view.estimatedHeapUsage should be(expectedSizeOfOneRow * 2 + Morsel.INSTANCE_SIZE)
    }
    // Cursor
    morsel.fullCursor(true).estimatedHeapUsage() should be(expectedSizeOfOneRow + morsel.Cursor.SHALLOW_SIZE)
  }
}
