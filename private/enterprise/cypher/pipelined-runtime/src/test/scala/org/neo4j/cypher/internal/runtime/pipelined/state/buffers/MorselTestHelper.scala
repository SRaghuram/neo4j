/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.pipelined.execution.FilteringMorsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.util.symbols

trait MorselTestHelper {
  def longMorsel(longsPerRow: Int)(values: Long*): Morsel = {
    val nRows = values.size / longsPerRow
    val slots =
      (0 until longsPerRow)
        .foldLeft(SlotConfiguration.empty)( (slots, i) => slots.newLong(s"v$i", nullable = false, symbols.CTAny) )

    new FilteringMorsel(values.toArray, Array.empty, slots, nRows, 0, nRows)
  }
}
