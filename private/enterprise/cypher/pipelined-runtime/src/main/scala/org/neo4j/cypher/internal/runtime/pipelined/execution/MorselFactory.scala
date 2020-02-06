/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.values.AnyValue

object MorselFactory {

  def allocate(slots: SlotConfiguration,
               rowsPerMorsel: Int,
               producingWorkUnitEvent: WorkUnitEvent = null
              ): MorselCypherRow = {

    val longs = new Array[Long](slots.numberOfLongs * rowsPerMorsel)
    val refs = new Array[AnyValue](slots.numberOfReferences * rowsPerMorsel)

    new MorselCypherRow(
      longs,
      refs,
      slots,
      rowsPerMorsel,
      0,
      0,
      rowsPerMorsel,
      producingWorkUnitEvent)
  }

  def allocateFiltering(slots: SlotConfiguration,
                        rowsPerMorsel: Int,
                        producingWorkUnitEvent: WorkUnitEvent = null
                       ): MorselCypherRow = {

    val longs = new Array[Long](slots.numberOfLongs * rowsPerMorsel)
    val refs = new Array[AnyValue](slots.numberOfReferences * rowsPerMorsel)

    new FilteringMorselCypherRow(
      longs,
      refs,
      slots,
      rowsPerMorsel,
      0,
      0,
      rowsPerMorsel,
      producingWorkUnitEvent)
  }
}
