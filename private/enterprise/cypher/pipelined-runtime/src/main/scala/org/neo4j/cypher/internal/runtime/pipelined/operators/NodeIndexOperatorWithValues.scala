/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor

/**
 * For index operators that get nodes together with actual property values.
 */
abstract class NodeIndexOperatorWithValues[CURSOR <: NodeValueIndexCursor](nodeOffset: Int, properties: Array[SlottedIndexedProperty])
  extends StreamingOperator {

  protected val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)

  protected def iterate(inputCursor: MorselReadCursor, outputCursor: MorselWriteCursor, cursor: CURSOR, argumentSize: SlotConfiguration.Size): Unit = {
    while (outputCursor.onValidRow && cursor.next()) {
      outputCursor.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      outputCursor.setLongAt(nodeOffset, cursor.nodeReference())

      var i = 0
      while (i < indexPropertyIndices.length) {
        outputCursor.setCachedPropertyAt(indexPropertySlotOffsets(i), cursor.propertyValue(indexPropertyIndices(i)))
        i += 1
      }

      outputCursor.next()
    }
  }
}
