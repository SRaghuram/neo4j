/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.internal.kernel.api.{NodeIndexCursor, NodeValueIndexCursor}

abstract class NodeIndexOperator[CURSOR <: NodeIndexCursor](nodeOffset: Int) extends StreamingOperator {

  protected def iterate(inputRow: MorselExecutionContext, outputRow: MorselExecutionContext, cursor: CURSOR, argumentSize: SlotConfiguration.Size): Unit = {
    while (outputRow.isValidRow && cursor.next()) {
      outputRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(nodeOffset, cursor.nodeReference())
      outputRow.moveToNextRow()
    }
  }
}

/**
  * For index operators that get nodes together with actual property values.
  */
abstract class NodeIndexOperatorWithValues[CURSOR <: NodeValueIndexCursor](nodeOffset: Int, properties: Array[SlottedIndexedProperty])
  extends StreamingOperator {

  protected val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)

  protected def iterate(inputRow: MorselExecutionContext, outputRow: MorselExecutionContext, cursor: CURSOR, argumentSize: SlotConfiguration.Size): Unit = {
    while (outputRow.isValidRow && cursor.next()) {
      outputRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(nodeOffset, cursor.nodeReference())

      var i = 0
      while (i < indexPropertyIndices.length) {
        outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), cursor.propertyValue(indexPropertyIndices(i)))
        i += 1
      }

      outputRow.moveToNextRow()
    }
  }
}
