/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.vectorized._
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
abstract class NodeIndexOperatorWithValues[CURSOR <: NodeValueIndexCursor](nodeOffset: Int, maybeValueFromIndexOffset: Option[Int])
  extends StreamingOperator {

  protected def iterate(inputRow: MorselExecutionContext, outputRow: MorselExecutionContext, cursor: CURSOR, argumentSize: SlotConfiguration.Size): Unit = {
    while (outputRow.isValidRow && cursor.next()) {
      outputRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
      outputRow.setLongAt(nodeOffset, cursor.nodeReference())

      maybeValueFromIndexOffset.foreach { valueOffset =>
        if (!cursor.hasValue) {
          // We were promised at plan time that we can get values everywhere, so this should never happen
          throw new IllegalStateException("NodeCursor unexpectedly had no values during index scan.")
        }
        val indexPropertyIndex = 0 // Because we only allow scan / contains with on single prop indexes
        val value = cursor.propertyValue(indexPropertyIndex)
        outputRow.setRefAt(valueOffset, value)
      }

      outputRow.moveToNextRow()
    }
  }
}
