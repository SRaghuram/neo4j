/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.{InputCursor, InputDataStream, QueryContext}
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

class InputOperator(val workIdentity: WorkIdentity,
                    nodeOffsets: Array[Int],
                    refOffsets: Array[Int]) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    if (state.singeThreaded)
      IndexedSeq(new InputTask(state.input, inputMorsel.nextCopy))
    else
      new Array[InputTask](state.numberOfWorkers).map(_ => new InputTask(state.input, inputMorsel.nextCopy))
  }

  /**
    * A [[InputTask]] reserves new batches from the InputStream, until there are no more batches.
    */
  class InputTask(input: InputDataStream, val inputMorsel: MorselExecutionContext) extends ContinuableOperatorTaskWithMorsel {

    private var cursor: InputCursor = _
    private var _canContinue = true

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         queryState: QueryState,
                         resources: QueryResources): Unit = {

      while (outputRow.isValidRow && nextInput()) {
        var i = 0
        while (i < nodeOffsets.length) {
          val value = cursor.value(i)
          outputRow.setLongAt(nodeOffsets(i), if (value == Values.NO_VALUE) NullChecker.NULL_ENTITY else value.asInstanceOf[VirtualNodeValue].id())
          i += 1
        }
        i = 0
        while (i < refOffsets.length) {
          outputRow.setRefAt(refOffsets(i), cursor.value(i))
          i += 1
        }
        outputRow.moveToNextRow()
      }

      outputRow.finishedWriting()
    }

    private def nextInput(): Boolean = {
      while (true) {
        if (cursor == null) {
          cursor = input.nextInputBatch()
          if (cursor == null) {
            // We ran out of work
            _canContinue = false
            return false
          }
        }
        if (cursor.next()) {
          return true
        } else {
          cursor.close()
          cursor = null
        }
      }

      throw new IllegalStateException("Unreachable code")
    }

    override def canContinue: Boolean = _canContinue
  }

}
