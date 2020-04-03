/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData

/**
 * Operator task which takes [[MorselData]] and produces zero or more output rows
 * for each input row, and might require several operate calls to be fully executed.
 *
 * It also offers extending classes to handle the end of data per argument row id.
 */
abstract class InputLoopWithMorselDataTask(override final val morselData: MorselData) extends ContinuableOperatorTaskWithMorselData {

  private val morselIterator = morselData.morsels.iterator
  // To remember whether we already processed the ArgumentStream.
  private var consumedArgumentStream = false
  private var currentMorsel: MorselReadCursor = _

  /**
   * Called once at the beginning of processing one input [[MorselData]]
   */
  def initialize(state: PipelinedQueryState, resources: QueryResources): Unit

  /**
   * Called for each row of data
   */
  def processRow(outputCursor: MorselWriteCursor, inputCursor: MorselReadCursor): Unit

  /**
   * Called once (per argument row id) at the end of current [[morselData]]
   */
  def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit

  /**
   * Called after all morsels from [[morselData]] have been processed, but task [[canContinue]]
   */
  def processRemainingOutput(outputCursor: MorselWriteCursor): Unit

  override final def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    initialize(state, resources)
    val outputCursor = outputMorsel.writeCursor(onFirstRow = true)

    while (outputCursor.onValidRow && canContinue) {
      if (currentMorsel == null || !currentMorsel.onValidRow) {
        if (morselIterator.hasNext) {
          // Take the next morsel to process
          currentMorsel = morselIterator.next().readCursor(onFirstRow = true)
        } else {
          // No more morsels to process
          currentMorsel = null
        }
      }

      if (currentMorsel != null) {
        while (outputCursor.onValidRow && currentMorsel.onValidRow) {
          processRow(outputCursor, currentMorsel)
          currentMorsel.next()
        }
      } else {
        if (!consumedArgumentStream) {
          processEndOfMorselData(outputCursor)
          consumedArgumentStream = true
        }
        processRemainingOutput(outputCursor)
      }
    }
    outputCursor.truncate()
  }

  override def canContinue: Boolean =
    (currentMorsel != null && currentMorsel.onValidRow) ||
      morselIterator.hasNext ||
      !consumedArgumentStream

  override final protected def closeCursors(resources: QueryResources): Unit = ()

  override final def setExecutionEvent(event: OperatorProfileEvent): Unit = ()
}
