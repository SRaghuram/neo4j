/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.OptionalArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

class OptionalOperator(val workIdentity: WorkIdentity,
                       argumentStateMapId: ArgumentStateMapId,
                       argumentSlotOffset: Int,
                       nullableSlots: Seq[Slot],
                       slots: SlotConfiguration,
                       argumentSize: SlotConfiguration.Size)
                      (val id: Id = Id.INVALID_ID)
  extends Operator {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setNullableSlotToNullFunctions: Seq[WritableRow => Unit] =
  nullableSlots.map {
    case LongSlot(offset, _, _) =>
      (context: WritableRow) => context.setLongAt(offset, -1L)
    case RefSlot(offset, _, _) =>
      (context: WritableRow) => context.setRefAt(offset, Values.NO_VALUE)
  }

  //===========================================================================
  // Runtime code
  //===========================================================================
  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new OptionalArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    new OptionalOperatorState
  }

  class OptionalOperatorState() extends OperatorState {
    override def nextTasks(state: PipelinedQueryState,
                           operatorInput: OperatorInput,
                           parallelism: Int,
                           resources: QueryResources,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
      val input: MorselData = operatorInput.takeData()
      if (input != null) {
        IndexedSeq(new OTask(input))
      } else {
        null
      }
    }
  }

  class OTask(val morselData: MorselData) extends OptionalOperatorTask {

    private val morselIterator = morselData.morsels.iterator
    // To remember whether we already processed the ArgumentStream, which can lead to a null row.
    private var consumedArgumentStream = false
    private var currentMorsel: MorselReadCursor = _

    override def workIdentity: WorkIdentity = OptionalOperator.this.workIdentity

    override def operate(outputMorsel: Morsel, state: PipelinedQueryState, resources: QueryResources): Unit = {
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
            outputCursor.copyFrom(currentMorsel)
            outputCursor.next()
            currentMorsel.next()
          }
        } else if (!consumedArgumentStream) {
          morselData.argumentStream match {
            case EndOfEmptyStream(viewOfArgumentRow) =>
              // An argument id did not produce any rows. We need to manufacture a row with arguments + nulls
              // 1) Copy arguments from state
              outputCursor.copyFrom(viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
              // 2) Set nullable slots
              setNullableSlotToNullFunctions.foreach(f => f(outputCursor))
              outputCursor.next()

            case _ =>
            // Do nothing
          }
          consumedArgumentStream = true
        }
      }
      outputCursor.truncate()
    }

    override protected def closeCursors(resources: QueryResources): Unit = {}

    override def canContinue: Boolean =
      (currentMorsel != null && currentMorsel.onValidRow) ||
        morselIterator.hasNext ||
        !consumedArgumentStream

    override protected def closeInput(operatorCloser: OperatorCloser): Unit = {
      operatorCloser.closeData(morselData)
    }

    override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
      false
    }

    override def producingWorkUnitEvent: WorkUnitEvent = null

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  }
}

trait OptionalOperatorTask extends ContinuableOperatorTask {
  def morselData: MorselData

  override def estimatedHeapUsage: Long = morselData.morsels.map(_.estimatedHeapUsage).sum
}
