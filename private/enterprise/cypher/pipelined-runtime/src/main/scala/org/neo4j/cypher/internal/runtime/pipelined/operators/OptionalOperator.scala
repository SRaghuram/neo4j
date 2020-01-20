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
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.OptionalArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.values.storable.Values

class OptionalOperator(val workIdentity: WorkIdentity,
                       argumentStateMapId: ArgumentStateMapId,
                       argumentSlotOffset: Int,
                       nullableSlots: Seq[Slot],
                       slots: SlotConfiguration,
                       argumentSize: SlotConfiguration.Size)
  extends Operator {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setNullableSlotToNullFunctions: Seq[ExecutionContext => Unit] =
  nullableSlots.map {
    case LongSlot(offset, _, _) =>
      (context: ExecutionContext) => context.setLongAt(offset, -1L)
    case RefSlot(offset, _, _) =>
      (context: ExecutionContext) => context.setRefAt(offset, Values.NO_VALUE)
  }

  //===========================================================================
  // Runtime code
  //===========================================================================
  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new OptionalArgumentStateBuffer.Factory(stateFactory))
    new OptionalOperatorState
  }

  class OptionalOperatorState() extends OperatorState {
    override def nextTasks(context: QueryContext,
                           state: QueryState,
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
    private var currentMorsel: MorselExecutionContext = _

    override def workIdentity: WorkIdentity = OptionalOperator.this.workIdentity

    override def operate(output: MorselExecutionContext, context: QueryContext, state: QueryState, resources: QueryResources): Unit = {
      while (output.isValidRow && canContinue) {
        if (currentMorsel == null || !currentMorsel.isValidRow) {
          if (morselIterator.hasNext) {
            // Take the next morsel to process
            currentMorsel = morselIterator.next()
          } else {
            // No more morsels to process
            currentMorsel = null
          }
        }

        if (currentMorsel != null) {
          while (output.isValidRow && currentMorsel.isValidRow) {
            output.copyFrom(currentMorsel)
            output.moveToNextRow()
            currentMorsel.moveToNextRow()
          }
        } else if (!consumedArgumentStream) {
          morselData.argumentStream match {
            case EndOfEmptyStream(viewOfArgumentRow) =>
              // An argument id did not produce any rows. We need to manufacture a row with arguments + nulls
              // 1) Copy arguments from state
              output.copyFrom(viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
              // 2) Set nullable slots
              setNullableSlotToNullFunctions.foreach(f => f(output))
              output.moveToNextRow()

            case _ =>
            // Do nothing
          }
          consumedArgumentStream = true
        }
      }
      output.finishedWriting()
    }

    override protected def closeCursors(resources: QueryResources): Unit = {}

    override def canContinue: Boolean =
      (currentMorsel != null && currentMorsel.isValidRow) ||
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
