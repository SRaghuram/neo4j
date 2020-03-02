/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.OptionalArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

class AntiOperator(val workIdentity: WorkIdentity,
                   argumentStateMapId: ArgumentStateMapId,
                   argumentSlotOffset: Int,
                   slots: SlotConfiguration,
                   argumentSize: SlotConfiguration.Size)
                  (val id: Id = Id.INVALID_ID)
  extends Operator {

  //===========================================================================
  // Runtime code
  //===========================================================================
  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new OptionalArgumentStateBuffer.Factory(stateFactory, id))
    new AntiOperatorState
  }

  class AntiOperatorState() extends OperatorState {
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

  class OTask(val morselData: MorselData) extends ContinuableOperatorTask {
    private var hasWritten = false

    override def workIdentity: WorkIdentity = AntiOperator.this.workIdentity

    override def operate(outputMorsel: Morsel, context: QueryContext, state: QueryState, resources: QueryResources): Unit = {
      val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
      if (outputCursor.onValidRow()) {
        hasWritten = true
        morselData.argumentStream match {
          case EndOfEmptyStream(viewOfArgumentRow) =>
            // An argument id did not produce any rows. We need to manufacture a row with arguments
            // 1) Copy arguments from state
            outputCursor.copyFrom(viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
            outputCursor.next()
          case EndOfNonEmptyStream =>
            // Do nothing
        }
      }
      outputCursor.truncate()
    }

    override protected def closeCursors(resources: QueryResources): Unit = {}

    override def canContinue: Boolean = !hasWritten

    override protected def closeInput(operatorCloser: OperatorCloser): Unit = {
      operatorCloser.closeData(morselData)
    }

    override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
      false
    }

    override def producingWorkUnitEvent: WorkUnitEvent = null

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override def estimatedHeapUsage: Long = morselData.morsels.map(_.estimatedHeapUsage).sum
  }
}
