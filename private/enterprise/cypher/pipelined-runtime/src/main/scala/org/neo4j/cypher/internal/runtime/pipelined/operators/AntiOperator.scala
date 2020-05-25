/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.AntiArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

class AntiOperator(val workIdentity: WorkIdentity,
                   argumentStateMapId: ArgumentStateMapId,
                   argumentSize: SlotConfiguration.Size)
                  (val id: Id = Id.INVALID_ID)
  extends Operator {

  //===========================================================================
  // Runtime code
  //===========================================================================
  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AntiArgumentState.Factory(id), ordered = true)
    new AntiOperatorState
  }

  class AntiOperatorState() extends DataInputOperatorState[Seq[MorselData]] {
    override def nextTasks(input: Seq[MorselData]): IndexedSeq[ContinuableOperatorTask] = singletonIndexedSeq(new OTask(input))
  }

  class OTask(val morselDatas: Seq[MorselData]) extends AntiOperatorTask {
    private val morselDataIterator: Iterator[MorselData] = morselDatas.iterator

    override def workIdentity: WorkIdentity = AntiOperator.this.workIdentity

    override def operate(outputMorsel: Morsel, state: PipelinedQueryState, resources: QueryResources): Unit = {
      val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
      while (outputCursor.onValidRow() && canContinue) {
        val morselData = morselDataIterator.next()
        morselData.argumentStream match {
          case EndOfEmptyStream =>
            val row = morselData.viewOfArgumentRow
            outputCursor.copyFrom(row, argumentSize.nLongs, argumentSize.nReferences)
            outputCursor.next()
          case EndOfNonEmptyStream =>
            // ignore, nothing to see here
        }
      }
      outputCursor.truncate()
    }

    override protected def closeCursors(resources: QueryResources): Unit = {}

    override def canContinue: Boolean = morselDataIterator.hasNext

    override protected def closeInput(operatorCloser: OperatorCloser): Unit = {
      operatorCloser.closeData(morselDatas)
    }

    override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
      false
    }

    override def producingWorkUnitEvent: WorkUnitEvent = null

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  }
}

trait AntiOperatorTask extends ContinuableOperatorTask {
  def morselDatas: Seq[MorselData]

  override def estimatedHeapUsage: Long = morselDatas.map(morselData => morselData.morsels.map(_.estimatedHeapUsage).sum).sum
}
