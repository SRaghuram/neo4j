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
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
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
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    new OptionalOperatorState
  }

  class OptionalOperatorState() extends DataInputOperatorState[MorselData] {
    override def nextTasks(state: PipelinedQueryState,
                           input: MorselData,
                           argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] =
      singletonIndexedSeq(new OTask(input))
  }

  class OTask(morselData: MorselData) extends InputLoopWithMorselDataTask(morselData) {

    override def workIdentity: WorkIdentity = OptionalOperator.this.workIdentity

    override def initialize(state: PipelinedQueryState,
                            resources: QueryResources): Unit = ()

    override def processRow(outputCursor: MorselWriteCursor,
                            inputCursor: MorselReadCursor): Unit = {
      outputCursor.copyFrom(inputCursor)
      outputCursor.next()
    }

    override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit = {
      morselData.argumentStream match {
        case EndOfEmptyStream =>
          // An argument id did not produce any rows. We need to manufacture a row with arguments + nulls
          // 1) Copy arguments from state
          outputCursor.copyFrom(morselData.viewOfArgumentRow, argumentSize.nLongs, argumentSize.nReferences)
          // 2) Set nullable slots
          setNullableSlotToNullFunctions.foreach(f => f(outputCursor))
          outputCursor.next()

        case _ =>
        // Do nothing
      }
    }

    override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit = ()

    override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = ()
  }
}
