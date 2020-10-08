/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

class TriadicFilterOperator(val workIdentity: WorkIdentity,
                            bufferAsmId: ArgumentStateMapId,
                            triadicStateAsmId: ArgumentStateMapId,
                            positivePredicate: Boolean,
                            sourceOffset: Int,
                            targetOffset: Int,
                            argumentOffset: Int,
                            id: Id) extends Operator {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(bufferAsmId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), memoryTracker, ordered = true)

    // `triadicStateAsmId` is an ID of a map that should be shared between triadic build and filter operators.
    // We want to make sure that we don't accidentally create a second map, hence `createOrGetArgumentStateMap`.
    val triadicStateAsm = argumentStateCreator.createOrGetArgumentStateMap(triadicStateAsmId, new TriadicState.Factory(memoryTracker), memoryTracker, ordered = true)

    new TriadicFilterOperatorState(positivePredicate, sourceOffset, targetOffset, triadicStateAsm, argumentOffset, workIdentity)
  }
}

class TriadicFilterOperatorState(positivePredicate: Boolean,
                                 sourceOffset: Int,
                                 targetOffset: Int,
                                 triadicStateAsm: ArgumentStateMap[TriadicState],
                                 argumentOffset: Int,
                                 workIdentity: WorkIdentity) extends DataInputOperatorState[MorselData] {

  override def nextTasks(state: PipelinedQueryState, input: MorselData, argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] =
    singletonIndexedSeq(new TriadicFilterTask(input, triadicStateAsm, positivePredicate, sourceOffset, targetOffset, argumentOffset, workIdentity))
}

class TriadicFilterTask(input: MorselData,
                        triadicStateAsm: ArgumentStateMap[TriadicState],
                        positivePredicate: Boolean,
                        sourceOffset: Int,
                        targetOffset: Int,
                        argumentOffset: Int,
                        override val workIdentity: WorkIdentity) extends InputLoopWithMorselDataTask(input) {

  private[this] val argumentRowId: Long = ArgumentSlots.getArgumentAt(morselData.viewOfArgumentRow, argumentOffset)
  private[this] val triadicState: TriadicState = triadicStateAsm.peek(argumentRowId)

  override def processRow(outputCursor: MorselWriteCursor, inputCursor: MorselReadCursor): Unit = {
    val shouldWriteToOutput = {
      val sourceId = inputCursor.getLongAt(sourceOffset)
      val targetId = inputCursor.getLongAt(targetOffset)
      val hasSeenTargetId = triadicState.contains(sourceId, targetId)
      if (positivePredicate) hasSeenTargetId else !hasSeenTargetId
    }
    if (shouldWriteToOutput) {
      outputCursor.copyFrom(inputCursor)
      outputCursor.next()
    }
  }

  override def processEndOfMorselData(outputCursor: MorselWriteCursor): Unit = morselData.argumentStream match {
    case _: EndOfStream =>
      triadicStateAsm.remove(argumentRowId).close()
    case _ => ()
  }

  override def initialize(state: PipelinedQueryState, resources: QueryResources): Unit = ()
  override def processRemainingOutput(outputCursor: MorselWriteCursor): Unit = ()
  override def onNewInputMorsel(inputCursor: MorselReadCursor): Unit = ()
}
