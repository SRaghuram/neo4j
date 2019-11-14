/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.Comparator

import org.neo4j.cypher.internal.physicalplanning.{BufferId, PipelineId}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.execution.{PipelinedExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ColumnOrder

class SortPreOperator(val workIdentity: WorkIdentity,
                      argumentSlotOffset: Int,
                      outputBufferId: BufferId,
                      orderBy: Seq[ColumnOrder]) extends OutputOperator {

  override def toString: String = "SortPre"
  override def outputBuffer: Option[BufferId] = Some(outputBufferId)

  override def createState(executionState: ExecutionState,
                           pipelineId: PipelineId): OutputOperatorState =
    new State(executionState.getSink[IndexedSeq[PerArgument[PipelinedExecutionContext]]](pipelineId, outputBufferId))

  class State(sink: Sink[IndexedSeq[PerArgument[PipelinedExecutionContext]]]) extends OutputOperatorState {

    override def workIdentity: WorkIdentity = SortPreOperator.this.workIdentity

    override def prepareOutput(morsel: PipelinedExecutionContext,
                               context: QueryContext,
                               state: QueryState,
                               resources: QueryResources,
                               operatorExecutionEvent: OperatorProfileEvent): PreSortedOutput = {

      val rowCloneForComparators = morsel.shallowCopy()
      val comparator: Comparator[Integer] = orderBy
        .map(MorselSorting.compareMorselIndexesByColumnOrder(rowCloneForComparators))
        .reduce((a, b) => a.thenComparing(b))

      val preSorted = ArgumentStateMap.map(argumentSlotOffset,
                                           morsel,
                                           morselView => sortInPlace(morselView, comparator))

      new PreSortedOutput(preSorted, sink)
    }

    private def sortInPlace(morsel: PipelinedExecutionContext, comparator: Comparator[Integer]): PipelinedExecutionContext = {
      // First we create an array of the same size as the rows in the morsel that we'll sort.
      // This array contains only the pointers to the morsel rows
      val outputToInputIndexes: Array[Integer] = MorselSorting.createMorselIndexesArray(morsel)

      // We have to sort everything
      java.util.Arrays.sort(outputToInputIndexes, comparator)

      // Now that we have a sorted array, we need to shuffle the morsel rows around until they follow the same order
      // as the sorted array
      MorselSorting.createSortedMorselData(morsel, outputToInputIndexes)
      morsel
    }
  }

  class PreSortedOutput(preSorted: IndexedSeq[PerArgument[PipelinedExecutionContext]],
                        sink: Sink[IndexedSeq[PerArgument[PipelinedExecutionContext]]]) extends PreparedOutput {
    override def produce(): Unit = sink.put(preSorted)
  }
}
