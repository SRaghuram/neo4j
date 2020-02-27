/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe.RowMapping
import org.neo4j.internal.kernel.api.IndexReadSession

class UnionOperator(val workIdentity: WorkIdentity,
                    lhsSlotConfig: SlotConfiguration,
                    rhsSlotConfig: SlotConfiguration,
                    lhsMapping: RowMapping,
                    rhsMapping: RowMapping
                   ) extends StreamingOperator {

  override def toString: String = "Union"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    val morsel = inputMorsel.nextCopy

    val rowMapping =
      if(morsel.slots eq lhsSlotConfig) lhsMapping
      else if (morsel.slots eq rhsSlotConfig) rhsMapping
      else throw new IllegalStateException(s"Unknown slot configuration in UnionOperator. Got: ${morsel.slots}. LHS: $lhsSlotConfig. RHS: $rhsSlotConfig")

    IndexedSeq(new UnionTask(morsel,
      workIdentity,
      rowMapping))
  }

}

class UnionTask(val inputMorsel: Morsel,
                val workIdentity: WorkIdentity,
                rowMapping: RowMapping) extends ContinuableOperatorTaskWithMorsel {

  override def toString: String = "UnionTask"

  override protected def closeCursors(resources: QueryResources): Unit = {}

  override def operate(outputMorsel: Morsel,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {
    val slottedQueryState = new SlottedQueryState(context,
      resources = null,
      params = state.params,
      resources.expressionCursors,
      Array.empty[IndexReadSession],
      resources.expressionVariables(state.nExpressionSlots),
      state.subscriber,
      NoMemoryTracker)

    val inputCursor = inputMorsel.fullCursor(onFirstRow = false)
    val outputCursor = outputMorsel.fullCursor(onFirstRow = true)

    // We write one output row per input row.
    // Since the input morsel can at most have as many rows as the output morsel, we don't need to check `outputCursor.onValidRow()`.
    while(inputCursor.next()) {
      rowMapping.mapRows(inputCursor, outputCursor, slottedQueryState)
      outputCursor.next()
    }
    outputCursor.truncate()
  }

  override def canContinue: Boolean = false

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}
