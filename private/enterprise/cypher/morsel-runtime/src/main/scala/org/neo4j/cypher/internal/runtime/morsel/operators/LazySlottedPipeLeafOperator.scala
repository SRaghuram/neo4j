/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState => SlottedQueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.morsel._

class LazySlottedPipeLeafOperator(val workIdentity: WorkIdentity, val initialSource: Pipe, argumentSize: SlotConfiguration.Size)
  extends LazySlottedPipeStreamingOperator {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, resources: QueryResources): IndexedSeq[ContinuableOperatorTask] = {
    val slottedQueryState: SlottedQueryState = LazySlottedPipeStreamingOperator.createSlottedQueryState(context, state, resources.expressionCursors)
    IndexedSeq(new OTask(inputMorsel, slottedQueryState))
  }

  class OTask(val inputRow: MorselExecutionContext, slottedQueryState: SlottedQueryState) extends StreamingContinuableOperatorTask {
    private var iterator: Iterator[ExecutionContext] = _

    override protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean = {
      iterator = finalPipe.createResults(slottedQueryState.withInitialContext(inputRow))
      true
    }

    override def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.isValidRow && iterator.hasNext) {
        val slottedRow = iterator.next().asInstanceOf[SlottedExecutionContext]
        outputRow.copyFrom(slottedRow, slottedRow.slots.numberOfLongs, slottedRow.slots.numberOfReferences)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }
}
