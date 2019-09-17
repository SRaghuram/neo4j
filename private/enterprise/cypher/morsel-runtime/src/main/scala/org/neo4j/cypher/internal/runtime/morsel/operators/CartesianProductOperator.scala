/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.morsel.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.StateFactory
import org.neo4j.cypher.internal.runtime.morsel.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}

class CartesianProductOperator(val workIdentity: WorkIdentity,
                               lhsArgumentStateMapId: ArgumentStateMapId,
                               rhsArgumentStateMapId: ArgumentStateMapId,
                               slots: SlotConfiguration,
                               argumentSize: SlotConfiguration.Size) extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(lhsArgumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory))
    argumentStateCreator.createArgumentStateMap(rhsArgumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory))
    this
  }

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulator[MorselExecutionContext, ArgumentStateBuffer]] = {
    val accAndMorsel = operatorInput.takeAccumulatorAndMorsel()
    if (accAndMorsel != null) {
      Array(new OTask(accAndMorsel.acc, accAndMorsel.morsel))
    } else {
      null
    }

  }

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: ArgumentStateBuffer, rhsRow: MorselExecutionContext)
    extends InputLoopTask
      with ContinuableOperatorTaskWithMorselAndAccumulator[MorselExecutionContext, ArgumentStateBuffer] {

    override def workIdentity: WorkIdentity = CartesianProductOperator.this.workIdentity

    override val inputMorsel: MorselExecutionContext = rhsRow

    override def toString: String = "CartesianProductTask"

    private var lhsMorselIterator: java.util.Iterator[MorselExecutionContext] = _
    private var lhsMorsel: MorselExecutionContext = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
     lhsMorselIterator = accumulator.iterator
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && next()) {
        lhsMorsel.copyTo(outputRow)
        rhsRow.copyTo(outputRow,
          sourceLongOffset = argumentSize.nLongs, sourceRefOffset = argumentSize.nReferences, // Skip over arguments since they should be identical to lhsCtx
          targetLongOffset = lhsMorsel.slots.numberOfLongs, targetRefOffset = lhsMorsel.slots.numberOfReferences)
        outputRow.moveToNextRow()
      }
    }

    private def next(): Boolean = {
      while (true) {
        if (lhsMorsel != null && lhsMorsel.hasNextRow) {
          lhsMorsel.moveToNextRow()
          return true
        } else if(lhsMorselIterator.hasNext) {
          lhsMorsel = lhsMorselIterator.next().shallowCopy() // The morsels can be traversed in parallel by many tasks with different RHS morsels
          lhsMorsel.resetToBeforeFirstRow()
        } else {
          return false
        }
      }
      throw new IllegalStateException("Unreachable code")
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}
