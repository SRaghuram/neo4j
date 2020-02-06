/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselAttachBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.pipelined.operators.CartesianProductOperator.LHSMorsel
import org.neo4j.cypher.internal.util.attribution.Id

class CartesianProductOperator(val workIdentity: WorkIdentity,
                               lhsArgumentStateMapId: ArgumentStateMapId,
                               rhsArgumentStateMapId: ArgumentStateMapId,
                               slots: SlotConfiguration,
                               argumentSize: SlotConfiguration.Size)
                              (val id: Id = Id.INVALID_ID)
  extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(lhsArgumentStateMapId, new LHSMorsel.Factory(stateFactory))
    argumentStateCreator.createArgumentStateMap(rhsArgumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory, id))
    this
  }

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulator[MorselCypherRow, LHSMorsel]] = {
    val accAndMorsel = operatorInput.takeAccumulatorAndMorsel()
    if (accAndMorsel != null) {
      Array(new OTask(accAndMorsel.acc, accAndMorsel.morsel))
    } else {
      null
    }

  }

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: LHSMorsel, rhsRow: MorselCypherRow)
    extends InputLoopTask
    with ContinuableOperatorTaskWithMorselAndAccumulator[MorselCypherRow, LHSMorsel] {

    override def workIdentity: WorkIdentity = CartesianProductOperator.this.workIdentity

    // This is the LHS input. We create a shallow copy because
    // accumulator.lhsMorsel may be accessed in parallel by multiple tasks, with different RHS morsels.
    override val inputMorsel: MorselCypherRow = accumulator.lhsMorsel.shallowCopy()

    private val lhsSlots = inputMorsel.slots

    override def toString: String = "CartesianProductTask"

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: CypherRow): Boolean = {
      rhsRow.resetToBeforeFirstRow()
      true
    }

    override protected def innerLoop(outputRow: MorselCypherRow,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && rhsRow.hasNextRow) {
        rhsRow.moveToNextRow()
        inputMorsel.copyTo(outputRow) // lhs
        rhsRow.copyTo(outputRow, sourceLongOffset = argumentSize.nLongs, sourceRefOffset = argumentSize.nReferences, targetLongOffset = lhsSlots.numberOfLongs, targetRefOffset = lhsSlots.numberOfReferences)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object CartesianProductOperator {

  class LHSMorsel(override val argumentRowId: Long,
                  val lhsMorsel: MorselCypherRow,
                  override val argumentRowIdsForReducers: Array[Long])
    extends MorselAccumulator[MorselCypherRow] {

    override def update(morsel: MorselCypherRow): Unit =
      throw new IllegalStateException("LHSMorsel is complete on construction, and cannot be further updated.")

    override def toString: String = {
      s"LHSMorsel(argumentRowId=$argumentRowId)"
    }
  }

  object LHSMorsel {

    /**
     * This Factory creates [[LHSMorsel]] instances by detaching the morsel which is attached to the argument morsel. It has been attached in
     * the [[MorselAttachBuffer]].
     */
    class Factory(stateFactory: StateFactory) extends ArgumentStateFactory[LHSMorsel] {
      override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): LHSMorsel =
        new LHSMorsel(argumentRowId, argumentMorsel.detach(), argumentRowIdsForReducers)

      override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselCypherRow, argumentRowIdsForReducers: Array[Long]): LHSMorsel =
        new LHSMorsel(argumentRowId, argumentMorsel.detach(), argumentRowIdsForReducers)

      override def completeOnConstruction: Boolean = true
    }
  }
}
