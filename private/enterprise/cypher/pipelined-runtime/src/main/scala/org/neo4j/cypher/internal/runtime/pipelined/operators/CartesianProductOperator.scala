/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.{ArgumentStateMapId, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.CartesianProductOperator.LHSMorsel
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.{ArgumentStateFactory, ArgumentStateMaps, MorselAccumulator}
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.{ArgumentStateBuffer, MorselAttachBuffer}
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
    argumentStateCreator.createArgumentStateMap(lhsArgumentStateMapId, new LHSMorsel.Factory(stateFactory))
    argumentStateCreator.createArgumentStateMap(rhsArgumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory))
    this
  }

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         operatorInput: OperatorInput,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulator[MorselExecutionContext, LHSMorsel]] = {
    val accAndMorsel = operatorInput.takeAccumulatorAndMorsel()
    if (accAndMorsel != null) {
      Array(new OTask(accAndMorsel.acc, accAndMorsel.morsel))
    } else {
      null
    }

  }

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: LHSMorsel, rhsRow: MorselExecutionContext)
    extends InputLoopTask
      with ContinuableOperatorTaskWithMorselAndAccumulator[MorselExecutionContext, LHSMorsel] {

    override def workIdentity: WorkIdentity = CartesianProductOperator.this.workIdentity

    // This is the LHS input. We create a shallow copy because
    // accumulator.lhsMorsel may be accessed in parallel by multiple tasks, with different RHS morsels.
    override val inputMorsel: MorselExecutionContext = accumulator.lhsMorsel.shallowCopy()

    private val lhsSlots = inputMorsel.slots

    override def toString: String = "CartesianProductTask"

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      rhsRow.resetToBeforeFirstRow()
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && rhsRow.hasNextRow) {
        rhsRow.moveToNextRow()
        inputMorsel.copyTo(outputRow) // lhs
        rhsRow.copyTo(outputRow,
                      sourceLongOffset = argumentSize.nLongs, // Skip over arguments since they should be identical to lhsCtx
                      sourceRefOffset = argumentSize.nReferences,
                      targetLongOffset = lhsSlots.numberOfLongs,
                      targetRefOffset = lhsSlots.numberOfReferences)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object CartesianProductOperator {

  class LHSMorsel(override val argumentRowId: Long,
                  val lhsMorsel: MorselExecutionContext,
                  override val argumentRowIdsForReducers: Array[Long])
    extends MorselAccumulator[MorselExecutionContext] {

    override def update(morsel: MorselExecutionContext): Unit =
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
      override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LHSMorsel =
        new LHSMorsel(argumentRowId, argumentMorsel.detach(), argumentRowIdsForReducers)

      override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext, argumentRowIdsForReducers: Array[Long]): LHSMorsel =
        new LHSMorsel(argumentRowId, argumentMorsel.detach(), argumentRowIdsForReducers)

      override def completeOnConstruction: Boolean = true
    }
  }
}
