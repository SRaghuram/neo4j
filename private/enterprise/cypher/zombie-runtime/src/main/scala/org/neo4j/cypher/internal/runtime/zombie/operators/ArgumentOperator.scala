/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer

class ArgumentOperator(val workIdentity: WorkIdentity,
                       argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def toString: String = "Argument"

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselParallelizer,
                    resources: QueryResources): IndexedSeq[ContinuableInputOperatorTask] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselExecutionContext) extends ContinuableInputOperatorTask {

    override def toString: String = "ArgumentTask"

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      while (outputRow.isValidRow && inputMorsel.isValidRow) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)

        inputMorsel.moveToNextRow()
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = false
  }
}
