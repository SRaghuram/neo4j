/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._

class ArgumentOperator(argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext): ContinuableOperatorTask = new OTask(inputMorsel)

  class OTask(argument: MorselExecutionContext) extends ContinuableOperatorTask {
    override def operate(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      outputRow.copyFrom(argument, argumentSize.nLongs, argumentSize.nReferences)

      outputRow.moveToNextRow()
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = false
  }
}
