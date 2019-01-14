/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}

class ArgumentOperator(val workIdentity: WorkIdentity,
                       argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): IndexedSeq[ContinuableOperatorTask] = IndexedSeq(new OTask(inputMorsel))

  class OTask(argument: MorselExecutionContext) extends ContinuableOperatorTask {
    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      outputRow.copyFrom(argument, argumentSize.nLongs, argumentSize.nReferences)

      outputRow.moveToNextRow()
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = false
  }
}
