/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{Dependency, Rows, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.zombie.operators.{ProduceResultOperator, StreamingOperator}
import org.neo4j.cypher.internal.runtime.morsel.{PipelineTask => _, _}

case class ExecutablePipeline(id: Int,
                              start: StreamingOperator,
                              produceResult: Option[ProduceResultOperator],
                              slots: SlotConfiguration,
                              lhsRows: Rows,
                              output: Dependency) {

  def init(inputMorsel: MorselExecutionContext,
           context: QueryContext,
           state: QueryState,
           resources: QueryResources): IndexedSeq[PipelineTask] = {

    val streamTasks = start.init(context, state, inputMorsel, resources)
    val produceResultsTask = produceResult.map(_.init(context, state, resources)).orNull
    streamTasks.map(startTask => PipelineTask(startTask, produceResultsTask, slots, context, state, this))
  }
}
