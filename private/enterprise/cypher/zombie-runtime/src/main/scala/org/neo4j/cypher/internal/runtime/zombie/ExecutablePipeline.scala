/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.{PipelineId, RowBufferDefinition, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.zombie.operators.{Operator, OperatorState, ProduceResultOperator, StatelessOperator}
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}

case class ExecutablePipeline(id: PipelineId,
                              start: Operator,
                              middleOperators: Seq[StatelessOperator],
                              produceResult: Option[ProduceResultOperator],
                              slots: SlotConfiguration,
                              inputRowBuffer: RowBufferDefinition,
                              output: RowBufferDefinition) {

  def createState(argumentStateCreator: ArgumentStateCreator): PipelineState =
    new PipelineState(this, start.createState(argumentStateCreator))

  override def toString: String = {
    val opStrings = (start.toString +: middleOperators.map(_.toString)) ++ produceResult.map(_.toString)
    s"Pipeline(${opStrings.mkString("-")})"
  }
}

class PipelineState(val pipeline: ExecutablePipeline, startState: OperatorState) {

  def init(inputMorsel: MorselExecutionContext,
           context: QueryContext,
           state: QueryState,
           resources: QueryResources): IndexedSeq[PipelineTask] = {

    val streamTasks = startState.init(context, state, inputMorsel, resources)
    val produceResultsTask = pipeline.produceResult.map(_.init(context, state, resources)).orNull
    streamTasks.map(startTask => PipelineTask(startTask, pipeline.middleOperators, produceResultsTask, context, state, pipeline))
  }
}
