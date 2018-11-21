/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.Task

/**
  * A streaming pipeline.
  */
class StreamingPipeline(start: StreamingOperator,
                        override val slots: SlotConfiguration,
                        override val upstream: Option[Pipeline]) extends Pipeline {

  def init(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): PipelineTask = {
    val streamTask = start.init(context, state, inputMorsel, cursors)
    // init next reduce
    val nextState = initDownstreamReduce(state)
    pipelineTask(streamTask, context, nextState)
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] =
    Some(pipelineTask(start.init(context, state, inputMorsel, cursors), context, state))

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"StreamingPipeline(${x.mkString(",")})"
  }
}
