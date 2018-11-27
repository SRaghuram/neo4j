/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.parallel.{Task, WorkIdentity}
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.values.AnyValue

/**
  * A streaming pipeline merging two upstreams.
  */
class StreamingMergePipeline(val start: StreamingMergeOperator,
                             override val slots: SlotConfiguration,
                             val lhsUpstream: Pipeline,
                             val rhsUpstream: Pipeline) extends Pipeline {

  override val upstream = Some(lhsUpstream)

  def init(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState): PipelineTask = {
    throw new IllegalStateException("We cannot have a join pipeline as a leaf pipeline")
  }

  override def connectPipeline(downstream: Option[Pipeline], downstreamReduce: Option[ReducePipeline]): Unit = {
    this.downstream = downstream
    this.downstreamReduce = downstreamReduce
    this.lhsUpstream.connectPipeline(Some(this), getThisOrDownstreamReduce(downstreamReduce))
    this.rhsUpstream.connectPipeline(Some(this), getThisOrDownstreamReduce(downstreamReduce))
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                            pipelineArgument: PipelineArgument, from: AbstractPipelineTask): Seq[Task[ExpressionCursors]] = {
    if (from.ownerPipeline.eq(rhsUpstream)) {
      val maybeTask = start.initFromRhs(context, state, inputMorsel, cursors, pipelineArgument)
      maybeTask.map(pipelineTask(_, context, state, PipelineArgument.EMPTY)).toSeq
    } else {
      // We got a morsel from the lhs, which is the primary input to the whole join
      val (maybeTask, maybePipelineArgument) = start.initFromLhs(context, state, inputMorsel, cursors)
      val lhsTasks: Seq[Task[ExpressionCursors]] = maybeTask.map(pipelineTask(_, context, state, PipelineArgument.EMPTY)).toSeq
      val rhsTasks: Seq[Task[ExpressionCursors]] = maybePipelineArgument.map { pipelineArgument =>

        // Copy argument into a single row morsel TODO: Cleanup - Extract method
        val argumentMorsel = new Morsel(new Array[Long](inputMorsel.getLongsPerRow), new Array[AnyValue](inputMorsel.getRefsPerRow), 1)
        val argumentRow = new MorselExecutionContext(argumentMorsel, inputMorsel.getLongsPerRow, inputMorsel.getRefsPerRow, 0)
        argumentRow.copyFrom(inputMorsel, start.argumentSize.nLongs, start.argumentSize.nReferences)

        scheduleRhs(argumentRow, context, state, cursors, pipelineArgument, from)
      }.getOrElse(Seq.empty)
      lhsTasks ++ rhsTasks
    }
  }

  private def scheduleRhs(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                          pipelineArgument: PipelineArgument, from: AbstractPipelineTask): Seq[Task[ExpressionCursors]] = {
    val startPipeline = rhsUpstream.getUpstreamLeafPipeline
    val rhsTasks = startPipeline.acceptMorsel(inputMorsel, context, state, cursors, pipelineArgument, from)

    if (rhsTasks.isEmpty) {
      throw new IllegalStateException(s"RHS pipeline $rhsUpstream did not produce a PipelineTask")
    }

    rhsTasks
  }

  override val workIdentity: WorkIdentity = composeWorkIdentities(start, operators)
}

