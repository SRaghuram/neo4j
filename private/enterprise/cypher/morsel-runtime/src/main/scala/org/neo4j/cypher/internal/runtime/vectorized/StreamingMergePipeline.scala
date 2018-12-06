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
class StreamingMergePipeline(val start: StreamingMergeOperator[PipelineArgument],
                             override val slots: SlotConfiguration,
                             val lhsUpstream: Pipeline,
                             val rhsUpstream: Pipeline) extends Pipeline {

  override val upstream = Some(lhsUpstream)

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

      // Did we get a new task for the LHS?
      val lhsTasks: Seq[Task[ExpressionCursors]] = maybeTask.map(pipelineTask(_, context, state, PipelineArgument.EMPTY)).toSeq

      // If a PipelineArgument was returned we should start new tasks from the RHS pipeline
      val rhsTasks: Seq[Task[ExpressionCursors]] = maybePipelineArgument.map { pipelineArgument =>
        val argumentRow = createSingleArgumentRow(from = inputMorsel)
        startRhsPipelineTasks(argumentRow, context, state, cursors, pipelineArgument, from)
      }.getOrElse(Seq.empty)

      lhsTasks ++ rhsTasks
    }
  }

  private def startRhsPipelineTasks(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                                    pipelineArgument: PipelineArgument, from: AbstractPipelineTask): Seq[Task[ExpressionCursors]] = {
    val startPipeline = rhsUpstream.getUpstreamLeafPipeline
    val rhsTasks = startPipeline.acceptMorsel(inputMorsel, context, state, cursors, pipelineArgument, from)

    if (rhsTasks.isEmpty) {
      throw new IllegalStateException(s"RHS pipeline $rhsUpstream did not produce a PipelineTask")
    }

    rhsTasks
  }

  // Copy argument into a single row morsel
  private def createSingleArgumentRow(from: MorselExecutionContext): MorselExecutionContext = {
    // TODO: If we move validRows from Morsel to MorselExecutionContext we can do this without creating a new Morsel
    val argumentMorsel = new Morsel(new Array[Long](from.getLongsPerRow), new Array[AnyValue](from.getRefsPerRow))
    val argumentRow = new MorselExecutionContext(argumentMorsel, from.getLongsPerRow, from.getRefsPerRow, 0, 0, from.slots)
    argumentRow.copyFrom(from, start.argumentSize.nLongs, start.argumentSize.nReferences)
    argumentRow
  }

  override val workIdentity: WorkIdentity = composeWorkIdentities(start, operators)
}

