/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

  override def connectPipeline(downstream: Option[Pipeline], downstreamReduce: Option[ReducePipeline]): Unit = {
    this.downstream = downstream
    this.downstreamReduce = downstreamReduce
    this.lhsUpstream.connectPipeline(Some(this), getThisOrDownstreamReduce(downstreamReduce))
    this.rhsUpstream.connectPipeline(Some(this), getThisOrDownstreamReduce(downstreamReduce))
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext,
                            context: QueryContext,
                            state: QueryState,
                            resources: QueryResources,
                            pipelineArgument: PipelineArgument,
                            from: AbstractPipelineTask): IndexedSeq[Task[QueryResources]] = {
    if (from.ownerPipeline.eq(rhsUpstream)) {
      val maybeTask = start.initFromRhs(context, state, inputMorsel, resources, pipelineArgument)
      maybeTask.map(pipelineTask(_, context, state, pipelineArgument.tail)).toIndexedSeq
    } else {
      // We got a morsel from the lhs, which is the primary input to the whole join
      val (maybeTask, maybePipelineArgument) = start.initFromLhs(context, state, inputMorsel, resources)

      // Did we get a new task for the LHS?
      val lhsTasks: IndexedSeq[Task[QueryResources]] = maybeTask.map(pipelineTask(_, context, state, pipelineArgument)).toIndexedSeq

      // If a PipelineArgument was returned we should start new tasks from the RHS pipeline
      val rhsTasks: Seq[Task[QueryResources]] = maybePipelineArgument.map { innerPipelineArgument =>
        val argumentRow = createSingleArgumentRow(from = inputMorsel)
        startRhsPipelineTasks(argumentRow, context, state, resources, innerPipelineArgument :: pipelineArgument, from)
      }.getOrElse(Seq.empty)

      lhsTasks ++ rhsTasks
    }
  }

  private def startRhsPipelineTasks(inputMorsel: MorselExecutionContext,
                                    context: QueryContext,
                                    state: QueryState,
                                    resources: QueryResources,
                                    pipelineArgument: PipelineArgument,
                                    from: AbstractPipelineTask): Seq[Task[QueryResources]] = {
    val startPipeline = rhsUpstream.getUpstreamLeafPipeline
    val rhsTasks = startPipeline.acceptMorsel(inputMorsel, context, state, resources, pipelineArgument, from)

    if (rhsTasks.isEmpty) {
      throw new IllegalStateException(s"RHS pipeline $rhsUpstream did not produce a PipelineTask")
    }

    rhsTasks
  }

  // Create a view of only the current row in the input morsel to use as argument
  private def createSingleArgumentRow(from: MorselExecutionContext): MorselExecutionContext = {
    val argumentRow = from.createViewOfCurrentRow()
    argumentRow
  }

  override val workIdentity: WorkIdentity = composeWorkIdentities(start, operators)
}

