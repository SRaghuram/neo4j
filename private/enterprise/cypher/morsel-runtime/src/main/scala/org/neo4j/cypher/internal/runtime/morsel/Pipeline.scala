/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.physical_planning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.scheduling.{Task, WorkIdentity, WorkIdentityImpl, HasWorkIdentity}

import scala.collection.mutable.ArrayBuffer

object Pipeline {
  private[morsel] val DEBUG = false

  /**
    * Debug print method toggled by the DEBUG flag and with output prefixed with the current thread name
    * The argument string is evaluated lazily (unfortunately call-by-name doesn't work with function values)
    */
  //noinspection ScalaUnnecessaryParentheses
  val dprintln: (() => String) => Unit =
    if (DEBUG)
      (s: () => String) => println(s"[${Thread.currentThread().getName}] ${s()}")
    else
      (_: () => String) => {}
}

/**
  * A pipeline of physical operators. Consists of one [[StreamingOperator]] or [[EagerReduceOperator]], called
  * the start operator, and 0-n [[StatelessOperator]]s.
  */
abstract class Pipeline() extends HasWorkIdentity {

  self =>

  // abstract
  def slots: SlotConfiguration
  def upstream: Option[Pipeline]
  def acceptMorsel(inputMorsel: MorselExecutionContext,
                   context: QueryContext,
                   state: QueryState,
                   resources: QueryResources,
                   pipelineArgument: PipelineArgument,
                   from: AbstractPipelineTask): IndexedSeq[Task[QueryResources]]

  protected def composeWorkIdentities(first: HasWorkIdentity, others: Seq[HasWorkIdentity]): WorkIdentity = {
    val workIdentities = (Seq(first) ++ others).map(_.workIdentity)
    val description = s"${workIdentities.map(_.workDescription).mkString(",")}"
    WorkIdentityImpl(workIdentities.head.workId, description)
  }

  override final def toString: String = s"${getClass.getSimpleName}[${workIdentity.workId}](${workIdentity.workDescription})"

  // operators
  protected val operators: ArrayBuffer[StatelessOperator] = new ArrayBuffer[StatelessOperator]
  def addOperator(operator: StatelessOperator): Unit =
    operators += operator

  def hasAdditionalOperators: Boolean = operators.nonEmpty

  // downstream
  var downstream: Option[Pipeline] = None
  var downstreamReduce: Option[ReducePipeline] = None
  def endPipeline: Boolean = downstream.isEmpty

  /**
    * Walks the tree, setting parent information everywhere so we can push up the tree
    */
  def construct: Pipeline = {
    connectPipeline(None, None)
    this
  }

  def connectPipeline(downstream: Option[Pipeline], downstreamReduce: Option[ReducePipeline]): Unit = {
    this.downstream = downstream
    this.downstreamReduce = downstreamReduce
    this.upstream.foreach(_.connectPipeline(Some(this), getThisOrDownstreamReduce(downstreamReduce)))
  }

  def getUpstreamLeafPipeline: AbstractStreamingPipeline = {
    var leafPipeline = this
    while (leafPipeline.upstream.nonEmpty) {
      leafPipeline = leafPipeline.upstream.get
    }

    leafPipeline.asInstanceOf[AbstractStreamingPipeline]
  }

  protected def getThisOrDownstreamReduce(downstreamReduce: Option[ReducePipeline]): Option[ReducePipeline] =
    this match {
      case reducePipeline: ReducePipeline => Some(reducePipeline)
      case _ => downstreamReduce
    }

  protected def initDownstreamReduce(state: QueryState): QueryState = {
    state.copy(reduceCollector = downstreamReduce.map(_.initCollector()))
  }

  protected def produceTaskScheduledForReduceCollector(state: QueryState): Unit =
    state.reduceCollector.foreach(_.produceTaskScheduled())

  protected def pipelineTask(startOperatorTask: ContinuableOperatorTask, context: QueryContext, state: QueryState, pipelineArgument: PipelineArgument): PipelineTask = {
    produceTaskScheduledForReduceCollector(state)
    PipelineTask(startOperatorTask,
                 operators,
                 slots,
                 workIdentity,
                 context,
                 state,
                 pipelineArgument,
                 this,
                 downstream)
  }
}
