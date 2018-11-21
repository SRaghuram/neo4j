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

import scala.collection.mutable.ArrayBuffer

object Pipeline {
  private[vectorized] val DEBUG = false
}

/**
  * A pipeline of physical operators. Consists of one [[StreamingOperator]] or [[ReduceOperator]], called
  * the start operator, and 0-n [[StatelessOperator]]s.
  */
abstract class Pipeline() {

  self =>

  // abstract
  def upstream: Option[Pipeline]
  def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]]
  def slots: SlotConfiguration

  // operators
  protected val operators: ArrayBuffer[StatelessOperator] = new ArrayBuffer[StatelessOperator]
  def addOperator(operator: StatelessOperator): Unit =
    operators += operator

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

  protected def connectPipeline(downstream: Option[Pipeline], downstreamReduce: Option[ReducePipeline]): Unit = {
    this.downstream = downstream
    this.downstreamReduce = downstreamReduce
    this.upstream.foreach(_.connectPipeline(Some(this), getThisOrDownstreamReduce(downstreamReduce)))
  }

  private def getThisOrDownstreamReduce(downstreamReduce: Option[ReducePipeline]): Option[ReducePipeline] =
    this match {
      case reducePipeline: ReducePipeline => Some(reducePipeline)
      case _ => downstreamReduce
    }

  protected def initDownstreamReduce(state: QueryState): QueryState = {
    state.copy(reduceCollector = downstreamReduce.map(_.initCollector()))
  }

  protected def pipelineTask(startOperatorTask: ContinuableOperatorTask, context: QueryContext, state: QueryState): PipelineTask = {
    state.reduceCollector.foreach(_.produceTaskScheduled(this.toString))
    PipelineTask(startOperatorTask,
                 operators,
                 slots,
                 this.toString,
                 context,
                 state,
                 downstream)
  }
}
