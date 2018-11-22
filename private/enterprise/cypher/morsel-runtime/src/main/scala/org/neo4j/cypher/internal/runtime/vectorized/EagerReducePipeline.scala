/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.Task

import scala.collection.JavaConverters._

/**
  * A pipeline that eagerly aggregates input morsels into a queue,
  * and after that processes these morsels in a [[ContinuableOperatorTask]].
  */
class EagerReducePipeline(start: ReduceOperator,
                          override val slots: SlotConfiguration,
                          override val upstream: Option[Pipeline]) extends ReducePipeline {

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"EagerReducePipeline(${x.mkString(",")})"
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
    state.reduceCollector.get.acceptMorsel(inputMorsel, context, state, cursors)
  }

  override def initCollector(): ReduceCollector = new Collector

  class Collector() extends ReduceCollector {

    private val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[MorselExecutionContext]()
    private val taskCount = new AtomicInteger(0)

    override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
      eagerData.add(inputMorsel)
      None
    }

    override def produceTaskScheduled(task: String): Unit = {
      val tasks = taskCount.incrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: scheduled %s".format(tasks, task))
    }

    override def produceTaskCompleted(task: String, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
      val tasksLeft = taskCount.decrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: completed %s".format(tasksLeft, task))

      if (tasksLeft == 0) {
        val inputMorsels: Array[MorselExecutionContext] = eagerData.asScala.toArray
        val reduceTask = start.init(context, state, inputMorsels, cursors)
        // init next reduce
        val nextState = initDownstreamReduce(state)
        Some(pipelineTask(reduceTask, context, nextState))
      } else if (tasksLeft < 0) {
        throw new IllegalStateException("Reference counting of tasks has failed: now at task count " + tasksLeft)
      } else {
        None
      }
    }
  }
}
