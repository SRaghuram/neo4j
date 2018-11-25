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
import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

import scala.collection.JavaConverters._

/**
  * A pipeline that eagerly aggregates input morsels into a queue,
  * and after that processes these morsels in a [[ContinuableOperatorTask]].
  *
  * We need to be careful to avoid race conditions when we update task count:
  *
  *  1. Increment of `taskCount` always occurs immediately before creation of a pipeline task (nothing to do with when it is actually executed)
  *  2. Decrement occurs on completion, only after all downstream pipeline tasks have been created (so `taskCount` has been incremented for them too)
  *
  * An example with a plan that contains these pipelines:
  *
  *                  ...
  *                 /
  *                C(reduce pipeline)
  *               /
  *              /
  *             B (regular pipeline)
  *            /
  *           /
  *          A (regular pipeline)
  *
  * Take ‘+P’ to mean increment `taskCount` for the creation of pipeline ‘P’
  * And ‘-P’ to mean decrement `taskCount` for completion of ‘P’
  *
  * The reference counting would work something like this:
  *
  *                      +A                                      +B1                                 -B1           +B2              -A                    -B2
  *                 count=1                                  count=2                            count=1       count=2          count=1               count=0
  *
  *                create(A)  run(A)                       create(B1)   run(B1)
  *                  +-+       +-+                            +--+       +--+
  * Pipeline.init()->|A|-exec->|A|-downstream.acceptMorsel()->|B1|-exec->|B1|-coll.complete(B1)->kill(B1)
  * coll.schedule(A) +-+       +-+  coll.schedule(B1)         +--+       +--+
  *                                      |
  *                                      |
  *                                      |                                            run(A)                 create(B2)   run(B2)
  *                                      |                             +-+             +-+                      +--+       +--+                    +-+       +-+
  *                                      +---------------canContinue-->|A|-------exec->|A|-down.acceptMorsel()->|B2|-exec->|B2|-coll.complete(B2)->|C|-exec->|C|
  *                                                     query.await()  +-+             +-+  coll.scheduled()    +--+       +--+                    +-+       +-+
  *                                                                                     |
  *                                                                                     |
  *                                                                                     |
  *                                                                                     +-------------------coll.complete(A)->kill(A)
  */
class EagerReducePipeline(start: EagerReduceOperator,
                          override val slots: SlotConfiguration,
                          override val upstream: Option[Pipeline]) extends ReducePipeline {

  override def toString: String = {
    val classNames = (start +: operators).map(op => op.getClass.getSimpleName)
    s"EagerReducePipeline(${classNames.mkString(",")})"
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                            from: AbstractPipelineTask): Option[Task[ExpressionCursors]] = {
    state.reduceCollector.get.acceptMorsel(inputMorsel, context, state, cursors, from)
  }

  override def initCollector(): ReduceCollector = new Collector

  class Collector() extends ReduceCollector {

    private val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[MorselExecutionContext]()
    private val taskCount = new AtomicInteger(0)

    override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors,
                             from: AbstractPipelineTask): Option[Task[ExpressionCursors]] = {
      eagerData.add(inputMorsel)
      None
    }

    override def produceTaskScheduled(task: String): Unit = {
      val tasks = taskCount.incrementAndGet()
      if (Pipeline.DEBUG)
        dprintln(() => "taskCount [%3d]: scheduled %s".format(tasks, task))
    }

    override def produceTaskCompleted(task: String, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
      val tasksLeft = taskCount.decrementAndGet()
      if (Pipeline.DEBUG)
        dprintln(() => "taskCount [%3d]: completed %s".format(tasksLeft, task))

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
