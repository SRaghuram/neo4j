/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.parallel.Task

import scala.collection.JavaConverters._
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
  def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Seq[Task[ExpressionCursors]]
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

  def initTask(startOperatorTask: ContinuableOperatorTask, context: QueryContext, state: QueryState): PipelineTask = {
    val stateWithReduceCollector = state.copy(reduceCollector = downstreamReduce.map(_.init()))
    pipelineTask(startOperatorTask, context, stateWithReduceCollector)
  }

  def pipelineTask(startOperatorTask: ContinuableOperatorTask, context: QueryContext, state: QueryState): PipelineTask = {
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

/**
  * The [[Task]] of executing a [[Pipeline]] once.
  *
  * @param start task for executing the start operator
  * @param operators the subsequent [[OperatorTask]]s
  * @param slots the slotConfiguration of this Pipeline
  * @param name name of this task
  * @param originalQueryContext the query context
  * @param state the current QueryState
  * @param downstream the downstream Pipeline
  */
case class PipelineTask(start: ContinuableOperatorTask,
                        operators: IndexedSeq[OperatorTask],
                        slots: SlotConfiguration,
                        name: String,
                        originalQueryContext: QueryContext,
                        state: QueryState,
                        downstream: Option[Pipeline]) extends Task[ExpressionCursors] {

  override def executeWorkUnit(cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {
    val outputMorsel = Morsel.create(slots, state.morselSize)
    val currentRow = new MorselExecutionContext(outputMorsel, slots.numberOfLongs, slots.numberOfReferences, 0)
    val queryContext =
      if (state.singeThreaded) originalQueryContext
      else originalQueryContext.createNewQueryContext()
    start.operate(currentRow, queryContext, state, cursors)

    for (op <- operators) {
      currentRow.resetToFirstRow()
      op.operate(currentRow, queryContext, state, cursors)
    }

    if (org.neo4j.cypher.internal.runtime.vectorized.Pipeline.DEBUG) {
      println(s"Pipeline: $name")

      val longCount = slots.numberOfLongs
      val refCount = slots.numberOfReferences

      println("Resulting rows")
      for (i <- 0 until outputMorsel.validRows) {
        val ls =  util.Arrays.toString(outputMorsel.longs.slice(i * longCount, (i + 1) * longCount))
        val rs =  util.Arrays.toString(outputMorsel.refs.slice(i * refCount, (i + 1) * refCount).asInstanceOf[Array[AnyRef]])
        println(s"$ls $rs")
      }
      println(s"can continue: ${start.canContinue}")
      println()
      println("-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/")
    }

    currentRow.resetToFirstRow()
    val downstreamTasks = downstream.map(_.acceptMorsel(currentRow, queryContext, state, cursors)).getOrElse(Nil)

    state.reduceCollector match {
      case Some(x) if !start.canContinue =>
        downstreamTasks ++ x.produceTaskCompleted(name, queryContext, state, cursors)

      case _ =>
        downstreamTasks
    }
  }

  override def canContinue: Boolean = start.canContinue

  override def toString: String = name
}

/**
  * A streaming pipeline.
  */
class StreamingPipeline(start: StreamingOperator,
                        override val slots: SlotConfiguration,
                        override val upstream: Option[Pipeline]) extends Pipeline {

  def init(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): PipelineTask = {
    initTask(start.init(context, state, inputMorsel, cursors), context, state)
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] =
    List(pipelineTask(start.init(context, state, inputMorsel, cursors), context, state))

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"StreamingPipeline(${x.mkString(",")})"
  }
}

/**
  * A reduce pipeline.
  */
class ReducePipeline(start: ReduceOperator,
                     override val slots: SlotConfiguration,
                     override val upstream: Option[Pipeline]) extends Pipeline {

  override def toString: String = {
    val x = (start +: operators).map(x => x.getClass.getSimpleName)
    s"ReducePipeline(${x.mkString(",")})"
  }

  override def acceptMorsel(inputMorsel: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Seq[Task[ExpressionCursors]] = {

    state.reduceCollector.get.acceptMorsel(inputMorsel)
    Nil
  }

  def init() = new Collector

  class Collector() extends ReduceCollector {

    private val eagerData = new java.util.concurrent.ConcurrentLinkedQueue[MorselExecutionContext]()
    private val taskCount = new AtomicInteger(0)

    def acceptMorsel(inputMorsel: MorselExecutionContext): Unit = {
      eagerData.add(inputMorsel)
    }

    def produceTaskScheduled(task: String): Unit = {
      val tasks = taskCount.incrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: scheduled %s".format(tasks, task))
    }

    def produceTaskCompleted(task: String, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Option[Task[ExpressionCursors]] = {
      val tasksLeft = taskCount.decrementAndGet()
      if (Pipeline.DEBUG)
        println("taskCount [%3d]: completed %s".format(tasksLeft, task))

      if (tasksLeft == 0) {
        val inputMorsels: Array[MorselExecutionContext] = eagerData.asScala.toArray
        Some(initTask(start.init(context, state, inputMorsels, cursors), context, state))
      }
      else if (tasksLeft < 0) {
        throw new IllegalStateException("Reference counting of tasks has failed: now at task count " + tasksLeft)
      }
      else
        None
    }
  }
}
