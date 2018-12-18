/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent._
import java.util.function.Supplier

import org.neo4j.cypher.internal.runtime.vectorized.Pipeline.dprintln

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
  * A simple implementation of the Scheduler trait
  */
class SimpleScheduler[THREAD_LOCAL_RESOURCE <: AutoCloseable](executorService: ExecutorService,
                                                              waitTimeout: Duration,
                                                              threadLocalResourceFactory: () => THREAD_LOCAL_RESOURCE,
                                                              override val numberOfWorkers: Int
                                                             ) extends Scheduler[THREAD_LOCAL_RESOURCE] {

  private val threadLocalResource = ThreadLocal.withInitial(new Supplier[THREAD_LOCAL_RESOURCE] {
    override def get(): THREAD_LOCAL_RESOURCE = threadLocalResourceFactory()
  })

  override def execute(tracer: SchedulerTracer, tasks: IndexedSeq[Task[THREAD_LOCAL_RESOURCE]]): QueryExecution = {
    dprintln(() => s"SimpleScheduler execute $tasks")
    val queryTracer: QueryExecutionTracer = tracer.traceQuery()
    new SimpleQueryExecution(tasks.map(schedule(_, None, queryTracer)), queryTracer, waitTimeout.toMillis)
  }

  override def isMultiThreaded: Boolean = true

  def schedule(task: Task[THREAD_LOCAL_RESOURCE], upstreamWorkUnit: Option[WorkUnitEvent], queryTracer: QueryExecutionTracer): Future[TaskResult] = {
    dprintln(() => s"SimpleScheduler schedule $task")
    val scheduledWorkUnitEvent = queryTracer.scheduleWorkUnit(task, upstreamWorkUnit)
    val callableTask =
      new Callable[TaskResult] {
        override def call(): TaskResult = {
          dprintln(() => s"SimpleScheduler running $task")
          val workUnitEvent = scheduledWorkUnitEvent.start()
          try {
            val newDownstreamTasks = task.executeWorkUnit(threadLocalResource.get())
            TaskResult(task, workUnitEvent, newDownstreamTasks)
          } finally {
            workUnitEvent.stop()
          }
        }
      }

    executorService.submit(callableTask)
  }

  class SimpleQueryExecution(initialTasks: IndexedSeq[Future[TaskResult]],
                             queryTracer: QueryExecutionTracer,
                             waitTimeoutMilli: Long) extends QueryExecution {

    var inFlightTasks = new ArrayBuffer[Future[TaskResult]]
    inFlightTasks ++= initialTasks

    override def await(): Option[Throwable] = {
      while (inFlightTasks.nonEmpty) {
        val newInFlightTasks = new ArrayBuffer[Future[TaskResult]]
        for (future <- inFlightTasks) {
          try {
            val taskResult = future.get(waitTimeoutMilli, TimeUnit.MILLISECONDS)
            for (newTask <- taskResult.newDownstreamTasks)
              newInFlightTasks += SimpleScheduler.this.schedule(newTask, Some(taskResult.workUnitEvent), queryTracer)

            if (taskResult.task.canContinue)
              newInFlightTasks += SimpleScheduler.this.schedule(taskResult.task, Some(taskResult.workUnitEvent), queryTracer)
          } catch {
            case e: TimeoutException =>
              // got tired of waiting for future to complete, put it back into the queue
              newInFlightTasks += future
            case e: ExecutionException =>
              queryTracer.stopQuery()
              return Some(e.getCause)
            case e: Throwable =>
              queryTracer.stopQuery()
              return Some(e)
          }
        }
        inFlightTasks = newInFlightTasks
      }
      queryTracer.stopQuery()
      None
    }
  }

  case class TaskResult(task: Task[THREAD_LOCAL_RESOURCE],
                        workUnitEvent: WorkUnitEvent,
                        newDownstreamTasks: Seq[Task[THREAD_LOCAL_RESOURCE]])

}
