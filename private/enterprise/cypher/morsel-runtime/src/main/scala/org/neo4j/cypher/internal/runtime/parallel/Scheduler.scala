/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

/**
  * A Spatula (scheduler)
  */
trait Scheduler {

  /**
    * Execute the provided task by calling [[Task#executeWorkUnit()]] repeatedly until
    * [[Task#canContinue]] is false. Any tasks returned by [[Task#executeWorkUnit()]] will
    * also be executed in the same fashion. Each call to executeWorkUnit() may happen on
    * a separate thread at the Scheduler convenience.
    *
    * @param task the initial task to execute
    * @return QueryExecution representing the ongoing execution
    */
  def execute(task: Task, tracer: SchedulerTracer): QueryExecution

  def isMultiThreaded: Boolean
}

/**
  * A single task
  */
trait Task {

  /**
    * Execute the next work-unit of this task. After the first call, [[executeWorkUnit]] will be
    * called again iff [[canContinue]] returns `true`.
    *
    * @return A collection of additional tasks that should also be executed. Can be empty.
    */
  def executeWorkUnit(): Seq[Task]

  /**
    * Returns true if there is another work unit to execute.
    *
    * @return true if there is another work unit to execute.
    */
  def canContinue: Boolean
}

/**
  * A query execution represents the ongoing execution of tasks initiated by a call to [[Scheduler#execute]].
  */
trait QueryExecution {
  /**
    * Wait for this QueryExecution to complete.
    *
    * @return An optional error if anything when wrong with the query execution.
    */
  def await(): Option[Throwable]
}
