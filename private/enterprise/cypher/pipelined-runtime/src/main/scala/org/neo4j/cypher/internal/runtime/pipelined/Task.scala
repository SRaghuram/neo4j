/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.PreparedOutput
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

/**
 * A single task
 */
trait Task[THREAD_LOCAL_RESOURCE] extends WorkIdentity {

  /**
   * Execute the next work-unit of this task. After the first call, [[executeWorkUnit]] will be
   * called again iff [[canContinue]] returns `true`.
   *
   * @param threadLocalResource resources to use for execution
   * @param workUnitEvent the current tracing even
   */
  def executeWorkUnit(threadLocalResource: THREAD_LOCAL_RESOURCE,
                      workUnitEvent: WorkUnitEvent,
                      queryProfiler: QueryProfiler): PreparedOutput

  /**
   * Returns true if there is another work unit to execute.
   *
   * @return true if there is another work unit to execute.
   */
  def canContinue: Boolean

  override def toString: String = s"${getClass.getSimpleName}[$workId]($workDescription)"
}

/**
 * Result from scheduling something
 *
 * @param task                   a task or `null`
 * @param someTaskWasFilteredOut `true` if some task was filtered out, `false` otherwise.
 * @tparam T the type of task
 */
case class SchedulingResult[+T](task: T, someTaskWasFilteredOut: Boolean)
