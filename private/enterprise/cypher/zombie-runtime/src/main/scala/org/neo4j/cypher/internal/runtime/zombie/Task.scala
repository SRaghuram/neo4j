/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.morsel.{Morsel, MorselExecutionContext}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

/**
  * A single task
  */
trait Task[THREAD_LOCAL_RESOURCE <: AutoCloseable] extends WorkIdentity {

  /**
    * Execute the next work-unit of this task. After the first call, [[executeWorkUnit]] will be
    * called again iff [[canContinue]] returns `true`.
    *
    * @param threadLocalResource resources to use for execution
    * @param output morsel into which execution results are written
    */
  def executeWorkUnit(threadLocalResource: THREAD_LOCAL_RESOURCE,
                      output: MorselExecutionContext): Unit

  /**
    * Returns true if there is another work unit to execute.
    *
    * @return true if there is another work unit to execute.
    */
  def canContinue: Boolean

  override def toString: String = s"${getClass.getSimpleName}[$workId]($workDescription)"
}
