/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition
import org.neo4j.cypher.internal.runtime.morsel.{SchedulingResult, Task}

/**
 * Gives scheduling policies for execution graphs.
 */
trait SchedulingPolicy {
  def executionGraphSchedulingPolicy(executionGraphDefinition: ExecutionGraphDefinition): ExecutionGraphSchedulingPolicy
}

/**
 * Gives scheduling policies for executing queries. Must be cacheable.
 */
trait ExecutionGraphSchedulingPolicy {
  def querySchedulingPolicy(executingQuery: ExecutingQuery): QuerySchedulingPolicy
}

/**
  * Policy which selects the next task to execute. Must be thread-safe.
  */
trait QuerySchedulingPolicy {
  /**
   * Return the next task (together with the information if some task was cancelled), if there was any work to be done.
   * @param queryResources the query resources
   */
  def nextTask(queryResources: QueryResources): SchedulingResult[Task[QueryResources]]
}
