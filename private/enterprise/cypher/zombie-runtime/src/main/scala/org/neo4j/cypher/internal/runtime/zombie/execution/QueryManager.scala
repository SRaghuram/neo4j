/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

import java.util.concurrent.ConcurrentLinkedQueue

/**
  * The [[QueryManager]] keeps track of currently executing queries
  * and selects the next one to work on.
  */
class QueryManager {

  private val runningQueries = new ConcurrentLinkedQueue[ExecutingQuery]()

  def addQuery(query: ExecutingQuery): Unit = {
    runningQueries.add(query)
  }

  def removeQuery(query: ExecutingQuery): Unit = {
    if (!runningQueries.remove(query)) {
      throw new IllegalStateException("Tried to remove query that did not exist in the queue")
    }
  }

  def nextQueryToWorkOn(workerId: Int): ExecutingQuery = {
    runningQueries.peek()
  }
}
