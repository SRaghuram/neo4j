/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.cypher.internal.runtime.debug.DebugSupport

/**
  * The [[QueryManager]] keeps track of currently executing queries
  * and selects the next one to work on.
  */
class QueryManager {

  private val runningQueries = new ConcurrentLinkedQueue[ExecutingQuery]()

  def addQuery(query: ExecutingQuery): Unit = {
    DebugSupport.logQueries(s" Adding query $query")
    runningQueries.add(query)
  }

  /**
    * Select the next query to work on. As a side effect, we also remove queries
    * which have complete (successfully or not) from the set of running queries.
    */
  def nextQueryToWorkOn(workerId: Int): ExecutingQuery = {
    var query = runningQueries.peek()
    while (query != null && query.executionState.isCompleted) {
      DebugSupport.logQueries(s"Removing query $query")
      runningQueries.remove(query)
      query = runningQueries.peek()
    }
    query
  }
}
