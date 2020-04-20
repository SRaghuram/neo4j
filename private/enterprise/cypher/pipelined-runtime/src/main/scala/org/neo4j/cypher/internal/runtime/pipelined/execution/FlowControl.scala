/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.kernel.impl.query.QuerySubscription

trait FlowControl extends QuerySubscription {
  /**
   * Get the current demand unless this query is cancelled, in which case return 0
   */
  def getDemandUnlessCancelled: Long

  /**
   * Return true if the current query has demand. When cancelled, this method might
   * return `true` even if `getDemandUnlessCancelled()` returns zero.
   */
  def hasDemand: Boolean

  /**
   * Report newly served rows to this flow control.
   */
  def addServed(newlyServed: Long): Unit
}
