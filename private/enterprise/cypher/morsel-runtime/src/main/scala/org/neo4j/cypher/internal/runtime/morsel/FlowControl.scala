/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.kernel.impl.query.QuerySubscription
//TODO: the reason this lives with morsel instead of with zombie is because it must be known by QueryState
trait FlowControl extends QuerySubscription {
  def getDemand: Long
  def hasDemand: Boolean
  def addServed(newlyServed: Long): Unit
}