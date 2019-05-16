/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.kernel.impl.query.QuerySubscription

trait FlowControl extends QuerySubscription {
  def getDemand: Long
  def hasDemand: Boolean
  def addServed(newlyServed: Long): Unit
}
