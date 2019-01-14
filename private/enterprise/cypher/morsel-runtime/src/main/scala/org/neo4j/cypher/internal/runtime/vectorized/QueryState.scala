/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.{MapValue, VirtualValues}

object QueryState {
  val EMPTY = QueryState(VirtualValues.EMPTY_MAP, null, 10000, singeThreaded = true)
}

/**
  * The query state of the morsel runtime
  */
case class QueryState(params: MapValue,
                      visitor: QueryResultVisitor[_],
                      morselSize: Int,
                      singeThreaded: Boolean, // hack until we solve [Transaction 1 - * Threads] problem
                      reduceCollector: Option[ReduceCollector] = None)
