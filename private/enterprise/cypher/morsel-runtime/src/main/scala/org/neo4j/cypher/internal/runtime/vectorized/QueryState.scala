/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue

/**
  * The query state of the morsel runtime
  */
case class QueryState(params: MapValue,
                      visitor: QueryResultVisitor[_],
                      morselSize: Int,
                      queryIndexes: Array[IndexReadSession],
                      transactionBinder: TransactionBinder, // hack until we stop prePopulate from using NodeProxy logic
                      reduceCollector: Option[ReduceCollector] = None)
