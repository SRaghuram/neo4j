/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

/**
 * The query state of the pipelined runtime
 */
case class QueryState(queryContext: QueryContext,
                      params: Array[AnyValue],
                      subscriber: QuerySubscriber,
                      flowControl: FlowControl,
                      morselSize: Int,
                      queryIndexes: Array[IndexReadSession],
                      numberOfWorkers: Int,
                      nExpressionSlots: Int,
                      prepopulateResults: Boolean,
                      doProfile: Boolean,
                      input: InputDataStream)
