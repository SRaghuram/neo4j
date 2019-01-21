/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.io.IOUtils

/**
  * Resources used by the morsel runtime for query execution.
  */
class QueryResources(cursorFactory: CursorFactory) extends AutoCloseable {

  val expressionCursors: ExpressionCursors = new ExpressionCursors(cursorFactory)
  val cursorPools: CursorPools = new CursorPools(cursorFactory)

  override def close(): Unit = {
    IOUtils.closeAll(expressionCursors, cursorPools)
  }
}

