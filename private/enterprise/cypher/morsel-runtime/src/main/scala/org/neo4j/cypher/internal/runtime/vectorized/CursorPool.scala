/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.internal.kernel.api.{Cursor, CursorFactory, NodeCursor}
import org.neo4j.io.IOUtils

class CursorPools(cursorFactory: CursorFactory) extends AutoCloseable {

  val nodeCursorPool = new CursorPool[NodeCursor](() => cursorFactory.allocateNodeCursor())

  override def close(): Unit = {
    IOUtils.closeAll(nodeCursorPool)
  }
}

class CursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends AutoCloseable {

  var cached: CURSOR = _

  def allocate(): CURSOR = {
    if (cached != null) {
      val temp = cached
      cached = null.asInstanceOf[CURSOR]
      temp
    } else {
      cursorFactory()
    }
  }

  def free(cursor: CURSOR): Unit = {
    if (cached != null)
      cached.close()
    cached = cursor
  }

  override def close(): Unit = {
    if (cached != null)
      cached.close()
  }
}
