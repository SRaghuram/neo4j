/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.internal.kernel.api._
import org.neo4j.io.IOUtils

class CursorPools(cursorFactory: CursorFactory) extends AutoCloseable {

  val nodeCursorPool = new CursorPool[NodeCursor](
    () => cursorFactory.allocateNodeCursor())
  val relationshipGroupCursorPool = new CursorPool[RelationshipGroupCursor](
    () => cursorFactory.allocateRelationshipGroupCursor())
  val relationshipTraversalCursorPool = new CursorPool[RelationshipTraversalCursor](
    () => cursorFactory.allocateRelationshipTraversalCursor())
  val nodeValueIndexCursorPool = new CursorPool[NodeValueIndexCursor](
    () => cursorFactory.allocateNodeValueIndexCursor())
  val nodeLabelIndexCursorPool = new CursorPool[NodeLabelIndexCursor](
    () => cursorFactory.allocateNodeLabelIndexCursor())

  override def close(): Unit = {
    IOUtils.closeAll(nodeCursorPool)
  }
}

class CursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends AutoCloseable {

  private var cached: CURSOR = _

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
