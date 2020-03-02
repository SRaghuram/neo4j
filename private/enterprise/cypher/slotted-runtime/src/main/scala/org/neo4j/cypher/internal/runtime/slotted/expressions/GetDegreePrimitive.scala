/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class GetDegreePrimitive(offset: Int, typ: Option[String], direction: SemanticDirection)
  extends Expression
  with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = typ match {
    case None => Values.longValue(state.query.nodeGetDegree(row.getLongAt(offset), direction, state.cursors.nodeCursor))
    case Some(t) => state.query.getOptRelTypeId(t) match {
      case None => Values.ZERO_INT
      case Some(relTypeId) => Values.longValue(state.query.nodeGetDegree(row.getLongAt(offset), direction, relTypeId, state.cursors.nodeCursor))
    }
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}
