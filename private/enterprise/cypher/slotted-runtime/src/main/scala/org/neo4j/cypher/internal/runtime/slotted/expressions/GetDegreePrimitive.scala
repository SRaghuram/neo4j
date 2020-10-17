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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue

case class GetDegreePrimitive(offset: Int, direction: SemanticDirection)
  extends Expression
  with SlottedExpression {
  override def apply(row: ReadableRow, state: QueryState): AnyValue = longValue(state.query.nodeGetDegree(row.getLongAt(offset), direction, state.cursors.nodeCursor))
  override def children: Seq[AstNode[_]] = Seq.empty
}

case class GetDegreeWithTypePrimitive(offset: Int, typeId: Int, direction: SemanticDirection)
  extends Expression
  with SlottedExpression {
  override def apply(row: ReadableRow, state: QueryState): AnyValue = longValue(state.query.nodeGetDegree(row.getLongAt(offset), direction, typeId, state.cursors.nodeCursor))
  override def children: Seq[AstNode[_]] = Seq.empty
}

case class GetDegreeWithTypePrimitiveLate(offset: Int, typ: LazyType, direction: SemanticDirection)
  extends Expression
  with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val typeId = typ.getId(state.query)
    if (typeId == LazyType.UNKNOWN) Values.ZERO_INT
    else longValue(state.query.nodeGetDegree(row.getLongAt(offset), direction, typeId, state.cursors.nodeCursor))
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}
