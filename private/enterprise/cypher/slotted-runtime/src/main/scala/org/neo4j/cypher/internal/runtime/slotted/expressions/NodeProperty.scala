/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class NodeProperty(offset: Int, token: Int) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    state.query.nodeOps.getProperty(row.getLongAt(offset), token, state.cursors.nodeCursor, state.cursors.propertyCursor, throwOnDeleted = true)

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class NodePropertyLate(offset: Int, propKey: String) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    if (maybeToken.isEmpty)
      Values.NO_VALUE
    else
      state.query.nodeOps.getProperty(row.getLongAt(offset), maybeToken.get, state.cursors.nodeCursor, state.cursors.propertyCursor, throwOnDeleted = true)
  }

  override def children: Seq[AstNode[_]] = Seq.empty

}

case class NodePropertyExists(offset: Int, token: Int) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    Some(state.query.nodeOps.hasProperty(ctx.getLongAt(offset), token, state.cursors.nodeCursor, state.cursors.propertyCursor))
  }

  override def containsIsNull = false

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class NodePropertyExistsLate(offset: Int, propKey: String) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    val result = if (maybeToken.isEmpty)
      false
    else
      state.query.nodeOps.hasProperty(ctx.getLongAt(offset), maybeToken.get, state.cursors.nodeCursor, state.cursors.propertyCursor)
    Some(result)
  }

  override def containsIsNull = false

  override def children: Seq[AstNode[_]] = Seq.empty
}
