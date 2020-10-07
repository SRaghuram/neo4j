/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class HasTypeFromSlot(offset: Int, resolvedTypeToken: Int) extends Predicate with SlottedExpression {
  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    Some(state.query.isTypeSetOnRelationship(resolvedTypeToken, ctx.getLongAt(offset), state.cursors.relationshipScanCursor))
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}

case class HasTypeFromSlotLate(offset: Int, typeName: String) extends Predicate with SlottedExpression {
  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptRelTypeId(typeName)
    val result =
      if (maybeToken.isEmpty)
        false
      else
        state.query.isTypeSetOnRelationship(maybeToken.get, ctx.getLongAt(offset), state.cursors.relationshipScanCursor)

    Some(result)
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}
