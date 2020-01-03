/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class HasLabelFromSlot(offset: Int, resolvedLabelToken: Int) extends Predicate with SlottedExpression {
  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    Some(state.query.isLabelSetOnNode(resolvedLabelToken, m.getLongAt(offset), state.cursors.nodeCursor))
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}

case class HasLabelFromSlotLate(offset: Int, labelName: String) extends Predicate with SlottedExpression {
  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptLabelId(labelName)
    val result =
      if (maybeToken.isEmpty)
        false
      else
        state.query.isLabelSetOnNode(maybeToken.get, m.getLongAt(offset), state.cursors.nodeCursor)

    Some(result)
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}
