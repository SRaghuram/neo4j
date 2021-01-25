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

case class RelationshipProperty(offset: Int, token: Int) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue =
    state.query.relationshipOps.getProperty(row.getLongAt(offset),
                                            token,
                                            state.cursors.relationshipScanCursor,
                                            state.cursors.propertyCursor,
                                            throwOnDeleted = true)

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class RelationshipPropertyLate(offset: Int, propKey: String) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    if (maybeToken.isEmpty)
      Values.NO_VALUE
    else
      state.query.relationshipOps.getProperty(row.getLongAt(offset),
                                              maybeToken.get,
                                              state.cursors.relationshipScanCursor,
                                              state.cursors.propertyCursor,
                                              throwOnDeleted = true)
  }

  override def children: Seq[AstNode[_]] = Seq.empty

}

case class RelationshipPropertyExists(offset: Int, token: Int) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    Some(state.query.relationshipOps.hasProperty(ctx.getLongAt(offset),
                                                 token,
                                                 state.cursors.relationshipScanCursor,
                                                 state.cursors.propertyCursor))
  }

  override def containsIsNull = false

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class RelationshipPropertyExistsLate(offset: Int, propKey: String) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    val result = if (maybeToken.isEmpty)
      false
    else
      state.query.relationshipOps.hasProperty(ctx.getLongAt(offset),
                                              maybeToken.get,
                                              state.cursors.relationshipScanCursor,
                                              state.cursors.propertyCursor)
    Some(result)
  }

  override def containsIsNull = false

  override def children: Seq[AstNode[_]] = Seq.empty
}
