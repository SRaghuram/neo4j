/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class RelationshipProperty(offset: Int, token: Int) extends Expression with SlottedExpression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    state.query.relationshipOps.getProperty(ctx.getLongAt(offset),
                                            token,
                                            state.cursors.relationshipScanCursor,
                                            state.cursors.propertyCursor)

}

case class RelationshipPropertyLate(offset: Int, propKey: String) extends Expression with SlottedExpression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    if (maybeToken.isEmpty)
      Values.NO_VALUE
    else
      state.query.relationshipOps.getProperty(ctx.getLongAt(offset),
                                              maybeToken.get,
                                              state.cursors.relationshipScanCursor,
                                              state.cursors.propertyCursor)
  }

}

case class RelationshipPropertyExists(offset: Int, token: Int) extends Predicate with SlottedExpression {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    Some(state.query.relationshipOps.hasProperty(m.getLongAt(offset),
                                                 token,
                                                 state.cursors.relationshipScanCursor,
                                                 state.cursors.propertyCursor))
  }

  override def containsIsNull = false
}

case class RelationshipPropertyExistsLate(offset: Int, propKey: String) extends Predicate with SlottedExpression {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    val result = if (maybeToken.isEmpty)
      false
    else
      state.query.relationshipOps.hasProperty(m.getLongAt(offset),
                                              maybeToken.get,
                                              state.cursors.relationshipScanCursor,
                                              state.cursors.propertyCursor)
    Some(result)
  }

  override def containsIsNull = false
}
