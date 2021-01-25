/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.virtual.RelationshipValue

case class RelationshipFromSlot(offset: Int) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): RelationshipValue =
    state.query.relationshipOps.getById(row.getLongAt(offset))

  override def children: Seq[AstNode[_]] = Seq.empty
}
