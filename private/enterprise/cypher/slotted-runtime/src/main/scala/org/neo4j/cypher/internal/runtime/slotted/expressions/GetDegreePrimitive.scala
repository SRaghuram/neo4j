/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class GetDegreePrimitive(offset: Int, typ: Option[String], direction: SemanticDirection)
  extends Expression
    with SlottedExpression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = typ match {
    case None => Values.longValue(state.query.nodeGetDegree(ctx.getLongAt(offset), direction))
    case Some(t) => state.query.getOptRelTypeId(t) match {
      case None => Values.ZERO_INT
      case Some(relTypeId) => Values.longValue(state.query.nodeGetDegree(ctx.getLongAt(offset), direction, relTypeId))
    }
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}
