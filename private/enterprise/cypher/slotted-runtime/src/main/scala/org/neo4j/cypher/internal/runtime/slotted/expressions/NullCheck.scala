/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

case class NullCheck(offset: Int, inner: Expression) extends Expression with SlottedExpression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    if (entityIsNull(ctx.getLongAt(offset))) Values.NO_VALUE else inner(ctx, state)

}
