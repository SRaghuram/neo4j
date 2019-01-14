/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedNodeProperty
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue

case class SlottedCachedNodeProperty(nodeOffset: Int,
                                     propertyKey: Int,
                                     cachedPropertyOffset: Int) extends AbstractCachedNodeProperty with SlottedExpression {

  override def getNodeId(ctx: ExecutionContext): Long = ctx.getLongAt(nodeOffset)

  override def getCachedProperty(ctx: ExecutionContext): AnyValue = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def getPropertyKey(tokenContext: TokenContext): Int = propertyKey
}

case class SlottedCachedNodePropertyLate(nodeOffset: Int,
                                         propertyKey: String,
                                         cachedPropertyOffset: Int) extends AbstractCachedNodeProperty with SlottedExpression {

  override def getNodeId(ctx: ExecutionContext): Long = ctx.getLongAt(nodeOffset)

  override def getCachedProperty(ctx: ExecutionContext): AnyValue = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def getPropertyKey(tokenContext: TokenContext): Int =
    tokenContext.getOptPropertyKeyId(propertyKey).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)
}
