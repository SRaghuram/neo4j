/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedNodeProperty
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.VirtualNodeValue

case class SlottedCachedNodeProperty(nodeOffset: Int,
                                     offsetIsForLongSlot: Boolean,
                                     propertyKey: Int,
                                     cachedPropertyOffset: Int) extends AbstractCachedNodeProperty with SlottedExpression {

  override def getId(ctx: ExecutionContext): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(nodeOffset)
    else
      ctx.getRefAt(nodeOffset).asInstanceOf[VirtualNodeValue].id()

  override def getCachedProperty(ctx: ExecutionContext): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ExecutionContext, value: Value): Unit = ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: TokenContext): Int = propertyKey

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class SlottedCachedNodePropertyLate(nodeOffset: Int,
                                         offsetIsForLongSlot: Boolean,
                                         propertyKey: String,
                                         cachedPropertyOffset: Int) extends AbstractCachedNodeProperty with SlottedExpression {

  override def getId(ctx: ExecutionContext): Long =
    if (offsetIsForLongSlot)
    ctx.getLongAt(nodeOffset)
  else
    ctx.getRefAt(nodeOffset).asInstanceOf[VirtualNodeValue].id()

  override def getCachedProperty(ctx: ExecutionContext): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ExecutionContext, value: Value): Unit = ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: TokenContext): Int =
    tokenContext.getOptPropertyKeyId(propertyKey).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq.empty
}
