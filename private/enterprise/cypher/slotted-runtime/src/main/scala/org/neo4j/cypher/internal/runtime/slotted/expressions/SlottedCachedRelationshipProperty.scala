/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.AbstractCachedRelationshipProperty
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.VirtualRelationshipValue

case class SlottedCachedRelationshipProperty(relationshipOffset: Int,
                                             offsetIsForLongSlot: Boolean,
                                             propertyKey: Int,
                                             cachedPropertyOffset: Int) extends AbstractCachedRelationshipProperty with SlottedExpression {

  override def getId(ctx: ReadableRow): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(relationshipOffset)
    else
      ctx.getRefAt(relationshipOffset).asInstanceOf[VirtualRelationshipValue].id()

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit = ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: TokenContext): Int = propertyKey

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class SlottedCachedRelationshipPropertyLate(relationshipOffset: Int,
                                                 offsetIsForLongSlot: Boolean,
                                                 propertyKey: String,
                                                 cachedPropertyOffset: Int) extends AbstractCachedRelationshipProperty with SlottedExpression {

  override def getId(ctx: ReadableRow): Long =
    if (offsetIsForLongSlot)
      ctx.getLongAt(relationshipOffset)
    else
      ctx.getRefAt(relationshipOffset).asInstanceOf[VirtualRelationshipValue].id()

  override def getCachedProperty(ctx: ReadableRow): Value = ctx.getCachedPropertyAt(cachedPropertyOffset)

  override def setCachedProperty(ctx: ReadableRow, value: Value): Unit = ctx.setCachedPropertyAt(cachedPropertyOffset, value)

  override def getPropertyKey(tokenContext: TokenContext): Int =
    tokenContext.getOptPropertyKeyId(propertyKey).getOrElse(StatementConstants.NO_SUCH_PROPERTY_KEY)

  override def children: Seq[AstNode[_]] = Seq.empty
}
