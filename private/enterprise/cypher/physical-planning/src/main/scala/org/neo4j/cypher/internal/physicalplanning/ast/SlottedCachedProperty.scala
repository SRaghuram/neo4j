/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression

trait SlottedCachedProperty extends ASTCachedProperty with RuntimeExpression {
  def offset: Int
  def offsetIsForLongSlot: Boolean
  def cachedPropertyOffset: Int

  /**
   * For slotted we don't need to distinct between the original and rewritten name
   */
  override def originalEntityName: String = entityName
}

/**
  *
  * @param entityName The name of the node or relationship
  * @param propertyKey the name of the property key
  * @param offset the offset into the long slots or ref slots for the entity
  * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
  * @param propToken token for the cached property
  * @param cachedPropertyOffset offset of the cached property in the ref slots
  * @param entityType the type of the entity, node or relationship
  * @param nullable `true` if entity is nullable otherwise `false`
  */
case class SlottedCachedPropertyWithPropertyToken(entityName: String,
                                                  propertyKey: PropertyKeyName,
                                                  offset: Int,
                                                  offsetIsForLongSlot: Boolean,
                                                  propToken: Int,
                                                  cachedPropertyOffset: Int,
                                                  entityType: EntityType,
                                                  nullable: Boolean
                                                 ) extends SlottedCachedProperty

// Token did not exist at plan time, so we'll need to look it up at runtime
/**
  *
  * @param entityName The name of the node or relationship
  * @param propertyKey the name of the property key
  * @param offset the offset into the long slots or ref slots for the entity
  * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
  * @param propKey property name for the cached property
  * @param cachedPropertyOffset ffset of the cached property in the ref slots
  * @param entityType the type of the entity, node or relationship
  * @param nullable `true` if entity is nullable otherwise `false`
  */
case class SlottedCachedPropertyWithoutPropertyToken(entityName: String,
                                                     propertyKey: PropertyKeyName,
                                                     offset: Int,
                                                     offsetIsForLongSlot: Boolean,
                                                     propKey: String,
                                                     cachedPropertyOffset: Int,
                                                     entityType: EntityType,
                                                     nullable: Boolean
                                                    ) extends SlottedCachedProperty
