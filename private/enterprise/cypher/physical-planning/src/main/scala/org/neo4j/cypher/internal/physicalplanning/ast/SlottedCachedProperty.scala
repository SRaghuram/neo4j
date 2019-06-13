/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.v4_0.expressions.{ASTCachedProperty, EntityType, PropertyKeyName}

/**
  * @param offset the offset into the long slots or ref slots for the entity
  * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
  * @param propToken token for the cached property
  * @param cachedPropertyOffset offset of the cached property in the ref slots
  */
case class SlottedCachedProperty(entityName: String,
                                 propertyKey: PropertyKeyName,
                                 offset: Int,
                                 offsetIsForLongSlot: Boolean,
                                 propToken: Int,
                                 cachedPropertyOffset: Int,
                                 override val entityType: EntityType
                                ) extends ASTCachedProperty with RuntimeExpression

// Token did not exist at plan time, so we'll need to look it up at runtime
/**
 * @param offset the offset into the long slots or ref slots for the entity
 * @param offsetIsForLongSlot if this is `true`, `offset` refers to the long slots. If this is `false`, `offset` refers to the ref slots.
 * @param propKey property name for the cached property
 * @param cachedPropertyOffset offset of the cached property in the ref slots
  */
case class SlottedCachedPropertyLate(entityName: String,
                                     propertyKey: PropertyKeyName,
                                     offset: Int,
                                     offsetIsForLongSlot: Boolean,
                                     propKey: String,
                                     cachedPropertyOffset: Int,
                                     override val entityType: EntityType
                                    ) extends ASTCachedProperty with RuntimeExpression
