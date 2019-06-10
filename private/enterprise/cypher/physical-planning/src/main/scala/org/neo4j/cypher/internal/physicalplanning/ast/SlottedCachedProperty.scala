/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.v4_0.expressions.{ASTCachedProperty, EntityType, PropertyKeyName}

case class SlottedCachedProperty(entityName: String,
                                 propertyKey: PropertyKeyName,
                                 offset: Int,
                                 propToken: Int,
                                 cachedPropertyOffset: Int,
                                 override val entityType: EntityType
                                ) extends ASTCachedProperty with RuntimeExpression

// Token did not exist at plan time, so we'll need to look it up at runtime
case class SlottedCachedPropertyLate(entityName: String,
                                     propertyKey: PropertyKeyName,
                                     offset: Int,
                                     propKey: String,
                                     cachedPropertyOffset: Int,
                                     override val entityType: EntityType
                                    ) extends ASTCachedProperty with RuntimeExpression
