/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.logical.plans.{ASTCachedProperty, CachedType}
import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression

case class CachedProperty(offset: Int,
                          propToken: Int,
                          cachedPropertyOffset: Int,
                          override val cachedType: CachedType
                         ) extends RuntimeExpression with ASTCachedProperty

// Token did not exist at plan time, so we'll need to look it up at runtime
case class CachedPropertyLate(offset: Int,
                              propKey: String,
                              cachedPropertyOffset: Int,
                              override val cachedType: CachedType
                             ) extends RuntimeExpression with ASTCachedProperty
