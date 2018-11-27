/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast

import org.neo4j.cypher.internal.v4_0.logical.plans.ASTCachedNodeProperty
import org.neo4j.cypher.internal.v4_0.expressions.Property

case class NodeProperty(offset: Int, propToken: Int, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

// Token did not exist at plan time, so we'll need to look it up at runtime
case class NodePropertyLate(offset: Int, propKey: String, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

case class CachedNodeProperty(offset: Int,
                              propToken: Int,
                              cachedPropertyOffset: Int
                             ) extends RuntimeExpression with ASTCachedNodeProperty

// Token did not exist at plan time, so we'll need to look it up at runtime
case class CachedNodePropertyLate(offset: Int,
                                  propKey: String,
                                  cachedPropertyOffset: Int
                                 ) extends RuntimeExpression with ASTCachedNodeProperty

case class NodePropertyExists(offset: Int, propToken: Int, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

// Token did not exist at plan time, so we'll need to look it up at runtime
case class NodePropertyExistsLate(offset: Int, propKey: String, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}
