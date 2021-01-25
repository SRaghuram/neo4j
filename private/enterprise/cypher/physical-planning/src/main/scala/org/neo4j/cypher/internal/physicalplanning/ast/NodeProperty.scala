/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.runtime.ast.RuntimeProperty

case class NodeProperty(offset: Int, propToken: Int, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

// Token did not exist at plan time, so we'll need to look it up at runtime
case class NodePropertyLate(offset: Int, propKey: String, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

case class NodePropertyExists(offset: Int, propToken: Int, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

// Token did not exist at plan time, so we'll need to look it up at runtime
case class NodePropertyExistsLate(offset: Int, propKey: String, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}
