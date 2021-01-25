/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.runtime.ast.RuntimeProperty

case class RelationshipProperty(offset: Int, propToken: Int, name: String)(prop: LogicalProperty) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

case class RelationshipPropertyLate(offset: Int, propKey: String, name: String)(prop: LogicalProperty) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

case class RelationshipPropertyExists(offset: Int, propToken: Int, name: String)(prop: LogicalProperty) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

case class RelationshipPropertyExistsLate(offset: Int, propKey: String, name: String)(prop: LogicalProperty) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}
