/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast

import org.neo4j.cypher.internal.v4_0.expressions.LogicalProperty

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
