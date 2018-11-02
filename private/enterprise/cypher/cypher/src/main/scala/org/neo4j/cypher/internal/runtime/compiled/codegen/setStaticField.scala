/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen

object setStaticField {
  def apply(clazz: Class[_], name: String, value: AnyRef) = {
    clazz.getDeclaredField(name).set(null, value)
  }
}
