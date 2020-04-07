/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import scala.collection.mutable

class VariableNamer {
  private var counter: Int = 0
  private val parameters = mutable.Map.empty[String, String]
  private val variables = mutable.Map.empty[String, String]

  def nextVariableName(): String = {
    nextVariableName("")
  }

  def nextVariableName(suffix: String): String = {
    def maybeUnderscore = if (suffix.isEmpty) "" else "_"
    val nextName = s"v$counter$maybeUnderscore$suffix"
    counter += 1
    nextName
  }

  def parameterName(name: String): String = parameters.getOrElseUpdate(name, nextVariableName())
  def variableName(name: String): String = variables.getOrElseUpdate(name, nextVariableName())
}
