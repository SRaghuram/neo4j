/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

object Columns {

  def combine(left: Seq[String], right: Seq[String]): Seq[String] =
    left.filterNot(right.contains) ++ right

  def paramName(varName: String): String =
    s"@@$varName"

  def asParamMappings(varNames: Seq[String]): Map[String, String] =
    varNames.map(varName => varName -> paramName(varName)).toMap
}
