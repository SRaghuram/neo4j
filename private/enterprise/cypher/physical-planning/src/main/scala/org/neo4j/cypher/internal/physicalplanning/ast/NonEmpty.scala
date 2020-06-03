/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition

/**
  * Special aggregating function for checking if rows are produced. Not exposed in Cypher.
  */
case class NonEmpty()(val position: InputPosition) extends Expression
