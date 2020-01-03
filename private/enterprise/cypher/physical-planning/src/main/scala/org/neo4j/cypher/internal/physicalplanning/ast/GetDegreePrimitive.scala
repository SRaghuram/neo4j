/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection

case class GetDegreePrimitive(offset: Int, typ: Option[String], direction: SemanticDirection) extends RuntimeExpression
