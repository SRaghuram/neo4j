/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast

import org.opencypher.v9_0.expressions.SemanticDirection

case class GetDegreePrimitive(offset: Int, typ: Option[String], direction: SemanticDirection) extends RuntimeExpression
