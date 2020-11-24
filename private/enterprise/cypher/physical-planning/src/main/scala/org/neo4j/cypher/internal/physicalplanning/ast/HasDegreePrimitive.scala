/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ast.BooleanRuntimeExpression

case class HasDegreeGreaterThanPrimitive(offset: Int, typ: Option[Either[Int, String]], direction: SemanticDirection, degree: Expression) extends BooleanRuntimeExpression
case class HasDegreeGreaterThanOrEqualPrimitive(offset: Int, typ: Option[Either[Int, String]], direction: SemanticDirection, degree: Expression) extends BooleanRuntimeExpression
case class HasDegreePrimitive(offset: Int, typ: Option[Either[Int, String]], direction: SemanticDirection, degree: Expression) extends BooleanRuntimeExpression
case class HasDegreeLessThanPrimitive(offset: Int, typ: Option[Either[Int, String]], direction: SemanticDirection, degree: Expression) extends BooleanRuntimeExpression
case class HasDegreeLessThanOrEqualPrimitive(offset: Int, typ: Option[Either[Int, String]], direction: SemanticDirection, degree: Expression) extends BooleanRuntimeExpression
