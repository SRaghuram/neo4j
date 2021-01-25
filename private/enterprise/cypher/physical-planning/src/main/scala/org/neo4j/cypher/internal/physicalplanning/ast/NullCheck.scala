/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.runtime.ast.RuntimeProperty
import org.neo4j.cypher.internal.runtime.ast.RuntimeVariable

// These first three case classes are used to null check primitive entities stored in long slots
case class NullCheck(offset: Int, inner: Expression) extends RuntimeExpression

// This needs to be used to be able to rewrite an expression declared as a LogicalVariable
case class NullCheckVariable(offset: Int, inner: LogicalVariable) extends RuntimeVariable(inner.name)

// This needs to be used to be able to rewrite an expression declared as a LogicalProperty
case class NullCheckProperty(offset: Int, inner: LogicalProperty) extends RuntimeProperty(inner)

// This needs to be used to be able to rewrite an expression declared as a LogicalProperty
case class NullCheckReferenceProperty(offset: Int, inner: LogicalProperty) extends RuntimeProperty(inner)
