/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

case class NullCheck(offset: Int, inner: Expression) extends RuntimeExpression

// This needs to be used to be able to rewrite an expression declared as a LogicalVariable
case class NullCheckVariable(offset: Int, inner: LogicalVariable) extends RuntimeVariable(inner.name)

// This needs to be used to be able to rewrite an expression declared as a LogicalProperty
case class NullCheckProperty(offset: Int, inner: LogicalProperty) extends RuntimeProperty(inner) {

  // We have to override the implementation in RuntimeProperty for correctness. This smells a bit...
  override def dup(children: Seq[AnyRef]): this.type = {
    val newOffset = children.head.asInstanceOf[Int]
    val newInner = children(1).asInstanceOf[LogicalProperty]
    // We only ever rewrite this with inner already rewritten, so we should not need to copy
    if (offset == newOffset && inner == newInner)
      this
    else
      copy(offset = newOffset, inner = newInner).asInstanceOf[this.type]
  }
}
