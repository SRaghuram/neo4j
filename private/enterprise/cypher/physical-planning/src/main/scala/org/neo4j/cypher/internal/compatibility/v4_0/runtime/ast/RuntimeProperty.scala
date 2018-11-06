/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast

import org.opencypher.v9_0.ast.semantics.{SemanticCheck, SemanticCheckResult, SemanticCheckableExpression}
import org.opencypher.v9_0.expressions.{Expression, LogicalProperty, PropertyKeyName}
import org.opencypher.v9_0.util.AssertionUtils.ifAssertionsEnabled
import org.opencypher.v9_0.util.{InputPosition, InternalException, Rewritable}

abstract class RuntimeProperty(val prop: LogicalProperty) extends LogicalProperty with SemanticCheckableExpression{
  override def semanticCheck(ctx: Expression.SemanticContext): SemanticCheck = SemanticCheckResult.success

  override def position: InputPosition = InputPosition.NONE

  override def map: Expression = prop.map

  override def propertyKey: PropertyKeyName = prop.propertyKey

  override def dup(children: Seq[AnyRef]): this.type = {
    val constructor = Rewritable.copyConstructor(this)
    val args = children.toVector

    ifAssertionsEnabled {
      val params = constructor.getParameterTypes
      val ok = params.length == args.length + 1 && classOf[LogicalProperty].isAssignableFrom(params.last)
      if (!ok)
        throw new InternalException(s"Unexpected rewrite children $children")
    }

    val ctorArgs = args :+ prop // Add the original Property expression
    val duped = constructor.invoke(this, ctorArgs: _*)
    duped.asInstanceOf[this.type]
  }
}
