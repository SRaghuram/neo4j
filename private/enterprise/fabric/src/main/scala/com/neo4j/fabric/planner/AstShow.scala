/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */

package com.neo4j.fabric.planner

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Parameter}
import org.neo4j.cypher.internal.v4_0.util.ASTNode
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

object AstShow {

  def show(node: ASTNode): String = node match {
    case fg: FromGraph => "FROM" + show(fg.expression)
    case p: Parameter  => "$" + p.name
    case e: Expression => e.asCanonicalStringVal
    case x             => x.asCanonicalStringVal
  }

  def show(n: CatalogName) = n.parts.mkString(".")

  def show(t: CypherType) = t.toNeoTypeString

  def show(av: AnyValue): String = av match {
    case v: Value => s"${v.prettyPrint()}: ${v.getTypeName}"
    case x        => x.getTypeName
  }

  def show(a: Catalog.Arg[_]) = s"${a.name}: ${a.tpe.getSimpleName}"

  def show(seq: Seq[_]): String = seq.map {
    case v: AnyValue       => show(v)
    case a: Catalog.Arg[_] => show(a)
  }.mkString("(", ",", ")")

}
