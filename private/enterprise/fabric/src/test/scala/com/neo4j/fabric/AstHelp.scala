/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric

import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.util.InputPosition

trait AstHelp {

  protected val ? : InputPosition = InputPosition.NONE

  protected def at(o: Int, l: Int, c: Int): InputPosition = InputPosition(o, l, c)

  implicit class StringOps(q: String) {
    def dropLines(p: String => Boolean): String = q.linesIterator.filterNot(p).mkString("\n")

    def dropFromLines: String = q.dropLines(_.startsWith("FROM"))
  }

  implicit class StatementOps(stmt: Statement) {
    private val p = Prettifier(ExpressionStringifier())
    def show = p.asString(stmt)
  }
}
