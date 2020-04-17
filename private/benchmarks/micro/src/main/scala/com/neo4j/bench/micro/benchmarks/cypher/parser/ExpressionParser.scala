/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.parser

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.parser.Expressions
import org.parboiled.scala.ReportingParseRunner
import org.parboiled.scala.Rule1

class ExpressionParser extends Expressions {
  private val parser: Rule1[Expression] = Expression

  def parse(text: String): Expression = {
    val res = ReportingParseRunner(parser).run(text)
    res.result match {
      case Some(e) => e
      case None => throw new IllegalArgumentException(s"Could not parse expression: ${res.parseErrors}")
    }
  }
}
