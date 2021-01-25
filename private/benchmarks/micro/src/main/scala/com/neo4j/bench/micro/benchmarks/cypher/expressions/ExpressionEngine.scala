/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

sealed trait ExpressionEngine

case object CompiledExpressionEngine extends ExpressionEngine {
  final val NAME = "COMPILED"
}

case object InterpretedExpressionEngine extends ExpressionEngine {
  final val NAME = "INTERPRETED"
}
