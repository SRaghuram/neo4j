/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

object CypherRuntime {
  def from(cypherRuntimeString: String): CypherRuntime =
    cypherRuntimeString match {
      case CompiledByteCode.NAME => CompiledByteCode
      case CompiledSourceCode.NAME => CompiledSourceCode
      case Interpreted.NAME => Interpreted
      case Slotted.NAME => Slotted
      case Morsel.NAME => Morsel
      case Parallel.NAME => Parallel
      case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntimeString")
    }
}

sealed trait CypherRuntime {
  val debugOptions: Set[String]
}

case object Interpreted extends CypherRuntime {
  final val NAME = "interpreted"
  override val debugOptions: Set[String] = Set()
}

case object Slotted extends CypherRuntime {
  final val NAME = "slotted"
  override val debugOptions: Set[String] = Set()
}

case object CompiledByteCode extends CypherRuntime {
  final val NAME = "compiled-bytecode"
  override val debugOptions: Set[String] = Set()
}

case object CompiledSourceCode extends CypherRuntime {
  final val NAME = "compiled-sourcecode"
  override val debugOptions: Set[String] = Set("generate_java_source")
}

case object Morsel extends CypherRuntime {
  final val NAME = "morsel"
  override val debugOptions: Set[String] = Set()
}

case object Parallel extends CypherRuntime {
  final val NAME = "parallel"
  override val debugOptions: Set[String] = Set()
}
