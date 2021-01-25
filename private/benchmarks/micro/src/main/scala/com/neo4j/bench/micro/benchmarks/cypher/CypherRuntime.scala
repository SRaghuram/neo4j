/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions

object CypherRuntime {
  def from(cypherRuntimeString: String): CypherRuntime =
    cypherRuntimeString match {
      case Interpreted.NAME => Interpreted
      case Slotted.NAME => Slotted
      case Pipelined.NAME => Pipelined
      case PipelinedSourceCode.NAME => PipelinedSourceCode
      case Parallel.NAME => Parallel
      case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntimeString")
    }
}

sealed trait CypherRuntime {
  val debugOptions: CypherDebugOptions
}

case object Interpreted extends CypherRuntime {
  final val NAME = "interpreted"
  override val debugOptions: CypherDebugOptions = CypherDebugOptions.default
}

case object Slotted extends CypherRuntime {
  final val NAME = "slotted"
  override val debugOptions: CypherDebugOptions = CypherDebugOptions.default
}

case object Pipelined extends CypherRuntime {
  final val NAME = "pipelined"
  override val debugOptions: CypherDebugOptions = CypherDebugOptions.default
}

case object PipelinedSourceCode extends CypherRuntime {
  final val NAME = "pipelined-sourcecode"
  override val debugOptions: CypherDebugOptions = CypherDebugOptions.default.withOptionEnabled(CypherDebugOption.generateJavaSource)
}

case object Parallel extends CypherRuntime {
  final val NAME = "parallel"
  override val debugOptions: CypherDebugOptions = CypherDebugOptions.default
}
