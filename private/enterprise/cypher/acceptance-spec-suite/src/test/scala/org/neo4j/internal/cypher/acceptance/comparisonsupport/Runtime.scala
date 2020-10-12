/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.codegen.api.CodeGeneration.GENERATE_JAVA_SOURCE_DEBUG_OPTION
import org.neo4j.cypher.internal.runtime.debug.DebugSupport

case class Runtimes(runtimes: Runtime*)

object Runtimes {
  implicit def runtimeToRuntimes(runtime: Runtime): Runtimes = Runtimes(runtime)

  val all: Runtimes = Runtimes(SlottedWithInterpretedExpressions, SlottedWithCompiledExpressions, Interpreted, Parallel, PipelinedFused, PipelinedNonFused)

  def definedBy(preParserArgs: Array[String]): Runtimes = {
    val runtimes = all.runtimes.filter(_.isDefinedBy(preParserArgs))
    if (runtimes.nonEmpty) Runtimes(runtimes: _*) else all
  }

  object SlottedWithInterpretedExpressions extends Runtime(Set("SLOTTED", "SCHEMA"), "runtime=slotted expressionEngine=interpreted")

  object SlottedWithCompiledExpressions extends Runtime(Set("SLOTTED", "SCHEMA"), "runtime=slotted expressionEngine=compiled")

  object Interpreted extends Runtime(Set("INTERPRETED", "SCHEMA"), "runtime=interpreted")

  object Parallel extends Runtime(Set("PARALLEL"), "runtime=parallel")

  object PipelinedFused extends Runtime(Set("PIPELINED", "SCHEMA"), "runtime=pipelined operatorEngine=compiled" +
    (if (DebugSupport.DEBUG_GENERATED_SOURCE_CODE) s" debug=$GENERATE_JAVA_SOURCE_DEBUG_OPTION" else ""))

  object PipelinedNonFused extends Runtime(Set("PIPELINED", "SCHEMA"), "runtime=pipelined operatorEngine=interpreted")

  object PipelinedFull extends Runtime(Set("PIPELINED", "SCHEMA"), "runtime=pipelined interpretedPipesFallback=all")
}

case class Runtime(acceptedRuntimeNames: Set[String], preparserOption: String) {
  def isDefinedBy(preParserArgs: Array[String]): Boolean =
    preparserOption.split(" ").forall(preParserArgs.contains(_))
}
