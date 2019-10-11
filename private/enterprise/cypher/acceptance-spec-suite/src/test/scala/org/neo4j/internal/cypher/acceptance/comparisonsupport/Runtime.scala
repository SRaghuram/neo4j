/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Runtimes(runtimes: Runtime*)

object Runtimes {
  implicit def runtimeToRuntimes(runtime: Runtime): Runtimes = Runtimes(runtime)

  val all = Runtimes(CompiledBytecode, CompiledSource, Slotted, SlottedWithCompiledExpressions, Interpreted, Parallel, Morsel)

  def definedBy(preParserArgs: Array[String]): Runtimes = {
    val runtimes = all.runtimes.filter(_.isDefinedBy(preParserArgs))
    if (runtimes.nonEmpty) Runtimes(runtimes: _*) else all
  }

  object CompiledSource extends Runtime(Set("LEGACY_COMPILED", "SCHEMA"), "runtime=legacy_compiled debug=generate_java_source")

  object CompiledBytecode extends Runtime(Set("LEGACY_COMPILED", "SCHEMA"), "runtime=legacy_compiled")

  object Slotted extends Runtime(Set("SLOTTED", "SCHEMA"), "runtime=slotted")

  object SlottedWithCompiledExpressions extends Runtime(Set("SLOTTED", "SCHEMA"), "runtime=slotted expressionEngine=COMPILED")

  object Interpreted extends Runtime(Set("INTERPRETED", "SCHEMA"), "runtime=interpreted")

  object Parallel extends Runtime(Set("PARALLEL"), "runtime=parallel")

  object Morsel extends Runtime(Set("MORSEL", "SCHEMA"), "runtime=morsel")

  object MorselFull extends Runtime(Set("MORSEL", "PROCEDURE"), "runtime=morsel interpretedPipesFallback=all")
}

case class Runtime(acceptedRuntimeNames: Set[String], preparserOption: String) {
  def isDefinedBy(preParserArgs: Array[String]): Boolean = preparserOption.split(" ").forall(preParserArgs.contains(_))
}
