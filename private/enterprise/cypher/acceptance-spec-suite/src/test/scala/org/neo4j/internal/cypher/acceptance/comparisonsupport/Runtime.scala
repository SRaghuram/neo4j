/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Runtimes(runtimes: Runtime*)

object Runtimes {
  implicit def runtimeToRuntimes(runtime: Runtime): Runtimes = Runtimes(runtime)

  val all = Runtimes(CompiledBytecode, CompiledSource, Slotted, SlottedWithCompiledExpressions, Interpreted)

  def definedBy(preParserArgs: Array[String]): Runtimes = {
    val runtimes = all.runtimes.filter(_.isDefinedBy(preParserArgs))
    if (runtimes.nonEmpty) Runtimes(runtimes: _*) else all
  }

  object CompiledSource extends Runtime(Set("COMPILED", "PROCEDURE"), "runtime=compiled debug=generate_java_source")

  object CompiledBytecode extends Runtime(Set("COMPILED", "PROCEDURE"), "runtime=compiled")

  object Slotted extends Runtime(Set("SLOTTED", "PROCEDURE"), "runtime=slotted")

  object SlottedWithCompiledExpressions extends Runtime(Set("SLOTTED", "PROCEDURE"), "runtime=slotted expressionEngine=COMPILED")

  object Interpreted extends Runtime(Set("INTERPRETED", "PROCEDURE"), "runtime=interpreted")

  // Not included in `all`
  object Morsel extends Runtime(Set("MORSEL", "PROCEDURE"), "runtime=morsel")

}

case class Runtime(acceptedRuntimeNames: Set[String], preparserOption: String) {
  def isDefinedBy(preParserArgs: Array[String]): Boolean = preparserOption.split(" ").forall(preParserArgs.contains(_))
}
