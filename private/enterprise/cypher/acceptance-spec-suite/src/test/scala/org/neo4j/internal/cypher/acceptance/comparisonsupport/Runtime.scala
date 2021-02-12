/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.codegen.api.CodeGeneration.GENERATE_JAVA_SOURCE_DEBUG_OPTION
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.impl.notification.NotificationCode

case class Runtimes(runtimes: Runtime*)

object Runtimes {
  implicit def runtimeToRuntimes(runtime: Runtime): Runtimes = Runtimes(runtime)

  val all: Runtimes = Runtimes(SlottedWithInterpretedExpressions, SlottedWithCompiledExpressions, Interpreted, Parallel, PipelinedFused, PipelinedNonFused)

  def definedBy(preParserArgs: Array[String]): Runtimes = {
    val runtimes = all.runtimes.filter(_.isDefinedBy(preParserArgs))
    if (runtimes.nonEmpty) Runtimes(runtimes: _*) else all
  }

  object SlottedWithInterpretedExpressions extends Runtime("SLOTTED", true, "runtime=slotted expressionEngine=interpreted")

  object SlottedWithCompiledExpressions extends Runtime("SLOTTED", true, "runtime=slotted expressionEngine=compiled") {
    override def checkNotificationsForWarnings(notifications: Seq[Notification]): Option[String] = {
      val errorStrings = notifications
        .collect { case n: Notification if n.getCode == NotificationCode.CODE_GENERATION_FAILED.notification(InputPosition.empty).getCode => n.getDescription }
      if (errorStrings.isEmpty) None else Some(s"Expression compilation failed with '${errorStrings.mkString(", ")}'")
    }
  }

  object Interpreted extends Runtime("INTERPRETED", true, "runtime=interpreted")

  object Parallel extends Runtime("PARALLEL", false, "runtime=parallel")

  object PipelinedFused extends Runtime("PIPELINED", true, "runtime=pipelined operatorEngine=compiled" +
    (if (DebugSupport.DEBUG_GENERATED_SOURCE_CODE) s" debug=$GENERATE_JAVA_SOURCE_DEBUG_OPTION" else "")) {
    override def checkNotificationsForWarnings(notifications: Seq[Notification]): Option[String] = {
      val errorStrings = notifications
        .collect { case n: Notification if n.getCode == NotificationCode.CODE_GENERATION_FAILED.notification(InputPosition.empty).getCode => n.getDescription }
      if (errorStrings.isEmpty) None else Some(s"Fusing failed with '${errorStrings.mkString(", ")}'")
    }
  }

  object PipelinedNonFused extends Runtime("PIPELINED", true, "runtime=pipelined operatorEngine=interpreted") {
    override def checkNotificationsForWarnings(notifications: Seq[Notification]): Option[String] = {
      val errorStrings = notifications
        .collect { case n: Notification if n.getCode == NotificationCode.CODE_GENERATION_FAILED.notification(InputPosition.empty).getCode => n.getDescription }
      if (errorStrings.isEmpty) None else Some(s"Expression compilation failed with '${errorStrings.mkString(", ")}'")
    }
  }

  object PipelinedFull extends Runtime("PIPELINED", true, "runtime=pipelined interpretedPipesFallback=all")
}

case class Runtime(name: String, schema: Boolean, preparserOption: String) {
  val acceptedRuntimeNames: Set[String] = if (schema) Set(name, "SCHEMA") else Set(name)

  def isDefinedBy(preParserArgs: Array[String]): Boolean =
    preparserOption.split(" ").forall(preParserArgs.contains(_))

  def checkNotificationsForWarnings(notifications: Seq[Notification]): Option[String] = None
}
