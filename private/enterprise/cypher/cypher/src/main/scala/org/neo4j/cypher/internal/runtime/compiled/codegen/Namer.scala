/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen

import java.util.concurrent.atomic.AtomicInteger

class Namer(classNameCounter: AtomicInteger, varPrefix: String = "v", methodPrefix: String = "m", operationPrefix: String = "OP") {

  private var methodNameCounter = 0
  private var varNameCounter = 0
  private var opNameCounter = 0

  def newMethodName(): String = {
    methodNameCounter += 1
    s"$methodPrefix$methodNameCounter"
  }

  def newVarName(): String = {
    varNameCounter += 1
    s"$varPrefix$varNameCounter"
  }

  def newOpName(planName: String): String = {
    opNameCounter += 1
    s"$operationPrefix${opNameCounter}_$planName"
  }
}

object Namer {

  private val classNameCounter = new AtomicInteger()

  def apply(): Namer = new Namer(classNameCounter)

  def newClassName() = {
    s"GeneratedExecutionPlan${System.nanoTime()}"
  }
}
