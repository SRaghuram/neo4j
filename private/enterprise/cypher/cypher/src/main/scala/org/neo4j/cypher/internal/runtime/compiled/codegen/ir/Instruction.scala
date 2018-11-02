/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure

trait Instruction {
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = children.foreach(_.init(generator))
  def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  protected def children: Seq[Instruction]

  private def treeView: Seq[Instruction] = {
    children.foldLeft(Seq(this)) { (acc, child) => acc ++ child.treeView }
  }

  // Aggregating methods -- final to prevent overriding
  final def allOperatorIds: Set[String] = treeView.flatMap(_.operatorId).toSet

  protected def operatorId: Set[String] = Set.empty
}

object Instruction {

  val empty = new Instruction {
    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

    override protected def children = Seq.empty

    override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}
  }
}
