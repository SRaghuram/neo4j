/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen

import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.JoinTableType
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan

trait CodeGenPlan {

  val logicalPlan: LogicalPlan

  def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction])

  def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction])
}

trait LeafCodeGenPlan extends CodeGenPlan {

  override final def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) =
    throw new UnsupportedOperationException("Leaf plan does not consume")
}

case class JoinTableMethod(name: String, tableType: JoinTableType)

