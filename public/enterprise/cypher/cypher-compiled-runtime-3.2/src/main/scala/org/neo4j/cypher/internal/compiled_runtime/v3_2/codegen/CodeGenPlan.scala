/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.JoinTableType
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan

trait CodeGenPlan {

  val logicalPlan: LogicalPlan

  def produce(context: CodeGenContext): (Option[JoinTableMethod], List[Instruction])

  def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction])
}

trait LeafCodeGenPlan extends CodeGenPlan {

  override final def consume(context: CodeGenContext, child: CodeGenPlan): (Option[JoinTableMethod], List[Instruction]) =
    throw new UnsupportedOperationException("Leaf plan does not consume")
}

case class JoinTableMethod(name: String, tableType: JoinTableType)

