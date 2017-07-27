/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection

case class ExpandAllLoopDataGenerator(opName: String, fromVar: Variable, dir: SemanticDirection,
                   types: Map[String, String], toVar: Variable, relVar: Variable)
  extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.createRelExtractor(relVar.name)
    types.foreach {
      case (typeVar,relType) => generator.lookupRelationshipTypeId(typeVar, relType)
    }
  }

  override def produceIterator[E](iterVar: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    if(types.isEmpty)
      generator.nodeGetRelationshipsWithDirection(iterVar, fromVar.name, fromVar.codeGenType, dir)
    else
      generator.nodeGetRelationshipsWithDirectionAndTypes(iterVar, fromVar.name, fromVar.codeGenType, dir, types.keys.toIndexedSeq)
    generator.incrementDbHits()
  }

  override def produceNext[E](nextVar: Variable, iterVar: String, generator: MethodStructure[E])
                             (implicit context: CodeGenContext) = {
    generator.incrementDbHits()
    generator.nextRelationshipAndNode(toVar.name, iterVar, dir, fromVar.name, relVar.name)
  }

  def foo[E](generator: MethodStructure[E]) = fromVar.codeGenType match {
    case c if c.isPrimitive => generator.loadVariable(fromVar.name)
  }

  override def hasNext[E](generator: MethodStructure[E], iterVar: String): E = generator.hasNextRelationship(iterVar)
}
