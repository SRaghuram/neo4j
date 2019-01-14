/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}

abstract class ElementProperty(token: Option[Int], propName: String, elementIdVar: String, propKeyVar: String)
  extends CodeGenExpression {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    if (token.isEmpty) generator.lookupPropertyKey(propName, propKeyVar)

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E = {
    val localName = context.namer.newVarName()
    structure.declareProperty(localName)
    if (token.isEmpty)
      propertyByName(structure, localName)
    else
      propertyById(structure, localName)
    structure.incrementDbHits()
    structure.loadVariable(localName)
  }

  def propertyByName[E](body: MethodStructure[E], localName: String): Unit

  def propertyById[E](body: MethodStructure[E], localName: String): Unit

  override def nullable(implicit context: CodeGenContext) = true
}

case class NodeProperty(token: Option[Int], propName: String, nodeIdVar: Variable, propKeyVar: String)
  extends ElementProperty(token, propName, nodeIdVar.name, propKeyVar) {

  override def propertyByName[E](body: MethodStructure[E], localName: String) =
    if (nodeIdVar.nullable)
      body.ifNotStatement(body.isNull(nodeIdVar.name, nodeIdVar.codeGenType)) {ifBody =>
        ifBody.nodeGetPropertyForVar(nodeIdVar.name, nodeIdVar.codeGenType, propKeyVar, localName)
      }
    else
      body.nodeGetPropertyForVar(nodeIdVar.name, nodeIdVar.codeGenType, propKeyVar, localName)

  override def propertyById[E](body: MethodStructure[E], localName: String) =
    if (nodeIdVar.nullable)
      body.ifNotStatement(body.isNull(nodeIdVar.name, nodeIdVar.codeGenType)) {ifBody =>
        ifBody.nodeGetPropertyById(nodeIdVar.name, nodeIdVar.codeGenType, token.get, localName)
      }
    else
      body.nodeGetPropertyById(nodeIdVar.name, nodeIdVar.codeGenType, token.get, localName)

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.Value
}

case class RelProperty(token: Option[Int], propName: String, relIdVar: Variable, propKeyVar: String)
  extends ElementProperty(token, propName, relIdVar.name, propKeyVar) {

  override def propertyByName[E](body: MethodStructure[E], localName: String) =
    if (relIdVar.nullable)
      body.ifNotStatement(body.isNull(relIdVar.name, CodeGenType.primitiveRel)) { ifBody =>
        ifBody.relationshipGetPropertyForVar(relIdVar.name, relIdVar.codeGenType, propKeyVar, localName)
      }
    else
      body.relationshipGetPropertyForVar(relIdVar.name, relIdVar.codeGenType, propKeyVar, localName)

  override def propertyById[E](body: MethodStructure[E], localName: String) =
  if (relIdVar.nullable)
    body.ifNotStatement(body.isNull(relIdVar.name, CodeGenType.primitiveRel)) { ifBody =>
      ifBody.relationshipGetPropertyById(relIdVar.name, relIdVar.codeGenType, token.get, localName)
    }
    else
      body.relationshipGetPropertyById(relIdVar.name, relIdVar.codeGenType, token.get, localName)

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType.Value
}
