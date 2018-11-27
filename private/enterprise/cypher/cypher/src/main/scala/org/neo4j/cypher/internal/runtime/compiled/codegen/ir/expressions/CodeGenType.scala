/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.v3_5.util.symbols._

trait CodeGenType {
  def isPrimitive: Boolean

  def isValue: Boolean

  def isAnyValue: Boolean

  def canBeNullable: Boolean

  def repr: RepresentationType
}

object CodeGenType {
  val Any = CypherCodeGenType(CTAny, ReferenceType)
  val AnyValue = CypherCodeGenType(CTAny, AnyValueType)
  val Value = CypherCodeGenType(CTAny, ValueType)
  val primitiveNode = CypherCodeGenType(CTNode, LongType)
  val primitiveRel = CypherCodeGenType(CTRelationship, LongType)
  val primitiveInt = CypherCodeGenType(CTInteger, LongType)
  val primitiveFloat = CypherCodeGenType(CTFloat, FloatType)
  val primitiveBool = CypherCodeGenType(CTBoolean, BoolType)
  val javaInt = JavaCodeGenType(IntType)
  val javaLong = JavaCodeGenType(LongType)
}

case class JavaCodeGenType(repr: RepresentationType) extends CodeGenType {
  override def isPrimitive = RepresentationType.isPrimitive(repr)

  def isValue = RepresentationType.isValue(repr)

  def isAnyValue = RepresentationType.isAnyValue(repr)

  override def canBeNullable: Boolean = false
}

case class CypherCodeGenType(ct: CypherType, repr: RepresentationType) extends CodeGenType {
  def isPrimitive = RepresentationType.isPrimitive(repr)

  def isValue = RepresentationType.isValue(repr)

  def isAnyValue = RepresentationType.isAnyValue(repr)

  def canBeNullable = !isPrimitive || (ct == CTNode) || (ct == CTRelationship)
}
