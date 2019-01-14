/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions

/**
  * Type representation of a CodeGenExpression - these are the JVM types that will be used!
  */
sealed trait RepresentationType

case object IntType extends RepresentationType // Primitive int

case object LongType extends RepresentationType // Primitive long

case object BoolType extends RepresentationType // Primitive boolean

case object FloatType extends RepresentationType // Primitive double

sealed trait ReferenceType extends RepresentationType // Boxed type (Object)

case object ReferenceType extends ReferenceType

sealed trait AnyValueType extends ReferenceType

case object AnyValueType extends AnyValueType

case object ValueType extends AnyValueType

case class ListReferenceType(inner: RepresentationType) extends RepresentationType

object RepresentationType {
  def isPrimitive(repr: RepresentationType): Boolean = repr match {
    case IntType | LongType | FloatType | BoolType => true
    case _ => false
  }

  def isValue(repr: RepresentationType): Boolean = repr match {
    case ValueType => true
    case _ => false
  }

  def isAnyValue(repr: RepresentationType): Boolean = repr match {
    case AnyValueType | ValueType => true
    case _ => false
  }
}
