/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen.expressions

import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class CodeGenExpressionTypesTest extends CypherFunSuite {

  val int = Literal(1: java.lang.Integer)
  val double = Literal(1.1: java.lang.Double)
  val long = Literal(1234567890L: java.lang.Long)
  val string = Literal("apa")
  val node = NodeProjection(Variable("a", CypherCodeGenType(CTNode, ReferenceType)))
  val rel = RelationshipProjection(Variable("a", CypherCodeGenType(CTRelationship, ReferenceType)))
  val intCollection = ListLiteral(Seq(int))
  val doubleCollection = ListLiteral(Seq(double))
  val stringCollection = ListLiteral(Seq(string))
  val nodeCollection = ListLiteral(Seq(node))
  val relCollection = ListLiteral(Seq(rel))

  test("collection") {
    implicit val context: CodeGenContext = null

                       ListLiteral(Seq(int)).codeGenType.ct should equal(CTList(CTInteger))
                       ListLiteral(Seq(double)).codeGenType.ct should equal(CTList(CTFloat))
                       ListLiteral(Seq(int, double)).codeGenType.ct should equal(CTList(CTNumber))
                       ListLiteral(Seq(string, int)).codeGenType.ct should equal(CTList(CTAny))
                       ListLiteral(Seq(node, rel)).codeGenType.ct should equal(CTList(CTMap))
  }

  test("add") {
    implicit val context: CodeGenContext = null

    Addition(int, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Addition(string, int).codeGenType should equal(CypherCodeGenType(CTString, ReferenceType))
    Addition(string, string).codeGenType should equal(CypherCodeGenType(CTString, ReferenceType))
    Addition(intCollection, int).codeGenType should equal(CypherCodeGenType(CTList(CTInteger), ReferenceType))
    Addition(int, intCollection).codeGenType should equal(CypherCodeGenType(CTList(CTInteger), ReferenceType))
    Addition(double, intCollection).codeGenType should equal(CypherCodeGenType(CTList(CTNumber), ReferenceType))
    Addition(doubleCollection, intCollection).codeGenType should equal(CypherCodeGenType(CTList(CTNumber), ReferenceType))
    Addition(stringCollection, string).codeGenType should equal(CypherCodeGenType(CTList(CTString), ReferenceType))
    Addition(string, stringCollection).codeGenType should equal(CypherCodeGenType(CTList(CTString), ReferenceType))
  }

  test("division") {
    implicit val context: CodeGenContext = null
    Division(int, int).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))
    Division(int, long).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))
    Division(long, int).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))
    Division(long, long).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))

    Division(double, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Division(int, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Division(double, int).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Division(double, long).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Division(long, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
  }

  test("modulo") {
    implicit val context: CodeGenContext = null
    Modulo(int, int).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))
    Modulo(int, long).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))
    Modulo(long, int).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))
    Modulo(long, long).codeGenType should equal(CypherCodeGenType(CTInteger, ReferenceType))

    Modulo(double, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Modulo(int, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Modulo(double, int).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Modulo(double, long).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
    Modulo(long, double).codeGenType should equal(CypherCodeGenType(CTFloat, ReferenceType))
  }
}
