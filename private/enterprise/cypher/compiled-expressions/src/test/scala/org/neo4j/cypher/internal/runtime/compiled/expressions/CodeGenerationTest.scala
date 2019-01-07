/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.lang.Math.{PI, sin}
import java.time.{Clock, Duration}
import java.util.concurrent.ThreadLocalRandom

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{LongSlot, RefSlot, Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.{DbAccess, ExpressionCursors}
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, ExpressionCursors, MapExecutionContext}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.symbols.{CypherType, ListType}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.procs.{QualifiedName => KernelQualifiedName}
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, RelationshipScanCursor}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.CoordinateReferenceSystem.{Cartesian, WGS84}
import org.neo4j.values.storable.LocalTimeValue.localTime
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues._
import org.neo4j.values.virtual.{MapValue, NodeValue, RelationshipValue, VirtualValues}
import org.neo4j.values.{AnyValue, AnyValues}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable

class CodeGenerationTest extends CypherFunSuite with AstConstructionTestSupport {

  private val ctx = mock[ExecutionContext]
  private val db = mock[DbAccess]

  private val nodeCursor = mock[NodeCursor]
  private val relationshipScanCursor = mock[RelationshipScanCursor]
  private val propertyCursor = mock[PropertyCursor]
  private val cursors = mock[ExpressionCursors]
  when(cursors.nodeCursor).thenReturn(nodeCursor)
  when(cursors.propertyCursor).thenReturn(propertyCursor)
  when(cursors.relationshipScanCursor).thenReturn(relationshipScanCursor)
  when(db.relationshipGetStartNode(any[RelationshipValue])).thenAnswer(new Answer[NodeValue] {
    override def answer(in: InvocationOnMock): NodeValue = in.getArgument[RelationshipValue](0).startNode()
  })
  when(db.relationshipGetEndNode(any[RelationshipValue])).thenAnswer(new Answer[NodeValue] {
    override def answer(in: InvocationOnMock): NodeValue = in.getArgument[RelationshipValue](0).endNode()
  })

  private val random = ThreadLocalRandom.current()

  test("round function") {
    compile(function("round", literalFloat(PI))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(3.0))
    compile(function("round", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("rand function") {
    // Given
    val expression = function("rand")

    // When
    val compiled = compile(expression)

    // Then
    val value = compiled.evaluate(ctx, db, EMPTY_MAP, cursors).asInstanceOf[DoubleValue].doubleValue()
    value should (be >= 0.0 and be <1.0)
  }

  test("sin function") {
    val arg = random.nextDouble()
    compile(function("sin", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(sin(arg)))
    compile(function("sin", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("asin function") {
    val arg = random.nextDouble()
    compile(function("asin", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.asin(arg)))
    compile(function("asin", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("haversin function") {
    val arg = random.nextDouble()
    compile(function("haversin", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue((1.0 - Math.cos(arg)) / 2))
    compile(function("haversin", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("acos function") {
    val arg = random.nextDouble()
    compile(function("acos", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.acos(arg)))
    compile(function("acos", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("cos function") {
    val arg = random.nextDouble()
    compile(function("cos", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.cos(arg)))
    compile(function("cos", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("cot function") {
    val arg = random.nextDouble()
    compile(function("cot", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(1 / Math.tan(arg)))
    compile(function("cot", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("atan function") {
    val arg = random.nextDouble()
    compile(function("atan", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.atan(arg)))
    compile(function("atan", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("atan2 function") {
    val arg1 = random.nextDouble()
    val arg2 = random.nextDouble()
    compile(function("atan2", literalFloat(arg1), literalFloat(arg2))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.atan2(arg1, arg2)))
    compile(function("atan2", nullLiteral, literalFloat(arg1))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(function("atan2", literalFloat(arg1), nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(function("atan2", nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("tan function") {
    val arg = random.nextDouble()
    compile(function("tan", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.tan(arg)))
    compile(function("tan", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("ceil function") {
    val arg = random.nextDouble()
    compile(function("ceil", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.ceil(arg)))
    compile(function("ceil", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("floor function") {
    val arg = random.nextDouble()
    compile(function("floor", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.floor(arg)))
    compile(function("floor", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("abs function") {
    compile(function("abs", literalFloat(3.2))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(3.2))
    compile(function("abs", literalFloat(-3.2))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(3.2))
    compile(function("abs", literalInt(3))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(3))
    compile(function("abs", literalInt(-3))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(3))
    compile(function("abs", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("radians function") {
    val arg = random.nextDouble()
    compile(function("radians", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.toRadians(arg)))
    compile(function("radians", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("degrees function") {
    val arg = random.nextDouble()
    compile(function("degrees", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.toDegrees(arg)))
    compile(function("degrees", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("exp function") {
    val arg = random.nextDouble()
    compile(function("exp", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.exp(arg)))
    compile(function("exp", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("log function") {
    val arg = random.nextDouble()
    compile(function("log", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.log(arg)))
    compile(function("log", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("log10 function") {
    val arg = random.nextDouble()
    compile(function("log10", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.log10(arg)))
    compile(function("log10", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("sign function") {
    val arg = random.nextInt()
    compile(function("sign", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.signum(arg)))
    compile(function("sign", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("sqrt function") {
    val arg = random.nextDouble()
    compile(function("sqrt", literalFloat(arg))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(doubleValue(Math.sqrt(arg)))
    compile(function("sqrt", nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("pi function") {
    compile(function("pi")).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.PI)
  }

  test("e function") {
    compile(function("e")).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.E)
  }

  test("range function with no step") {
    val range = function("range", literalInt(5), literalInt(9))
    compile(range).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(list(longValue(5), longValue(6), longValue(7),
                                                                  longValue(8), longValue(9)))
  }

  test("range function with step") {
    val range = function("range", literalInt(5), literalInt(9), literalInt(2))
    compile(range).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(list(longValue(5), longValue(7), longValue(9)))
  }

  test("coalesce function") {
    compile(function("coalesce", nullLiteral, nullLiteral, literalInt(2), nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(2))
    compile(function("coalesce", nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("coalesce function with parameters") {
    val compiled = compile(function("coalesce", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, db, map(Array("a", "b", "c"), Array(NO_VALUE, longValue(2), NO_VALUE)), cursors) should equal(longValue(2))
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"), Array(NO_VALUE, NO_VALUE, NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("distance function") {
    val compiled = compile(function("distance", parameter("p1"), parameter("p2")))
    val keys = Array("p1", "p2")
    compiled.evaluate(ctx, db, map(keys,
                                       Array(pointValue(Cartesian, 0.0, 0.0),
                                             pointValue(Cartesian, 1.0, 1.0))), cursors) should equal(doubleValue(Math.sqrt(2)))
    compiled.evaluate(ctx, db, map(keys,
                                       Array(pointValue(Cartesian, 0.0, 0.0),
                                             NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(keys,
                                       Array(pointValue(Cartesian, 0.0, 0.0),
                                             pointValue(WGS84, 1.0, 1.0))), cursors) should equal(NO_VALUE)

  }

  test("startNode") {
    val compiled = compile(function("startNode", parameter("a")))
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(rel.startNode())
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("endNode") {
    val compiled = compile(function("endNode", parameter("a")))
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(rel.endNode())
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)

  }

  test("exists on node") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, map(Array("prop"), Array(stringValue("hello"))))
    when(db.propertyKey("prop")).thenReturn(42)
    when(db.nodeHasProperty(1, 42, nodeCursor, propertyCursor)).thenReturn(true)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(Values.TRUE)
  }

  test("exists on relationship") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), map(Array("prop"), Array(stringValue("hello"))))
    when(db.propertyKey("prop")).thenReturn(42)
    when(db.relationshipHasProperty(43, 42, relationshipScanCursor, propertyCursor)).thenReturn(true)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(Values.TRUE)
  }

  test("exists on map") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val mapValue = map(Array("prop"), Array(stringValue("hello")))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue)), cursors) should equal(Values.TRUE)
  }

  test("head function") {
    val compiled = compile(function("head", parameter("a")))
    val listValue = list(stringValue("hello"), intValue(42))

    compiled.evaluate(ctx, db, map(Array("a"), Array(listValue)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(EMPTY_LIST)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("last function") {
    val compiled = compile(function("last", parameter("a")))
    val listValue = list(intValue(42), stringValue("hello"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(listValue)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(EMPTY_LIST)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("left function") {
    val compiled = compile(function("left", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(4))), cursors) should
      equal(stringValue("HELL"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(17))), cursors) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)

    an[IndexOutOfBoundsException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(-1))), cursors)
  }

  test("ltrim function") {
    val compiled = compile(function("ltrim", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("  HELLO  "))), cursors) should
      equal(stringValue("HELLO  "))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("rtrim function") {
    val compiled = compile(function("rtrim", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("  HELLO  "))), cursors) should
      equal(stringValue("  HELLO"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("trim function") {
    val compiled = compile(function("trim", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("  HELLO  "))), cursors) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("replace function") {
    val compiled = compile(function("replace", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                       Array(stringValue("HELLO"),
                                             stringValue("LL"),
                                             stringValue("R"))), cursors) should equal(stringValue("HERO"))
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                       Array(NO_VALUE,
                                             stringValue("LL"),
                                             stringValue("R"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                       Array(stringValue("HELLO"),
                                             NO_VALUE,
                                             stringValue("R"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b", "c"),
                                       Array(stringValue("HELLO"),
                                             stringValue("LL"),
                                             NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("reverse function") {
    val compiled = compile(function("reverse", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("PARIS"))), cursors) should equal(stringValue("SIRAP"))
    val original = list(intValue(1), intValue(2), intValue(3))
    val reversed = list(intValue(3), intValue(2), intValue(1))
    compiled.evaluate(ctx, db, map(Array("a"), Array(original)), cursors) should equal(reversed)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("right function") {
    val compiled = compile(function("right", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(4))), cursors) should
      equal(stringValue("ELLO"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)
  }

  test("split function") {
    val compiled = compile(function("split", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), stringValue("LL"))), cursors) should
      equal(list(stringValue("HE"), stringValue("O")))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, stringValue("LL"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), EMPTY_STRING)), cursors) should
      equal(list(stringValue("H"), stringValue("E"), stringValue("L"), stringValue("L"), stringValue("O")))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(EMPTY_STRING, stringValue("LL"))), cursors) should equal(list(EMPTY_STRING))

  }

  test("substring function no length") {
    val compiled = compile(function("substring", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(1))), cursors) should
      equal(stringValue("ELLO"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
  }

  test("substring function with length") {
    val compiled = compile(function("substring", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, db, map(Array("a", "b", "c"), Array(stringValue("HELLO"), intValue(1), intValue(2))), cursors) should
      equal(stringValue("EL"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
  }

  test("toLower function") {
    val compiled = compile(function("toLower", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("HELLO"))), cursors) should
      equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toUpper function") {
    val compiled = compile(function("toUpper", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("hello"))), cursors) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("nodes function") {
    val compiled = compile(function("nodes", parameter("a")))

    val nodes = Array(nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
          nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
          nodeValue(3, EMPTY_TEXT_ARRAY, EMPTY_MAP))
    val rels = Array( relationshipValue(11, nodes(0), nodes(1), stringValue("R"), EMPTY_MAP),
                      relationshipValue(12, nodes(1), nodes(2), stringValue("R"), EMPTY_MAP))
    val path = VirtualValues.path(nodes, rels)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(path)), cursors) should equal(list(nodes:_*))
  }

  test("relationships function") {
    val compiled = compile(function("relationships", parameter("a")))

    val nodes = Array(nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(3, EMPTY_TEXT_ARRAY, EMPTY_MAP))
    val rels = Array( relationshipValue(11, nodes(0), nodes(1), stringValue("R"), EMPTY_MAP),
                      relationshipValue(12, nodes(1), nodes(2), stringValue("R"), EMPTY_MAP))
    val path = VirtualValues.path(nodes, rels)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(path)), cursors) should equal(list(rels:_*))
  }

  test("id on node") {
    val compiled = compile(function("id", parameter("a")))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP)

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(longValue(1))
  }

  test("id on relationship") {
    val compiled = compile(function("id", parameter("a")))

    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"),EMPTY_MAP)


    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(longValue(43))
  }

  test("labels function") {
    val compiled = compile(function("labels", parameter("a")))

    val labels = Values.stringArray("A", "B", "C")
    val node = nodeValue(1, labels, EMPTY_MAP)
    when(db.getLabelsForNode(node.id(), nodeCursor)).thenReturn(VirtualValues.fromArray(labels))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(labels)
  }

  test("type function") {
    val compiled = compile(function("type", parameter("a")))
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)

    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(stringValue("R"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("points from node") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val node = nodeValue(1, EMPTY_TEXT_ARRAY, pointMap)
    when(db.propertyKey("x")).thenReturn(1)
    when(db.propertyKey("y")).thenReturn(2)
    when(db.propertyKey("crs")).thenReturn(3)

    when(db.nodeProperty(any[Long], any[Int], any[NodeCursor], any[PropertyCursor])).thenAnswer(new Answer[AnyValue] {
      override def answer(in: InvocationOnMock): AnyValue = in.getArgument[Int](1) match {
        case 1 => pointMap.get("x")
        case 2 => pointMap.get("y")
        case 3 => pointMap.get("crs")
        case _ => NO_VALUE
      }
    })

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(PointValue.fromMap(pointMap))
  }

  test("points from relationship") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val rel = relationshipValue(43,
                      nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      stringValue("R"),pointMap)
    when(db.propertyKey("x")).thenReturn(1)
    when(db.propertyKey("y")).thenReturn(2)
    when(db.propertyKey("crs")).thenReturn(3)

    when(db.relationshipProperty(any[Long], any[Int], any[RelationshipScanCursor], any[PropertyCursor])).thenAnswer(new Answer[AnyValue] {
      override def answer(in: InvocationOnMock): AnyValue = in.getArgument[Int](1) match {
        case 1 => pointMap.get("x")
        case 2 => pointMap.get("y")
        case 3 => pointMap.get("crs")
        case _ => NO_VALUE
      }
    })

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(PointValue.fromMap(pointMap))
  }

  test("points from map") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(pointMap)), cursors) should equal(PointValue.fromMap(pointMap))
  }

  test("keys on node") {
    val compiled = compile(function("keys", parameter("a")))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP)
    when(db.nodePropertyIds(1, nodeCursor, propertyCursor)).thenReturn(Array(1,2,3))
    when(db.getPropertyKeyName(1)).thenReturn("A")
    when(db.getPropertyKeyName(2)).thenReturn("B")
    when(db.getPropertyKeyName(3)).thenReturn("C")

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on relationship") {
    val compiled = compile(function("keys", parameter("a")))


    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), EMPTY_MAP)
    when(db.relationshipPropertyIds(43, relationshipScanCursor, propertyCursor)).thenReturn(Array(1,2,3))
    when(db.getPropertyKeyName(1)).thenReturn("A")
    when(db.getPropertyKeyName(2)).thenReturn("B")
    when(db.getPropertyKeyName(3)).thenReturn("C")


    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on map") {
    val compiled = compile(function("keys", parameter("a")))

    val mapValue = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue)), cursors) should equal(mapValue.keys())
  }

  test("size function") {
    val compiled = compile(function("size", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("HELLO"))), cursors) should equal(intValue(5))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)
  }

  test("length function") {
    val compiled = compile(function("length", parameter("a")))
    val nodes = Array(nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                      nodeValue(3, EMPTY_TEXT_ARRAY, EMPTY_MAP))
    val rels = Array( relationshipValue(11, nodes(0), nodes(1), stringValue("R"), EMPTY_MAP),
                      relationshipValue(12, nodes(1), nodes(2), stringValue("R"), EMPTY_MAP))
    val path = VirtualValues.path(nodes, rels)

    compiled.evaluate(ctx, db, map(Array("a"), Array(path)), cursors) should equal(intValue(2))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)
  }

  test("tail function") {
    val compiled = compile(function("tail", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(list(intValue(2), intValue(3)))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toBoolean function") {
    val compiled = compile(function("toBoolean", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(Values.TRUE)), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(Values.FALSE)), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("false"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("true"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("uncertain"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toFloat function") {
    val compiled = compile(function("toFloat", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(doubleValue(3.2))), cursors) should equal(doubleValue(3.2))
    compiled.evaluate(ctx, db, map(Array("a"), Array(intValue(3))), cursors) should equal(doubleValue(3))
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("3.2"))), cursors) should equal(doubleValue(3.2))
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("three dot two"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toInteger function") {
    val compiled = compile(function("toInteger", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(doubleValue(3.2))), cursors) should equal(longValue(3))
    compiled.evaluate(ctx, db, map(Array("a"), Array(intValue(3))), cursors) should equal(intValue(3))
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("3"))), cursors) should equal(longValue(3))
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("three"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toString function") {
    val compiled = compile(function("toString", parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(doubleValue(3.2))), cursors) should equal(stringValue("3.2"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(Values.TRUE)), cursors) should equal(stringValue("true"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(pointValue(Cartesian, 0.0, 0.0))), cursors) should
      equal(stringValue("point({x: 0.0, y: 0.0, crs: 'cartesian'})"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(durationValue(Duration.ofHours(3)))), cursors) should
      equal(stringValue("PT3H"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(temporalValue(localTime(20, 0, 0, 0)))), cursors) should
      equal(stringValue("20:00:00"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    a [ParameterWrongTypeException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a"), Array(intArray(Array(1,2,3)))), cursors)
  }

  test("properties function on node") {
    val compiled = compile(function("properties", parameter("a")))
    val mapValue = map(Array("prop"), Array(longValue(42)))
    val node = nodeValue(1, EMPTY_TEXT_ARRAY, mapValue)
    when(db.nodeAsMap(1, nodeCursor, propertyCursor)).thenReturn(mapValue)

    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(mapValue)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("properties function on relationship") {
    val compiled = compile(function("properties", parameter("a")))
    val mapValue = map(Array("prop"), Array(longValue(42)))
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), mapValue)
    when(db.relationshipAsMap(43, relationshipScanCursor, propertyCursor)).thenReturn(mapValue)

    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(mapValue)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("properties function on map") {
    val compiled = compile(function("properties", parameter("a")))
    val mapValue = map(Array("prop"), Array(longValue(42)))

    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue)), cursors) should equal(mapValue)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("add numbers") {
    // Given
    val expression = add(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(52))
  }

  test("add temporals") {
    val compiled = compile(add(parameter("a"), parameter("b")))

    // temporal + duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(temporalValue(localTime(0)),
                                       durationValue(Duration.ofHours(10)))), cursors) should
      equal(localTime(10, 0, 0, 0))

    // duration + temporal
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                             temporalValue(localTime(0)))), cursors) should
      equal(localTime(10, 0, 0, 0))

    //duration + duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                              durationValue(Duration.ofHours(10)))), cursors) should
      equal(durationValue(Duration.ofHours(20)))
  }

  test("add with NO_VALUE") {
    // Given
    val expression = add(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
  }

  test("add strings") {
    // When
    val compiled = compile(add(parameter("a"), parameter("b")))

    // string1 + string2
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello "), stringValue("world"))), cursors) should
      equal(stringValue("hello world"))
    //string + other
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                              Array(stringValue("hello "), longValue(1337))), cursors) should
      equal(stringValue("hello 1337"))
    //other + string
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                              Array(longValue(1337), stringValue(" hello"))), cursors) should
      equal(stringValue("1337 hello"))

  }

  test("add arrays") {
    // Given
    val expression = add(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                       Array(longArray(Array(42, 43)),
                                            longArray(Array(44, 45)))), cursors) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))
  }

  test("list addition") {
    // When
    val compiled = compile(add(parameter("a"), parameter("b")))

    // [a1,a2 ..] + [b1,b2 ..]
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                       Array(list(longValue(42), longValue(43)),
                                             list(longValue(44), longValue(45)))), cursors) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))

    // [a1,a2 ..] + b
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                       Array(list(longValue(42), longValue(43)), longValue(44))), cursors) should
      equal(list(longValue(42), longValue(43), longValue(44)))

    // a + [b1,b2 ..]
    compiled.evaluate(ctx, db, map(Array("a", "b"),
                                       Array(longValue(43),
                                             list(longValue(44), longValue(45)))), cursors) should
      equal(list(longValue(43), longValue(44), longValue(45)))
  }

  test("unary add ") {
    // Given
    val expression = unaryAdd(literalInt(42))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(42))
  }

  test("subtract numbers") {
    // Given
    val expression = subtract(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(32))
  }

  test("subtract with NO_VALUE") {
    // Given
    val expression = subtract(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
  }

  test("subtract temporals") {
    val compiled = compile(subtract(parameter("a"), parameter("b")))

    // temporal - duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(temporalValue(localTime(20, 0, 0, 0)),
                                                              durationValue(Duration.ofHours(10)))), cursors) should
      equal(localTime(10, 0, 0, 0))

    //duration - duration
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                              durationValue(Duration.ofHours(10)))), cursors) should
      equal(durationValue(Duration.ofHours(0)))
  }

  test("unary subtract ") {
    // Given
    val expression = unarySubtract(literalInt(42))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(-42))
  }

  test("multiply function") {
    // Given
    val expression = multiply(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(420))
  }

  test("multiply with NO_VALUE") {
    // Given
    val expression = multiply(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
  }

  test("division") {
    val compiled = compile(divide(parameter("a"), parameter("b")))

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(6), longValue(3))), cursors) should equal(longValue(2))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(5), doubleValue(2))), cursors) should equal(doubleValue(2.5))
    an[ArithmeticException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(5), longValue(0))), cursors)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(doubleValue(3.0), doubleValue(0.0))), cursors) should equal(doubleValue(Double.PositiveInfinity))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(durationValue(Duration.ofHours(4)), longValue(2))), cursors) should equal(durationValue(Duration.ofHours(2)))
    an[ArithmeticException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(0))), cursors)
  }

  test("modulo") {
    val compiled = compile(modulo(parameter("a"), parameter("b")))

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(doubleValue(8.0), longValue(6))), cursors) should equal(doubleValue(2.0))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(8), doubleValue(6))), cursors) should equal(doubleValue(2.0))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(8), longValue(6))), cursors) should equal(longValue(2))
  }

  test("pow") {
    val compiled = compile(pow(parameter("a"), parameter("b")))

    // Then
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(doubleValue(2), longValue(3))), cursors) should equal(doubleValue(8.0))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(2), longValue(3))), cursors) should equal(doubleValue(8.0))
  }

  test("extract parameter") {
    compile(parameter("prop")).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(parameter("prop")).evaluate(ctx, db, map(Array("prop"), Array(stringValue("foo"))), cursors) should equal(stringValue("foo"))
    compile(parameter("    AUTOBLAH BLAH BLAHA   ")).evaluate(ctx, db, map(Array("    AUTOBLAH BLAH BLAHA   "), Array(stringValue("foo"))), cursors) should equal(stringValue("foo"))
  }

  test("extract multiple parameters with whitespaces") {
    compile(add(parameter(" A "), parameter("\tA\t")))
          .evaluate(ctx, db, map(Array(" A ", "\tA\t"), Array(longValue(1), longValue(2) )), cursors) should equal(longValue(3))
    compile(add(parameter(" A "), parameter("_A_")))
          .evaluate(ctx, db, map(Array(" A ", "_A_"), Array(longValue(1), longValue(2) )), cursors) should equal(longValue(3))
  }

  test("NULL") {
    // Given
    val expression = nullLiteral

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("TRUE") {
    // Given
    val expression = trueLiteral

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("FALSE") {
    // Given
    val expression = falseLiteral

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("OR") {
    compile(or(trueLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(falseLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(trueLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(falseLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)

    compile(or(nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(or(nullLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(trueLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(nullLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(or(falseLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("XOR") {
    compile(xor(trueLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(xor(falseLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(xor(trueLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(xor(falseLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)

    compile(xor(nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(nullLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(trueLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(nullLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(falseLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("OR should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy compile(or(literalInt(42), falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors)
    a [CypherTypeException] should be thrownBy compile(or(falseLiteral, literalInt(42))).evaluate(ctx, db, EMPTY_MAP, cursors)
    compile(or(trueLiteral, literalInt(42))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(literalInt(42), trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("OR should handle coercion") {
    val expression =  compile(or(parameter("a"), parameter("b")))
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("ORS") {
    compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, trueLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, nullLiteral, falseLiteral, falseLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(ors(falseLiteral, falseLiteral, falseLiteral, trueLiteral, nullLiteral, trueLiteral, falseLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("ORS should throw on non-boolean input") {
    val compiled = compile(ors(parameter("a"), parameter("b"), parameter("c"), parameter("d"), parameter("e")))
    val keys = Array("a", "b", "c", "d", "e")
    compiled.evaluate(ctx, db, map(keys, Array(Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE)), cursors) should equal(Values.FALSE)

    compiled.evaluate(ctx, db, map(keys, Array(Values.FALSE, Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE)), cursors) should equal(Values.TRUE)

    compiled.evaluate(ctx, db, map(keys, Array(intValue(42), Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE)), cursors) should equal(Values.TRUE)

    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, db, map(keys, Array(intValue(42), Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE)), cursors)
  }

  test("ORS should handle coercion") {
    val expression =  compile(ors(parameter("a"), parameter("b")))
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.FALSE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("AND") {
    compile(and(trueLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(and(falseLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(trueLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(falseLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)

    compile(and(nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(and(nullLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(and(trueLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(and(nullLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(falseLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("AND should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy compile(and(literalInt(42), trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors)
    a [CypherTypeException] should be thrownBy compile(and(trueLiteral, literalInt(42))).evaluate(ctx, db, EMPTY_MAP, cursors)
    compile(and(falseLiteral, literalInt(42))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(literalInt(42), falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("AND should handle coercion") {
    val expression =  compile(and(parameter("a"), parameter("b")))
   expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
   expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("ANDS") {
    compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, trueLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, nullLiteral, trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(ands(trueLiteral, trueLiteral, trueLiteral, falseLiteral, nullLiteral, falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("ANDS should throw on non-boolean input") {
    val compiled = compile(ands(parameter("a"), parameter("b"), parameter("c"), parameter("d"), parameter("e")))
    val keys = Array("a", "b", "c", "d", "e")
    compiled.evaluate(ctx, db, map(keys, Array(Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE)), cursors) should equal(Values.TRUE)

    compiled.evaluate(ctx, db, map(keys, Array(Values.TRUE, Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE)), cursors) should equal(Values.FALSE)

    compiled.evaluate(ctx, db, map(keys, Array(intValue(42), Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE)), cursors) should equal(Values.FALSE)

    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, db, map(keys, Array(intValue(42), Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE)), cursors)
  }

  test("ANDS should handle coercion") {
    val expression =  compile(ands(parameter("a"), parameter("b")))
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    expression.evaluate(ctx, db, map(Array("a", "b"), Array(Values.TRUE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("NOT") {
    compile(not(falseLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(not(trueLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(not(nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("NOT should handle coercion") {
    val expression =  compile(not(parameter("a")))
    expression.evaluate(ctx, db, map(Array("a"), Array(EMPTY_LIST)), cursors) should equal(Values.TRUE)
    expression.evaluate(ctx, db, map(Array("a"), Array(list(stringValue("hello")))), cursors) should equal(Values.FALSE)
  }

  test("EQUALS") {
    compile(equals(literalInt(42), literalInt(42))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(equals(literalInt(42), literalInt(43))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(equals(nullLiteral, literalInt(43))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(equals(literalInt(42), nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(equals(nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(equals(TRUE, equals(TRUE, equals(TRUE, nullLiteral)))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("NOT EQUALS") {
    compile(notEquals(literalInt(42), literalInt(42))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(notEquals(literalInt(42), literalInt(43))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(notEquals(nullLiteral, literalInt(43))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(notEquals(literalInt(42), nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(notEquals(nullLiteral, nullLiteral)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(notEquals(TRUE, notEquals(TRUE, notEquals(TRUE, nullLiteral)))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("regex match on literal pattern") {
    val compiled= compile(regex(parameter("a"), literalString("hell.*")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("helo"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(Values.NO_VALUE)), cursors) should equal(Values.NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(longValue(42))), cursors) should equal(Values.NO_VALUE)
  }

  test("regex match on general expression") {
    val compiled= compile(regex(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hell.*"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("helo") , stringValue("hell.*"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(Values.NO_VALUE, stringValue("hell.*"))), cursors) should equal(Values.NO_VALUE)
    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("forty-two"), longValue(42))), cursors)
    an [InvalidSemanticsException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("["))), cursors)
  }

  test("startsWith") {
    val compiled= compile(startsWith(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hell"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hi"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, stringValue("hi"))), cursors) should equal(NO_VALUE)
  }

  test("endsWith") {
    val compiled= compile(endsWith(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("ello"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hi"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, stringValue("hi"))), cursors) should equal(NO_VALUE)
  }

  test("contains") {
    val compiled= compile(contains(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("ell"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hi"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(stringValue("hello"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, stringValue("hi"))), cursors) should equal(NO_VALUE)
  }

  test("in") {
    val compiled = compile(in(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(intValue(3), list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(intValue(4), list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(intValue(3), list(intValue(1), NO_VALUE, intValue(3)))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(intValue(4), list(intValue(1), NO_VALUE, intValue(3)))), cursors) should equal(Values.NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(intValue(4), NO_VALUE)), cursors) should equal(Values.NO_VALUE)
  }

  test("should compare values using <") {
    for (left <- allValues)
      for (right <- allValues) {
        lessThan(literal(left), literal(right))  should compareUsingLessThan(left, right)
      }
  }

  test("should compare values using <=") {
    for (left <- allValues)
      for (right <- allValues) {
        lessThanOrEqual(literal(left), literal(right))  should compareUsingLessThanOrEqual(left, right)
      }
  }

  test("should compare values using >") {
    for (left <- allValues)
      for (right <- allValues) {
        greaterThan(literal(left), literal(right))  should compareUsingGreaterThan(left, right)
      }
  }

  test("should compare values using >=") {
    for (left <- allValues)
      for (right <- allValues) {
        greaterThanOrEqual(literal(left), literal(right))  should compareUsingGreaterThanOrEqual(left, right)
      }
  }

  test("isNull") {
    val compiled= compile(isNull(parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(Values.TRUE)
  }

  test("isNotNull") {
    val compiled= compile(isNotNull(parameter("a")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(Values.FALSE)
  }

  test("CoerceToPredicate") {
    val coerced = CoerceToPredicate(parameter("a"))

    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(Values.FALSE)), cursors) should equal(Values.FALSE)
    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(Values.TRUE)), cursors) should equal(Values.TRUE)
    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(list(stringValue("A")))), cursors) should equal(Values.TRUE)
    compile(coerced).evaluate(ctx, db, map(Array("a"), Array(list(EMPTY_LIST))), cursors) should equal(Values.TRUE)
  }

  test("ReferenceFromSlot") {
    // Given
    val offset = 1337
    val expression = ReferenceFromSlot(offset, "foo")
    when(ctx.getRefAt(offset)).thenReturn(stringValue("hello"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("IdFromSlot") {
    // Given
    val offset = 1337
    val expression = IdFromSlot(offset)
    when(ctx.getLongAt(offset)).thenReturn(42L)

    // When
    val compiled = compile(expression, SlotConfiguration(Map("a" -> LongSlot(offset, nullable = false, symbols.CTNode)), 1, 0))

    // Then
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(longValue(42))
  }

  test("PrimitiveEquals") {
    val compiled = compile(PrimitiveEquals(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), longValue(42))), cursors) should
      equal(Values.TRUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(longValue(42), longValue(1337))), cursors) should
      equal(Values.FALSE)
  }

  test("NullCheck") {
    val nullOffset = 1337
    val offset = 42
    when(ctx.getLongAt(nullOffset)).thenReturn(-1L)
    when(ctx.getLongAt(offset)).thenReturn(42L)

    compile(NullCheck(nullOffset, literalFloat(PI))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(NullCheck(offset, literalFloat(PI))).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.PI)
  }

  test("NullCheckVariable") {
    val nullOffset = 1337
    val offset = 42
    when(ctx.getLongAt(nullOffset)).thenReturn(-1L)
    when(ctx.getLongAt(offset)).thenReturn(42L)
    when(ctx.getRefAt(nullOffset)).thenReturn(NO_VALUE)
    when(ctx.getRefAt(offset)).thenReturn(stringValue("hello"))

    compile(NullCheckVariable(nullOffset, ReferenceFromSlot(offset, "a"))).evaluate(ctx, db, EMPTY_MAP, cursors) should
      equal(Values.NO_VALUE)
    compile(NullCheckVariable(offset, ReferenceFromSlot(offset, "a"))).evaluate(ctx, db, EMPTY_MAP, cursors) should
      equal(stringValue("hello"))
  }

  test("IsPrimitiveNull") {
    val nullOffset = 1337
    val offset = 42
    when(ctx.getLongAt(nullOffset)).thenReturn(-1L)
    when(ctx.getLongAt(offset)).thenReturn(77L)

    compile(IsPrimitiveNull(nullOffset)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(IsPrimitiveNull(offset)).evaluate(ctx, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("containerIndex on node") {
    val node =  nodeValue(1, EMPTY_TEXT_ARRAY, map(Array("prop"), Array(stringValue("hello"))))
    when(db.propertyKey("prop")).thenReturn(42)
    when(db.nodeProperty(1, 42, nodeCursor, propertyCursor)).thenReturn(stringValue("hello"))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("containerIndex on relationship") {
    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), map(Array("prop"), Array(stringValue("hello"))))
    when(db.propertyKey("prop")).thenReturn(42)
    when(db.relationshipProperty(43, 42, relationshipScanCursor, propertyCursor)).thenReturn(stringValue("hello"))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("containerIndex on map") {
    val mapValue = map(Array("prop"), Array(stringValue("hello")))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, db, map(Array("a"), Array(mapValue)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("containerIndex on list") {
    val listValue = list(longValue(42), stringValue("hello"), intValue(42))
    val compiled = compile(containerIndex(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(listValue, intValue(1))), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(listValue, intValue(-1))), cursors) should equal(intValue(42))
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(listValue, intValue(3))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
    an [InvalidArgumentException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a", "b"), Array(listValue, longValue(Int.MaxValue + 1L))), cursors)
  }

  test("handle list literals") {
    val literal = literalList(trueLiteral, literalInt(5), nullLiteral, falseLiteral)

    val compiled = compile(literal)

    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(list(Values.TRUE, intValue(5), NO_VALUE, Values.FALSE))
  }

  test("handle map literals") {
    val literal = literalIntMap("foo" -> 1, "bar" -> 2, "baz" -> 3)

    val compiled = compile(literal)

    import scala.collection.JavaConverters._
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(ValueUtils.asMapValue(Map("foo" -> 1, "bar" -> 2, "baz" -> 3).asInstanceOf[Map[String, AnyRef]].asJava))
  }

  test("handle map literals with null") {
    val literal = literalMap("foo" -> literalInt(1), "bar" -> nullLiteral, "baz" -> literalString("three"))

    val compiled = compile(literal)

    import scala.collection.JavaConverters._
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(ValueUtils.asMapValue(Map("foo" -> 1, "bar" -> null, "baz" -> "three").asInstanceOf[Map[String, AnyRef]].asJava))
  }

  test("handle empty map literals") {
    val literal = literalMap()

    val compiled = compile(literal)

    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(EMPTY_MAP)
  }

  test("from slice") {
    val slice = compile(sliceFrom(parameter("a"), parameter("b")))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    slice.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(3))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, NO_VALUE)), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, intValue(2))), cursors) should equal(VirtualValues.list(intValue(3)))
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, intValue(-2))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, intValue(0))), cursors) should equal(list)
  }

  test("to slice") {
    val slice = compile(sliceTo(parameter("a"), parameter("b")))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    slice.evaluate(ctx, db, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, NO_VALUE)), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, intValue(2))), cursors) should equal(VirtualValues.list(intValue(1), intValue(2)))
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, intValue(-2))), cursors) should equal(VirtualValues.list(intValue(1)))
    slice.evaluate(ctx, db, map(Array("a", "b"), Array(list, intValue(0))), cursors) should equal(EMPTY_LIST)
  }

  test("full slice") {
    val slice = compile(sliceFull(parameter("a"), parameter("b"), parameter("c")))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3), intValue(4), intValue(5))

    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(NO_VALUE, intValue(1), intValue(3))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, NO_VALUE, intValue(3))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, intValue(3), NO_VALUE)), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, intValue(1), intValue(3))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, intValue(1), intValue(-2))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, intValue(-4), intValue(3))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, intValue(-4), intValue(-2))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, db, map(Array("a", "b", "c"), Array(list, intValue(0), intValue(0))), cursors) should equal(EMPTY_LIST)
  }

  test("handle variables") {
    val variable = varFor("key")
    val compiled = compile(variable)
    when(ctx.getByName("key")).thenReturn(stringValue("hello"))
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("handle variables with whitespace ") {
    val varName = "   k\te\ty   "
    val variable = varFor(varName)
    val compiled = compile(variable)
    when(ctx.getByName(varName)).thenReturn(stringValue("hello"))
    compiled.evaluate(ctx, db, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("coerceTo tests") {
    //numbers
    coerce(longValue(42), symbols.CTAny) should equal(longValue(42))
    coerce(longValue(42), symbols.CTInteger) should equal(longValue(42))
    coerce(longValue(42), symbols.CTFloat) should equal(doubleValue(42))
    coerce(longValue(42), symbols.CTNumber) should equal(longValue(42))
    coerce(doubleValue(2.1), symbols.CTAny) should equal(doubleValue(2.1))
    coerce(doubleValue(2.1), symbols.CTInteger) should equal(longValue(2))
    coerce(doubleValue(2.1), symbols.CTFloat) should equal(doubleValue(2.1))
    coerce(doubleValue(2.1), symbols.CTNumber) should equal(doubleValue(2.1))
    //misc
    coerce(Values.TRUE, symbols.CTBoolean) should equal(Values.TRUE)
    coerce(Values.FALSE, symbols.CTBoolean) should equal(Values.FALSE)
    coerce(stringValue("hello"), symbols.CTString) should equal(stringValue("hello"))
    coerce(pointValue(Cartesian, 0.0, 0.0), symbols.CTPoint) should equal(pointValue(Cartesian, 0.0, 0.0))
    coerce(pointValue(Cartesian, 0.0, 0.0), symbols.CTGeometry) should equal(pointValue(Cartesian, 0.0, 0.0))

    //date and time
    case class Generator(generator: Clock => AnyValue, ct: CypherType)
    val generators: List[Generator] =
      List(Generator(DateValue.now, symbols.CTDate),
           Generator(TimeValue.now, symbols.CTTime),
           Generator(LocalTimeValue.now, symbols.CTLocalTime),
           Generator(DateTimeValue.now, symbols.CTDateTime),
           Generator(LocalDateTimeValue.now, symbols.CTLocalDateTime))

    generators.foreach{ generator =>
      val now = generator.generator(Clock.systemUTC())
      coerce(now, generator.ct) should equal(now)
    }
    coerce(durationValue(Duration.ofHours(3)), symbols.CTDuration) should equal(durationValue(Duration.ofHours(3)))

    //nodes, rels, path
    coerce(node(42), symbols.CTNode) should equal(node(42))
    coerce(relationship(42), symbols.CTRelationship) should equal(relationship(42))
    coerce(path(5), symbols.CTPath) should equal(path(5))

    //maps
    val mapValue = map(Array("prop"), Array(longValue(1337)))
    when(db.nodeAsMap(42, nodeCursor, propertyCursor)).thenReturn(mapValue)
    when(db.relationshipAsMap(42, relationshipScanCursor, propertyCursor)).thenReturn(mapValue)
    coerce(mapValue, symbols.CTMap) should equal(mapValue)
    coerce(node(42, mapValue), symbols.CTMap) should equal(mapValue)
    coerce(relationship(42, mapValue), symbols.CTMap) should equal(mapValue)

    //list
    coerce(list(longValue(42), longValue(43)), ListType(symbols.CTAny)) should equal(list(longValue(42), longValue(43)))
    coerce(path(7), ListType(symbols.CTAny)) should equal(path(7).asList())
    coerce(list(node(42), node(43)), ListType(symbols.CTNode)) should equal(list(node(42), node(43)))
    coerce(list(relationship(42), relationship(43)), ListType(symbols.CTRelationship)) should equal(list(relationship(42), relationship(43)))
    coerce(list(doubleValue(1.2), longValue(2), doubleValue(3.1)),
           ListType(symbols.CTInteger)) should equal(list(longValue(1), longValue(2), longValue(3)))
    coerce(list(doubleValue(1.2), longValue(2), doubleValue(3.1)),
           ListType(symbols.CTFloat)) should equal(list(doubleValue(1.2), doubleValue(2), doubleValue(3.1)))
    coerce(list(list(doubleValue(1.2), longValue(2)), list(doubleValue(3.1))),
           ListType(ListType(symbols.CTInteger))) should equal(list(list(longValue(1), longValue(2)), list(longValue(3))))
    coerce(list(longValue(42), NO_VALUE, longValue(43)), ListType(symbols.CTInteger)) should equal(
      list(longValue(42), NO_VALUE, longValue(43)))

    a [CypherTypeException] should be thrownBy coerce(path(11), ListType(symbols.CTNode))
    a [CypherTypeException] should be thrownBy coerce(path(11), ListType(symbols.CTRelationship))
  }

  test("coerceTo list happy path") {
    types.foreach {
      case (v, typ) =>
        coerce(list(v), ListType(typ)) should equal(list(v))
        coerce(list(list(v)), ListType(ListType(typ))) should equal(list(list(v)))
        coerce(list(list(list(v))), ListType(ListType(ListType(typ)))) should equal(list(list(list(v))))
    }
  }

  test("coerceTo unhappy path") {
    for {value <- types.keys
          typ <- types.values} {
      if (types(value) == typ) coerce(value, typ) should equal(value)
      else a [CypherTypeException] should be thrownBy coerce(value, typ)
    }
  }

  test("access property on node") {
    val compiled = compile(property(parameter("a"), "prop"))

    val node = nodeValue(1, EMPTY_TEXT_ARRAY, map(Array("prop"), Array(stringValue("hello"))))
    when(db.propertyKey("prop")).thenReturn(42)
    when(db.nodeProperty(1, 42, nodeCursor, propertyCursor)).thenReturn(stringValue("hello"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(node)), cursors) should equal(stringValue("hello"))
  }

  test("access property on relationship") {
    val compiled = compile(property(parameter("a"), "prop"))

    val rel = relationshipValue(43,
                                nodeValue(1, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                nodeValue(2, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                stringValue("R"), map(Array("prop"), Array(stringValue("hello"))))
    when(db.propertyKey("prop")).thenReturn(42)
    when(db.relationshipProperty(43, 42, relationshipScanCursor, propertyCursor)).thenReturn(stringValue("hello"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(rel)), cursors) should equal(stringValue("hello"))
  }

  test("access property on map") {
    val compiled = compile(property(parameter("a"), "prop"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(map(Array("prop"), Array(stringValue("hello"))))), cursors) should equal(stringValue("hello"))
  }

  test("access property on temporal") {
    val value = TimeValue.now(Clock.systemUTC())
    val compiled = compile(property(parameter("a"), "timezone"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(value)), cursors) should equal(value.get("timezone"))
  }

  test("access property on duration") {
    val value = durationValue(Duration.ofHours(3))
    val compiled = compile(property(parameter("a"), "seconds"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(value)), cursors) should equal(value.get("seconds"))
  }

  test("access property on point") {
    val value = pointValue(Cartesian, 1.0, 3.6)

    val compiled = compile(property(parameter("a"), "x"))

    compiled.evaluate(ctx, db, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, db, map(Array("a"), Array(value)), cursors) should equal(doubleValue(1.0))
  }

  test("access property on point with invalid key") {
    val value = pointValue(Cartesian, 1.0, 3.6)

    val compiled = compile(property(parameter("a"), "foobar"))

    an[InvalidArgumentException] should be thrownBy compiled.evaluate(ctx, db, map(Array("a"), Array(value)), cursors)
  }

  test("should project") {
    //given
    val context = mock[ExecutionContext]
    val projections = Map(0 -> literal("hello"), 1 -> function("sin", parameter("param")))
    val compiled = compileProjection(projections)

    //when
    compiled.project(context, db, map(Array("param"), Array(NO_VALUE)), cursors)

    //then
    verify(context).setRefAt(0, stringValue("hello"))
    verify(context).setRefAt(1, NO_VALUE)
    verifyNoMoreInteractions(context)
  }

  test("single in list function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When
    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "b")
    val compiledNone = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("b"))))
    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aaa")
    val compiledSingle = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("aaa"))))
    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "a")
    val compiledMany = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("a"))))

    //Then
    compiledNone.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
    compiledSingle.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledMany.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("single in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map(
      "b" -> stringValue("b"),
      "a" -> stringValue("a"),
      "aaa" -> stringValue("aaa")))

    //When
    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "b")
    val compiledNone = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("b"))))
    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aaa")
    val compiledSingle = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("aaa"))))
    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "a")
    val compiledMany = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("a"))))

    //Then
    compiledNone.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
    compiledSingle.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledMany.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("single in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, single(bar IN null WHERE bar = foo)
    val compiled = compile(singleInList("bar", nullLiteral,
                                        equals(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("single in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, single(bar IN ['a','aa','aaa'] WHERE bar = null)
    val compiled = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("single function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When, single(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(singleInList("bar", varFor("foo"),
                                      equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("single function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When, single(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(singleInList("bar", parameter("a"),
                                      equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(Values.TRUE)
  }

  test("single on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, single(bar IN [] WHERE bar = 42)
    val compiled = compile(singleInList("bar", literalList(), equals(literalInt(42), varFor("bar"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }


  test("none in list function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When
    // none(bar IN ["a", "aa", "aaa"] WHERE bar = "b")
    val compiledTrue = compile(noneInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), literalString("b"))))
    // none(bar IN ["a", "aa", "aaa"] WHERE bar = "a")
    val compiledFalse = compile(noneInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), literalString("a"))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("none in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("a" -> stringValue("a"), "b" -> stringValue("b")))

    //When
    // none(bar IN ["a", "aa", "aaa"] WHERE bar = b)
    val compiledTrue = compile(noneInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), varFor("b"))))
    // none(bar IN ["a", "aa", "aaa"] WHERE bar = a)
    val compiledFalse = compile(noneInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), varFor("a"))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("none in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, none(bar IN null WHERE bar = foo)
    val compiled = compile(noneInList("bar", nullLiteral,
                                      equals(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("none in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, none(bar IN null WHERE bar = null)
    val compiled = compile(noneInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), nullLiteral )))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("none function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  none(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(noneInList("bar", varFor("foo"),
                                  equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(FALSE)
  }

  test("none function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  none(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(noneInList("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(FALSE)
  }

  test("none on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, none(bar IN [] WHERE bar = 42)
    val compiled = compile(noneInList("bar", literalList(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("any in list function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When
    // any(bar IN ["a", "aa", "aaa"] WHERE bar = "a")
    val compiledTrue = compile(anyInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), literalString("a"))))
    // any(bar IN ["a", "aa", "aaa"] WHERE bar = "b")
    val compiledFalse = compile(anyInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), literalString("b"))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("any in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("a" -> stringValue("a"), "b" -> stringValue("b")))

    //When
    // any(bar IN ["a", "aa", "aaa"] WHERE bar = a)
    val compiledTrue = compile(anyInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), varFor("a"))))
    // any(bar IN ["a", "aa", "aaa"] WHERE bar = aa)
    val compiledFalse = compile(anyInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), varFor("b"))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("any in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, any(bar IN null WHERE bar = foo)
    val compiled = compile(anyInList("bar", nullLiteral,
                                     equals(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("any in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, any(bar IN ['a','aa','aaa'] WHERE bar = null)
    val compiled = compile(anyInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("any function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  any(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(anyInList("bar", varFor("foo"),
                                  equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("any function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  any(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(anyInList("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(Values.TRUE)
  }

  test("any on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, any(bar IN [] WHERE bar = 42)
    val compiled = compile(anyInList("bar", literalList(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("all in list function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When
    // all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "a")
    val compiledTrue = compile(allInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("a"))))
    //all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aa")
    val compiledFalse = compile(allInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("aa"))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("all in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("a" -> stringValue("a"), "aa" -> stringValue("aa")))

    //When
    // all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH a)
    val compiledTrue = compile(allInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("a"))))
    //all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH aa)
    val compiledFalse = compile(allInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("aa"))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("all in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, all(bar IN null WHERE bar STARTS WITH foo)
    val compiled = compile(allInList("bar", nullLiteral,
                                     startsWith(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("all in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, all(bar IN null WHERE bar STARTS WITH null)
    val compiled = compile(allInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("all function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When, all(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(allInList("bar", varFor("foo"),
                                  equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(FALSE)
  }

  test("all function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  all(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(allInList("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(Values.FALSE)
  }

  test("all on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, all(bar IN [] WHERE bar = 42)
    val compiled = compile(allInList("bar", literalList(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("filter function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aa")
    val compiled = compile(filter("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("aa"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("filter function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> stringValue("aa")))

    //When, filter(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH foo)
    val compiled = compile(filter("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("filter on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN null WHERE bar STARTS WITH 'aa')
    val compiled = compile(filter("bar", nullLiteral,
                                  startsWith(varFor("bar"), varFor("aa"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("filter with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN null WHERE bar STARTS WITH null)
    val compiled = compile(filter("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list())
  }

  test("filter function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  filter(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(filter("bar", varFor("foo"),
                                   equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(VirtualValues.list(stringValue("aaa")))
  }

  test("filter function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  filter(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(filter("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(VirtualValues.list(stringValue("aaa")))
  }

  test("filter on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN [] WHERE bar = 42)
    val compiled = compile(filter("bar", literalList(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(EMPTY_LIST)
  }

  test("nested list expressions local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When
    // none(bar IN ["a"] WHERE any(foo IN ["b"] WHERE bar = foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = "bar",
        collection = listOf(literalString("a")),
        predicate = anyInList(
          variable = "foo",
          collection = listOf(literalString("b")),
          predicate = equals(varFor("bar"), varFor("foo")))))
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = "bar",
        collection = listOf(literalString("a")),
        predicate = anyInList(
          variable = "foo",
          collection = listOf(literalString("a")),
          predicate = equals(varFor("bar"), varFor("foo")))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("nested list expressions, outer expression accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("list" -> list(stringValue("a"))))

    //When
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar <> foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = "bar",
        collection = varFor("list"),
        predicate = anyInList(
          variable = "foo",
          collection = listOf(literalString("a")),
          predicate = notEquals(varFor("bar"), varFor("foo")))))
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = "bar",
        collection = varFor("list"),
        predicate = anyInList(
          variable = "foo",
          collection = listOf(literalString("a")),
          predicate = equals(varFor("bar"), varFor("foo")))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("nested list expressions, inner expression accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("list" -> list(stringValue("a"))))

    //When
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar <> foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = "bar",
        collection = listOf(literalString("a")),
        predicate = anyInList(
          variable = "foo",
          collection = listOf(literalString("a")),
          predicate = notEquals(varFor("bar"), varFor("foo")))))
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = "bar",
        collection = varFor("list"),
        predicate = anyInList(
          variable = "foo",
          collection = varFor("list"),
          predicate = equals(varFor("bar"), varFor("foo")))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("nested list expressions, both accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("list" -> list(stringValue("a"))))

    //When
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar <> foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = "bar",
        collection = varFor("list"),
        predicate = anyInList(
          variable = "foo",
          collection = varFor("list"),
          predicate = notEquals(varFor("bar"), varFor("foo")))))
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = "bar",
        collection = varFor("list"),
        predicate = anyInList(
          variable = "foo",
          collection = varFor("list"),
          predicate = equals(varFor("bar"), varFor("foo")))))

    //Then
    compiledTrue.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, db, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("extract function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, extract(bar IN ["a", "aa", "aaa"] | size(bar))
    val compiled = compile(extract("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                   function("size", varFor("bar"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(intValue(1), intValue(2), intValue(3)))
  }

  test("extract function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> intValue(10)), mutable.Map.empty)

    //When, extract(bar IN [1, 2, 3] | bar + foo)
    val compiled = compile(extract("bar", listOf(literalInt(1), literalInt(2), literalInt(3)),
                                   add(varFor("foo"), varFor("bar"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(intValue(11), intValue(12), intValue(13)))
  }

  test("extract on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, extract(bar IN null | size(bar)
    val compiled = compile(extract("bar", nullLiteral,
                                   function("size", varFor("bar"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("extract function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(intValue(1), intValue(2), intValue(3))), mutable.Map.empty)

    //When, extract(bar IN foo | size(foo)
    val compiled = compile(extract("bar", varFor("foo"),
                                  function("size", varFor("foo"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(VirtualValues.list(intValue(3), intValue(3), intValue(3)))
  }

  test("extract function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    //When, extract(bar IN $a | size($a)
    val compiled = compile(extract("bar", parameter("a"),
                                   function("size", parameter("a"))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(VirtualValues.list(intValue(3), intValue(3), intValue(3)))
  }

  test("extract on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, extaract(bar IN [] | bar = 42)
    val compiled = compile(extract("bar", literalList(), literalInt(42)))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(EMPTY_LIST)
  }


  test("reduce function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, reduce(count = 0, bar IN ["a", "aa", "aaa"] | count + size(bar))
    val compiled = compile(reduce("count", literalInt(0), "bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                   add(function("size", varFor("bar")), varFor("count"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(intValue(6))
  }

  test("reduce function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> intValue(10)), mutable.Map.empty)

    //When, reduce(count = 0, bar IN [1, 2, 3] | count + bar + foo)
    val compiled = compile(reduce("count", literalInt(0),  "bar", listOf(literalInt(1), literalInt(2), literalInt(3)),
                                   add(add(varFor("foo"), varFor("bar")), varFor("count"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(intValue(36))
  }

  test("reduce on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, reduce(count = 0, bar IN null | count + size(bar))
    val compiled = compile(reduce("count", literalInt(0), "bar", nullLiteral,
                                  add(function("size", varFor("bar")), varFor("count"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("reduce function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(intValue(1), intValue(2), intValue(3))), mutable.Map.empty)

    //When, reduce(count = 0, bar IN foo | count + size(foo)
    val compiled = compile(reduce("count", literalInt(0), "bar", varFor("foo"),
                                  add(function("size", varFor("foo")), varFor("count"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(intValue(9))
  }

  test("reduce function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    //When, reduce(count = 0, bar IN $a | count + size($a))
    val compiled = compile(reduce("count", literalInt(0), "bar", parameter("a"),
                                  add(function("size", parameter("a")), varFor("count"))))

    //Then
    compiled.evaluate(context, db, map(Array("a"), Array(list)), cursors) should equal(intValue(9))
  }

  test("reduce on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, reduce(count = 42, bar IN [] | count + 3)
    val compiled = compile(reduce("count", literalInt(42), "bar", literalList(),
                                  add(literalInt(3), varFor("count"))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(Values.intValue(42))
  }

  test("list comprehension with predicate and extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH 'aa' | bar + 'A']
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = Some(startsWith(varFor("bar"), literalString("aa"))),
                                             extractExpression = Some(add(varFor("bar"), literalString("A")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(stringValue("aaA"), stringValue("aaaA")))
  }

  test("list comprehension with no predicate but an extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"] | bar + 'A']
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = None,
                                             extractExpression = Some(add(varFor("bar"), literalString("A")))))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(stringValue("aA"), stringValue("aaA"), stringValue("aaaA")))
  }

  test("list comprehension with predicate but no extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH 'aa']
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = Some(startsWith(varFor("bar"), literalString("aa"))),
                                             extractExpression = None))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("list comprehension with no predicate nor extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"]]
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = None,
                                             extractExpression = None))

    //Then
    compiled.evaluate(context, db, EMPTY_MAP, cursors) should equal(list(stringValue("a"), stringValue("aa"), stringValue("aaa")))
  }

  test("simple case expressions") {
    val alts = List(literalInt(42) -> literalString("42"), literalInt(1337) -> literalString("1337"))

    compile(simpleCase(parameter("a"), alts))
      .evaluate(ctx, db, parameters("a" -> intValue(42)), cursors) should equal(stringValue("42"))
    compile(simpleCase(parameter("a"), alts))
      .evaluate(ctx, db, parameters("a" -> intValue(1337)), cursors) should equal(stringValue("1337"))
    compile(simpleCase(parameter("a"), alts))
      .evaluate(ctx, db, parameters("a" -> intValue(-1)), cursors) should equal(NO_VALUE)
    compile(simpleCase(parameter("a"), alts, Some(literalString("THIS IS THE DEFAULT"))))
      .evaluate(ctx, db, parameters("a" -> intValue(-1)), cursors) should equal(stringValue("THIS IS THE DEFAULT"))
  }

  test("generic case expressions") {
    compile(genericCase(List(falseLiteral -> literalString("no"), trueLiteral -> literalString("yes"))))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(stringValue("yes"))
    compile(genericCase(List(trueLiteral -> literalString("no"), falseLiteral -> literalString("yes"))))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(stringValue("no"))
    compile(genericCase(List(falseLiteral -> literalString("no"), falseLiteral -> literalString("yes"))))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(genericCase(List(falseLiteral -> literalString("no"), falseLiteral -> literalString("yes")), Some(literalString("default"))))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(stringValue("default"))
  }

  test("map projection node with map context") {
      val propertyMap = map(Array("prop"), Array(stringValue("hello")))
      val node = nodeValue(1, EMPTY_TEXT_ARRAY, propertyMap)
      when(ctx.getByName("n")).thenReturn(node)
      when(db.nodeAsMap(any[Long], any[NodeCursor], any[PropertyCursor])).thenReturn(propertyMap)

      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")))
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")))
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection node from long slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 32
    val nodeId = 11
    val node = nodeValue(nodeId, EMPTY_TEXT_ARRAY, propertyMap)
    when(ctx.getLongAt(offset)).thenReturn(nodeId)
    when(db.nodeById(nodeId)).thenReturn(node)
    when(db.nodeAsMap(any[Long], any[NodeCursor], any[PropertyCursor])).thenReturn(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> LongSlot(offset, nullable, symbols.CTNode)), 1, 0)
      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection node from ref slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 32
    val nodeId = 11
    val node = nodeValue(nodeId, EMPTY_TEXT_ARRAY, propertyMap)
    when(ctx.getRefAt(offset)).thenReturn(node)
    when(db.nodeAsMap(any[Long], any[NodeCursor], any[PropertyCursor])).thenReturn(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> RefSlot(offset, nullable, symbols.CTNode)), 0, 1)
      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection relationship with map context") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val relationship = relationshipValue(1, nodeValue(11, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                         nodeValue(12, EMPTY_TEXT_ARRAY, EMPTY_MAP), stringValue("R"), propertyMap)
    when(ctx.getByName("r")).thenReturn(relationship)
    when(db.relationshipAsMap(any[Long], any[RelationshipScanCursor], any[PropertyCursor])).thenReturn(propertyMap)

    compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
    compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection relationship from long slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 32
    val relationshipId = 1337
    val relationship = relationshipValue(relationshipId, nodeValue(11, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                         nodeValue(12, EMPTY_TEXT_ARRAY, EMPTY_MAP), stringValue("R"), propertyMap)
    when(ctx.getLongAt(offset)).thenReturn(relationshipId)
    when(db.relationshipById(relationshipId)).thenReturn(relationship)
    when(db.relationshipAsMap(any[Long], any[RelationshipScanCursor], any[PropertyCursor])).thenReturn(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("r" -> LongSlot(offset, nullable, symbols.CTRelationship)), 1, 0)
      compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection relationship from ref slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 32
    val relationshipId = 1337
    val relationship = relationshipValue(relationshipId, nodeValue(11, EMPTY_TEXT_ARRAY, EMPTY_MAP),
                                         nodeValue(12, EMPTY_TEXT_ARRAY, EMPTY_MAP), stringValue("R"), propertyMap)
    when(ctx.getRefAt(offset)).thenReturn(relationship)
    when(db.relationshipAsMap(any[Long], any[RelationshipScanCursor], any[PropertyCursor])).thenReturn(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("r" -> RefSlot(offset, nullable, symbols.CTRelationship)), 0, 1)
      compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection mapValue with map context") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    when(ctx.getByName("map")).thenReturn(propertyMap)

    compile(mapProjection("map", includeAllProps = true, "foo" -> literalString("projected")))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
    compile(mapProjection("map", includeAllProps = false, "foo" -> literalString("projected")))
      .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection mapValue from ref slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 32
    when(ctx.getRefAt(offset)).thenReturn(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> RefSlot(offset, nullable, symbols.CTMap)), 0, 1)
      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(ctx, db, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("call function by id") {
    // given
    val access = mock[DbAccess]
    val udf = callByName(signature(qualifiedName("foo"), Some(42)), literalString("hello"))
    when(access.callFunction(anyInt(), any[Array[AnyValue]], any[Array[String]])).thenAnswer(new Answer[AnyValue] {
      override def answer(invocationOnMock: InvocationOnMock): AnyValue = {
        invocationOnMock.getArgument[Int](0) should equal(42)
        invocationOnMock.getArgument[Array[AnyValue]](1).toList should equal(List(stringValue("hello")))
        stringValue("success")
      }
    })

    //then
    compile(udf).evaluate(ctx, access, EMPTY_MAP, cursors) should equal(stringValue("success"))
  }

  test("call function by id with default argument") {
    // given
    val access = mock[DbAccess]
    val udf = callByName(
      signature(qualifiedName("foo"), id = Some(42), field = fieldSignature("in", default = Some("I am default"))))
    when(access.callFunction(anyInt(), any[Array[AnyValue]], any[Array[String]])).thenAnswer(new Answer[AnyValue] {
      override def answer(invocationOnMock: InvocationOnMock): AnyValue = {
        invocationOnMock.getArgument[Int](0) should equal(42)
        invocationOnMock.getArgument[Array[AnyValue]](1).toList should equal(List(stringValue("I am default")))
        stringValue("success")
      }
    })

    //then
    compile(udf).evaluate(ctx, access, EMPTY_MAP, cursors) should equal(stringValue("success"))
  }

  test("call function by name") {
    // given
    val access = mock[DbAccess]
    val udf = callByName(signature(qualifiedName("foo")), literalString("hello"))
    when(access.callFunction(any[KernelQualifiedName], any[Array[AnyValue]], any[Array[String]])).thenAnswer(new Answer[AnyValue] {
      override def answer(invocationOnMock: InvocationOnMock): AnyValue = {
        invocationOnMock.getArgument[KernelQualifiedName](0).name() should equal("foo")
        invocationOnMock.getArgument[Array[AnyValue]](1).toList should equal(List(stringValue("hello")))
        stringValue("success")
      }
    })

    //then
    compile(udf).evaluate(ctx, access, EMPTY_MAP, cursors) should equal(stringValue("success"))
  }

  test("call function by name with default argument") {
    // given
    val access = mock[DbAccess]
    val udf = callByName(signature(qualifiedName("foo"), field = fieldSignature("in", default = Some("I am default"))))
    when(access.callFunction(any[KernelQualifiedName], any[Array[AnyValue]], any[Array[String]])).thenAnswer(new Answer[AnyValue] {
      override def answer(invocationOnMock: InvocationOnMock): AnyValue = {
        invocationOnMock.getArgument[KernelQualifiedName](0).name() should equal("foo")
        invocationOnMock.getArgument[Array[AnyValue]](1).toList should equal(List(stringValue("I am default")))
        stringValue("success")
      }
    })

    //then
    compile(udf).evaluate(ctx, access, EMPTY_MAP, cursors) should equal(stringValue("success"))
  }

  test("should compile grouping key with single expression") {
    //given
    val context = mock[ExecutionContext]
    val projections: Map[Slot, Expression] = Map(RefSlot(0, nullable = false, symbols.CTAny) -> literal("hello"))
    val compiled: CompiledGroupingExpression = compileGroupingExpression(projections)

    //when
    val key = compiled.computeGroupingKey(context, db, EMPTY_MAP, cursors)
    compiled.projectGroupingKey(context, key)

    //then
    key should equal(stringValue("hello"))
    verify(context).setRefAt(0, stringValue("hello"))
  }

  test("should compile grouping key with multiple expressions") {
    //given
    val context = mock[ExecutionContext]
    val nodeId = 1337
    val nodeValue = node(nodeId)
    when(context.getLongAt(0)).thenReturn(nodeId)
    when(db.nodeById(nodeId)).thenReturn(nodeValue)
    val projections: Map[Slot, Expression] = Map(RefSlot(0, nullable = false, symbols.CTAny) -> literal("hello"),
                                                 LongSlot(1, nullable = false, symbols.CTNode) -> NodeFromSlot(0, "node"))
    val compiled: CompiledGroupingExpression = compileGroupingExpression(projections)

    //when
    val key = compiled.computeGroupingKey(context, db, EMPTY_MAP, cursors)
    compiled.projectGroupingKey(context, key)

    //then
    key should equal(VirtualValues.list(stringValue("hello"), nodeValue))
    verify(context).setRefAt(0, stringValue("hello"))
    verify(context).setLongAt(1, nodeId)
  }

  test("should compile grouping key with multiple expressions all primitive") {
    //given
    val context = mock[ExecutionContext]
    val nodeId = 1337
    val relId = 42
    val relationshipValue = relationship(relId)
    val nodeValue = node(nodeId)
    when(context.getLongAt(0)).thenReturn(relId)
    when(context.getLongAt(1)).thenReturn(nodeId)
    when(db.nodeById(nodeId)).thenReturn(nodeValue)
    when(db.relationshipById(relId)).thenReturn(relationshipValue)
    val projections: Map[Slot, Expression] = Map(LongSlot(42, nullable = false, symbols.CTRelationship) -> RelationshipFromSlot(0, "relationship"),
                                                 LongSlot(47, nullable = false, symbols.CTNode) -> NodeFromSlot(1, "node"))
    val compiled: CompiledGroupingExpression = compileGroupingExpression(projections)

    //when
    val key = compiled.computeGroupingKey(context, db, EMPTY_MAP, cursors)
    compiled.projectGroupingKey(context, key)

    //then
    key should equal(VirtualValues.list( relationshipValue, nodeValue))
    verify(context).setLongAt(42, relId)
    verify(context).setLongAt(47, nodeId)
  }

  test("single outgoing path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 2)
    val r = RelAt(relationship(1337, n1.node, n2.node), 1)
    addNodes(context, dbAccess, n1, n2)
    addRelationships(context, dbAccess, r)

    //when
    //p = (n1)-[r]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(1, "r"), OUTGOING, Some(NodeFromSlot(2, "n2")),
                                                                   NilPathStep)))
    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("single-node path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    addNodes(context, dbAccess, n1)

    //when
    //p = (n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"), NilPathStep))

    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node), Array.empty))
  }

  test("single incoming path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 2)
    val r = RelAt(relationship(1337, n2.node, n1.node), 1)
    addNodes(context, dbAccess, n1, n2)
    addRelationships(context, dbAccess, r)

    //when
    //p = (n1)<-[r]-(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(1, "r"),
                                                                   INCOMING, Some(NodeFromSlot(2, "n2")), NilPathStep)))

    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("single undirected path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 2)
    val r = RelAt(relationship(1337, n1.node, n2.node), 1)
    addNodes(context, dbAccess, n1, n2)
    addRelationships(context, dbAccess, r)

    //when
    val p1 = compile(pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                                 SingleRelationshipPathStep(RelationshipFromSlot(1, "r"),
                                                                            BOTH, Some(NodeFromSlot(2, "n2")),
                                                                            NilPathStep))))
    val p2 = compile(pathExpression(NodePathStep(NodeFromSlot(2, "n2"),
                                                 SingleRelationshipPathStep(RelationshipFromSlot(1, "r"),
                                                                            BOTH, Some(NodeFromSlot(0, "n1")),
                                                                            NilPathStep))))

    //then
    p1.evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
    p2.evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n2.node, n1.node), Array(r.rel)))
  }

  test("single path with NO_VALUE") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 2)
    val slots = SlotConfiguration(Map("r" -> LongSlot(1, nullable = true, symbols.CTRelationship)), 1, 0)

    when(context.getLongAt(1)).thenReturn(-1L)
    addNodes(context, dbAccess, n1, n2)

    //when
    //p = (n1)-[r]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(1, "r"),
                                                                   OUTGOING,  Some(NodeFromSlot(2, "n2")), NilPathStep)))
    //then
    compile(p, slots).evaluate(context, dbAccess, EMPTY_MAP, cursors) should be(NO_VALUE)
  }

  test("longer path with different direction") {
    //given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 1)
    val n3 = NodeAt(node(44), 2)
    val n4 = NodeAt(node(45), 3)
    val r1 = RelAt(relationship(1337, n1.node, n2.node), 10)
    val r2 =  RelAt(relationship(1338, n3.node, n2.node), 20)
    val r3 =  RelAt(relationship(1339, n3.node, n4.node), 30)
    addNodes(context, dbAccess, n1, n2, n3, n4)
    addRelationships(context, dbAccess, r1, r2, r3)

    //when
    //p = (n1)-[r1]->(n2)<-[r2]-(n3)-[r3]-(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(10, "r1"), OUTGOING, Some(NodeFromSlot(1, "n2")),
                                                                   SingleRelationshipPathStep(RelationshipFromSlot(20, "r2"), INCOMING, Some(NodeFromSlot(2, "n3")),
                                                                                              SingleRelationshipPathStep(RelationshipFromSlot(30, "r3"), BOTH, Some(NodeFromSlot(3, "n4")), NilPathStep)))))

    // then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node, n3.node, n4.node), Array(r1.rel, r2.rel, r3.rel)))
  }

  test("multiple outgoing path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 1)
    val n3 = NodeAt(node(43), 2)
    val n4 = NodeAt(node(44), 3)
    val r1 = RelAt(relationship(1337, n1.node, n2.node), 10)
    val r2 = RelAt(relationship(1337, n2.node, n3.node), 11)
    val r3 = RelAt(relationship(1337, n3.node, n4.node), 12)
    addNodes(context, dbAccess, n1, n2, n3, n4)
    addRelationships(context, dbAccess, r1, r2, r3)
    when(context.getRefAt(100)).thenReturn(list(r1.rel, r2.rel, r3.rel))

    //when
    //p = (n1)-[r*]->(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(100, "r"),
                                                                  OUTGOING, Some(NodeFromSlot(3, "n4")), NilPathStep)))

    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(
      VirtualValues.path(Array(n1.node, n2.node, n3.node, n4.node), Array(r1.rel, r2.rel, r3.rel)))
  }

  test("multiple incoming path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 1)
    val n3 = NodeAt(node(43), 2)
    val n4 = NodeAt(node(44), 3)
    val r1 = RelAt(relationship(1337, n1.node, n2.node), 10)
    val r2 = RelAt(relationship(1337, n2.node, n3.node), 11)
    val r3 = RelAt(relationship(1337, n3.node, n4.node), 12)
    addNodes(context, dbAccess, n1, n2, n3, n4)
    addRelationships(context, dbAccess, r1, r2, r3)
    when(context.getRefAt(100)).thenReturn(list(r3.rel, r2.rel, r1.rel))

    //when
    //p = (n4)<-[r*]-(n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(3, "n4"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(100, "r"),
                                                                  INCOMING, Some(NodeFromSlot(0, "n1")), NilPathStep)))

    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(
      VirtualValues.path(Array(n4.node, n3.node, n2.node, n1.node), Array(r3.rel, r2.rel, r1.rel)))
  }

  test("multiple undirected path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 1)
    val n3 = NodeAt(node(43), 2)
    val n4 = NodeAt(node(44), 3)
    val r1 = RelAt(relationship(1337, n1.node, n2.node), 10)
    val r2 = RelAt(relationship(1337, n2.node, n3.node), 11)
    val r3 = RelAt(relationship(1337, n3.node, n4.node), 12)
    addNodes(context, dbAccess, n1, n2, n3, n4)
    addRelationships(context, dbAccess, r1, r2, r3)
    when(context.getRefAt(100)).thenReturn(list(r3.rel, r2.rel, r1.rel))

    //when
    //p = (n4)<-[r*]-(n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(3, "n4"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(100, "r"),
                                                                  BOTH, Some(NodeFromSlot(0, "n1")), NilPathStep)))

    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should equal(
      VirtualValues.path(Array(n4.node, n3.node, n2.node, n1.node), Array(r3.rel, r2.rel, r1.rel)))
  }

  test("multiple path containing NO_VALUE") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 1)
    val n3 = NodeAt(node(43), 2)
    val n4 = NodeAt(node(44), 3)
    val r1 = RelAt(relationship(1337, n1.node, n2.node), 10)
    val r2 = RelAt(relationship(1337, n2.node, n3.node), 11)
    val r3 = RelAt(relationship(1337, n3.node, n4.node), 12)
    addNodes(context, dbAccess, n1, n2, n3, n4)
    addRelationships(context, dbAccess, r1, r2, r3)
    when(context.getRefAt(100)).thenReturn(list(r1.rel, NO_VALUE, r3.rel))

    //when
    //p = (n1)-[r*]->(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(100, "r"),
                                                                  OUTGOING, Some(NodeFromSlot(n4.slot, "n4")), NilPathStep)))

    //then
    compile(p).evaluate(context, dbAccess, EMPTY_MAP, cursors) should be(NO_VALUE)
  }

  test("multiple NO_VALUE path") {
    // given
    val context = mock[ExecutionContext]
    val dbAccess = mock[DbAccess]
    val n1 = NodeAt(node(42), 0)
    val n2 = NodeAt(node(43), 1)
    addNodes(context, dbAccess, n1, n2)
    val slots = SlotConfiguration(Map("r" -> RefSlot(100, nullable = true, symbols.CTList(symbols.CTRelationship))), 0, 1)
    when(context.getRefAt(100)).thenReturn(NO_VALUE)

    //when
    //p = (n1)-[r*]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(0, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(100, "r"),
                                                                  OUTGOING, Some(NodeFromSlot(1, "n2")), NilPathStep)))

    //then
    compile(p, slots).evaluate(context, dbAccess, EMPTY_MAP, cursors) should be(NO_VALUE)
  }

  case class NodeAt(node: NodeValue, slot: Int)
  case class RelAt(rel: RelationshipValue, slot: Int)

  private def addNodes(context: ExecutionContext, dbAccess: DbAccess, nodes: NodeAt*): Unit = {
    for (node <- nodes) {
      when(context.getLongAt(node.slot)).thenReturn(node.node.id())
      when(dbAccess.nodeById(node.node.id())).thenReturn(node.node)
    }
  }

  private def addRelationships(context: ExecutionContext, dbAccess: DbAccess, rels: RelAt*): Unit = {
    for (rel <- rels) {
      when(context.getLongAt(rel.slot)).thenReturn(rel.rel.id())
      when(dbAccess.relationshipById(rel.rel.id())).thenReturn(rel.rel)
    }
  }
  private def pathExpression(step: PathStep) = PathExpression(step)(pos)

  private def mapProjection(name: String, includeAllProps: Boolean, items: (String,Expression)*) =
    DesugaredMapProjection(varFor(name), items.map(kv => LiteralEntry(PropertyKeyName(kv._1)(pos), kv._2)(pos)), includeAllProps)(pos)

  private def simpleCase(inner: Expression, alternatives: List[(Expression, Expression)], default: Option[Expression] = None) =
    CaseExpression(Some(inner), alternatives, default)(pos)

  private def genericCase(alternatives: List[(Expression, Expression)], default: Option[Expression] = None) =
    CaseExpression(None, alternatives, default)(pos)

  private def path(size: Int) =
    VirtualValues.path((0 to size).map(i => node(i)).toArray, (0 until size).map(i => relationship(i)).toArray)

  private def node(id: Int, props: MapValue = EMPTY_MAP) = nodeValue(id, EMPTY_TEXT_ARRAY, EMPTY_MAP)

  private def relationship(id: Int, props: MapValue = EMPTY_MAP) =
    relationshipValue(id, node(id-1), node(id + 1), stringValue("R"), props)

  private def relationship(id: Int, from: NodeValue, to: NodeValue) =
    relationshipValue(id, from, to, stringValue("R"), EMPTY_MAP)

  private def compile(e: Expression, slots: SlotConfiguration) =
    CodeGeneration.compileExpression(new IntermediateCodeGeneration(slots).compileExpression(e).getOrElse(fail()))

  private def compile(e: Expression) =
    CodeGeneration.compileExpression(new IntermediateCodeGeneration(SlotConfiguration.empty).compileExpression(e).getOrElse(fail()))

  private def compileProjection(projections: Map[Int, Expression]) = {
    val compiler = new IntermediateCodeGeneration(SlotConfiguration.empty)
    val compiled = for ((s,e) <- projections) yield s -> compiler.compileExpression(e).getOrElse(fail(s"failed to compile $e"))
    CodeGeneration.compileProjection(compiler.compileProjection(compiled))
  }

  private def compileGroupingExpression(projections: Map[Slot, Expression]): CompiledGroupingExpression = {
    val compiler = new IntermediateCodeGeneration(SlotConfiguration.empty)
    val compiled = for ((s,e) <- projections) yield s -> compiler.compileExpression(e).getOrElse(fail(s"failed to compile $e"))
    CodeGeneration.compileGroupingExpression(compiler.compileGroupingExpression(compiled))
  }

  private def add(l: Expression, r: Expression) = expressions.Add(l, r)(pos)

  private def unaryAdd(source: Expression) = UnaryAdd(source)(pos)

  private def subtract(l: Expression, r: Expression) = expressions.Subtract(l, r)(pos)

  private def unarySubtract(source: Expression) = UnarySubtract(source)(pos)

  private def multiply(l: Expression, r: Expression) = Multiply(l, r)(pos)

  private def divide(l: Expression, r: Expression) = Divide(l, r)(pos)

  private def modulo(l: Expression, r: Expression) = Modulo(l, r)(pos)

  private def pow(l: Expression, r: Expression) = Pow(l, r)(pos)

  private def parameter(key: String) = Parameter(key, symbols.CTAny)(pos)

  private def nullLiteral = Null()(pos)

  private def trueLiteral = True()(pos)

  private def falseLiteral = False()(pos)

  private def or(l: Expression, r: Expression) = Or(l, r)(pos)

  private def xor(l: Expression, r: Expression) = Xor(l, r)(pos)

  private def ors(es: Expression*) = Ors(es.toSet)(pos)

  private def and(l: Expression, r: Expression) = And(l, r)(pos)

  private def ands(es: Expression*) = Ands(es.toSet)(pos)

  private def not(e: Expression) = expressions.Not(e)(pos)

  private def equals(lhs: Expression, rhs: Expression) = Equals(lhs, rhs)(pos)

  private def notEquals(lhs: Expression, rhs: Expression) = NotEquals(lhs, rhs)(pos)

  private def property(map: Expression, key: String) = Property(map, PropertyKeyName(key)(pos))(pos)

  private def containerIndex(container: Expression, index: Expression) = ContainerIndex(container, index)(pos)

  private def literalString(s: String) = expressions.StringLiteral(s)(pos)

  private def literal(a: Any) = a match {
    case null => nullLiteral
    case s: String => literalString(s)
    case d: Double => literalFloat(d)
    case d: java.lang.Float => literalFloat(d.doubleValue())
    case i: Byte => literalInt(i)
    case i: Short => literalInt(i)
    case i: Int => literalInt(i)
    case l: Long => SignedDecimalIntegerLiteral(l.toString)(pos)
  }

  private def literalMap(keyValues: (String,Expression)*) =
    MapExpression(keyValues.map(kv => (PropertyKeyName(kv._1)(pos), kv._2)))(pos)

  private def lessThan(lhs: Expression, rhs: Expression) = LessThan(lhs, rhs)(pos)

  private def lessThanOrEqual(lhs: Expression, rhs: Expression) = LessThanOrEqual(lhs, rhs)(pos)

  private def greaterThan(lhs: Expression, rhs: Expression) = GreaterThan(lhs, rhs)(pos)

  private def greaterThanOrEqual(lhs: Expression, rhs: Expression) = GreaterThanOrEqual(lhs, rhs)(pos)

  private def regex(lhs: Expression, rhs: Expression) = RegexMatch(lhs, rhs)(pos)

  private def startsWith(lhs: Expression, rhs: Expression) = StartsWith(lhs, rhs)(pos)

  private def endsWith(lhs: Expression, rhs: Expression) = EndsWith(lhs, rhs)(pos)

  private def contains(lhs: Expression, rhs: Expression) = Contains(lhs, rhs)(pos)

  private def in(lhs: Expression, rhs: Expression) = In(lhs, rhs)(pos)

  private def coerceTo(expression: Expression, typ: CypherType) = CoerceTo(expression, typ)

  private def coerce(value: AnyValue, ct: CypherType) =
    compile(coerceTo(parameter("a"), ct)).evaluate(ctx, db, map(Array("a"), Array(value)), cursors)

  private def isNull(expression: Expression) = expressions.IsNull(expression)(pos)

  private def isNotNull(expression: Expression) = expressions.IsNotNull(expression)(pos)

  private def sliceFrom(list: Expression, from: Expression) = ListSlice(list, Some(from), None)(pos)

  private def sliceTo(list: Expression, to: Expression) = ListSlice(list, None, Some(to))(pos)

  private def sliceFull(list: Expression, from: Expression, to: Expression) = ListSlice(list, Some(from), Some(to))(pos)

  private def singleInList(variable: String, collection: Expression, predicate: Expression) =
    SingleIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  private def noneInList(variable: String, collection: Expression, predicate: Expression) =
    NoneIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  private def anyInList(variable: String, collection: Expression, predicate: Expression) =
    AnyIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  private def allInList(variable: String, collection: Expression, predicate: Expression) =
    AllIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  private def filter(variable: String, collection: Expression, predicate: Expression) =
    FilterExpression(varFor(variable), collection, Some(predicate) )(pos)

  private def extract(variable: String, collection: Expression, extract: Expression) =
    ExtractExpression(varFor(variable), collection, None, Some(extract) )(pos)

  private def reduce(accumulator: String, init: Expression, variable: String, collection: Expression, expression: Expression) =
    ReduceExpression(varFor(accumulator), init, varFor(variable), collection,  expression)(pos)

  private def listComprehension(variable: String,
                                collection: Expression,
                                predicate: Option[Expression],
                                extractExpression: Option[Expression]) =
    ListComprehension(varFor(variable), collection, predicate, extractExpression)(pos)

  private def callByName(ufs: UserFunctionSignature, args: Expression*) =
    ResolvedFunctionInvocation(ufs.name, Some(ufs), args.toIndexedSeq)(pos)

  private def signature(name: KernelQualifiedName, id: Option[Int] = None, field: FieldSignature = fieldSignature("foo")) =
    UserFunctionSignature(QualifiedName(Seq.empty, name.name()), IndexedSeq(field), symbols.CTAny, None,
                          Array.empty, None, isAggregate = false, id= id)


  private def fieldSignature(name: String, cypherType: CypherType = symbols.CTAny, default: Option[AnyRef] = None) =
    FieldSignature(name, cypherType, default = default.map(CypherValue(_, cypherType)))

  private def qualifiedName(name: String) = new KernelQualifiedName(Array.empty[String], name)

  private val numericalValues: Seq[AnyRef] = Seq[Number](
    Double.NegativeInfinity,
    Double.MinValue,
    Long.MinValue,
    -1,
    -0.5,
    0,
    Double.MinPositiveValue,
    0.5,
    1,
    10.00,
    10.33,
    10.66,
    11.00,
    Math.PI,
    Long.MaxValue,
    Double.MaxValue,
    Double.PositiveInfinity,
    Double.NaN,
    null
  ).flatMap {
    case null => Seq(null)
    case v: Number if v.doubleValue().isNaN => Seq[Number](v.doubleValue(), v.floatValue(), v)
    case v: Number =>
      Seq[Number](v.doubleValue(), v.floatValue(), v.longValue(), v.intValue(), v.shortValue(), v.byteValue(), v)
  }

  private val textualValues: Seq[String] = Seq(
    "",
    "Hal",
    s"Hal${Character.MIN_VALUE}",
    "Hallo",
    "Hallo!",
    "Hello",
    "Hullo",
    null,
    "\uD801\uDC37"
  ).flatMap {
    case null => Seq(null)
    case v: String => Seq(v, v.toUpperCase, v.toLowerCase, reverse(v))
  }

  private def reverse(s: String) = new StringBuilder(s).reverse.toString()

  private val allValues = numericalValues ++ textualValues

  case class compareUsingLessThan(left: Any, right: Any) extends compareUsing(left, right, "<")

  case class compareUsingLessThanOrEqual(left: Any, right: Any) extends compareUsing(left, right, "<=")

  case class compareUsingGreaterThanOrEqual(left: Any, right: Any) extends compareUsing(left, right, ">=")

  case class compareUsingGreaterThan(left: Any, right: Any) extends compareUsing(left, right, ">")

  class compareUsing(left: Any, right: Any, operator: String) extends Matcher[Expression] {
    def apply(predicate: Expression): MatchResult = {
      val actual = compile(predicate).evaluate(ctx, db, EMPTY_MAP, cursors)

      if (isIncomparable(left, right))
        buildResult(actual == NO_VALUE, actual)
      else {
        assert(actual != NO_VALUE && actual.isInstanceOf[BooleanValue], s"$left $operator $right")
        val actualBoolean = actual.asInstanceOf[BooleanValue].booleanValue()
        val expected = AnyValues.COMPARATOR.compare(Values.of(left), Values.of(right))
        val result = operator match {
          case "<" => (expected < 0) == actualBoolean
          case "<=" => (expected <= 0) == actualBoolean
          case ">=" => (expected >= 0) == actualBoolean
          case ">" => (expected > 0) == actualBoolean
        }
        buildResult(result, actual)
      }
    }

    def isIncomparable(left: Any, right: Any): Boolean = {
      left == null || (left.isInstanceOf[Number] && left.asInstanceOf[Number].doubleValue().isNaN) ||
        right == null || (right.isInstanceOf[Number] && right.asInstanceOf[Number].doubleValue().isNaN) ||
        left.isInstanceOf[Number] && right.isInstanceOf[String] ||
        left.isInstanceOf[String] && right.isInstanceOf[Number]
    }

    def buildResult(result: Boolean, actual: Any): MatchResult = {
      MatchResult(
        result,
        s"Expected $left $operator $right to compare as $result but it was $actual",
        s"Expected $left $operator $right to not compare as $result but it was $actual"
      )
    }
  }

  private def parameters(kvs: (String, AnyValue)*) = map(kvs.map(_._1).toArray, kvs.map(_._2).toArray)

  private val types = Map(longValue(42) -> symbols.CTNumber, stringValue("hello") -> symbols.CTString,
                   Values.TRUE -> symbols.CTBoolean, node(42) -> symbols.CTNode,
                   relationship(1337) -> symbols.CTRelationship, path(13) -> symbols.CTPath,
                   pointValue(Cartesian, 1.0, 3.6) -> symbols.CTPoint,
                   DateTimeValue.now(Clock.systemUTC()) -> symbols.CTDateTime,
                   LocalDateTimeValue.now(Clock.systemUTC()) -> symbols.CTLocalDateTime,
                   TimeValue.now(Clock.systemUTC()) -> symbols.CTTime,
                   LocalTimeValue.now(Clock.systemUTC()) -> symbols.CTLocalTime,
                   DateValue.now(Clock.systemUTC()) -> symbols.CTDate,
                   durationValue(Duration.ofHours(3)) -> symbols.CTDuration)

}
