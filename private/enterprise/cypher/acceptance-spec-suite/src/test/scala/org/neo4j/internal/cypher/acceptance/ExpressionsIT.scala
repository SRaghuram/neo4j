/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Math.{PI, sin}
import java.time.{Clock, Duration}
import java.util.concurrent.ThreadLocalRandom

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, ArgumentSizes, NestedPlanArgumentConfigurations, SlotConfigurations}
import org.neo4j.cypher.internal.physicalplanning.ast._
import org.neo4j.cypher.internal.physicalplanning.{ast, _}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.ast.{ExpressionVariable, ParameterFromSlot, RuntimeExpression, RuntimeProperty}
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.defaultGenerator
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionContext, SlottedQueryState}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CypherType, ListType}
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions
import org.neo4j.exceptions.{CypherTypeException, InvalidArgumentException, InvalidSemanticsException, ParameterWrongTypeException}
import org.neo4j.graphdb.{Entity, Relationship}
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.procs.{Neo4jTypes, QualifiedName => KernelQualifiedName}
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.procedure.CallableUserFunction.BasicUserFunction
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, TransactionalContext}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.CoordinateReferenceSystem.{Cartesian, WGS84}
import org.neo4j.values.storable.LocalTimeValue.localTime
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues.{EMPTY_LIST, EMPTY_MAP, list}
import org.neo4j.values.virtual._
import org.neo4j.values.{AnyValue, AnyValues, VirtualValue}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class ExpressionsIT extends ExecutionEngineFunSuite with AstConstructionTestSupport {

  private val ctx = SlottedExecutionContext(SlotConfiguration.empty)

  override protected def initTest() {
    super.initTest()
    startNewTransaction()
  }

  private def startNewTransaction(): Unit = {
    if (cursors != null) {
      cursors.close()
    }
    if (context != null) {
      context.close()
    }

    tx = graph.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    context = Neo4jTransactionalContextFactory.create(graph).newContext(tx, "X", EMPTY_MAP)
    query = new TransactionBoundQueryContext(TransactionalContextWrapper(context))(mock[IndexSearchMonitor])
    cursors = new ExpressionCursors(TransactionalContextWrapper(context).cursors)
  }

  override protected def stopTest(): Unit = {
    if (cursors != null) {
      cursors.close()
    }
    if (context != null) {
      context.close()
      context = null
    }
    super.stopTest()
  }

  protected var query: QueryContext = _
  private var cursors: ExpressionCursors = _
  private val expressionVariables: Array[AnyValue] = Array.empty
  private var tx: InternalTransaction = _
  private var context: TransactionalContext = _
  private val random = ThreadLocalRandom.current()

  test("round function") {
    evaluate(compile(function("round", literalFloat(PI)))) should equal(doubleValue(3.0))
    evaluate(compile(function("round", nullLiteral))) should equal(NO_VALUE)
  }

  test("rand function") {
    // Given
    val expression = function("rand")

    // When
    val compiled = compile(expression)

    // Then
    val value = evaluate(compiled).asInstanceOf[DoubleValue].doubleValue()
    value should (be >= 0.0 and be < 1.0)
  }

  test("sin function") {
    val arg = random.nextDouble()
    evaluate(compile(function("sin", literalFloat(arg)))) should equal(doubleValue(sin(arg)))
    evaluate(compile(function("sin", nullLiteral))) should equal(NO_VALUE)
  }

  test("asin function") {
    val arg = random.nextDouble()
    evaluate(compile(function("asin", literalFloat(arg)))) should equal(doubleValue(Math.asin(arg)))
    evaluate(compile(function("asin", nullLiteral))) should equal(NO_VALUE)
  }

  test("haversin function") {
    val arg = random.nextDouble()
    evaluate(compile(function("haversin", literalFloat(arg)))) should equal(doubleValue((1.0 - Math.cos(arg)) / 2))
    evaluate(compile(function("haversin", nullLiteral))) should equal(NO_VALUE)
  }

  test("acos function") {
    val arg = random.nextDouble()
    evaluate(compile(function("acos", literalFloat(arg)))) should equal(doubleValue(Math.acos(arg)))
    evaluate(compile(function("acos", nullLiteral))) should equal(NO_VALUE)
  }

  test("cos function") {
    val arg = random.nextDouble()
    evaluate(compile(function("cos", literalFloat(arg)))) should equal(doubleValue(Math.cos(arg)))
    evaluate(compile(function("cos", nullLiteral))) should equal(NO_VALUE)
  }

  test("cot function") {
    val arg = random.nextDouble()
    evaluate(compile(function("cot", literalFloat(arg)))) should equal(doubleValue(1 / Math.tan(arg)))
    evaluate(compile(function("cot", nullLiteral))) should equal(NO_VALUE)
  }

  test("atan function") {
    val arg = random.nextDouble()
    evaluate(compile(function("atan", literalFloat(arg)))) should equal(doubleValue(Math.atan(arg)))
    evaluate(compile(function("atan", nullLiteral))) should equal(NO_VALUE)
  }

  test("atan2 function") {
    val arg1 = random.nextDouble()
    val arg2 = random.nextDouble()
    evaluate(compile(function("atan2", literalFloat(arg1), literalFloat(arg2)))) should equal(doubleValue(Math.atan2(arg1, arg2)))
    evaluate(compile(function("atan2", nullLiteral, literalFloat(arg1)))) should equal(NO_VALUE)
    evaluate(compile(function("atan2", literalFloat(arg1), nullLiteral))) should equal(NO_VALUE)
    evaluate(compile(function("atan2", nullLiteral, nullLiteral))) should equal(NO_VALUE)
  }

  test("tan function") {
    val arg = random.nextDouble()
    evaluate(compile(function("tan", literalFloat(arg)))) should equal(doubleValue(Math.tan(arg)))
    evaluate(compile(function("tan", nullLiteral))) should equal(NO_VALUE)
  }

  test("ceil function") {
    val arg = random.nextDouble()
    evaluate(compile(function("ceil", literalFloat(arg)))) should equal(doubleValue(Math.ceil(arg)))
    evaluate(compile(function("ceil", nullLiteral))) should equal(NO_VALUE)
  }

  test("floor function") {
    val arg = random.nextDouble()
    evaluate(compile(function("floor", literalFloat(arg)))) should equal(doubleValue(Math.floor(arg)))
    evaluate(compile(function("floor", nullLiteral))) should equal(NO_VALUE)
  }

  test("abs function") {
    evaluate(compile(function("abs", literalFloat(3.2)))) should equal(doubleValue(3.2))
    evaluate(compile(function("abs", literalFloat(-3.2)))) should equal(doubleValue(3.2))
    evaluate(compile(function("abs", literalInt(3)))) should equal(longValue(3))
    evaluate(compile(function("abs", literalInt(-3)))) should equal(longValue(3))
    evaluate(compile(function("abs", nullLiteral))) should equal(Values.NO_VALUE)
  }

  test("radians function") {
    val arg = random.nextDouble()
    evaluate(compile(function("radians", literalFloat(arg)))) should equal(doubleValue(Math.toRadians(arg)))
    evaluate(compile(function("radians", nullLiteral))) should equal(NO_VALUE)
  }

  test("degrees function") {
    val arg = random.nextDouble()
    evaluate(compile(function("degrees", literalFloat(arg)))) should equal(doubleValue(Math.toDegrees(arg)))
    evaluate(compile(function("degrees", nullLiteral))) should equal(NO_VALUE)
  }

  test("exp function") {
    val arg = random.nextDouble()
    evaluate(compile(function("exp", literalFloat(arg)))) should equal(doubleValue(Math.exp(arg)))
    evaluate(compile(function("exp", nullLiteral))) should equal(NO_VALUE)
  }

  test("log function") {
    val arg = random.nextDouble()
    evaluate(compile(function("log", literalFloat(arg)))) should equal(doubleValue(Math.log(arg)))
    evaluate(compile(function("log", nullLiteral))) should equal(NO_VALUE)
  }

  test("log10 function") {
    val arg = random.nextDouble()
    evaluate(compile(function("log10", literalFloat(arg)))) should equal(doubleValue(Math.log10(arg)))
    evaluate(compile(function("log10", nullLiteral))) should equal(NO_VALUE)
  }

  test("sign function") {
    val arg = random.nextInt()
    evaluate(compile(function("sign", literalFloat(arg)))) should equal(doubleValue(Math.signum(arg)))
    evaluate(compile(function("sign", nullLiteral))) should equal(NO_VALUE)
  }

  test("sqrt function") {
    val arg = random.nextDouble()
    evaluate(compile(function("sqrt", literalFloat(arg)))) should equal(doubleValue(Math.sqrt(arg)))
    evaluate(compile(function("sqrt", nullLiteral))) should equal(NO_VALUE)
  }

  test("pi function") {
    evaluate(compile(function("pi"))) should equal(Values.PI)
  }

  test("e function") {
    evaluate(compile(function("e"))) should equal(Values.E)
  }

  test("range function with no step") {
    val range = function("range", literalInt(5), literalInt(9))
    evaluate(compile(range)) should equal(list(longValue(5), longValue(6), longValue(7),
                                               longValue(8), longValue(9)))
  }

  test("range function with step") {
    val range = function("range", literalInt(5), literalInt(9), literalInt(2))
    evaluate(compile(range)) should equal(list(longValue(5), longValue(7), longValue(9)))
  }

  test("coalesce function") {
    evaluate(compile(function("coalesce", nullLiteral, nullLiteral, literalInt(2), nullLiteral))) should equal(longValue(2))
    evaluate(compile(function("coalesce", nullLiteral, nullLiteral))) should equal(NO_VALUE)
  }

  test("coalesce function with parameters") {
    val compiled = compile(function("coalesce", parameter(0), parameter(1), parameter(2)))

    evaluate(compiled, params(NO_VALUE, longValue(2), NO_VALUE)) should equal(longValue(2))
    evaluate(compiled, params(NO_VALUE, NO_VALUE, NO_VALUE)) should equal(NO_VALUE)
  }

  test("distance function") {
    val compiled = compile(function("distance", parameter(0), parameter(1)))
    evaluate(compiled, params(pointValue(Cartesian, 0.0, 0.0),
                                 pointValue(Cartesian, 1.0, 1.0))) should equal(doubleValue(Math.sqrt(2)))
    evaluate(compiled, params(pointValue(Cartesian, 0.0, 0.0),
                                 NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(pointValue(Cartesian, 0.0, 0.0),
                                 pointValue(WGS84, 1.0, 1.0))) should equal(NO_VALUE)

  }

  test("startNode") {
    val rel = relationshipValue()
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    val compiled = compile(function("startNode", parameter(0)), slots)
    addRelationships(context, RelAt(rel, 0))
    evaluate(compiled, params(rel)) should equal(rel.startNode())
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("endNode") {
    val rel = relationshipValue()
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    val compiled = compile(function("endNode", parameter(0)), slots)
    addRelationships(context, RelAt(rel, 0))
    evaluate(compiled, params(rel)) should equal(rel.endNode())
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("exists on node") {
    val compiled = compile(function("exists", prop(parameter(0), "prop")))

    val node = nodeValue(VirtualValues.map(Array("prop"), Array(stringValue("hello"))))
    evaluate(compiled,params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(node)) should equal(Values.TRUE)
  }

  test("exists on relationship") {
    val compiled = compile(function("exists", prop(parameter(0), "prop")))

    val rel = relationshipValue(nodeValue(),
                                nodeValue(),
                                VirtualValues.map(Array("prop"), Array(stringValue("hello"))))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(rel)) should equal(Values.TRUE)
  }

  test("exists on map") {
    val compiled = compile(function("exists", prop(parameter(0), "prop")))

    val mapValue = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(mapValue)) should equal(Values.TRUE)
  }

  test("head function") {
    val compiled = compile(function("head", parameter(0)))
    val listValue = list(stringValue("hello"), intValue(42))

    evaluate(compiled, params(listValue)) should equal(stringValue("hello"))
    evaluate(compiled, params(EMPTY_LIST)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("last function") {
    val compiled = compile(function("last", parameter(0)))
    val listValue = list(intValue(42), stringValue("hello"))

    evaluate(compiled, params(listValue)) should equal(stringValue("hello"))
    evaluate(compiled, params(EMPTY_LIST)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("left function") {
    val compiled = compile(function("left", parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("HELLO"), intValue(4))) should
      equal(stringValue("HELL"))
    evaluate(compiled, params(stringValue("HELLO"), intValue(17))) should
      equal(stringValue("HELLO"))
    evaluate(compiled, params(NO_VALUE, intValue(4))) should equal(NO_VALUE)

    an[IndexOutOfBoundsException] should be thrownBy evaluate(compiled,
                                                              params(stringValue("HELLO"), intValue(-1)))
  }

  test("ltrim function") {
    val compiled = compile(function("ltrim", parameter(0)))

    evaluate(compiled, params(stringValue("  HELLO  "))) should
      equal(stringValue("HELLO  "))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("rtrim function") {
    val compiled = compile(function("rtrim", parameter(0)))

    evaluate(compiled, params(stringValue("  HELLO  "))) should
      equal(stringValue("  HELLO"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("trim function") {
    val compiled = compile(function("trim", parameter(0)))

    evaluate(compiled, params(stringValue("  HELLO  "))) should
      equal(stringValue("HELLO"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("replace function") {
    val compiled = compile(function("replace", parameter(0), parameter(1), parameter(2)))

    evaluate(compiled, params(stringValue("HELLO"),
                                 stringValue("LL"),
                                 stringValue("R"))) should equal(stringValue("HERO"))
    evaluate(compiled, params(NO_VALUE,
                                 stringValue("LL"),
                                 stringValue("R"))) should equal(NO_VALUE)
    evaluate(compiled, params(stringValue("HELLO"),
                                 NO_VALUE,
                                 stringValue("R"))) should equal(NO_VALUE)
    evaluate(compiled, params(stringValue("HELLO"),
                                 stringValue("LL"),
                                 NO_VALUE)) should equal(NO_VALUE)
  }

  test("reverse function") {
    val compiled = compile(function("reverse", parameter(0)))

    evaluate(compiled, params(stringValue("PARIS"))) should equal(stringValue("SIRAP"))
    val original = list(intValue(1), intValue(2), intValue(3))
    val reversed = list(intValue(3), intValue(2), intValue(1))
    evaluate(compiled, params(original)) should equal(reversed)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("right function") {
    val compiled = compile(function("right", parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("HELLO"), intValue(4))) should
      equal(stringValue("ELLO"))
    evaluate(compiled, params(NO_VALUE, intValue(4))) should equal(NO_VALUE)
  }

  test("split function") {
    val compiled = compile(function("split", parameter(0), parameter(1)))
    def sl(s: String*): ListValue = list(s.map(stringValue): _*)

    // Strings with single delimiter
    evaluate(compiled, params(stringValue("HELLO"), stringValue("LL"))) should be(sl("HE", "O"))
    evaluate(compiled, params(NO_VALUE, stringValue("LL"))) should be(NO_VALUE)
    evaluate(compiled, params(stringValue("HELLO"), NO_VALUE)) should be(NO_VALUE)
    evaluate(compiled, params(stringValue("HELLO"), EMPTY_STRING)) should be(sl("H", "E", "L", "L", "O"))
    evaluate(compiled, params(EMPTY_STRING, stringValue("LL"))) should equal(list(EMPTY_STRING))

    // Strings with multiple delimiters
    evaluate(compiled, params(stringValue("first,second;third"), sl(",", ";"))) should be(sl("first", "second", "third"))
    evaluate(compiled, params(stringValue("(a)-->(b)<--(c)-->(d)--(e)"), sl("-->", "<--", "--"))) should be(sl("(a)", "(b)", "(c)", "(d)", "(e)"))
    val sentence = "This is a sentence, with punctuation."
    evaluate(compiled, params(stringValue(sentence), sl(",", ".", ";", ""))) should be(sl(sentence.replaceAll("[,.;]", "").split(""): _*))

    // Splitting chars
    evaluate(compiled, params(charValue('x'), stringValue("x"))) should be(sl("", ""))
    evaluate(compiled, params(charValue('x'), stringValue("y"))) should be(sl("x"))
    evaluate(compiled, params(charValue('x'), stringValue(""))) should be(sl("x"))
    evaluate(compiled, params(charValue('x'), sl("x", "y"))) should be(sl("", ""))
  }

  test("substring function no length") {
    val compiled = compile(function("substring", parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("HELLO"), intValue(1))) should
      equal(stringValue("ELLO"))
    evaluate(compiled, params(NO_VALUE, intValue(1))) should equal(NO_VALUE)
  }

  test("substring function with length") {
    evaluate(compile(function("substring", parameter(0), parameter(1), parameter(2))),
             params(stringValue("HELLO"), intValue(1), intValue(2))) should equal(stringValue("EL"))
    evaluate(compile(function("substring", parameter(0), parameter(1))),
             params(NO_VALUE, intValue(1))) should equal(NO_VALUE)
  }

  test("toLower function") {
    val compiled = compile(function("toLower", parameter(0)))

    evaluate(compiled, params(stringValue("HELLO"))) should
      equal(stringValue("hello"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("toUpper function") {
    val compiled = compile(function("toUpper", parameter(0)))

    evaluate(compiled, params(stringValue("hello"))) should
      equal(stringValue("HELLO"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("nodes function") {
    val compiled = compile(function("nodes", parameter(0)))

    val p = path(2)

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(p)) should equal(VirtualValues.list(p.nodes():_*))
  }

  test("relationships function") {
    val compiled = compile(function("relationships", parameter(0)))

    val p = path(2)

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(p)) should equal(VirtualValues.list(p.relationships():_*))
  }

  test("id on node") {
    val compiled = compile(id(parameter(0)))

    val node = nodeValue()

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(node)) should equal(longValue(node.id()))
  }

  test("id on relationship") {
    val compiled = compile(id(parameter(0)))

    val rel = relationshipValue()

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(rel)) should equal(longValue(rel.id()))
  }

  test("labels function") {
    val compiled = compile(function("labels", parameter(0)))

    val labels = Values.stringArray("A", "B", "C")
    val node = ValueUtils.fromNodeProxy(createLabeledNode("A", "B", "C"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(node)) should equal(labels)
  }

  test("type function") {
    val compiled = compile(function("type", parameter(0)))
    val rel = ValueUtils.fromRelationshipProxy(relate(createNode(), createNode(), "R"))

    evaluate(compiled, params(rel)) should equal(stringValue("R"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("points from node") {
    val compiled = compile(function("point", parameter(0)))

    val pointMap = VirtualValues.map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val node = nodeValue(pointMap)

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(node)) should equal(PointValue.fromMap(pointMap))
  }

  test("points from relationship") {
    val compiled = compile(function("point", parameter(0)))

    val pointMap = VirtualValues.map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val rel = relationshipValue(nodeValue(), nodeValue(), pointMap)

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(rel)) should equal(PointValue.fromMap(pointMap))
  }

  test("points from map") {
    val compiled = compile(function("point", parameter(0)))

    val pointMap = VirtualValues.map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(pointMap)) should equal(PointValue.fromMap(pointMap))
  }

  test("keys on node") {
    val compiled = compile(function("keys", parameter(0)))
    val node = nodeValue(VirtualValues.map(Array("A", "B", "C"), Array(stringValue("a"), stringValue("b"), stringValue("c"))))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(node)) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on relationship") {
    val compiled = compile(function("keys", parameter(0)))

    val rel = relationshipValue(nodeValue(), nodeValue(),
                                VirtualValues.map(Array("A", "B", "C"),
                                    Array(stringValue("a"), stringValue("b"), stringValue("c"))))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(rel)) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on map") {
    val compiled = compile(function("keys", parameter(0)))

    val mapValue = VirtualValues.map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(mapValue)) should equal(mapValue.keys())
  }

  test("size function") {
    val compiled = compile(function("size", parameter(0)))

    evaluate(compiled, params(stringValue("HELLO"))) should equal(intValue(5))
    evaluate(compiled, params(NO_VALUE, intValue(4))) should equal(NO_VALUE)
  }

  test("length function") {
    val compiled = compile(function("length", parameter(0)))

    val p = path(2)

    evaluate(compiled, params(p)) should equal(intValue(2))
    evaluate(compiled, params(NO_VALUE, intValue(4))) should equal(NO_VALUE)
  }

  test("tail function") {
    val compiled = compile(function("tail", parameter(0)))

    evaluate(compiled,
             params(list(intValue(1), intValue(2), intValue(3)))) should equal(list(intValue(2), intValue(3)))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("toBoolean function") {
    val compiled = compile(function("toBoolean", parameter(0)))

    evaluate(compiled, params(Values.TRUE)) should equal(Values.TRUE)
    evaluate(compiled, params(Values.FALSE)) should equal(Values.FALSE)
    evaluate(compiled, params(stringValue("false"))) should equal(Values.FALSE)
    evaluate(compiled, params(stringValue("true"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("uncertain"))) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("toFloat function") {
    val compiled = compile(function("toFloat", parameter(0)))

    evaluate(compiled, params(doubleValue(3.2))) should equal(doubleValue(3.2))
    evaluate(compiled, params(intValue(3))) should equal(doubleValue(3))
    evaluate(compiled, params(stringValue("3.2"))) should equal(doubleValue(3.2))
    evaluate(compiled, params(stringValue("three dot two"))) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("toInteger function") {
    val compiled = compile(function("toInteger", parameter(0)))

    evaluate(compiled, params(doubleValue(3.2))) should equal(longValue(3))
    evaluate(compiled, params(intValue(3))) should equal(intValue(3))
    evaluate(compiled, params(stringValue("3"))) should equal(longValue(3))
    evaluate(compiled, params(stringValue("three"))) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("toString function") {
    val compiled = compile(function("toString", parameter(0)))

    evaluate(compiled, params(doubleValue(3.2))) should equal(stringValue("3.2"))
    evaluate(compiled, params(Values.TRUE)) should equal(stringValue("true"))
    evaluate(compiled, params(stringValue("hello"))) should equal(stringValue("hello"))
    evaluate(compiled, params(pointValue(Cartesian, 0.0, 0.0))) should
      equal(stringValue("point({x: 0.0, y: 0.0, crs: 'cartesian'})"))
    evaluate(compiled, params(durationValue(Duration.ofHours(3)))) should
      equal(stringValue("PT3H"))
    evaluate(compiled, params(temporalValue(localTime(20, 0, 0, 0)))) should
      equal(stringValue("20:00:00"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    a[ParameterWrongTypeException] should be thrownBy evaluate(compiled, params(intArray(Array(1, 2, 3))))
  }

  test("properties function on node") {
    val compiled = compile(function("properties", parameter(0)))
    val mapValue = VirtualValues.map(Array("prop"), Array(longValue(42)))
    val node = nodeValue(mapValue)
    evaluate(compiled, params(node)) should equal(mapValue)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("properties function on relationship") {
    val compiled = compile(function("properties", parameter(0)))
    val mapValue = VirtualValues.map(Array("prop"), Array(longValue(42)))
    val rel = relationshipValue(nodeValue(),
                                nodeValue(), mapValue)
    evaluate(compiled, params(rel)) should equal(mapValue)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("properties function on map") {
    val compiled = compile(function("properties", parameter(0)))
    val mapValue = VirtualValues.map(Array("prop"), Array(longValue(42)))

    evaluate(compiled, params(mapValue)) should equal(mapValue)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("add numbers") {
    // Given
    val expression = add(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(longValue(52))
  }

  test("add temporals") {
    val compiled = compile(add(parameter(0), parameter(1)))

    // temporal + duration
    evaluate(compiled, params(temporalValue(localTime(0)),
                                    durationValue(Duration.ofHours(10)))) should
      equal(localTime(10, 0, 0, 0))

    // duration + temporal
    evaluate(compiled, params(durationValue(Duration.ofHours(10)),
                                    temporalValue(localTime(0)))) should
      equal(localTime(10, 0, 0, 0))

    //duration + duration
    evaluate(compiled, params(durationValue(Duration.ofHours(10)),
                                    durationValue(Duration.ofHours(10)))) should
      equal(durationValue(Duration.ofHours(20)))
  }

  test("add with NO_VALUE") {
    // Given
    val expression = add(parameter(0), parameter(1))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled, params(longValue(42), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, longValue(42))) should equal(NO_VALUE)
  }

  test("add strings") {
    // When
    val compiled = compile(add(parameter(0), parameter(1)))

    // string1 + string2
    evaluate(compiled, params(stringValue("hello "), stringValue("world"))) should
      equal(stringValue("hello world"))
    //string + other
    evaluate(compiled, params(stringValue("hello "), longValue(1337))) should
      equal(stringValue("hello 1337"))
    //other + string
    evaluate(compiled, params(longValue(1337), stringValue(" hello"))) should
      equal(stringValue("1337 hello"))

  }

  test("add arrays") {
    // Given
    val expression = add(parameter(0), parameter(1))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled, params(longArray(Array(42, 43)),
                                    longArray(Array(44, 45)))) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))
  }

  test("list addition") {
    // When
    val compiled = compile(add(parameter(0), parameter(1)))

    // [a1,a2 ..] + [b1,b2 ..]
    evaluate(compiled, params(list(longValue(42), longValue(43)),
                                    list(longValue(44), longValue(45)))) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))

    // [a1,a2 ..] + b
    evaluate(compiled, params(list(longValue(42), longValue(43)), longValue(44))) should
      equal(list(longValue(42), longValue(43), longValue(44)))

    // a + [b1,b2 ..]
    evaluate(compiled, params(longValue(43),
                                    list(longValue(44), longValue(45)))) should
      equal(list(longValue(43), longValue(44), longValue(45)))
  }

  test("unary add ") {
    // Given
    val expression = unaryAdd(literalInt(42))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(longValue(42))
  }

  test("subtract numbers") {
    // Given
    val expression = subtract(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(longValue(32))
  }

  test("subtract with NO_VALUE") {
    // Given
    val expression = subtract(parameter(0), parameter(1))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled, params(longValue(42), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, longValue(42))) should equal(NO_VALUE)
  }

  test("subtract temporals") {
    val compiled = compile(subtract(parameter(0), parameter(1)))

    // temporal - duration
    evaluate(compiled, params(temporalValue(localTime(20, 0, 0, 0)),
                                    durationValue(Duration.ofHours(10)))) should
      equal(localTime(10, 0, 0, 0))

    //duration - duration
    evaluate(compiled, params(durationValue(Duration.ofHours(10)),
                                    durationValue(Duration.ofHours(10)))) should
      equal(durationValue(Duration.ofHours(0)))
  }

  test("unary subtract ") {
    // Given
    val expression = unarySubtract(literalInt(42))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(longValue(-42))
  }

  test("multiply function") {
    // Given
    val expression = multiply(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(longValue(420))
  }

  test("multiply with NO_VALUE") {
    // Given
    val expression = multiply(parameter(0), parameter(1))

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled, params(longValue(42), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, longValue(42))) should equal(NO_VALUE)
  }

  test("division") {
    val compiled = compile(divide(parameter(0), parameter(1)))

    // Then
    evaluate(compiled, params(longValue(42), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, longValue(42))) should equal(NO_VALUE)
    evaluate(compiled, params(longValue(6), longValue(3))) should equal(longValue(2))
    evaluate(compiled, params(longValue(5), doubleValue(2))) should equal(doubleValue(2.5))
    an[exceptions.ArithmeticException] should be thrownBy evaluate(compiled, params(longValue(5), longValue(0)))
    evaluate(compiled, params(doubleValue(3.0), doubleValue(0.0))) should equal(doubleValue(Double.PositiveInfinity))
    evaluate(compiled, params(durationValue(Duration.ofHours(4)), longValue(2))) should equal(durationValue(Duration.ofHours(2)))
    an[exceptions.ArithmeticException] should be thrownBy evaluate(compiled, params(NO_VALUE, longValue(0)))
  }

  test("modulo") {
    val compiled = compile(modulo(parameter(0), parameter(1)))

    // Then
    evaluate(compiled, params(longValue(42), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, longValue(42))) should equal(NO_VALUE)
    evaluate(compiled, params(doubleValue(8.0), longValue(6))) should equal(doubleValue(2.0))
    evaluate(compiled, params(longValue(8), doubleValue(6))) should equal(doubleValue(2.0))
    evaluate(compiled, params(longValue(8), longValue(6))) should equal(longValue(2))
  }

  test("pow") {
    val compiled = compile(pow(parameter(0), parameter(1)))

    // Then
    evaluate(compiled, params(longValue(42), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, longValue(42))) should equal(NO_VALUE)
    evaluate(compiled, params(doubleValue(2), longValue(3))) should equal(doubleValue(8.0))
    evaluate(compiled, params(longValue(2), longValue(3))) should equal(doubleValue(8.0))
  }

  test("extract parameter") {
    evaluate(compile(parameter(0)), params(stringValue("foo"))) should equal(stringValue("foo"))
  }

  test("NULL") {
    // Given
    val expression = nullLiteral

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(NO_VALUE)
  }

  test("TRUE") {
    // Given
    val expression = trueLiteral

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(Values.TRUE)
  }

  test("FALSE") {
    // Given
    val expression = falseLiteral

    // When
    val compiled = compile(expression)

    // Then
    evaluate(compiled) should equal(Values.FALSE)
  }

  test("OR") {
    evaluate(compile(or(trueLiteral, trueLiteral))) should equal(Values.TRUE)
    evaluate(compile(or(falseLiteral, trueLiteral))) should equal(Values.TRUE)
    evaluate(compile(or(trueLiteral, falseLiteral))) should equal(Values.TRUE)
    evaluate(compile(or(falseLiteral, falseLiteral))) should equal(Values.FALSE)

    evaluate(compile(or(nullLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(or(nullLiteral, trueLiteral))) should equal(Values.TRUE)
    evaluate(compile(or(trueLiteral, nullLiteral))) should equal(Values.TRUE)
    evaluate(compile(or(nullLiteral, falseLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(or(falseLiteral, nullLiteral))) should equal(Values.NO_VALUE)
  }

  test("XOR") {
    evaluate(compile(xor(trueLiteral, trueLiteral))) should equal(Values.FALSE)
    evaluate(compile(xor(falseLiteral, trueLiteral))) should equal(Values.TRUE)
    evaluate(compile(xor(trueLiteral, falseLiteral))) should equal(Values.TRUE)
    evaluate(compile(xor(falseLiteral, falseLiteral))) should equal(Values.FALSE)

    evaluate(compile(xor(nullLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(xor(nullLiteral, trueLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(xor(trueLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(xor(nullLiteral, falseLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(xor(falseLiteral, nullLiteral))) should equal(Values.NO_VALUE)
  }

  test("OR should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy evaluate(compile(or(literalInt(42), falseLiteral)))
    a [CypherTypeException] should be thrownBy evaluate(compile(or(falseLiteral, literalInt(42))))
    evaluate(compile(or(trueLiteral, literalInt(42)))) should equal(Values.TRUE)
    evaluate(compile(or(literalInt(42), trueLiteral))) should equal(Values.TRUE)
  }

  test("OR should handle coercion") {
    val expression =  compile(or(parameter(0), parameter(1)))
    evaluate(expression, params(Values.FALSE, EMPTY_LIST)) should equal(Values.FALSE)
    evaluate(expression, params(Values.FALSE, list(stringValue("hello")))) should equal(Values.TRUE)
  }

  test("ORS") {
    evaluate(compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, trueLiteral, falseLiteral))) should equal(Values.TRUE)
    evaluate(compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral))) should equal(Values.FALSE)
    evaluate(compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, nullLiteral, falseLiteral, falseLiteral, falseLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(ors(falseLiteral, falseLiteral, falseLiteral, trueLiteral, nullLiteral, trueLiteral, falseLiteral, falseLiteral))) should equal(Values.TRUE)
  }

  test("ORS should throw on non-boolean input") {
    val compiled = compile(ors(parameter(0), parameter(1), parameter(2), parameter(3), parameter(4)))
    evaluate(compiled, params(Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE)) should equal(Values.FALSE)

    evaluate(compiled, params(Values.FALSE, Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE)) should equal(Values.TRUE)

    evaluate(compiled, params(intValue(42), Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE)) should equal(Values.TRUE)

    a [CypherTypeException] should be thrownBy evaluate(compiled,
                                                        params(intValue(42), Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE))
  }

  test("ORS should handle coercion") {
    val expression =  compile(ors(parameter(0), parameter(1)))
    evaluate(expression, params(Values.FALSE, EMPTY_LIST)) should equal(Values.FALSE)
    evaluate(expression, params(Values.FALSE, list(stringValue("hello")))) should equal(Values.TRUE)
  }

  test("AND") {
    evaluate(compile(and(trueLiteral, trueLiteral))) should equal(Values.TRUE)
    evaluate(compile(and(falseLiteral, trueLiteral))) should equal(Values.FALSE)
    evaluate(compile(and(trueLiteral, falseLiteral))) should equal(Values.FALSE)
    evaluate(compile(and(falseLiteral, falseLiteral))) should equal(Values.FALSE)

    evaluate(compile(and(nullLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(and(nullLiteral, trueLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(and(trueLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(and(nullLiteral, falseLiteral))) should equal(Values.FALSE)
    evaluate(compile(and(falseLiteral, nullLiteral))) should equal(Values.FALSE)
  }

  test("AND should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy evaluate(compile(and(literalInt(42), trueLiteral)))
    a [CypherTypeException] should be thrownBy evaluate(compile(and(trueLiteral, literalInt(42))))
    evaluate(compile(and(falseLiteral, literalInt(42)))) should equal(Values.FALSE)
    evaluate(compile(and(literalInt(42), falseLiteral))) should equal(Values.FALSE)
  }

  test("AND should handle coercion") {
    val expression =  compile(and(parameter(0), parameter(1)))
   evaluate(expression, params(Values.TRUE, EMPTY_LIST)) should equal(Values.FALSE)
   evaluate(expression, params(Values.TRUE, list(stringValue("hello")))) should equal(Values.TRUE)
  }

  test("ANDS") {
    evaluate(compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, trueLiteral))) should equal(Values.TRUE)
    evaluate(compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, trueLiteral, falseLiteral))) should equal(Values.FALSE)
    evaluate(compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, nullLiteral, trueLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(ands(trueLiteral, trueLiteral, trueLiteral, falseLiteral, nullLiteral, falseLiteral))) should equal(Values.FALSE)
  }

  test("ANDS should throw on non-boolean input") {
    val compiled = compile(ands(parameter(0), parameter(1), parameter(2), parameter(3), parameter(4)))
    evaluate(compiled, params(Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE)) should equal(Values.TRUE)

    evaluate(compiled, params(Values.TRUE, Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE)) should equal(Values.FALSE)

    evaluate(compiled, params(intValue(42), Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE)) should equal(Values.FALSE)

    a [CypherTypeException] should be thrownBy evaluate(compiled,
                                                        params(intValue(42), Values.TRUE, Values.TRUE,
                                                               Values.TRUE, Values.TRUE))
  }

  test("ANDS should handle coercion") {
    val expression =  compile(ands(parameter(0), parameter(1)))
    evaluate(expression, params(Values.TRUE, EMPTY_LIST)) should equal(Values.FALSE)
    evaluate(expression, params(Values.TRUE, list(stringValue("hello")))) should equal(Values.TRUE)
  }

  test("NOT") {
    evaluate(compile(not(falseLiteral))) should equal(Values.TRUE)
    evaluate(compile(not(trueLiteral))) should equal(Values.FALSE)
    evaluate(compile(not(nullLiteral))) should equal(Values.NO_VALUE)
  }

  test("NOT should handle coercion") {
    val expression =  compile(not(parameter(0)))
    evaluate(expression, params(EMPTY_LIST)) should equal(Values.TRUE)
    evaluate(expression, params(list(stringValue("hello")))) should equal(Values.FALSE)
  }

  test("EQUALS") {
    evaluate(compile(equals(literalInt(42), literalInt(42)))) should equal(Values.TRUE)
    evaluate(compile(equals(literalInt(42), literalInt(43)))) should equal(Values.FALSE)
    evaluate(compile(equals(nullLiteral, literalInt(43)))) should equal(Values.NO_VALUE)
    evaluate(compile(equals(literalInt(42), nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(equals(nullLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(equals(trueLiteral, equals(trueLiteral, equals(trueLiteral, nullLiteral))))) should equal(Values.NO_VALUE)
  }

  test("NOT EQUALS") {
    evaluate(compile(notEquals(literalInt(42), literalInt(42)))) should equal(Values.FALSE)
    evaluate(compile(notEquals(literalInt(42), literalInt(43)))) should equal(Values.TRUE)
    evaluate(compile(notEquals(nullLiteral, literalInt(43)))) should equal(Values.NO_VALUE)
    evaluate(compile(notEquals(literalInt(42), nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(notEquals(nullLiteral, nullLiteral))) should equal(Values.NO_VALUE)
    evaluate(compile(notEquals(trueLiteral, notEquals(trueLiteral, notEquals(trueLiteral, nullLiteral))))) should equal(Values.NO_VALUE)
  }

  test("regex match on literal pattern") {
    val compiled= compile(regex(parameter(0), literalString("hell.*")))

    evaluate(compiled, params(stringValue("hello"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("helo"))) should equal(Values.FALSE)
    evaluate(compiled, params(Values.NO_VALUE)) should equal(Values.NO_VALUE)
    evaluate(compiled, params(longValue(42))) should equal(Values.NO_VALUE)
  }

  test("regex match on general expression") {
    val compiled= compile(regex(parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("hello"), stringValue("hell.*"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("helo"), stringValue("hell.*"))) should equal(Values.FALSE)
    evaluate(compiled, params(Values.NO_VALUE, stringValue("hell.*"))) should equal(Values.NO_VALUE)
    a [CypherTypeException] should be thrownBy evaluate(compiled,
                                                        params(stringValue("forty-two"), longValue(42)))
    an [InvalidSemanticsException] should be thrownBy evaluate(compiled,
                                                               params(stringValue("hello"), stringValue("[")))
  }

  test("startsWith") {
    val compiled= compile(startsWith(parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("hello"), stringValue("hell"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("hello"), stringValue("hi"))) should equal(Values.FALSE)
    evaluate(compiled, params(stringValue("hello"), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, stringValue("hi"))) should equal(NO_VALUE)
  }

  test("endsWith") {
    val compiled= compile(endsWith(parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("hello"), stringValue("ello"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("hello"), stringValue("hi"))) should equal(Values.FALSE)
    evaluate(compiled, params(stringValue("hello"), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, stringValue("hi"))) should equal(NO_VALUE)
  }

  test("contains") {
    val compiled= compile(contains(parameter(0), parameter(1)))

    evaluate(compiled, params(stringValue("hello"), stringValue("ell"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("hello"), stringValue("hi"))) should equal(Values.FALSE)
    evaluate(compiled, params(stringValue("hello"), NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, stringValue("hi"))) should equal(NO_VALUE)
  }

  test("in") {
    val compiled = compile(in(parameter(0), parameter(1)))

    evaluate(compiled, params(intValue(3), list(intValue(1), intValue(2), intValue(3)))) should equal(Values.TRUE)
    evaluate(compiled, params(intValue(4), list(intValue(1), intValue(2), intValue(3)))) should equal(Values.FALSE)
    evaluate(compiled, params(NO_VALUE, list(intValue(1), intValue(2), intValue(3)))) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, EMPTY_LIST)) should equal(Values.FALSE)
    evaluate(compiled, params(intValue(3), list(intValue(1), NO_VALUE, intValue(3)))) should equal(Values.TRUE)
    evaluate(compiled, params(intValue(4), list(intValue(1), NO_VALUE, intValue(3)))) should equal(Values.NO_VALUE)
    evaluate(compiled, params(intValue(4), NO_VALUE)) should equal(Values.NO_VALUE)
  }

  test("in with literal list not containing null") {
    val compiled = compile(in(parameter(0),
                              listOfString("a", "b", "c")))

    evaluate(compiled, params(stringValue("a"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("b"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("c"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("A"))) should equal(Values.FALSE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("in with literal list containing null") {
    val compiled = compile(in(parameter(0),
                              listOf(literalString("a"), nullLiteral, literalString("c"))))

    evaluate(compiled, params(stringValue("a"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("c"))) should equal(Values.TRUE)
    evaluate(compiled, params(stringValue("b"))) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("in with empty literal list") {
    val compiled = compile(in(parameter(0), listOf()))

    evaluate(compiled, params(stringValue("a"))) should equal(Values.FALSE)
    evaluate(compiled, params(NO_VALUE)) should equal(Values.FALSE)
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
        lessThanOrEqual(literal(left), literal(right)) should compareUsingLessThanOrEqual(left, right)
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
    val compiled= compile(isNull(parameter(0)))

    evaluate(compiled, params(stringValue("hello"))) should equal(Values.FALSE)
    evaluate(compiled, params(NO_VALUE)) should equal(Values.TRUE)
  }

  test("isNull on top of NullCheck") {
    val nullOffset = 0
    val nodeOffset = 1
    val slots = SlotConfiguration(Map(
      "nullNode" -> LongSlot(nullOffset, nullable = true, symbols.CTNode),
      "node" -> LongSlot(nodeOffset, nullable = true, symbols.CTNode)
    ), 2, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(nullOffset, -1)
    context.setLongAt(nodeOffset, nodeValue().id())

    evaluate(compile(isNull(NullCheckVariable(nullOffset, NodeFromSlot(nullOffset, "n"))), slots), context) should equal(Values.TRUE)
    evaluate(compile(isNull(NullCheckVariable(nodeOffset, NodeFromSlot(nodeOffset, "n"))), slots), context) should equal(Values.FALSE)
  }

  test("isNotNull") {
    val compiled= compile(isNotNull(parameter(0)))

    evaluate(compiled, params(stringValue("hello"))) should equal(Values.TRUE)
    evaluate(compiled, params(NO_VALUE)) should equal(Values.FALSE)
  }

  test("isNotNull on top of NullCheck") {
    val nullOffset = 0
    val nodeOffset = 1
    val slots = SlotConfiguration(Map(
      "nullNode" -> LongSlot(nullOffset, nullable = true, symbols.CTNode),
      "node" -> LongSlot(nodeOffset, nullable = true, symbols.CTNode)
    ), 2, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(nullOffset, -1)
    context.setLongAt(nodeOffset, nodeValue().id())

    evaluate(compile(isNotNull(NullCheckVariable(nullOffset, NodeFromSlot(nullOffset, "n"))), slots), context) should equal(Values.FALSE)
    evaluate(compile(isNotNull(NullCheckVariable(nodeOffset, NodeFromSlot(nodeOffset, "n"))), slots), context) should equal(Values.TRUE)
  }

  test("CoerceToPredicate") {
    val coerced = CoerceToPredicate(parameter(0))

    evaluate(compile(coerced), params(Values.FALSE)) should equal(Values.FALSE)
    evaluate(compile(coerced), params(Values.TRUE)) should equal(Values.TRUE)
    evaluate(compile(coerced), params(list(stringValue("A")))) should equal(Values.TRUE)
    evaluate(compile(coerced), params(list(EMPTY_LIST))) should equal(Values.TRUE)
  }

  test("ReferenceFromSlot") {
    // Given
    val offset = 0
    val slots = SlotConfiguration(Map("foo" -> RefSlot(offset, nullable = true, symbols.CTAny)), 0, 1)
    val context = SlottedExecutionContext(slots)
    context.setRefAt(offset, stringValue("hello"))
    val expression = ReferenceFromSlot(offset, "foo")

    // When
    val compiled = compile(expression, slots)

    // Then
    evaluate(compiled, context) should equal(stringValue("hello"))
  }

  test("IdFromSlot") {
    // Given
    val offset = 0
    val expression = IdFromSlot(offset)
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(offset, 42L)

    // When
    val compiled = compile(expression, slots)

    // Then
    evaluate(compiled, context) should equal(longValue(42))
  }

  test("LabelsFromSlot") {
    // Given
    val labels = Values.stringArray("A", "B", "C")
    val node = ValueUtils.fromNodeProxy(createLabeledNode("A", "B", "C"))

    val offset = 0
    val expression = LabelsFromSlot(offset)
    val slots = SlotConfiguration.empty.newLong("n", nullable = true, symbols.CTNode)
    val context = SlottedExecutionContext(slots)

    // When
    val compiled = compile(expression, slots)

    // Then
    context.setLongAt(offset, node.id)
    evaluate(compiled, context) should equal(labels)
  }

  test("RelationshipTypeFromSlot") {
    // Given
    val relType = Values.stringValue("R")
    val r = ValueUtils.fromRelationshipProxy(relate(createNode(), createNode(), "R"))

    val offset = 0
    val expression = RelationshipTypeFromSlot(offset)
    val slots = SlotConfiguration.empty.newLong("r", nullable = true, symbols.CTRelationship)
    val context = SlottedExecutionContext(slots)

    // When
    val compiled = compile(expression, slots)

    // Then
    context.setLongAt(offset, r.id)
    evaluate(compiled, context) should equal(relType)
  }

  test("PrimitiveEquals") {
    val compiled = compile(PrimitiveEquals(parameter(0), parameter(1)))

    evaluate(compiled, params(longValue(42), longValue(42))) should
      equal(Values.TRUE)
    evaluate(compiled, params(longValue(42), longValue(1337))) should
      equal(Values.FALSE)
  }

  test("NullCheck") {
    val nullOffset = 0
    val offset = 1
    val slots = SlotConfiguration(Map("n1" -> LongSlot(0, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(1, nullable = true, symbols.CTNode)), 2, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(nullOffset, -1L)
    context.setLongAt(offset, 42L)

    evaluate(compile(NullCheck(nullOffset, literalFloat(PI)), slots), context) should equal(Values.NO_VALUE)
    evaluate(compile(NullCheck(offset, literalFloat(PI)), slots), context) should equal(Values.PI)
  }

  test("NullCheckVariable") {
    val notNullOffset = 0
    val nullOffset = 1
    val slots = SlotConfiguration(Map(
      "aRef" -> RefSlot(0, nullable = true, symbols.CTNode),
      "notNull" -> LongSlot(notNullOffset, nullable = true, symbols.CTNode),
      "null" -> LongSlot(nullOffset, nullable = true, symbols.CTNode)), 2, 1)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(nullOffset, -1)
    context.setLongAt(notNullOffset, 42L)
    context.setRefAt(0, stringValue("hello"))

    evaluate(compile(NullCheckVariable(1, ReferenceFromSlot(0, "aRef")), slots), context) should
      equal(Values.NO_VALUE)
    evaluate(compile(NullCheckVariable(0, ReferenceFromSlot(0, "aRef")), slots), context) should
      equal(stringValue("hello"))
  }

  test("IsPrimitiveNull") {
    val notNullOffset = 0
    val nullOffset = 1
    val slots = SlotConfiguration(Map(
      "notNull" -> LongSlot(notNullOffset, nullable = true, symbols.CTNode),
      "null" -> LongSlot(nullOffset, nullable = true, symbols.CTNode)), 2, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(nullOffset, -1)
    context.setLongAt(notNullOffset, 42L)

    evaluate(compile(IsPrimitiveNull(nullOffset), slots), context) should equal(Values.TRUE)
    evaluate(compile(IsPrimitiveNull(notNullOffset)), context) should equal(Values.FALSE)
  }

  test("containerIndex on node") {
    val node =  nodeValue(VirtualValues.map(Array("prop"), Array(stringValue("hello"))))
    val compiled = compile(containerIndex(parameter(0), literalString("prop")))

    evaluate(compiled, params(node)) should equal(stringValue("hello"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("containerIndex on relationship") {
    val rel = relationshipValue(nodeValue(),
                                nodeValue(),
                                VirtualValues.map(Array("prop"), Array(stringValue("hello"))))
    val compiled = compile(containerIndex(parameter(0), literalString("prop")))

    evaluate(compiled, params(rel)) should equal(stringValue("hello"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("containerIndex on map") {
    val mapValue = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val compiled = compile(containerIndex(parameter(0), literalString("prop")))

    evaluate(compiled, params(mapValue)) should equal(stringValue("hello"))
    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("containerIndex on list") {
    val listValue = list(longValue(42), stringValue("hello"), intValue(42))
    val compiled = compile(containerIndex(parameter(0), parameter(1)))

    evaluate(compiled, params(listValue, intValue(1))) should equal(stringValue("hello"))
    evaluate(compiled, params(listValue, intValue(-1))) should equal(intValue(42))
    evaluate(compiled, params(listValue, intValue(3))) should equal(NO_VALUE)
    evaluate(compiled, params(NO_VALUE, intValue(1))) should equal(NO_VALUE)
    an [InvalidArgumentException] should be thrownBy evaluate(compiled, params(listValue, longValue(Int.MaxValue + 1L)))
  }

  test("handle list literals") {
    val literal = listOf(trueLiteral, literalInt(5), nullLiteral, falseLiteral)

    val compiled = compile(literal)

    evaluate(compiled) should equal(list(Values.TRUE, intValue(5), NO_VALUE, Values.FALSE))
  }

  test("handle map literals") {
    val literal = mapOfInt("foo" -> 1, "bar" -> 2, "baz" -> 3)

    val compiled = compile(literal)

    import scala.collection.JavaConverters._
    evaluate(compiled) should equal(ValueUtils.asMapValue(Map("foo" -> 1, "bar" -> 2, "baz" -> 3).asInstanceOf[Map[String, AnyRef]].asJava))
  }

  test("handle map literals with null") {
    val literal = mapOf("foo" -> literalInt(1), "bar" -> nullLiteral, "baz" -> literalString("three"))

    val compiled = compile(literal)

    import scala.collection.JavaConverters._
    evaluate(compiled) should equal(ValueUtils.asMapValue(Map("foo" -> 1, "bar" -> null, "baz" -> "three").asInstanceOf[Map[String, AnyRef]].asJava))
  }

  test("handle empty map literals") {
    val literal = mapOf()

    val compiled = compile(literal)

    evaluate(compiled) should equal(EMPTY_MAP)
  }

  test("from slice") {
    val slice = compile(sliceFrom(parameter(0), parameter(1)))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    evaluate(slice, params(NO_VALUE, intValue(3))) should equal(NO_VALUE)
    evaluate(slice, params(list, NO_VALUE)) should equal(NO_VALUE)
    evaluate(slice, params(list, intValue(2))) should equal(VirtualValues.list(intValue(3)))
    evaluate(slice, params(list, intValue(-2))) should equal(VirtualValues.list(intValue(2), intValue(3)))
    evaluate(slice, params(list, intValue(0))) should equal(list)
  }

  test("to slice") {
    val slice = compile(sliceTo(parameter(0), parameter(1)))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    evaluate(slice, params(NO_VALUE, intValue(1))) should equal(NO_VALUE)
    evaluate(slice, params(list, NO_VALUE)) should equal(NO_VALUE)
    evaluate(slice, params(list, intValue(2))) should equal(VirtualValues.list(intValue(1), intValue(2)))
    evaluate(slice, params(list, intValue(-2))) should equal(VirtualValues.list(intValue(1)))
    evaluate(slice, params(list, intValue(0))) should equal(EMPTY_LIST)
  }

  test("full slice") {
    val slice = compile(sliceFull(parameter(0), parameter(1), parameter(2)))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3), intValue(4), intValue(5))

    evaluate(slice, params(NO_VALUE, intValue(1), intValue(3))) should equal(NO_VALUE)
    evaluate(slice, params(list, NO_VALUE, intValue(3))) should equal(NO_VALUE)
    evaluate(slice, params(list, intValue(3), NO_VALUE)) should equal(NO_VALUE)
    evaluate(slice, params(list, intValue(1), intValue(3))) should equal(VirtualValues.list(intValue(2), intValue(3)))
    evaluate(slice, params(list, intValue(1), intValue(-2))) should equal(VirtualValues.list(intValue(2), intValue(3)))
    evaluate(slice, params(list, intValue(-4), intValue(3))) should equal(VirtualValues.list(intValue(2), intValue(3)))
    evaluate(slice, params(list, intValue(-4), intValue(-2))) should equal(VirtualValues.list(intValue(2), intValue(3)))
    evaluate(slice, params(list, intValue(0), intValue(0))) should equal(EMPTY_LIST)
  }

  test("handle variables") {
    val variable = varFor("key")
    val compiled = compile(variable)
    val context = new MapExecutionContext(mutable.Map("key" -> stringValue("hello")))
    evaluate(compiled, context) should equal(stringValue("hello"))
  }

  test("handle variables with whitespace ") {
    val varName = "   k\te\ty   "
    val variable = varFor(varName)
    val compiled = compile(variable)
    val context = new MapExecutionContext(mutable.Map(varName -> stringValue("hello")))
    evaluate(compiled, context) should equal(stringValue("hello"))
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
    val n = nodeValue()
    coerce(n, symbols.CTNode) should equal(n)
    val r = relationshipValue()
    coerce(r, symbols.CTRelationship) should equal(r)
    val p = path(5)
    coerce(p, symbols.CTPath) should equal(p)

    //maps
    val mapValue = VirtualValues.map(Array("prop"), Array(longValue(1337)))
    coerce(mapValue, symbols.CTMap) should equal(mapValue)
    coerce(nodeValue(mapValue), symbols.CTMap) should equal(mapValue)
    coerce(relationshipValue(mapValue), symbols.CTMap) should equal(mapValue)

    //list
    coerce(list(longValue(42), longValue(43)), ListType(symbols.CTAny)) should equal(list(longValue(42), longValue(43)))

    val value = path(7)
    coerce(value, ListType(symbols.CTAny)) should equal(value.asList())
    val nodeValues = list(nodeValue(), nodeValue())
    coerce(nodeValues, ListType(symbols.CTNode)) should equal(nodeValues)
    val relationshipValues = list(relationshipValue(), relationshipValue())
    coerce(relationshipValues, ListType(symbols.CTRelationship)) should equal(relationshipValues)
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
    types().foreach {
      case (v, typ) =>
        coerce(list(v), ListType(typ)) should equal(list(v))
        coerce(list(list(v)), ListType(ListType(typ))) should equal(list(list(v)))
        coerce(list(list(list(v))), ListType(ListType(ListType(typ)))) should equal(list(list(list(v))))
    }
  }

  test("coerceTo unhappy path") {
    val all = types()
    for {value <- all.keys
          typ <- all.values} {
      if (all(value) == typ) coerce(value, typ) should equal(value)
      else a [CypherTypeException] should be thrownBy coerce(value, typ)
    }
  }

  test("access property on node") {
    val compiled = compile(prop(parameter(0), "prop"))

    val node = nodeValue(VirtualValues.map(Array("prop"), Array(stringValue("hello"))))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(node)) should equal(stringValue("hello"))
  }

  test("access property on relationship") {
    val compiled = compile(prop(parameter(0), "prop"))

    val rel = relationshipValue(VirtualValues.map(Array("prop"), Array(stringValue("hello"))))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(rel)) should equal(stringValue("hello"))
  }

  test("access property on map") {
    val compiled = compile(prop(parameter(0), "prop"))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(VirtualValues.map(Array("prop"), Array(stringValue("hello"))))) should equal(stringValue("hello"))
  }

  test("access property on temporal") {
    val value = TimeValue.now(Clock.systemUTC())
    val compiled = compile(prop(parameter(0), "timezone"))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(value)) should equal(value.get("timezone"))
  }

  test("access property on duration") {
    val value = durationValue(Duration.ofHours(3))
    val compiled = compile(prop(parameter(0), "seconds"))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(value)) should equal(value.get("seconds"))
  }

  test("access property on point") {
    val value = pointValue(Cartesian, 1.0, 3.6)

    val compiled = compile(prop(parameter(0), "x"))

    evaluate(compiled, params(NO_VALUE)) should equal(NO_VALUE)
    evaluate(compiled, params(value)) should equal(doubleValue(1.0))
  }

  test("access property on point with invalid key") {
    val value = pointValue(Cartesian, 1.0, 3.6)

    val compiled = compile(prop(parameter(0), "foobar"))

    an[InvalidArgumentException] should be thrownBy evaluate(compiled, params(value))
  }

  test("should project") {
    //given
    val slots = SlotConfiguration(Map("a" -> RefSlot(0, nullable = true, symbols.CTAny),
                                      "b" -> RefSlot(1, nullable = true, symbols.CTAny)), 0, 2)
    val context = new SlottedExecutionContext(slots)
    val projections = Map("a" -> literal("hello"), "b" -> function("sin", parameter(0)))
    val compiled = compileProjection(projections, slots)

    //when
    compiled.project(context, query, params(NO_VALUE), cursors, expressionVariables)

    //then
    context.getRefAt(0) should equal(stringValue("hello"))
    context.getRefAt(1) should equal(NO_VALUE)
  }

  test("single in list basic") {
    //When
    val bar = ExpressionVariable(0, "bar")

    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "b")
    val compiledNone = compile(singleInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, literalString("b"))))

    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aaa")
    val compiledSingle = compile(singleInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, literalString("aaa"))))

    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "a")
    val compiledMany = compile(singleInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, literalString("a"))))

    //Then
    evaluate(compiledNone, 1) should equal(booleanValue(false))
    evaluate(compiledSingle, 1) should equal(booleanValue(true))
    evaluate(compiledMany, 1) should equal(booleanValue(false))
  }

  test("single in list accessing variable") {
    //Given
    val context = new MapExecutionContext(mutable.Map(
      "b" -> stringValue("b"),
      "a" -> stringValue("a"),
      "aaa" -> stringValue("aaa")))

    //When
    val bar = ExpressionVariable(0, "bar")

    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "b")
    val compiledNone = compile(singleInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, varFor("b"))))

    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aaa")
    val compiledSingle = compile(singleInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, varFor("aaa"))))

    // single(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "a")
    val compiledMany = compile(singleInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, varFor("a"))))

    //Then
    evaluate(compiledNone, 1, Array.empty, context) should equal(booleanValue(false))
    evaluate(compiledSingle, 1, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledMany, 1, Array.empty, context) should equal(booleanValue(false))
  }

  test("single in list on null") {
    //When, single(bar IN null WHERE bar = foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(singleInList(bar, nullLiteral,
                                        equals(bar, varFor("foo"))))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("single in list with null predicate") {
    //When, single(bar IN ['a','aa','aaa'] WHERE bar = null)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(singleInList(bar, listOfString("a", "aa", "aaa"), equals(bar, nullLiteral)))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("single in list accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When, single(bar IN foo WHERE size(bar) = size(foo))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(singleInList(bar, varFor("foo"),
                                        equals(function("size", bar), function("size", varFor("foo")))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(Values.TRUE)
  }

  test("single in list accessing the same parameter in inner and outer") {
    //Given
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When, single(bar IN $a WHERE size(bar) = size($a))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(singleInList(bar, parameter(0),
                                        equals(function("size", bar), function("size", parameter(0)))))

    //Then
    evaluate(compiled, 1, params(list)) should equal(Values.TRUE)
  }

  test("single in list on empty list") {
    //When, single(bar IN [] WHERE bar = 42)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(singleInList(bar, listOf(), equals(literalInt(42), bar)))

    //Then
    evaluate(compiled, 1) should equal(Values.FALSE)
  }


  test("none in list function basic") {
    //When
    val bar = ExpressionVariable(0, "bar")

    // none(bar IN ["a", "aa", "aaa"] WHERE bar = "b")
    val compiledTrue = compile(noneInList(bar, listOfString("a", "aa", "aaa"), equals(bar, literalString("b"))))

    // none(bar IN ["a", "aa", "aaa"] WHERE bar = "a")
    val compiledFalse = compile(noneInList(bar, listOfString("a", "aa", "aaa"), equals(bar, literalString("a"))))

    //Then
    evaluate(compiledTrue, 1) should equal(booleanValue(true))
    evaluate(compiledFalse, 1) should equal(booleanValue(false))
  }

  test("none in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("a" -> stringValue("a"), "b" -> stringValue("b")))

    //When
    val bar = ExpressionVariable(0, "bar")

    // none(bar IN ["a", "aa", "aaa"] WHERE bar = b)
    val compiledTrue = compile(noneInList(bar, listOfString("a", "aa", "aaa"), equals(bar, varFor("b"))))

    // none(bar IN ["a", "aa", "aaa"] WHERE bar = a)
    val compiledFalse = compile(noneInList(bar, listOfString("a", "aa", "aaa"), equals(bar, varFor("a"))))

    //Then
    evaluate(compiledTrue, 1, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledFalse, 1, Array.empty, context) should equal(booleanValue(false))
  }

  test("none in list on null") {
    //When, none(bar IN null WHERE bar = foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(noneInList(bar, nullLiteral, equals(bar, varFor("foo"))))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("none in list with null predicate") {
    //When, none(bar IN null WHERE bar = null)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(noneInList(bar, listOfString("a", "aa", "aaa"), equals(bar, nullLiteral )))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("none in list accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  none(bar IN foo WHERE size(bar) = size(foo))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(noneInList(bar, varFor("foo"),
                                      equals(function("size", bar), function("size", varFor("foo")))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(FALSE)
  }

  test("none in list accessing the same parameter in inner and outer") {
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  none(bar IN $a WHERE size(bar) = size($a))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(noneInList(bar, parameter(0),
                                      equals(function("size", bar), function("size", parameter(0)))))

    //Then
    evaluate(compiled, 1, params(list)) should equal(FALSE)
  }

  test("none in list on empty list") {
    //When, none(bar IN [] WHERE bar = 42)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(noneInList(bar, listOf(),
                                      equals(bar, literalInt(42))))

    //Then
    evaluate(compiled, 1) should equal(Values.TRUE)
  }

  test("any in list function basic") {
    //When
    val bar = ExpressionVariable(0, "bar")

    // any(bar IN ["a", "aa", "aaa"] WHERE bar = "a")
    val compiledTrue = compile(anyInList(bar, listOfString("a", "aa", "aaa"), equals(bar, literalString("a"))))

    // any(bar IN ["a", "aa", "aaa"] WHERE bar = "b")
    val compiledFalse = compile(anyInList(bar, listOfString("a", "aa", "aaa"), equals(bar, literalString("b"))))

    //Then
    evaluate(compiledTrue, 1) should equal(booleanValue(true))
    evaluate(compiledFalse, 1) should equal(booleanValue(false))
  }

  test("any in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("a" -> stringValue("a"), "b" -> stringValue("b")))

    //When
    val bar = ExpressionVariable(0, "bar")

    // any(bar IN ["a", "aa", "aaa"] WHERE bar = a)
    val compiledTrue = compile(anyInList(bar, listOfString("a", "aa", "aaa"), equals(bar, varFor("a"))))

    // any(bar IN ["a", "aa", "aaa"] WHERE bar = aa)
    val compiledFalse = compile(anyInList(bar, listOfString("a", "aa", "aaa"), equals(bar, varFor("b"))))

    //Then
    evaluate(compiledTrue, 1, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledFalse, 1, Array.empty, context) should equal(booleanValue(false))
  }

  test("any in list on null") {
    //When, any(bar IN null WHERE bar = foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(anyInList(bar, nullLiteral, equals(bar, varFor("foo"))))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("any in list with null predicate") {
    //When, any(bar IN ['a','aa','aaa'] WHERE bar = null)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(anyInList(bar, listOfString("a", "aa", "aaa"), equals(bar, nullLiteral)))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("any in list accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  any(bar IN foo WHERE size(bar) = size(foo))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(anyInList(bar, varFor("foo"),
                                     equals(function("size", bar), function("size", varFor("foo")))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(Values.TRUE)
  }

  test("any in list accessing the same parameter in inner and outer") {
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  any(bar IN $a WHERE size(bar) = size($a))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(anyInList(bar, parameter(0),
                                     equals(function("size", bar), function("size", parameter(0)))))

    //Then
    evaluate(compiled, 1, params(list)) should equal(Values.TRUE)
  }

  test("any in list on empty list") {
    //When, any(bar IN [] WHERE bar = 42)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(anyInList(bar, listOf(), equals(bar, literalInt(42))))

    //Then
    evaluate(compiled, 1) should equal(Values.FALSE)
  }

  test("all in list function basic") {
    //When
    val bar = ExpressionVariable(0, "bar")

    // all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "a")
    val compiledTrue = compile(allInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, literalString("a"))))

    //all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aa")
    val compiledFalse = compile(allInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, literalString("aa"))))

    //Then
    evaluate(compiledTrue, 1) should equal(booleanValue(true))
    evaluate(compiledFalse, 1) should equal(booleanValue(false))
  }

  test("all in list function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("a" -> stringValue("a"), "aa" -> stringValue("aa")))

    //When
    val bar = ExpressionVariable(0, "bar")

    // all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH a)
    val compiledTrue = compile(allInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, varFor("a"))))

    //all(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH aa)
    val compiledFalse = compile(allInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, varFor("aa"))))

    //Then
    evaluate(compiledTrue, 1, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledFalse, 1, Array.empty, context) should equal(booleanValue(false))
  }

  test("all in list on null") {
    //When, all(bar IN null WHERE bar STARTS WITH foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(allInList(bar, nullLiteral, startsWith(bar, varFor("foo"))))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("all in list with null predicate") {
    //When, all(bar IN null WHERE bar STARTS WITH null)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(allInList(bar, listOfString("a", "aa", "aaa"), startsWith(bar, nullLiteral)))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("all in list accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When, all(bar IN foo WHERE size(bar) = size(foo))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(allInList(bar, varFor("foo"),
                                     equals(function("size", bar), function("size", varFor("foo")))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(FALSE)
  }

  test("all in list accessing the same parameter in inner and outer") {
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  all(bar IN $a WHERE size(bar) = size($a))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(allInList(bar, parameter(0),
                                     equals(function("size", bar), function("size", parameter(0)))))

    //Then
    evaluate(compiled, 1, params(list)) should equal(Values.FALSE)
  }

  test("all in list on empty list") {
    //When, all(bar IN [] WHERE bar = 42)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(allInList(bar, listOf(), equals(bar, literalInt(42))))

    //Then
    evaluate(compiled, 1) should equal(Values.TRUE)
  }

  test("filter basic") {
    //When, filter(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aa")
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, listOfString("a", "aa", "aaa"), startsWith(bar, literalString("aa"))))

    //Then
    evaluate(compiled, 1) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("filter accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> stringValue("aa")))

    //When, filter(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, listOfString("a", "aa", "aaa"), startsWith(bar, varFor("foo"))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("filter on null") {
    //When, filter(bar IN null WHERE bar STARTS WITH 'aa')
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, nullLiteral, startsWith(bar, varFor("aa"))))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("filter with null predicate") {
    //When, filter(bar IN null WHERE bar STARTS WITH null)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, listOfString("a", "aa", "aaa"), startsWith(bar, nullLiteral)))

    //Then
    evaluate(compiled, 1) should equal(list())
  }

  test("filter accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  filter(bar IN foo WHERE size(bar) = size(foo))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, varFor("foo"),
                                  equals(function("size", bar), function("size", varFor("foo")))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(VirtualValues.list(stringValue("aaa")))
  }

  test("filter accessing the same parameter in inner and outer") {
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  filter(bar IN $a WHERE size(bar) = size($a))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, parameter(0),
                                  equals(function("size", bar), function("size", parameter(0)))))

    //Then
    evaluate(compiled, 1, params(list)) should equal(VirtualValues.list(stringValue("aaa")))
  }

  test("filter on empty list") {
    //When, filter(bar IN [] WHERE bar = 42)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(filter(bar, listOf(), equals(bar, literalInt(42))))

    //Then
    evaluate(compiled, 1) should equal(EMPTY_LIST)
  }

  test("nested list expressions basic") {

    //When
    val bar = ExpressionVariable(0, "bar")
    val foo = ExpressionVariable(1, "foo")
    // none(bar IN ["a"] WHERE any(foo IN ["b"] WHERE bar = foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = bar,
        collection = listOfString("a"),
        predicate = anyInList(
          variable = foo,
          collection = listOfString("b"),
          predicate = equals(bar, foo))))

    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = bar,
        collection = listOfString("a"),
        predicate = anyInList(
          variable = foo,
          collection = listOfString("a"),
          predicate = equals(bar, foo))))

    //Then
    evaluate(compiledTrue, 2) should equal(booleanValue(true))
    evaluate(compiledFalse, 2) should equal(booleanValue(false))
  }

  test("nested list expressions, outer expression accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("list" -> list(stringValue("a"))))

    //When
    val bar = ExpressionVariable(0, "bar")
    val foo = ExpressionVariable(1, "foo")

    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar <> foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = bar,
        collection = varFor("list"),
        predicate = anyInList(
          variable = foo,
          collection = listOfString("a"),
          predicate = notEquals(bar, foo))))

    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = bar,
        collection = varFor("list"),
        predicate = anyInList(
          variable = foo,
          collection = listOfString("a"),
          predicate = equals(bar, foo))))

    //Then
    evaluate(compiledTrue, 2, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledFalse, 2, Array.empty, context) should equal(booleanValue(false))
  }

  test("nested list expressions, inner expression accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("list" -> list(stringValue("a"))))

    //When
    val bar = ExpressionVariable(0, "bar")
    val foo = ExpressionVariable(1, "foo")

    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar <> foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = bar,
        collection = listOfString("a"),
        predicate = anyInList(
          variable = foo,
          collection = listOfString("a"),
          predicate = notEquals(bar, foo))))
    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false

    val compiledFalse = compile(
      noneInList(
        variable = bar,
        collection = varFor("list"),
        predicate = anyInList(
          variable = foo,
          collection = varFor("list"),
          predicate = equals(bar, foo))))

    //Then
    evaluate(compiledTrue, 2, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledFalse, 2, Array.empty, context) should equal(booleanValue(false))
  }

  test("nested list expressions, both accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("list" -> list(stringValue("a"))))

    //When
    val bar = ExpressionVariable(0, "bar")
    val foo = ExpressionVariable(1, "foo")

    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar <> foo)) --> true
    val compiledTrue = compile(
      noneInList(
        variable = bar,
        collection = varFor("list"),
        predicate = anyInList(
          variable = foo,
          collection = varFor("list"),
          predicate = notEquals(bar, foo))))

    // none(bar IN ["a"] WHERE any(foo IN ["a"] WHERE bar = foo)) --> false
    val compiledFalse = compile(
      noneInList(
        variable = bar,
        collection = varFor("list"),
        predicate = anyInList(
          variable = foo,
          collection = varFor("list"),
          predicate = equals(bar, foo))))

    //Then
    evaluate(compiledTrue, 2, Array.empty, context) should equal(booleanValue(true))
    evaluate(compiledFalse, 2, Array.empty, context) should equal(booleanValue(false))
  }

  test("extract basic") {
    //When, extract(bar IN ["a", "aa", "aaa"] | size(bar))
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(extract(bar, listOfString("a", "aa", "aaa"), function("size", bar)))

    //Then
    evaluate(compiled, 1) should equal(list(intValue(1), intValue(2), intValue(3)))
  }

  test("extract accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> intValue(10)), mutable.Map.empty)

    //When, extract(bar IN [1, 2, 3] | bar + foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(extract(bar, listOfInt(1, 2, 3),
                                   add(varFor("foo"), bar)))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(list(intValue(11), intValue(12), intValue(13)))
  }

  test("extract on null") {
    //When, extract(bar IN null | size(bar)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(extract(bar, nullLiteral, function("size", bar)))

    //Then
    evaluate(compiled, 1) should equal(NO_VALUE)
  }

  test("extract accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(intValue(1), intValue(2), intValue(3))), mutable.Map.empty)

    //When, extract(bar IN foo | size(foo)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(extract(bar, varFor("foo"), function("size", varFor("foo"))))

    //Then
    evaluate(compiled, 1, Array.empty, context) should equal(VirtualValues.list(intValue(3), intValue(3), intValue(3)))
  }

  test("extract accessing the same parameter in inner and outer") {
    //Given
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    //When, extract(bar IN $a | size($a)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(extract(bar, parameter(0), function("size", parameter(0))))

    //Then
    evaluate(compiled, 1, params(list)) should equal(VirtualValues.list(intValue(3), intValue(3), intValue(3)))
  }

  test("extract on empty list") {
    //When, extract(bar IN [] | bar = 42)
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(extract(bar, listOf(), equals(bar, literalInt(42))))

    //Then
    evaluate(compiled, 1) should equal(EMPTY_LIST)
  }

  test("reduce basic") {
    //Given
    val count = ExpressionVariable(0, "count")
    val bar = ExpressionVariable(1, "bar")
    val reduceExpression = reduce(count, literalInt(0), bar, parameter(0), add(function("size", bar), count))

    //When, reduce(count = 0, bar IN ["a", "aa", "aaa"] | count + size(bar))
    val compiled = compile(reduceExpression)

    //Then
    evaluate(compiled, 2, params(list(stringValue("a"), stringValue("aa"), stringValue("aaa"))), ctx) should equal(intValue(6))
    evaluate(compiled, 2, params(NO_VALUE)) should equal(NO_VALUE)
  }

  test("reduce accessing variable") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> intValue(10)), mutable.Map.empty)

    //When, reduce(count = 0, bar IN [1, 2, 3] | count + bar + foo)
    val count = ExpressionVariable(0, "count")
    val bar = ExpressionVariable(1, "bar")
    val compiled = compile(reduce(count, literalInt(0), bar,
                                  listOfInt(1, 2, 3),
                                  add(add(varFor("foo"), bar), count)))

    //Then
    evaluate(compiled, 2, Array.empty, context) should equal(intValue(36))
  }

  test("reduce on null") {
    //When, reduce(count = 0, bar IN null | count + size(bar))
    val count = ExpressionVariable(0, "count")
    val bar = ExpressionVariable(1, "bar")
    val compiled = compile(reduce(count, literalInt(0), bar, nullLiteral,
                                  add(function("size", bar), count)))

    //Then
    evaluate(compiled, 2) should equal(NO_VALUE)
  }

  test("reduce accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(intValue(1), intValue(2), intValue(3))), mutable.Map.empty)

    //When, reduce(count = 0, bar IN foo | count + size(foo)
    val count = ExpressionVariable(0, "count")
    val bar = ExpressionVariable(1, "bar")
    val compiled = compile(reduce(count, literalInt(0), bar, varFor("foo"),
                                  add(function("size", varFor("foo")), count)))

    //Then
    evaluate(compiled, 2, Array.empty, context) should equal(intValue(9))
  }

  test("reduce accessing the same parameter in inner and outer") {
    //When, reduce(count = 0, bar IN $a | count + size($a))
    val count = ExpressionVariable(0, "count")
    val bar = ExpressionVariable(1, "bar")
    val compiled = compile(reduce(count, literalInt(0), bar, parameter(0),
                                  add(function("size", parameter(0)), count)))

    //Then
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))
    evaluate(compiled, 2, params(list)) should equal(intValue(9))
  }

  test("reduce on empty list") {
    //When, reduce(count = 42, bar IN [] | count + 3)
    val count = ExpressionVariable(0, "count")
    val bar = ExpressionVariable(1, "bar")
    val compiled = compile(reduce(count, literalInt(42), bar, listOf(),
                                  add(literalInt(3), count)))

    //Then
    evaluate(compiled, 2) should equal(Values.intValue(42))
  }

  test("list comprehension with predicate and extract expression") {
    //When, [bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH 'aa' | bar + 'A']
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(listComprehension(bar, listOfString("a", "aa", "aaa"),
                                             predicate = Some(startsWith(bar, literalString("aa"))),
                                             extractExpression = Some(add(bar, literalString("A")))))

    //Then
    evaluate(compiled, 1) should equal(list(stringValue("aaA"), stringValue("aaaA")))
  }

  test("list comprehension with no predicate but an extract expression") {
    //When, [bar IN ["a", "aa", "aaa"] | bar + 'A']
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(listComprehension(bar, listOfString("a", "aa", "aaa"),
                                             predicate = None,
                                             extractExpression = Some(add(bar, literalString("A")))))

    //Then
    evaluate(compiled, 1) should equal(list(stringValue("aA"), stringValue("aaA"), stringValue("aaaA")))
  }

  test("list comprehension with predicate but no extract expression") {
    //When, [bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH 'aa']
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(listComprehension(bar, listOfString("a", "aa", "aaa"),
                                             predicate = Some(startsWith(bar, literalString("aa"))),
                                             extractExpression = None))

    //Then
    evaluate(compiled, 1) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("list comprehension with no predicate nor extract expression") {
    //When, [bar IN ["a", "aa", "aaa"]]
    val bar = ExpressionVariable(0, "bar")
    val compiled = compile(listComprehension(bar, listOfString("a", "aa", "aaa"),
                                             predicate = None,
                                             extractExpression = None))

    //Then
    evaluate(compiled, 1) should equal(list(stringValue("a"), stringValue("aa"), stringValue("aaa")))
  }

  test("simple case expressions") {
    val alts = List(literalInt(42) -> literalString("42"), literalInt(1337) -> literalString("1337"))

    evaluate(compile(simpleCase(parameter(0), alts)),
             parameters("a" -> intValue(42))) should equal(stringValue("42"))
    evaluate(compile(simpleCase(parameter(0), alts)),
             parameters("a" -> intValue(1337))) should equal(stringValue("1337"))
    evaluate(compile(simpleCase(parameter(0), alts)),
             parameters("a" -> intValue(-1))) should equal(NO_VALUE)
    evaluate(compile(simpleCase(parameter(0), alts, Some(literalString("THIS IS THE DEFAULT")))),
             parameters("a" -> intValue(-1))) should equal(stringValue("THIS IS THE DEFAULT"))
  }

  test("generic case expressions") {
    evaluate(compile(genericCase(List(falseLiteral -> literalString("no"), trueLiteral -> literalString("yes"))))
      ) should equal(stringValue("yes"))
    evaluate(compile(genericCase(List(trueLiteral -> literalString("no"), falseLiteral -> literalString("yes"))))
      ) should equal(stringValue("no"))
    evaluate(compile(genericCase(List(falseLiteral -> literalString("no"), falseLiteral -> literalString("yes"))))
      ) should equal(NO_VALUE)
    evaluate(compile(genericCase(List(falseLiteral -> literalString("no"), falseLiteral -> literalString("yes")), Some(literalString("default"))))
      ) should equal(stringValue("default"))
  }

  test("map projection node with map context") {
      val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
      val node = nodeValue(propertyMap)
      val context = new MapExecutionContext(mutable.Map("n" -> node))

      evaluate(compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected"))),
         context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      evaluate(compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected"))),
         context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection node from long slot") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val node = nodeValue(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> LongSlot(offset, nullable, symbols.CTNode)), 1, 0)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(offset, node.id())
      evaluate(compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots),
         context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      evaluate(compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots),
         context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection node from ref slot") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val node = nodeValue(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> RefSlot(offset, nullable, symbols.CTNode)), 0, 1)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(offset, node)
      evaluate(compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots),
         context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      evaluate(compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots),
         context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection relationship with map context") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val relationship = relationshipValue(nodeValue(),
                                         nodeValue(), propertyMap)
    val context = new MapExecutionContext(mutable.Map("r" -> relationship))
    evaluate(compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected"))),
       context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
    evaluate(compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected"))),
       context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection relationship from long slot") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val relationship = relationshipValue(nodeValue(),
                                         nodeValue(), propertyMap)

    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("r" -> LongSlot(offset, nullable, symbols.CTRelationship)), 1, 0)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(offset, relationship.id())
      evaluate(compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")), slots),
         context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      evaluate(compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")), slots),
         context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection relationship from ref slot") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val relationship = relationshipValue(nodeValue(),
                                         nodeValue(), propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("r" -> RefSlot(offset, nullable, symbols.CTRelationship)), 0, 1)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(offset, relationship)
      evaluate(compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")), slots),
         context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      evaluate(compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")), slots),
         context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection mapValue with map context") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val context = new MapExecutionContext(mutable.Map("map" -> propertyMap))

    evaluate(compile(mapProjection("map", includeAllProps = true, "foo" -> literalString("projected"))),
       context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
    evaluate(compile(mapProjection("map", includeAllProps = false, "foo" -> literalString("projected"))),
       context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection mapValue from ref slot") {
    val propertyMap = VirtualValues.map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> RefSlot(offset, nullable, symbols.CTMap)), 0, 1)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(offset, propertyMap)
      evaluate(compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots),
         context) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      evaluate(compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots),
         context) should equal(VirtualValues.map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("call function by id") {
    // given
    registerUserDefinedFunction("foo") { builder =>
      builder.out(Neo4jTypes.NTString)
      new BasicUserFunction(builder.build) {
        override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = stringValue("success")
      }
    }
    val id = getUserFunctionHandle("foo").id()
    val udf = callFunction(signature(qualifiedName("foo"), id), literalString("hello"))

    //then
    evaluate(compile(udf)) should equal(stringValue("success"))
  }

  test("call function by id with default argument") {
    // given
    registerUserDefinedFunction("foo") { builder =>
      builder.out(Neo4jTypes.NTString)
      new BasicUserFunction(builder.build) {
        override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = stringValue("success")
      }
    }
    val id = getUserFunctionHandle("foo").id()
    val udf = callFunction(
      signature(qualifiedName("foo"), id = id, field = fieldSignature("in", default = Some("I am default"))))


    //then
    evaluate(compile(udf)) should equal(stringValue("success"))
  }

  test("should compile grouping key with single expression") {
    //given
    val slot = RefSlot(0, nullable = true, symbols.CTAny)
    val slots = SlotConfiguration(Map("a" -> slot), 0, 1)
    val incoming = SlottedExecutionContext(slots)
    val outgoing = SlottedExecutionContext(slots)
    val projections = Map("a" -> literal("hello"))
    val compiled: CompiledGroupingExpression = compileGroupingExpression(projections, slots)

    //when
    val key = compiled.computeGroupingKey(incoming, query, Array.empty, cursors, expressionVariables)
    compiled.projectGroupingKey(outgoing, key)

    //then
    key should equal(stringValue("hello"))
    outgoing.getRefAt(0) should equal(stringValue("hello"))
  }

  test("should compile grouping key with multiple expressions") {
    //given
    val node = nodeValue()
    val refSlot = RefSlot(0, nullable = true, symbols.CTAny)
    val longSlot = LongSlot(0, nullable = true, symbols.CTNode)
    val slots = SlotConfiguration(Map("a" -> refSlot, "b" -> longSlot), 1, 1)
    val incoming = SlottedExecutionContext(slots)
    val outgoing = SlottedExecutionContext(slots)
    incoming.setLongAt(0, node.id())
    val projections = Map("a" -> literal("hello"),
                          "b" -> NodeFromSlot(0, "node"))
    val compiled: CompiledGroupingExpression = compileGroupingExpression(projections, slots)

    //when
    val key = compiled.computeGroupingKey(incoming, query, Array.empty, cursors, expressionVariables)
    compiled.projectGroupingKey(outgoing, key)

    //then
    key should equal(VirtualValues.list(stringValue("hello"), node))
    outgoing.getRefAt(0) should equal(stringValue("hello"))
    outgoing.getLongAt(0) should equal(node.id())
  }

  test("should compile grouping key with multiple expressions all primitive") {
    //given
    val rel = relationshipValue()
    val node = nodeValue()
    val relSlot = LongSlot(0, nullable = true, symbols.CTRelationship)
    val nodeSlot = LongSlot(1, nullable = true, symbols.CTNode)
    val slots = SlotConfiguration(Map("node" -> nodeSlot, "rel" -> relSlot), 2, 0)
    val incoming = SlottedExecutionContext(slots)
    incoming.setLongAt(0, rel.id())
    incoming.setLongAt(1, node.id())
    val outgoing = SlottedExecutionContext(slots)
    val projections = Map("rel" -> RelationshipFromSlot(0, "rel"),
                          "node" -> NodeFromSlot(1, "node"))
    val compiled: CompiledGroupingExpression = compileGroupingExpression(projections, slots)

    //when
    val key = compiled.computeGroupingKey(incoming, query, Array.empty, cursors, expressionVariables)
    compiled.projectGroupingKey(outgoing, key)

    //then
    key should equal(VirtualValues.list( rel, node))
    incoming.getLongAt(0) should equal(rel.id())
    incoming.getLongAt(1) should equal(node.id())
  }

  test("single outgoing path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(r.slot, nullable = true, symbols.CTRelationship)
                                      ), 3, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2)
    addRelationships(context, r)

    //when
    //p = (n1)-[r]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"), OUTGOING, Some(NodeFromSlot(n2.slot, "n2")),
                                                                   NilPathStep)))
    //then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("single outgoing path where target node not known (will only happen for legacy plans)") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(r.slot, nullable = true, symbols.CTRelationship)
    ), 3, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2)
    addRelationships(context, r)

    //when
    //p = (n1)-[r]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"), OUTGOING, None,
                                                                   NilPathStep)))
    //then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("single-node path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1)

    //when
    //p = (n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"), NilPathStep))

    //then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node), Array.empty))
  }

  test("single incoming path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(r.slot, nullable = true, symbols.CTRelationship)
    ), 3, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2)
    addRelationships(context, r)

    //when
    //p = (n1)<-[r]-(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"),
                                                                   INCOMING, Some(NodeFromSlot(n2.slot, "n2")), NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("single incoming path where target node not known (will only happen for legacy plans)") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n2.node, n1.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(r.slot, nullable = true, symbols.CTRelationship)
    ), 3, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2)
    addRelationships(context, r)

    //when
    //p = (n1)<-[r]-(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"), INCOMING, None, NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("single undirected path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(r.slot, nullable = true, symbols.CTRelationship)
    ), 3, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2)
    addRelationships(context, r)

    //when
    val p1 = compile(pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                                 SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"),
                                                                            BOTH, Some(NodeFromSlot(n2.slot, "n2")),
                                                                            NilPathStep))), slots)
    val p2 = compile(pathExpression(NodePathStep(NodeFromSlot(n2.slot, "n2"),
                                                 SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"),
                                                                            BOTH, Some(NodeFromSlot(n1.slot, "n1")),
                                                                            NilPathStep))), slots)
    //then
    evaluate(p1, context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
    evaluate(p2, context) should equal(VirtualValues.path(Array(n2.node, n1.node), Array(r.rel)))
  }

  test("single undirected path where target node not known (will only happen for legacy plans)") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(r.slot, nullable = true, symbols.CTRelationship)
    ), 3, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2)
    addRelationships(context, r)

    //when
    val p1 = compile(pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                                 SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"),
                                                                            BOTH, None,
                                                                            NilPathStep))), slots)
    val p2 = compile(pathExpression(NodePathStep(NodeFromSlot(n2.slot, "n2"),
                                                 SingleRelationshipPathStep(RelationshipFromSlot(r.slot, "r"),
                                                                            BOTH, None,
                                                                            NilPathStep))), slots)
    //then
    evaluate(p1, context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
    evaluate(p2, context) should equal(VirtualValues.path(Array(n2.node, n1.node), Array(r.rel)))
  }

  test("single path with NO_VALUE") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> LongSlot(2, nullable = true, symbols.CTRelationship)
    ), 3, 0)
    val context = SlottedExecutionContext(slots)

    context.setLongAt(2, -1L)
    addNodes(context, n1, n2)

    //when
    //p = (n1)-[r]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(NullCheckVariable(2, RelationshipFromSlot(2, "r")),
                                                                   OUTGOING,  Some(NodeFromSlot(n2.slot, "n2")), NilPathStep)))
    //then
    evaluate(compile(p, slots), context) should be(NO_VALUE)
  }

  test("single path with statically undetermined entities") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val r = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 2)
    val slots = SlotConfiguration(Map("n1" -> RefSlot(n1.slot, nullable = true, symbols.CTAny),
                                      "n2" -> RefSlot(n2.slot, nullable = true, symbols.CTAny),
                                      "r" -> RefSlot(r.slot, nullable = true, symbols.CTAny)
    ), 0, 3)
    val context = SlottedExecutionContext(slots)
    context.setRefAt(n1.slot, n1.node)
    context.setRefAt(n2.slot, n2.node)
    context.setRefAt(r.slot, r.rel)

    //when
    //p = (n1)-[r]->(n2)
    val p = pathExpression(NodePathStep(ReferenceFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(ReferenceFromSlot(r.slot, "r"),
                                                                   OUTGOING, Some(ReferenceFromSlot(n2.slot, "n2")), NilPathStep)))
    //then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
  }

  test("longer path with different direction") {
    //given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 =  RelAt(relationshipValue(n3.node, n2.node, EMPTY_MAP), 5)
    val r3 =  RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship)), 7, 0)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)

    //when
    //p = (n1)-[r1]->(n2)<-[r2]-(n3)-[r3]-(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        SingleRelationshipPathStep(RelationshipFromSlot(r1.slot, "r1"), OUTGOING, Some(NodeFromSlot(n2.slot, "n2")),
                                                                   SingleRelationshipPathStep(RelationshipFromSlot(r2.slot, "r2"), INCOMING, Some(NodeFromSlot(n3.slot, "n3")),
                                                                                              SingleRelationshipPathStep(RelationshipFromSlot(r3.slot, "r3"), BOTH, Some(NodeFromSlot(n4.slot, "n4")), NilPathStep)))))

    // then
    evaluate(compile(p, slots), context) should equal(VirtualValues.path(Array(n1.node, n2.node, n3.node, n4.node), Array(r1.rel, r2.rel, r3.rel)))
  }

  test("multiple outgoing path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 =  RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 =  RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r1.rel, r2.rel, r3.rel))

    //when
    //p = (n1)-[r*]->(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  OUTGOING, Some(NodeFromSlot(n4.slot, "n4")), NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(
      VirtualValues.path(Array(n1.node, n2.node, n3.node, n4.node), Array(r1.rel, r2.rel, r3.rel)))
  }

  test("multiple outgoing path where target node not known (will only happen for legacy plans)") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 =  RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 =  RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r1.rel, r2.rel, r3.rel))

    //when
    //p = (n1)-[r*]->(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  OUTGOING, None, NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(
      VirtualValues.path(Array(n1.node, n2.node, n3.node, n4.node), Array(r1.rel, r2.rel, r3.rel)))
  }

  test("multiple incoming path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 = RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 = RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r3.rel, r2.rel, r1.rel))

    //when
    //p = (n4)<-[r*]-(n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(n4.slot, "n4"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  INCOMING, Some(NodeFromSlot(n1.slot, "n1")), NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(
      VirtualValues.path(Array(n4.node, n3.node, n2.node, n1.node), Array(r3.rel, r2.rel, r1.rel)))
  }

  test("multiple incoming path where target node not known (will only happen for legacy plans)") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 = RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 = RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r3.rel, r2.rel, r1.rel))

    //when
    //p = (n4)<-[r*]-(n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(n4.slot, "n4"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  INCOMING, None, NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(
      VirtualValues.path(Array(n4.node, n3.node, n2.node, n1.node), Array(r3.rel, r2.rel, r1.rel)))
  }

  test("multiple undirected path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 = RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 = RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r3.rel, r2.rel, r1.rel))

    //when
    //p = (n4)<-[r*]-(n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(n4.slot, "n4"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  BOTH, Some(NodeFromSlot(n1.slot, "n1")), NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(
      VirtualValues.path(Array(n4.node, n3.node, n2.node, n1.node), Array(r3.rel, r2.rel, r1.rel)))
  }

  test("multiple undirected path where target node not known (will only happen for legacy plans)") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 = RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 = RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r3.rel, r2.rel, r1.rel))

    //when
    //p = (n4)<-[r*]-(n1)
    val p = pathExpression(NodePathStep(NodeFromSlot(n4.slot, "n4"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  BOTH, None, NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should equal(
      VirtualValues.path(Array(n4.node, n3.node, n2.node, n1.node), Array(r3.rel, r2.rel, r1.rel)))
  }

  test("multiple path containing NO_VALUE") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val n3 = NodeAt(nodeValue(), 2)
    val n4 = NodeAt(nodeValue(), 3)
    val r1 = RelAt(relationshipValue(n1.node, n2.node, EMPTY_MAP), 4)
    val r2 = RelAt(relationshipValue(n2.node, n3.node, EMPTY_MAP), 5)
    val r3 = RelAt(relationshipValue(n3.node, n4.node, EMPTY_MAP), 6)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "n3" -> LongSlot(n3.slot, nullable = true, symbols.CTNode),
                                      "n4" -> LongSlot(n4.slot, nullable = true, symbols.CTNode),
                                      "r1" -> LongSlot(r1.slot, nullable = true, symbols.CTRelationship),
                                      "r2" -> LongSlot(r2.slot, nullable = true, symbols.CTRelationship),
                                      "r3" -> LongSlot(r3.slot, nullable = true, symbols.CTRelationship),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 7, 1)
    val context = SlottedExecutionContext(slots)
    addNodes(context, n1, n2, n3, n4)
    addRelationships(context, r1, r2, r3)
    context.setRefAt(0, list(r1.rel, NO_VALUE, r3.rel))

    //when
    //p = (n1)-[r*]->(n4)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  OUTGOING, Some(NodeFromSlot(n4.slot, "n4")), NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should be(NO_VALUE)
  }

  test("multiple NO_VALUE path") {
    // given
    val n1 = NodeAt(nodeValue(), 0)
    val n2 = NodeAt(nodeValue(), 1)
    val slots = SlotConfiguration(Map("n1" -> LongSlot(n1.slot, nullable = true, symbols.CTNode),
                                      "n2" -> LongSlot(n2.slot, nullable = true, symbols.CTNode),
                                      "r" -> RefSlot(0, nullable = true, symbols.CTList(symbols.CTRelationship))), 2, 1)
    val context = SlottedExecutionContext(slots)
    context.setRefAt(0, NO_VALUE)
    addNodes(context, n1, n2)

    //when
    //p = (n1)-[r*]->(n2)
    val p = pathExpression(NodePathStep(NodeFromSlot(n1.slot, "n1"),
                                        MultiRelationshipPathStep(ReferenceFromSlot(0, "r"),
                                                                  OUTGOING, Some(NodeFromSlot(n2.slot, "n2")), NilPathStep)))

    //then
    evaluate(compile(p, slots), context) should be(NO_VALUE)
  }

  // Testing different permutation of property accesses

  case class PropertyTest(name: String,
                          entity: String => Entity,
                          entityWithNoProp: () => Entity,
                          typ: CypherType,
                          entityType: EntityType,
                          virtualValueConstructor: Long => VirtualValue,
                          earlyExpression: Int => RuntimeProperty,
                          lateExpression: Int => RuntimeProperty,
                          earlyCachedExpression: (PropertyKeyName, Int, Int, Boolean) => RuntimeExpression,
                          lateCachedExpression: (PropertyKeyName, Int, Int, Boolean) => RuntimeExpression,
                          invalidate: (ExecutionContext, Long) => Unit)

  case class RuntimeAccess(time: String,
                           expression: Int => RuntimeProperty,
                           cachedExpression: (PropertyKeyName, Int, Int, Boolean) => RuntimeExpression)

  for {
    PropertyTest(name, entity, entityWithNoProp, typ, entityType, virtualValueConstructor, earlyExpression, lateExpression, earlyCachedExpression, lateCachedExpression, invalidate) <- Seq(
      PropertyTest("node",
        prop => createNode("prop" -> prop),
        () => createNode(),
                   symbols.CTNode,
                   NODE_TYPE,
                   VirtualValues.node,
        token => NodeProperty(0, token, "prop")(null),
        _ => NodePropertyLate(0, "prop", "prop")(null),
        (pkn, token, cachedPropertyOffset, offsetIsForLongSlot) => ast.SlottedCachedPropertyWithPropertyToken("n", pkn, 0, offsetIsForLongSlot, token, cachedPropertyOffset, NODE_TYPE),
        (pkn, _, cachedPropertyOffset, offsetIsForLongSlot) => ast.SlottedCachedPropertyWithoutPropertyToken("n", pkn, 0, offsetIsForLongSlot, "prop", cachedPropertyOffset, NODE_TYPE),
                   _.invalidateCachedNodeProperties(_)),
      PropertyTest("relationship",
        prop => relate(createNode(), createNode(), "prop" -> prop),
        () => relate(createNode(), createNode()),
                   symbols.CTRelationship,
                   RELATIONSHIP_TYPE,
                   VirtualValues.relationship,
        token => RelationshipProperty(0, token, "prop")(null),
        _ => RelationshipPropertyLate(0, "prop", "prop")(null),
        (pkn, token, cachedPropertyOffset, offsetIsForLongSlot) => ast.SlottedCachedPropertyWithPropertyToken("n", pkn, 0, offsetIsForLongSlot, token, cachedPropertyOffset, RELATIONSHIP_TYPE),
        (pkn, _, cachedPropertyOffset, offsetIsForLongSlot) => ast.SlottedCachedPropertyWithoutPropertyToken("n", pkn, 0, offsetIsForLongSlot, "prop", cachedPropertyOffset, RELATIONSHIP_TYPE),
                   _.invalidateCachedRelationshipProperties(_))
    )
    RuntimeAccess(time, expression, cachedExpression) <- Seq(
      RuntimeAccess("early", earlyExpression, earlyCachedExpression),
      RuntimeAccess("late", lateExpression, lateCachedExpression)
    )} {

    test(s"$time $name property access") {
      // Given
      val n = entity("hello")
      val token = tokenReader(_.propertyKey("prop"))
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, n.getId)

      // When
      val compiled = compile(expression(token), slots)

      // Then
      evaluate(compiled, context) should equal(stringValue("hello"))
    }

    test(s"$time cached $name property access from tx state") {
      //NOTE: we are in an open transaction so everything we add here will populate the tx state
      val n = entity("hello from tx state")
      val token = tokenReader(_.propertyKey("prop"))
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val context = SlottedExecutionContext(slots)
      val pkn = PropertyKeyName("prop")(pos)
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      context.setLongAt(0, n.getId)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val expression = cachedExpression(pkn, token, cachedPropertyOffset, true)
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(stringValue("hello from tx state"))
    }

    test(s"$time cached $name property access") {
      //create an entity and force it to be properly stored
      val n = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      //now we have a stored entity that's not in the tx state
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, n.getId)
      context.setCachedProperty(property, stringValue("hello from cache"))
      val expression = cachedExpression(pkn, token, cachedPropertyOffset, true)
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(stringValue("hello from cache"))
    }

    test(s"$time cached $name property access for ref slots") {
      //create an entity and force it to be properly stored
      val n = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      //now we have a stored entity that's not in the tx state
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> RefSlot(0, nullable = true, typ)), 0, 1)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(0, virtualValueConstructor(n.getId))
      context.setCachedProperty(property, stringValue("hello from cache"))
      val expression = cachedExpression(pkn, token, cachedPropertyOffset, false)
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(stringValue("hello from cache"))
    }

    test(s"$time cached $name property access, when invalidated") {
      //create an entity and force it to be properly stored
      val n = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      //now we have a stored entity that's not in the tx state
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, n.getId)
      context.setCachedProperty(property, stringValue("hello from cache"))
      invalidate(context, n.getId)

      val expression = cachedExpression(pkn, token, cachedPropertyOffset, true)
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(stringValue("hello from disk"))
    }

    test(s"$time cached $name property existence with cached value") {
      val node = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, node.getId)
      context.setCachedProperty(property, stringValue("hello from cache"))
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, true))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.TRUE)
    }

    test(s"$time cached $name property existence with cached value - ref slot") {
      val node = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> RefSlot(0, nullable = true, typ)), 0, 1)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(0, virtualValueConstructor(node.getId))
      context.setCachedProperty(property, stringValue("hello from cache"))
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, false))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.TRUE)
    }

    test(s"$time cached $name property existence with cached deleted value") {
      val node = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, node.getId)
      context.setCachedProperty(property, Values.NO_VALUE)
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, true))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.FALSE)
    }

    test(s"$time cached $name property existence with cached invalidated value - exists") {
      val node = entity("hello from disk")
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, node.getId)
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, true))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.TRUE)
    }

    test(s"$time cached $name property existence with cached invalidated value - does not exist") {
      val node = entityWithNoProp()
      startNewTransaction()

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, node.getId)
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, true))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.FALSE)
    }

    test(s"$time cached $name property existence with tx state deleted value") {
      val node = entity("hello from disk")
      startNewTransaction()

      node.removeProperty("prop")
      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, node.getId)
      context.setCachedProperty(property, stringValue("hello from cache"))
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, true))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.FALSE)
    }

    test(s"$time cached $name property existence with tx state value") {
      val node = entity("hello from tx state")

      val pkn = PropertyKeyName("prop")(pos)
      val token = tokenReader(_.propertyKey("prop"))
      val property = expressions.CachedProperty("n", Variable("n")(pos), pkn, entityType)(pos)
      val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, typ)), 1, 0)
      val cachedPropertyOffset = slots.newCachedProperty(property).getCachedPropertyOffsetFor(property)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(0, node.getId)
      context.setCachedProperty(property, stringValue("hello from cache"))
      val expression = function("exists", cachedExpression(pkn, token, cachedPropertyOffset, true))
      val compiled = compile(expression, slots)

      evaluate(compiled, context) should equal(Values.TRUE)
    }
  }

  test("invalidation only goes to the right type (node/relationship") {
    val node = createNode("prop" -> "hello from node disk")
    val rel = relate(createNode(), createNode(), "prop" -> "hello from rel disk")
    startNewTransaction()

    val pkn = PropertyKeyName("prop")(pos)
    val nProperty = expressions.CachedProperty("n", Variable("n")(pos), pkn, NODE_TYPE)(pos)
    val rProperty = expressions.CachedProperty("r", Variable("r")(pos), pkn, RELATIONSHIP_TYPE)(pos)
    val n2Property = expressions.CachedProperty("n2", Variable("n2")(pos), pkn, NODE_TYPE)(pos)
    val r2Property = expressions.CachedProperty("r2", Variable("r2")(pos), pkn, RELATIONSHIP_TYPE)(pos)
    val slots = SlotConfiguration.empty
      .newLong("n", nullable = true, symbols.CTNode)
      .newLong("r", nullable = true, symbols.CTRelationship)
      .newReference("n2", nullable = true, symbols.CTNode)
      .newReference("r2", nullable = true, symbols.CTRelationship)
    val cachedNPropertyOffset = slots.newCachedProperty(nProperty).getCachedPropertyOffsetFor(nProperty)
    val cachedRPropertyOffset = slots.newCachedProperty(rProperty).getCachedPropertyOffsetFor(rProperty)
    val cachedN2PropertyOffset = slots.newCachedProperty(n2Property).getCachedPropertyOffsetFor(n2Property)
    val cachedR2PropertyOffset = slots.newCachedProperty(r2Property).getCachedPropertyOffsetFor(r2Property)
    val context = SlottedExecutionContext(slots)

    // Set nodes and rels
    context.setLongAt(0, node.getId)
    context.setLongAt(1, rel.getId)
    context.setRefAt(0, VirtualValues.node(node.getId))
    context.setRefAt(1, VirtualValues.relationship(rel.getId))

    // Set cached properties
    context.setCachedProperty(nProperty, stringValue("hello from node cache: 1"))
    context.setCachedProperty(rProperty, stringValue("hello from rel cache: 1"))
    context.setCachedProperty(n2Property, stringValue("hello from node cache: 2"))
    context.setCachedProperty(r2Property, stringValue("hello from rel cache: 2"))

    // invalidate nodes
    context.invalidateCachedNodeProperties(node.getId)

    //  read
    val propToken = tokenReader(_.propertyKey("prop"))
    val getN = compile(ast.SlottedCachedPropertyWithPropertyToken("n", pkn, 0, offsetIsForLongSlot = true, propToken, cachedNPropertyOffset, NODE_TYPE), slots)
    evaluate(getN, context) should equal(stringValue("hello from node disk"))
    val getR = compile(ast.SlottedCachedPropertyWithPropertyToken("r", pkn, 1, offsetIsForLongSlot = true, propToken, cachedRPropertyOffset, RELATIONSHIP_TYPE), slots)
    evaluate(getR, context) should equal(stringValue("hello from rel cache: 1"))
    val getN2 = compile(ast.SlottedCachedPropertyWithPropertyToken("n2", pkn, 0, offsetIsForLongSlot = false, propToken, cachedN2PropertyOffset, NODE_TYPE), slots)
    evaluate(getN2, context) should equal(stringValue("hello from node disk"))
    val getR2 = compile(ast.SlottedCachedPropertyWithPropertyToken("r2", pkn, 1, offsetIsForLongSlot = false, propToken, cachedR2PropertyOffset, RELATIONSHIP_TYPE), slots)
    evaluate(getR2, context) should equal(stringValue("hello from rel cache: 2"))


    // Set cached properties again
    context.setCachedProperty(nProperty, stringValue("hello from node cache: 1"))
    context.setCachedProperty(rProperty, stringValue("hello from rel cache: 1"))
    context.setCachedProperty(n2Property, stringValue("hello from node cache: 2"))
    context.setCachedProperty(r2Property, stringValue("hello from rel cache: 2"))

    // invalidate rel
    context.invalidateCachedRelationshipProperties(rel.getId)

    // read again
    evaluate(getN, context) should equal(stringValue("hello from node cache: 1"))
    evaluate(getR, context) should equal(stringValue("hello from rel disk"))
    evaluate(getN2, context) should equal(stringValue("hello from node cache: 2"))
    evaluate(getR2, context) should equal(stringValue("hello from rel disk"))
  }

  test("getDegree without type") {
    //given node with three outgoing and two incoming relationships
    val n = createNode("prop" -> "hello")
    relate(createNode(), n)
    relate(createNode(), n)
    relate(n, createNode())
    relate(n, createNode())
    relate(n, createNode())

    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    evaluate(compile(GetDegreePrimitive(0, None, OUTGOING), slots), context) should
      equal(Values.longValue(3))
    evaluate(compile(GetDegreePrimitive(0, None, INCOMING), slots), context) should
      equal(Values.longValue(2))
    evaluate(compile(GetDegreePrimitive(0, None, BOTH), slots), context) should
      equal(Values.longValue(5))
  }

  test("getDegree with type") {
    //given node with three outgoing and two incoming relationships
    val n = createNode("prop" -> "hello")
    val relType = "R"
    relate(createNode(), n, relType)
    relate(createNode(), n, "OTHER")
    relate(n, createNode(), relType)
    relate(n, createNode(), relType)
    relate(n, createNode(), "OTHER")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    evaluate(compile(GetDegreePrimitive(0, Some(relType), OUTGOING), slots), context) should equal(Values.longValue(2))
    evaluate(compile(GetDegreePrimitive(0, Some(relType), INCOMING), slots), context) should equal(Values.longValue(1))
    evaluate(compile(GetDegreePrimitive(0, Some(relType), BOTH), slots), context) should equal(Values.longValue(3))
  }

  test("NodePropertyExists") {
    val n = createNode("prop" -> "hello")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)
    val property = tokenReader(_.propertyKey("prop"))
    val nonExistingProperty = 1337

    evaluate(compile(NodePropertyExists(0, property, "prop")(null), slots), context) should equal(Values.TRUE)
    evaluate(compile(NodePropertyExists(0, nonExistingProperty, "otherProp")(null), slots), context) should equal(Values.FALSE)
  }

  test("NodePropertyExistsLate") {
    val n = createNode("prop" -> "hello")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    evaluate(compile(NodePropertyExistsLate(0, "prop", "prop")(null), slots), context) should equal(Values.TRUE)
    evaluate(compile(NodePropertyExistsLate(0, "otherProp", "otherProp")(null), slots), context) should equal(Values.FALSE)
  }

  test("RelationshipPropertyExists") {
    val r = relate(createNode(), createNode(), "prop" -> "hello")
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.getId)
    val property = tokenReader(_.propertyKey("prop"))
    val nonExistingProperty = 1337

    evaluate(compile(RelationshipPropertyExists(0, property, "prop")(null), slots), context) should equal(Values.TRUE)
    evaluate(compile(RelationshipPropertyExists(0, nonExistingProperty, "otherProp")(null), slots), context) should equal(Values.FALSE)
  }

  test("RelationshipPropertyExistsLate") {
    val r = relate(createNode(), createNode(), "prop" -> "hello")
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.getId)

    evaluate(compile(RelationshipPropertyExistsLate(0, "prop", "prop")(null), slots), context) should equal(Values.TRUE)
    evaluate(compile(RelationshipPropertyExistsLate(0, "otherProp", "otherProp")(null), slots), context) should equal(Values.FALSE)
  }

  test("NodeFromSlot") {
    // Given
    val n = nodeValue()
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.id())
    val expression = NodeFromSlot(0, "foo")

    // When
    val compiled = compile(expression, slots)

    // Then
    evaluate(compiled, context) should equal(n)
  }

  test("RelationshipFromSlot") {
    // Given
    val r = relationshipValue()
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.id())
    val expression = RelationshipFromSlot(0, "foo")

    // When
    val compiled = compile(expression, slots)

    // Then
    evaluate(compiled, context) should equal(r)
  }

  test("HasLabels") {
    val n = createLabeledNode("L1", "L2")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    evaluate(compile(hasLabels(NodeFromSlot(0, "n"), "L1"), slots), context) should equal(Values.TRUE)
    evaluate(compile(hasLabels(NodeFromSlot(0, "n"), "L1", "L2"), slots), context) should equal(Values.TRUE)
    evaluate(compile(hasLabels(NodeFromSlot(0, "n"), "L1", "L3"), slots), context) should equal(Values.FALSE)
    evaluate(compile(hasLabels(NodeFromSlot(0, "n"), "L2", "L3"), slots), context) should equal(Values.FALSE)
    evaluate(compile(hasLabels(NodeFromSlot(0, "n"), "L1", "L2", "L3"), slots), context) should equal(Values.FALSE)
  }

  case class NodeAt(node: NodeValue, slot: Int)
  case class RelAt(rel: RelationshipValue, slot: Int)

  private def addNodes(context: ExecutionContext, nodes: NodeAt*): Unit = {

    for (node <- nodes) {
      context.setLongAt(node.slot, node.node.id())
    }
  }

  private def addRelationships(context: ExecutionContext, rels: RelAt*): Unit = {
    graph.inTx(
      for (rel <- rels) {
        context.setLongAt(rel.slot, rel.rel.id())
      })
  }

  private def pathExpression(step: PathStep) = PathExpression(step)(pos)

  private def mapProjection(name: String, includeAllProps: Boolean, items: (String,Expression)*) =
    DesugaredMapProjection(varFor(name), items.map(kv => LiteralEntry(PropertyKeyName(kv._1)(pos), kv._2)(pos)), includeAllProps)(pos)

  private def simpleCase(inner: Expression, alternatives: List[(Expression, Expression)], default: Option[Expression] = None) =
    CaseExpression(Some(inner), alternatives, default)(pos)

  private def genericCase(alternatives: List[(Expression, Expression)], default: Option[Expression] = None) =
    CaseExpression(None, alternatives, default)(pos)

  private def path(size: Int) = {
    val nodeValues = ArrayBuffer.empty[NodeValue]
    val relValues = ArrayBuffer.empty[RelationshipValue]
    nodeValues.append(nodeValue())
    for (_ <- 0 until size) {
      val n = nodeValue()
      relValues.append(relationshipValue(nodeValues.last, nodeValue(), EMPTY_MAP))
      nodeValues.append(n)
    }
    VirtualValues.path(nodeValues.toArray, relValues.toArray)
  }

  private def nodeValue(properties: MapValue = EMPTY_MAP): NodeValue = {
    graph.inTx {
      val node = createNode()
      properties.foreach((t: String, u: AnyValue) => {
        node.setProperty(t, u.asInstanceOf[Value].asObject())
      })

      ValueUtils.fromNodeProxy(node)
    }
  }

  private def relationshipValue(properties: MapValue = EMPTY_MAP): RelationshipValue = {
    relationshipValue(nodeValue(), nodeValue(), properties)
  }

  private def relationshipValue(from: NodeValue, to: NodeValue, properties: MapValue): RelationshipValue = {
    graph.inTx {
      val r: Relationship = relate(graphOps.getNodeById(from.id()), graphOps.getNodeById(to.id()))
      properties.foreach((t: String, u: AnyValue) => {
        r.setProperty(t, u.asInstanceOf[Value].asObject())
      })
      ValueUtils.fromRelationshipProxy(r)
    }
  }

  def compile(e: Expression, slots: SlotConfiguration = SlotConfiguration.empty): CompiledExpression

  def compileProjection(projections: Map[String, Expression],
                        slots: SlotConfiguration = SlotConfiguration.empty): CompiledProjection

  def compileGroupingExpression(projections: Map[String, Expression],
                                slots: SlotConfiguration = SlotConfiguration.empty): CompiledGroupingExpression

  private def evaluate(compiled: CompiledExpression): AnyValue =
    compiled.evaluate(ctx, query, Array.empty, cursors, expressionVariables)

  private def evaluate(compiled: CompiledExpression,
                       params: Array[AnyValue]): AnyValue =
    compiled.evaluate(ctx, query, params, cursors, expressionVariables)

  private def evaluate(compiled: CompiledExpression,
                       context: ExecutionContext): AnyValue =
    compiled.evaluate(context, query, Array.empty, cursors, expressionVariables)

  private def evaluate(compiled: CompiledExpression,
                       nExpressionSlots: Int,
                       params: Array[AnyValue] = Array.empty,
                       context: ExecutionContext = ctx): AnyValue =
    compiled.evaluate(context, query, params, cursors, new Array(nExpressionSlots))
  
  private def parameter(offset: Int): Expression = ParameterFromSlot(offset, s"a$offset", CTAny)

  private def params(values: AnyValue*): Array[AnyValue] = values.toArray

  private def coerce(value: AnyValue, ct: CypherType) =
    evaluate(compile(coerceTo(parameter(0), ct)), Array(value))

  private def callFunction(ufs: UserFunctionSignature, args: Expression*) =
    ResolvedFunctionInvocation(ufs.name, Some(ufs), args.toIndexedSeq)(pos)

  private def signature(name: KernelQualifiedName, id: Int, field: FieldSignature = fieldSignature("foo")) =
    UserFunctionSignature(QualifiedName(Seq.empty, name.name()), IndexedSeq(field), symbols.CTAny, None,
                          Array.empty, None, isAggregate = false, id = id)

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
      val actual = evaluate(compile(predicate))

      if (isIncomparable(left, right)) {
        buildResult(actual == NO_VALUE, actual)
      } else {
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

  private def parameters(kvs: (String, AnyValue)*) = params(kvs.map(_._2).toArray: _*)

  private def types() = Map(longValue(42) -> symbols.CTNumber, stringValue("hello") -> symbols.CTString,
                          Values.TRUE -> symbols.CTBoolean, nodeValue() -> symbols.CTNode,
                          relationshipValue() -> symbols.CTRelationship, path(13) -> symbols.CTPath,
                          pointValue(Cartesian, 1.0, 3.6) -> symbols.CTPoint,
                          DateTimeValue.now(Clock.systemUTC()) -> symbols.CTDateTime,
                          LocalDateTimeValue.now(Clock.systemUTC()) -> symbols.CTLocalDateTime,
                          TimeValue.now(Clock.systemUTC()) -> symbols.CTTime,
                          LocalTimeValue.now(Clock.systemUTC()) -> symbols.CTLocalTime,
                          DateValue.now(Clock.systemUTC()) -> symbols.CTDate,
                          durationValue(Duration.ofHours(3)) -> symbols.CTDuration)

}

class CompiledExpressionsIT extends ExpressionsIT {

  override def compile(e: Expression, slots: SlotConfiguration = SlotConfiguration.empty): CompiledExpression =
       defaultGenerator(slots, readOnly = false).compileExpression(e).getOrElse(fail(s"Failed to compile expression $e"))

  override def compileProjection(projections: Map[String, Expression], slots: SlotConfiguration = SlotConfiguration.empty): CompiledProjection =
       defaultGenerator(slots, readOnly = false).compileProjection(projections).getOrElse(fail(s"Failed to compile projection $projections"))

  override def compileGroupingExpression(projections: Map[String, Expression], slots: SlotConfiguration = SlotConfiguration.empty): CompiledGroupingExpression =
    defaultGenerator(slots, readOnly = false).compileGrouping(orderGroupingKeyExpressions(projections, orderToLeverage = Seq.empty))
      .getOrElse(fail(s"Failed to compile grouping $projections"))
}

class InterpretedExpressionIT extends ExpressionsIT {
  override  def compile(e: Expression, slots: SlotConfiguration): CompiledExpression = {
    val expression = converter(slots, (converter, id) => converter.toCommandExpression(id, e))
    (context: ExecutionContext, dbAccess: DbAccess, params: Array[AnyValue], cursors: ExpressionCursors,
     expressionVariables: Array[AnyValue]) => expression(context, state(dbAccess, params, cursors, expressionVariables))
  }

  override  def compileProjection(projections: Map[String, Expression],
                                          slots: SlotConfiguration): CompiledProjection = {
    val projector = converter(slots, (converter, id) => converter.toCommandProjection(id, projections))
    (context: ExecutionContext, dbAccess: DbAccess, params: Array[AnyValue], cursors: ExpressionCursors,
     expressionVariables: Array[AnyValue]) => projector
      .project(context, state(dbAccess, params, cursors, expressionVariables))
  }

  override  def compileGroupingExpression(projections: Map[String, Expression],
                                                  slots: SlotConfiguration): CompiledGroupingExpression = {
    val grouping = converter(slots, (converter, id) => converter.toGroupingExpression(id, projections, Seq.empty))
    new CompiledGroupingExpression {

      override def projectGroupingKey(context: ExecutionContext,
                                      groupingKey: AnyValue): Unit = grouping.project(context, groupingKey.asInstanceOf[grouping.KeyType])

      override def computeGroupingKey(context: ExecutionContext,
                                      dbAccess: DbAccess,
                                      params: Array[AnyValue],
                                      cursors: ExpressionCursors,
                                      expressionVariables: Array[AnyValue]): AnyValue =
        grouping.computeGroupingKey(context, state(dbAccess, params, cursors, expressionVariables))


      override def getGroupingKey(context: ExecutionContext): AnyValue = grouping.getGroupingKey(context)
    }
  }


  private def state(dbAccess: DbAccess, params: Array[AnyValue], cursors: ExpressionCursors, expressionVariables: Array[AnyValue]) =
    new SlottedQueryState(dbAccess.asInstanceOf[QueryContext],
                          null,
                          params,
                          cursors,
                          Array.empty,
                          expressionVariables,
                          DO_NOTHING_SUBSCRIBER,
                          NoMemoryTracker)

  private def converter[T](slots: SlotConfiguration, producer: (ExpressionConverters, Id) => T): T = {
    val plan = PhysicalPlan(null,
                            0,
                            new SlotConfigurations,
                            new ArgumentSizes,
                            new ApplyPlans,
                            new NestedPlanArgumentConfigurations,
                            new AvailableExpressionVariables,
                            ParameterMapping.empty)
    val id = Id(0)
    plan.slotConfigurations.set(id, slots)
    val converters = new ExpressionConverters(SlottedExpressionConverters(plan),
                                             CommunityExpressionConverter(query))
    producer(converters, id)
  }
}
