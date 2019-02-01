/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Math.{PI, sin}
import java.time.{Clock, Duration}
import java.util.concurrent.ThreadLocalRandom

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ArgumentSizes, SlotConfigurations}
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.ast._
import org.neo4j.cypher.internal.physicalplanning.{ast, _}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.logical.plans
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.symbols.{CypherType, ListType}
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.procs.{Neo4jTypes, QualifiedName => KernelQualifiedName}
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.proc.CallableUserFunction.BasicUserFunction
import org.neo4j.kernel.api.proc.Context
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, TransactionalContext}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.CoordinateReferenceSystem.{Cartesian, WGS84}
import org.neo4j.values.storable.LocalTimeValue.localTime
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues.{list, map, _}
import org.neo4j.values.virtual.{MapValue, NodeValue, RelationshipValue, VirtualValues}
import org.neo4j.values.{AnyValue, AnyValues}
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
    if (context != null) {
      context.close(true)
    }

    tx = graph.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    context = Neo4jTransactionalContextFactory.create(graph).newContext(tx, "X", EMPTY_MAP)
    query = new TransactionBoundQueryContext(TransactionalContextWrapper(context))(mock[IndexSearchMonitor])
    cursors = new ExpressionCursors(TransactionalContextWrapper(context).cursors)
  }

  override protected def stopTest(): Unit = {
    if (context != null) {
      context.close(true)
      context = null
    }
    super.stopTest()
  }

  protected var query: QueryContext = _
  private var cursors :ExpressionCursors = _
  private var tx: InternalTransaction = _
  private var context: TransactionalContext = _
  private val random = ThreadLocalRandom.current()

  test("round function") {
    compile(function("round", literalFloat(PI))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(3.0))
    compile(function("round", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("rand function") {
    // Given
    val expression = function("rand")

    // When
    val compiled = compile(expression)

    // Then
    val value = compiled.evaluate(ctx, query, EMPTY_MAP, cursors).asInstanceOf[DoubleValue].doubleValue()
    value should (be >= 0.0 and be <1.0)
  }

  test("sin function") {
    val arg = random.nextDouble()
    compile(function("sin", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(sin(arg)))
    compile(function("sin", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("asin function") {
    val arg = random.nextDouble()
    compile(function("asin", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.asin(arg)))
    compile(function("asin", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("haversin function") {
    val arg = random.nextDouble()
    compile(function("haversin", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue((1.0 - Math.cos(arg)) / 2))
    compile(function("haversin", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("acos function") {
    val arg = random.nextDouble()
    compile(function("acos", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.acos(arg)))
    compile(function("acos", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("cos function") {
    val arg = random.nextDouble()
    compile(function("cos", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.cos(arg)))
    compile(function("cos", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("cot function") {
    val arg = random.nextDouble()
    compile(function("cot", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(1 / Math.tan(arg)))
    compile(function("cot", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("atan function") {
    val arg = random.nextDouble()
    compile(function("atan", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.atan(arg)))
    compile(function("atan", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("atan2 function") {
    val arg1 = random.nextDouble()
    val arg2 = random.nextDouble()
    compile(function("atan2", literalFloat(arg1), literalFloat(arg2))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.atan2(arg1, arg2)))
    compile(function("atan2", nullLiteral, literalFloat(arg1))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(function("atan2", literalFloat(arg1), nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(function("atan2", nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("tan function") {
    val arg = random.nextDouble()
    compile(function("tan", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.tan(arg)))
    compile(function("tan", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("ceil function") {
    val arg = random.nextDouble()
    compile(function("ceil", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.ceil(arg)))
    compile(function("ceil", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("floor function") {
    val arg = random.nextDouble()
    compile(function("floor", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.floor(arg)))
    compile(function("floor", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("abs function") {
    compile(function("abs", literalFloat(3.2))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(3.2))
    compile(function("abs", literalFloat(-3.2))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(3.2))
    compile(function("abs", literalInt(3))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(3))
    compile(function("abs", literalInt(-3))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(3))
    compile(function("abs", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("radians function") {
    val arg = random.nextDouble()
    compile(function("radians", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.toRadians(arg)))
    compile(function("radians", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("degrees function") {
    val arg = random.nextDouble()
    compile(function("degrees", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.toDegrees(arg)))
    compile(function("degrees", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("exp function") {
    val arg = random.nextDouble()
    compile(function("exp", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.exp(arg)))
    compile(function("exp", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("log function") {
    val arg = random.nextDouble()
    compile(function("log", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.log(arg)))
    compile(function("log", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("log10 function") {
    val arg = random.nextDouble()
    compile(function("log10", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.log10(arg)))
    compile(function("log10", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("sign function") {
    val arg = random.nextInt()
    compile(function("sign", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.signum(arg)))
    compile(function("sign", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("sqrt function") {
    val arg = random.nextDouble()
    compile(function("sqrt", literalFloat(arg))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(doubleValue(Math.sqrt(arg)))
    compile(function("sqrt", nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("pi function") {
    compile(function("pi")).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.PI)
  }

  test("e function") {
    compile(function("e")).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.E)
  }

  test("range function with no step") {
    val range = function("range", literalInt(5), literalInt(9))
    compile(range).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(list(longValue(5), longValue(6), longValue(7),
                                                                              longValue(8), longValue(9)))
  }

  test("range function with step") {
    val range = function("range", literalInt(5), literalInt(9), literalInt(2))
    compile(range).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(list(longValue(5), longValue(7), longValue(9)))
  }

  test("coalesce function") {
    compile(function("coalesce", nullLiteral, nullLiteral, literalInt(2), nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(2))
    compile(function("coalesce", nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("coalesce function with parameters") {
    val compiled = compile(function("coalesce", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, query, map(Array("a", "b", "c"), Array(NO_VALUE, longValue(2), NO_VALUE)), cursors) should equal(longValue(2))
    compiled.evaluate(ctx, query, map(Array("a", "b", "c"), Array(NO_VALUE, NO_VALUE, NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("distance function") {
    val compiled = compile(function("distance", parameter("p1"), parameter("p2")))
    val keys = Array("p1", "p2")
    compiled.evaluate(ctx, query, map(keys,
                                      Array(pointValue(Cartesian, 0.0, 0.0),
                                             pointValue(Cartesian, 1.0, 1.0))), cursors) should equal(doubleValue(Math.sqrt(2)))
    compiled.evaluate(ctx, query, map(keys,
                                      Array(pointValue(Cartesian, 0.0, 0.0),
                                             NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(keys,
                                      Array(pointValue(Cartesian, 0.0, 0.0),
                                             pointValue(WGS84, 1.0, 1.0))), cursors) should equal(NO_VALUE)

  }

  test("startNode") {
    val rel = relationshipValue()
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    val compiled = compile(function("startNode", parameter("a")), slots)
    addRelationships(context, RelAt(rel, 0))
    compiled.evaluate(context, query, map(Array("a"), Array(rel)), cursors) should equal(rel.startNode())
    compiled.evaluate(context, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("endNode") {
    val rel = relationshipValue()
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    val compiled = compile(function("endNode", parameter("a")), slots)
    addRelationships(context, RelAt(rel, 0))
    compiled.evaluate(context, query, map(Array("a"), Array(rel)), cursors) should equal(rel.endNode())
    compiled.evaluate(context, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("exists on node") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val node = nodeValue(map(Array("prop"), Array(stringValue("hello"))))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(Values.TRUE)
  }

  test("exists on relationship") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val rel = relationshipValue(nodeValue(),
                                nodeValue(),
                                map(Array("prop"), Array(stringValue("hello"))))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(Values.TRUE)
  }

  test("exists on map") {
    val compiled = compile(function("exists", property(parameter("a"), "prop")))

    val mapValue = map(Array("prop"), Array(stringValue("hello")))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(mapValue)), cursors) should equal(Values.TRUE)
  }

  test("head function") {
    val compiled = compile(function("head", parameter("a")))
    val listValue = list(stringValue("hello"), intValue(42))

    compiled.evaluate(ctx, query, map(Array("a"), Array(listValue)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(EMPTY_LIST)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("last function") {
    val compiled = compile(function("last", parameter("a")))
    val listValue = list(intValue(42), stringValue("hello"))

    compiled.evaluate(ctx, query, map(Array("a"), Array(listValue)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(EMPTY_LIST)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("left function") {
    val compiled = compile(function("left", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(4))), cursors) should
      equal(stringValue("HELL"))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(17))), cursors) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)

    an[IndexOutOfBoundsException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(-1))), cursors)
  }

  test("ltrim function") {
    val compiled = compile(function("ltrim", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("  HELLO  "))), cursors) should
      equal(stringValue("HELLO  "))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("rtrim function") {
    val compiled = compile(function("rtrim", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("  HELLO  "))), cursors) should
      equal(stringValue("  HELLO"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("trim function") {
    val compiled = compile(function("trim", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("  HELLO  "))), cursors) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("replace function") {
    val compiled = compile(function("replace", parameter("a"), parameter("b"), parameter("c")))

    compiled.evaluate(ctx, query, map(Array("a", "b", "c"),
                                      Array(stringValue("HELLO"),
                                             stringValue("LL"),
                                             stringValue("R"))), cursors) should equal(stringValue("HERO"))
    compiled.evaluate(ctx, query, map(Array("a", "b", "c"),
                                      Array(NO_VALUE,
                                             stringValue("LL"),
                                             stringValue("R"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b", "c"),
                                      Array(stringValue("HELLO"),
                                             NO_VALUE,
                                             stringValue("R"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b", "c"),
                                      Array(stringValue("HELLO"),
                                             stringValue("LL"),
                                             NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("reverse function") {
    val compiled = compile(function("reverse", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("PARIS"))), cursors) should equal(stringValue("SIRAP"))
    val original = list(intValue(1), intValue(2), intValue(3))
    val reversed = list(intValue(3), intValue(2), intValue(1))
    compiled.evaluate(ctx, query, map(Array("a"), Array(original)), cursors) should equal(reversed)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("right function") {
    val compiled = compile(function("right", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(4))), cursors) should
      equal(stringValue("ELLO"))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)
  }

  test("split function") {
    val compiled = compile(function("split", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), stringValue("LL"))), cursors) should
      equal(list(stringValue("HE"), stringValue("O")))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, stringValue("LL"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), EMPTY_STRING)), cursors) should
      equal(list(stringValue("H"), stringValue("E"), stringValue("L"), stringValue("L"), stringValue("O")))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(EMPTY_STRING, stringValue("LL"))), cursors) should equal(list(EMPTY_STRING))

  }

  test("substring function no length") {
    val compiled = compile(function("substring", parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("HELLO"), intValue(1))), cursors) should
      equal(stringValue("ELLO"))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
  }

  test("substring function with length") {
    compile(function("substring", parameter("a"), parameter("b"), parameter("c")))
      .evaluate(ctx, query, map(Array("a", "b", "c"), Array(stringValue("HELLO"), intValue(1), intValue(2))), cursors) should equal(stringValue("EL"))
    compile(function("substring", parameter("a"), parameter("b")))
      .evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
  }

  test("toLower function") {
    val compiled = compile(function("toLower", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("HELLO"))), cursors) should
      equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toUpper function") {
    val compiled = compile(function("toUpper", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("hello"))), cursors) should
      equal(stringValue("HELLO"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("nodes function") {
    val compiled = compile(function("nodes", parameter("a")))

    val p = path(2)

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(p)), cursors) should equal(VirtualValues.list(p.nodes():_*))
  }

  test("relationships function") {
    val compiled = compile(function("relationships", parameter("a")))

    val p = path(2)

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(p)), cursors) should equal(VirtualValues.list(p.relationships():_*))
  }

  test("id on node") {
    val compiled = compile(function("id", parameter("a")))

    val node = nodeValue()

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(longValue(node.id()))
  }

  test("id on relationship") {
    val compiled = compile(function("id", parameter("a")))

    val rel = relationshipValue()

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(longValue(rel.id()))
  }

  test("labels function") {
    val compiled = compile(function("labels", parameter("a")))

    val labels = Values.stringArray("A", "B", "C")
    val node = ValueUtils.fromNodeProxy(createLabeledNode("A", "B", "C"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(labels)
  }

  test("type function") {
    val compiled = compile(function("type", parameter("a")))
    val rel = ValueUtils.fromRelationshipProxy(relate(createNode(), createNode(), "R"))

    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(stringValue("R"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("points from node") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val node = nodeValue(pointMap)

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(PointValue.fromMap(pointMap))
  }

  test("points from relationship") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    val rel = relationshipValue(nodeValue(), nodeValue(), pointMap)

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(PointValue.fromMap(pointMap))
  }

  test("points from map") {
    val compiled = compile(function("point", parameter("a")))

    val pointMap = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(pointMap)), cursors) should equal(PointValue.fromMap(pointMap))
  }

  test("keys on node") {
    val compiled = compile(function("keys", parameter("a")))
    val node = nodeValue(map(Array("A", "B", "C"), Array(stringValue("a"), stringValue("b"), stringValue("c"))))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on relationship") {
    val compiled = compile(function("keys", parameter("a")))


    val rel = relationshipValue(nodeValue(), nodeValue(),
                                map(Array("A", "B", "C"),
                                    Array(stringValue("a"), stringValue("b"), stringValue("c"))))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(Values.stringArray("A", "B", "C"))
  }

  test("keys on map") {
    val compiled = compile(function("keys", parameter("a")))

    val mapValue = map(Array("x", "y", "crs"),
                       Array(doubleValue(1.0), doubleValue(2.0), stringValue("cartesian")))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(mapValue)), cursors) should equal(mapValue.keys())
  }

  test("size function") {
    val compiled = compile(function("size", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("HELLO"))), cursors) should equal(intValue(5))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)
  }

  test("length function") {
    val compiled = compile(function("length", parameter("a")))

    val p = path(2)

    compiled.evaluate(ctx, query, map(Array("a"), Array(p)), cursors) should equal(intValue(2))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(4))), cursors) should equal(NO_VALUE)
  }

  test("tail function") {
    val compiled = compile(function("tail", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(list(intValue(2), intValue(3)))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toBoolean function") {
    val compiled = compile(function("toBoolean", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(Values.TRUE)), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(Values.FALSE)), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("false"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("true"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("uncertain"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toFloat function") {
    val compiled = compile(function("toFloat", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(doubleValue(3.2))), cursors) should equal(doubleValue(3.2))
    compiled.evaluate(ctx, query, map(Array("a"), Array(intValue(3))), cursors) should equal(doubleValue(3))
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("3.2"))), cursors) should equal(doubleValue(3.2))
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("three dot two"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toInteger function") {
    val compiled = compile(function("toInteger", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(doubleValue(3.2))), cursors) should equal(longValue(3))
    compiled.evaluate(ctx, query, map(Array("a"), Array(intValue(3))), cursors) should equal(intValue(3))
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("3"))), cursors) should equal(longValue(3))
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("three"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("toString function") {
    val compiled = compile(function("toString", parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(doubleValue(3.2))), cursors) should equal(stringValue("3.2"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(Values.TRUE)), cursors) should equal(stringValue("true"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(pointValue(Cartesian, 0.0, 0.0))), cursors) should
      equal(stringValue("point({x: 0.0, y: 0.0, crs: 'cartesian'})"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(durationValue(Duration.ofHours(3)))), cursors) should
      equal(stringValue("PT3H"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(temporalValue(localTime(20, 0, 0, 0)))), cursors) should
      equal(stringValue("20:00:00"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    a [ParameterWrongTypeException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a"), Array(intArray(Array(1, 2, 3)))), cursors)
  }

  test("properties function on node") {
    val compiled = compile(function("properties", parameter("a")))
    val mapValue = map(Array("prop"), Array(longValue(42)))
    val node = nodeValue(mapValue)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(mapValue)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("properties function on relationship") {
    val compiled = compile(function("properties", parameter("a")))
    val mapValue = map(Array("prop"), Array(longValue(42)))
    val rel = relationshipValue(nodeValue(),
                                nodeValue(), mapValue)
    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(mapValue)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("properties function on map") {
    val compiled = compile(function("properties", parameter("a")))
    val mapValue = map(Array("prop"), Array(longValue(42)))

    compiled.evaluate(ctx, query, map(Array("a"), Array(mapValue)), cursors) should equal(mapValue)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("add numbers") {
    // Given
    val expression = add(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(52))
  }

  test("add temporals") {
    val compiled = compile(add(parameter("a"), parameter("b")))

    // temporal + duration
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(temporalValue(localTime(0)),
                                                             durationValue(Duration.ofHours(10)))), cursors) should
      equal(localTime(10, 0, 0, 0))

    // duration + temporal
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                             temporalValue(localTime(0)))), cursors) should
      equal(localTime(10, 0, 0, 0))

    //duration + duration
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                             durationValue(Duration.ofHours(10)))), cursors) should
      equal(durationValue(Duration.ofHours(20)))
  }

  test("add with NO_VALUE") {
    // Given
    val expression = add(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
  }

  test("add strings") {
    // When
    val compiled = compile(add(parameter("a"), parameter("b")))

    // string1 + string2
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello "), stringValue("world"))), cursors) should
      equal(stringValue("hello world"))
    //string + other
    compiled.evaluate(ctx, query, map(Array("a", "b"),
                                      Array(stringValue("hello "), longValue(1337))), cursors) should
      equal(stringValue("hello 1337"))
    //other + string
    compiled.evaluate(ctx, query, map(Array("a", "b"),
                                      Array(longValue(1337), stringValue(" hello"))), cursors) should
      equal(stringValue("1337 hello"))

  }

  test("add arrays") {
    // Given
    val expression = add(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"),
                                      Array(longArray(Array(42, 43)),
                                            longArray(Array(44, 45)))), cursors) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))
  }

  test("list addition") {
    // When
    val compiled = compile(add(parameter("a"), parameter("b")))

    // [a1,a2 ..] + [b1,b2 ..]
    compiled.evaluate(ctx, query, map(Array("a", "b"),
                                      Array(list(longValue(42), longValue(43)),
                                             list(longValue(44), longValue(45)))), cursors) should
      equal(list(longValue(42), longValue(43), longValue(44), longValue(45)))

    // [a1,a2 ..] + b
    compiled.evaluate(ctx, query, map(Array("a", "b"),
                                      Array(list(longValue(42), longValue(43)), longValue(44))), cursors) should
      equal(list(longValue(42), longValue(43), longValue(44)))

    // a + [b1,b2 ..]
    compiled.evaluate(ctx, query, map(Array("a", "b"),
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
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(42))
  }

  test("subtract numbers") {
    // Given
    val expression = subtract(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(32))
  }

  test("subtract with NO_VALUE") {
    // Given
    val expression = subtract(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
  }

  test("subtract temporals") {
    val compiled = compile(subtract(parameter("a"), parameter("b")))

    // temporal - duration
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(temporalValue(localTime(20, 0, 0, 0)),
                                                             durationValue(Duration.ofHours(10)))), cursors) should
      equal(localTime(10, 0, 0, 0))

    //duration - duration
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(durationValue(Duration.ofHours(10)),
                                                             durationValue(Duration.ofHours(10)))), cursors) should
      equal(durationValue(Duration.ofHours(0)))
  }

  test("unary subtract ") {
    // Given
    val expression = unarySubtract(literalInt(42))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(-42))
  }

  test("multiply function") {
    // Given
    val expression = multiply(literalInt(42), literalInt(10))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(longValue(420))
  }

  test("multiply with NO_VALUE") {
    // Given
    val expression = multiply(parameter("a"), parameter("b"))

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
  }

  test("division") {
    val compiled = compile(divide(parameter("a"), parameter("b")))

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(6), longValue(3))), cursors) should equal(longValue(2))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(5), doubleValue(2))), cursors) should equal(doubleValue(2.5))
    an[ArithmeticException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(5), longValue(0))), cursors)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(doubleValue(3.0), doubleValue(0.0))), cursors) should equal(doubleValue(Double.PositiveInfinity))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(durationValue(Duration.ofHours(4)), longValue(2))), cursors) should equal(durationValue(Duration.ofHours(2)))
    an[ArithmeticException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(0))), cursors)
  }

  test("modulo") {
    val compiled = compile(modulo(parameter("a"), parameter("b")))

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(doubleValue(8.0), longValue(6))), cursors) should equal(doubleValue(2.0))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(8), doubleValue(6))), cursors) should equal(doubleValue(2.0))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(8), longValue(6))), cursors) should equal(longValue(2))
  }

  test("pow") {
    val compiled = compile(pow(parameter("a"), parameter("b")))

    // Then
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, longValue(42))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(doubleValue(2), longValue(3))), cursors) should equal(doubleValue(8.0))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(2), longValue(3))), cursors) should equal(doubleValue(8.0))
  }

  test("extract parameter") {
    a[ParameterNotFoundException] should be thrownBy compile(parameter("prop")).evaluate(ctx, query, EMPTY_MAP, cursors)
    compile(parameter("prop")).evaluate(ctx, query, map(Array("prop"), Array(stringValue("foo"))), cursors) should equal(stringValue("foo"))
    compile(parameter("    AUTOBLAH BLAH BLAHA   ")).evaluate(ctx, query, map(Array("    AUTOBLAH BLAH BLAHA   "), Array(stringValue("foo"))), cursors) should equal(stringValue("foo"))
  }

  test("extract multiple parameters with whitespaces") {
    compile(add(parameter(" A "), parameter("\tA\t")))
          .evaluate(ctx, query, map(Array(" A ", "\tA\t"), Array(longValue(1), longValue(2) )), cursors) should equal(longValue(3))
    compile(add(parameter(" A "), parameter("_A_")))
          .evaluate(ctx, query, map(Array(" A ", "_A_"), Array(longValue(1), longValue(2) )), cursors) should equal(longValue(3))
  }

  test("NULL") {
    // Given
    val expression = nullLiteral

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("TRUE") {
    // Given
    val expression = trueLiteral

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("FALSE") {
    // Given
    val expression = falseLiteral

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("OR") {
    compile(or(trueLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(falseLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(trueLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(falseLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)

    compile(or(nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(or(nullLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(trueLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(nullLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(or(falseLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("XOR") {
    compile(xor(trueLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(xor(falseLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(xor(trueLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(xor(falseLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)

    compile(xor(nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(nullLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(trueLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(nullLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(xor(falseLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("OR should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy compile(or(literalInt(42), falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors)
    a [CypherTypeException] should be thrownBy compile(or(falseLiteral, literalInt(42))).evaluate(ctx, query, EMPTY_MAP, cursors)
    compile(or(trueLiteral, literalInt(42))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(or(literalInt(42), trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("OR should handle coercion") {
    val expression =  compile(or(parameter("a"), parameter("b")))
    expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.FALSE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.FALSE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("ORS") {
    compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, trueLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(ors(falseLiteral, falseLiteral, falseLiteral, falseLiteral, nullLiteral, falseLiteral, falseLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(ors(falseLiteral, falseLiteral, falseLiteral, trueLiteral, nullLiteral, trueLiteral, falseLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("ORS should throw on non-boolean input") {
    val compiled = compile(ors(parameter("a"), parameter("b"), parameter("c"), parameter("d"), parameter("e")))
    val keys = Array("a", "b", "c", "d", "e")
    compiled.evaluate(ctx, query, map(keys, Array(Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE)), cursors) should equal(Values.FALSE)

    compiled.evaluate(ctx, query, map(keys, Array(Values.FALSE, Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE)), cursors) should equal(Values.TRUE)

    compiled.evaluate(ctx, query, map(keys, Array(intValue(42), Values.FALSE, Values.TRUE, Values.FALSE, Values.FALSE)), cursors) should equal(Values.TRUE)

    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, query, map(keys, Array(intValue(42), Values.FALSE, Values.FALSE, Values.FALSE, Values.FALSE)), cursors)
  }

  test("ORS should handle coercion") {
    val expression =  compile(ors(parameter("a"), parameter("b")))
    expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.FALSE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.FALSE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("AND") {
    compile(and(trueLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(and(falseLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(trueLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(falseLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)

    compile(and(nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(and(nullLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(and(trueLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(and(nullLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(falseLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("AND should throw on non-boolean input") {
    a [CypherTypeException] should be thrownBy compile(and(literalInt(42), trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors)
    a [CypherTypeException] should be thrownBy compile(and(trueLiteral, literalInt(42))).evaluate(ctx, query, EMPTY_MAP, cursors)
    compile(and(falseLiteral, literalInt(42))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(and(literalInt(42), falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("AND should handle coercion") {
    val expression =  compile(and(parameter("a"), parameter("b")))
   expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.TRUE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
   expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.TRUE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("ANDS") {
    compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, trueLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(ands(trueLiteral, trueLiteral, trueLiteral, trueLiteral, nullLiteral, trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(ands(trueLiteral, trueLiteral, trueLiteral, falseLiteral, nullLiteral, falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("ANDS should throw on non-boolean input") {
    val compiled = compile(ands(parameter("a"), parameter("b"), parameter("c"), parameter("d"), parameter("e")))
    val keys = Array("a", "b", "c", "d", "e")
    compiled.evaluate(ctx, query, map(keys, Array(Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE)), cursors) should equal(Values.TRUE)

    compiled.evaluate(ctx, query, map(keys, Array(Values.TRUE, Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE)), cursors) should equal(Values.FALSE)

    compiled.evaluate(ctx, query, map(keys, Array(intValue(42), Values.TRUE, Values.FALSE, Values.TRUE, Values.TRUE)), cursors) should equal(Values.FALSE)

    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, query, map(keys, Array(intValue(42), Values.TRUE, Values.TRUE, Values.TRUE, Values.TRUE)), cursors)
  }

  test("ANDS should handle coercion") {
    val expression =  compile(ands(parameter("a"), parameter("b")))
    expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.TRUE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    expression.evaluate(ctx, query, map(Array("a", "b"), Array(Values.TRUE, list(stringValue("hello")))), cursors) should equal(Values.TRUE)
  }

  test("NOT") {
    compile(not(falseLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(not(trueLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(not(nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("NOT should handle coercion") {
    val expression =  compile(not(parameter("a")))
    expression.evaluate(ctx, query, map(Array("a"), Array(EMPTY_LIST)), cursors) should equal(Values.TRUE)
    expression.evaluate(ctx, query, map(Array("a"), Array(list(stringValue("hello")))), cursors) should equal(Values.FALSE)
  }

  test("EQUALS") {
    compile(equals(literalInt(42), literalInt(42))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(equals(literalInt(42), literalInt(43))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(equals(nullLiteral, literalInt(43))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(equals(literalInt(42), nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(equals(nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(equals(TRUE, equals(TRUE, equals(TRUE, nullLiteral)))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("NOT EQUALS") {
    compile(notEquals(literalInt(42), literalInt(42))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(notEquals(literalInt(42), literalInt(43))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(notEquals(nullLiteral, literalInt(43))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(notEquals(literalInt(42), nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(notEquals(nullLiteral, nullLiteral)).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(notEquals(TRUE, notEquals(TRUE, notEquals(TRUE, nullLiteral)))).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
  }

  test("regex match on literal pattern") {
    val compiled= compile(regex(parameter("a"), literalString("hell.*")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("helo"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(Values.NO_VALUE)), cursors) should equal(Values.NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(longValue(42))), cursors) should equal(Values.NO_VALUE)
  }

  test("regex match on general expression") {
    val compiled= compile(regex(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hell.*"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("helo"), stringValue("hell.*"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(Values.NO_VALUE, stringValue("hell.*"))), cursors) should equal(Values.NO_VALUE)
    a [CypherTypeException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("forty-two"), longValue(42))), cursors)
    an [InvalidSemanticsException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("["))), cursors)
  }

  test("startsWith") {
    val compiled= compile(startsWith(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hell"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hi"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, stringValue("hi"))), cursors) should equal(NO_VALUE)
  }

  test("endsWith") {
    val compiled= compile(endsWith(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("ello"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hi"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, stringValue("hi"))), cursors) should equal(NO_VALUE)
  }

  test("contains") {
    val compiled= compile(contains(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("ell"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), stringValue("hi"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(stringValue("hello"), NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, stringValue("hi"))), cursors) should equal(NO_VALUE)
  }

  test("in") {
    val compiled = compile(in(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(intValue(3), list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(intValue(4), list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, list(intValue(1), intValue(2), intValue(3)))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, EMPTY_LIST)), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(intValue(3), list(intValue(1), NO_VALUE, intValue(3)))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(intValue(4), list(intValue(1), NO_VALUE, intValue(3)))), cursors) should equal(Values.NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(intValue(4), NO_VALUE)), cursors) should equal(Values.NO_VALUE)
  }

  test("in with literal list not containing null") {
    val compiled = compile(in(parameter("a"),
                              listOf(literalString("a"), literalString("b"), literalString("c"))))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("a"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("b"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("c"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("A"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("in with literal list containing null") {
    val compiled = compile(in(parameter("a"),
                              listOf(literalString("a"), nullLiteral, literalString("c"))))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("a"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("c"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("b"))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("in with empty literal list") {
    val compiled = compile(in(parameter("a"), listOf()))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("a"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(Values.FALSE)
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

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(Values.FALSE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(Values.TRUE)
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

    compile(isNull(NullCheckVariable(nullOffset, NodeFromSlot(nullOffset, "n"))), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(isNull(NullCheckVariable(nodeOffset, NodeFromSlot(nodeOffset, "n"))), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("isNotNull") {
    val compiled= compile(isNotNull(parameter("a")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(stringValue("hello"))), cursors) should equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(Values.FALSE)
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

    compile(isNotNull(NullCheckVariable(nullOffset, NodeFromSlot(nullOffset, "n"))), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(isNotNull(NullCheckVariable(nodeOffset, NodeFromSlot(nodeOffset, "n"))), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("CoerceToPredicate") {
    val coerced = CoerceToPredicate(parameter("a"))

    compile(coerced).evaluate(ctx, query, map(Array("a"), Array(Values.FALSE)), cursors) should equal(Values.FALSE)
    compile(coerced).evaluate(ctx, query, map(Array("a"), Array(Values.TRUE)), cursors) should equal(Values.TRUE)
    compile(coerced).evaluate(ctx, query, map(Array("a"), Array(list(stringValue("A")))), cursors) should equal(Values.TRUE)
    compile(coerced).evaluate(ctx, query, map(Array("a"), Array(list(EMPTY_LIST))), cursors) should equal(Values.TRUE)
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
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello"))
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
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(longValue(42))
  }

  test("PrimitiveEquals") {
    val compiled = compile(PrimitiveEquals(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), longValue(42))), cursors) should
      equal(Values.TRUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(longValue(42), longValue(1337))), cursors) should
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

    compile(NullCheck(nullOffset, literalFloat(PI)), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.NO_VALUE)
    compile(NullCheck(offset, literalFloat(PI)), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.PI)
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

    compile(NullCheckVariable(1, ReferenceFromSlot(0, "aRef")), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(Values.NO_VALUE)
    compile(NullCheckVariable(0, ReferenceFromSlot(0, "aRef")), slots).evaluate(context, query, EMPTY_MAP, cursors) should
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

    compile(IsPrimitiveNull(nullOffset), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(IsPrimitiveNull(notNullOffset)).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("containerIndex on node") {
    val node =  nodeValue(map(Array("prop"), Array(stringValue("hello"))))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("containerIndex on relationship") {
    val rel = relationshipValue(nodeValue(),
                                nodeValue(),
                                map(Array("prop"), Array(stringValue("hello"))))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("containerIndex on map") {
    val mapValue = map(Array("prop"), Array(stringValue("hello")))
    val compiled = compile(containerIndex(parameter("a"), literalString("prop")))

    compiled.evaluate(ctx, query, map(Array("a"), Array(mapValue)), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("containerIndex on list") {
    val listValue = list(longValue(42), stringValue("hello"), intValue(42))
    val compiled = compile(containerIndex(parameter("a"), parameter("b")))

    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(listValue, intValue(1))), cursors) should equal(stringValue("hello"))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(listValue, intValue(-1))), cursors) should equal(intValue(42))
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(listValue, intValue(3))), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
    an [InvalidArgumentException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a", "b"), Array(listValue, longValue(Int.MaxValue + 1L))), cursors)
  }

  test("handle list literals") {
    val literal = listOf(trueLiteral, literalInt(5), nullLiteral, falseLiteral)

    val compiled = compile(literal)

    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(list(Values.TRUE, intValue(5), NO_VALUE, Values.FALSE))
  }

  test("handle map literals") {
    val literal = mapOfInt("foo" -> 1, "bar" -> 2, "baz" -> 3)

    val compiled = compile(literal)

    import scala.collection.JavaConverters._
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(ValueUtils.asMapValue(Map("foo" -> 1, "bar" -> 2, "baz" -> 3).asInstanceOf[Map[String, AnyRef]].asJava))
  }

  test("handle map literals with null") {
    val literal = mapOf("foo" -> literalInt(1), "bar" -> nullLiteral, "baz" -> literalString("three"))

    val compiled = compile(literal)

    import scala.collection.JavaConverters._
    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(ValueUtils.asMapValue(Map("foo" -> 1, "bar" -> null, "baz" -> "three").asInstanceOf[Map[String, AnyRef]].asJava))
  }

  test("handle empty map literals") {
    val literal = mapOf()

    val compiled = compile(literal)

    compiled.evaluate(ctx, query, EMPTY_MAP, cursors) should equal(EMPTY_MAP)
  }

  test("from slice") {
    val slice = compile(sliceFrom(parameter("a"), parameter("b")))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    slice.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(3))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, NO_VALUE)), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, intValue(2))), cursors) should equal(VirtualValues.list(intValue(3)))
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, intValue(-2))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, intValue(0))), cursors) should equal(list)
  }

  test("to slice") {
    val slice = compile(sliceTo(parameter("a"), parameter("b")))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    slice.evaluate(ctx, query, map(Array("a", "b"), Array(NO_VALUE, intValue(1))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, NO_VALUE)), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, intValue(2))), cursors) should equal(VirtualValues.list(intValue(1), intValue(2)))
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, intValue(-2))), cursors) should equal(VirtualValues.list(intValue(1)))
    slice.evaluate(ctx, query, map(Array("a", "b"), Array(list, intValue(0))), cursors) should equal(EMPTY_LIST)
  }

  test("full slice") {
    val slice = compile(sliceFull(parameter("a"), parameter("b"), parameter("c")))
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3), intValue(4), intValue(5))

    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(NO_VALUE, intValue(1), intValue(3))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, NO_VALUE, intValue(3))), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, intValue(3), NO_VALUE)), cursors) should equal(NO_VALUE)
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, intValue(1), intValue(3))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, intValue(1), intValue(-2))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, intValue(-4), intValue(3))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, intValue(-4), intValue(-2))), cursors) should equal(VirtualValues.list(intValue(2), intValue(3)))
    slice.evaluate(ctx, query, map(Array("a", "b", "c"), Array(list, intValue(0), intValue(0))), cursors) should equal(EMPTY_LIST)
  }

  test("handle variables") {
    val variable = varFor("key")
    val compiled = compile(variable)
    val context = new MapExecutionContext(mutable.Map("key" -> stringValue("hello")))
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("handle variables with whitespace ") {
    val varName = "   k\te\ty   "
    val variable = varFor(varName)
    val compiled = compile(variable)
    val context = new MapExecutionContext(mutable.Map(varName -> stringValue("hello")))
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello"))
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
    val mapValue = map(Array("prop"), Array(longValue(1337)))
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
    val compiled = compile(property(parameter("a"), "prop"))

    val node = nodeValue(map(Array("prop"), Array(stringValue("hello"))))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(node)), cursors) should equal(stringValue("hello"))
  }

  test("access property on relationship") {
    val compiled = compile(property(parameter("a"), "prop"))

    val rel = relationshipValue(map(Array("prop"), Array(stringValue("hello"))))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(rel)), cursors) should equal(stringValue("hello"))
  }

  test("access property on map") {
    val compiled = compile(property(parameter("a"), "prop"))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(map(Array("prop"), Array(stringValue("hello"))))), cursors) should equal(stringValue("hello"))
  }

  test("access property on temporal") {
    val value = TimeValue.now(Clock.systemUTC())
    val compiled = compile(property(parameter("a"), "timezone"))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(value)), cursors) should equal(value.get("timezone"))
  }

  test("access property on duration") {
    val value = durationValue(Duration.ofHours(3))
    val compiled = compile(property(parameter("a"), "seconds"))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(value)), cursors) should equal(value.get("seconds"))
  }

  test("access property on point") {
    val value = pointValue(Cartesian, 1.0, 3.6)

    val compiled = compile(property(parameter("a"), "x"))

    compiled.evaluate(ctx, query, map(Array("a"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
    compiled.evaluate(ctx, query, map(Array("a"), Array(value)), cursors) should equal(doubleValue(1.0))
  }

  test("access property on point with invalid key") {
    val value = pointValue(Cartesian, 1.0, 3.6)

    val compiled = compile(property(parameter("a"), "foobar"))

    an[InvalidArgumentException] should be thrownBy compiled.evaluate(ctx, query, map(Array("a"), Array(value)), cursors)
  }

  test("should project") {
    //given
    val slots = SlotConfiguration(Map("a" -> RefSlot(0, nullable = true, symbols.CTAny),
                                      "b" -> RefSlot(1, nullable = true, symbols.CTAny)), 0, 2)
    val context = new SlottedExecutionContext(slots)
    val projections = Map("a" -> literal("hello"), "b" -> function("sin", parameter("param")))
    val compiled = compileProjection(projections, slots)

    //when
    compiled.project(context, query, map(Array("param"), Array(NO_VALUE)), cursors)

    //then
    context.getRefAt(0) should equal(stringValue("hello"))
    context.getRefAt(1) should equal(NO_VALUE)
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
    compiledNone.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
    compiledSingle.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledMany.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledNone.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
    compiledSingle.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledMany.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("single in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, single(bar IN null WHERE bar = foo)
    val compiled = compile(singleInList("bar", nullLiteral,
                                        equals(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("single in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, single(bar IN ['a','aa','aaa'] WHERE bar = null)
    val compiled = compile(singleInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("single function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When, single(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(singleInList("bar", varFor("foo"),
                                      equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("single function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When, single(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(singleInList("bar", parameter("a"),
                                      equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(Values.TRUE)
  }

  test("single on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, single(bar IN [] WHERE bar = 42)
    val compiled = compile(singleInList("bar", listOf(), equals(literalInt(42), varFor("bar"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("none in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, none(bar IN null WHERE bar = foo)
    val compiled = compile(noneInList("bar", nullLiteral,
                                      equals(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("none in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, none(bar IN null WHERE bar = null)
    val compiled = compile(noneInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), nullLiteral )))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("none function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  none(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(noneInList("bar", varFor("foo"),
                                  equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(FALSE)
  }

  test("none function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  none(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(noneInList("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(FALSE)
  }

  test("none on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, none(bar IN [] WHERE bar = 42)
    val compiled = compile(noneInList("bar", listOf(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("any in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, any(bar IN null WHERE bar = foo)
    val compiled = compile(anyInList("bar", nullLiteral,
                                     equals(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("any in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, any(bar IN ['a','aa','aaa'] WHERE bar = null)
    val compiled = compile(anyInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      equals(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("any function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  any(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(anyInList("bar", varFor("foo"),
                                  equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("any function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  any(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(anyInList("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(Values.TRUE)
  }

  test("any on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, any(bar IN [] WHERE bar = 42)
    val compiled = compile(anyInList("bar", listOf(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("all in list on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, all(bar IN null WHERE bar STARTS WITH foo)
    val compiled = compile(allInList("bar", nullLiteral,
                                     startsWith(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("all in list with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, all(bar IN null WHERE bar STARTS WITH null)
    val compiled = compile(allInList("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("all function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When, all(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(allInList("bar", varFor("foo"),
                                  equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(FALSE)
  }

  test("all function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  all(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(allInList("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(Values.FALSE)
  }

  test("all on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, all(bar IN [] WHERE bar = 42)
    val compiled = compile(allInList("bar", listOf(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("filter function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH "aa")
    val compiled = compile(filter("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), literalString("aa"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("filter function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> stringValue("aa")))

    //When, filter(bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH foo)
    val compiled = compile(filter("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), varFor("foo"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("filter on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN null WHERE bar STARTS WITH 'aa')
    val compiled = compile(filter("bar", nullLiteral,
                                  startsWith(varFor("bar"), varFor("aa"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("filter with null predicate") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN null WHERE bar STARTS WITH null)
    val compiled = compile(filter("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
      startsWith(varFor("bar"), nullLiteral)))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list())
  }

  test("filter function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))))

    //When,  filter(bar IN foo WHERE size(bar) = size(foo))
    val compiled = compile(filter("bar", varFor("foo"),
                                   equals(function("size", varFor("bar")), function("size", varFor("foo")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.list(stringValue("aaa")))
  }

  test("filter function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(stringValue("a"), stringValue("aa"), stringValue("aaa"))

    //When,  filter(bar IN $a WHERE size(bar) = size($a))
    val compiled = compile(filter("bar", parameter("a"),
                                  equals(function("size", varFor("bar")), function("size", parameter("a")))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(VirtualValues.list(stringValue("aaa")))
  }

  test("filter on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, filter(bar IN [] WHERE bar = 42)
    val compiled = compile(filter("bar", listOf(),
                                      equals(varFor("bar"), literalInt(42))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(EMPTY_LIST)
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
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
    compiledTrue.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(true))
    compiledFalse.evaluate(context, query, EMPTY_MAP, cursors) should equal(booleanValue(false))
  }

  test("extract function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, extract(bar IN ["a", "aa", "aaa"] | size(bar))
    val compiled = compile(extract("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                   function("size", varFor("bar"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(intValue(1), intValue(2), intValue(3)))
  }

  test("extract function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> intValue(10)), mutable.Map.empty)

    //When, extract(bar IN [1, 2, 3] | bar + foo)
    val compiled = compile(extract("bar", listOf(literalInt(1), literalInt(2), literalInt(3)),
                                   add(varFor("foo"), varFor("bar"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(intValue(11), intValue(12), intValue(13)))
  }

  test("extract on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, extract(bar IN null | size(bar)
    val compiled = compile(extract("bar", nullLiteral,
                                   function("size", varFor("bar"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("extract function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(intValue(1), intValue(2), intValue(3))), mutable.Map.empty)

    //When, extract(bar IN foo | size(foo)
    val compiled = compile(extract("bar", varFor("foo"),
                                  function("size", varFor("foo"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.list(intValue(3), intValue(3), intValue(3)))
  }

  test("extract function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    //When, extract(bar IN $a | size($a)
    val compiled = compile(extract("bar", parameter("a"),
                                   function("size", parameter("a"))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(VirtualValues.list(intValue(3), intValue(3), intValue(3)))
  }

  test("extract on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, extaract(bar IN [] | bar = 42)
    val compiled = compile(extract("bar", listOf(), literalInt(42)))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(EMPTY_LIST)
  }

  test("reduce function local access only") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, reduce(count = 0, bar IN ["a", "aa", "aaa"] | count + size(bar))
    val compiled = compile(reduce(varFor("count"), literalInt(0), varFor("bar"), listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                   add(function("size", varFor("bar")), varFor("count"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(intValue(6))
  }

  test("reduce function local access only slotted") {
    //Given
    val slots = SlotConfiguration(Map("count" -> RefSlot(0, nullable = true, symbols.CTAny),
                                      "bar" -> RefSlot(1, nullable = true, symbols.CTAny)), 0, 2)
    //Note: this is needed for interpreted
    SlotConfigurationUtils.generateSlotAccessorFunctions(slots)

    val context = SlottedExecutionContext(slots)
    val count = ReferenceFromSlot(slots("count").offset, "count")
    val bar = ReferenceFromSlot(slots("bar").offset, "bar")

    val reduceExpression = reduce(count, literalInt(0), bar, parameter("list"),  add(function("size", bar), count))

    //When, reduce(count = 0, bar IN ["a", "aa", "aaa"] | count + size(bar))
    val compiled = compile(reduceExpression, slots)

    //Then
    compiled.evaluate(context, query, map(Array("list"), Array(list(stringValue("a"), stringValue("aa"),
                                                                    stringValue("aaa")))), cursors) should equal(intValue(6))
    compiled.evaluate(context, query, map(Array("list"), Array(NO_VALUE)), cursors) should equal(NO_VALUE)
  }

  test("reduce function accessing outer scope") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> intValue(10)), mutable.Map.empty)

    //When, reduce(count = 0, bar IN [1, 2, 3] | count + bar + foo)
    val compiled = compile(reduce(varFor("count"), literalInt(0),  varFor("bar"),
                                  listOf(literalInt(1), literalInt(2), literalInt(3)),
                                   add(add(varFor("foo"), varFor("bar")), varFor("count"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(intValue(36))
  }

  test("reduce on null") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, reduce(count = 0, bar IN null | count + size(bar))
    val compiled = compile(reduce(varFor("count"), literalInt(0), varFor("bar"), nullLiteral,
                                  add(function("size", varFor("bar")), varFor("count"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
  }

  test("reduce function accessing same variable in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map("foo" -> VirtualValues.list(intValue(1), intValue(2), intValue(3))), mutable.Map.empty)

    //When, reduce(count = 0, bar IN foo | count + size(foo)
    val compiled = compile(reduce(varFor("count"), literalInt(0), varFor("bar"), varFor("foo"),
                                  add(function("size", varFor("foo")), varFor("count"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(intValue(9))
  }

  test("reduce function accessing the same parameter in inner and outer") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)
    val list = VirtualValues.list(intValue(1), intValue(2), intValue(3))

    //When, reduce(count = 0, bar IN $a | count + size($a))
    val compiled = compile(reduce(varFor("count"), literalInt(0), varFor("bar"), parameter("a"),
                                  add(function("size", parameter("a")), varFor("count"))))

    //Then
    compiled.evaluate(context, query, map(Array("a"), Array(list)), cursors) should equal(intValue(9))
  }

  test("reduce on empty list") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty)

    //When, reduce(count = 42, bar IN [] | count + 3)
    val compiled = compile(reduce(varFor("count"), literalInt(42), varFor("bar"), listOf(),
                                  add(literalInt(3), varFor("count"))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.intValue(42))
  }

  test("list comprehension with predicate and extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH 'aa' | bar + 'A']
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = Some(startsWith(varFor("bar"), literalString("aa"))),
                                             extractExpression = Some(add(varFor("bar"), literalString("A")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(stringValue("aaA"), stringValue("aaaA")))
  }

  test("list comprehension with no predicate but an extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"] | bar + 'A']
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = None,
                                             extractExpression = Some(add(varFor("bar"), literalString("A")))))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(stringValue("aA"), stringValue("aaA"), stringValue("aaaA")))
  }

  test("list comprehension with predicate but no extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"] WHERE bar STARTS WITH 'aa']
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = Some(startsWith(varFor("bar"), literalString("aa"))),
                                             extractExpression = None))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(stringValue("aa"), stringValue("aaa")))
  }

  test("list comprehension with no predicate nor extract expression") {
    //Given
    val context = new MapExecutionContext(mutable.Map.empty, mutable.Map.empty)

    //When, [bar IN ["a", "aa", "aaa"]]
    val compiled = compile(listComprehension("bar", listOf(literalString("a"), literalString("aa"), literalString("aaa")),
                                             predicate = None,
                                             extractExpression = None))

    //Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(list(stringValue("a"), stringValue("aa"), stringValue("aaa")))
  }

  test("simple case expressions") {
    val alts = List(literalInt(42) -> literalString("42"), literalInt(1337) -> literalString("1337"))

    compile(simpleCase(parameter("a"), alts))
      .evaluate(ctx, query, parameters("a" -> intValue(42)), cursors) should equal(stringValue("42"))
    compile(simpleCase(parameter("a"), alts))
      .evaluate(ctx, query, parameters("a" -> intValue(1337)), cursors) should equal(stringValue("1337"))
    compile(simpleCase(parameter("a"), alts))
      .evaluate(ctx, query, parameters("a" -> intValue(-1)), cursors) should equal(NO_VALUE)
    compile(simpleCase(parameter("a"), alts, Some(literalString("THIS IS THE DEFAULT"))))
      .evaluate(ctx, query, parameters("a" -> intValue(-1)), cursors) should equal(stringValue("THIS IS THE DEFAULT"))
  }

  test("generic case expressions") {
    compile(genericCase(List(falseLiteral -> literalString("no"), trueLiteral -> literalString("yes"))))
      .evaluate(ctx, query, EMPTY_MAP, cursors) should equal(stringValue("yes"))
    compile(genericCase(List(trueLiteral -> literalString("no"), falseLiteral -> literalString("yes"))))
      .evaluate(ctx, query, EMPTY_MAP, cursors) should equal(stringValue("no"))
    compile(genericCase(List(falseLiteral -> literalString("no"), falseLiteral -> literalString("yes"))))
      .evaluate(ctx, query, EMPTY_MAP, cursors) should equal(NO_VALUE)
    compile(genericCase(List(falseLiteral -> literalString("no"), falseLiteral -> literalString("yes")), Some(literalString("default"))))
      .evaluate(ctx, query, EMPTY_MAP, cursors) should equal(stringValue("default"))
  }

  test("map projection node with map context") {
      val propertyMap = map(Array("prop"), Array(stringValue("hello")))
      val node = nodeValue(propertyMap)
      val context = new MapExecutionContext(mutable.Map("n" -> node))

      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")))
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")))
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection node from long slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val node = nodeValue(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> LongSlot(offset, nullable, symbols.CTNode)), 1, 0)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(offset, node.id())
      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection node from ref slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val node = nodeValue(propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> RefSlot(offset, nullable, symbols.CTNode)), 0, 1)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(offset, node)
      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection relationship with map context") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val relationship = relationshipValue(nodeValue(),
                                         nodeValue(), propertyMap)
    val context = new MapExecutionContext(mutable.Map("r" -> relationship))
    compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")))
      .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
    compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")))
      .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection relationship from long slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val relationship = relationshipValue(nodeValue(),
                                         nodeValue(), propertyMap)

    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("r" -> LongSlot(offset, nullable, symbols.CTRelationship)), 1, 0)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setLongAt(offset, relationship.id())
      compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection relationship from ref slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    val relationship = relationshipValue(nodeValue(),
                                         nodeValue(), propertyMap)
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("r" -> RefSlot(offset, nullable, symbols.CTRelationship)), 0, 1)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(offset, relationship)
      compile(mapProjection("r", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("r", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
    }
  }

  test("map projection mapValue with map context") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val context = new MapExecutionContext(mutable.Map("map" -> propertyMap))

    compile(mapProjection("map", includeAllProps = true, "foo" -> literalString("projected")))
      .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
    compile(mapProjection("map", includeAllProps = false, "foo" -> literalString("projected")))
      .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
  }

  test("map projection mapValue from ref slot") {
    val propertyMap = map(Array("prop"), Array(stringValue("hello")))
    val offset = 0
    for (nullable <- List(true, false)) {
      val slots = SlotConfiguration(Map("n" -> RefSlot(offset, nullable, symbols.CTMap)), 0, 1)
      //needed for interpreted
      SlotConfigurationUtils.generateSlotAccessorFunctions(slots)
      val context = SlottedExecutionContext(slots)
      context.setRefAt(offset, propertyMap)
      compile(mapProjection("n", includeAllProps = true, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(propertyMap.updatedWith("foo", stringValue("projected")))
      compile(mapProjection("n", includeAllProps = false, "foo" -> literalString("projected")), slots)
        .evaluate(context, query, EMPTY_MAP, cursors) should equal(map(Array("foo"), Array(stringValue("projected"))))
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
    compile(udf).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(stringValue("success"))
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
    compile(udf).evaluate(ctx, query, EMPTY_MAP, cursors) should equal(stringValue("success"))
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
    val key = compiled.computeGroupingKey(incoming, query, EMPTY_MAP, cursors)
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
    val key = compiled.computeGroupingKey(incoming, query, EMPTY_MAP, cursors)
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
    val key = compiled.computeGroupingKey(incoming, query, EMPTY_MAP, cursors)
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node), Array.empty))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
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
    p1.evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
    p2.evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n2.node, n1.node), Array(r.rel)))
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
    p1.evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
    p2.evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n2.node, n1.node), Array(r.rel)))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should be(NO_VALUE)
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node), Array(r.rel)))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(VirtualValues.path(Array(n1.node, n2.node, n3.node, n4.node), Array(r1.rel, r2.rel, r3.rel)))
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should be(NO_VALUE)
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
    compile(p, slots).evaluate(context, query, EMPTY_MAP, cursors) should be(NO_VALUE)
  }

  test("node property access") {
    // Given
    val n = createNode("prop" -> "hello")
    val token = tokenReader(_.propertyKey("prop"))
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 2, 1)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)
    val expression = NodeProperty(0, token, "prop")(null)

    // When
    val compiled = compile(expression, slots)

    // Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("late node property access") {
    val n = createNode("prop" -> "hello")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    compile(NodePropertyLate(0, "prop", "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(stringValue("hello"))
    compile(NodePropertyLate(0, "notThere", "notThere")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(NO_VALUE)
  }

  test("relationship property access") {
    // Given
    val r = relate(createNode(), createNode(), "prop" -> "hello")
    val token = tokenReader(_.propertyKey("prop"))
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.getId)
    val expression = RelationshipProperty(0, token, "prop")(null)

    // When
    val compiled = compile(expression, slots)

    // Then
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("late relationship property access") {
    val r = relate(createNode(), createNode(), "prop" -> "hello")
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.getId)

    compile(RelationshipPropertyLate(0, "prop", "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(stringValue("hello"))
    compile(RelationshipPropertyLate(0, "notThere", "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(NO_VALUE)
  }

  test("cached node property access from tx state") {
    //NOTE: we are in an open transaction so everthing we add here will populate the tx state
    val node = createNode("txStateProp" -> "hello from tx state")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    context.setLongAt(0, node.getId)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val expression = ast.CachedNodeProperty(0, tokenReader(_.propertyKey("txStateProp")), cachedPropertyOffset)
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello from tx state"))
  }

  test("cached node property access") {
    //create a node and force it to be properly stored
    val node = createNode("txStateProp" -> "hello from disk")
    startNewTransaction()

    //now we have a stored node that's not in the tx state
    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, node.getId)
    context.setCachedProperty(property, stringValue("hello from cache"))
    val expression = ast.CachedNodeProperty(0, tokenReader(_.propertyKey("txStateProp")), cachedPropertyOffset)
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello from cache"))
  }

  test("cached node property access, when invalidated") {
    //create a node and force it to be properly stored
    val node = createNode("txStateProp" -> "hello from disk")
    startNewTransaction()

    //now we have a stored node that's not in the tx state
    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, node.getId)
    context.setCachedProperty(property, stringValue("hello from cache"))
    //invalidate
    context.invalidateCachedProperties(node.getId)

    val expression = ast.CachedNodeProperty(0, tokenReader(_.propertyKey("txStateProp")), cachedPropertyOffset)
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello from disk"))
  }

  test("late cached node property access from tx state") {
    //NOTE: we are in an open transaction so everthing we add here will populate the tx state
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    val node = createNode("txStateProp" -> "hello from tx state")
    context.setLongAt(0, node.getId)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val expression = CachedNodePropertyLate(0, "txStateProp", cachedPropertyOffset)
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello from tx state"))
  }

  test("late cached node property access") {
    //create a node and force it to be properly stored
    val node = createNode("txStateProp" -> "hello from tx state")
    startNewTransaction()

    //now we have a stored node that's not in the tx state
    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, node.getId)
    context.setCachedProperty(property, stringValue("hello from cache"))
    val expression = CachedNodePropertyLate(0, "txStateProp", cachedPropertyOffset)
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(stringValue("hello from cache"))
  }

  test("cached node property existence") {
    val node = createNode("prop" -> "hello")

    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, node.getId)
    context.setCachedProperty(property, stringValue("hello from cache"))
    val expression = function("exists", ast.CachedNodeProperty(0, tokenReader(_.propertyKey("txStateProp")), cachedPropertyOffset))
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
  }

  test("late cached node property existence") {
    val node = createNode("prop" -> "hello")

    val property = plans.CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val cachedPropertyOffset = slots.newCachedProperty(property).getCachedNodePropertyOffsetFor(property)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, node.getId)
    context.setCachedProperty(property, stringValue("hello from cache"))
    val expression = function("exists",
                              CachedNodePropertyLate(0, "prop", cachedPropertyOffset))
    val compiled = compile(expression, slots)

    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
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

    compile(GetDegreePrimitive(0, None, OUTGOING), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(Values.longValue(3))
    compile(GetDegreePrimitive(0, None, INCOMING), slots).evaluate(context, query, EMPTY_MAP, cursors) should
      equal(Values.longValue(2))
    compile(GetDegreePrimitive(0, None, BOTH), slots).evaluate(context, query, EMPTY_MAP, cursors) should
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

    compile(GetDegreePrimitive(0, Some(relType), OUTGOING), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.longValue(2))
    compile(GetDegreePrimitive(0, Some(relType), INCOMING), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.longValue(1))
    compile(GetDegreePrimitive(0, Some(relType), BOTH), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.longValue(3))
  }

  test("NodePropertyExists") {
    val n = createNode("prop" -> "hello")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)
    val property = tokenReader(_.propertyKey("prop"))
    val nonExistingProperty = 1337

    compile(NodePropertyExists(0, property, "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(NodePropertyExists(0, nonExistingProperty, "otherProp")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("NodePropertyExistsLate") {
    val n = createNode("prop" -> "hello")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    compile(NodePropertyExistsLate(0, "prop", "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(NodePropertyExistsLate(0, "otherProp", "otherProp")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("RelationshipPropertyExists") {
    val r = relate(createNode(), createNode(), "prop" -> "hello")
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.getId)
    val property = tokenReader(_.propertyKey("prop"))
    val nonExistingProperty = 1337

    compile(RelationshipPropertyExists(0, property, "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(RelationshipPropertyExists(0, nonExistingProperty, "otherProp")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  test("RelationshipPropertyExistsLate") {
    val r = relate(createNode(), createNode(), "prop" -> "hello")
    val slots = SlotConfiguration(Map("r" -> LongSlot(0, nullable = true, symbols.CTRelationship)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, r.getId)

    compile(RelationshipPropertyExistsLate(0, "prop", "prop")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(RelationshipPropertyExistsLate(0, "otherProp", "otherProp")(null), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
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
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(n)
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
    compiled.evaluate(context, query, EMPTY_MAP, cursors) should equal(r)
  }

  test("HasLabels") {
    val n = createLabeledNode("L1", "L2")
    val slots = SlotConfiguration(Map("n" -> LongSlot(0, nullable = true, symbols.CTNode)), 1, 0)
    val context = SlottedExecutionContext(slots)
    context.setLongAt(0, n.getId)

    compile(hasLabels(NodeFromSlot(0, "n"), "L1"), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(hasLabels(NodeFromSlot(0, "n"), "L1", "L2"), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(hasLabels(NodeFromSlot(0, "n"), "L1", "L3"), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(hasLabels(NodeFromSlot(0, "n"), "L2", "L3"), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(hasLabels(NodeFromSlot(0, "n"), "L1", "L2", "L3"), slots).evaluate(context, query, EMPTY_MAP, cursors) should equal(Values.FALSE)
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
      properties.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
        override def accept(t: String, u: AnyValue): Unit = {
           node.setProperty(t, u.asInstanceOf[Value].asObject())
        }
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
      properties.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
        override def accept(t: String, u: AnyValue): Unit = {
          r.setProperty(t, u.asInstanceOf[Value].asObject())
        }
      })
      ValueUtils.fromRelationshipProxy(r)
    }
  }

  def compile(e: Expression, slots: SlotConfiguration = SlotConfiguration.empty): CompiledExpression

  def compileProjection(projections: Map[String, Expression],
                        slots: SlotConfiguration = SlotConfiguration.empty): CompiledProjection

  def compileGroupingExpression(projections: Map[String, Expression],
                                slots: SlotConfiguration = SlotConfiguration.empty): CompiledGroupingExpression

  private def coerce(value: AnyValue, ct: CypherType) =
    compile(coerceTo(parameter("a"), ct)).evaluate(ctx, query, map(Array("a"), Array(value)), cursors)

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
      val actual = compile(predicate).evaluate(ctx, query, EMPTY_MAP, cursors)

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
      CodeGeneration.compileExpression(new IntermediateCodeGeneration(slots).compileExpression(e).getOrElse(fail()))

     override def compileProjection(projections: Map[String, Expression], slots: SlotConfiguration = SlotConfiguration.empty): CompiledProjection = {
      val compiler = new IntermediateCodeGeneration(slots)
      val compiled = for ((s,e) <- projections) yield slots(s).offset -> compiler.compileExpression(e).getOrElse(fail(s"failed to compile $e"))
      CodeGeneration.compileProjection(compiler.compileProjection(compiled))
    }

     override def compileGroupingExpression(projections: Map[String, Expression], slots: SlotConfiguration = SlotConfiguration.empty): CompiledGroupingExpression = {
      val compiler = new IntermediateCodeGeneration(slots)
      val compiled = for ((s,e) <- projections) yield slots(s) -> compiler.compileExpression(e).getOrElse(fail(s"failed to compile $e"))
      CodeGeneration.compileGroupingExpression(compiler.compileGroupingExpression(compiled))
    }
}

class InterpretedExpressionIT extends ExpressionsIT {
  override  def compile(e: Expression,
                                slots: SlotConfiguration): CompiledExpression = {
    val expression = converter(slots, (converter, id) => converter.toCommandExpression(id, e))
    new CompiledExpression() {
      override def evaluate(context: ExecutionContext,
                            dbAccess: DbAccess,
                            params: MapValue,
                            cursors: ExpressionCursors): AnyValue = expression(context, state(dbAccess, params, cursors))

    }
  }

  override  def compileProjection(projections: Map[String, Expression],
                                          slots: SlotConfiguration): CompiledProjection = {
    val projector = converter(slots, (converter, id) => converter.toCommandProjection(id, projections))
    new CompiledProjection {
      override def project(context: ExecutionContext,
                           dbAccess: DbAccess,
                           params: MapValue,
                           cursors: ExpressionCursors): Unit = projector.project(context, state(dbAccess, params, cursors))

    }
  }

  override  def compileGroupingExpression(projections: Map[String, Expression],
                                                  slots: SlotConfiguration): CompiledGroupingExpression = {
    val grouping = converter(slots, (converter, id) => converter.toGroupingExpression(id, projections))
    new CompiledGroupingExpression {

      override def projectGroupingKey(context: ExecutionContext,
                                      groupingKey: AnyValue): Unit = grouping.project(context, groupingKey.asInstanceOf[grouping.KeyType])

      override def computeGroupingKey(context: ExecutionContext,
                                      dbAccess: DbAccess,
                                      params: MapValue,
                                      cursors: ExpressionCursors): AnyValue =
        grouping.computeGroupingKey(context, state(dbAccess, params, cursors))


      override def getGroupingKey(context: ExecutionContext): AnyValue = grouping.getGroupingKey(context)
    }
  }


  private def state(dbAccess: DbAccess, params: MapValue, cursors: ExpressionCursors) = new QueryState(dbAccess
                                         .asInstanceOf[QueryContext],
                                       null,
                                       params,
                                       cursors,
                                       Array.empty)

  private def converter[T](slots: SlotConfiguration, producer: (ExpressionConverters, Id) => T): T = {
    val plan = PhysicalPlan(new SlotConfigurations, new ArgumentSizes)
    val id = Id(0)
    plan.slotConfigurations.set(id, slots)
    val converters = new ExpressionConverters(SlottedExpressionConverters(plan),
                                             CommunityExpressionConverter(query))
    producer(converters, id)
  }
}
