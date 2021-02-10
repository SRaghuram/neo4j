/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util

import org.neo4j.codegen.api.AssignToLocalVariable
import org.neo4j.codegen.api.Block
import org.neo4j.codegen.api.Constant
import org.neo4j.codegen.api.DeclareLocalVariable
import org.neo4j.codegen.api.Eq
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.falseValue
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.trueValue
import org.neo4j.codegen.api.IntermediateRepresentation.tryCatch
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.PrettyIR
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.functions.Sin
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty
import org.neo4j.cypher.internal.physicalplanning.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.compiled.expressions.AbstractExpressionCompilerFront.NODE_PROPERTY
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.NODE_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.PROPERTY_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.loadExpressionVariable
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.setExpressionVariable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.operations.CypherBoolean
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.cypher.operations.CypherMath
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

class CodeChainExpressionCompilerTest extends CypherFunSuite with AstConstructionTestSupport {
  private val context = load[ReadableRow]("row")
  private val declareReturnValue = declareAndAssign("retVal", noValue)
  private val loadReturnValue = load[AnyValue]("retVal")
  private val id = Id.INVALID_ID

  private def assignReturnValue(value: IntermediateRepresentation) = assign("retVal", value)

  val namer: VariableNamer = new VariableNamer {
    override def nextVariableName(suffix: String): String = suffix
  }
  private val tokenContext = TokenContext.EMPTY

  test("literal") {
    val slots = SlotConfiguration.empty
      .newReference("nullable", nullable = true, CTAny)
      .newReference("nonNullable", nullable = false, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val expr = literalFloat(1.28)

    assertCompilesTo(testCompiler, expr, slots, getStatic[DoubleValue]("DOUBLELIT"))
  }

  test("node") {
    val slots = SlotConfiguration.empty
      .newReference("nullable", nullable = true, CTAny)
      .newReference("nonNullable", nullable = false, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, new VariableNamer, tokenContext)
    val offset = 1
    val expr = refFromSlot(offset, slots)

    assertCompilesTo(testCompiler, expr, slots, getRefAt(offset))
  }

  /**
   * property:
   *
   * return dbAccess.nodeProperty(row.getRefAt(offset), propertyToken, NODE_CURSOR, PROPERTY_CURSOR, true)
   */
  test("property - nullable") {
    // setup
    val nodeName = "nullable"
    val slots = SlotConfiguration.empty
      .newLong(nodeName, nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val propToken = 1

    val offset = slots.getLongOffsetFor(nodeName)

    val expectedResult = getNodeProperty(getLongAt(offset), offset, propToken)
    val property = nodeProperty(propToken, offset, slots)
    assertCompilesTo(testCompiler, property, slots, expectedResult)
  }

  /**
   * val sinInput = row.getRefAt(offset)
   * return sin(load(sinInput))
   */
  test("sin(x) - non nullable") {
    val x = "nonNullable"
    val slots = SlotConfiguration.empty
      .newReference(x, nullable = false, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)

    val offset = slots.getReferenceOffsetFor(x)
    val ref = getRefAt(offset)
    val assignedX = declareAndAssign("sinInput", ref)
    val expectedResult = block(
      assignedX,
      invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), load[AnyValue]("sinInput"))
    )
    val expression = FunctionInvocation(FunctionName(Sin.name)(InputPosition.NONE), refFromSlot(offset, slots))(InputPosition.NONE)

    assertCompilesTo(testCompiler, expression, slots, expectedResult)
  }

  /**
   * val sinInput = row.getRefAt(offset)
   * return sinInput == null ? null : sin(sinInput)
   */
  test("sin(x) - nullable") {
    val x = "nullable"
    val slots = SlotConfiguration.empty
      .newReference(x, nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)

    val offset = slots.getReferenceOffsetFor(x)
    val ref = getRefAt(offset)
    val assignedX = declareAndAssign("sinInput", ref)
    val expectedResult = block(
      assignedX,
      ternary(
        Eq(load[AnyValue]("sinInput"), noValue),
        noValue,
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), load[AnyValue]("sinInput"))
      )
    )
    val expression = FunctionInvocation(FunctionName(Sin.name)(InputPosition.NONE), refFromSlot(offset, slots))(InputPosition.NONE)

    assertCompilesTo(testCompiler, expression, slots, expectedResult)
  }

  test("cos(sin(x)) - nullable") {
    val x = "nullable"
    val slots = SlotConfiguration.empty
      .newReference(x, nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val offset = slots.getReferenceOffsetFor(x)

    // AST
    val sinExpression = call("sin", refFromSlot(offset, slots))
    val cosExpression = call("cos", sinExpression)

    // expected IR
    val ref = getRefAt(offset)
    val sinInput = declareAndAssign("sinInput", ref)
    val expectedResult = block(
      sinInput,
      declareAndAssign("retVal", noValue),
      condition(IntermediateRepresentation.not(Eq(load[AnyValue]("sinInput"), noValue)))
        (assignReturnValue(block(
        declareAndAssign(
          typeRefOf[AnyValue],
          "cosInput",
          invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), load[AnyValue]("sinInput"))
        ),
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), load[AnyValue]("cosInput"))
        ))),
      loadReturnValue
    )

    // then
    assertCompilesTo(testCompiler, cosExpression, slots, expectedResult)
  }

  /*
  val x = load("x")
  val retVal = null
  if (!x==null) retVal = {
    val y = load("y")
    y == null ? null : x + y
  }
  retVal
   */
  test("x + y - both nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = true, CTAny)
      .newReference("y", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = add(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("addLhs", getRefAt(xOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
        (assignReturnValue(block(
          declareAndAssign("addRhs", getRefAt(yOffset)),
          ternary(Eq(load[AnyValue]("addRhs"), noValue), noValue,
            invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))
          )
          ))),
        loadReturnValue
      )
    )
  }

  /*
  val x = load("x")
  val retVal = null
  if (!x==null) retVal = {
    val y = load("y")
    x + y
  }
  retVal
 */
  test("x + y if x nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = true, CTAny)
      .newReference("y", nullable = false, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = add(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("addLhs", getRefAt(xOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
        (assignReturnValue(block(
          declareAndAssign("addRhs", getRefAt(yOffset)),
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))
          ))),
        loadReturnValue
      )
    )
  }

  /*
  val x = load("y")
  val retVal = null
  if (!y==null) retVal = {
    val x = load("x")
    x + y
  }
  retVal
  */
  test("x + y if y nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = false, CTAny)
      .newReference("y", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = add(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("addRhs", getRefAt(yOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addRhs"), noValue)))
        (assignReturnValue(block(
          declareAndAssign("addLhs", getRefAt(xOffset)),
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))
          ))),
        loadReturnValue
      )
    )
  }

  /*
   val x = load("y")
   val x = load("x")
   x + y
 */
  test("x + y if neither x or y nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = false, CTAny)
      .newReference("y", nullable = false, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = add(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("addRhs", getRefAt(yOffset)),
        declareAndAssign("addLhs", getRefAt(xOffset)),
        invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))
      )
    )
  }

  /*
   val x = load("x")
   val retVal1 = null
  if (!x==null) retVal1 = {
    val y = load("y")
    val retVal2 = null
    if(!y==null) retVal2 = else {
      val cosInput = x + y
      val sinInput = cos(cosInput)
      sin(sinInput)
    }
    retVal2
  }
  retVal1
   */
  test("sin(cos(x + y)) - both nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = true, CTAny)
      .newReference("y", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = call("sin", call("cos", add(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("addLhs", getRefAt(xOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
        (assignReturnValue(block(
          declareAndAssign("addRhs", getRefAt(yOffset)),
          declareReturnValue,
          condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addRhs"), noValue)))
          (assignReturnValue(block(
            declareAndAssign(typeRefOf[AnyValue], "cosInput", invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))),
            declareAndAssign(typeRefOf[AnyValue], "sinInput", invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), load[AnyValue]("cosInput"))),
            invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), load[AnyValue]("sinInput"))
            ))),
          loadReturnValue
        ))),
        loadReturnValue
      )
    )
  }

  /*
  val x = load("x")
  val retVal1 = null
  if (!x==null) retVal1 = {
    val y = load("y")
    val retVal2 = null
    if(!y == null) retVal2 = {
      val z = load("z")
      val retVal3 = null
      if(!z==null) retValr = {
        val subRhs = sin(z)
        val subLhs = x + y
        subLhs - subRhs
      }
      retVal3
    }
    retVal2
  }
  retVal1
   */
  test("(x+y)-(sin(z)) - all nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = true, CTAny)
      .newReference("y", nullable = true, CTAny)
      .newReference("z", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")
    val zOffset = slots.getReferenceOffsetFor("z")

    // AST
    val ast = subtract(
      add(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots)),
      call("sin", refFromSlot(zOffset, slots)))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("addLhs", getRefAt(xOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
        (assignReturnValue(block(
          declareAndAssign("addRhs", getRefAt(yOffset)),
          declareReturnValue,
          condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addRhs"), noValue)))
          (assignReturnValue(block(
            declareAndAssign("sinInput", getRefAt(zOffset)),
            declareReturnValue,
            condition(IntermediateRepresentation.not(Eq(load[AnyValue]("sinInput"), noValue)))
            (assignReturnValue(block(
              declareAndAssign(typeRefOf[AnyValue],"subRhs", invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), load[AnyValue]("sinInput"))),
              declareAndAssign(typeRefOf[AnyValue], "subLhs", invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))),
              invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), load[AnyValue]("subLhs"), load[AnyValue]("subRhs"))
            )
            )
          ),
            loadReturnValue
          ))),
          loadReturnValue
        ))),
        loadReturnValue
      )
    )
  }

  /*
  val addLhs = n.prop1
  val retVal1 = null
  if(!addLhs==null) retVal1 = else {
    val addRhs = n.prop2
    val retVal2 = null
    if(!addRhs==null) retVal2 = {
      val subRhs = n.prop3
      val retVal3 = null
      if(!subRhs==null) retVal3 = {
         val subLhs = addLhs + addRhs
         subLhs - subRhs
      }
      retVal3
    }
    retVal2
  }
  retVal1
   */
  test("(n.prop1+n.prop2)-n.prop3 - n nullable") {
    val slots = SlotConfiguration.empty
      .newLong("n", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val nOffset = slots.getLongOffsetFor("n")

    // AST
    val ast =
      subtract(
        add(nodeProperty(10, nOffset, slots), nodeProperty(11, nOffset, slots)),
        nodeProperty(12, nOffset, slots)
      )

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign(typeRefOf[AnyValue],"addLhs", getNodeProperty(getLongAt(nOffset), nOffset, 10)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
        (assignReturnValue(block(
          declareAndAssign(typeRefOf[AnyValue],"addRhs", getNodeProperty(getLongAt(nOffset), nOffset, 11)),
          declareReturnValue,
          condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addRhs"), noValue)))
          (assignReturnValue(block(
            declareAndAssign(typeRefOf[AnyValue],"subRhs", getNodeProperty(getLongAt(nOffset), nOffset, 12)),
            declareReturnValue,
            condition(IntermediateRepresentation.not(Eq(load[AnyValue]("subRhs"), noValue)))
            (assignReturnValue(block(
              declareAndAssign(typeRefOf[AnyValue],"subLhs", invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs"))),
              invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), load[AnyValue]("subLhs"), load[AnyValue]("subRhs"))
            ))),
            loadReturnValue
          ))),
          loadReturnValue
        ))),
        loadReturnValue
      )
    )
  }

  /*
   val isNotNullInput = {
     val n = load("n")
     (n==null) ? null : n.prop1
   }
   isNotNullInput != null
  */
  test("(n.prop1) IS NOT NULL - n nullable") {
    val slots = SlotConfiguration.empty
      .newLong("n", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val nOffset = slots.getLongOffsetFor("n")

    // AST
    val ast = isNotNull(nodeProperty(8, nOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign(typeRefOf[AnyValue],"isNotNullInput",
          getNodeProperty(getLongAt(nOffset), nOffset, 8)
        ),
        ternary(notEqual("isNotNullInput", noValue), trueValue, falseValue)
      )
    )
  }

  /**
   *
   * Expression: "x" or "y" both nullable
   *
   *{
   *  orReturn = noValue
   *  orError = null
   *  seenNull = false
   *  orLhs = "x"
   *  if (orLhs == null) seenNull = true else {
   *    try {
   *      orReturn = coerceToPredicate(orLhs)
   *    } catch (e: exception) {
   *      error = e
   *    }
   *  }
   *
   *  if( orResult != trueValue) {
   *    orRhs = "y"
   *    if (orRhs == null) seenNull = true else {
   *     try {
   *       orReturn = coerceToPredicate(orRhs)
   *     } catch (e) {
   *       error = e;
   *     }
   *   }
   * }
   *
   * if(orResult != trueValue && error != null) {
   *    throw error
   * }
   *
   * orResult == trueValue ? trueValue : (seenNull ? noValue : falseValue)
   */
  test("or(x, y) - both nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = true, CTAny)
      .newReference("y", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = or(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        // declare all temporary variables
        declareAndAssign("orReturn", noValue),
        declareAndAssign(typeRefOf[RuntimeException], "orError", constant(null)),
        declareAndAssign(typeRefOf[Boolean], "seenNull", constant(false)),
        declareAndAssign(typeRefOf[AnyValue],"orLhs", getRefAt(xOffset)),

        // check lhs
        ifElse(Eq(load[AnyValue]("orLhs"), noValue))
        (block(assign("seenNull", constant(true))))
        (block(
          tryCatch[RuntimeException]("orException1")
            (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orLhs"))))
            (assign("orError", load[RuntimeException]("orException1")))
        )),

        // only evaluate rhs if lhs isn't trueValue
        condition(notEqual("orReturn", trueValue))(block(
          declareAndAssign(typeRefOf[AnyValue],"orRhs", getRefAt(yOffset)),
          ifElse(Eq(load[AnyValue]("orRhs"), noValue))
          (block(assign("seenNull", constant(true))))
          (block(
            tryCatch[RuntimeException]("orException2")
              (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orRhs"))))
              (assign("orError", load[RuntimeException]("orException2")))
          ))
        )),

        // if no side is trueValue and there was a runtime error -> throw error
        condition(IntermediateRepresentation.and(
          notEqual("orReturn", trueValue),
          notEqual(load[RuntimeException]("orError"), constant(null))
        ))
        (block(IntermediateRepresentation.fail(load[RuntimeException]("orError")))),

        ternary(IntermediateRepresentation.equal("orReturn", trueValue), trueValue, ternary(load[Boolean]("seenNull"), noValue, falseValue))
      )
    )
  }

  /**
   *
   * Expression: "x.prop1" or "x.prop1" both nullable
   *
   *{
   *  orReturn = noValue
   *  orError = null
   *  seenNull = false
   *  orLhs = "x"
   *  if (orLhs == null) seenNull = true else {
   *    try {
   *      orReturn = coerseToPredicate(orLhs)
   *    } catch (e: exception) {
   *      error = e
   *    }
   *  }
   *
   *  if( orResult != trueValue) {
   *    orRhs = "y"
   *    if (orRhs == null) seenNull = true else {
   *     try {
   *       orReturn = coerseToPredicate(orRhs)
   *     } catch (e) {
   *       error = e;
   *     }
   *   }
   * }
   *
   * if(orResult != trueValue && error != null) {
   *    throw error
   * }
   *
   * orResult == trueValue ? trueValue : (seenNull ? noValue : falseValue)
   */
  test("or(x.prop, x.prop) - both nullable") {
    val slots = SlotConfiguration.empty
      .newLong("x", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getLongOffsetFor("x")

    // AST
    val ast = or(nodeProperty(0, xOffset, slots), nodeProperty(0, xOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        // declare all temporary variables
        declareAndAssign("orReturn", noValue),
        declareAndAssign(typeRefOf[RuntimeException], "orError", constant(null)),
        declareAndAssign(typeRefOf[Boolean], "seenNull", constant(false)),
        declareAndAssign("orLhs", getNodeProperty(getLongAt(xOffset), xOffset, 0)),

        // check lhs
        ifElse(Eq(load[AnyValue]("orLhs"), noValue))
        (block(assign("seenNull", constant(true))))
        (block(
          tryCatch[RuntimeException]("orException1")
            (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orLhs"))))
            (assign("orError", load[RuntimeException]("orException1")))
        )),

        // only evaluate rhs if lhs isn't trueValue
        condition(notEqual("orReturn", trueValue))(block(
          declareAndAssign("orRhs", getNodeProperty(getLongAt(xOffset), xOffset, 0)),
          ifElse(Eq(load[AnyValue]("orRhs"), noValue))
          (block(assign("seenNull", constant(true))))
          (block(
            tryCatch[RuntimeException]("orException2")
              (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orRhs"))))
              (assign("orError", load[RuntimeException]("orException2")))
          ))
        )),

        // if no side is trueValue and there was a runtime error -> throw error
        condition(IntermediateRepresentation.and(
          notEqual("orReturn", trueValue),
          notEqual(load[RuntimeException]("orError"), constant(null))
        ))
        (block(IntermediateRepresentation.fail(load[RuntimeException]("orError")))),

        ternary(IntermediateRepresentation.equal("orReturn", trueValue), trueValue, ternary(load[Boolean]("seenNull"), noValue, falseValue))
      )
    )
  }

  /*
   *
   * Expression: "x" or "y" non nullable
   *
   *{
   *  orReturn = noValue
   *  orError = null
   *  seenNull = false
   *  orLhs = "x"
   *  try {
   *    orReturn = coerseToPredicate(orLhs)
   *  } catch (e: exception) {
   *    error = e
   *  }
   *
   *  if( orResult != trueValue) {
   *    orRhs = "y"
   *    try {
   *      orReturn = coerseToPredicate(orRhs)
   *    } catch (e) {
   *      error = e;
   *    }
   * }
   *
   * if(orResult != trueValue && error != null) {
   *    throw error
   * }
   *
   * orResult == trueValue ? trueValue : (seenNull ? noValue : falseValue)
   */
  test("or(x, y) - non nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = false, CTAny)
      .newReference("y", nullable = false, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = or(refFromSlot(xOffset, slots), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        // declare all temporary variables
        declareAndAssign("orReturn", noValue),
        declareAndAssign(typeRefOf[RuntimeException], "orError", constant(null)),
        declareAndAssign(typeRefOf[Boolean], "seenNull", constant(false)),
        declareAndAssign("orLhs", getRefAt(xOffset)),

        // check lhs
        tryCatch[RuntimeException]("orException1")
          (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orLhs"))))
          (assign("orError", load[RuntimeException]("orException1"))),

        // only evaluate rhs if lhs isn't trueValue
        condition(notEqual("orReturn", trueValue))(block(
          declareAndAssign("orRhs", getRefAt(yOffset)),
            tryCatch[RuntimeException]("orException2")
              (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orRhs"))))
              (assign("orError", load[RuntimeException]("orException2")))
        )),

        // if no side is trueValue and there was a runtime error -> throw error
        condition(IntermediateRepresentation.and(
          notEqual("orReturn", trueValue),
          notEqual(load[RuntimeException]("orError"), constant(null))
        ))
        (block(IntermediateRepresentation.fail(load[RuntimeException]("orError")))),

        ternary(IntermediateRepresentation.equal("orReturn", trueValue), trueValue, ternary(load[Boolean]("seenNull"), noValue, falseValue))
      )
    )
  }

  /*
   *{
   *  orReturn = noValue
   *  orError = null
   *  seenNull = false
   *  orLhs = dbAccess.nodeProperty(row.getLongAt(0), 4, nodeCursor, propertyCursor, true)
   *  if (orLhs == null) seenNull = true else {
   *    try {
   *      orReturn = coerseToPredicate(orLhs)
   *    } catch (e: exception) {
   *      error = e
   *    }
   *  }
   *
   *  if( orResult != trueValue) {
   *    orRhs = dbAccess.nodeProperty(row.getLongAt(0), 3, nodeCursor, propertyCursor, true)
   *    if (orRhs == null) seenNull = true else {
   *     try {
   *       orReturn = coerseToPredicate(orRhs)
   *     } catch (e) {
   *       error = e;
   *     }
   *   }
   * }
   *
   * if(orResult != trueValue && error != null) {
   *    throw error
   * }
   *
   * orResult == trueValue ? trueValue : (seenNull ? noValue : falseValue)
   */

  test("or(n.prop1, n.prop2) - both nullable") {
    val slots = SlotConfiguration.empty
      .newLong("n", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val nOffset = slots.getLongOffsetFor("n")

    // AST
    val ast = or(nodeProperty(4, nOffset, slots), nodeProperty(3, nOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        // declare all temporary variables
        declareAndAssign("orReturn", noValue),
        declareAndAssign(typeRefOf[RuntimeException], "orError", constant(null)),
        declareAndAssign(typeRefOf[Boolean], "seenNull", constant(false)),
        declareAndAssign("orLhs", getNodeProperty(getLongAt(nOffset), nOffset, 4)),

        // check lhs
        ifElse(Eq(load[AnyValue]("orLhs"), noValue))
        (block(assign("seenNull", constant(true))))
        (block(
          tryCatch[RuntimeException]("orException1")
            (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orLhs"))))
            (assign("orError", load[RuntimeException]("orException1")))
        )),

        // only evaluate rhs if lhs isn't trueValue
        condition(notEqual("orReturn", trueValue))(block(
          declareAndAssign("orRhs", getNodeProperty(getLongAt(nOffset), nOffset, 3)),
          ifElse(Eq(load[AnyValue]("orRhs"), noValue))
          (block(assign("seenNull", constant(true))))
          (block(
            tryCatch[RuntimeException]("orException2")
              (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orRhs"))))
              (assign("orError", load[RuntimeException]("orException2")))
          ))
        )),

        // if no side is trueValue and there was a runtime error -> throw error
        condition(IntermediateRepresentation.and(
          notEqual("orReturn", trueValue),
          notEqual(load[RuntimeException]("orError"), constant(null))
        ))
        (block(IntermediateRepresentation.fail(load[RuntimeException]("orError")))),

        ternary(IntermediateRepresentation.equal("orReturn", trueValue), trueValue, ternary(load[Boolean]("seenNull"), noValue, falseValue))
      )
    )
  }

  /*
   *
   * note: IS NOT NULL is a predicate
   *
   * {
   *  orReturn = noValue
   *  orError = null
   *  seenNull = false
   *  orLhs = {
   *    val isNotNullInput = load("x")
   *    isNotNullInput != null
   *  }
   *  orReturn = orLhs
   *
   *  if( orResult != trueValue) {
   *    orRhs = dbAccess.nodeProperty(row.getLongAt(0), 3, nodeCursor, propertyCursor, true)
   *    if (orRhs == null) seenNull = true else {
   *     try {
   *       orReturn = coerseToPredicate(orRhs)
   *     } catch (e) {
   *       error = e;
   *     }
   *   }
   * }
   *
   * if(orResult != trueValue && error != null) {
   *    throw error
   * }
   *
   * orResult == trueValue ? trueValue : (seenNull ? noValue : falseValue)
   */

  test("or((x) IS NOT NULL, y) - both nullable") {
    val slots = SlotConfiguration.empty
      .newReference("x", nullable = true, CTAny)
      .newReference("y", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val xOffset = slots.getReferenceOffsetFor("x")
    val yOffset = slots.getReferenceOffsetFor("y")

    // AST
    val ast = or(isNotNull(refFromSlot(xOffset, slots)), refFromSlot(yOffset, slots))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        // declare all temporary variables
        declareAndAssign("orReturn", noValue),
        declareAndAssign(typeRefOf[RuntimeException], "orError", constant(null)),
        declareAndAssign(typeRefOf[Boolean], "seenNull", constant(false)),
        declareAndAssign("orLhs", block(
          declareAndAssign("isNotNullInput", getRefAt(xOffset)),
            ternary(notEqual("isNotNullInput", noValue), trueValue, falseValue)
          ),
        ),

        // no need to check lhs - not nullable and it is a predicate
        assign("orReturn", load[AnyValue]("orLhs")),

        // only evaluate rhs if lhs isn't trueValue
        condition(notEqual("orReturn", trueValue))(block(
          declareAndAssign("orRhs", getRefAt(yOffset)),
          ifElse(Eq(load[AnyValue]("orRhs"), noValue))
          (block(assign("seenNull", constant(true))))
          (block(
            tryCatch[RuntimeException]("orException2")
              (assign("orReturn", invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), load[AnyValue]("orRhs"))))
              (assign("orError", load[RuntimeException]("orException2")))
          ))
        )),

        // if no side is trueValue and there was a runtime error -> throw error
        condition(IntermediateRepresentation.and(
          notEqual("orReturn", trueValue),
          notEqual(load[RuntimeException]("orError"), constant(null))
        ))
        (block(IntermediateRepresentation.fail(load[RuntimeException]("orError")))),

        ternary(IntermediateRepresentation.equal("orReturn", trueValue), trueValue, ternary(load[Boolean]("seenNull"), noValue, falseValue))
      )
    )
  }

  /*
  val as = load("as")
  val retVal1 = null
  if (!as==null) retVal1 = {
    val list = (ListValue) as
    val heapUsage = 0
    ArrayList<AnyValue> extracted = new ArrayList();
    for ( AnyValue currentValue : list ) {
      expressionVariables[innerVarOffset] = currentValue;
      val result = {
        val addLhs = expressionVariables[innerVarOffset]
        val retVal2 = null
        if (!addLhs==null) retVal2 = {
          val addRhs = 42
          result = addLhs + addRhs
        }
        retVal2
      }
      heapUsage = result == NO_VALUE ? heapUsage : heapUsage + result.heapUsage
      extracted.add(result)
    }

    VirtualValues.fromList(extracted)
  }
  retVal1
   */
  test("[ a IN as | a + 42 ] - as nullable") {
    val slots = SlotConfiguration.empty
      .newReference("as", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val asOffset = slots.getReferenceOffsetFor("as")

    val exprVariable = ExpressionVariable(3, "a")
    // AST
    val ast = listComprehension(exprVariable, refFromSlot(asOffset, slots), None, Some(add(exprVariable, literalInt(42))))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("maybeList", getRefAt(asOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("maybeList"), noValue)))
        (assignReturnValue(block(
          declareAndAssign(
            typeRefOf[ListValue],
            "list",
            invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), load[AnyValue]("maybeList"))),
          declareAndAssign(typeRefOf[util.ArrayList[AnyValue]], "extracted", newInstance(constructor[java.util.ArrayList[AnyValue]])),
          declareAndAssign("heapUsage", constant(0L)),
          declareAndAssign(typeRefOf[java.util.Iterator[AnyValue]], "iter", invoke(load[ListValue]("list"), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(invoke(load[java.util.Iterator[AnyValue]]("iter"), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))(
            block(
              setExpressionVariable(exprVariable, invoke(load[java.util.Iterator[AnyValue]]("iter"), method[java.util.Iterator[AnyValue], Object]("next"))),
              declareAndAssign(
                typeRefOf[AnyValue],
                "result", block(
                  declareAndAssign(typeRefOf[AnyValue],"addLhs", loadExpressionVariable(exprVariable)),
                  declareReturnValue,
                  condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
                  (assignReturnValue(block(
                    declareAndAssign(typeRefOf[AnyValue],"addRhs", getStatic[LongValue]("INTLIT")),
                    invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs")),
                  ))),
                  loadReturnValue
                )
              ),
              assign("heapUsage",
                ternary(
                  Eq(load[AnyValue]("result"), noValue),
                  load[Long]("heapUsage"),
                  IntermediateRepresentation.add(
                    load[Long]("heapUsage"),
                    invoke(load[AnyValue]("result"), method[AnyValue, Long]("estimatedHeapUsage"))
                  )
                )),
              invokeSideEffect(load[util.ArrayList[AnyValue]]("extracted"), method[java.util.ArrayList[_], Boolean, Object]("add"), load[AnyValue]("result"))
            )
          ),
          invokeStatic(method[VirtualValues, ListValue, java.util.List[AnyValue], Long]("fromList"), load[util.ArrayList[AnyValue]]("extracted"), load[Long]("heapUsage")))
        )),
        loadReturnValue
      )
    )
  }

  /*
val maybeList = load("as")
val retVal1 = null
if (!maybeList==null) retVal1 = {
  val list = (ListValue) maybeList
  expressionVariables[accVarOffset] = init;
  for ( AnyValue currentValue : list ) {
    expressionVariables[innerVarOffset] = currentValue
    expressionVariables[accVarOffset] = {
      val result = {
        val addLhs = expressionVariables[accVarOffset]
        val retVal2 = null
        if (!addLhs==null) retVal2 = {
          val addRhs = expressionVariables[currentValue]
          (addRhs == null) ? null : addLhs + addRhs
        }
        retVal2
      }
    }
  }
  expressionVariables[accVarOffset]
}
retVal1
 */
  test("reduce(as, sum, 0) - as nullable") {
    val slots = SlotConfiguration.empty
      .newReference("as", nullable = true, CTAny)
    val testCompiler = new CodeChainExpressionCompiler(slots, namer, tokenContext)
    val asOffset = slots.getReferenceOffsetFor("as")

    val accumulatorVariable = ExpressionVariable(2, "a")
    val reduceVariable = ExpressionVariable(3, "a")
    // AST
    val ast = reduce(accumulatorVariable, literalInt(0), reduceVariable, refFromSlot(asOffset, slots), add(accumulatorVariable, reduceVariable))

    // then
    assertCompilesTo(testCompiler, ast, slots,
      block(
        declareAndAssign("maybeList", getRefAt(asOffset)),
        declareReturnValue,
        condition(IntermediateRepresentation.not(Eq(load[AnyValue]("maybeList"), noValue)))
        (assignReturnValue(block(
          declareAndAssign(
            typeRefOf[ListValue],
            "list",
            invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), load[AnyValue]("maybeList"))),
          setExpressionVariable(accumulatorVariable, getStatic[LongValue]("INTLIT")),
          declareAndAssign(typeRefOf[java.util.Iterator[AnyValue]], "iter", invoke(load[ListValue]("list"), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(invoke(load[java.util.Iterator[AnyValue]]("iter"), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))(
            block(
              setExpressionVariable(reduceVariable, invoke(load[java.util.Iterator[AnyValue]]("iter"), method[java.util.Iterator[AnyValue], Object]("next"))),
              setExpressionVariable(accumulatorVariable, block(
                declareAndAssign(typeRefOf[AnyValue], "addLhs", loadExpressionVariable(accumulatorVariable)),
                declareReturnValue,
                condition(IntermediateRepresentation.not(Eq(load[AnyValue]("addLhs"), noValue)))
                (assignReturnValue(block(
                  declareAndAssign(typeRefOf[AnyValue], "addRhs", loadExpressionVariable(reduceVariable)),
                  ternary(
                    Eq(load[AnyValue]("addRhs"), noValue),
                    noValue,
                    invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), load[AnyValue]("addLhs"), load[AnyValue]("addRhs")))
                ))),
                loadReturnValue
              ))
            )
          ),
          declareAndAssign(typeRefOf[AnyValue], "result", loadExpressionVariable(accumulatorVariable)),
          load[AnyValue]("result")
        ))
        ),
        loadReturnValue
      )
    )
  }

  test("commonPrefix - empty prefix") {
    val assignedX = declareAndAssign("x", getRefAt(0))
    val assignedY = declareAndAssign("y", getRefAt(1))
    val a = CodeLink(Seq(assignedX), isNullable = true, Set.empty, Set.empty)(NullCheckLink(IntermediateRepresentation.equal(assignedX, assignedY))(START))
    val b = CodeLink(Seq(assignedY), isNullable = false, Set.empty, Set.empty)(START)
    val (pre, aSuf, bSuf) = CodeChainExpressionCompiler.extractCommonPrefix(a, b)
    pre.deepEq(START) shouldBe true
    aSuf.deepEq(a) shouldBe true
    bSuf.deepEq(b) shouldBe true
  }

  test("commonPrefix - non empty prefix") {
    val assignedX = declareAndAssign("x", getRefAt(0))
    val assignedY = declareAndAssign("y", getRefAt(1))
    val prefix = CodeLink(Seq(assignedX), isNullable = true, Set.empty, Set.empty)(START)
    val a = CodeLink(Seq(assignedX), isNullable = true, Set.empty, Set.empty)(_)
    val b = CodeLink(Seq(assignedY), isNullable = true, Set.empty, Set.empty)(_)
    val (pre, aSuf, bSuf) = CodeChainExpressionCompiler.extractCommonPrefix(a(prefix), b(prefix))
    assert(pre.deepEq(prefix))
    assert(aSuf.deepEq(a(START)))
    assert(bSuf.deepEq(b(START)))
  }

  test("commonPrefix - subExpression") {
    val assignedX = declareAndAssign("x", getRefAt(0))
    val prefix = CodeLink(Seq(assignedX), isNullable = true, Set.empty, Set.empty)(START)
    val a = CodeLink(Seq(assignedX), isNullable = true, Set.empty, Set.empty)(_)

    val (pre1, aSuf1, bSuf1) = CodeChainExpressionCompiler.extractCommonPrefix(a(prefix), prefix)
    assert(pre1.deepEq(prefix))
    assert(aSuf1.deepEq(a(START)))
    assert(bSuf1.deepEq(START))

    val (pre2, aSuf2, bSuf2) = CodeChainExpressionCompiler.extractCommonPrefix(prefix, a(prefix))
    assert(pre2.deepEq(prefix))
    assert(aSuf2.deepEq(START))
    assert(bSuf2.deepEq(a(START)))
  }

  test("commonPrefix - equal expressions") {
    val assignedX = declareAndAssign("x", getRefAt(0))
    val prefix = CodeLink(Seq(assignedX), isNullable = true, Set.empty, Set.empty)(START)
    val (pre2, aSuf2, bSuf2) = CodeChainExpressionCompiler.extractCommonPrefix(prefix, prefix)
    assert(pre2.deepEq(prefix))
    assert(aSuf2.deepEq(START))
    assert(bSuf2.deepEq(START))
  }

  private def call(functionName: String, arg: Expression) = {
    FunctionInvocation(FunctionName(functionName)(InputPosition.NONE), arg)(InputPosition.NONE)
  }

  private def refFromSlot(offset: Int, slots: SlotConfiguration): Expression = {
    ReferenceFromSlot(offset, slots.nameOfSlot(offset, longSlot = false).get)
  }

  private def nodeFromSlot(offset: Int, slots: SlotConfiguration): Expression = {
    NodeFromSlot(offset, slots.nameOfSlot(offset, longSlot = true).get)
  }

  private def getRefAt(offset: Int) =
    invoke(context, method[ReadableRow, AnyValue, Int]("getRefAt"), constant(offset))

  private def getLongAt(offset: Int) =
    invoke(context, method[ReadableRow, Long, Int]("getLongAt"), constant(offset))

  private def getNodeProperty(nodeId: IntermediateRepresentation, offset: Int, propertyToken: Int) =
    invoke(DB_ACCESS, NODE_PROPERTY, nodeId, Constant(propertyToken), NODE_CURSOR, PROPERTY_CURSOR, constant(true))

  private def nodeProperty(propToken: Int, offset: Int, slots: SlotConfiguration) = {
    NodeProperty(offset, propToken, "prop")(prop(nodeFromSlot(offset, slots), "prop"))
  }

  /**
   * If nested blocks, this function flattens out the block.
   *
   * Example:
   * {
   *   val a = "a"
   *   {
   *     val b = "b"
   *   }
   * }
   *
   *   |
   *   v
   *
   * {
   *   val a = "a"
   *   val b = "b"
   * }
   *
  */
  def flattenBlocks: Rewriter =
    bottomUp(Rewriter.lift {
      case Block(ops) if ops.exists(_.isInstanceOf[Block]) => block(ops.flatMap {
        case Block(ops) if !(ops.size == 2 && ops.exists(_.isInstanceOf[DeclareLocalVariable]) && ops.exists(_.isInstanceOf[AssignToLocalVariable])) => ops
        case other => Seq(other)
      }: _*)
    })

  /**
   * Asserts that the compiler creates an intermediate representation for `ast` that is equal to the expected intermediate representation.
   * The blocks in both the resulting and expected intermediate represetation will be flattened out before comparing the two.
   *
   * @param compiler the compiler to test
   * @param ast the expression to be evaluated
   * @param slots the slot configuration
   * @param expected the expected intermediate representation
   */
  private def assertCompilesTo(compiler: CodeChainExpressionCompiler,
                               ast: Expression,
                               slots: SlotConfiguration,
                               expected: IntermediateRepresentation): Unit = {

    val (got, _, _) = compiler.exprToIntermediateRepresentation(ast)
    val oldExpressionCompiler = new DefaultExpressionCompilerFront(slots, false, new VariableNamer, compiler.tokenContext)
    val defaultExpressionCompilerIR = ExpressionCompilation.nullCheckIfRequired(oldExpressionCompiler.compileExpression(ast, id).get)
    val clues = Seq(
      "------------ DefaultExpressionCompilerFront --------------\n",
      PrettyIR.pretty(defaultExpressionCompilerIR),
      "\n------------ CodeChainExpressionCompiler ------------\n",
      PrettyIR.pretty(expected))


    withClue(clues.mkString("\n", "\n", "\n\n")) {
      got should IrMatcher(expected.asInstanceOf[IntermediateRepresentation])
    }

    case class IrMatcher(expected: IntermediateRepresentation) extends Matcher[IntermediateRepresentation] {
      override def apply(actual: IntermediateRepresentation): MatchResult = {
        val flattenedExpected = flattenBlocks(expected).asInstanceOf[IntermediateRepresentation]
        val flattenedActual = flattenBlocks(actual).asInstanceOf[IntermediateRepresentation]
        val failureMsg =
          s"""$ast
             |${PrettyIR.pretty(flattenedExpected)}
             |  got:
             |${PrettyIR.pretty(flattenedActual)}
             |""".stripMargin

        flattenedActual should equal(flattenedExpected)
        MatchResult(
          matches = flattenedExpected == flattenedActual,
          rawFailureMessage = s"Failed to correctly compile $failureMsg",
          rawNegatedFailureMessage = s"Expected to get different results. $failureMsg",
        )
      }
    }
  }
}

