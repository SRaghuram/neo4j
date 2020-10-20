/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.Constant
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.add
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.fail
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
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.trueValue
import org.neo4j.codegen.api.IntermediateRepresentation.tryCatch
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.Load
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.compiler.helpers.PredicateHelper
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.functions.Acos
import org.neo4j.cypher.internal.expressions.functions.Asin
import org.neo4j.cypher.internal.expressions.functions.Atan
import org.neo4j.cypher.internal.expressions.functions.Cos
import org.neo4j.cypher.internal.expressions.functions.Cot
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Haversin
import org.neo4j.cypher.internal.expressions.functions.Sin
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.expressions.functions.Tan
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty
import org.neo4j.cypher.internal.physicalplanning.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.compiled.expressions.CodeChainExpressionCompiler.row
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.NODE_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.PROPERTY_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.RELATIONSHIP_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.loadExpressionVariable
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.setExpressionVariable
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vCURSORS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vNODE_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vPROPERTY_CURSOR
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherBoolean
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.cypher.operations.CypherMath
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.VirtualValues

class CodeChainExpressionCompiler(override val slots: SlotConfiguration,
                                  override val namer: VariableNamer) extends DefaultExpressionCompilerFront(slots, false, namer) {

  def exprToIntermediateRepresentation(expr: Expression): (IntermediateRepresentation, Set[Field], Set[LocalVariable]) = {
    val irInfo = exprToIRInner(expr).extract(namer)

   (irInfo.code, irInfo.fields, irInfo.localVariables)
  }

  override def compileExpression(expression: Expression, id: Id): Option[IntermediateExpression] = {
    val (ir, fields, localVars) = exprToIntermediateRepresentation(expression)
    Some(IntermediateExpression(ir, fields.toSeq, localVars.toSeq, Set.empty, requireNullCheck = false))
  }

  private def exprToIRInner(expr: Expression): CodeLink = expr match {

    case d: DoubleLiteral =>
      val constant = staticConstant[DoubleValue](namer.nextVariableName("doubleLit").toUpperCase, Values.doubleValue(d.value))
      START.withCode(isNullable = false, IRInfo(getStatic[DoubleValue](constant.name), Set(constant)))
    case i: IntegerLiteral =>
      val constant = staticConstant[LongValue](namer.nextVariableName("intLit").toUpperCase, Values.longValue(i.value))
      START.withCode(isNullable = false, IRInfo(getStatic[LongValue](constant.name), Set(constant)))
    case s: expressions.StringLiteral =>
      val constant = staticConstant[TextValue](namer.nextVariableName("strLit").toUpperCase, Values.utf8Value(s.value))
      START.withCode(isNullable = false, IRInfo(getStatic[TextValue](constant.name), Set(constant)))
    case Null() => START.withCode(isNullable = true, noValue)
    case True() => START.withCode(isNullable = false, trueValue)
    case False() => START.withCode(isNullable = false, falseValue)
    /*
     * Expression: "x"
     *
     * IR: row.getRefAt(offset)
     */
    case ReferenceFromSlot(offset, _) =>
      START.withCode(isNullable(offset, isLongSlot = false), invoke(row, method[ReadableRow, AnyValue, Int]("getRefAt"), constant(offset)))

    /*
     * Expression: "n"
     *
     * IR: row.getLongAt(offset)
     */
    case NodeFromSlot(offset, _) =>
      START.withCode(
        isNullable(offset, isLongSlot = true),
        IRInfo(invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset)), Set.empty[Field], Set(vNODE_CURSOR))
      )

    /*
     * IR: expressionVariables[ev.offset]
     */
    case ev: ExpressionVariable =>
      START.withCode(isNullable = true, loadExpressionVariable(ev))

    /*
     *
     * Expression: "n.prop" (offset 0)
     *
     * IR if n is nullable:
     *    longoffset_0 = getLongAt(offset)
     *    if (longoffset_0 == noValue) noValue else {
     *      longoffset_0.prop
     *    }
     *
     * IR if n not nullable:
     *    longoffset_0 = getLongAt(offset)
     *    longoffset_0.prop
     */
    case NodeProperty(offset, propToken, _) =>
      val nodeProperty = getNodeProperty(Constant(propToken), offset, offsetIsForLongSlot = true, nullable = isNullable(offset, isLongSlot = true))

      START
        .withCode(isNullable = true, IRInfo(nodeProperty, Set.empty[Field], Set(vPROPERTY_CURSOR, vNODE_CURSOR)))

    case ParameterFromSlot(offset, name, _) =>
      val parameterVariable = namer.parameterName(name)
      val local = variable[AnyValue](parameterVariable,
        arrayLoad(ExpressionCompilation.PARAMS, offset))

      START.withCode(isNullable = true, IRInfo(load(parameterVariable), Set.empty[Field], vCURSORS.toSet + local))

    case Property(targetExpression, PropertyKeyName(key)) =>
      val propVarName = namer.nextVariableName("propVar")
      exprToIRInner(targetExpression)
        .assignToAndNullCheck(typeRefOf[AnyValue], propVarName, noValue)
        .withCode(isNullable = true, getProperty(key, load(propVarName)))

    case Add(lhs, rhs) =>
      addSub(lhs, rhs, "add", invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), _, _))

    case Subtract(lhs, rhs) =>
      addSub(lhs, rhs, "sub", invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), _, _))

    /*
     *
     * Expression: "x" or "y" both nullable and no predicate
     *
     *{
     *  error = null
     *  orReturn = null
     *  orLhs = "x"
     *  seenNull = false
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
    case Or(lhs, rhs) =>
      val lhsName = namer.nextVariableName("orLhs")
      val rhsName = namer.nextVariableName("orRhs")

      // TODO: use extractCommonPrefix to not have duplicate code
      val lhsCodeLink = exprToIRInner(lhs)
      val rhsCodeLink = exprToIRInner(rhs)
      val lhsIRInfo = lhsCodeLink.extract(namer)
      val rhsIRInfo = rhsCodeLink.extract(namer)

      val exceptionName1 = namer.nextVariableName("orException1")
      val exceptionName2 = namer.nextVariableName("orException2")
      val returnValue = namer.nextVariableName("orReturn")
      val error = namer.nextVariableName("orError")
      val seenNull = namer.nextVariableName("seenNull")
      /*
       * - if expression is a predicate, we do not need to coerse or check null
       * - if expression isn't nullable we do not need to check nullability, but we still need to coerse:
       *
       *   try {
       *     orReturn = coerseToPredicate(orLhs)
       *   } catch (e: exception) {
       *     error = e
       *   }
       *
       * - if expression is nullable and not a predicate, we need to check null value before coersing:
       *
       *  if (orLhs == null) seenNull = true else {
       *    try {
       *      orReturn = coerseToPredicate(orLhs)
       *    } catch (e: exception) {
       *      error = e
       *    }
       *  }
       *
       */
      val exprToPredicate = (expression: Expression, ir: IntermediateRepresentation, isNullable: Boolean, exceptionName: String) => {
        val tryCatchBlock = (tryCatch[RuntimeException](exceptionName)
        (assign(returnValue, coerceToPredicate(ir)))
        (assign(error, load(exceptionName))))

        if (PredicateHelper.isPredicate(expression)) {
          assign(returnValue, ir)
        } else if(isNullable) {
          (ifElse(equal(ir, noValue))
          (block(assign(seenNull, Constant(true))))
          (block(tryCatchBlock)))
        } else {
          tryCatchBlock
        }
      }

      START.withCode(lhsCodeLink.isNullable || rhsCodeLink.isNullable, Seq(
        declareAndAssign(typeRefOf[AnyValue], returnValue, noValue),
        declareAndAssign(typeRefOf[RuntimeException], error, Constant(null)),
        declareAndAssign(typeRefOf[Boolean], seenNull, Constant(false)),
        declareAndAssign(typeRefOf[AnyValue], lhsName, lhsIRInfo.code),
        exprToPredicate(lhs, load(lhsName), lhsCodeLink.isNullable, exceptionName1),
        // Only evaluate rhs if lhs isn't true value
        condition(notEqual(load(returnValue), trueValue))(
          block(
            declareAndAssign(typeRefOf[AnyValue], rhsName, rhsIRInfo.code),
            exprToPredicate(rhs, load(rhsName), rhsCodeLink.isNullable, exceptionName2)
          )
        ),
        if (!PredicateHelper.isPredicate(lhs) || !PredicateHelper.isPredicate(rhs)) {
          condition(and(notEqual(load(returnValue), trueValue), notEqual(load(error), Constant(null))))(block(fail(load(error))))
        } else {
          noop()
        },
        ternary(
          equal(load(returnValue), trueValue),
          trueValue,
          ternary(
            equal(load(seenNull), Constant(true)),
            noValue,
            falseValue
          )
        )
      ), lhsIRInfo.fields ++ rhsIRInfo.fields, lhsIRInfo.localVariables ++ rhsIRInfo.localVariables)

    case IsNotNull(input) => // TODO: if not nullable -> return true
      val irInfo = exprToIRInner(input)
        .extract(namer)
      val varName = namer.nextVariableName("isNotNullInput")

      START
        .withCode(isNullable = true, irInfo)
        .assignTo(typeRefOf[AnyValue], varName)
        .withCode(isNullable = false, ternary(notEqual(load(varName), noValue), trueValue, falseValue))

    case f: FunctionInvocation =>
      f.function match {
        case Sin | Asin | Haversin | Cos | Acos | Cot | Tan | Atan => trigonometric(f)
        case Exists => f.arguments.head match {
          case Property(targetExpr, propertyKey) =>
            // TODO: if not nullable -> return true
            val irAndFields = exprToIRInner(targetExpr)
              .extract(namer)
            val varName = namer.nextVariableName("existsInput")

            START
              .withCode(isNullable = true, irAndFields)
              .assignToAndNullCheck(typeRefOf[AnyValue], varName, noValue)
              .withCode(isNullable = false, hasProperty(propertyKey.name, load(varName)))
        }
        case Size =>
          val varName = namer.nextVariableName("sizeInput")

          exprToIRInner(f.args.head)
          .assignToAndNullCheck(typeRefOf[AnyValue], varName, noValue)
          .withCode(isNullable = true, invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("size"), load(varName)))
      }

    case ListComprehension(ExtractScope(variable, None, extractExpression), listAst) =>
      val maybeList = load(namer.nextVariableName("maybeList"))
      val list = load(namer.nextVariableName("list"))
      val extracted = load(namer.nextVariableName("extracted"))
      val iter = load(namer.nextVariableName("iter"))
      val result = load(namer.nextVariableName("result"))
      val heapUsage = load(namer.nextVariableName("heapUsage"))

      val expressionVariable = variable.asInstanceOf[ExpressionVariable]
      val irInfo = exprToIRInner(extractExpression.getOrElse(expressionVariable)).extract(namer)

      val code =
        Seq(
          declareAndAssign(typeRefOf[ListValue], list, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), maybeList)),
          declareAndAssign(typeRefOf[util.ArrayList[AnyValue]], extracted, newInstance(constructor[java.util.ArrayList[AnyValue]])),
          declareAndAssign(typeRefOf[Long], heapUsage, constant(0L)),
          declareAndAssign(typeRefOf[java.util.Iterator[AnyValue]], iter.variable,
            invoke(list, method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(invoke(iter, method[java.util.Iterator[AnyValue], Boolean]("hasNext")))(block(
            setExpressionVariable(expressionVariable, invoke(iter, method[java.util.Iterator[AnyValue], Object]("next"))),
            declareAndAssign(typeRefOf[AnyValue], result, irInfo.code),
            assign(heapUsage.variable, ternary(equal(result, noValue), heapUsage, add(heapUsage, invoke(result, method[AnyValue, Long]("estimatedHeapUsage"))))),
            invokeSideEffect(extracted, method[java.util.ArrayList[_], Boolean, Object]("add"), result)
          )),
          invokeStatic(method[VirtualValues, ListValue, java.util.List[AnyValue], Long]("fromList"), extracted, heapUsage)
        )

      exprToIRInner(listAst)
        .assignToAndNullCheck(typeRefOf[AnyValue], maybeList, noValue)
        .withCode(isNullable = false, code, irInfo.fields, irInfo.localVariables)

    case ReduceExpression(ReduceScope(accumulator, reduce, expression), initAst, listAst) =>
      val maybeList = load(namer.nextVariableName("maybeList"))
      val list = load(namer.nextVariableName("list"))
      val iter = load(namer.nextVariableName("iter"))

      val initIRInfo = exprToIRInner(initAst).extract(namer)
      val reduceVariable = reduce.asInstanceOf[ExpressionVariable]
      val accumulatorVariable = accumulator.asInstanceOf[ExpressionVariable]
      val resultIRInfo = exprToIRInner(expression).extract(namer)

      val code =
        Seq(
          declareAndAssign(typeRefOf[ListValue], list, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), maybeList)),
          setExpressionVariable(accumulatorVariable, initIRInfo.code),
          declareAndAssign(typeRefOf[java.util.Iterator[AnyValue]], iter.variable,
            invoke(list, method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(invoke(iter, method[java.util.Iterator[AnyValue], Boolean]("hasNext")))(block(
            setExpressionVariable(reduceVariable, invoke(iter, method[java.util.Iterator[AnyValue], Object]("next"))),
            setExpressionVariable(accumulatorVariable, resultIRInfo.code),
          )),
          loadExpressionVariable(accumulatorVariable)
        )

      exprToIRInner(listAst)
        .assignToAndNullCheck(typeRefOf[AnyValue], maybeList, noValue)
        .withCode(isNullable = false, code, initIRInfo.fields ++ resultIRInfo.fields, initIRInfo.localVariables ++ resultIRInfo.localVariables)
  }

  /*
   * Expression: "a + b"
   *
   * If both a and b nullable:
   *  val x = load("x")
   *  if (x==null) null else {
   *    val y = load("y")
   *    if(y == null) null else {
   *       x + y
   *    }
   *  }
   *
   */
  private def addSub(lhs: Expression,
                     rhs: Expression,
                     name: String,
                     ir: (IntermediateRepresentation, IntermediateRepresentation) => IntermediateRepresentation): CodeLink = {
    val lhsName = namer.nextVariableName(name + "Lhs")
    val rhsName = namer.nextVariableName(name + "Rhs")

    val lhsCode = exprToIRInner(lhs)

    if (lhsCode.isNullable) { // Evaluate lhs first
      val lhsBase = lhsCode.assignToAndNullCheck(typeRefOf[AnyValue], lhsName, noValue)

      exprToIRInner(rhs)
        .rebaseOn(lhsBase)
        .assignToAndNullCheck(typeRefOf[AnyValue], rhsName, noValue)
        .withCode(isNullable = false, ir(load(lhsName), load(rhsName)))
    } else { // Evaluate rhs first
      exprToIRInner(rhs)
        .rebaseOn(lhsCode.inner)
        .assignToAndNullCheck(typeRefOf[AnyValue], rhsName, noValue)
        .withCode(isNullable = false, lhsCode.code, lhsCode.fields, lhsCode.variables)
        .assignTo(typeRefOf[AnyValue], lhsName)
        .withCode(isNullable = false, ir(load(lhsName), load(rhsName)))
    }

  }

  private def trigonometric(f: FunctionInvocation): CodeLink = {
    val name = f.function.name
    val inputExpression = f.args.head
    val inputName = namer.nextVariableName(name + "Input")

    exprToIRInner(inputExpression)
      .assignToAndNullCheck(typeRefOf[AnyValue], inputName, noValue)
      .withCode(isNullable = false, invokeStatic(method[CypherFunctions, DoubleValue, AnyValue](name), load(inputName)))
  }

  private def hasProperty(property: String, container: IntermediateRepresentation) =
    invokeStatic(method[CypherFunctions, BooleanValue, String, AnyValue, DbAccess,
      NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyExists"),
      constant(property),
      container, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR )

  private def coerceToPredicate(e: IntermediateRepresentation) =
    invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), e)

  private def isNullable(offset: Int, isLongSlot: Boolean) =
    slots.nameOfSlot(offset, longSlot = isLongSlot)
    .flatMap(slots.get)
    .get
    .nullable
}

object CodeChainExpressionCompiler {
  val row: Load = Load("row")

  /*
   * convert
   *
   * val x = {
   * s1
   * s2
   * s3
   * }
   *
   * to
   *
   * s1
   * s2
   * val x = s3
   */
  def unnestAssignBlock(typ: TypeReference,
                        inputName: String,
                        valueIr: Seq[IntermediateRepresentation],
                       ): Seq[IntermediateRepresentation] = {

    val declaredExpr = declareAndAssign(typ, inputName, valueIr.last)
    valueIr.init :+ declaredExpr
  }

  /**
   * Extract the common prefix between the two code chains `a` and `b`.
   *
   * Example:
   *
   * a:
   * val x1 = x.prop1
   * val x2 = x.prop2
   * x1 OR x2
   *
   * b:
   * val x1 = x.prop1
   * val x3 = x.prop3
   * x1 OR x3
   *
   * then the result should be:
   *
   * commonPrefix:
   * val x1 = x.prop1
   *
   * a':
   * val x2 = x.prop2
   * x1 OR x2
   *
   * b':
   * val x3 = x.prop3
   * x1 OR x3
   *
   */
  def extractCommonPrefix(a: CodeLink, b: CodeLink): (CodeChain, CodeChain, CodeChain) = {
    val aList = a.toList()
    val bList = b.toList()

    val commonPrefix = aList.zip(bList)
      .takeWhile { case (cn1, cn2) => cn1 == cn2 }
    val aPrunePoint = aList.lift(commonPrefix.length)
    val bPrunePoint = bList.lift(commonPrefix.length)

    (commonPrefix.last._1, aPrunePoint.map(a.pruneAt).getOrElse(START), bPrunePoint.map(b.pruneAt).getOrElse(START))
  }
}
