/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.{Optional, regex}

import org.neo4j.codegen.api.CodeGeneration.compileAnonymousClass
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api._
import org.neo4j.cypher.internal.compiler.helpers.PredicateHelper.isPredicate
import org.neo4j.cypher.internal.logical.plans.{CoerceToPredicate, NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.physicalplanning._
import org.neo4j.cypher.internal.physicalplanning.ast._
import org.neo4j.cypher.internal.runtime.ast.{ExpressionVariable, ParameterFromSlot}
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpression
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, ExpressionCursors}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType, ListType}
import org.neo4j.cypher.operations._
import org.neo4j.exceptions.{CypherTypeException, InternalException}
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, RelationshipScanCursor}
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.impl.util.ValueUtils.asAnyValue
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class VariableNamer {
  private var counter: Int = 0
  private val parameters = mutable.Map.empty[String, String]
  private val variables = mutable.Map.empty[String, String]
  def nextVariableName(): String = {
    val nextName = s"v$counter"
    counter += 1
    nextName
  }

  def parameterName(name: String): String = parameters.getOrElseUpdate(name, nextVariableName())
  def variableName(name: String): String = variables.getOrElseUpdate(name, nextVariableName())
}

/**
  * Compiles a Cypher Expression to a class or intermediate representation
  */
abstract class ExpressionCompiler(val slots: SlotConfiguration,
                                  val readOnly: Boolean,
                                  val codeGenerationMode: CodeGeneration.CodeGenerationMode,
                                  val namer: VariableNamer = new VariableNamer) {

  import ExpressionCompiler._
  import IntermediateRepresentation._

  /**
    * Compiles the given expression to an instance of [[CompiledExpression]]
    * @param e the expression to compile
    * @return an instance of [[CompiledExpression]] corresponding to the provided expression
    */
  def compileExpression(e: Expression): Option[CompiledExpression] = {
    intermediateCompileExpression(e).map { expression =>
      val classDeclaration =
        ClassDeclaration[CompiledExpression](
          PACKAGE_NAME,
          className(),
          None,
          Seq(typeRefOf[CompiledExpression]),
          Seq.empty,
          initializationCode = noop(),
          genFields = () => expression.fields,
          methods = Seq(
            MethodDeclaration("evaluate",
                              returnType = typeRefOf[AnyValue],
                              parameters = Seq(param[ExecutionContext]("context"),
                                               param[DbAccess]("dbAccess"),
                                               param[Array[AnyValue]]("params"),
                                               param[ExpressionCursors]("cursors"),
                                               param[Array[AnyValue]]("expressionVariables")),
                              body = block(
                                block(expression.variables.distinct.map { v =>
                                  declareAndAssign(v.typ, v.name, v.value)
                                }: _*),
                                nullCheckIfRequired(expression)
                              ))))
      compileAnonymousClass(classDeclaration, CodeGeneration.createGenerator(codeGenerationMode)).getDeclaredConstructor().newInstance()
    }
  }

  /**
    * Compiles the given projection to an instance of [[CompiledProjection]]
    * @param projections the projection to compile
    * @return an instance of [[CompiledProjection]] corresponding to the provided projection
    */
  def compileProjection(projections: Map[String, Expression]): Option[CompiledProjection] = {
      intermediateCompileProjection(projections).map(expression => {
      val classDeclaration =
        ClassDeclaration[CompiledProjection](
          PACKAGE_NAME,
          className(),
          None,
          Seq(typeRefOf[CompiledProjection]),
          Seq.empty,
          initializationCode = noop(),
          genFields = () => expression.fields,
          methods = Seq(
            MethodDeclaration("project",
                              returnType = typeRefOf[Unit],
                              parameters = Seq(param[ExecutionContext]("context"),
                                               param[DbAccess]("dbAccess"),
                                               param[Array[AnyValue]]("params"),
                                               param[ExpressionCursors]("cursors"),
                                               param[Array[AnyValue]]("expressionVariables")),
                              body = block(
                                block(expression.variables.distinct.map { v =>
                                  declareAndAssign(v.typ, v.name, v.value)
                                }: _*),
                                expression.ir
                              ))))
      compileAnonymousClass(classDeclaration, CodeGeneration.createGenerator(codeGenerationMode)).getDeclaredConstructor().newInstance()
    })
  }

  /**
    * Compiles the given groupings to an instance of [[CompiledGroupingExpression]]
    * @param orderedGroupings the groupings to compile
    * @return an instance of [[CompiledGroupingExpression]] corresponding to the provided groupings
    */
  def compileGrouping(orderedGroupings: SlotConfiguration => Seq[(String, Expression, Boolean)]): Option[CompiledGroupingExpression] = {
    def declarations(e: IntermediateExpression) = block(e.variables.distinct.map { v =>
      declareAndAssign(v.typ, v.name, v.value)
    }: _*)
    val orderedGroupingsBySlots = orderedGroupings(slots) // Apply the slot configuration to get the complete order
    val compiled = for {(k, v, _) <- orderedGroupingsBySlots
                        c <- intermediateCompileExpression(v)} yield slots(k) -> c
    if (compiled.size < orderedGroupingsBySlots.size) None
    else {
      val grouping = intermediateCompileGroupingExpression(compiled)
      val classDeclaration =
        ClassDeclaration[CompiledGroupingExpression](
          PACKAGE_NAME,
          className(),
          None,
          Seq(typeRefOf[CompiledGroupingExpression]),
          Seq.empty,
          initializationCode = noop(),
          genFields = () => grouping.projectKey.fields ++ grouping.computeKey.fields ++ grouping.getKey.fields,
          methods = Seq(
            MethodDeclaration("projectGroupingKey",
                              returnType = typeRefOf[Unit],
                              parameters = Seq(param[ExecutionContext]("context"),
                                               param[AnyValue]("key")),
                              body = block(
                                declarations(grouping.projectKey),
                                grouping.projectKey.ir)),
            MethodDeclaration("computeGroupingKey",
                              returnType = typeRefOf[AnyValue],
                              parameters = Seq(param[ExecutionContext]("context"),
                                               param[DbAccess]("dbAccess"),
                                               param[Array[AnyValue]]("params"),
                                               param[ExpressionCursors]("cursors"),
                                               param[Array[AnyValue]]("expressionVariables")),
                              body = block(
                                declarations(grouping.computeKey),
                                nullCheckIfRequired(grouping.computeKey))),
            MethodDeclaration("getGroupingKey",
                              returnType = typeRefOf[AnyValue],
                              parameters = Seq(param[ExecutionContext]("context")),
                              body = block(
                                declarations(grouping.getKey),
                                nullCheckIfRequired(grouping.getKey)))
          ))
      Some(compileAnonymousClass(classDeclaration, CodeGeneration.createGenerator(codeGenerationMode)).getDeclaredConstructor().newInstance())
    }
  }

  /**
    * Compiles the given grouping keys to an instance of [[IntermediateExpression]]
    * @param orderedGroupings the groupings to compile, already sorted in correct grouping key order
    * @return an instance of [[IntermediateGroupingExpression]] corresponding to the provided groupings
    */
  def intermediateCompileGroupingKey(orderedGroupings: Seq[Expression]): Option[IntermediateExpression] = {
    val projections = orderedGroupings.flatMap(intermediateCompileExpression)
    if (projections.size < orderedGroupings.size) None
    else {
      require(projections.nonEmpty)
      val singleValue = projections.size == 1

      val computeKeyOps = projections.map(p => nullCheckIfRequired(p))
      val computeKey =
        if (singleValue) computeKeyOps.head
        else invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf[AnyValue](computeKeyOps: _*))

      Some(IntermediateExpression(computeKey, projections.flatMap(_.fields), projections.flatMap(_.variables), Set.empty))
    }
  }

  /**
    * Compiles the given expression to an [[IntermediateExpression]]
    * @param expression the expression to compile
    * @return an [[IntermediateExpression]] corresponding to the provided expression.
    */
  def intermediateCompileExpression(expression: Expression): Option[IntermediateExpression] = expression match {

    //functions
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None
    case c: FunctionInvocation => compileFunction(c)

    //math
    case expressions.Multiply(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("multiply"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)
      }

    case expressions.Add(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)
      }

    case UnaryAdd(source) => intermediateCompileExpression(source)

    case expressions.Subtract(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)
      }

    case UnarySubtract(source) =>
      for {arg <- intermediateCompileExpression(source)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"),
                       getStatic[Values, IntegralValue]("ZERO_INT"), arg.ir), arg.fields, arg.variables, arg.nullChecks)
      }

    case Divide(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("divide"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(invokeStatic(method[CypherMath, Boolean, AnyValue, AnyValue]("divideCheckForNull"),
                           nullCheckIfRequired(l), nullCheckIfRequired(r))))
      }

    case Modulo(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("modulo"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)
      }

    case Pow(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("pow"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)
      }

    //literals
    case d: DoubleLiteral =>
      val constant = staticConstant[DoubleValue](namer.nextVariableName().toUpperCase, Values.doubleValue(d.value))
      Some(IntermediateExpression(getStatic[DoubleValue](constant.name), Seq(constant), Seq.empty, Set.empty, requireNullCheck = false))
    case i: IntegerLiteral =>
      val constant = staticConstant[LongValue](namer.nextVariableName().toUpperCase, Values.longValue(i.value))
      Some(IntermediateExpression(getStatic[LongValue](constant.name), Seq(constant), Seq.empty, Set.empty, requireNullCheck = false))
    case s: expressions.StringLiteral =>
      val constant = staticConstant[TextValue](namer.nextVariableName().toUpperCase, Values.stringValue(s.value))
      Some(IntermediateExpression(getStatic[TextValue](constant.name), Seq(constant), Seq.empty, Set.empty, requireNullCheck = false))
    case _: Null => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
    case _: True => Some(IntermediateExpression(trueValue, Seq.empty, Seq.empty, Set.empty, requireNullCheck = false))
    case _: False => Some(IntermediateExpression(falseValue, Seq.empty, Seq.empty, Set.empty, requireNullCheck = false))
    case ListLiteral(args) =>
      val in = args.flatMap(intermediateCompileExpression)
      if (in.size < args.size) None
      else {
        val fields: Seq[Field] = in.foldLeft(Seq.empty[Field])((a, b) => a ++ b.fields)
        val variables: Seq[LocalVariable] = in.foldLeft(Seq.empty[LocalVariable])((a, b) => a ++ b.variables)
        Some(IntermediateExpression(
          invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf[AnyValue](in.map(i => nullCheckIfRequired(i)): _*)),
          fields, variables, Set.empty, requireNullCheck = false))
      }

    case MapExpression(items) =>
      val compiled = (for {(k, v) <- items
                           c <- intermediateCompileExpression(v)} yield k -> c).toMap
      if (compiled.size < items.size) None
      else {
        val tempVariable = namer.nextVariableName()
        val ops = Seq(
          declare[MapValueBuilder](tempVariable),
          assign(tempVariable, newInstance(constructor[MapValueBuilder, Int], constant(compiled.size)))
        ) ++ compiled.map {
          case (k, v) => invokeSideEffect(load(tempVariable),
                                          method[MapValueBuilder, AnyValue, String, AnyValue]("add"),
                                          constant(k.name), nullCheckIfRequired(v))
        } :+ invoke(load(tempVariable), method[MapValueBuilder, MapValue]("build"))

        Some(IntermediateExpression(block(ops: _*), compiled.values.flatMap(_.fields).toSeq,
                                    compiled.values.flatMap(_.variables).toSeq, Set.empty, requireNullCheck = false))
      }

    case _: MapProjection => throw new InternalException("should have been rewritten away")
    case _: NestedPlanExpression => throw new InternalException("should have been rewritten away")

    case DesugaredMapProjection(name, items, includeAllProps) =>
      val expressions = items.flatMap(i => intermediateCompileExpression(i.exp))

      if (expressions.size < items.size) None
      else {
        val expressionMap = items.map(_.key.name).zip(expressions)

        val builderVar = namer.nextVariableName()
        val buildMapValue = if (expressionMap.nonEmpty) {
          Seq(
            declare[MapValueBuilder](builderVar),
            assign(builderVar, newInstance(constructor[MapValueBuilder, Int], constant(expressionMap.size)))) ++
            expressionMap.map {
              case (key, exp) => invokeSideEffect(load(builderVar),
                                                  method[MapValueBuilder, AnyValue, String, AnyValue]("add"),
                                                  constant(key), exp.ir)
            }
        } else Seq.empty

        val (accessName, maybeNullCheck) = accessVariable(name.name)

        if (!includeAllProps) {
          Some(IntermediateExpression(
            block(buildMapValue :+ invoke(load(builderVar), method[MapValueBuilder, MapValue]("build")): _*),
            expressions.flatMap(_.fields), expressions.flatMap(_.variables), maybeNullCheck.toSet))

        } else if (buildMapValue.isEmpty) {
          Some(IntermediateExpression(
            block(buildMapValue ++ Seq(
              invokeStatic(
                method[CypherCoercions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor](
                  "asMapValue"),
                accessName, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)): _*),
            expressions.flatMap(_.fields), expressions.flatMap(_.variables) ++ vCURSORS,
            maybeNullCheck.toSet))
        } else {
          Some(IntermediateExpression(
            block(buildMapValue ++ Seq(
              invoke(
                invokeStatic(
                  method[CypherCoercions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor](
                    "asMapValue"),
                  accessName, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
                method[MapValue, MapValue, MapValue]("updatedWith"),
                invoke(load(builderVar), method[MapValueBuilder, MapValue]("build")))): _*),
            expressions.flatMap(_.fields), expressions.flatMap(_.variables) ++ vCURSORS,
            maybeNullCheck.toSet))
        }
      }

    case ListSlice(collection, None, None) => intermediateCompileExpression(collection)

    case ListSlice(collection, Some(from), None) =>
      for {c <- intermediateCompileExpression(collection)
           f <- intermediateCompileExpression(from)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("fromSlice"), c.ir, f.ir),
          c.fields ++ f.fields, c.variables ++ f.variables, c.nullChecks ++ f.nullChecks)

      }

    case ListSlice(collection, None, Some(to)) =>
      for {c <- intermediateCompileExpression(collection)
           t <- intermediateCompileExpression(to)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("toSlice"), c.ir, t.ir),
          c.fields ++ t.fields, c.variables ++ t.variables, c.nullChecks ++ t.nullChecks)
      }

    case ListSlice(collection, Some(from), Some(to)) =>
      for {c <- intermediateCompileExpression(collection)
           f <- intermediateCompileExpression(from)
           t <- intermediateCompileExpression(to)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("fullSlice"), c.ir, f.ir, t.ir),
          c.fields ++ f.fields ++ t.fields, c.variables ++ f.variables ++ t.variables,
          c.nullChecks ++ f.nullChecks ++ t.nullChecks)
      }

    case Variable(name) =>
      val (variableAccess, nullCheck) = accessVariable(name)
      Some(IntermediateExpression(variableAccess, Seq.empty, Seq.empty, nullCheck.toSet))

    case e: ExpressionVariable =>
      val varLoad = loadExpressionVariable(e)
      Some(IntermediateExpression(varLoad, Seq.empty, Seq.empty, Set(equal(varLoad, noValue)) ))

    case SingleIterablePredicate(scope, collectionExpression) =>
      /*
        ListValue list = [evaluate collection expression];
        Iterator<AnyValue> listIterator = list.iterator();
        int matches = 0;
        boolean isNull = false;
        while( matches < 2 && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            expressionVariables[innerVarOffset] = currentValue;
            Value isMatch = [result from inner expression]
            if (isMatch == Values.TRUE)
            {
                matches++;
            }
            if (isMatch == Values.NO_VALUE)
            {
                isNull = true;
            }
        }
        return (matches < 2 && isNull) ? Values.NO_VALUE : Values.booleanValue(matches == 1);
       */
      val innerVariable = ExpressionVariable.cast(scope.variable)
      val iterVariable = namer.nextVariableName()
      for {collection <- intermediateCompileExpression(collectionExpression)
           inner <- intermediateCompileExpression(scope.innerPredicate.get)
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val matches = namer.nextVariableName()
        val isNull = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // int matches = 0;
          // boolean isNull = false;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[Int](matches),
          assign(matches, constant(0)),
          declare[Boolean](isNull),
          assign(isNull, constant(false)),
          // Iterator<AnyValue> listIterator = list.iterator();
          // while( matches < 2 && listIterator.hasNext())
          // {
          //    AnyValue currentValue = listIterator.next();
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(and(lessThan(load(matches), constant(2)),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              // expressionVariables[innerVarOffset] = currentValue;
              setExpressionVariable(innerVariable, load(currentValue)),
              // Value isMatch = [result from inner expression]
              // if (isMatch == Values.TRUE)
              // {
              //     matches = matches + 1;
              // }
              declare[Value](isMatch),
              assign(isMatch, nullCheckIfRequired(inner)),
              condition(equal(load(isMatch), trueValue))(
                assign(matches, add(load(matches), constant(1)))
              ),
              // if (isMatch == Values.NO_VALUE)
              // {
              //     isNull=true;
              // }
              condition(equal(load(isMatch), noValue))(
                assign(isNull, constant(true))
              )
            ): _*)
          },
          // }
          // return (matches < 2 && isNull) ? Values.NO_VALUE : Values.booleanValue(matches==1);
          ternary(and(lessThan(load(matches), constant(2)), load(isNull)),
                  noValue,
                  invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                               equal(load(matches), constant(1))))
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                               collection.variables ++ inner.variables,
                               collection.nullChecks)
      }

    case NoneIterablePredicate(scope, collectionExpression) =>
      /*
        ListValue list = [evaluate collection expression];
        Iterator<AnyValue> listIterator = list.iterator();
        Value isMatch = listIterator.hasNext() ? Values.NO_VALUE : Values.FALSE;
        boolean isNull = false;
        while( isMatch != Values.TRUE && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            expressionVariables[innerVarOffset] = currentValue;
            isMatch = [result from inner expression]
            if (isMatch == Values.NO_VALUE)
            {
                isNull = true;
            }
        }
        return (isNull && isMatch != Values.TRUE) ? Values.NO_VALUE : Values.booleanValue(isMatch == Values.FALSE);
       */
      val innerVariable = ExpressionVariable.cast(scope.variable)
      val iterVariable = namer.nextVariableName()
      for {collection <- intermediateCompileExpression(collectionExpression)
           inner <- intermediateCompileExpression(scope.innerPredicate.get)
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val isNull = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // Value isMatch = Values.NO_VALUE;
          // boolean isNull = false;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          declare[Value](isMatch),
          // assign(isMatch, noValue),
          assign(isMatch,
                 ternary(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")), noValue,
                         falseValue)),
          declare[Boolean](isNull),
          assign(isNull, constant(false)),
          // Iterator<AnyValue> listIterator = list.iterator();
          // while( isMatch != Values.TRUE, && listIterator.hasNext() )
          // {
          //    AnyValue currentValue = listIterator.next();
          loop(and(notEqual(load(isMatch), trueValue),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              // expressionVariables[innerVarOffset] = currentValue;
              setExpressionVariable(innerVariable, load(currentValue)),
              // isMatch = [result from inner expression]
              assign(isMatch, nullCheckIfRequired(inner)),
              // if (isMatch == Values.NO_VALUE)
              // {
              //     isNull=true;
              // }
              condition(equal(load(isMatch), noValue))(
                assign(isNull, constant(true))
              )
            ): _*)
          },
          // }
          // return (isNull && isMatch != Values.TRUE) ? Values.NO_VALUE : Values.booleanValue(isMatch == Values.FALSE);
          ternary(and(load(isNull), notEqual(load(isMatch), trueValue)),
                  noValue,
                  invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"), equal(load(isMatch), falseValue)))
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                               collection.variables ++ inner.variables,
                               collection.nullChecks)
      }

    case AnyIterablePredicate(scope, collectionExpression) =>
      /*
        ListValue list = [evaluate collection expression];
        Iterator<AnyValue> listIterator = list.iterator();
        Value isMatch = Values.FALSE;
        boolean isNull = false;
        while( isMatch != Values.TRUE && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            expressionVariables[innerVarOffset] = currentValue;
            isMatch = [result from inner expression]
            if (isMatch == Values.NO_VALUE)
            {
                isNull = true;
            }
        }
        return (isNull && isMatch != Values.TRUE) ? Values.NO_VALUE : isMatch;
       */
      val innerVariable = ExpressionVariable.cast(scope.variable)
      val iterVariable = namer.nextVariableName()
      for {collection <- intermediateCompileExpression(collectionExpression)
           inner <- intermediateCompileExpression(scope.innerPredicate.get)
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val isNull = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // Value isMatch = Values.FALSE;
          // boolean isNull = false;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[Value](isMatch),
          assign(isMatch, falseValue),
          declare[Boolean](isNull),
          assign(isNull, constant(false)),
          // Iterator<AnyValue> listIterator = list.iterator();
          // while( isMatch != Values.TRUE listIterator.hasNext())
          // {
          //    AnyValue currentValue = listIterator.next();
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(and(notEqual(load(isMatch), trueValue),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              // expressionVariables[innerVarOffset] = currentValue;
              setExpressionVariable(innerVariable, load(currentValue)),
              // isMatch = [result from inner expression]
              assign(isMatch, nullCheckIfRequired(inner)),
              // if (isMatch == Values.NO_VALUE)
              // {
              //     isNull=true;
              // }
              condition(equal(load(isMatch), noValue))(
                assign(isNull, constant(true))
              )
            ): _*)
          },
          // }
          // return (isNull && isMatch != Values.TRUE) ? Values.NO_VALUE : isMatch;
          ternary(and(load(isNull), notEqual(load(isMatch), trueValue)),
                  noValue,
                  load(isMatch))
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                               collection.variables ++ inner.variables,
                               collection.nullChecks)
      }

    case AllIterablePredicate(scope, collectionExpression) =>
      /*
        ListValue list = [evaluate collection expression];
        Iterator<AnyValue> listIterator = list.iterator();
        Value isMatch = Values.TRUE;
        while( isMatch==Values.TRUE && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            expressionVariables[innerVarOffset] = currentValue;
            isMatch = [result from inner expression]
        }
        return isMatch;
       */
      val innerVariable = ExpressionVariable.cast(scope.variable)
      val iterVariable = namer.nextVariableName()
      for {collection <- intermediateCompileExpression(collectionExpression)
           inner <- intermediateCompileExpression(scope.innerPredicate.get)
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // Value isMatch = Values.TRUE;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[Value](isMatch),
          assign(isMatch, trueValue),
          // Iterator<AnyValue> listIterator = list.iterator();
          // while( isMatch==Values.TRUE && listIterator.hasNext())
          // {
          //    AnyValue currentValue = listIterator.next();
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(and(equal(load(isMatch), trueValue),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              // expressionVariables[innerVarOffset] = currentValue;
              setExpressionVariable(innerVariable, load(currentValue)),
              // isMatch = [result from inner expression]
              assign(isMatch, nullCheckIfRequired(inner))
            ): _*)
          },
          // }
          // return isMatch;
          load(isMatch)
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                               collection.variables ++ inner.variables,
                               collection.nullChecks)
      }

    case ListComprehension(scope, list) =>
      val filter = scope.innerPredicate match {
        case Some(_: True) | None => intermediateCompileExpression(list)
        case Some(inner) => filterExpression(intermediateCompileExpression(list),
                                             inner, ExpressionVariable.cast(scope.variable))
      }
      scope.extractExpression match {
        case None => filter
        case Some(extract) =>
          extractExpression(filter, extract, ExpressionVariable.cast(scope.variable))
      }

    case ReduceExpression(scope, initExpression, collectionExpression) =>
      /*
        The generated code will be something along the lines of:

        ListValue list = [evaluate collection expression];
        expressionVariables[accVarOffset] = init;
        for ( AnyValue currentValue : list ) {
            expressionVariables[innerVarOffset] = currentValue;
            expressionVariables[accVarOffset] = [result])
        }
        return expressionVariables[accVarOffset]
       */
      val accVar = ExpressionVariable.cast(scope.accumulator)
      val innerVar = ExpressionVariable.cast(scope.variable)

      val iterVariable = namer.nextVariableName()
      for {collection <- intermediateCompileExpression(collectionExpression)
           init <- intermediateCompileExpression(initExpression)
           inner <- intermediateCompileExpression(scope.expression)
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          // expressionVariables[accOffset] = init;
          setExpressionVariable(accVar, nullCheckIfRequired(init)),
          // Iterator<AnyValue> iter = list.iterator();
          // while (iter.hasNext) {
          //   AnyValue currentValue = iter.next();
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext"))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              // expressionVariables[iterOffset] = currentValue;
              setExpressionVariable(innerVar, load(currentValue)),
              // expressionVariables[accOffset] = [inner expression];
              setExpressionVariable(accVar, nullCheckIfRequired(inner))
            ): _*)
          },
          // return expressionVariables[accOffset];
          loadExpressionVariable(accVar)
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields ++ init.fields, collection.variables ++
          inner.variables ++ init.variables, collection.nullChecks ++ init.nullChecks)
      }

    //boolean operators
    case Or(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateOrs(List(left, right))
      }

    case Ors(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty))
        { (acc, current) =>
          for {l <- acc
               e <- intermediateCompileExpression(current)} yield l :+ (e -> isPredicate(current))
        }

      for (e <- compiled) yield e match {
        case Nil => IntermediateExpression(trueValue, Seq.empty, Seq.empty,
                                           Set.empty) //this will not really happen because of rewriters etc
        case (a, isPredicate) :: Nil => if (isPredicate) a else coerceToPredicate(a)
        case list =>
          val coerced = list.map {
            case (p, true) => p
            case (p, false) => coerceToPredicate(p)
          }
          generateOrs(coerced)
      }

    case Xor(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("xor"), left.ir, right.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)
      }

    case And(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateAnds(List(left, right))
      }

    case Ands(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty))
        { (acc, current) =>
          for {l <- acc
               e <- intermediateCompileExpression(current)} yield l :+ (e -> isPredicate(current))
        }

      for (e <- compiled) yield e match {
        case Nil => IntermediateExpression(trueValue, Seq.empty, Seq.empty,
                                           Set.empty) //this will not really happen because of rewriters etc
        case (a, isPredicate) :: Nil => if (isPredicate) a else coerceToPredicate(a)
        case list =>
          val coerced = list.map {
            case (p, true) => p
            case (p, false) => coerceToPredicate(p)
          }
          generateAnds(coerced)
      }

    case AndedPropertyInequalities(_, _, inequalities) =>
      val compiledInequalities = inequalities.toIndexedSeq.flatMap(i => intermediateCompileExpression(i))
      if (compiledInequalities.size < inequalities.size) None
      else Some(generateAnds(compiledInequalities.toList))

    case expressions.Not(arg) =>
      intermediateCompileExpression(arg).map(a => {
        val in = if (isPredicate(arg)) a else coerceToPredicate(a)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue]("not"), in.ir), in.fields, in.variables, in.nullChecks)
      })

    case Equals(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName, noValueOr(l, r)(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("equals"), l.ir, r.ir))))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    case NotEquals(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName, noValueOr(l, r)(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("notEquals"), l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    case CoerceToPredicate(inner) => intermediateCompileExpression(inner).map(coerceToPredicate)

    case RegexMatch(lhs, rhs) => rhs match {
      case expressions.StringLiteral(name) =>
        for (e <- intermediateCompileExpression(lhs)) yield {
          val f = field[regex.Pattern](namer.nextVariableName())
          val ops = block(
            //if (f == null) { f = Pattern.compile(...) }
            condition(isNull(loadField(f)))(
              setField(f, invokeStatic(method[regex.Pattern, regex.Pattern, String]("compile"), constant(name)))),
            invokeStatic(method[CypherBoolean, BooleanValue, TextValue, regex.Pattern]("regex"), cast[TextValue](e.ir),
                         loadField(f)))

          IntermediateExpression(ops, e.fields :+ f, e.variables, Set(not(instanceOf[TextValue](e.ir))))
        }

      case _ =>
        for {l <- intermediateCompileExpression(lhs)
             r <- intermediateCompileExpression(rhs)
        } yield {
          IntermediateExpression(
            invokeStatic(method[CypherBoolean, BooleanValue, TextValue, TextValue]("regex"),
                         cast[TextValue](l.ir),
                         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("asTextValue"), r.ir)),
            l.fields ++ r.fields, l.variables ++ r.variables, r.nullChecks + not(instanceOf[TextValue](l.ir)))
        }
    }

    case StartsWith(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("startsWith"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case EndsWith(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("endsWith"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case Contains(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("contains"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case expressions.IsNull(test) =>
      for (e <- intermediateCompileExpression(test)) yield {
        IntermediateExpression(
          nullCheck(e)(trueValue)(falseValue), e.fields, e.variables, Set.empty
        )
      }

    case expressions.IsNotNull(test) =>
      for (e <- intermediateCompileExpression(test)) yield {
        IntermediateExpression(
          nullCheck(e)(falseValue)(trueValue), e.fields, e.variables, Set.empty
        )
      }

    case LessThan(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {

        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
                                               noValueOr(l, r)(
                                       invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("lessThan"), l.ir,
                                                    r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    case LessThanOrEqual(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
                                               noValueOr(l, r)(
                                       invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("lessThanOrEqual"),
                                                    l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    case GreaterThan(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
                                               noValueOr(l, r)(
                                       invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("greaterThan"),
                                                    l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    case GreaterThanOrEqual(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
                                               noValueOr(l, r)(invokeStatic(
                                       method[CypherBoolean, Value, AnyValue, AnyValue]("greaterThanOrEqual"), l.ir,
                                       r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    case In(_, ListLiteral(expressions)) if expressions.isEmpty =>
      Some(IntermediateExpression(falseValue, Seq.empty, Seq.empty, Set.empty, requireNullCheck = false))

    case In(lhs, ListLiteral(expressions)) if expressions.forall(e => e.isInstanceOf[Literal]) =>
      //we create the set at compile time here and at runtime we basically only
      //do `set.contains`
      val set = setFromLiterals(expressions.asInstanceOf[Seq[Literal]])
      //in the case the list contains null we have true of null, otherwise true or false
      val containsNull = expressions.exists {
        case _: Null => true
        case _ => false
      }
      val onNotFound = if (containsNull) noValue else falseValue
      for (l <- intermediateCompileExpression(lhs)) yield {
        val setField = staticConstant[util.HashSet[AnyValue]](namer.nextVariableName(), set)
        IntermediateExpression(
          ternary(invoke(getStatic[util.HashSet[AnyValue]](setField.name), method[util.Set[AnyValue], Boolean, Object]("contains"), l.ir),
                  trueValue, onNotFound), l.fields :+ setField, l.variables, l.nullChecks)
      }

    case In(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)} yield {

        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
                                               noValueOr(r)(invokeStatic(
                                       method[CypherBoolean, Value, AnyValue, AnyValue]("in"), l.ir,
                                       nullCheckIfRequired(r)))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables, Set(nullChecks), requireNullCheck = false)
      }

    // misc
    case CoerceTo(expr, typ) =>
      for (e <- intermediateCompileExpression(expr)) yield {
        typ match {
          case CTAny => e
          case CTString =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, TextValue, AnyValue]("asTextValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTNode =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, NodeValue, AnyValue]("asNodeValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTRelationship =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, RelationshipValue, AnyValue]("asRelationshipValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTPath =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PathValue, AnyValue]("asPathValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTInteger =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, IntegralValue, AnyValue]("asIntegralValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTFloat =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, FloatingPointValue, AnyValue]("asFloatingPointValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTMap =>
            IntermediateExpression(
              invokeStatic(
                method[CypherCoercions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor](
                  "asMapValue"),
                e.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
              e.fields, e.variables ++ vCURSORS, e.nullChecks)

          case l: ListType =>
            val typ = asNeoType(l.innerType)

            IntermediateExpression(
              invokeStatic(method[CypherCoercions, ListValue, AnyValue, AnyType, DbAccess, ExpressionCursors]("asList"),
                           e.ir, typ, DB_ACCESS, CURSORS),
              e.fields, e.variables, e.nullChecks)

          case CTBoolean =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, BooleanValue, AnyValue]("asBooleanValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTNumber =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, NumberValue, AnyValue]("asNumberValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTPoint =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PointValue, AnyValue]("asPointValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTGeometry =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PointValue, AnyValue]("asPointValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTDate =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DateValue, AnyValue]("asDateValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTLocalTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, LocalTimeValue, AnyValue]("asLocalTimeValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, TimeValue, AnyValue]("asTimeValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTLocalDateTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, LocalDateTimeValue, AnyValue]("asLocalDateTimeValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTDateTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DateTimeValue, AnyValue]("asDateTimeValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case CTDuration =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DurationValue, AnyValue]("asDurationValue"), e.ir),
              e.fields, e.variables, e.nullChecks)
          case _ => throw new CypherTypeException(s"Can't coerce to $typ")
        }
      }

    //data access
    case ContainerIndex(container, index) =>
      for {c <- intermediateCompileExpression(container)
           idx <- intermediateCompileExpression(index)
      } yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName, noValueOr(c, idx)(noValueOr(c, idx)(
          invokeStatic(method[CypherFunctions, AnyValue, AnyValue, AnyValue, DbAccess,
            NodeCursor, RelationshipScanCursor, PropertyCursor]("containerIndex"),
                       c.ir, idx.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)))))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(
          ops, c.fields ++ idx.fields, c.variables ++ idx.variables ++ vCURSORS, Set(nullChecks), requireNullCheck = false)
      }

    case ParameterFromSlot(offset, name, _) =>
      val parameterVariable = namer.parameterName(name)
      val local = variable[AnyValue](parameterVariable,
                                      arrayLoad(load("params"), offset))
      Some(IntermediateExpression(load(parameterVariable), Seq.empty, Seq(local),
                                  Set(equal(load(parameterVariable), noValue)), requireNullCheck = false))

    case CaseExpression(Some(innerExpression), alternativeExpressions, defaultExpression) =>

      val maybeDefault = defaultExpression match {
        case Some(e) => intermediateCompileExpression(e)
        case None => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
      }
      val (checkExpressions, loadExpressions) = alternativeExpressions.unzip

      val checks = checkExpressions.flatMap(intermediateCompileExpression)
      val loads = loadExpressions.flatMap(intermediateCompileExpression)
      if (checks.size != loads.size || checks.isEmpty || maybeDefault.isEmpty) None
      else {
        for {inner: IntermediateExpression <- intermediateCompileExpression(innerExpression)
        } yield {

          //AnyValue compare = inner;
          //AnyValue returnValue = null;
          //if (alternatives[0]._1.equals(compare)) {
          //  returnValue = alternatives[0]._2;
          //} else {
          //  if (alternatives[1]._1.equals(compare)) {
          //    returnValue = alternatives[1]._2;
          //  } else {
          //   ...
          //}
          //return returnValue == null ? default : returnValue
          val default = maybeDefault.get
          val returnVariable = namer.nextVariableName()
          val local = variable[AnyValue](returnVariable, constant(null))
          val ops = caseExpression(returnVariable, checks.map(_.ir), loads.map(_.ir), default.ir,
                          toCheck => invoke(inner.ir, method[AnyValue, Boolean, AnyRef]("equals"), toCheck))
          IntermediateExpression(ops,
                                 inner.fields ++ checks.flatMap(_.fields) ++ loads.flatMap(_.fields) ++ default.fields,
                                 inner.variables ++ checks.flatMap(_.variables) ++ loads.flatMap(_.variables) ++ default.variables :+ local,
                                 Set(block(equal(load(returnVariable), noValue))))
        }
      }


    case CaseExpression(None, alternativeExpressions, defaultExpression) =>
      val maybeDefault = defaultExpression match {
        case Some(e) => intermediateCompileExpression(e)
        case None => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
      }
      val (checkExpressions, loadExpressions) = alternativeExpressions.unzip
      val checks = checkExpressions.flatMap { e =>
        if (isPredicate(e)) intermediateCompileExpression(e)
        else intermediateCompileExpression(e).map(coerceToPredicate)
      }

      val loads = loadExpressions.flatMap(intermediateCompileExpression)
      if (checks.size != loads.size || maybeDefault.isEmpty) None
      else {

        //AnyValue compare = inner;
        //AnyValue returnValue = null;
        //if (alternatives[0]._1 == Values.TRUE) {
        //  returnValue = alternatives[0]._2;
        //} else {
        //  if (alternatives[1]._1== Values.TRUE) {
        //    returnValue = alternatives[1]._2;
        //  } else {
        //   ...
        //}
        //return returnValue == null ? default : returnValue
        val default = maybeDefault.get
        val returnVariable = namer.nextVariableName()
        val local = variable[AnyValue](returnVariable, constant(null))
        val ops = caseExpression(returnVariable, checks.map(_.ir), loads.map(_.ir), default.ir,
                        toCheck => equal(toCheck, trueValue))
        Some(IntermediateExpression(ops,
                                    checks.flatMap(_.fields) ++ loads.flatMap(_.fields) ++ default.fields,
                                    checks.flatMap(_.variables) ++ loads.flatMap(_.variables) ++ default
                                      .variables :+ local,
                                    Set(block(equal(load(returnVariable), noValue)))))
      }

    case Property(targetExpression, PropertyKeyName(key)) =>
      for (map <- intermediateCompileExpression(targetExpression)) yield {
        val variableName = namer.nextVariableName()
        val propertyGet = getProperty(key, nullCheckIfRequired(map))
        val call = noValueOr(map)(propertyGet)
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName, call))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, map.fields, map.variables ++ vCURSORS , Set(nullChecks), requireNullCheck = false)
      }

    case NodeProperty(offset, token, _) =>
      val variableName = namer.nextVariableName()
      val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
        getNodeProperty(constant(token), offset)))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks), requireNullCheck = false))

    case property@SlottedCachedPropertyWithPropertyToken(_, _, offset, offsetIsForLongSlot, token, _, entityType) =>
      if (token == StatementConstants.NO_SUCH_PROPERTY_KEY) Some(
        IntermediateExpression(noValue, Seq.empty, Seq.empty, Set.empty, requireNullCheck = false))
      else {
        val variableName = namer.nextVariableName()
        val entityId = getEntityId(offsetIsForLongSlot, offset, entityType)
        val (propertyGet, txStatePropertyGet, cursor, cursorVar) = callPropertyGet(entityType)
        def checkPropertyTxState(continuation: IntermediateRepresentation): IntermediateRepresentation = {
          if (readOnly) continuation
          else {
            block(
              assign(variableName, invoke(DB_ACCESS, txStatePropertyGet, entityId, constant(token))),
              condition(isNull(load(variableName)))(continuation))
          }
        }
        /**
          * {{{
          *   var property = NO_VALUE
          *   if (id != -1L) {
          *     property = [get property from tx state]
          *     if (property == null) {
          *       property = [getCachedProperty]
          *       if (property == null) {
          *         property = [get property from store]
          *         [cacheProperty]
          *       }
          *     }
          *   }
          * }}}
          */
        val getAndCacheProperty = oneTime(
          block(
            declareAndAssign(typeRefOf[Value], variableName, noValue),
            condition(notEqual(entityId, constant(-1L)))(
              checkPropertyTxState(
                  block(
                    assign(variableName,
                           getCachedPropertyAt(property,
                                               invoke(DB_ACCESS, propertyGet, entityId, constant(token), cursor, PROPERTY_CURSOR, constant(true))))),
                    )
                  )
                )
              )
        val ops = block(getAndCacheProperty, load(variableName))
        val nullChecks = block(getAndCacheProperty, equal(load(variableName), noValue))
        Some(IntermediateExpression(ops, Seq.empty, Seq(cursorVar, vPROPERTY_CURSOR), Set(nullChecks),
                                    requireNullCheck = false))
      }

    case NodePropertyLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,  block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
          getNodeProperty(loadField(f), offset))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq(f), Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks), requireNullCheck = false))

    case property@SlottedCachedPropertyWithoutPropertyToken(_, _, offset, offsetIsForLongSlot, propKey, cachedPropertyOffset, entityType) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val entityId = getEntityId(offsetIsForLongSlot, offset, entityType)
      val (propertyGet, txStatePropertyGet, cursor, cursorVar) = callPropertyGet(entityType)
      def checkPropertyTxState(continuation: IntermediateRepresentation): IntermediateRepresentation = {
        if (readOnly) continuation
        else {
          block(
            assign(variableName, invoke(DB_ACCESS, txStatePropertyGet, entityId, loadField(f))),
            condition(isNull(load(variableName)))(continuation))
        }
      }

      /**
        * {{{
        *   var property = NO_VALUE
        *   if (id != -1L && prop != -1) {
        *     property = [get property from tx state]
        *     if (property == null) {
        *       property = [getCachedProperty]
        *       if (property == null) {
        *         property = [get property from store]
        *         [cacheProperty]
        *       }
        *     }
        *   }
        * }}}
        */
      val getAndCacheProperty =
        block(
          declareAndAssign(typeRefOf[Value], variableName, noValue),
          condition(and(notEqual(entityId, constant(NO_SUCH_NODE)), notEqual(loadField(f), constant(-1))))(
            checkPropertyTxState(
                block(
                  assign(variableName,
                         getCachedPropertyAt(property,
                                             invoke(DB_ACCESS, propertyGet, entityId, loadField(f), cursor, PROPERTY_CURSOR, constant(true)))),
                  )
                )
              )
            )


      val lazySet =
        oneTime(
          block(
            condition(equal(loadField(f), constant(-1)))(
              setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(propKey)))),
            getAndCacheProperty))
      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq(f), Seq(cursorVar, vPROPERTY_CURSOR), Set(nullChecks), requireNullCheck = false))

    case NodePropertyExists(offset, token, _) =>
      Some(
        IntermediateExpression(
          ternary(hasNodeProperty(constant(token), offset), trueValue, falseValue),
          Seq.empty, Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case NodePropertyExistsLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(hasNodeProperty(loadField(f), offset), trueValue, falseValue)),
        Seq(f), Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case RelationshipProperty(offset, token, _) =>
      val variableName = namer.nextVariableName()
      val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName,
        getRelationshipProperty(constant(token), offset)))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set(nullChecks), requireNullCheck = false))

    case RelationshipPropertyLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val lazySet = oneTime(declareAndAssign(typeRefOf[Value], variableName, block(
        condition(equal(loadField(f), constant(-1)))(
          setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        getRelationshipProperty(loadField(f), offset))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))

      Some(IntermediateExpression(ops, Seq(f), Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set(nullChecks), requireNullCheck = false))

    case RelationshipPropertyExists(offset, token, _) =>
      Some(IntermediateExpression(
        ternary(
          hasRelationshipProperty(constant(token), offset), trueValue, falseValue),
        Seq.empty, Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set.empty)
      )

    case RelationshipPropertyExistsLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(
          hasRelationshipProperty(loadField(f), offset), trueValue, falseValue)),
        Seq(f), Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case HasLabelsFromSlot(offset, resolvedLabelTokens, lateLabels) if resolvedLabelTokens.nonEmpty || lateLabels.nonEmpty =>
      val (tokenFields, inits) = tokenFieldsForLabels(lateLabels)

      val predicate: IntermediateRepresentation = ternary(
        (resolvedLabelTokens.map { tokenId => isLabelSetOnNode(constant(tokenId), offset)
        } ++
        tokenFields.map { tokenField => isLabelSetOnNode(loadField(tokenField), offset)
        }).reduceLeft(and), trueValue, falseValue)

      Some(IntermediateExpression(block(inits :+ predicate:_*),
        tokenFields, Seq(vNODE_CURSOR), Set.empty, requireNullCheck = false))

    case HasLabels(nodeExpression, labels) if labels.nonEmpty =>
      for (node <- intermediateCompileExpression(nodeExpression)) yield {
        val (tokenFields, inits) = tokenFieldsForLabels(labels.map(_.name))

        val predicate: IntermediateRepresentation = ternary(tokenFields.map { token =>
          invokeStatic(method[CypherFunctions, Boolean, AnyValue, Int, DbAccess, NodeCursor]("hasLabel"),
                       node.ir, loadField(token), DB_ACCESS, NODE_CURSOR)
        }.reduceLeft(and), trueValue, falseValue)

        IntermediateExpression(block(inits :+ predicate:_*),
                               node.fields ++ tokenFields, node.variables :+ vNODE_CURSOR, node.nullChecks)
      }

    case NodeFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset)),
        Seq.empty, Seq.empty, Set.empty))

    case RelationshipFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"),
               getLongAt(offset)), Seq.empty, Seq.empty, Set.empty))

    case GetDegreePrimitive(offset, typ, dir) =>
      val methodName = dir match {
        case SemanticDirection.OUTGOING => "nodeGetOutgoingDegree"
        case SemanticDirection.INCOMING => "nodeGetIncomingDegree"
        case SemanticDirection.BOTH => "nodeGetTotalDegree"
      }
      typ match {
        case None =>
          Some(
            IntermediateExpression(
              invokeStatic(method[Values, IntValue, Int]("intValue"),
                           invoke(DB_ACCESS, method[DbAccess, Int, Long, NodeCursor](methodName), getLongAt(offset), NODE_CURSOR)),
              Seq.empty, Seq(vNODE_CURSOR), Set.empty))

        case Some(t) =>
          val f = field[Int](namer.nextVariableName(), constant(-1))
          Some(
            IntermediateExpression(
              block(
                condition(equal(loadField(f), constant(-1)))(
                  setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("relationshipType"), constant(t)))),
                invokeStatic(method[Values, IntValue, Int]("intValue"),
                           invoke(DB_ACCESS, method[DbAccess, Int, Long, Int, NodeCursor](methodName),
                                  getLongAt(offset), loadField(f), NODE_CURSOR))
              ), Seq(f), Seq(vNODE_CURSOR), Set.empty))
      }

    case f@ResolvedFunctionInvocation(name, Some(signature), args) if !f.isAggregate =>
      val inputArgs = args.map(Some(_))
        .zipAll(signature.inputSignature.map(_.default.map(_.value)), None, None).flatMap {
        case (Some(given), _) => intermediateCompileExpression(given)
        case (_, Some(default)) =>
          val constant = staticConstant[AnyValue](namer.nextVariableName().toUpperCase(), asAnyValue(default))
          Some(IntermediateExpression(getStatic[AnyValue](constant.name), Seq(constant), Seq.empty, Set.empty))
        case _ => None
      }
      if (inputArgs.size != signature.inputSignature.size) None
      else {
        val variableName = namer.nextVariableName()
        val allowed = staticConstant[Array[String]](namer.nextVariableName(), signature.allowed)
        val fields = inputArgs.flatMap(_.fields) :+ allowed
        val variables = inputArgs.flatMap(_.variables)
        val ops = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName, invoke(
          DB_ACCESS,
          method[DbAccess, AnyValue, Int, Array[AnyValue], Array[String]]("callFunction"),
          constant(signature.id),
          arrayOf[AnyValue](inputArgs.map(_.ir): _*),
          getStatic[Array[String]](allowed.name))))

        Some(IntermediateExpression(block(ops, load(variableName)), fields, variables,
                                    Set(block(ops, equal(load(variableName), noValue))), requireNullCheck = false))
      }


    case PathExpression(steps) =>

      @tailrec
      def isStaticallyKnown(step: PathStep): Boolean = step match {
        case NodePathStep(_, next) => isStaticallyKnown(next)
        case SingleRelationshipPathStep(_, _, _,next) => isStaticallyKnown(next)
        case _: MultiRelationshipPathStep => false
        case NilPathStep => true
      }

      if (isStaticallyKnown(steps)) compileStaticPath(steps)
      else compileDynamicPath(steps)

    //slotted operations
    case ReferenceFromSlot(offset, name) =>
      //NOTE we must call getRefAt before using it in the nullcheck because of how getRefAt might be extended
      //and have side effects, if it happens that we never use the nullcheck later things may not have been properly
      //initialized
      val loadRef = getRefAt(offset)
      val nullCheck = slots.get(name).filter(_.nullable).map(_ => equal(getRefAt(offset), noValue)).toSet
      Some(IntermediateExpression(loadRef, Seq.empty, Seq.empty, nullCheck, requireNullCheck = false))

    case IdFromSlot(offset) =>
      val nameOfSlot = slots.nameOfLongSlot(offset)
      val nullCheck = nameOfSlot.filter(n => slots(n).nullable).map(_ => equal(getLongAt(offset), constant(-1L))).toSet
      val value = invokeStatic(method[Values, LongValue, Long]("longValue"), getLongAt(offset))

      Some(IntermediateExpression(value, Seq.empty, Seq.empty, nullCheck))

    case LabelsFromSlot(offset) =>
      val nameOfSlot = slots.nameOfLongSlot(offset)
      val nullCheck = nameOfSlot.filter(n => slots(n).nullable).map(_ => equal(getLongAt(offset), constant(-1L))).toSet

      val value = invoke(DB_ACCESS, method[DbAccess, ListValue, Long, NodeCursor]("getLabelsForNode"), getLongAt(offset), NODE_CURSOR)

      Some(IntermediateExpression(value, Seq.empty, Seq(vNODE_CURSOR), nullCheck))

    case RelationshipTypeFromSlot(offset) =>
      val nameOfSlot = slots.nameOfLongSlot(offset)
      val nullCheck = nameOfSlot.filter(n => slots(n).nullable).map(_ => equal(getLongAt(offset), constant(-1L))).toSet

      nameOfSlot.flatMap(cursorFor) match {
        case Some(cursor) =>
          val value = invokeStatic(method[Values, TextValue, String]("stringValue"),
          invoke(DB_ACCESS, method[DbAccess, String, Int]("relationshipTypeName"), cursor.relationshipType))
          Some(IntermediateExpression(value, Seq.empty, Seq.empty, nullCheck))
        case None =>
          val value = invoke(DB_ACCESS,
                             method[DbAccess, TextValue, Long, RelationshipScanCursor]("getTypeForRelationship"),
                             getLongAt(offset), RELATIONSHIP_CURSOR)
          Some(IntermediateExpression(value, Seq.empty, Seq(vRELATIONSHIP_CURSOR), nullCheck))
      }

    case PrimitiveEquals(lhs, rhs) =>
      for {l <- intermediateCompileExpression(lhs)
           r <- intermediateCompileExpression(rhs)
      } yield
        IntermediateExpression(
          ternary(invoke(l.ir, method[AnyValue, Boolean, AnyRef]("equals"), r.ir), trueValue, falseValue),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullChecks ++ r.nullChecks)

    case NullCheck(offset, inner) =>
      intermediateCompileExpression(inner).map(i => i.copy(nullChecks = i.nullChecks + equal(getLongAt(offset), constant(-1L)), requireNullCheck = true))

    case NullCheckVariable(offset, inner) =>
      intermediateCompileExpression(inner).map(i => i.copy(nullChecks = i.nullChecks + equal(getLongAt(offset), constant(-1L)), requireNullCheck = true))

    case NullCheckProperty(offset, inner) =>
      intermediateCompileExpression(inner).map(i => i.copy(nullChecks = i.nullChecks + equal(getLongAt(offset), constant(-1L)), requireNullCheck = true))

    case IsPrimitiveNull(offset) =>
      Some(IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), trueValue, falseValue),
                                  Seq.empty, Seq.empty, Set.empty))

    case _ => None
  }

  private def tokenFieldsForLabels(labels: Seq[String]): (Seq[InstanceField], Seq[IntermediateRepresentation]) = {
    val tokensAndInits = labels.map(label => {
      val tokenField = field[Int](namer.variableName(label), constant(-1))
      val init = condition(equal(loadField(tokenField), constant(-1)))(setField(tokenField,
        invoke(DB_ACCESS, method[DbAccess, Int, String]("nodeLabel"), constant(label))))
      (tokenField, init)
    })
    tokensAndInits.unzip
  }

  def compileFunction(c: FunctionInvocation): Option[IntermediateExpression] = c.function match {
    case functions.Acos =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("acos"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Cos =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Cot =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cot"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Asin =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("asin"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Haversin =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("haversin"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Sin =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Atan =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("atan"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Atan2 =>
      for {y <- intermediateCompileExpression(c.args(0))
           x <- intermediateCompileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, DoubleValue, AnyValue, AnyValue]("atan2"), y.ir, x.ir),
          y.fields ++ x.fields, y.variables ++ x.variables, y.nullChecks ++ x.nullChecks)
      }
    case functions.Tan =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("tan"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Round =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("round"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Rand =>
      Some(IntermediateExpression(invokeStatic(method[CypherFunctions, DoubleValue]("rand")),
                                  Seq.empty, Seq.empty, Set.empty))
    case functions.Abs =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NumberValue, AnyValue]("abs"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Ceil =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("ceil"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Floor =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("floor"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Degrees =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toDegrees"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Exp =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("exp"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Log =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Log10 =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log10"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Radians =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toRadians"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Sign =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("signum"), in.ir), in.fields, in.variables, in.nullChecks))
    case functions.Sqrt =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sqrt"), in.ir), in.fields, in.variables, in.nullChecks))

    case functions.Range  if c.args.length == 2 =>
      for {start <- intermediateCompileExpression(c.args(0))
           end <- intermediateCompileExpression(c.args(1))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("range"),
                                                  nullCheckIfRequired(start), nullCheckIfRequired(end)),
                                     start.fields ++ end.fields,
                                     start.variables ++ end.variables, Set.empty)

    case functions.Range  if c.args.length == 3 =>
      for {start <- intermediateCompileExpression(c.args(0))
           end <- intermediateCompileExpression(c.args(1))
           step <- intermediateCompileExpression(c.args(2))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("range"),
                                                  nullCheckIfRequired(start), nullCheckIfRequired(end), nullCheckIfRequired(step)),
                                     start.fields ++ end.fields ++ step.fields,
                                     start.variables ++ end.variables ++ step.variables, Set.empty)

    case functions.Pi => Some(IntermediateExpression(getStatic[Values, DoubleValue]("PI"), Seq.empty, Seq.empty, Set.empty))
    case functions.E => Some(IntermediateExpression(getStatic[Values, DoubleValue]("E"), Seq.empty, Seq.empty, Set.empty))

    case functions.Coalesce =>
      val args = c.args.flatMap(intermediateCompileExpression)
      if (args.size < c.args.size) None
      else {
        val tempVariable = namer.nextVariableName()
        val local = variable[AnyValue](tempVariable, noValue)
        // This loop will generate:
        // AnyValue tempVariable = arg0;
        //if (tempVariable == NO_VALUE) {
        //  tempVariable = arg1;
        //  if ( tempVariable == NO_VALUE) {
        //    tempVariable = arg2;
        //  ...
        //}
        def loop(expressions: List[IntermediateExpression]): IntermediateRepresentation = expressions match {
          case Nil => throw new InternalException("we should never exhaust this loop")
          case expression :: Nil => assign(tempVariable, nullCheckIfRequired(expression))
          case expression :: tail =>
            //tempVariable = hd; if (tempVariable == NO_VALUE){[continue with tail]}
            if (expression.nullChecks.nonEmpty) block(assign(tempVariable, nullCheckIfRequired(expression)),
                                                      condition(expression.nullChecks.reduceLeft((acc, current) => or(acc, current)))(loop(tail)))
            // WHOAH[Keanu Reeves voice] if not nullable we don't even need to generate code for the coming expressions,
            else assign(tempVariable, expression.ir)
        }
        val repr = block(loop(args.toList),
                          load(tempVariable))

        Some(IntermediateExpression(repr, args.foldLeft(Seq.empty[Field])((a,b) => a ++ b.fields),
                                    args.foldLeft(Seq.empty[LocalVariable])((a,b) => a ++ b.variables) :+ local,
                                    Set(equal(load(tempVariable), noValue))))
      }

    case functions.Distance =>
      for {p1 <- intermediateCompileExpression(c.args(0))
           p2 <- intermediateCompileExpression(c.args(1))
      } yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                       invokeStatic(method[CypherFunctions, Value, AnyValue, AnyValue]("distance"), p1.ir, p2.ir)))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, p1.fields ++ p2.fields,  p1.variables ++ p2.variables, Set(nullChecks))
      }

    case functions.StartNode =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess, RelationshipScanCursor]("startNode"),
                     in.ir, DB_ACCESS, RELATIONSHIP_CURSOR), in.fields, in.variables :+ vRELATIONSHIP_CURSOR, in.nullChecks))

    case functions.EndNode =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess, RelationshipScanCursor]("endNode"), in.ir,
                                      DB_ACCESS, RELATIONSHIP_CURSOR), in.fields, in.variables :+ vRELATIONSHIP_CURSOR, in.nullChecks))

    case functions.Nodes =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue]("nodes"), in.ir), in.fields, in.variables, in.nullChecks))

    case functions.Relationships =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue]("relationships"), in.ir), in.fields, in.variables, in.nullChecks))

    case functions.Exists =>
      c.arguments.head match {
        case property: Property =>
          intermediateCompileExpression(property.map).map(in => IntermediateExpression(
            invokeStatic(method[CypherFunctions, BooleanValue, String, AnyValue, DbAccess,
                                  NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyExists"),
                           constant(property.propertyKey.name),
                           in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR ),
            in.fields, in.variables ++ vCURSORS, in.nullChecks))

        case property: ASTCachedProperty => cachedExists(property)
        case _: PatternExpression => None//TODO
        case _: NestedPipeExpression => None//TODO?
        case _: NestedPlanExpression => throw new InternalException("should have been rewritten away")
        case _ => None
      }

    case functions.Head =>
      intermediateCompileExpression(c.args.head).map(in => {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                               noValueOr(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("head"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables, Set(nullChecks), requireNullCheck = false)
      })

    case functions.Id =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("id"), in.ir),
        in.fields, in.variables, in.nullChecks))

    case functions.Labels =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess, NodeCursor]("labels"), in.ir, DB_ACCESS, NODE_CURSOR),
        in.fields, in.variables :+ vNODE_CURSOR, in.nullChecks))

    case functions.Type =>
      intermediateCompileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, TextValue, AnyValue]("type"), in.ir),
        in.fields, in.variables, in.nullChecks))

    case functions.Last =>
      intermediateCompileExpression(c.args.head).map(in => {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                               noValueOr(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("last"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables, Set(nullChecks), requireNullCheck = false)
      })

    case functions.Left =>
      for {in <- intermediateCompileExpression(c.args(0))
           endPos <- intermediateCompileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("left"), in.ir, endPos.ir),
          in.fields ++ endPos.fields, in.variables ++ endPos.variables, in.nullChecks)
      }

    case functions.LTrim =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("ltrim"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.RTrim =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("rtrim"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.Trim =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("trim"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.Replace =>
      for {original <- intermediateCompileExpression(c.args(0))
           search <- intermediateCompileExpression(c.args(1))
           replaceWith <- intermediateCompileExpression(c.args(2))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("replace"),
                         original.ir, search.ir, replaceWith.ir),
          original.fields ++ search.fields ++ replaceWith.fields, original.variables ++ search.variables ++ replaceWith.variables,
          original.nullChecks ++ search.nullChecks ++ replaceWith.nullChecks)
      }

    case functions.Reverse =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("reverse"), in.ir), in.fields, in.variables, in.nullChecks)
      }

    case functions.Right =>
      for {in <- intermediateCompileExpression(c.args(0))
           len <- intermediateCompileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("right"), in.ir, len.ir),
          in.fields ++ len.fields, in.variables ++ len.variables, in.nullChecks)
      }

    case functions.Split =>
      for {original <- intermediateCompileExpression(c.args(0))
           sep <- intermediateCompileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("split"), original.ir, sep.ir),
          original.fields ++ sep.fields, original.variables ++ sep.variables,
          original.nullChecks ++ sep.nullChecks)
      }

    case functions.Substring if c.args.size == 2 =>
      for {original <- intermediateCompileExpression(c.args(0))
           start <- intermediateCompileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("substring"), original.ir, start.ir),
          original.fields ++ start.fields, original.variables ++ start.variables, original.nullChecks)
      }

    case functions.Substring  =>
      for {original <- intermediateCompileExpression(c.args(0))
           start <- intermediateCompileExpression(c.args(1))
           len <- intermediateCompileExpression(c.args(2))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("substring"),
                                              original.ir, start.ir, len.ir),
          original.fields ++ start.fields ++ len.fields,
          original.variables ++ start.variables ++ len.variables, original.nullChecks)
      }

    case functions.ToLower =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toLower"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.ToUpper =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toUpper"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.Point =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, Value, AnyValue, DbAccess, ExpressionCursors]("point"),
            in.ir, DB_ACCESS, CURSORS),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.Keys =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("keys"),
            in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
          in.fields, in.variables ++ vCURSORS, in.nullChecks)
      }

    case functions.Size =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("size"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.Length =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("length"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.Tail =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue]("tail"), in.ir),
          in.fields, in.variables, in.nullChecks)
      }

    case functions.ToBoolean =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                               noValueOr(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toBoolean"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables, Set(nullChecks), requireNullCheck = false)
      }

    case functions.ToFloat =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                               noValueOr(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toFloat"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables, Set(nullChecks), requireNullCheck = false)
      }

    case functions.ToInteger =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                               noValueOr(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toInteger"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables, Set(nullChecks), requireNullCheck = false)
      }

    case functions.ToString =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        IntermediateExpression(invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toString"), in.ir), in.fields, in.variables, in.nullChecks)
      }

    case functions.Properties =>
      for (in <- intermediateCompileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName, noValueOr(in)(invokeStatic(
          method[CypherFunctions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("properties"),
          in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables ++ vCURSORS, Set(nullChecks), requireNullCheck = false)
      }

    case _ =>
      None
  }

  def intermediateCompileProjection(projections: Map[String, Expression]): Option[IntermediateExpression] = {
    val removed = projections.keys.count(slots(_).isLongSlot)
    val compiled = for {(k, v) <- projections
                        c <- intermediateCompileExpression(v) if !slots(k).isLongSlot}
      yield slots(k).offset -> c
    if (compiled.size + removed < projections.size) None
    else {
      val all = compiled.toSeq.map {
        case (slot, value) => setRefAt(slot, nullCheckIfRequired(value))
      }
      Some(IntermediateExpression(block(all: _*), compiled.values.flatMap(_.fields).toSeq,
                             compiled.values.flatMap(_.variables).toSeq, Set.empty))
    }
  }

  private def intermediateCompileGroupingExpression(orderedGroupings: Seq[(Slot, IntermediateExpression)]): IntermediateGroupingExpression = {
    require(orderedGroupings.nonEmpty)
    val listVar = namer.nextVariableName()
    val singleValue = orderedGroupings.size == 1
    def id[T](value: IntermediateRepresentation, nullable: Boolean)(implicit m: Manifest[T]) =  {
      val getId = invoke(cast[T](value), method[T, Long]("id"))
      if (!nullable) getId
      else ternary(equal(value, noValue), constant(-1L), getId)
    }

    def accessValue(i: Int) =  {
      if (singleValue) load("key")
      else invoke(load(listVar), method[ListValue, AnyValue, Int]("value"), constant(i))
    }

    val projectKeyOps = orderedGroupings.map(_._1).zipWithIndex.map {
      case (LongSlot(offset, nullable, CTNode), index) =>
        setLongAt(offset, id[VirtualNodeValue](accessValue(index), nullable))
      case (LongSlot(offset, nullable, CTRelationship), index) =>
        setLongAt(offset, id[VirtualRelationshipValue](accessValue(index), nullable))
      case (RefSlot(offset, _, _), index) =>
        setRefAt(offset, accessValue(index))
      case slot =>
        throw new InternalException(s"Do not know how to make setter for slot $slot")
    }
    val projectKey =
      if (singleValue) projectKeyOps
      else Seq(declare[ListValue](listVar), assign(listVar, cast[ListValue](load("key")))) ++ projectKeyOps

    val getKeyOps = orderedGroupings.map(_._1).map {
      case LongSlot(offset, nullable, CTNode) =>
        val getter = invokeStatic(method[VirtualValues, NodeReference, Long]("node"), getLongAt(offset))
        if (nullable) ternary(equal(getLongAt(offset), constant(-1L)), noValue, getter)
        else getter
      case LongSlot(offset, nullable, CTRelationship) =>
        val getter = invokeStatic(method[VirtualValues, RelationshipReference, Long]("relationship"), getLongAt(offset))
        if (nullable) ternary(equal(getLongAt(offset), constant(-1L)), noValue, getter)
        else getter
      case RefSlot(offset, _, _) => getRefAt(offset)
      case slot =>
        throw new InternalException(s"Do not know how to make getter for slot $slot")
    }

    val getKey =
      if (singleValue) getKeyOps.head
      else invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf[AnyValue](getKeyOps:_*))

    val computeKeyOps = orderedGroupings.map(_._2).map(p => nullCheckIfRequired(p)).toArray
    val computeKey =
      if (singleValue) computeKeyOps.head
      else invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf[AnyValue](computeKeyOps:_*))

    val orderedGroupingExpressions = orderedGroupings.map(_._2)
    IntermediateGroupingExpression(
      IntermediateExpression(block(projectKey:_*), Seq.empty, Seq.empty, Set.empty),
      IntermediateExpression(computeKey, orderedGroupingExpressions.flatMap(_.fields), orderedGroupingExpressions.flatMap(_.variables), Set.empty),
      IntermediateExpression(getKey, Seq.empty, Seq.empty, Set.empty))
  }

  //Extension points
  protected def getLongAt(offset: Int): IntermediateRepresentation

  protected def getRefAt(offset: Int): IntermediateRepresentation

  protected def setRefAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation

  protected def setLongAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation

  protected def setCachedPropertyAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation

  protected def getCachedPropertyAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): IntermediateRepresentation

  protected def isLabelSetOnNode(labelToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation

  protected def getNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation

  protected def hasNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation

  protected def getRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation

  protected def hasRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation

  protected def getProperty(key: String, container: IntermediateRepresentation): IntermediateRepresentation

  /**
    * Gives the cursor that currently is used for the given variable name or `None`
    * @param name the variable name of the node or relationship the cursor is used for
    */
  def cursorFor(name: String): Option[CursorRepresentation] = None

  def getArgumentAt(offset: Int): IntermediateRepresentation =
    if (offset == TopLevelArgument.SLOT_OFFSET) {
      constant(0L)
    } else {
      getLongAt(offset)
    }

  final def getLongFromExecutionContext(offset: Int, context: IntermediateRepresentation = LOAD_CONTEXT): IntermediateRepresentation =
    invoke(context, method[ExecutionContext, Long, Int]("getLongAt"), constant(offset))

  final def getRefFromExecutionContext(offset: Int, context: IntermediateRepresentation = LOAD_CONTEXT): IntermediateRepresentation =
    invoke(context, method[ExecutionContext, AnyValue, Int]("getRefAt"), constant(offset))

  final def getCachedPropertyFromExecutionContext(offset: Int, context: IntermediateRepresentation = LOAD_CONTEXT): IntermediateRepresentation =
    invoke(context, method[ExecutionContext, Value, Int]("getCachedPropertyAt"), constant(offset))

  final def setRefInExecutionContext(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(LOAD_CONTEXT, method[ExecutionContext, Unit, Int, AnyValue]("setRefAt"),
                     constant(offset), value)

   final def setLongInExecutionContext(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(LOAD_CONTEXT, method[ExecutionContext, Unit, Int, Long]("setLongAt"),
                     constant(offset), value)

  protected final def setCachedPropertyInExecutionContext(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(LOAD_CONTEXT, method[ExecutionContext, Unit, Int, Value]("setCachedPropertyAt"),
                     constant(offset), value)

  //==================================================================================================

  private def coerceToPredicate(e: IntermediateExpression) = IntermediateExpression(
    invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), e.ir), e.fields, e.variables, e.nullChecks)

  /**
    * Ok AND and ANDS are complicated.  At the core we try to find a single `FALSE` if we find one there is no need to look
    * at more predicates. If it doesn't find a `FALSE` it will either return `NULL` if any of the predicates has evaluated
    * to `NULL` or `TRUE` if all predicates evaluated to `TRUE`.
    *
    * For example:
    * - AND(FALSE, NULL) -> FALSE
    * - AND(NULL, FALSE) -> FALSE
    * - AND(TRUE, NULL) -> NULL
    * - AND(NULL, TRUE) -> NULL
    *
    * Errors are an extra complication here, errors are treated as `NULL` except that we will throw an error instead of
    * returning `NULL`, so for example:
    *
    * - AND(FALSE, 42) -> FALSE
    * - AND(42, FALSE) -> FALSE
    * - AND(TRUE, 42) -> throw type error
    * - AND(42, TRUE) -> throw type error
    *
    * The generated code below will look something like;
    *
    * RuntimeException error = null;
    * boolean seenNull = false;
    * Value returnValue = null;
    * try
    * {
    *   returnValue = [expressions.head];
    * }
    * catch( RuntimeException e)
    * {
    *   error = e;
    * }
    * seenNull = returnValue == NO_VALUE;
    * if ( returnValue != FALSE )
    * {
    *    try
    *    {
    *      returnValue = expressions.tail.head;
    *    }
    *    catch( RuntimeException e)
    *    {
    *      error = e;
    *    }
    *    seenValue = returnValue == FALSE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *    if ( returnValue != FALSE )
    *    {
    *       try
    *       {
    *         returnValue = expressions.tail.tail.head;
    *       }
    *       catch( RuntimeException e)
    *       {
    *         error = e;
    *       }
    *       seenValue = returnValue == FALSE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *       ...[continue unroll until we are at the end of expressions]
    *     }
    * }
    * if ( error != null && returnValue != FALSE )
    * {
    *   throw error;
    * }
    * return seenNull ? NO_VALUE : returnValue;
    */
  private def generateAnds(expressions: List[IntermediateExpression]) =
    generateCompositeBoolean(expressions, falseValue)

  /**
    * Ok OR and ORS are also complicated.  At the core we try to find a single `TRUE` if we find one there is no need to look
    * at more predicates. If it doesn't find a `TRUE` it will either return `NULL` if any of the predicates has evaluated
    * to `NULL` or `FALSE` if all predicates evaluated to `FALSE`.
    *
    * For example:
    * - OR(FALSE, NULL) -> NULL
    * - OR(NULL, FALSE) -> NULL
    * - OR(TRUE, NULL) -> TRUE
    * - OR(NULL, TRUE) -> TRUE
    *
    * Errors are an extra complication here, errors are treated as `NULL` except that we will throw an error instead of
    * returning `NULL`, so for example:
    *
    * - OR(TRUE, 42) -> TRUE
    * - OR(42, TRUE) -> TRUE
    * - OR(FALSE, 42) -> throw type error
    * - OR(42, FALSE) -> throw type error
    *
    * The generated code below will look something like;
    *
    * RuntimeException error = null;
    * boolean seenNull = false;
    * Value returnValue = null;
    * try
    * {
    *   returnValue = [expressions.head];
    * }
    * catch( RuntimeException e)
    * {
    *   error = e;
    * }
    * seenNull = returnValue == NO_VALUE;
    * if ( returnValue != TRUE )
    * {
    *    try
    *    {
    *      returnValue = expressions.tail.head;
    *    }
    *    catch( RuntimeException e)
    *    {
    *      error = e;
    *    }
    *    seenValue = returnValue == TRUE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *    if ( returnValue != TRUE )
    *    {
    *       try
    *       {
    *         returnValue = expressions.tail.tail.head;
    *       }
    *       catch( RuntimeException e)
    *       {
    *         error = e;
    *       }
    *       seenValue = returnValue == TRUE ? false : (seenValue ? true : returnValue == NO_VALUE);
    *       ...[continue unroll until we are at the end of expressions]
    *     }
    * }
    * if ( error != null && returnValue != TRUE )
    * {
    *   throw error;
    * }
    * return seenNull ? NO_VALUE : returnValue;
    */
  private def generateOrs(expressions: List[IntermediateExpression]): IntermediateExpression =
    generateCompositeBoolean(expressions, trueValue)

  private def generateCompositeBoolean(expressions: List[IntermediateExpression], breakValue: IntermediateRepresentation): IntermediateExpression = {
    //do we need to do nullchecks
    val nullable = expressions.exists(_.nullChecks.nonEmpty)

    //these are the temp variables used
    val returnValue = namer.nextVariableName()
    val local = variable[AnyValue](returnValue, constant(null))
    val seenNull = namer.nextVariableName()
    val error = namer.nextVariableName()
    //this is setting up  a `if (returnValue != breakValue)`
    val ifNotBreakValue: IntermediateRepresentation => IntermediateRepresentation = condition(notEqual(load(returnValue), breakValue))
    //this is the inner block of the condition
    val inner = (e: IntermediateExpression) => {
      val exceptionName = namer.nextVariableName()
      val loadValue = tryCatch[RuntimeException](exceptionName)(assign(returnValue, noValueOr(e)(invokeStatic(ASSERT_PREDICATE, e.ir))))(
        assign(error, load(exceptionName)))

        if (nullable) {
          Seq(loadValue,
              assign(seenNull,
                     //returnValue == breakValue ? false :
                     ternary(equal(load(returnValue), breakValue), constant(false),
                             //seenNull ? true : (returnValue == NO_VALUE)
                             ternary(load(seenNull), constant(true), equal(load(returnValue), noValue)))))
        } else Seq(loadValue)
      }


    //this loop generates the nested expression:
    //if (returnValue != breakValue) {
    //  try {
    //    returnValue = ...;
    //  } catch ( RuntimeException e) { error = e}
    //  ...
    //  if (returnValue != breakValue ) {
    //    try {
    //        returnValue = ...;
    //    } catch ( RuntimeException e) { error = e}
    //    ...
    def loop(e: List[IntermediateExpression]): IntermediateRepresentation = e match {
      case Nil => throw new InternalException("we should never get here")
      case a :: Nil => ifNotBreakValue(block(inner(a):_*))
      case hd::tl => ifNotBreakValue(block(inner(hd) :+ loop(tl):_*))
    }

    val firstExpression = expressions.head
    val nullChecks = if (nullable) Seq(declare[Boolean](seenNull), assign(seenNull, constant(false))) else Seq.empty
    val nullCheckAssign = if (firstExpression.nullChecks.nonEmpty) Seq(assign(seenNull, equal(load(returnValue), noValue))) else Seq.empty
    val exceptionName = namer.nextVariableName()
    //otherwise check if we have seen a null which implicitly also mean we never seen a FALSE
    //if we seen a null we should return null otherwise we return whatever currently
    //stored in returnValue
    val actualReturnValue = if (nullable) ternary(load(seenNull), noValue, load(returnValue)) else load(returnValue)
    val ir =
      block(
        //set up all temp variables
        nullChecks ++ Seq(
          declare[RuntimeException](error),
          assign(error, constant(null)),
          //assign returnValue to head of expressions
          tryCatch[RuntimeException](exceptionName)(
            assign(returnValue, noValueOr(firstExpression)(invokeStatic(ASSERT_PREDICATE, firstExpression.ir))))(
            assign(error, load(exceptionName)))) ++ nullCheckAssign ++ Seq(
          //generated unrolls tail of expression
          loop(expressions.tail),
          //checks if there was an error and that we never evaluated to breakValue, if so throw
          condition(and(notEqual(load(error), constant(null)), notEqual(load(returnValue), breakValue)))(
            fail(load(error))),
          actualReturnValue): _*)
    IntermediateExpression(ir,
                           expressions.foldLeft(Seq.empty[Field])((a,b) => a ++ b.fields),
                           expressions.foldLeft(Seq.empty[LocalVariable])((a,b) => a ++ b.variables) :+ local,
                           Set(equal(load(returnValue), noValue)), requireNullCheck = false)
  }

  private def asNeoType(ct: CypherType): IntermediateRepresentation = ct match {
    case CTString => getStatic[Neo4jTypes, Neo4jTypes.TextType]("NTString")
    case CTInteger => getStatic[Neo4jTypes, Neo4jTypes.IntegerType]("NTInteger")
    case CTFloat => getStatic[Neo4jTypes, Neo4jTypes.FloatType]("NTFloat")
    case CTNumber =>  getStatic[Neo4jTypes, Neo4jTypes.NumberType]("NTNumber")
    case CTBoolean => getStatic[Neo4jTypes, Neo4jTypes.BooleanType]("NTBoolean")
    case l: ListType => invokeStatic(method[Neo4jTypes , Neo4jTypes.ListType, AnyType]("NTList"), asNeoType(l.innerType))
    case CTDateTime => getStatic[Neo4jTypes, Neo4jTypes.DateTimeType]("NTDateTime")
    case CTLocalDateTime => getStatic[Neo4jTypes, Neo4jTypes.LocalDateTimeType]("NTLocalDateTime")
    case CTDate => getStatic[Neo4jTypes, Neo4jTypes.DateType]("NTDate")
    case CTTime => getStatic[Neo4jTypes, Neo4jTypes.TimeType]("NTTime")
    case CTLocalTime => getStatic[Neo4jTypes, Neo4jTypes.LocalTimeType]("NTLocalTime")
    case CTDuration => getStatic[Neo4jTypes, Neo4jTypes.DurationType]("NTDuration")
    case CTPoint => getStatic[Neo4jTypes, Neo4jTypes.PointType]("NTPoint")
    case CTNode => getStatic[Neo4jTypes, Neo4jTypes.NodeType]("NTNode")
    case CTRelationship => getStatic[Neo4jTypes, Neo4jTypes.RelationshipType]("NTRelationship")
    case CTPath => getStatic[Neo4jTypes, Neo4jTypes.PathType]("NTPath")
    case CTGeometry => getStatic[Neo4jTypes, Neo4jTypes.GeometryType]("NTGeometry")
    case CTMap => getStatic[Neo4jTypes, Neo4jTypes.MapType]("NTMap")
    case CTAny => getStatic[Neo4jTypes, Neo4jTypes.AnyType]("NTAny")
  }

  private def caseExpression(returnVariable: String,
                             checks: Seq[IntermediateRepresentation],
                             loads: Seq[IntermediateRepresentation],
                             default: IntermediateRepresentation,
                             conditionToCheck: IntermediateRepresentation => IntermediateRepresentation) = {
    def loop(expressions: List[(IntermediateRepresentation, IntermediateRepresentation)]): IntermediateRepresentation = expressions match {
      case Nil => throw new IllegalStateException()
      case (toCheck, toLoad) :: Nil =>
        condition(conditionToCheck(toCheck)) {
          assign(returnVariable, toLoad)
        }
      case (toCheck, toLoad) :: tl =>
        ifElse(conditionToCheck(toCheck)) {
          assign(returnVariable, toLoad)
        } {
          loop(tl)
        }
    }

    block(oneTime(block(
      assign(returnVariable, constant(null)), // Since the expression may be executed in a loop of an operator, returnVariable needs to be reset to null in every iteration
      loop(checks.zip(loads).toList),
      condition(equal(load(returnVariable), constant(null)))(assign(returnVariable, default))
    )), load(returnVariable))
  }

  private def accessVariable(name: String): (IntermediateRepresentation, Option[IntermediateRepresentation]) = {

    def computeRepresentation(ir: IntermediateRepresentation, nullCheck: Option[IntermediateRepresentation], nullable: Boolean): (IntermediateRepresentation, Option[IntermediateRepresentation]) = {
        (ir, if (nullable) nullCheck else None)
      }

    slots.get(name) match {
      case Some(LongSlot(offset, nullable, CTNode)) =>
        computeRepresentation(ir = ternary(equal(getLongAt(offset), constant(-1L)), noValue,
                                        invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"),
                                               getLongAt(offset))),
                              nullCheck = Some(equal(getLongAt(offset), constant(-1L))),
                              nullable = nullable)
      case Some(LongSlot(offset, nullable, CTRelationship)) =>
        computeRepresentation(ir = ternary(equal(getLongAt(offset), constant(-1L)), noValue,
                                        invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"),
                                               getLongAt(offset))),
                              nullCheck = Some(equal(getLongAt(offset), constant(-1L))),
                              nullable = nullable)

      case Some(RefSlot(offset, nullable, _)) =>
        computeRepresentation(ir = getRefAt(offset), nullCheck = None, nullable = nullable)

      case _ =>
        computeRepresentation(ir =
                                invoke(LOAD_CONTEXT,
                                          method[ExecutionContext, AnyValue, String]("getByName"), constant(name)),
                              nullCheck = None, nullable = true)
    }
  }

  private def filterExpression(collectionExpression: Option[IntermediateExpression],
                               innerPredicate: Expression,
                               innerVariable: ExpressionVariable): Option[IntermediateExpression] = {
    /*
       ListValue list = [evaluate collection expression];
       ArrayList<AnyValue> filtered = new ArrayList<>();
       Iterator<AnyValue> listIterator = list.iterator();
       while( listIterator.hasNext() )
       {
           AnyValue currentValue = listIterator.next();
           expressionVariables[innerVarOffset] = currentValue;
           Value isFiltered = [result from inner expression]
           if (isFiltered == Values.TRUE)
           {
               filtered.add(currentValue);
           }
       }
       return VirtualValues.fromList(filtered);
      */
    val iterVariable = namer.nextVariableName()
    for {collection <- collectionExpression
         inner <- intermediateCompileExpression(innerPredicate)
    } yield {
      val listVar = namer.nextVariableName()
      val filteredVars = namer.nextVariableName()
      val currentValue = namer.nextVariableName()
      val isFiltered = namer.nextVariableName()
      val ops = Seq(
        // ListValue list = [evaluate collection expression];
        // ArrayList<AnyValue> filtered = new ArrayList<>();
        declare[ListValue](listVar),
        assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
        declare[java.util.ArrayList[AnyValue]](filteredVars),
        assign(filteredVars, newInstance(constructor[java.util.ArrayList[AnyValue]])),
        // Iterator<AnyValue> listIterator = list.iterator();
        // while( listIterator.hasNext() )
        // {
        //    AnyValue currentValue = listIterator.next();
        declare[java.util.Iterator[AnyValue]](iterVariable),
        assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
        loop(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext"))) {
          block(Seq(
            declare[AnyValue](currentValue),
            assign(currentValue,
                   cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
            // expressionVariables[innerVarOffset] = currentValue;
            setExpressionVariable(innerVariable, load(currentValue)),
            declare[Value](isFiltered),
            // Value isFiltered = [result from inner expression]
            assign(isFiltered, nullCheckIfRequired(inner)),
            // if (isFiltered == Values.TRUE)
            // {
            //    filtered.add(currentValue);
            // }
            condition(equal(load(isFiltered), trueValue))(
              invokeSideEffect(load(filteredVars), method[java.util.ArrayList[_], Boolean, Object]("add"),
                               load(currentValue))
            )): _*)
        },
        // }
        // return VirtualValues.fromList(extracted);
        invokeStatic(method[VirtualValues, ListValue, java.util.List[AnyValue]]("fromList"), load(filteredVars))
      )
      IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                             collection.variables ++ inner.variables,
                             collection.nullChecks)
    }
  }

  private def extractExpression(collectionExpression: Option[IntermediateExpression],
                                extractExpression: Expression,
                                innerVariable: ExpressionVariable): Option[IntermediateExpression] = {
    /*
        The generated code will be something along the line of:

        ListValue list = [evaluate collection expression];
        ArrayList<AnyValue> extracted = new ArrayList<>();
        for ( AnyValue currentValue : list ) {
            expressionVariables[innerVarOffset] = currentValue;
            extracted.add([result from inner expression]);
        }
        return VirtualValues.fromList(extracted);
       */
    val iterVariable = namer.nextVariableName()
    for {collection <- collectionExpression
         inner <- intermediateCompileExpression(extractExpression)
    } yield {
      val listVar = namer.nextVariableName()
      val extractedVars = namer.nextVariableName()
      val currentValue = namer.nextVariableName()
      val ops = Seq(
        // ListValue list = [evaluate collection expression];
        // ArrayList<AnyValue> extracted = new ArrayList<>();
        declare[ListValue](listVar),
        assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
        declare[java.util.ArrayList[AnyValue]](extractedVars),
        assign(extractedVars, newInstance(constructor[java.util.ArrayList[AnyValue]])),
        // Iterator<AnyValue> iter = list.iterator();
        // while (iter.hasNext) {
        //   AnyValue currentValue = iter.next();
        declare[java.util.Iterator[AnyValue]](iterVariable),
        assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
        loop(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext"))) {
          block(Seq(
            declare[AnyValue](currentValue),
            assign(currentValue,
                   cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
            // expressionVariables[innerVarOffset] = currentValue;
            setExpressionVariable(innerVariable, load(currentValue)),
            // extracted.add([result from inner expression]);
            invokeSideEffect(load(extractedVars), method[java.util.ArrayList[_], Boolean, Object]("add"),
                             nullCheckIfRequired(inner))): _*)
        },
        // }
        // return VirtualValues.fromList(extracted);
        invokeStatic(method[VirtualValues, ListValue, java.util.List[AnyValue]]("fromList"), load(extractedVars))
      )
      IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                             collection.variables ++ inner.variables,
                             collection.nullChecks)
    }
  }

  /**
    *
    * Used when compiling paths where the size of the path is known at compile time, e.g. (a)-[r1]->(b)<-[r2]-(c)-[r3]-(d)
    *
    * The generated code will do something along the line of
    *
    * return VirtualValues.path( new NodeValue[]{a, b, c, d), new RelationshipValue[]{r1, r2, r3};
    */
  private def compileStaticPath(steps: PathStep) = {

    @tailrec
    def compileSteps(step: PathStep,
                     nodeOps: Seq[IntermediateExpression] = ArrayBuffer.empty,
                     relOps: Seq[IntermediateExpression] = ArrayBuffer.empty): Option[(Seq[IntermediateExpression], Seq[IntermediateExpression])] = step match {
      case NodePathStep(node, next) => compileSteps(next, nodeOps ++ intermediateCompileExpression(node), relOps)

      case SingleRelationshipPathStep(relExpression, _, Some(targetExpression), next) =>
        (intermediateCompileExpression(relExpression), intermediateCompileExpression(targetExpression)) match {
        case (Some(rel), Some(target)) => compileSteps(next, nodeOps :+ target, relOps :+ rel)
        case _ => None
      }

      case SingleRelationshipPathStep(rel, SemanticDirection.BOTH, _, next) => intermediateCompileExpression(rel) match {
        case Some(compiled) =>
          val relVar = variable[RelationshipValue](namer.nextVariableName(), constant(null))
          val lazyRel = oneTime(assign(relVar.name, cast[RelationshipValue](compiled.ir)))
          val node = IntermediateExpression(
            block(lazyRel,
                  invokeStatic(method[CypherFunctions, NodeValue, VirtualRelationshipValue, DbAccess, VirtualNodeValue, RelationshipScanCursor]("otherNode"),
                               cast[RelationshipValue](load(relVar.name)), DB_ACCESS,  cast[NodeValue](nodeOps.last.ir), RELATIONSHIP_CURSOR)),
            compiled.fields, compiled.variables :+ relVar :+ vRELATIONSHIP_CURSOR, compiled.nullChecks ++ nodeOps.last.nullChecks)
          val rel = IntermediateExpression(
            block(lazyRel, load(relVar.name)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullChecks)

          compileSteps(next, nodeOps :+ node, relOps :+ rel)
        case None => None
      }

      case SingleRelationshipPathStep(rel, direction, _, next) => intermediateCompileExpression(rel) match {
        case Some(compiled) =>
          val methodName = if (direction == SemanticDirection.INCOMING) "startNode" else "endNode"
          val relVar = variable[RelationshipValue](namer.nextVariableName(), constant(null))
          val lazyRel = oneTime(assign(relVar.name, cast[RelationshipValue](compiled.ir)))
          val node = IntermediateExpression(
            block(lazyRel,
              invokeStatic(method[CypherFunctions, NodeValue, VirtualRelationshipValue, DbAccess, RelationshipScanCursor](methodName),
                           cast[RelationshipValue](load(relVar.name)), DB_ACCESS, RELATIONSHIP_CURSOR)),
            compiled.fields, compiled.variables :+ relVar :+ vRELATIONSHIP_CURSOR, compiled.nullChecks)
          val rel = IntermediateExpression(
            block(lazyRel, load(relVar.name)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullChecks)

          compileSteps(next, nodeOps :+ node, relOps :+ rel)
        case None => None
      }

      case MultiRelationshipPathStep(_, _, _, _) => throw new IllegalStateException("Cannot be used for static paths")
      case NilPathStep => Some((nodeOps, relOps))
    }

    for ((nodeOps, relOps) <- compileSteps(steps) ) yield {
      val variableName = namer.nextVariableName()
      val lazySet = oneTime(declareAndAssign(typeRefOf[AnyValue], variableName,
                                             noValueOr(nodeOps ++ relOps: _*)(
                                     invokeStatic(
                                       method[VirtualValues, PathValue, Array[NodeValue], Array[RelationshipValue]](
                                         "path"),
                                       arrayOf[NodeValue](nodeOps.map(n => cast[NodeValue](n.ir)): _*),
                                       arrayOf[RelationshipValue](relOps.map(r => cast[RelationshipValue](r.ir)): _*))
                                   )))

      val ops = block(lazySet, load(variableName))
      val nullChecks =
        if (nodeOps.forall(_.nullChecks.isEmpty) && relOps.forall(_.nullChecks.isEmpty)) Set.empty[IntermediateRepresentation]
        else Set(block(lazySet, equal(load(variableName), noValue)))

      IntermediateExpression(ops,
                             nodeOps.flatMap(_.fields) ++ relOps.flatMap(_.fields),
                             nodeOps.flatMap(_.variables) ++ relOps.flatMap(_.variables),
                             nullChecks, requireNullCheck = false)
    }
  }

  private def compileDynamicPath(steps: PathStep) = {

    val builderVar = namer.nextVariableName()
    @tailrec
    def compileSteps(step: PathStep, acc: Seq[IntermediateExpression] = ArrayBuffer.empty): Option[Seq[IntermediateExpression]] = step match {
      case NodePathStep(node, next) => intermediateCompileExpression(node) match {
        case Some(nodeOps) =>
          val addNode =
            if (nodeOps.nullChecks.isEmpty) invokeSideEffect(load(builderVar),
                                                             method[PathValueBuilder, Unit, NodeValue]("addNode"),
                                                             cast[NodeValue](nodeOps.ir))
            else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue]("addNode"),
                        nullCheckIfRequired(nodeOps))
          compileSteps(next, acc :+ nodeOps.copy(ir = addNode))
        case None => None
      }

      //For this case end node is known
      case SingleRelationshipPathStep(rel, direction, Some(targetExpression), next) =>
        (intermediateCompileExpression(rel), intermediateCompileExpression(targetExpression)) match {
        case (Some(relOps), Some(target)) =>
          //this will generate something like
          //builder.addNode(n);
          //builder.addRelationship(r);
          val addNodeAndRelationship =
              block(
                if (target.nullChecks.isEmpty) {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, NodeValue]("addNode"), cast[NodeValue](target.ir))
                } else {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue]("addNode"), nullCheckIfRequired(target))
                },
                if (relOps.nullChecks.isEmpty) {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, RelationshipValue]("addRelationship"), cast[RelationshipValue](relOps.ir))
                } else {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue]("addRelationship"),
                                   nullCheckIfRequired(relOps))
                }
              )

          compileSteps(next, acc :+ relOps.copy(ir = addNodeAndRelationship))
        case _ => None
      }

      //Here end node is not known and will require lookup
      case SingleRelationshipPathStep(rel, direction, _, next) =>  intermediateCompileExpression(rel) match {
        case Some(relOps) =>
          val methodName = direction match {
            case SemanticDirection.INCOMING => "addIncoming"
            case SemanticDirection.OUTGOING => "addOutgoing"
            case _ => "addUndirected"
          }

          val addRel =
            if (relOps.nullChecks.isEmpty) invokeSideEffect(load(builderVar),
                                                            method[PathValueBuilder, Unit, RelationshipValue](methodName),
                                                            cast[RelationshipValue](relOps.ir))
            else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue](methodName),
                        nullCheckIfRequired(relOps))
          compileSteps(next, acc :+ relOps.copy(ir = addRel))
        case None => None
      }
      case MultiRelationshipPathStep(rel, direction, maybeTarget, next) => intermediateCompileExpression(rel) match {
        case Some(relOps) =>
          val methodName = direction match {
            case SemanticDirection.INCOMING => "addMultipleIncoming"
            case SemanticDirection.OUTGOING => "addMultipleOutgoing"
            case _ => "addMultipleUndirected"
          }

          val addRels = maybeTarget.flatMap(t => intermediateCompileExpression(t)) match {
            case Some(target) =>
              if (relOps.nullChecks.isEmpty && target.nullChecks.isEmpty) {
                invokeSideEffect(load(builderVar),
                                 method[PathValueBuilder, Unit, ListValue, NodeValue](methodName),
                                 cast[ListValue](relOps.ir), cast[NodeValue](target.ir))
              } else {
                invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue, AnyValue](methodName),
                                 nullCheckIfRequired(relOps), noValueOr(target)(target.ir))
              }
            case None =>
              if (relOps.nullChecks.isEmpty) {
                invokeSideEffect(load(builderVar),
                                 method[PathValueBuilder, Unit, ListValue](methodName),
                                 cast[ListValue](relOps.ir))
              }
              else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue](methodName),
                                    nullCheckIfRequired(relOps))
          }
          compileSteps(next, acc :+ relOps.copy(ir = addRels))
        case None => None
      }
      case NilPathStep => Some(acc)
    }

    for (pathOps <- compileSteps(steps) ) yield {
      val variableName = namer.nextVariableName()
      val lazySet = oneTime(
        block(
          Seq(
            declare[PathValueBuilder](builderVar),
            assign(builderVar, newInstance(constructor[PathValueBuilder, DbAccess, RelationshipScanCursor], DB_ACCESS, RELATIONSHIP_CURSOR)))
            ++ pathOps.map(_.ir) :+ declareAndAssign(typeRefOf[AnyValue], variableName, invoke(load(builderVar), method[PathValueBuilder, AnyValue]("build"))): _*))


      val ops = block(lazySet, load(variableName))
      val nullChecks =
        if (pathOps.forall(_.nullChecks.isEmpty)) Set.empty[IntermediateRepresentation]
        else Set(block(lazySet, equal(load(variableName), noValue)))

      IntermediateExpression(ops,
                             pathOps.flatMap(_.fields),
                             pathOps.flatMap(_.variables) :+ vRELATIONSHIP_CURSOR,
                             nullChecks, requireNullCheck = false)
    }
  }

  private def setFromLiterals(literals: Seq[Literal]): util.HashSet[AnyValue] = {
    val set = new util.HashSet[AnyValue]()
    literals.foreach(l => set.add(ValueUtils.of(l.value)))
    set
  }

  private def cachedExists(property: ASTCachedProperty) = property match {
    case property@SlottedCachedPropertyWithPropertyToken(_, _, entityOffset, offsetIsForLongSlot, prop, _, entityType) =>
      if (prop == StatementConstants.NO_SUCH_PROPERTY_KEY) Some(
        IntermediateExpression(falseValue, Seq.empty, Seq.empty, Set.empty, requireNullCheck = false))
      else {
        val existsVariable = namer.nextVariableName()
        val propertyVariable = namer.nextVariableName()
        val entityId = getEntityId(offsetIsForLongSlot, entityOffset, entityType)
        val (propertyGet, txStateHasCachedProperty, cursor, cursorVar) = callPropertyExists(entityType)
        def checkPropertyTxState(onNoChanges: IntermediateRepresentation): IntermediateRepresentation = {
          val hasChanges = namer.nextVariableName()
          if (readOnly) onNoChanges
          else {
            block(
              declareAndAssign(typeRefOf[Optional[java.lang.Boolean]], hasChanges,
                               invoke(DB_ACCESS, txStateHasCachedProperty, entityId, constant(prop))),
              ifElse(invoke(load(hasChanges), method[Optional[_], Boolean]("isEmpty")))(onNoChanges)(
                assign(existsVariable,
                       ternary(unbox(cast[java.lang.Boolean](
                         invoke(load(hasChanges), method[Optional[_], Object]("get")))), trueValue, falseValue)))
              )
          }
        }

        /**
          * {{{
          *   var hasProperty = NO_VALUE
          *   if (id != -1L) {
          *     val hasChanges = [check if exists in tx state]
          *     if (hasChanges.isEmpty) {
          *       var property = [getCachedProperty]
          *       if (property == null) {
          *         property = [get property from store]
          *         [cacheProperty]
          *       }
          *       hasProperty = property == NO_VALUE ? FALSE : TRUE
          *     } else {
          *       hasProperty = hasChanges.get ? TRUE: FALSE
          *     }
          *   }
          * }}}
          */
        val getAndCacheProperty = oneTime(
          block(
            declareAndAssign(typeRefOf[Value], existsVariable, noValue),
            condition(notEqual(entityId, constant(-1L)))(
              checkPropertyTxState(
                block(
                  declareAndAssign(typeRefOf[Value], propertyVariable,
                                   getCachedPropertyAt(property,
                                                       invoke(DB_ACCESS, propertyGet, entityId, constant(prop), cursor,
                                                              PROPERTY_CURSOR, constant(true)))),
                  assign(existsVariable, ternary(equal(load(propertyVariable), noValue), falseValue, trueValue))
                  )
                )
              )
            )
          )
        val ops = block(getAndCacheProperty, load(existsVariable))
        val nullChecks = block(getAndCacheProperty, equal(load(existsVariable), noValue))
        Some(IntermediateExpression(ops, Seq.empty, Seq(cursorVar, vPROPERTY_CURSOR), Set(nullChecks),
                                    requireNullCheck = false))
      }

    case property@SlottedCachedPropertyWithoutPropertyToken(_, _, entityOffset, offsetIsForLongSlot, prop, _, entityType) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val existsVariable = namer.nextVariableName()
      val propertyVariable = namer.nextVariableName()
      val entityId = getEntityId(offsetIsForLongSlot, entityOffset, entityType)
      val (propertyGet, txStateHasCachedProperty, cursor, cursorVar) = callPropertyExists(entityType)
      def checkPropertyTxState(onNoChanges: IntermediateRepresentation): IntermediateRepresentation = {
        val hasChanges = namer.nextVariableName()
        if (readOnly) onNoChanges
        else {
          block(
            declareAndAssign(typeRefOf[Optional[java.lang.Boolean]], hasChanges,
                             invoke(DB_ACCESS, txStateHasCachedProperty, entityId, loadField(f))),
            ifElse(invoke(load(hasChanges), method[Optional[_], Boolean]("isEmpty")))(onNoChanges)(
              assign(existsVariable,
                     ternary(unbox(cast[java.lang.Boolean](
                       invoke(load(hasChanges), method[Optional[_], Object]("get")))), trueValue, falseValue)))
            )
        }
      }

      /**
        * {{{
        *   var hasProperty = NO_VALUE
        *   if (id != -1L) {
        *     val hasChanges = [check if exists in tx state]
        *     if (hasChanges.isEmpty) {
        *       var property = [getCachedProperty]
        *       if (property == null) {
        *         property = [get property from store]
        *         [cacheProperty]
        *       }
        *       hasProperty = property == NO_VALUE ? FALSE : TRUE
        *     } else {
        *       hasProperty = hasChanges.get ? TRUE: FALSE
        *     }
        *   }
        * }}}
        */
      val getAndCacheProperty = oneTime(
        block(
          declareAndAssign(typeRefOf[Value], existsVariable, noValue),
          condition(notEqual(entityId, constant(-1L)))(
            checkPropertyTxState(
                block(
                  declareAndAssign(typeRefOf[Value], propertyVariable,
                                   getCachedPropertyAt(property,
                                                       invoke(DB_ACCESS, propertyGet, entityId, loadField(f), cursor, PROPERTY_CURSOR, constant(true)))),
                  assign(existsVariable, ternary(equal(load(propertyVariable), noValue), falseValue, trueValue))
                  )
                )
              )
            )
          )

      val lazySet = oneTime(block(
        condition(equal(loadField(f), constant(-1)))(
          setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(prop)))),
        getAndCacheProperty))

      val ops = block(lazySet, load(existsVariable))
      val nullChecks = block(lazySet, equal(load(existsVariable), noValue))

      Some(IntermediateExpression(ops, Seq(f), Seq(cursorVar, vPROPERTY_CURSOR), Set(nullChecks), requireNullCheck = false))

    case _ => None
  }

  private def setExpressionVariable(ev: ExpressionVariable, value: IntermediateRepresentation): IntermediateRepresentation = {
    arraySet(load("expressionVariables"), ev.offset, value)
  }

  private def loadExpressionVariable(ev: ExpressionVariable): IntermediateRepresentation = {
    arrayLoad(load("expressionVariables"), ev.offset)
  }

  private def getEntityId(offsetIsForLongSlot: Boolean, offset: Int, entityType: EntityType): IntermediateRepresentation = {
    if (offsetIsForLongSlot) getLongAt(offset)
    else {
      val entityVar = namer.nextVariableName()
      val getId = entityType match {
        case NODE_TYPE => invoke(cast[VirtualNodeValue](getRefAt(offset)),
                                 method[VirtualNodeValue, Long]("id"))
        case RELATIONSHIP_TYPE => invoke(cast[VirtualRelationshipValue](getRefAt(offset)),
                                         method[VirtualRelationshipValue, Long]("id"))
      }
      block(
        oneTime(declareAndAssign(typeRefOf[Long], entityVar, getId)),
        load(entityVar))
    }
  }

  private def callPropertyGet(entityType: EntityType) = entityType match {
    case NODE_TYPE =>
      (NODE_PROPERTY, GET_TX_STATE_NODE_PROP, NODE_CURSOR, vNODE_CURSOR)
    case RELATIONSHIP_TYPE =>
      (RELATIONSHIP_PROPERTY, GET_TX_STATE_RELATIONSHIP_PROP, RELATIONSHIP_CURSOR, vRELATIONSHIP_CURSOR)
  }

  private def callPropertyExists(entityType: EntityType) = entityType match {
    case NODE_TYPE =>
      (NODE_PROPERTY, HAS_TX_STATE_NODE_PROP, NODE_CURSOR, vNODE_CURSOR)
    case RELATIONSHIP_TYPE =>
      (RELATIONSHIP_PROPERTY, HAS_TX_STATE_RELATIONSHIP_PROP, RELATIONSHIP_CURSOR, vRELATIONSHIP_CURSOR)
  }
}

object ExpressionCompiler {
  def defaultGenerator(slots: SlotConfiguration,
                       readOnly: Boolean,
                       codeGenerationMode: CodeGeneration.CodeGenerationMode,
                       namer: VariableNamer = new VariableNamer
                      ): ExpressionCompiler = new DefaultExpressionCompiler(slots, readOnly, codeGenerationMode, namer)

  private val COUNTER = new AtomicLong(0L)
  private val ASSERT_PREDICATE = method[CompiledHelpers, Value, AnyValue]("assertBooleanOrNoValue")
  val DB_ACCESS: IntermediateRepresentation = load("dbAccess")
  private val CURSORS = load("cursors")

  val NODE_CURSOR: IntermediateRepresentation = load("nodeCursor")
  val vNODE_CURSOR: LocalVariable = cursorVariable[NodeCursor]("nodeCursor")

  val RELATIONSHIP_CURSOR: IntermediateRepresentation = load("relationshipScanCursor")
  val vRELATIONSHIP_CURSOR: LocalVariable = cursorVariable[RelationshipScanCursor]("relationshipScanCursor")

  val PROPERTY_CURSOR: IntermediateRepresentation = load("propertyCursor")
  val vPROPERTY_CURSOR: LocalVariable = cursorVariable[PropertyCursor]("propertyCursor")

  private val vCURSORS = Seq(vNODE_CURSOR, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR)

  private val LOAD_CONTEXT = load("context")

  private val GET_TX_STATE_NODE_PROP: Method = method[DbAccess, Value, Long, Int]("getTxStateNodePropertyOrNull")
  private val GET_TX_STATE_RELATIONSHIP_PROP: Method = method[DbAccess, Value, Long, Int]("getTxStateRelationshipPropertyOrNull")
  private val HAS_TX_STATE_NODE_PROP: Method = method[DbAccess, Optional[_], Long, Int]("hasTxStatePropertyForCachedNodeProperty")
  private val HAS_TX_STATE_RELATIONSHIP_PROP: Method = method[DbAccess, Optional[_], Long, Int]("hasTxStatePropertyForCachedRelationshipProperty")

  val NODE_PROPERTY: Method = method[DbAccess, Value, Long, Int, NodeCursor, PropertyCursor, Boolean]("nodeProperty")
  val RELATIONSHIP_PROPERTY: Method = method[DbAccess, Value, Long, Int, RelationshipScanCursor, PropertyCursor, Boolean]("relationshipProperty")

  private val PACKAGE_NAME = "org.neo4j.codegen"

  private def className(): String = "Expression" + COUNTER.getAndIncrement()

  private def cursorVariable[T](name: String)(implicit m: Manifest[T]): LocalVariable =
    variable[T](name, invoke(load("cursors"), method[ExpressionCursors, T](name)))

  def noValueOr(expressions: IntermediateExpression*)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    nullCheck(expressions:_*)(noValue)(onNotNull)
  }

  def nullCheck(expressions: IntermediateExpression*)(onNull: IntermediateRepresentation = noValue)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    val checks = expressions.foldLeft(Set.empty[IntermediateRepresentation])((acc, current) => acc ++ current.nullChecks)
    if (checks.nonEmpty) ternary(checks.reduceLeft(or), onNull, onNotNull)
    else onNotNull
  }

  def nullCheckIfRequired(expression: IntermediateExpression, onNull: IntermediateRepresentation = noValue): IntermediateRepresentation =
    if (expression.requireNullCheck) nullCheck(expression)(onNull)(expression.ir) else expression.ir
}

class DefaultExpressionCompiler(slots: SlotConfiguration, readOnly: Boolean, codeGenerationMode: CodeGeneration.CodeGenerationMode, namer: VariableNamer) extends ExpressionCompiler(slots, readOnly, codeGenerationMode, namer) {

  override protected def getLongAt(offset: Int): IntermediateRepresentation = getLongFromExecutionContext(offset)

  override protected def getRefAt(offset: Int): IntermediateRepresentation = getRefFromExecutionContext(offset)

  override protected def setRefAt(offset: Int,
                                  value: IntermediateRepresentation): IntermediateRepresentation =
    setRefInExecutionContext(offset, value)

  override protected def setLongAt(offset: Int,
                                   value: IntermediateRepresentation): IntermediateRepresentation =
    setLongInExecutionContext(offset, value)

  override protected def setCachedPropertyAt(offset: Int,
                                             value: IntermediateRepresentation): IntermediateRepresentation =
    setCachedPropertyInExecutionContext(offset, value)

  override protected def getCachedPropertyAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): IntermediateRepresentation = {
    val variableName = namer.nextVariableName()
    block(
      declareAndAssign(typeRefOf[Value], variableName, getCachedPropertyFromExecutionContext(property.cachedPropertyOffset)),
      condition(isNull(load(variableName)))(
      block(
        assign(variableName, getFromStore),
        setCachedPropertyAt(property.cachedPropertyOffset, load(variableName)))
      ),
      load(variableName))
  }

  override protected def isLabelSetOnNode(labelToken: IntermediateRepresentation,
                                          offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS,
           method[DbAccess, Boolean, Int, Long, NodeCursor]("isLabelSetOnNode"),
           labelToken,
           getLongAt(offset),
           NODE_CURSOR)

  override protected def getNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS, NODE_PROPERTY,
           getLongAt(offset), propertyToken, NODE_CURSOR, PROPERTY_CURSOR, constant(true))

  override protected def hasNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, NodeCursor, PropertyCursor]("nodeHasProperty"),
           getLongAt(offset), propertyToken, NODE_CURSOR, PROPERTY_CURSOR)

  override protected def getRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS, RELATIONSHIP_PROPERTY,
           getLongAt(offset), propertyToken, RELATIONSHIP_CURSOR, PROPERTY_CURSOR, constant(true))

  override protected def hasRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipHasProperty"),
           getLongAt(offset), propertyToken, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)

  override protected def getProperty(key: String, container: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(
      method[CypherFunctions, AnyValue, String, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyGet"),
      constant(key),
      container,
      DB_ACCESS,
      NODE_CURSOR,
      RELATIONSHIP_CURSOR,
      PROPERTY_CURSOR)
}

trait CursorRepresentation {
  def hasLabel(labelToken: IntermediateRepresentation): IntermediateRepresentation
  def getProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation
  def hasProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation
  def reference: IntermediateRepresentation
  def relationshipType: IntermediateRepresentation
}
