/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util
import java.util.regex

import org.neo4j.codegen.api.IntermediateRepresentation.{invoke, load, method, noValue, or, ternary, variable}
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.compiler.helpers.PredicateHelper.isPredicate
import org.neo4j.cypher.internal.logical.plans.{ASTCachedNodeProperty, CoerceToPredicate, NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.physicalplanning.ast._
import org.neo4j.cypher.internal.physicalplanning.{LongSlot, RefSlot, Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.ast.{ExpressionVariable, ParameterFromSlot}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpression
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, ExpressionCursors}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType, ListType}
import org.neo4j.cypher.internal.v4_0.util.{CypherTypeException, InternalException}
import org.neo4j.cypher.operations._
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, RelationshipScanCursor}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.impl.util.ValueUtils.asAnyValue
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Produces IntermediateRepresentation from a Cypher Expression
  */
abstract class IntermediateCodeGeneration(slots: SlotConfiguration) {

  private val namer = new VariableNamer

  private class VariableNamer {
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

  import IntermediateCodeGeneration._
  import IntermediateRepresentation._

  def compileProjection(projections: Map[Int, IntermediateExpression]): IntermediateExpression = {
    val all = projections.toSeq.map {
      case (slot, value) => setRefAt(slot,
                                     if (value.nullCheck.isEmpty) value.ir
                                     else ternary(value.nullCheck.reduceLeft((acc,current) => or(acc, current)),
                                                                              noValue, value.ir))
    }
    IntermediateExpression(block(all:_*), projections.values.flatMap(_.fields).toSeq,
                                projections.values.flatMap(_.variables).toSeq, Set.empty)
  }

  def compileGroupingExpression(projections: Map[Slot, IntermediateExpression]): IntermediateGroupingExpression = {
    assert(projections.nonEmpty)
    val listVar = namer.nextVariableName()
    val singleValue = projections.size == 1
    def id[T](value: IntermediateRepresentation, nullable: Boolean)(implicit m: Manifest[T]) =  {
      val getId = invoke(cast[T](value), method[T, Long]("id"))
      if (!nullable) getId
      else ternary(equal(value, noValue), constant(-1L), getId)
    }

    def accessValue(i: Int) =  {
      if (singleValue) load("key")
      else invoke(load(listVar), method[ListValue, AnyValue, Int]("value"), constant(i))
    }
    val groupingsOrdered = projections.toSeq.sortBy(_._1.offset)

    val projectKeyOps = groupingsOrdered.map(_._1).zipWithIndex.map {
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

    val getKeyOps = groupingsOrdered.map(_._1).map {
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

    val computeKeyOps = groupingsOrdered.map(_._2).map(p => nullCheck(p)(p.ir)).toArray
    val computeKey =
      if (singleValue) computeKeyOps.head
      else invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf[AnyValue](computeKeyOps:_*))

    IntermediateGroupingExpression(
      IntermediateExpression(block(projectKey:_*), Seq.empty, Seq.empty, Set.empty),
      IntermediateExpression(computeKey, projections.values.flatMap(_.fields).toSeq, projections.values.flatMap(_.variables).toSeq, Set.empty),
      IntermediateExpression(getKey, Seq.empty, Seq.empty, Set.empty))
  }

  def compileExpression(expression: Expression): Option[IntermediateExpression] = expression match {

    //functions
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None
    case c: FunctionInvocation => compileFunction(c)

    //math
    case Multiply(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("multiply"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case expressions.Add(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case UnaryAdd(source) => compileExpression(source)

    case expressions.Subtract(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case UnarySubtract(source) =>
      for {arg <- compileExpression(source)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"),
                       getStatic[Values, IntegralValue]("ZERO_INT"), arg.ir), arg.fields, arg.variables, arg.nullCheck)
      }

    case Divide(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("divide"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(invokeStatic(method[CypherMath, Boolean, AnyValue, AnyValue]("divideCheckForNull"),
                           nullCheck(l)(l.ir), nullCheck(r)(r.ir))))
      }

    case Modulo(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("modulo"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case Pow(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("pow"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    //literals
    case d: DoubleLiteral =>
      val constant = staticConstant[DoubleValue](namer.nextVariableName().toUpperCase, Values.doubleValue(d.value))
      Some(IntermediateExpression(getStatic[DoubleValue](constant.name), Seq(constant), Seq.empty, Set.empty))
    case i: IntegerLiteral =>
      val constant = staticConstant[LongValue](namer.nextVariableName().toUpperCase, Values.longValue(i.value))
      Some(IntermediateExpression(getStatic[LongValue](constant.name), Seq(constant), Seq.empty, Set.empty))
    case s: expressions.StringLiteral =>
      val constant = staticConstant[TextValue](namer.nextVariableName().toUpperCase, Values.stringValue(s.value))
      Some(IntermediateExpression(getStatic[TextValue](constant.name), Seq(constant), Seq.empty, Set.empty))
    case _: Null => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
    case _: True => Some(IntermediateExpression(trueValue, Seq.empty, Seq.empty, Set.empty))
    case _: False => Some(IntermediateExpression(falseValue, Seq.empty, Seq.empty, Set.empty))
    case ListLiteral(args) =>
      val in = args.flatMap(compileExpression)
      if (in.size < args.size) None
      else {
        val fields: Seq[Field] = in.foldLeft(Seq.empty[Field])((a, b) => a ++ b.fields)
        val variables: Seq[LocalVariable] = in.foldLeft(Seq.empty[LocalVariable])((a, b) => a ++ b.variables)
        Some(IntermediateExpression(
          invokeStatic(method[VirtualValues, ListValue, Array[AnyValue]]("list"), arrayOf[AnyValue](in.map(_.ir): _*)),
          fields, variables, Set.empty))
      }

    case MapExpression(items) =>
      val compiled = (for {(k, v) <- items
                           c <- compileExpression(v)} yield k -> c).toMap
      if (compiled.size < items.size) None
      else {
        val tempVariable = namer.nextVariableName()
        val ops = Seq(
          declare[MapValueBuilder](tempVariable),
          assign(tempVariable, newInstance(constructor[MapValueBuilder, Int], constant(compiled.size)))
        ) ++ compiled.map {
          case (k, v) => invokeSideEffect(load(tempVariable),
                                          method[MapValueBuilder, AnyValue, String, AnyValue]("add"),
                                          constant(k.name), nullCheck(v)(v.ir))
        } :+ invoke(load(tempVariable), method[MapValueBuilder, MapValue]("build"))

        Some(IntermediateExpression(block(ops: _*), compiled.values.flatMap(_.fields).toSeq,
                                    compiled.values.flatMap(_.variables).toSeq, Set.empty))
      }

    case _: MapProjection => throw new InternalException("should have been rewritten away")
    case _: NestedPlanExpression => throw new InternalException("should have been rewritten away")

    case DesugaredMapProjection(name, items, includeAllProps) =>
      val expressions = items.flatMap(i => compileExpression(i.exp))

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

    case ListSlice(collection, None, None) => compileExpression(collection)

    case ListSlice(collection, Some(from), None) =>
      for {c <- compileExpression(collection)
           f <- compileExpression(from)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("fromSlice"), c.ir, f.ir),
          c.fields ++ f.fields, c.variables ++ f.variables, c.nullCheck ++ f.nullCheck)

      }

    case ListSlice(collection, None, Some(to)) =>
      for {c <- compileExpression(collection)
           t <- compileExpression(to)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("toSlice"), c.ir, t.ir),
          c.fields ++ t.fields, c.variables ++ t.variables, c.nullCheck ++ t.nullCheck)
      }

    case ListSlice(collection, Some(from), Some(to)) =>
      for {c <- compileExpression(collection)
           f <- compileExpression(from)
           t <- compileExpression(to)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("fullSlice"), c.ir, f.ir, t.ir),
          c.fields ++ f.fields ++ t.fields, c.variables ++ f.variables ++ t.variables,
          c.nullCheck ++ f.nullCheck ++ t.nullCheck)
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
      for {collection <- compileExpression(collectionExpression)
           inner <- compileExpression(scope.innerPredicate.get)
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
              assign(isMatch, nullCheck(inner)(inner.ir)),
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
                               collection.nullCheck)
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
      for {collection <- compileExpression(collectionExpression)
           inner <- compileExpression(scope.innerPredicate.get)
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
              assign(isMatch, nullCheck(inner)(inner.ir)),
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
                               collection.nullCheck)
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
      for {collection <- compileExpression(collectionExpression)
           inner <- compileExpression(scope.innerPredicate.get)
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
              assign(isMatch, nullCheck(inner)(inner.ir)),
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
                               collection.nullCheck)
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
      for {collection <- compileExpression(collectionExpression)
           inner <- compileExpression(scope.innerPredicate.get)
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
              assign(isMatch, nullCheck(inner)(inner.ir))
            ): _*)
          },
          // }
          // return isMatch;
          load(isMatch)
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                               collection.variables ++ inner.variables,
                               collection.nullCheck)
      }

    case FilterExpression(scope, collectionExpression) =>
      filterExpression(compileExpression(collectionExpression),
                       scope.innerPredicate.get, ExpressionVariable.cast(scope.variable))

    case ListComprehension(scope, list) =>
      val filter = scope.innerPredicate match {
        case Some(_: True) | None => compileExpression(list)
        case Some(inner) => filterExpression(compileExpression(list),
                                             inner, ExpressionVariable.cast(scope.variable))
      }
      scope.extractExpression match {
        case None => filter
        case Some(extract) =>
          extractExpression(filter, extract, ExpressionVariable.cast(scope.variable))
      }

    case ExtractExpression(scope, collectionExpression) =>
      extractExpression(compileExpression(collectionExpression),
                        scope.extractExpression.get, ExpressionVariable.cast(scope.variable))

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
      for {collection <- compileExpression(collectionExpression)
           init <- compileExpression(initExpression)
           inner <- compileExpression(scope.expression)
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          // expressionVariables[accOffset] = init;
          setExpressionVariable(accVar, nullCheck(init)(init.ir)),
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
              setExpressionVariable(accVar, nullCheck(inner)(inner.ir))
            ): _*)
          },
          // return expressionVariables[accOffset];
          loadExpressionVariable(accVar)
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields ++ init.fields, collection.variables ++
          inner.variables ++ init.variables, collection.nullCheck ++ init.nullCheck)
      }

    //boolean operators
    case Or(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateOrs(List(left, right))
      }

    case Ors(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty))
        { (acc, current) =>
          for {l <- acc
               e <- compileExpression(current)} yield l :+ (e -> isPredicate(current))
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
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("xor"), left.ir, right.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case And(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateAnds(List(left, right))
      }

    case Ands(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty))
        { (acc, current) =>
          for {l <- acc
               e <- compileExpression(current)} yield l :+ (e -> isPredicate(current))
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
      val compiledInequalities = inequalities.toIndexedSeq.flatMap(i => compileExpression(i))
      if (compiledInequalities.size < inequalities.size) None
      else Some(generateAnds(compiledInequalities.toList))

    case expressions.Not(arg) =>
      compileExpression(arg).map(a => {
        val in = if (isPredicate(arg)) a else coerceToPredicate(a)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue]("not"), in.ir), in.fields, in.variables, in.nullCheck)
      })

    case Equals(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(l, r)(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("equals"), l.ir, r.ir))))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case NotEquals(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(l, r)(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("notEquals"), l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case CoerceToPredicate(inner) => compileExpression(inner).map(coerceToPredicate)

    case RegexMatch(lhs, rhs) => rhs match {
      case expressions.StringLiteral(name) =>
        for (e <- compileExpression(lhs)) yield {
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
        for {l <- compileExpression(lhs)
             r <- compileExpression(rhs)
        } yield {
          IntermediateExpression(
            invokeStatic(method[CypherBoolean, BooleanValue, TextValue, TextValue]("regex"),
                         cast[TextValue](l.ir),
                         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("asTextValue"), r.ir)),
            l.fields ++ r.fields, l.variables ++ r.variables, r.nullCheck + not(instanceOf[TextValue](l.ir)))
        }
    }

    case StartsWith(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("startsWith"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case EndsWith(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("endsWith"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case Contains(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("contains"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case expressions.IsNull(test) =>
      for (e <- compileExpression(test)) yield {
        IntermediateExpression(
          ternary(equal(nullCheck(e)(e.ir), noValue), trueValue, falseValue), e.fields, e.variables, Set.empty)
      }

    case expressions.IsNotNull(test) =>
      for (e <- compileExpression(test)) yield {
        IntermediateExpression(
          ternary(notEqual(nullCheck(e)(e.ir), noValue), trueValue, falseValue), e.fields, e.variables, Set.empty)
      }

    case LessThan(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {

        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName,
                                     nullCheck(l, r)(
                                       invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("lessThan"), l.ir,
                                                    r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case LessThanOrEqual(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName,
                                     nullCheck(l, r)(
                                       invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("lessThanOrEqual"),
                                                    l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case GreaterThan(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName,
                                     nullCheck(l, r)(
                                       invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("greaterThan"),
                                                    l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case GreaterThanOrEqual(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName,
                                     nullCheck(l, r)(invokeStatic(
                                       method[CypherBoolean, Value, AnyValue, AnyValue]("greaterThanOrEqual"), l.ir,
                                       r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case In(_, ListLiteral(expressions)) if expressions.isEmpty =>
      Some(IntermediateExpression(falseValue, Seq.empty, Seq.empty, Set.empty))

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
      for (l <- compileExpression(lhs)) yield {
        val setField = staticConstant[util.HashSet[AnyValue]](namer.nextVariableName(), set)
        IntermediateExpression(
          ternary(invoke(getStatic[util.HashSet[AnyValue]](setField.name), method[util.Set[AnyValue], Boolean, Object]("contains"), l.ir),
                  trueValue, onNotFound), l.fields :+ setField, l.variables, l.nullCheck)
      }

    case In(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)} yield {

        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName,
                                     nullCheck(r)(invokeStatic(
                                       method[CypherBoolean, Value, AnyValue, AnyValue]("in"), l.ir,
                                       nullCheck(r)(r.ir)))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    // misc
    case CoerceTo(expr, typ) =>
      for (e <- compileExpression(expr)) yield {
        typ match {
          case CTAny => e
          case CTString =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, TextValue, AnyValue]("asTextValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTNode =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, NodeValue, AnyValue]("asNodeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTRelationship =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, RelationshipValue, AnyValue]("asRelationshipValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTPath =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PathValue, AnyValue]("asPathValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTInteger =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, IntegralValue, AnyValue]("asIntegralValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTFloat =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, FloatingPointValue, AnyValue]("asFloatingPointValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTMap =>
            IntermediateExpression(
              invokeStatic(
                method[CypherCoercions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor](
                  "asMapValue"),
                e.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
              e.fields, e.variables ++ vCURSORS, e.nullCheck)

          case l: ListType =>
            val typ = asNeoType(l.innerType)

            IntermediateExpression(
              invokeStatic(method[CypherCoercions, ListValue, AnyValue, AnyType, DbAccess, ExpressionCursors]("asList"),
                           e.ir, typ, DB_ACCESS, CURSORS),
              e.fields, e.variables, e.nullCheck)

          case CTBoolean =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, BooleanValue, AnyValue]("asBooleanValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTNumber =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, NumberValue, AnyValue]("asNumberValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTPoint =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PointValue, AnyValue]("asPointValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTGeometry =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, PointValue, AnyValue]("asPointValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTDate =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DateValue, AnyValue]("asDateValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTLocalTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, LocalTimeValue, AnyValue]("asLocalTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, TimeValue, AnyValue]("asTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTLocalDateTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, LocalDateTimeValue, AnyValue]("asLocalDateTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTDateTime =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DateTimeValue, AnyValue]("asDateTimeValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case CTDuration =>
            IntermediateExpression(
              invokeStatic(method[CypherCoercions, DurationValue, AnyValue]("asDurationValue"), e.ir),
              e.fields, e.variables, e.nullCheck)
          case _ => throw new CypherTypeException(s"Can't coerce to $typ")
        }
      }

    //data access
    case ContainerIndex(container, index) =>
      for {c <- compileExpression(container)
           idx <- compileExpression(index)
      } yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(c, idx)(nullCheck(c, idx)(
          invokeStatic(method[CypherFunctions, AnyValue, AnyValue, AnyValue, DbAccess,
            NodeCursor, RelationshipScanCursor, PropertyCursor]("containerIndex"),
                       c.ir, idx.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)))))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(
          ops, c.fields ++ idx.fields, c.variables ++ idx.variables ++ vCURSORS :+ local, Set(nullChecks))
      }

    case ParameterFromSlot(offset, name, _) =>
      val parameterVariable = namer.parameterName(name)
      val local = variable[AnyValue](parameterVariable,
                                      arrayLoad(load("params"), offset))
      Some(IntermediateExpression(load(parameterVariable), Seq.empty, Seq(local),
                                  Set(equal(load(parameterVariable), noValue))))

    case CaseExpression(Some(innerExpression), alternativeExpressions, defaultExpression) =>

      val maybeDefault = defaultExpression match {
        case Some(e) => compileExpression(e)
        case None => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
      }
      val (checkExpressions, loadExpressions) = alternativeExpressions.unzip

      val checks = checkExpressions.flatMap(compileExpression)
      val loads = loadExpressions.flatMap(compileExpression)
      if (checks.size != loads.size || checks.isEmpty || maybeDefault.isEmpty) None
      else {
        for {inner: IntermediateExpression <- compileExpression(innerExpression)
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
        case Some(e) => compileExpression(e)
        case None => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
      }
      val (checkExpressions, loadExpressions) = alternativeExpressions.unzip
      val checks = checkExpressions.flatMap { e =>
        if (isPredicate(e)) compileExpression(e)
        else compileExpression(e).map(coerceToPredicate)
      }

      val loads = loadExpressions.flatMap(compileExpression)
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
      for (map <- compileExpression(targetExpression)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val propertyGet = invokeStatic(method[CypherFunctions, AnyValue, String, AnyValue, DbAccess,
          NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyGet"),
                                       constant(key), map.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)
        val call =
          if (map.nullCheck.isEmpty) propertyGet
          else ternary(map.nullCheck.reduceLeft((acc, current) => or(acc, current)), noValue, propertyGet)
        val lazySet = oneTime(assign(variableName, call))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, map.fields, map.variables ++ vCURSORS :+ local, Set(nullChecks))
      }

    case NodeProperty(offset, token, _) =>
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName,
        invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, NodeCursor, PropertyCursor]("nodeProperty"),
               getLongAt(offset), constant(token), NODE_CURSOR, PROPERTY_CURSOR)))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(local, vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case CachedNodeProperty(offset, token, cachedPropertyOffset) =>
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName, invokeStatic(
        method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int, Int, NodeCursor, PropertyCursor]("cachedProperty"),
        LOAD_CONTEXT, DB_ACCESS, constant(offset), constant(token), constant(cachedPropertyOffset),
        NODE_CURSOR, PROPERTY_CURSOR)))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(local, vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case NodePropertyLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName,  block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
          invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, NodeCursor, PropertyCursor]("nodeProperty"),
                 getLongAt(offset), loadField(f), NODE_CURSOR, PROPERTY_CURSOR))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq(f), Seq(local, vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case CachedNodePropertyLate(offset, propKey, cachedPropertyOffset) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName, block(
        condition(equal(loadField(f), constant(-1)))(
          setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(propKey)))),
                                   invokeStatic(method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int, Int, NodeCursor, PropertyCursor]("cachedProperty"),
                                                LOAD_CONTEXT, DB_ACCESS, constant(offset), loadField(f), constant(cachedPropertyOffset), NODE_CURSOR, PROPERTY_CURSOR))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq(f), Seq(local, vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case NodePropertyExists(offset, token, _) =>
      Some(
        IntermediateExpression(
          ternary(
          invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, NodeCursor, PropertyCursor]("nodeHasProperty"),
                 getLongAt(offset), constant(token), NODE_CURSOR, PROPERTY_CURSOR),
            trueValue, falseValue), Seq.empty, Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case NodePropertyExistsLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(
        invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, NodeCursor, PropertyCursor]("nodeHasProperty"),
               getLongAt(offset), loadField(f), NODE_CURSOR, PROPERTY_CURSOR),
          trueValue, falseValue)), Seq(f), Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case RelationshipProperty(offset, token, _) =>
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName,
        invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipProperty"),
          getLongAt(offset), constant(token), RELATIONSHIP_CURSOR, PROPERTY_CURSOR)))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(local, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case RelationshipPropertyLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName, block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
          invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipProperty"),
                 getLongAt(offset), loadField(f), RELATIONSHIP_CURSOR, PROPERTY_CURSOR))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))

      Some(IntermediateExpression(ops, Seq(f), Seq(local, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case RelationshipPropertyExists(offset, token, _) =>
      Some(IntermediateExpression(
        ternary(
          invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipHasProperty"),
                 getLongAt(offset), constant(token), RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
          trueValue,
          falseValue), Seq.empty, Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set.empty)
      )

    case RelationshipPropertyExistsLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(
        invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipHasProperty"),
               getLongAt(offset), loadField(f), RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
        trueValue,
        falseValue)), Seq(f), Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case HasLabels(nodeExpression, labels)  if labels.nonEmpty =>
      for (node <- compileExpression(nodeExpression)) yield {
        val tokensAndNames = labels.map(l => field[Int](s"label${l.name}", constant(-1)) -> l.name)

        val init = tokensAndNames.map {
          case (token, labelName) =>
            condition(equal(loadField(token), constant(-1)))(setField(
              token, invoke(DB_ACCESS, method[DbAccess, Int, String]("nodeLabel"), constant(labelName))))
        }

        val predicate: IntermediateRepresentation = ternary(tokensAndNames.map { token =>
          invokeStatic(method[CypherFunctions, Boolean, AnyValue, Int, DbAccess, NodeCursor]("hasLabel"),
                       node.ir, loadField(token._1), DB_ACCESS, NODE_CURSOR)
        }.reduceLeft(and), trueValue, falseValue)

        IntermediateExpression(block(init :+ predicate:_*),
          node.fields ++ tokensAndNames.map(_._1), node.variables :+ vNODE_CURSOR, node.nullCheck)
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
        case (Some(given), _) => compileExpression(given)
        case (_, Some(default)) =>
          val constant = staticConstant[AnyValue](namer.nextVariableName().toUpperCase(), asAnyValue(default))
          Some(IntermediateExpression(getStatic[AnyValue](constant.name), Seq(constant), Seq.empty, Set.empty))
        case _ => None
      }
      if (inputArgs.size != signature.inputSignature.size) None
      else {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val allowed = staticConstant[Array[String]](namer.nextVariableName(), signature.allowed)
        val fields = inputArgs.flatMap(_.fields) :+ allowed
        val variables = inputArgs.flatMap(_.variables) :+ local
        val ops = oneTime(assign(variableName, invoke(
          DB_ACCESS,
          method[DbAccess, AnyValue, Int, Array[AnyValue], Array[String]]("callFunction"),
          constant(signature.id),
          arrayOf[AnyValue](inputArgs.map(_.ir): _*),
          getStatic[Array[String]](allowed.name))))

        Some(IntermediateExpression(block(ops, load(variableName)), fields, variables, Set(equal(load(variableName), noValue))))
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
      val nullCheck = slots.get(name).filter(_.nullable).map(_ => equal(getRefAt(offset), noValue)).toSet
      val loadRef =  getRefAt(offset)
      Some(IntermediateExpression(loadRef, Seq.empty, Seq.empty, nullCheck))

    case IdFromSlot(offset) =>
      val nameOfSlot = slots.nameOfLongSlot(offset)
      val nullCheck = nameOfSlot.filter(n => slots(n).nullable).map(_ => equal(getLongAt(offset), constant(-1L))).toSet
      val value = invokeStatic(method[Values, LongValue, Long]("longValue"), getLongAt(offset))

      Some(IntermediateExpression(value, Seq.empty, Seq.empty, nullCheck))

    case PrimitiveEquals(lhs, rhs) =>
      for {l <- compileExpression(lhs)
           r <- compileExpression(rhs)
      } yield
        IntermediateExpression(
          ternary(invoke(l.ir, method[AnyValue, Boolean, AnyRef]("equals"), r.ir), trueValue, falseValue),
          l.fields ++ r.fields, l.variables ++ r.variables, Set.empty)

    case NullCheck(offset, inner) =>
      compileExpression(inner).map(i => i.copy(nullCheck = i.nullCheck + equal(getLongAt(offset), constant(-1L))))

    case NullCheckVariable(offset, inner) =>
      compileExpression(inner).map(i => i.copy(nullCheck = i.nullCheck + equal(getLongAt(offset), constant(-1L))))

    case NullCheckProperty(offset, inner) =>
      compileExpression(inner).map(i => i.copy(nullCheck = i.nullCheck + equal(getLongAt(offset), constant(-1L))))

    case IsPrimitiveNull(offset) =>
      Some(IntermediateExpression(ternary(equal(getLongAt(offset), constant(-1L)), trueValue, falseValue),
                                  Seq.empty, Seq.empty, Set.empty))

    case _ => None
  }

  def compileFunction(c: FunctionInvocation): Option[IntermediateExpression] = c.function match {
    case functions.Acos =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("acos"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Cos =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Cot =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cot"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Asin =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("asin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Haversin =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("haversin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sin =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Atan =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("atan"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Atan2 =>
      for {y <- compileExpression(c.args(0))
           x <- compileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, DoubleValue, AnyValue, AnyValue]("atan2"), y.ir, x.ir),
          y.fields ++ x.fields, y.variables ++ x.variables, y.nullCheck ++ x.nullCheck)
      }
    case functions.Tan =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("tan"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Round =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("round"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Rand =>
      Some(IntermediateExpression(invokeStatic(method[CypherFunctions, DoubleValue]("rand")),
                                  Seq.empty, Seq.empty, Set.empty))
    case functions.Abs =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NumberValue, AnyValue]("abs"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Ceil =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("ceil"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Floor =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("floor"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Degrees =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toDegrees"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Exp =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("exp"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Log =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Log10 =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log10"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Radians =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toRadians"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sign =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("signum"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sqrt =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sqrt"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Range  if c.args.length == 2 =>
      for {start <- compileExpression(c.args(0))
           end <- compileExpression(c.args(1))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("range"),
                                                  nullCheck(start)(start.ir), nullCheck(end)(end.ir)),
                                     start.fields ++ end.fields,
                                     start.variables ++ end.variables, Set.empty)

    case functions.Range  if c.args.length == 3 =>
      for {start <- compileExpression(c.args(0))
           end <- compileExpression(c.args(1))
           step <- compileExpression(c.args(2))
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("range"),
                                                  nullCheck(start)(start.ir), nullCheck(end)(end.ir), nullCheck(step)(step.ir)),
                                     start.fields ++ end.fields ++ step.fields,
                                     start.variables ++ end.variables ++ step.variables, Set.empty)

    case functions.Pi => Some(IntermediateExpression(getStatic[Values, DoubleValue]("PI"), Seq.empty, Seq.empty, Set.empty))
    case functions.E => Some(IntermediateExpression(getStatic[Values, DoubleValue]("E"), Seq.empty, Seq.empty, Set.empty))

    case functions.Coalesce =>
      val args = c.args.flatMap(compileExpression)
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
          case expression :: Nil => assign(tempVariable, nullCheck(expression)(expression.ir))
          case expression :: tail =>
            //tempVariable = hd; if (tempVariable == NO_VALUE){[continue with tail]}
            if (expression.nullCheck.nonEmpty) block(assign(tempVariable, nullCheck(expression)(expression.ir)),
                                                     condition(expression.nullCheck.reduceLeft((acc,current) => or(acc, current)))(loop(tail)))
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
      for {p1 <- compileExpression(c.args(0))
           p2 <- compileExpression(c.args(1))
      } yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName,
                                       invokeStatic(method[CypherFunctions, Value, AnyValue, AnyValue]("distance"), p1.ir, p2.ir))
        IntermediateExpression(
          load(variableName),
          p1.fields ++ p2.fields,  p1.variables ++ p2.variables :+ local, Set(equal(load(variableName), noValue)))
      }

    case functions.StartNode =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess, RelationshipScanCursor]("startNode"),
                     in.ir, DB_ACCESS, RELATIONSHIP_CURSOR), in.fields, in.variables :+ vRELATIONSHIP_CURSOR, in.nullCheck))

    case functions.EndNode =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess, RelationshipScanCursor]("endNode"), in.ir,
                                      DB_ACCESS, RELATIONSHIP_CURSOR), in.fields, in.variables :+ vRELATIONSHIP_CURSOR, in.nullCheck))

    case functions.Nodes =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue]("nodes"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Relationships =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, ListValue, AnyValue]("relationships"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Exists =>
      c.arguments.head match {
        case property: Property =>
          compileExpression(property.map).map(in => IntermediateExpression(
              invokeStatic(method[CypherFunctions, BooleanValue, String, AnyValue, DbAccess,
                                  NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyExists"),
                           constant(property.propertyKey.name),
                           in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR ),
            in.fields, in.variables ++ vCURSORS, in.nullCheck))

        case property: ASTCachedNodeProperty => cachedNodeExists(property)
        case _: PatternExpression => None//TODO
        case _: NestedPipeExpression => None//TODO?
        case _: NestedPlanExpression => throw new InternalException("should have been rewritten away")
        case _ => None
      }

    case functions.Head =>
      compileExpression(c.args.head).map(in => {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName,  nullCheck(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("head"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      })

    case functions.Id =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("id"), in.ir),
        in.fields, in.variables, in.nullCheck))

    case functions.Labels =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess, NodeCursor]("labels"), in.ir, DB_ACCESS, NODE_CURSOR),
       in.fields, in.variables :+ vNODE_CURSOR, in.nullCheck))

    case functions.Type =>
      compileExpression(c.args.head).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, TextValue, AnyValue]("type"), in.ir),
         in.fields, in.variables, in.nullCheck))

    case functions.Last =>
      compileExpression(c.args.head).map(in => {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("last"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      })

    case functions.Left =>
      for {in <- compileExpression(c.args(0))
           endPos <- compileExpression(c.args(1))
      } yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("left"), in.ir, endPos.ir),
         in.fields ++ endPos.fields, in.variables ++ endPos.variables, in.nullCheck)
      }

    case functions.LTrim =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("ltrim"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.RTrim =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("rtrim"), in.ir),
         in.fields, in.variables, in.nullCheck)
      }

    case functions.Trim =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("trim"), in.ir),
         in.fields, in.variables, in.nullCheck)
      }

    case functions.Replace =>
      for {original <- compileExpression(c.args(0))
           search <- compileExpression(c.args(1))
           replaceWith <- compileExpression(c.args(2))
      } yield {
        IntermediateExpression(
            invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("replace"),
                         original.ir, search.ir, replaceWith.ir),
          original.fields ++ search.fields ++ replaceWith.fields, original.variables ++ search.variables ++ replaceWith.variables,
          original.nullCheck ++ search.nullCheck ++ replaceWith.nullCheck)
      }

    case functions.Reverse =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("reverse"), in.ir), in.fields, in.variables, in.nullCheck)
      }

    case functions.Right =>
      for {in <- compileExpression(c.args(0))
           len <- compileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("right"), in.ir, len.ir),
          in.fields ++ len.fields, in.variables ++ len.variables, in.nullCheck)
      }

    case functions.Split =>
      for {original <- compileExpression(c.args(0))
           sep <- compileExpression(c.args(1))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("split"), original.ir, sep.ir),
          original.fields ++ sep.fields, original.variables ++ sep.variables,
        original.nullCheck ++ sep.nullCheck)
      }

    case functions.Substring if c.args.size == 2 =>
      for {original <- compileExpression(c.args(0))
           start <- compileExpression(c.args(1))
      } yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("substring"), original.ir, start.ir),
          original.fields ++ start.fields, original.variables ++ start.variables, original.nullCheck)
      }

    case functions.Substring  =>
      for {original <- compileExpression(c.args(0))
           start <- compileExpression(c.args(1))
           len <- compileExpression(c.args(2))
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("substring"),
                                              original.ir, start.ir, len.ir),
          original.fields ++ start.fields ++ len.fields,
          original.variables ++ start.variables ++ len.variables, original.nullCheck)
      }

    case functions.ToLower =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toLower"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.ToUpper =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toUpper"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Point =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, Value, AnyValue, DbAccess, ExpressionCursors]("point"),
            in.ir, DB_ACCESS, CURSORS),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Keys =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("keys"),
            in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
          in.fields, in.variables ++ vCURSORS, in.nullCheck)
      }

    case functions.Size =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("size"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Length =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("length"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Tail =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue]("tail"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.ToBoolean =>
      for (in <- compileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName,  nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toBoolean"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      }

    case functions.ToFloat =>
      for (in <- compileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toFloat"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      }

    case functions.ToInteger =>
      for (in <- compileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toInteger"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      }

    case functions.ToString =>
      for (in <- compileExpression(c.args.head)) yield {
        IntermediateExpression(invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toString"), in.ir), in.fields, in.variables, in.nullCheck)
      }

    case functions.Properties =>
      for (in <- compileExpression(c.args.head)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(
          method[CypherFunctions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("properties"),
          in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables ++ Seq(local) ++ vCURSORS, Set(nullChecks))
      }

    case p =>
      None
  }

  //Extension points
  protected def getLongAt(offset: Int): IntermediateRepresentation

  protected def getRefAt(offset: Int): IntermediateRepresentation

  protected def setRefAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation

  protected def setLongAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation

  protected final def getLongFromExecutionContext(offset: Int): IntermediateRepresentation =
    invoke(LOAD_CONTEXT, method[ExecutionContext, Long, Int]("getLongAt"), constant(offset))

  protected final def getRefFromExecutionContext(offset: Int): IntermediateRepresentation =
    invoke(LOAD_CONTEXT, method[ExecutionContext, AnyValue, Int]("getRefAt"),
           constant(offset))

  protected final def setRefInExecutionContext(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(LOAD_CONTEXT, method[ExecutionContext, Unit, Int, AnyValue]("setRefAt"),
                     constant(offset), value)

  protected final def setLongInExecutionContext(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(LOAD_CONTEXT, method[ExecutionContext, Unit, Int, Long]("setLongAt"),
                     constant(offset), value)
  //==================================================================================================

  private def coerceToPredicate(e: IntermediateExpression) = IntermediateExpression(
    invokeStatic(method[CypherBoolean, Value, AnyValue]("coerceToBoolean"), e.ir), e.fields, e.variables, e.nullCheck)

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
    val nullable = expressions.exists(_.nullCheck.nonEmpty)

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
      val loadValue = tryCatch[RuntimeException](exceptionName)(assign(returnValue, nullCheck(e)(invokeStatic(ASSERT_PREDICATE, e.ir))))(
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
    val nullCheckAssign = if (firstExpression.nullCheck.nonEmpty) Seq(assign(seenNull, equal(load(returnValue), noValue))) else Seq.empty
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
            assign(returnValue, nullCheck(firstExpression)(invokeStatic(ASSERT_PREDICATE, firstExpression.ir))))(
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
                           Set(equal(load(returnValue), noValue)))
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
        computeRepresentation(ir =
                                invokeStatic(
                                  method[CompiledHelpers, AnyValue, ExecutionContext, DbAccess, Int]("nodeOrNoValue"),
                                  LOAD_CONTEXT, DB_ACCESS, constant(offset)),
                              nullCheck = Some(equal(getLongAt(offset), constant(-1L))),
                              nullable = nullable)
      case Some(LongSlot(offset, nullable, CTRelationship)) =>
        computeRepresentation(ir = invokeStatic(
                                  method[CompiledHelpers, AnyValue, ExecutionContext, DbAccess, Int]("relationshipOrNoValue"),
                                  LOAD_CONTEXT, DB_ACCESS, constant(offset)),
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
         inner <- compileExpression(innerPredicate)
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
            assign(isFiltered, nullCheck(inner)(inner.ir)),
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
                             collection.nullCheck)
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
         inner <- compileExpression(extractExpression)
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
                             nullCheck(inner)(inner.ir))): _*)
        },
        // }
        // return VirtualValues.fromList(extracted);
        invokeStatic(method[VirtualValues, ListValue, java.util.List[AnyValue]]("fromList"), load(extractedVars))
      )
      IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields,
                             collection.variables ++ inner.variables,
                             collection.nullCheck)
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
      case NodePathStep(node, next) => compileSteps(next, nodeOps ++ compileExpression(node), relOps)

      case SingleRelationshipPathStep(relExpression, _, Some(targetExpression), next) =>
        (compileExpression(relExpression), compileExpression(targetExpression)) match {
        case (Some(rel), Some(target)) => compileSteps(next, nodeOps :+ target, relOps :+ rel)
        case _ => None
      }

      case SingleRelationshipPathStep(rel, SemanticDirection.BOTH, _, next) => compileExpression(rel) match {
        case Some(compiled) =>
          val relVar = variable[RelationshipValue](namer.nextVariableName(), constant(null))
          val lazyRel = oneTime(assign(relVar.name, cast[RelationshipValue](compiled.ir)))
          val node = IntermediateExpression(
            block(lazyRel,
                  invokeStatic(method[CypherFunctions, NodeValue, VirtualRelationshipValue, DbAccess, VirtualNodeValue, RelationshipScanCursor]("otherNode"),
                               cast[RelationshipValue](load(relVar.name)), DB_ACCESS,  cast[NodeValue](nodeOps.last.ir), RELATIONSHIP_CURSOR)),
            compiled.fields, compiled.variables :+ relVar :+ vRELATIONSHIP_CURSOR, compiled.nullCheck ++ nodeOps.last.nullCheck)
          val rel = IntermediateExpression(
            block(lazyRel, load(relVar.name)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullCheck)

          compileSteps(next, nodeOps :+ node, relOps :+ rel)
        case None => None
      }

      case SingleRelationshipPathStep(rel, direction, _, next) => compileExpression(rel) match {
        case Some(compiled) =>
          val methodName = if (direction == SemanticDirection.INCOMING) "startNode" else "endNode"
          val relVar = variable[RelationshipValue](namer.nextVariableName(), constant(null))
          val lazyRel = oneTime(assign(relVar.name, cast[RelationshipValue](compiled.ir)))
          val node = IntermediateExpression(
            block(lazyRel,
              invokeStatic(method[CypherFunctions, NodeValue, VirtualRelationshipValue, DbAccess, RelationshipScanCursor](methodName),
                           cast[RelationshipValue](load(relVar.name)), DB_ACCESS, RELATIONSHIP_CURSOR)),
            compiled.fields, compiled.variables :+ relVar :+ vRELATIONSHIP_CURSOR, compiled.nullCheck)
          val rel = IntermediateExpression(
            block(lazyRel, load(relVar.name)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullCheck)

          compileSteps(next, nodeOps :+ node, relOps :+ rel)
        case None => None
      }

      case MultiRelationshipPathStep(_, _, _, _) => throw new IllegalStateException("Cannot be used for static paths")
      case NilPathStep => Some((nodeOps, relOps))
    }

    for ((nodeOps, relOps) <- compileSteps(steps) ) yield {
      val variableName = namer.nextVariableName()
      val local = variable[AnyValue](variableName, noValue)
      val lazySet = oneTime(assign(variableName,
                                   nullCheck(nodeOps ++ relOps: _*)(
                                     invokeStatic(
                                       method[VirtualValues, PathValue, Array[NodeValue], Array[RelationshipValue]](
                                         "path"),
                                       arrayOf[NodeValue](nodeOps.map(n => cast[NodeValue](n.ir)): _*),
                                       arrayOf[RelationshipValue](relOps.map(r => cast[RelationshipValue](r.ir)): _*))
                                   )))

      val ops = block(lazySet, load(variableName))
      val nullChecks =
        if (nodeOps.forall(_.nullCheck.isEmpty) && relOps.forall(_.nullCheck.isEmpty)) Set
          .empty[IntermediateRepresentation]
        else Set(block(lazySet, equal(load(variableName), noValue)))

      IntermediateExpression(ops,
                             nodeOps.flatMap(_.fields) ++ relOps.flatMap(_.fields),
                             nodeOps.flatMap(_.variables) ++ relOps.flatMap(_.variables) :+ local,
                             nullChecks)
    }
  }

  private def compileDynamicPath(steps: PathStep) = {

    val builderVar = namer.nextVariableName()
    @tailrec
    def compileSteps(step: PathStep, acc: Seq[IntermediateExpression] = ArrayBuffer.empty): Option[Seq[IntermediateExpression]] = step match {
      case NodePathStep(node, next) => compileExpression(node) match {
        case Some(nodeOps) =>
          val addNode =
            if (nodeOps.nullCheck.isEmpty) invokeSideEffect(load(builderVar),
                                                  method[PathValueBuilder, Unit, NodeValue]("addNode"),
                                                  cast[NodeValue](nodeOps.ir))
            else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue]("addNode"),
                        nullCheck(nodeOps)(nodeOps.ir))
          compileSteps(next, acc :+ nodeOps.copy(ir = addNode))
        case None => None
      }

      //For this case end node is known
      case SingleRelationshipPathStep(rel, direction, Some(targetExpression), next) =>
        (compileExpression(rel), compileExpression(targetExpression)) match {
        case (Some(relOps), Some(target)) =>
          //this will generate something like
          //builder.addNode(n);
          //builder.addRelationship(r);
          val addNodeAndRelationship =
              block(
                if (target.nullCheck.isEmpty) {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, NodeValue]("addNode"), cast[NodeValue](target.ir))
                } else {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue]("addNode"), nullCheck(target)(target.ir))
                },
                if (relOps.nullCheck.isEmpty) {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, RelationshipValue]("addRelationship"), cast[RelationshipValue](relOps.ir))
                } else {
                  invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue]("addRelationship"),
                                   nullCheck(relOps)(relOps.ir))
                }
              )

          compileSteps(next, acc :+ relOps.copy(ir = addNodeAndRelationship))
        case _ => None
      }

      //Here end node is not known and will require lookup
      case SingleRelationshipPathStep(rel, direction, _, next) =>  compileExpression(rel) match {
        case Some(relOps) =>
          val methodName = direction match {
            case SemanticDirection.INCOMING => "addIncoming"
            case SemanticDirection.OUTGOING => "addOutgoing"
            case _ => "addUndirected"
          }

          val addRel =
            if (relOps.nullCheck.isEmpty) invokeSideEffect(load(builderVar),
                                                  method[PathValueBuilder, Unit, RelationshipValue](methodName),
                                                  cast[RelationshipValue](relOps.ir))
            else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue](methodName),
                        nullCheck(relOps)(relOps.ir))
          compileSteps(next, acc :+ relOps.copy(ir = addRel))
        case None => None
      }
      case MultiRelationshipPathStep(rel, direction, maybeTarget, next) => compileExpression(rel) match {
        case Some(relOps) =>
          val methodName = direction match {
            case SemanticDirection.INCOMING => "addMultipleIncoming"
            case SemanticDirection.OUTGOING => "addMultipleOutgoing"
            case _ => "addMultipleUndirected"
          }

          val addRels = maybeTarget.flatMap(t => compileExpression(t)) match {
            case Some(target) =>
              if (relOps.nullCheck.isEmpty && target.nullCheck.isEmpty) {
                invokeSideEffect(load(builderVar),
                                 method[PathValueBuilder, Unit, ListValue, NodeValue](methodName),
                                 cast[ListValue](relOps.ir), cast[NodeValue](target.ir))
              } else {
                invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue, AnyValue](methodName),
                                 nullCheck(relOps)(relOps.ir), nullCheck(target)(target.ir))
              }
            case None =>
              if (relOps.nullCheck.isEmpty) {
                invokeSideEffect(load(builderVar),
                                 method[PathValueBuilder, Unit, ListValue](methodName),
                                 cast[ListValue](relOps.ir))
              }
              else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue](methodName),
                                    nullCheck(relOps)(relOps.ir))
          }
          compileSteps(next, acc :+ relOps.copy(ir = addRels))
        case None => None
      }
      case NilPathStep => Some(acc)
    }

    for (pathOps <- compileSteps(steps) ) yield {
      val variableName = namer.nextVariableName()
      val local = variable[AnyValue](variableName, noValue)
      val lazySet = oneTime(
        block(
          Seq(
            declare[PathValueBuilder](builderVar),
            assign(builderVar, newInstance(constructor[PathValueBuilder, DbAccess, RelationshipScanCursor], DB_ACCESS, RELATIONSHIP_CURSOR)))
            ++ pathOps.map(_.ir) :+ assign(variableName, invoke(load(builderVar), method[PathValueBuilder, AnyValue]("build"))): _*))


      val ops = block(lazySet, load(variableName))
      val nullChecks =
        if (pathOps.forall(_.nullCheck.isEmpty)) Set.empty[IntermediateRepresentation]
        else Set(block(lazySet, equal(load(variableName), noValue)))

      IntermediateExpression(ops,
                             pathOps.flatMap(_.fields),
                             pathOps.flatMap(_.variables) :+ local :+ vRELATIONSHIP_CURSOR,
                             nullChecks)
    }
  }

  private def setFromLiterals(literals: Seq[Literal]): util.HashSet[AnyValue] = {
    val set = new util.HashSet[AnyValue]()
    literals.foreach(l => set.add(ValueUtils.of(l.value)))
    set
  }

  private def cachedNodeExists(property: ASTCachedNodeProperty) = property match {
    case CachedNodeProperty(offset, prop, _) =>

      Some(IntermediateExpression(
        invokeStatic(
          method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int]("cachedPropertyExists"),
          LOAD_CONTEXT, DB_ACCESS, constant(offset), constant(prop)), Seq.empty,
        Seq.empty, Set.empty))

      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName, invokeStatic(
        method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int]("cachedPropertyExists"),
        LOAD_CONTEXT, DB_ACCESS, constant(offset), constant(prop))))
      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(local), Set(nullChecks)))

    case CachedNodePropertyLate(offset, prop, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName, block(
        condition(equal(loadField(f), constant(-1)))(
          setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(prop)))),
        invokeStatic(
          method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int]("cachedPropertyExists"),
          LOAD_CONTEXT, DB_ACCESS, constant(offset), loadField(f)))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))

      Some(IntermediateExpression(ops, Seq(f), Seq(local), Set(nullChecks)))

    case _ => None
  }

  private def setExpressionVariable(ev: ExpressionVariable, value: IntermediateRepresentation): IntermediateRepresentation = {
    arraySet(load("expressionVariables"), ev.offset, value)
  }

  private def loadExpressionVariable(ev: ExpressionVariable): IntermediateRepresentation = {
    arrayLoad(load("expressionVariables"), ev.offset)
  }
}

object IntermediateCodeGeneration {
  def defaultGenerator(slots: SlotConfiguration): IntermediateCodeGeneration = new DefaultIntermediateCodeGeneration(slots)
  private val ASSERT_PREDICATE = method[CompiledHelpers, Value, AnyValue]("assertBooleanOrNoValue")
  private val DB_ACCESS = load("dbAccess")
  private val CURSORS = load("cursors")

  private val NODE_CURSOR = load("nodeCursor")
  private val vNODE_CURSOR = cursorVariable[NodeCursor]("nodeCursor")

  private val RELATIONSHIP_CURSOR = load("relationshipScanCursor")
  private val vRELATIONSHIP_CURSOR = cursorVariable[RelationshipScanCursor]("relationshipScanCursor")

  private val PROPERTY_CURSOR = load("propertyCursor")
  private val vPROPERTY_CURSOR = cursorVariable[PropertyCursor]("propertyCursor")

  private val vCURSORS = Seq(vNODE_CURSOR, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR)

  private val LOAD_CONTEXT = load("context")

  private def cursorVariable[T](name: String)(implicit m: Manifest[T]): LocalVariable =
    variable[T](name, invoke(load("cursors"), method[ExpressionCursors, T](name)))

  def nullCheck(expressions: IntermediateExpression*)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    val checks = expressions.foldLeft(Set.empty[IntermediateRepresentation])((acc, current) => acc ++ current.nullCheck)
    if (checks.nonEmpty) ternary(checks.reduceLeft(or), noValue, onNotNull)
    else onNotNull
  }
}

private class DefaultIntermediateCodeGeneration(slots: SlotConfiguration) extends IntermediateCodeGeneration(slots) {

  override protected def getLongAt(offset: Int): IntermediateRepresentation = getLongFromExecutionContext(offset)

  override protected def getRefAt(offset: Int): IntermediateRepresentation = getRefFromExecutionContext(offset)

  override protected def setRefAt(offset: Int,
                                  value: IntermediateRepresentation): IntermediateRepresentation =
    setRefInExecutionContext(offset, value)

  override protected def setLongAt(offset: Int,
                                   value: IntermediateRepresentation): IntermediateRepresentation =
    setLongInExecutionContext(offset, value)
}
