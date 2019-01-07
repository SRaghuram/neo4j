/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import java.util.regex

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{LongSlot, RefSlot, Slot, SlotConfiguration}
import org.neo4j.cypher.internal.compiler.v4_0.helpers.PredicateHelper.isPredicate
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateRepresentation.{invoke, load, method, variable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpression
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, ExpressionCursors}
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.v4_0.logical.plans.{CoerceToPredicate, NestedPlanExpression, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType, ListType}
import org.neo4j.cypher.internal.v4_0.util.{CypherTypeException, InternalException}
import org.neo4j.cypher.operations._
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{Neo4jTypes, QualifiedName}
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, RelationshipScanCursor}
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
class IntermediateCodeGeneration(slots: SlotConfiguration) {

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
                                     None,
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
      else ternary(equal(value, noValue), constant(-1), getId)
    }

    def accessValue(i: Int) =  {
      if (singleValue) load("key")
      else invoke(load(listVar), method[ListValue, AnyValue, Int]("value"), constant(i))
    }
    val groupingsOrdered = projections.toSeq.sortBy(_._1.offset)

    val projectKeyOps = groupingsOrdered.map(_._1).zipWithIndex.map {
      case (LongSlot(offset, nullable, CTNode), index) =>
        setLongAt(offset, None, id[VirtualNodeValue](accessValue(index), nullable))
      case (LongSlot(offset, nullable, CTRelationship), index) =>
        setLongAt(offset, None, id[VirtualRelationshipValue](accessValue(index), nullable))
      case (RefSlot(offset, _, _), index) =>
          setRefAt(offset, None, accessValue(index))
      case slot =>
        throw new InternalException(s"Do not know how to make setter for slot $slot")
    }
    val projectKey =
      if (singleValue) projectKeyOps
      else Seq(declare[ListValue](listVar), assign(listVar, cast[ListValue](load("key")))) ++ projectKeyOps

    val getKeyOps = groupingsOrdered.map(_._1).map {
      case LongSlot(offset, nullable, CTNode) =>
        val getter = invokeStatic(method[VirtualValues, NodeValue, Long]("node"), getLongAt(offset, None))
        if (nullable) ternary(equal(getLongAt(offset, None), constant(-1)), noValue, getter)
        else getter
      case LongSlot(offset, nullable, CTRelationship) =>
        val getter = invokeStatic(method[VirtualValues, RelationshipValue, Long]("relationship"), getLongAt(offset, None))
        if (nullable) ternary(equal(getLongAt(offset, None), constant(-1)), noValue, getter)
        else getter
      case RefSlot(offset, _, _) => getRefAt(offset, None)
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

  def compileExpression(expression: Expression): Option[IntermediateExpression] = internalCompileExpression(expression, None)

  private def internalCompileExpression(expression: Expression, currentContext: Option[IntermediateRepresentation]): Option[IntermediateExpression] = expression match {

    //functions
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None
    case c: FunctionInvocation => compileFunction(c, currentContext)

    //math
    case Multiply(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("multiply"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case expressions.Add(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("add"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case UnaryAdd(source) => internalCompileExpression(source, currentContext)

    case expressions.Subtract(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case UnarySubtract(source) =>
      for {arg <- internalCompileExpression(source, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("subtract"),
                       getStatic[Values, IntegralValue]("ZERO_INT"), arg.ir), arg.fields, arg.variables, arg.nullCheck)
      }

    case Divide(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("divide"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(invokeStatic(method[CypherMath, Boolean, AnyValue, AnyValue]("divideCheckForNull"),
                           nullCheck(l)(l.ir), nullCheck(r)(r.ir))))
      }

    case Modulo(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherMath, AnyValue, AnyValue, AnyValue]("modulo"), l.ir, r.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case Pow(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
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
    case _: True => Some(IntermediateExpression(truthValue, Seq.empty, Seq.empty, Set.empty))
    case _: False => Some(IntermediateExpression(falseValue, Seq.empty, Seq.empty, Set.empty))
    case ListLiteral(args) =>
      val in = args.flatMap(internalCompileExpression(_, currentContext))
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
                           c <- internalCompileExpression(v, currentContext)} yield k -> c).toMap
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
      val expressions = items.flatMap(i => internalCompileExpression(i.exp, currentContext))

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

        val (accessName, maybeNullCheck, maybeVariable) = accessVariable(name.name, currentContext)

        if (!includeAllProps) {
          Some(IntermediateExpression(
            block(buildMapValue :+ invoke(load(builderVar), method[MapValueBuilder, MapValue]("build")): _*),
            expressions.flatMap(_.fields), expressions.flatMap(_.variables) ++ maybeVariable, maybeNullCheck.toSet))

        } else if (buildMapValue.isEmpty) {
          Some(IntermediateExpression(
            block(buildMapValue ++ Seq(
              invokeStatic(
                method[CypherCoercions, MapValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor](
                  "asMapValue"),
                accessName, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)): _*),
            expressions.flatMap(_.fields), expressions.flatMap(_.variables) ++ vCURSORS ++ maybeVariable,
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
            expressions.flatMap(_.fields), expressions.flatMap(_.variables) ++ vCURSORS ++ maybeVariable,
            maybeNullCheck.toSet))
        }
      }

    case ListSlice(collection, None, None) => internalCompileExpression(collection, currentContext)

    case ListSlice(collection, Some(from), None) =>
      for {c <- internalCompileExpression(collection, currentContext)
           f <- internalCompileExpression(from, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("fromSlice"), c.ir, f.ir),
          c.fields ++ f.fields, c.variables ++ f.variables, c.nullCheck ++ f.nullCheck)

      }

    case ListSlice(collection, None, Some(to)) =>
      for {c <- internalCompileExpression(collection, currentContext)
           t <- internalCompileExpression(to, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("toSlice"), c.ir, t.ir),
          c.fields ++ t.fields, c.variables ++ t.variables, c.nullCheck ++ t.nullCheck)
      }

    case ListSlice(collection, Some(from), Some(to)) =>
      for {c <- internalCompileExpression(collection, currentContext)
           f <- internalCompileExpression(from, currentContext)
           t <- internalCompileExpression(to, currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("fullSlice"), c.ir, f.ir, t.ir),
          c.fields ++ f.fields ++ t.fields, c.variables ++ f.variables ++ t.variables,
          c.nullCheck ++ f.nullCheck ++ t.nullCheck)
      }

    case Variable(name) =>
      val (variableAccess, nullCheck, maybeVariable) = accessVariable(name, currentContext)

      Some(IntermediateExpression(variableAccess, Seq.empty, maybeVariable.toSeq, nullCheck.toSet))

    case SingleIterablePredicate(scope, collectionExpression) =>
      /*
        ListValue list = [evaluate collection expression];
        ExecutionContext innerContext = context.createClone();
        Iterator<AnyValue> listIterator = list.iterator();
        int matches = 0;
        boolean isNull = false;
        while( matches < 2 && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            innerContext.set([name from scope], currentValue);
            Value isMatch = [result from inner expression using innerContext]
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
      val innerContext = namer.nextVariableName()
      val iterVariable = namer.nextVariableName()
      for {collection <- internalCompileExpression(collectionExpression, currentContext)
           inner <- internalCompileExpression(scope.innerPredicate.get,
                                              Some(load(innerContext))) //Note we update the context here
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val matches = namer.nextVariableName()
        val isNull = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // ExecutionContext innerContext = context.createClone();
          // int matches = 0;
          // boolean isNull = false;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[ExecutionContext](innerContext),
          assign(innerContext,
                 invoke(loadContext(currentContext), method[ExecutionContext, ExecutionContext]("createClone"))),
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
              //innerContext.set([name from scope], currentValue);
              contextSet(scope.variable.name, load(innerContext), load(currentValue)),
              // Value isMatch = [result from inner expression using innerContext]
              // if (isMatch == Values.TRUE)
              // {
              //     matches = matches + 1;
              // }
              declare[Value](isMatch),
              assign(isMatch, nullCheck(inner)(inner.ir)),
              condition(equal(load(isMatch), truthValue))(
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
        ExecutionContext innerContext = context.createClone();
        Iterator<AnyValue> listIterator = list.iterator();
        Value isMatch = listIterator.hasNext() ? Values.NO_VALUE : Values.FALSE;
        boolean isNull = false;
        while( isMatch != Values.TRUE && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            innerContext.set([name from scope], currentValue);
            isMatch = [result from inner expression using innerContext]
            if (isMatch == Values.NO_VALUE)
            {
                isNull = true;
            }
        }
        return (isNull && isMatch != Values.TRUE) ? Values.NO_VALUE : Values.booleanValue(isMatch == Values.FALSE);
       */
      val innerContext = namer.nextVariableName()
      val iterVariable = namer.nextVariableName()
      for {collection <- internalCompileExpression(collectionExpression, currentContext)
           inner <- internalCompileExpression(scope.innerPredicate.get,
                                              Some(load(innerContext))) //Note we update the context here
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val isNull = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // ExecutionContext innerContext = context.createClone();
          // Value isMatch = Values.NO_VALUE;
          // boolean isNull = false;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[ExecutionContext](innerContext),
          assign(innerContext,
                 invoke(loadContext(currentContext), method[ExecutionContext, ExecutionContext]("createClone"))),
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          declare[Value](isMatch),
          //assign(isMatch, noValue),
          assign(isMatch,
                 ternary(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")), noValue,
                         falseValue)),
          declare[Boolean](isNull),
          assign(isNull, constant(false)),
          // Iterator<AnyValue> listIterator = list.iterator();
          // while( isMatch != Values.TRUE, && listIterator.hasNext() )
          // {
          //    AnyValue currentValue = listIterator.next();
          loop(and(notEqual(load(isMatch), truthValue),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              //innerContext.set([name from scope], currentValue);
              contextSet(scope.variable.name, load(innerContext), load(currentValue)),
              // isMatch = [result from inner expression using innerContext]
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
          ternary(and(load(isNull), notEqual(load(isMatch), truthValue)),
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
        ExecutionContext innerContext = context.createClone();
        Iterator<AnyValue> listIterator = list.iterator();
        Value isMatch = Values.FALSE;
        boolean isNull = false;
        while( isMatch != Values.TRUE && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            innerContext.set([name from scope], currentValue);
            isMatch = [result from inner expression using innerContext]
            if (isMatch == Values.NO_VALUE)
            {
                isNull = true;
            }
        }
        return (isNull && isMatch != Values.TRUE) ? Values.NO_VALUE : isMatch;
       */
      val innerContext = namer.nextVariableName()
      val iterVariable = namer.nextVariableName()
      for {collection <- internalCompileExpression(collectionExpression, currentContext)
           inner <- internalCompileExpression(scope.innerPredicate.get,
                                              Some(load(innerContext))) //Note we update the context here
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val isNull = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // ExecutionContext innerContext = context.createClone();
          // Value isMatch = Values.FALSE;
          // boolean isNull = false;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[ExecutionContext](innerContext),
          assign(innerContext,
                 invoke(loadContext(currentContext), method[ExecutionContext, ExecutionContext]("createClone"))),
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
          loop(and(notEqual(load(isMatch), truthValue),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              //innerContext.set([name from scope], currentValue);
              contextSet(scope.variable.name, load(innerContext), load(currentValue)),
              // isMatch = [result from inner expression using innerContext]
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
          ternary(and(load(isNull), notEqual(load(isMatch), truthValue)),
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
        ExecutionContext innerContext = context.createClone();
        Iterator<AnyValue> listIterator = list.iterator();
        Value isMatch = Values.TRUE;
        while( isMatch==Values.TRUE && listIterator.hasNext() )
        {
            AnyValue currentValue = listIterator.next();
            innerContext.set([name from scope], currentValue);
            isMatch = [result from inner expression using innerContext]
        }
        return isMatch;
       */
      val innerContext = namer.nextVariableName()
      val iterVariable = namer.nextVariableName()
      for {collection <- internalCompileExpression(collectionExpression, currentContext)
           inner <- internalCompileExpression(scope.innerPredicate.get,
                                              Some(load(innerContext))) //Note we update the context here
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val isMatch = namer.nextVariableName()
        val ops = Seq(
          // ListValue list = [evaluate collection expression];
          // ExecutionContext innerContext = context.createClone();
          // Value isMatch = Values.TRUE;
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[ExecutionContext](innerContext),
          assign(innerContext,
                 invoke(loadContext(currentContext), method[ExecutionContext, ExecutionContext]("createClone"))),
          declare[Value](isMatch),
          assign(isMatch, truthValue),
          // Iterator<AnyValue> listIterator = list.iterator();
          // while( isMatch==Values.TRUE && listIterator.hasNext())
          // {
          //    AnyValue currentValue = listIterator.next();
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(and(equal(load(isMatch), truthValue),
                   invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext")))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              //innerContext.set([name from scope], currentValue);
              contextSet(scope.variable.name, load(innerContext), load(currentValue)),
              // isMatch = [result from inner expression using innerContext]
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
     filterExpression(internalCompileExpression(collectionExpression, currentContext),
                      scope.innerPredicate.get, scope.variable.name, currentContext)

    case ListComprehension(scope, list) =>
       val filter = scope.innerPredicate match {
         case Some(_: True) | None => internalCompileExpression(list, currentContext)
         case Some(inner) => filterExpression(internalCompileExpression(list, currentContext),
                                              inner, scope.variable.name, currentContext)
       }
      scope.extractExpression match {
        case None => filter
        case Some(extract) =>
          extractExpression(filter, extract, scope.variable.name, currentContext)
      }

    case ExtractExpression(scope, collectionExpression) =>
      extractExpression(internalCompileExpression(collectionExpression, currentContext),
                        scope.extractExpression.get, scope.variable.name, currentContext)

    case ReduceExpression(scope, initExpression, collectionExpression) =>
      /*
        reduce is tricky because it modifies the scope for future expressions. The generated code will be something
        along the line of:

        ListValue list = [evaluate collection expression];
        ExecutionContext innerContext = context.copyWith(acc, init);
        for ( AnyValue currentValue : list ) {
            innerContext.set([name from scope], currentValue);
            innerContext.set(acc, [result from inner expression using innerContext[)
        }
        return innerContext.apply(acc)
       */
      //this is the context inner expressions should see
      val innerContext = namer.nextVariableName()
      val iterVariable = namer.nextVariableName()
      for {collection <- internalCompileExpression(collectionExpression, currentContext)
           init <- internalCompileExpression(initExpression, currentContext)
           inner <- internalCompileExpression(scope.expression,
                                              Some(load(innerContext))) //Note we update the context here
      } yield {
        val listVar = namer.nextVariableName()
        val currentValue = namer.nextVariableName()
        val (accessInnerContext, variableIsNull, maybeVariable) = accessVariable(scope.accumulator.name, Some(load(innerContext)))
        val ops = Seq(
          //ListValue list = [evaluate collection expression];
          //ExecutionContext innerContext = context.copyWith(acc, init);
          declare[ListValue](listVar),
          assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
          declare[ExecutionContext](innerContext),
          assign(innerContext,
                 invoke(loadContext(currentContext),
                        method[ExecutionContext, ExecutionContext, String, AnyValue]("copyWith"),
                        constant(scope.accumulator.name), nullCheck(init)(init.ir))),
          //Iterator<AnyValue> iter = list.iterator();
          //while (iter.hasNext) {
          //   AnyValue currentValue = iter.next();
          declare[java.util.Iterator[AnyValue]](iterVariable),
          assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
          loop(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext"))) {
            block(Seq(
              declare[AnyValue](currentValue),
              assign(currentValue,
                     cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
              // innerContext.set([name from scope], currentValue);
              contextSet(scope.variable.name, load(innerContext), load(currentValue)),
              //innerContext.set(acc, [inner expression using innerContext])
              contextSet(scope.accumulator.name, load(innerContext), nullCheck(inner)(inner.ir))
            ): _*)
          },
          //return innerContext(acc);
          accessInnerContext
        )
        IntermediateExpression(block(ops: _*), collection.fields ++ inner.fields ++ init.fields, collection.variables ++
          inner.variables ++ init.variables ++ maybeVariable, collection.nullCheck ++ init.nullCheck ++ variableIsNull)
      }

    //boolean operators
    case Or(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateOrs(List(left, right))
      }

    case Ors(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty))
        { (acc, current) =>
          for {l <- acc
               e <- internalCompileExpression(current, currentContext)} yield l :+ (e -> isPredicate(current))
        }

      for (e <- compiled) yield e match {
        case Nil => IntermediateExpression(truthValue, Seq.empty, Seq.empty,
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
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("xor"), left.ir, right.ir),
          l.fields ++ r.fields, l.variables ++ r.variables, l.nullCheck ++ r.nullCheck)
      }

    case And(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        val left = if (isPredicate(lhs)) l else coerceToPredicate(l)
        val right = if (isPredicate(rhs)) r else coerceToPredicate(r)
        generateAnds(List(left, right))
      }

    case Ands(expressions) =>
      val compiled = expressions.foldLeft[Option[List[(IntermediateExpression, Boolean)]]](Some(List.empty))
        { (acc, current) =>
          for {l <- acc
               e <- internalCompileExpression(current, currentContext)} yield l :+ (e -> isPredicate(current))
        }

      for (e <- compiled) yield e match {
        case Nil => IntermediateExpression(truthValue, Seq.empty, Seq.empty,
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
      val compiledInequalities = inequalities.toIndexedSeq.flatMap(i => internalCompileExpression(i, currentContext))
      if (compiledInequalities.size < inequalities.size) None
      else Some(generateAnds(compiledInequalities.toList))

    case expressions.Not(arg) =>
      internalCompileExpression(arg, currentContext).map(a => {
        val in = if (isPredicate(arg)) a else coerceToPredicate(a)
        IntermediateExpression(
          invokeStatic(method[CypherBoolean, Value, AnyValue]("not"), in.ir), in.fields, in.variables, in.nullCheck)
      })

    case Equals(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
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
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(l, r)(
          invokeStatic(method[CypherBoolean, Value, AnyValue, AnyValue]("notEquals"), l.ir, r.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, l.fields ++ r.fields, l.variables ++ r.variables :+ local, Set(nullChecks))
      }

    case CoerceToPredicate(inner) => internalCompileExpression(inner, currentContext).map(coerceToPredicate)

    case RegexMatch(lhs, rhs) => rhs match {
      case expressions.StringLiteral(name) =>
        for (e <- internalCompileExpression(lhs, currentContext)) yield {
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
        for {l <- internalCompileExpression(lhs, currentContext)
             r <- internalCompileExpression(rhs, currentContext)
        } yield {
          IntermediateExpression(
            invokeStatic(method[CypherBoolean, BooleanValue, TextValue, TextValue]("regex"),
                         cast[TextValue](l.ir),
                         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("asTextValue"), r.ir)),
            l.fields ++ r.fields, l.variables ++ r.variables, r.nullCheck + not(instanceOf[TextValue](l.ir)))
        }
    }

    case StartsWith(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("startsWith"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case EndsWith(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("endsWith"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case Contains(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {
        IntermediateExpression(
          invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"),
                       invoke(cast[TextValue](l.ir), method[TextValue, Boolean, TextValue]("contains"),
                              cast[TextValue](r.ir))),
          l.fields ++ r.fields, l.variables ++ r.variables,
          Set(not(instanceOf[TextValue](l.ir)), not(instanceOf[TextValue](r.ir))))
      }

    case expressions.IsNull(test) =>
      for (e <- internalCompileExpression(test, currentContext)) yield {
        IntermediateExpression(
          ternary(equal(e.ir, noValue), truthValue, falseValue), e.fields, e.variables, Set.empty)
      }

    case expressions.IsNotNull(test) =>
      for (e <- internalCompileExpression(test, currentContext)) yield {
        IntermediateExpression(
          ternary(notEqual(e.ir, noValue), truthValue, falseValue), e.fields, e.variables, Set.empty)
      }

    case LessThan(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {

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
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {
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
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {
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
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {
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

    case In(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)} yield {

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
      for (e <- internalCompileExpression(expr, currentContext)) yield {
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
      for {c <- internalCompileExpression(container, currentContext)
           idx <- internalCompileExpression(index, currentContext)
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

    case Parameter(name, _) => //TODO parameters that are autogenerated from literals should have nullable = false
      //parameters are global in the sense that we only need one variable for the parameter
      val parameterVariable = namer.parameterName(name)

      val local = variable[AnyValue](parameterVariable,
                                     invoke(load("params"), method[MapValue, AnyValue, String]("get"),
                                            constant(name)))
      Some(IntermediateExpression(load(parameterVariable), Seq.empty, Seq(local),
                                  Set(equal(load(parameterVariable), noValue))))

    case CaseExpression(Some(innerExpression), alternativeExpressions, defaultExpression) =>

      val maybeDefault = defaultExpression match {
        case Some(e) => internalCompileExpression(e, currentContext)
        case None => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
      }
      val (checkExpressions, loadExpressions) = alternativeExpressions.unzip

      val checks = checkExpressions.flatMap(internalCompileExpression(_, currentContext))
      val loads = loadExpressions.flatMap(internalCompileExpression(_, currentContext))
      if (checks.size != loads.size || checks.isEmpty || maybeDefault.isEmpty) None
      else {
        for {inner: IntermediateExpression <- internalCompileExpression(innerExpression, currentContext)
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
        case Some(e) => internalCompileExpression(e, currentContext)
        case None => Some(IntermediateExpression(noValue, Seq.empty, Seq.empty, Set(constant(true))))
      }
      val (checkExpressions, loadExpressions) = alternativeExpressions.unzip
      val checks = checkExpressions.flatMap { e =>
        if (isPredicate(e)) internalCompileExpression(e, currentContext)
        else internalCompileExpression(e, currentContext).map(coerceToPredicate)
      }

      val loads = loadExpressions.flatMap(internalCompileExpression(_, currentContext))
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
                        toCheck => equal(toCheck, truthValue))
        Some(IntermediateExpression(ops,
                                    checks.flatMap(_.fields) ++ loads.flatMap(_.fields) ++ default.fields,
                                    checks.flatMap(_.variables) ++ loads.flatMap(_.variables) ++ default
                                      .variables :+ local,
                                    Set(block(equal(load(returnVariable), noValue)))))
      }

    case Property(targetExpression, PropertyKeyName(key)) =>
      for (map <- internalCompileExpression(targetExpression, currentContext)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, ternary(map.nullCheck.reduceLeft((acc,current) => or(acc, current)), noValue,
                                       invokeStatic(method[CypherFunctions, AnyValue, String, AnyValue, DbAccess,
                                                           NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyGet"),
                                         constant(key), map.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, map.fields, map.variables ++ vCURSORS :+ local, Set(nullChecks))
      }

    case NodeProperty(offset, token, _) =>
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName,
        invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, NodeCursor, PropertyCursor]("nodeProperty"),
               getLongAt(offset, currentContext), constant(token), NODE_CURSOR, PROPERTY_CURSOR)))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(local, vNODE_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case CachedNodeProperty(offset, token, cachedPropertyOffset) =>
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName, invokeStatic(
        method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int, Int]("cachedProperty"),
        loadContext(currentContext), DB_ACCESS, constant(offset), constant(token), constant(cachedPropertyOffset))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq.empty, Seq(local), Set(nullChecks)))

    case NodePropertyLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName,  block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
          invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, NodeCursor, PropertyCursor]("nodeProperty"),
                 getLongAt(offset, currentContext), loadField(f), NODE_CURSOR, PROPERTY_CURSOR))))

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
                                   invokeStatic(method[CompiledHelpers, Value, ExecutionContext, DbAccess, Int, Int, Int]("cachedProperty"),
        loadContext(currentContext), DB_ACCESS, constant(offset), loadField(f), constant(cachedPropertyOffset)))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))
      Some(IntermediateExpression(ops, Seq(f), Seq(local), Set(nullChecks)))

    case NodePropertyExists(offset, token, _) =>
      Some(
        IntermediateExpression(
          ternary(
          invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, NodeCursor, PropertyCursor]("nodeHasProperty"),
                 getLongAt(offset, currentContext), constant(token), NODE_CURSOR, PROPERTY_CURSOR),
            truthValue, falseValue), Seq.empty, Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case NodePropertyExistsLate(offset, key, _) =>
      val f = field[Int](namer.nextVariableName(), constant(-1))
      Some(IntermediateExpression(
        block(
          condition(equal(loadField(f), constant(-1)))(
            setField(f, invoke(DB_ACCESS, method[DbAccess, Int, String]("propertyKey"), constant(key)))),
        ternary(
        invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, NodeCursor, PropertyCursor]("nodeHasProperty"),
               getLongAt(offset, currentContext), loadField(f), NODE_CURSOR, PROPERTY_CURSOR),
          truthValue, falseValue)), Seq(f), Seq(vNODE_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case RelationshipProperty(offset, token, _) =>
      val variableName = namer.nextVariableName()
      val local = variable[Value](variableName, noValue)
      val lazySet = oneTime(assign(variableName,
        invoke(DB_ACCESS, method[DbAccess, Value, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipProperty"),
          getLongAt(offset, currentContext), constant(token), RELATIONSHIP_CURSOR, PROPERTY_CURSOR)))

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
                 getLongAt(offset, currentContext), loadField(f), RELATIONSHIP_CURSOR, PROPERTY_CURSOR))))

      val ops = block(lazySet, load(variableName))
      val nullChecks = block(lazySet, equal(load(variableName), noValue))

      Some(IntermediateExpression(ops, Seq(f), Seq(local, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set(nullChecks)))

    case RelationshipPropertyExists(offset, token, _) =>
      Some(IntermediateExpression(
        ternary(
          invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipHasProperty"),
                 getLongAt(offset, currentContext), constant(token), RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
          truthValue,
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
               getLongAt(offset, currentContext), loadField(f), RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
        truthValue,
        falseValue)), Seq(f), Seq(vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR), Set.empty))

    case HasLabels(nodeExpression, labels)  if labels.nonEmpty =>
      for (node <- internalCompileExpression(nodeExpression, currentContext)) yield {
        val tokensAndNames = labels.map(l => field[Int](s"label${l.name}", constant(-1)) -> l.name)

        val init = tokensAndNames.map {
          case (token, labelName) =>
            condition(equal(loadField(token), constant(-1)))(setField(
              token, invoke(DB_ACCESS, method[DbAccess, Int, String]("nodeLabel"), constant(labelName))))
        }

        val predicate: IntermediateRepresentation = ternary(tokensAndNames.map { token =>
          invokeStatic(method[CypherFunctions, Boolean, AnyValue, Int, DbAccess, NodeCursor]("hasLabel"),
                       node.ir, loadField(token._1), DB_ACCESS, NODE_CURSOR)
        }.reduceLeft(and), truthValue, falseValue)

        IntermediateExpression(block(init :+ predicate:_*),
          node.fields ++ tokensAndNames.map(_._1), node.variables :+ vNODE_CURSOR, node.nullCheck)
      }

    case NodeFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset, currentContext)),
        Seq.empty, Seq.empty,
        slots.get(name).filter(_.nullable).map(slot => equal(getLongAt(slot.offset, currentContext), constant(-1L))).toSet))

    case RelationshipFromSlot(offset, name) =>
      Some(IntermediateExpression(
        invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"),
               getLongAt(offset, currentContext)), Seq.empty, Seq.empty,
        slots.get(name).filter(_.nullable).map(slot => equal(getLongAt(slot.offset, currentContext), constant(-1L))).toSet))

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
                           invoke(DB_ACCESS, method[DbAccess, Int, Long, NodeCursor](methodName), getLongAt(offset, currentContext), NODE_CURSOR)),
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
                                  getLongAt(offset, currentContext), loadField(f), NODE_CURSOR))
              ), Seq(f), Seq(vNODE_CURSOR), Set.empty))
      }

    case f@ResolvedFunctionInvocation(name, Some(signature), args) if !f.isAggregate =>
      val inputArgs = args.map(Some(_))
        .zipAll(signature.inputSignature.map(_.default.map(_.value)), None, None).flatMap {
        case (Some(given), _) => internalCompileExpression(given, currentContext)
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
        val (ops, maybeField) = signature.id match {
          case Some(token) =>
            (oneTime(assign(variableName,
              invoke(DB_ACCESS, method[DbAccess, AnyValue, Int, Array[AnyValue], Array[String]]("callFunction"),
                   constant(token), arrayOf[AnyValue](inputArgs.map(_.ir):_*), getStatic[Array[String]](allowed.name)))), None)
          case None =>
            import scala.collection.JavaConverters._
            val kernelName = new QualifiedName(signature.name.namespace.asJava, signature.name.name)

            val name = staticConstant[QualifiedName](namer.nextVariableName(), kernelName)
            (oneTime(assign(variableName,
            invoke(DB_ACCESS, method[DbAccess, AnyValue, QualifiedName, Array[AnyValue], Array[String]]("callFunction"),
                   getStatic[QualifiedName](name.name), arrayOf[AnyValue](inputArgs.map(_.ir):_*), getStatic[Array[String]](allowed.name)))), Some(name))
        }

        Some(IntermediateExpression(block(ops, load(variableName)), fields ++ maybeField, variables, Set(equal(load(variableName), noValue))))
      }


    case PathExpression(steps) =>

      @tailrec
      def isStaticallyKnown(step: PathStep): Boolean = step match {
        case NodePathStep(_, next) => isStaticallyKnown(next)
        case SingleRelationshipPathStep(_, _, _,next) => isStaticallyKnown(next)
        case _: MultiRelationshipPathStep => false
        case NilPathStep => true
      }

      if (isStaticallyKnown(steps)) compileStaticPath(currentContext, steps)
      else compileDynamicPath(currentContext, steps)

    //slotted operations
    case ReferenceFromSlot(offset, name) =>
      val nullCheck = slots.get(name).filter(_.nullable).map(_ => equal(getRefAt(offset, currentContext), noValue)).toSet
      val loadRef =  getRefAt(offset, currentContext)
      Some(IntermediateExpression(loadRef, Seq.empty, Seq.empty, nullCheck))

    case IdFromSlot(offset) =>
      val nameOfSlot = slots.nameOfLongSlot(offset)
      val nullCheck = nameOfSlot.filter(n => slots(n).nullable).map(_ => equal(getLongAt(offset, currentContext), constant(-1L))).toSet
      val value = invokeStatic(method[Values, LongValue, Long]("longValue"), getLongAt(offset, currentContext))

      Some(IntermediateExpression(value, Seq.empty, Seq.empty, nullCheck))

    case PrimitiveEquals(lhs, rhs) =>
      for {l <- internalCompileExpression(lhs, currentContext)
           r <- internalCompileExpression(rhs, currentContext)
      } yield
        IntermediateExpression(
          ternary(invoke(l.ir, method[AnyValue, Boolean, AnyRef]("equals"), r.ir), truthValue, falseValue),
          l.fields ++ r.fields, l.variables ++ r.variables, Set.empty)

    case NullCheck(offset, inner) =>
      internalCompileExpression(inner, currentContext).map(i =>
        IntermediateExpression(
          ternary(equal(getLongAt(offset, currentContext), constant(-1L)), noValue, i.ir),
          i.fields, i.variables, Set(equal(getLongAt(offset, currentContext), constant(-1L))))
      )

    case NullCheckVariable(offset, inner) =>
      internalCompileExpression(inner, currentContext).map(i => IntermediateExpression(ternary(equal(getLongAt(offset, currentContext), constant(-1L)), noValue, i.ir),
                                                                                       i.fields, i.variables, Set(equal(getLongAt(offset, currentContext), constant(-1L)))))

    case NullCheckProperty(offset, inner) =>
      internalCompileExpression(inner, currentContext).map(i => {
        val variableName = namer.nextVariableName()
        val local = variable[Value](variableName, noValue)
        val lazySet = oneTime(condition(notEqual(getLongAt(offset, currentContext), constant(-1L)))(assign(variableName, i.ir)))
        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))
        IntermediateExpression(ops, i.fields, i.variables :+ local, Set(nullChecks))
      })

    case IsPrimitiveNull(offset) =>
      Some(IntermediateExpression(ternary(equal(getLongAt(offset, currentContext), constant(-1L)), truthValue, falseValue),
                                  Seq.empty, Seq.empty, Set.empty))

    case _ => None
  }

  def compileFunction(c: FunctionInvocation, currentContext: Option[IntermediateRepresentation]): Option[IntermediateExpression] = c.function match {
    case functions.Acos =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("acos"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Cos =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cos"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Cot =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("cot"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Asin =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("asin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Haversin =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("haversin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sin =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sin"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Atan =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("atan"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Atan2 =>
      for {y <- internalCompileExpression(c.args(0), currentContext)
           x <- internalCompileExpression(c.args(1), currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, DoubleValue, AnyValue, AnyValue]("atan2"), y.ir, x.ir),
          y.fields ++ x.fields, y.variables ++ x.variables, y.nullCheck ++ x.nullCheck)
      }
    case functions.Tan =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("tan"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Round =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("round"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Rand =>
      Some(IntermediateExpression(invokeStatic(method[CypherFunctions, DoubleValue]("rand")),
                                  Seq.empty, Seq.empty, Set.empty))
    case functions.Abs =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NumberValue, AnyValue]("abs"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Ceil =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("ceil"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Floor =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("floor"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Degrees =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toDegrees"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Exp =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("exp"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Log =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Log10 =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("log10"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Radians =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("toRadians"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sign =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("signum"), in.ir), in.fields, in.variables, in.nullCheck))
    case functions.Sqrt =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, DoubleValue, AnyValue]("sqrt"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Range  if c.args.length == 2 =>
      for {start <- internalCompileExpression(c.args(0), currentContext)
           end <- internalCompileExpression(c.args(1), currentContext)
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("range"),
                                                  nullCheck(start)(start.ir), nullCheck(end)(end.ir)),
                                     start.fields ++ end.fields,
                                     start.variables ++ end.variables, Set.empty)

    case functions.Range  if c.args.length == 3 =>
      for {start <- internalCompileExpression(c.args(0), currentContext)
           end <- internalCompileExpression(c.args(1), currentContext)
           step <- internalCompileExpression(c.args(2), currentContext)
      } yield IntermediateExpression(invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue, AnyValue]("range"),
                                                  nullCheck(start)(start.ir), nullCheck(end)(end.ir), nullCheck(step)(step.ir)),
                                     start.fields ++ end.fields ++ step.fields,
                                     start.variables ++ end.variables ++ step.variables, Set.empty)

    case functions.Pi => Some(IntermediateExpression(getStatic[Values, DoubleValue]("PI"), Seq.empty, Seq.empty, Set.empty))
    case functions.E => Some(IntermediateExpression(getStatic[Values, DoubleValue]("E"), Seq.empty, Seq.empty, Set.empty))

    case functions.Coalesce =>
      val args = c.args.flatMap(internalCompileExpression(_, currentContext))
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
      for {p1 <- internalCompileExpression(c.args(0), currentContext)
           p2 <- internalCompileExpression(c.args(1), currentContext)
      } yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName,
                                       invokeStatic(method[CypherFunctions, Value, AnyValue, AnyValue]("distance"), p1.ir, p2.ir))
        IntermediateExpression(
          load(variableName),
          p1.fields ++ p2.fields,  p1.variables ++ p2.variables :+ local, Set(equal(load(variableName), noValue)))
      }

    case functions.StartNode =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess]("startNode"), in.ir, DB_ACCESS), in.fields, in.variables, in.nullCheck))

    case functions.EndNode =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, NodeValue, AnyValue, DbAccess]("endNode"), in.ir,
                                      DB_ACCESS), in.fields, in.variables, in.nullCheck))

    case functions.Nodes =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, ListValue, AnyValue]("nodes"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Relationships =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, ListValue, AnyValue]("relationships"), in.ir), in.fields, in.variables, in.nullCheck))

    case functions.Exists =>
      c.arguments.head match {
        case property: Property =>
          internalCompileExpression(property.map, currentContext).map(in => IntermediateExpression(
              invokeStatic(method[CypherFunctions, BooleanValue, String, AnyValue, DbAccess,
                                  NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyExists"),
                           constant(property.propertyKey.name),
                           in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR ),
            in.fields, in.variables ++ vCURSORS, in.nullCheck))
        case _: PatternExpression => None//TODO
        case _: NestedPipeExpression => None//TODO?
        case _: NestedPlanExpression => throw new InternalException("should have been rewritten away")
        case _ => None
      }

    case functions.Head =>
      internalCompileExpression(c.args.head, currentContext).map(in => {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName,  nullCheck(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("head"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      })

    case functions.Id =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, LongValue, AnyValue]("id"), in.ir),
        in.fields, in.variables, in.nullCheck))

    case functions.Labels =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
       invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess, NodeCursor]("labels"), in.ir, DB_ACCESS, NODE_CURSOR),
       in.fields, in.variables :+ vNODE_CURSOR, in.nullCheck))

    case functions.Type =>
      internalCompileExpression(c.args.head, currentContext).map(in => IntermediateExpression(
        invokeStatic(method[CypherFunctions, TextValue, AnyValue]("type"), in.ir),
         in.fields, in.variables, in.nullCheck))

    case functions.Last =>
      internalCompileExpression(c.args.head, currentContext).map(in => {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("last"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      })

    case functions.Left =>
      for {in <- internalCompileExpression(c.args(0), currentContext)
           endPos <- internalCompileExpression(c.args(1), currentContext)
      } yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("left"), in.ir, endPos.ir),
         in.fields ++ endPos.fields, in.variables ++ endPos.variables, in.nullCheck)
      }

    case functions.LTrim =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("ltrim"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.RTrim =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("rtrim"), in.ir),
         in.fields, in.variables, in.nullCheck)
      }

    case functions.Trim =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue]("trim"), in.ir),
         in.fields, in.variables, in.nullCheck)
      }

    case functions.Replace =>
      for {original <- internalCompileExpression(c.args(0), currentContext)
           search <- internalCompileExpression(c.args(1), currentContext)
           replaceWith <- internalCompileExpression(c.args(2), currentContext)
      } yield {
        IntermediateExpression(
            invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("replace"),
                         original.ir, search.ir, replaceWith.ir),
          original.fields ++ search.fields ++ replaceWith.fields, original.variables ++ search.variables ++ replaceWith.variables,
          original.nullCheck ++ search.nullCheck ++ replaceWith.nullCheck)
      }

    case functions.Reverse =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, AnyValue, AnyValue]("reverse"), in.ir), in.fields, in.variables, in.nullCheck)
      }

    case functions.Right =>
      for {in <- internalCompileExpression(c.args(0), currentContext)
           len <- internalCompileExpression(c.args(1), currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("right"), in.ir, len.ir),
          in.fields ++ len.fields, in.variables ++ len.variables, in.nullCheck)
      }

    case functions.Split =>
      for {original <- internalCompileExpression(c.args(0), currentContext)
           sep <- internalCompileExpression(c.args(1), currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, AnyValue]("split"), original.ir, sep.ir),
          original.fields ++ sep.fields, original.variables ++ sep.variables,
        original.nullCheck ++ sep.nullCheck)
      }

    case functions.Substring if c.args.size == 2 =>
      for {original <- internalCompileExpression(c.args(0), currentContext)
           start <- internalCompileExpression(c.args(1), currentContext)
      } yield {
        IntermediateExpression(
         invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue]("substring"), original.ir, start.ir),
          original.fields ++ start.fields, original.variables ++ start.variables, original.nullCheck)
      }

    case functions.Substring  =>
      for {original <- internalCompileExpression(c.args(0), currentContext)
           start <- internalCompileExpression(c.args(1), currentContext)
           len <- internalCompileExpression(c.args(2), currentContext)
      } yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue, AnyValue, AnyValue]("substring"),
                                              original.ir, start.ir, len.ir),
          original.fields ++ start.fields ++ len.fields,
          original.variables ++ start.variables ++ len.variables, original.nullCheck)
      }

    case functions.ToLower =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toLower"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.ToUpper =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toUpper"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Point =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, Value, AnyValue, DbAccess, ExpressionCursors]("point"),
            in.ir, DB_ACCESS, CURSORS),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Keys =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("keys"),
            in.ir, DB_ACCESS, NODE_CURSOR, RELATIONSHIP_CURSOR, PROPERTY_CURSOR),
          in.fields, in.variables ++ vCURSORS, in.nullCheck)
      }

    case functions.Size =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("size"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Length =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, IntegralValue, AnyValue]("length"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.Tail =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(
          invokeStatic(method[CypherFunctions, ListValue, AnyValue]("tail"), in.ir),
          in.fields, in.variables, in.nullCheck)
      }

    case functions.ToBoolean =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName,  nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toBoolean"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      }

    case functions.ToFloat =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toFloat"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      }

    case functions.ToInteger =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        val variableName = namer.nextVariableName()
        val local = variable[AnyValue](variableName, noValue)
        val lazySet = oneTime(assign(variableName, nullCheck(in)(invokeStatic(method[CypherFunctions, Value, AnyValue]("toInteger"), in.ir))))

        val ops = block(lazySet, load(variableName))
        val nullChecks = block(lazySet, equal(load(variableName), noValue))

        IntermediateExpression(ops, in.fields, in.variables :+ local, Set(nullChecks))
      }

    case functions.ToString =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
        IntermediateExpression(invokeStatic(method[CypherFunctions, TextValue, AnyValue]("toString"), in.ir), in.fields, in.variables, in.nullCheck)
      }

    case functions.Properties =>
      for (in <- internalCompileExpression(c.args.head, currentContext)) yield {
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

  private def contextSet(key: String, context: IntermediateRepresentation, value: IntermediateRepresentation): IntermediateRepresentation = {
    slots.get(key) match {
      case Some(LongSlot(offset, _, _)) =>
        setLongAt(offset, Some(context), value)
      case Some(RefSlot(offset, _, _)) =>
        setRefAt(offset, Some(context), value)
      case None =>
        invokeSideEffect(context, method[ExecutionContext, Unit, String, AnyValue]("set"), constant(key), value)
    }
  }

  private def getLongAt(offset: Int, currentContext: Option[IntermediateRepresentation]): IntermediateRepresentation =
    invoke(loadContext(currentContext), method[ExecutionContext, Long, Int]("getLongAt"),
           constant(offset))

  private def getRefAt(offset: Int, currentContext: Option[IntermediateRepresentation]): IntermediateRepresentation =
    invoke(loadContext(currentContext), method[ExecutionContext, AnyValue, Int]("getRefAt"),
           constant(offset))

  private def setRefAt(offset: Int, currentContext: Option[IntermediateRepresentation], value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadContext(currentContext), method[ExecutionContext, Unit, Int, AnyValue]("setRefAt"),
                     constant(offset), value)

  private def setLongAt(offset: Int, currentContext: Option[IntermediateRepresentation], value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(loadContext(currentContext), method[ExecutionContext, Unit, Int, Long]("setLongAt"),
                     constant(offset), value)

  private def loadContext(currentContext: Option[IntermediateRepresentation]) = currentContext.getOrElse(load("context"))
  private def nullCheck(expressions: IntermediateExpression*)(onNotNull: IntermediateRepresentation): IntermediateRepresentation = {
    val checks = expressions.foldLeft(Set.empty[IntermediateRepresentation])((acc, current) => acc ++ current.nullCheck)
    if (checks.nonEmpty) ternary(checks.reduceLeft(or), noValue, onNotNull)
    else onNotNull
  }

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
    generateCompositeBoolean(expressions, truthValue)

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

  private def accessVariable(name: String, currentContext: Option[IntermediateRepresentation]): (IntermediateRepresentation, Option[IntermediateRepresentation], Option[LocalVariable]) =
    slots.get(name) match {
    case Some(LongSlot(offset, true, CTNode)) =>
      (invokeStatic(method[CompiledHelpers, AnyValue, ExecutionContext, DbAccess, Int]("nodeOrNoValue"),
                    loadContext(currentContext), DB_ACCESS, constant(offset)), Option(equal(getLongAt(offset, currentContext), constant(-1L))), None)
    case Some(LongSlot(offset, false, CTNode)) =>
      (invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), getLongAt(offset, currentContext)), None, None)

    case Some(LongSlot(offset, true, CTRelationship)) =>
      (invokeStatic(method[CompiledHelpers, AnyValue, ExecutionContext, DbAccess, Int]("relationshipOrNoValue"),
                    loadContext(currentContext), DB_ACCESS, constant(offset)),
        Option(equal(getLongAt(offset, currentContext), constant(-1L))), None)
    case Some(LongSlot(offset, false, CTRelationship)) =>
      (invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"), getLongAt(offset, currentContext)), None, None)
    case Some(RefSlot(offset, nullable, _)) =>
      (getRefAt(offset, currentContext),
        if (nullable) Option(equal(getRefAt(offset, currentContext), noValue)) else None, None)
    case _ =>
      val local = namer.nextVariableName()
      (block(oneTime(
       assign(local,
                 invoke(loadContext(currentContext),
                   method[ExecutionContext, AnyValue, String]("getByName"), constant(name)))), load(local)),
        Some(equal(load(local), noValue)), Some(variable[AnyValue](local, constant(null))))
  }

  private def filterExpression(collectionExpression: Option[IntermediateExpression],
                               innerPredicate: Expression,
                               scopeVariable: String,
                               currentContext: Option[IntermediateRepresentation]): Option[IntermediateExpression] = {
    /*
       ListValue list = [evaluate collection expression];
       ExecutionContext innerContext = context.createClone();
       ArrayList<AnyValue> filtered = new ArrayList<>();
       Iterator<AnyValue> listIterator = list.iterator();
       while( listIterator.hasNext() )
       {
           AnyValue currentValue = listIterator.next();
           innerContext.set([name from scope], currentValue);
           Value isFiltered = [result from inner expression using innerContext]
           if (isFiltered == Values.TRUE)
           {
               filtered.add(currentValue);
           }
       }
       return VirtualValues.fromList(filtered);
      */
    val innerContext = namer.nextVariableName()
    val iterVariable = namer.nextVariableName()
    for {collection <- collectionExpression
         inner <- internalCompileExpression(innerPredicate,
                                            Some(load(innerContext))) //Note we update the context here
    } yield {
      val listVar = namer.nextVariableName()
      val filteredVars = namer.nextVariableName()
      val currentValue = namer.nextVariableName()
      val isFiltered = namer.nextVariableName()
      val ops = Seq(
        // ListValue list = [evaluate collection expression];
        // ExecutionContext innerContext = context.createClone();
        // ArrayList<AnyValue> filtered = new ArrayList<>();
        declare[ListValue](listVar),
        assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
        declare[ExecutionContext](innerContext),
        assign(innerContext,
               invoke(loadContext(currentContext), method[ExecutionContext, ExecutionContext]("createClone"))),
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
            //innerContext.set([name from scope], currentValue);
            contextSet(scopeVariable, load(innerContext), load(currentValue)),
            declare[Value](isFiltered),
            // Value isFiltered = [result from inner expression using innerContext]
            assign(isFiltered, nullCheck(inner)(inner.ir)),
            // if (isFiltered == Values.TRUE)
            // {
            //    filtered.add(currentValue);
            // }
            condition(equal(load(isFiltered), truthValue))(
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
                                scopeVariable: String,
                                currentContext: Option[IntermediateRepresentation]): Option[IntermediateExpression] = {
    /*
        extract is tricky because it modifies the scope for future expressions. The generated code will be something
        along the line of:

        ListValue list = [evaluate collection expression];
        ExecutionContext innerContext = context.createClone();
        ArrayList<AnyValue> extracted = new ArrayList<>();
        for ( AnyValue currentValue : list ) {
            innerContext.set([name from scope], currentValue);
            extracted.add([result from inner expression using innerContext]);
        }
        return VirtualValues.fromList(extracted);
       */
    //this is the context inner expressions should see
    val innerContext = namer.nextVariableName()
    val iterVariable = namer.nextVariableName()
    for {collection <- collectionExpression
         inner <- internalCompileExpression(extractExpression,
                                            Some(load(innerContext))) //Note we update the context here
    } yield {
      val listVar = namer.nextVariableName()
      val extractedVars = namer.nextVariableName()
      val currentValue = namer.nextVariableName()
      val ops = Seq(
        //ListValue list = [evaluate collection expression];
        //ExecutionContext innerContext = context.createClone();
        //ArrayList<AnyValue> extracted = new ArrayList<>();
        declare[ListValue](listVar),
        assign(listVar, invokeStatic(method[CypherFunctions, ListValue, AnyValue]("asList"), collection.ir)),
        declare[ExecutionContext](innerContext),
        assign(innerContext,
               invoke(loadContext(currentContext), method[ExecutionContext, ExecutionContext]("createClone"))),
        declare[java.util.ArrayList[AnyValue]](extractedVars),
        assign(extractedVars, newInstance(constructor[java.util.ArrayList[AnyValue]])),
        //Iterator<AnyValue> iter = list.iterator();
        //while (iter.hasNext) {
        //   AnyValue currentValue = iter.next();
        declare[java.util.Iterator[AnyValue]](iterVariable),
        assign(iterVariable, invoke(load(listVar), method[ListValue, java.util.Iterator[AnyValue]]("iterator"))),
        loop(invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Boolean]("hasNext"))) {
          block(Seq(
            declare[AnyValue](currentValue),
            assign(currentValue,
                   cast[AnyValue](invoke(load(iterVariable), method[java.util.Iterator[AnyValue], Object]("next")))),
            //innerContext.set([name from scope], currentValue);
            contextSet(scopeVariable, load(innerContext), load(currentValue)),
            //extracted.add([result from inner expression using innerContext]);
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
  private def compileStaticPath(currentContext: Option[IntermediateRepresentation], steps: PathStep) = {

    @tailrec
    def compileSteps(step: PathStep,
                     nodeOps: Seq[IntermediateExpression] = ArrayBuffer.empty,
                     relOps: Seq[IntermediateExpression] = ArrayBuffer.empty): Option[(Seq[IntermediateExpression], Seq[IntermediateExpression])] = step match {
      case NodePathStep(node, next) => compileSteps(next, nodeOps ++ internalCompileExpression(node, currentContext), relOps)

      case SingleRelationshipPathStep(relExpression, _, Some(targetExpression), next) =>
        (internalCompileExpression(relExpression, currentContext), internalCompileExpression(targetExpression, currentContext)) match {
        case (Some(rel), Some(target)) => compileSteps(next, nodeOps :+ target, relOps :+ rel)
        case _ => None
      }

      case SingleRelationshipPathStep(rel, SemanticDirection.BOTH, _, next) => internalCompileExpression(rel,
                                                                                                      currentContext) match {
        case Some(compiled) =>
          val relVar = variable[RelationshipValue](namer.nextVariableName(), constant(null))
          val lazyRel = oneTime(assign(relVar.name, cast[RelationshipValue](compiled.ir)))
          val node = IntermediateExpression(
            block(lazyRel,
                  invokeStatic(method[CypherFunctions, NodeValue, VirtualRelationshipValue, DbAccess, VirtualNodeValue]("otherNode"),
                               load(relVar.name), DB_ACCESS,  nodeOps.last.ir)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullCheck ++ nodeOps.last.nullCheck)
          val rel = IntermediateExpression(
            block(lazyRel, load(relVar.name)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullCheck)

          compileSteps(next, nodeOps :+ node, relOps :+ rel)
        case None => None
      }

      case SingleRelationshipPathStep(rel, direction, _, next) => internalCompileExpression(rel, currentContext) match {
        case Some(compiled) =>
          val methodName = if (direction == SemanticDirection.INCOMING) "startNode" else "endNode"
          val relVar = variable[RelationshipValue](namer.nextVariableName(), constant(null))
          val lazyRel = oneTime(assign(relVar.name, cast[RelationshipValue](compiled.ir)))
          val node = IntermediateExpression(
            block(lazyRel,
              invokeStatic(method[CypherFunctions, NodeValue, VirtualRelationshipValue, DbAccess](methodName),
                           load(relVar.name), DB_ACCESS)),
            compiled.fields, compiled.variables :+ relVar, compiled.nullCheck)
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
                                       arrayOf[NodeValue](nodeOps.map(_.ir): _*),
                                       arrayOf[RelationshipValue](relOps.map(_.ir): _*))
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

  private def compileDynamicPath(currentContext: Option[IntermediateRepresentation], steps: PathStep) = {

    val builderVar = namer.nextVariableName()
    @tailrec
    def compileSteps(step: PathStep, acc: Seq[IntermediateExpression] = ArrayBuffer.empty): Option[Seq[IntermediateExpression]] = step match {
      case NodePathStep(node, next) => internalCompileExpression(node, currentContext) match {
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

      case SingleRelationshipPathStep(rel, direction, _, next) =>  internalCompileExpression(rel, currentContext) match {
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
      case MultiRelationshipPathStep(rel, direction, _,next) => internalCompileExpression(rel, currentContext) match {
        case Some(relOps) =>
          val methodName = direction match {
            case SemanticDirection.INCOMING => "addMultipleIncoming"
            case SemanticDirection.OUTGOING => "addMultipleOutgoing"
            case _ => "addMultipleUndirected"
          }

          val addRels =
            if (relOps.nullCheck.isEmpty) invokeSideEffect(load(builderVar),
                                                 method[PathValueBuilder, Unit, ListValue](methodName),
                                                 cast[ListValue](relOps.ir))
            else invokeSideEffect(load(builderVar), method[PathValueBuilder, Unit, AnyValue](methodName),
                        nullCheck(relOps)(relOps.ir))
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
            assign(builderVar, newInstance(constructor[PathValueBuilder, DbAccess], DB_ACCESS)))
            ++ pathOps.map(_.ir) :+ assign(variableName, invoke(load(builderVar), method[PathValueBuilder, AnyValue]("build"))): _*))


      val ops = block(lazySet, load(variableName))
      val nullChecks =
        if (pathOps.forall(_.nullCheck.isEmpty)) Set.empty[IntermediateRepresentation]
        else Set(block(lazySet, equal(load(variableName), noValue)))

      IntermediateExpression(ops,
                             pathOps.flatMap(_.fields),
                             pathOps.flatMap(_.variables) :+ local,
                             nullChecks)
    }
  }
}

object IntermediateCodeGeneration {
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

  private def cursorVariable[T](name: String)(implicit m: Manifest[T]): LocalVariable =
    variable[T](name, invoke(load("cursors"), method[ExpressionCursors, T](name)))
}
