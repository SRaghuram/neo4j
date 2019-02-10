/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data

import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.TypeParamValues._
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanContext
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.graphdb.{Label, RelationshipType}

object Plans {
  val IdGen = new SequentialIdGen()
  val Pos: InputPosition = InputPosition.NONE

  def cypherTypeFor(aType: String): CypherType = aType match {
    case LNG => symbols.CTInteger
    case DBL => symbols.CTFloat
    case STR_SML => symbols.CTString
    case DATE_TIME => symbols.CTDateTime
    case LOCAL_DATE_TIME => symbols.CTLocalDateTime
    case TIME => symbols.CTTime
    case DATE => symbols.CTDate
    case LOCAL_TIME => symbols.CTLocalTime
    case DURATION => symbols.CTDuration
    case POINT => symbols.CTPoint
    case _ => throw new IllegalArgumentException(s"Unsupported type: $aType")
  }

  def astRelTypeName(relType: RelationshipType): RelTypeName =
    RelTypeName(relType.name)(Pos)

  def astPropertyKeyToken(key: String, planContext: PlanContext): PropertyKeyToken =
    PropertyKeyToken(key, astPropertyKeyId(key, planContext))

  def astPropertyKeyId(key: String, planContext: PlanContext): PropertyKeyId =
    PropertyKeyId(planContext.getPropertyKeyId(key))

  def astLabelToken(label: Label, planContext: PlanContext) =
    expressions.LabelToken(label.name, astLabelId(label, planContext))

  def astLabelId(label: Label, planContext: PlanContext): LabelId =
    LabelId(planContext.getLabelId(label.name))

  def astLabelName(label: Label): LabelName =
    LabelName(label.name)(Pos)

  def astProperty(variable: Variable, key: String): Property =
    Property(variable, PropertyKeyName(key)(Pos))(Pos)

  def astFunctionInvocation(functionName: String, parameters: Expression*): FunctionInvocation =
    FunctionInvocation(astFunctionName(functionName), distinct = false, parameters.toIndexedSeq)(Pos)

  def astFunctionName(functionName: String): FunctionName =
    FunctionName(functionName)(Pos)

  def astParameter(paramName: String, paramType: CypherType): Parameter =
    Parameter(paramName, paramType)(Pos)

  def astVariable(name: String): Variable =
    Variable(name)(Pos)

  def astPointFunctionFor(xKey: String, xValue: Double, yKey: String, yValue: Double): FunctionInvocation =
    astPointFunctionFor(xKey, astLiteralFor(xValue, DBL), yKey, astLiteralFor(yValue, DBL))

  def astPointFunctionFor(xKey: String, xValue: Expression, yKey: String, yValue: Expression): FunctionInvocation = {
    val xKeyName = PropertyKeyName(xKey)(Pos)
    val yKeyName = PropertyKeyName(yKey)(Pos)
    val points = MapExpression(List((xKeyName, xValue), (yKeyName, yValue)))(Pos)
    FunctionInvocation(
      Namespace(List())(Pos),
      FunctionName("point")(Pos),
      distinct = false,
      IndexedSeq(points))(Pos)
  }

  def astStringPrefixQueryExpression(prefix: StringLiteral): QueryExpression[Expression] = {
    val range: PrefixRange[Expression] = PrefixRange(prefix)
    val rangeWrapper: PrefixSeekRangeWrapper = PrefixSeekRangeWrapper(range)(Pos)
    val rangeQuery: RangeQueryExpression[PrefixSeekRangeWrapper] = RangeQueryExpression(rangeWrapper)
    rangeQuery
  }

  def astRangeBetweenQueryExpression(lower: Literal, upper: Literal): QueryExpression[Expression] = {
    val rangeLessThan: RangeLessThan[Literal] = astRangeLessThan(upper, true)
    val rangeGreaterThan: RangeGreaterThan[Literal] = astRangeGreaterThan(lower, true)
    val rangeBetween: InequalitySeekRange[Literal] = RangeBetween(rangeGreaterThan, rangeLessThan)
    val rangeWrapper: InequalitySeekRangeWrapper = InequalitySeekRangeWrapper(rangeBetween)(Pos)
    val rangeQuery: RangeQueryExpression[InequalitySeekRangeWrapper] = RangeQueryExpression(rangeWrapper)
    rangeQuery
  }

  def astRangeLessThanQueryExpression(upper: Literal): QueryExpression[Expression] = {
    val rangeLessThan: RangeLessThan[Literal] = astRangeLessThan(upper, true)
    val rangeWrapper: InequalitySeekRangeWrapper = InequalitySeekRangeWrapper(rangeLessThan)(Pos)
    val rangeQuery: RangeQueryExpression[InequalitySeekRangeWrapper] = RangeQueryExpression(rangeWrapper)
    rangeQuery
  }

  def astRangeGreaterThanQueryExpression(lower: Literal): QueryExpression[Expression] = {
    val rangeGreaterThan: RangeGreaterThan[Literal] = astRangeGreaterThan(lower, true)
    val rangeWrapper: InequalitySeekRangeWrapper = InequalitySeekRangeWrapper(rangeGreaterThan)(Pos)
    val rangeQuery: RangeQueryExpression[InequalitySeekRangeWrapper] = RangeQueryExpression(rangeWrapper)
    rangeQuery
  }

  def astRangeBetweenPointsQueryExpression(point: Expression, distance: Literal): QueryExpression[Expression] = {
    val pointDistanceRange = PointDistanceRange[Expression](point, distance, inclusive = true)
    val rangeWrapper = PointDistanceSeekRangeWrapper(pointDistanceRange)(Pos)
    val rangeQuery = RangeQueryExpression(rangeWrapper)
    rangeQuery
  }

  def distanceFunction(point1: Expression, point2: Expression) =
    FunctionInvocation(
      Namespace(List())(Pos),
      FunctionName("distance")(Pos),
      distinct = false,
      IndexedSeq(point1, point2))(Pos)

  private def astRangeLessThan(upper: Literal, inclusive: Boolean): RangeLessThan[Literal] =
    RangeLessThan(astBounds(upper, true))

  private def astRangeGreaterThan(lower: Literal, inclusive: Boolean): RangeGreaterThan[Literal] =
    RangeGreaterThan(astBounds(lower, true))

  private def astBounds(lower: Literal, inclusive: Boolean): Bounds[Literal] =
    NonEmptyList(astBound(lower, inclusive))

  private def astBound(lower: Literal, inclusive: Boolean): Bound[Literal] =
    if (inclusive)
      InclusiveBound(lower)
    else
      ExclusiveBound(lower)

  def astLiteralFor(bucket: Bucket, valueType: String): Literal =
    astLiteralFor(bucket.value(), valueType)

  def astLiteralFor(value: Any, valueType: String): Literal = valueType match {
    case LNG => SignedDecimalIntegerLiteral(value.toString)(Pos)
    case DBL => DecimalDoubleLiteral(value.toString)(Pos)
    case STR_SML => StringLiteral(value.toString)(Pos)
    case STR_BIG => StringLiteral(value.toString)(Pos)
    case _ => throw new IllegalArgumentException(s"Invalid type: $valueType")
  }

  def astEquals(expression1: Expression, expression2: Expression): Expression = Equals(expression1, expression2)(Pos)

  def astNot(expression: Expression): Expression = Not(expression)(Pos)

  def astGte(l: Expression, r: Expression): Expression = GreaterThanOrEqual(l, r)(Pos)

  def astGt(l: Expression, r: Expression): Expression = GreaterThan(l, r)(Pos)

  def astLte(l: Expression, r: Expression): Expression = LessThanOrEqual(l, r)(Pos)

  def astLt(l: Expression, r: Expression): Expression = LessThan(l, r)(Pos)

  def astMultiply(l: Expression, r: Expression): Expression = Multiply(l, r)(Pos)

  def astAdd(l: Expression, r: Expression): Expression = Add(l, r)(Pos)

  def astOrs(es: Expression*): Expression = Ors(es.toSet)(Pos)

  def astAnds(es: Expression*): Expression = Ands(es.toSet)(Pos)

  def astMap(kvs: (String, Expression)*): Expression = MapExpression(kvs.map {
                                                                               case (key, value) => PropertyKeyName(key)(Pos) -> value
                                                                             })(Pos)

  def astReduce(accumulator: String, init: Expression, variable: String, list: Expression, expression: Expression): Expression =
    ReduceExpression(astVariable(accumulator), init, astVariable(variable), list, expression)(Pos)

  def astExtract(variable: String, list: Expression, expression: Expression): Expression =
    ExtractExpression(astVariable(variable), list, None, Some(expression))(Pos)

  def astFilter(variable: String, list: Expression, predicate: Expression): Expression =
    expressions.FilterExpression(astVariable(variable), list, Some(predicate))(Pos)

  def astAny(variable: String, list: Expression, predicate: Expression): Expression =
    expressions.AnyIterablePredicate(astVariable(variable), list, Some(predicate))(Pos)

  def astMapProjection(name: String, includeAllProps: Boolean, items: Seq[(String,Expression)]): Expression =
    DesugaredMapProjection(astVariable(name), items.map(kv => LiteralEntry(PropertyKeyName(kv._1)(Pos), kv._2)(Pos)), includeAllProps)(Pos)

  def astCase(input: Expression, alternatives: Seq[(Expression, Expression)], default: Option[Expression]): Expression =
    expressions.CaseExpression(Some(input), alternatives.toIndexedSeq, default)(Pos)
  def astIn(lhs: Expression, rhs: Expression): Expression = In(lhs, rhs)(Pos)

  def astListLiteral(expressions: Seq[Expression]): Expression = ListLiteral(expressions)(Pos)

  def astPathExpression(step: NodePathStep): Expression = PathExpression(step)(Pos)
}
