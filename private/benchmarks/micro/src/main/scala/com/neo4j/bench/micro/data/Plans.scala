/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data

import com.neo4j.bench.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.TypeParamValues.DATE
import com.neo4j.bench.micro.data.TypeParamValues.DATE_TIME
import com.neo4j.bench.micro.data.TypeParamValues.DBL
import com.neo4j.bench.micro.data.TypeParamValues.DURATION
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.TypeParamValues.LOCAL_DATE_TIME
import com.neo4j.bench.micro.data.TypeParamValues.LOCAL_TIME
import com.neo4j.bench.micro.data.TypeParamValues.POINT
import com.neo4j.bench.micro.data.TypeParamValues.STR_BIG
import com.neo4j.bench.micro.data.TypeParamValues.STR_SML
import com.neo4j.bench.micro.data.TypeParamValues.TIME
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Bound
import org.neo4j.cypher.internal.logical.plans.Bounds
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRange
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

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

  def astOrs(es: Expression*): Expression = Ors(es)(Pos)

  def astAnds(es: Expression*): Ands = Ands(es)(Pos)

  def astMap(kvs: (String, Expression)*): Expression = MapExpression(kvs.map {
    case (key, value) => PropertyKeyName(key)(Pos) -> value
  })(Pos)

  def astReduce(accumulator: String, init: Expression, variable: String, list: Expression, expression: Expression): Expression =
    ReduceExpression(astVariable(accumulator), init, astVariable(variable), list, expression)(Pos)

  def astExtract(variable: String, list: Expression, expression: Expression): Expression =
    astListComprehension(variable, list, None, Some(expression))

  def astFilter(variable: String, list: Expression, predicate: Expression): Expression =
    astListComprehension(variable, list, Some(predicate), None)

  def astListComprehension(variable: String, list: Expression, predicate: Option[Expression], extractExpression: Option[Expression]): Expression =
    expressions.ListComprehension(astVariable(variable), list, predicate, extractExpression)(Pos)

  def astAny(variable: String, list: Expression, predicate: Expression): Expression =
    expressions.AnyIterablePredicate(astVariable(variable), list, Some(predicate))(Pos)

  def astMapProjection(name: String, includeAllProps: Boolean, items: Seq[(String,Expression)]): Expression =
    DesugaredMapProjection(astVariable(name), items.map(kv => LiteralEntry(PropertyKeyName(kv._1)(Pos), kv._2)(Pos)), includeAllProps)(Pos)

  def astCase(input: Expression, alternatives: Seq[(Expression, Expression)], default: Option[Expression]): Expression =
    expressions.CaseExpression(Some(input), alternatives.toIndexedSeq, default)(Pos)
  def astIn(lhs: Expression, rhs: Expression): Expression = In(lhs, rhs)(Pos)

  def astListLiteral(expressions: Seq[Expression]): Expression = ListLiteral(expressions)(Pos)

  def astPathExpression(step: NodePathStep): Expression = PathExpression(step)(Pos)

  def astHasLabels(node: String, labels: String*): Expression = HasLabels(astVariable(node), labels.map(l => LabelName(l)(Pos)))(Pos)

  def astHasTypes(relationship: String, types: String*): Expression = HasTypes(astVariable(relationship), types.map(t => RelTypeName(t)(Pos)))(Pos)

  def astNestedExists(plan: LogicalPlan): Expression = NestedPlanExistsExpression(plan, "")(Pos)

  def astNestedCollect(plan: LogicalPlan, projection: Expression): Expression = NestedPlanCollectExpression(plan, projection, "")(Pos)

  def astTrue: Expression = True()(Pos)

  def astFalse: Expression = False()(Pos)
}
