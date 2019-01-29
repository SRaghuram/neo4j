package com.neo4j.bench.micro.data


import com.neo4j.bench.micro.data.TypeParamValues.{DBL, LNG, STR_BIG, STR_SML}
import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket;

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.ast.{InequalitySeekRangeWrapper, PrefixSeekRangeWrapper}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast.{PropertyKeyToken, _}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_3.{Bound, Bounds, ExclusiveBound, InclusiveBound, InputPosition, LabelId, PropertyKeyId, ast, symbols}
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality, CardinalityEstimation, PlannerQuery, RegularPlannerQuery}
import org.neo4j.cypher.internal.v3_3.logical.plans.{QueryExpression, RangeQueryExpression}
import org.neo4j.graphdb.{Label, RelationshipType}

object Plans {
  val Pos: InputPosition = InputPosition.NONE
  val Solved: RegularPlannerQuery with CardinalityEstimation = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(0))

  def cypherTypeFor(aType: String): CypherType = aType match {
    case LNG => symbols.CTInteger
    case DBL => symbols.CTFloat
    case STR_SML => symbols.CTString
    case _ => throw new IllegalArgumentException(s"Unsupported type: $aType")
  }

  def astRelTypeName(relType: RelationshipType): RelTypeName =
    ast.RelTypeName(relType.name)(Pos)

  def astPropertyKeyToken(key: String, planContext: PlanContext): PropertyKeyToken =
    PropertyKeyToken(key, astPropertyKeyId(key, planContext))

  def astPropertyKeyId(key: String, planContext: PlanContext): PropertyKeyId =
    PropertyKeyId(planContext.getPropertyKeyId(key))

  def astLabelToken(label: Label, planContext: PlanContext) =
    LabelToken(label.name, astLabelId(label, planContext))

  def astLabelId(label: Label, planContext: PlanContext): LabelId =
    LabelId(planContext.getLabelId(label.name))

  def astLabelName(label: Label): LabelName =
    LabelName(label.name)(Pos)

  def astProperty(variable: Variable, key: String): Property =
    Property(variable, PropertyKeyName(key)(Pos))(Pos)

  def astFunctionInvocation(functionName: String, variable: Variable): FunctionInvocation =
    FunctionInvocation(astFunctionName(functionName), variable)(Pos)

  def astFunctionName(functionName: String): FunctionName =
    FunctionName(functionName)(Pos)

  def astParameter(paramName: String, paramType: CypherType): Parameter =
    Parameter(paramName, paramType)(Pos)

  def astVariable(name: String): Variable =
    Variable(name)(Pos)

  def astEquals(expression1: Expression, expression2: Expression): Expression =
    Equals(expression1, expression2)(Pos)

  def astNot(expression: Expression): Expression =
    Not(expression)(Pos)

  def astLiteralFor(bucket: Bucket, valueType: String): Literal =
    astLiteralFor(bucket.value(), valueType)

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

  def astLiteralFor(value: Any, valueType: String): Literal = valueType match {
    case LNG => SignedDecimalIntegerLiteral(value.toString)(Pos)
    case DBL => DecimalDoubleLiteral(value.toString)(Pos)
    case STR_SML => StringLiteral(value.toString)(Pos)
    case STR_BIG => StringLiteral(value.toString)(Pos)
    case _ => throw new IllegalArgumentException(s"Invalid type: $valueType")
  }
}
