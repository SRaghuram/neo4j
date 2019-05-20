/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.utils

import com.neo4j.fabric.AstHelp
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.symbols.AnyType
import org.scalacheck._

case class AstGenerator(debug: Boolean = true) extends AstHelp {

  def _boolean: Gen[Boolean] =
    Gen.oneOf(true, false)

  def _string: Gen[String] =
    if (debug) Gen.alphaLowerChar.map(_.toString)
    else Gen.listOf(Gen.asciiChar).map(_.mkString)

  // IDENTIFIERS
  // ==========================================================================

  def _identifier: Gen[String] =
    if (debug) _string
    else Gen.identifier

  def _labelName: Gen[LabelName] =
    _identifier.map(LabelName(_)(?))

  def _relTypeName: Gen[RelTypeName] =
    _identifier.map(RelTypeName(_)(?))

  def _propertyKeyName: Gen[PropertyKeyName] =
    _identifier.map(PropertyKeyName(_)(?))

  // EXPRESSIONS
  // ==========================================================================

  def _stringLit: Gen[StringLiteral] =
    _string.flatMap(StringLiteral(_)(?))

  def _booleanLit: Gen[BooleanLiteral] =
    Gen.oneOf(True()(?), False()(?))

  def _signedIntLit: Gen[SignedIntegerLiteral] =
    Gen.posNum[Int].map(_.toString).map(SignedDecimalIntegerLiteral(_)(?))

  def _unsignedIntLit: Gen[UnsignedIntegerLiteral] =
    Gen.posNum[Int].map(_.toString).map(UnsignedDecimalIntegerLiteral(_)(?))

  def _variable: Gen[Variable] = for {
    name <- _identifier
  } yield Variable(name)(?)

  def _predicateComparison: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- Gen.oneOf(
      GreaterThanOrEqual(l, r)(?),
      GreaterThan(l, r)(?),
      LessThanOrEqual(l, r)(?),
      LessThan(l, r)(?),
      Equals(l, r)(?),
      Equivalent(l, r)(?),
      NotEquals(l, r)(?),
      InvalidNotEquals(l, r)(?),
    )
  } yield res

  def _predicateUnary: Gen[Expression] = for {
    r <- _expression
    res <- Gen.oneOf(
      Not(r)(?),
      IsNull(r)(?),
      IsNotNull(r)(?)
    )
  } yield res

  def _predicateBinary: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- Gen.oneOf(
      And(l, r)(?),
      Or(l, r)(?),
      Xor(l, r)(?),
      RegexMatch(l, r)(?),
      In(l, r)(?),
      StartsWith(l, r)(?),
      EndsWith(l, r)(?),
      Contains(l, r)(?)
    )
  } yield res

  def _predicateNary: Gen[Expression] = for {
    s <- Gen.choose(3, 5)
    l <- Gen.listOfN(s, _expression)
    res <- Gen.oneOf(
      Ands(l.toSet)(?),
      Ors(l.toSet)(?),

    )
  } yield res

  def _mapItem: Gen[(PropertyKeyName, Expression)] = for {
    key <- _propertyKeyName
    value <- _expression
  } yield (key, value)

  def _map: Gen[MapExpression] = for {
    items <- Gen.listOf(_mapItem)
  } yield MapExpression(items)(?)

  def _parameter: Gen[Parameter] =
    _identifier.map(Parameter(_, AnyType.instance)(?))

  def _expression: Gen[Expression] =
    Gen.frequency(
      10 -> Gen.oneOf(
        Gen.lzy(_stringLit),
        Gen.lzy(_booleanLit),
        Gen.lzy(_signedIntLit),
        Gen.lzy(_variable)
      ),
      1 -> Gen.oneOf(
        Gen.lzy(_predicateComparison),
        Gen.lzy(_predicateUnary),
        Gen.lzy(_predicateBinary)
        //        Gen.lzy(_predicateNary)
      )
    )

  // PATTERNS
  // ==========================================================================

  def _nodePattern: Gen[NodePattern] = for {
    variable <- Gen.option(_variable)
    labels <- Gen.listOf(_labelName)
    properties <- Gen.option(Gen.oneOf(_map, _parameter))
    baseNode <- Gen.option(_variable)
  } yield NodePattern(variable, labels, properties, baseNode)(?)

  def _range: Gen[Range] = for {
    lower <- Gen.option(_unsignedIntLit)
    upper <- Gen.option(_unsignedIntLit)
  } yield Range(lower, upper)(?)

  def _semanticDirection: Gen[SemanticDirection] =
    Gen.oneOf(OUTGOING, INCOMING, BOTH)

  def _relationshipPattern: Gen[RelationshipPattern] = for {
    variable <- Gen.option(_variable)
    types <- Gen.listOf(_relTypeName)
    length <- Gen.option(Gen.option(_range))
    properties <- Gen.option(Gen.oneOf(_map, _parameter))
    direction <- _semanticDirection
    baseRel <- Gen.option(_variable)
  } yield RelationshipPattern(variable, types, length, properties, direction, false, baseRel)(?)

  def _relationshipChain: Gen[RelationshipChain] = for {
    element <- _patternElement
    relationship <- _relationshipPattern
    rightNode <- _nodePattern
  } yield RelationshipChain(element, relationship, rightNode)(?)

  def _patternElement: Gen[PatternElement] = Gen.oneOf(
    _nodePattern,
    Gen.lzy(_relationshipChain)
  )

  def _anonPatternPart: Gen[AnonymousPatternPart] = for {
    element <- _patternElement
    single <- _boolean
    part <- Gen.oneOf(
      EveryPath(element),
      ShortestPaths(element, single)(?)
    )
  } yield part

  def _namedPatternPart: Gen[NamedPatternPart] = for {
    variable <- _variable
    part <- _anonPatternPart
  } yield NamedPatternPart(variable, part)(?)

  def _patternPart: Gen[PatternPart] =
    Gen.oneOf(
      _anonPatternPart,
      _namedPatternPart
    )

  def _pattern: Gen[Pattern] = for {
    parts <- Gen.nonEmptyListOf(_patternPart)
  } yield Pattern(parts)(?)

  // HINTS
  // ==========================================================================

  def _usingIndexHint: Gen[UsingIndexHint] = for {
    variable <- _variable
    label <- _labelName
    properties <- Gen.nonEmptyListOf(_propertyKeyName)
    spec <- Gen.oneOf(SeekOnly, SeekOrScan)
  } yield UsingIndexHint(variable, label, properties, spec)(?)

  def _usingJoinHint: Gen[UsingJoinHint] = for {
    variables <- Gen.nonEmptyListOf(_variable)
  } yield UsingJoinHint(variables)(?)

  def _usingScanHint: Gen[UsingScanHint] = for {
    variable <- _variable
    label <- _labelName
  } yield UsingScanHint(variable, label)(?)

  def _hint: Gen[UsingHint] =
    Gen.oneOf(_usingIndexHint, _usingJoinHint, _usingScanHint)

  // CLAUSES
  // ==========================================================================

  def _returnItem: Gen[ReturnItem] = for {
    expr <- _expression
    variable <- _variable
    item <- Gen.oneOf(
      UnaliasedReturnItem(expr, "")(?),
      AliasedReturnItem(expr, variable)(?)
    )
  } yield item

  def _sortItem: Gen[SortItem] = for {
    expr <- _expression
    item <- Gen.oneOf(
      AscSortItem(expr)(?),
      DescSortItem(expr)(?)
    )
  } yield item

  def _orderBy: Gen[OrderBy] = for {
    items <- Gen.nonEmptyListOf(_sortItem)
  } yield OrderBy(items)(?)

  def _skip: Gen[Skip] =
    _expression.map(Skip(_)(?))

  def _limit: Gen[Limit] =
    _expression.map(Limit(_)(?))

  def _where: Gen[Where] =
    _expression.map(Where(_)(?))

  def _returnItems1: Gen[ReturnItems] = for {
    retItems <- Gen.nonEmptyListOf(_returnItem)
  } yield ReturnItems(includeExisting = false, retItems)(?)

  def _returnItems2: Gen[ReturnItems] = for {
    retItems <- Gen.listOf(_returnItem)
  } yield ReturnItems(includeExisting = true, retItems)(?)

  def _returnItems: Gen[ReturnItems] =
    Gen.oneOf(_returnItems1, _returnItems2)

  def _with: Gen[With] = for {
    distinct <- _boolean
    inclExisting <- _boolean
    retItems <- Gen.nonEmptyListOf(_returnItem)
    orderBy <- Gen.option(_orderBy)
    skip <- Gen.option(_skip)
    limit <- Gen.option(_limit)
    where <- Gen.option(_where)
  } yield With(distinct, ReturnItems(inclExisting, retItems)(?), orderBy, skip, limit, where)(?)

  def _return: Gen[Return] = for {
    distinct <- _boolean
    inclExisting <- _boolean
    retItems <- Gen.nonEmptyListOf(_returnItem)
    orderBy <- Gen.option(_orderBy)
    skip <- Gen.option(_skip)
    limit <- Gen.option(_limit)
  } yield Return(distinct, ReturnItems(inclExisting, retItems)(?), orderBy, skip, limit)(?)

  def _match: Gen[Match] = for {
    optional <- _boolean
    pattern <- _pattern
    hints <- Gen.listOf(_hint)
    where <- Gen.option(_where)
  } yield Match(optional, pattern, hints, where)(?)

  def _clause: Gen[Clause] = Gen.oneOf(
    _with,
    _return,
    _match
    //  Create
    //  Unwind
    //  UnresolvedCall
    //  SetClause
    //  Delete
    //  Merge
    //  LoadCSV
    //  Foreach
    //  Start
    //  CreateUnique
  )

  def _query: Gen[Query] = for {
    s <- Gen.choose(1, 10)
    clauses <- Gen.listOfN(s, _clause)
  } yield Query(None, SingleQuery(clauses)(?))(?)


  object Shrinker {

    import com.neo4j.fabric.utils.Monoid._
    import com.neo4j.fabric.utils.Rewritten._
    import com.neo4j.fabric.utils.TreeFoldM._

    import scala.util.{Random, Success, Try}

    implicit val IntAddMonoid: Monoid[Int] = Monoid.create(0)(_ + _)

    def shrinkOnce(q: Query): Try[Option[Query]] = {
      val splitPoints = q.treeFoldM {
        case _: List[_] => Descend(1)
      }
      if (splitPoints == 0) {
        Success(None)
      } else {
        var point = Random.nextInt(splitPoints)
        Try(Some(
          q.rewritten.bottomUp {
            case l: List[_] if point > 0 =>
              point = point - 1
              l

            case l: List[_] if point == 0 =>
              point = point - 1
              println(s"-- dropping @ $l")
              Nil
          }))
      }
    }

    implicit val shrinkQuery: Shrink[Query] = Shrink[Query] { q =>
      Stream.continually(shrinkOnce(q))
        .collect {
          case Success(opt) => opt
        }
        .takeWhile(_.isDefined)
        .map(_.get)
    }
  }

}
