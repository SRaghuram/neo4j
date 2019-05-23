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
import org.scalacheck.util.Buildable

case class AstGenerator(debug: Boolean = true) extends AstHelp {

  def _boolean: Gen[Boolean] =
    Gen.oneOf(true, false)

  def _string: Gen[String] =
    if (debug) Gen.alphaLowerChar.map(_.toString)
    else Gen.listOf(Gen.asciiChar).map(_.mkString)

  def _smallListOf[T](gen: Gen[T]): Gen[List[T]] =
    Gen.choose(0, 3).flatMap(Gen.listOfN(_, gen))

  def _smallNonemptyListOf[T](gen: Gen[T]): Gen[List[T]] =
    Gen.choose(1, 3).flatMap(Gen.listOfN(_, gen))

  def _tuple[A, B](ga: Gen[A], gb: Gen[B]): Gen[(A, B)] = for {
    a <- ga
    b <- gb
  } yield (a, b)

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

  def _nullLit: Gen[Null] =
    Gen.const(Null.NULL)

  def _stringLit: Gen[StringLiteral] =
    _string.flatMap(StringLiteral(_)(?))

  def _booleanLit: Gen[BooleanLiteral] =
    Gen.oneOf(True()(?), False()(?))

  def _unsignedIntString(prefix: String, radix: Int): Gen[String] = for {
    num <- Gen.posNum[Int]
    str = Integer.toString(num, radix)
  } yield List(prefix, str).mkString

  def _signedIntString(prefix: String, radix: Int): Gen[String] = for {
    str <- _unsignedIntString(prefix, radix)
    neg <- _boolean
    sig = if (neg) "-" else ""
  } yield List(sig, str).mkString

  def _unsignedIntLit: Gen[UnsignedDecimalIntegerLiteral] =
    _unsignedIntString("", 10).map(UnsignedDecimalIntegerLiteral(_)(?))

  def _signedIntLit: Gen[SignedDecimalIntegerLiteral] =
    _signedIntString("", 10).map(SignedDecimalIntegerLiteral(_)(?))

  def _signedHexIntLit: Gen[SignedHexIntegerLiteral] =
    _signedIntString("0x", 16).map(SignedHexIntegerLiteral(_)(?))

  def _signedOctIntLit: Gen[SignedOctalIntegerLiteral] =
    _signedIntString("0", 8).map(SignedOctalIntegerLiteral(_)(?))

  def _doubleLit: Gen[DecimalDoubleLiteral] =
    Arbitrary.arbDouble.arbitrary.map(_.toString).map(DecimalDoubleLiteral(_)(?))

  def _variable: Gen[Variable] = for {
    name <- _identifier
  } yield Variable(name)(?)

  def _predicateComparisonPar(l: Expression, r: Expression): Gen[Expression] = Gen.oneOf(
    GreaterThanOrEqual(l, r)(?),
    GreaterThan(l, r)(?),
    LessThanOrEqual(l, r)(?),
    LessThan(l, r)(?),
    Equals(l, r)(?),
    Equivalent(l, r)(?),
    NotEquals(l, r)(?),
    InvalidNotEquals(l, r)(?)
  )

  def _predicateComparison: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- _predicateComparisonPar(l, r)
  } yield res

  def _predicateComparisonChain: Gen[Expression] = for {
    exprs <- Gen.listOfN(4, _expression)
    pairs = exprs.sliding(2)
    gens = pairs.map(p => _predicateComparisonPar(p.head, p.last)).toList
    chain <- Gen.sequence(gens)(Buildable.buildableCanBuildFrom)
  } yield Ands(chain.toSet)(?)

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

  def _map: Gen[MapExpression] = for {
    items <- _smallListOf(_tuple(_propertyKeyName, _expression))
  } yield MapExpression(items)(?)

  def _property: Gen[Property] = for {
    map <- _expression
    key <- _propertyKeyName
  } yield Property(map, key)(?)


  def _mapProjectionElement: Gen[MapProjectionElement] =
    Gen.oneOf(
      for {key <- _propertyKeyName; exp <- _expression} yield LiteralEntry(key, exp)(?),
      for {id <- _variable} yield VariableSelector(id)(?),
      for {id <- _variable} yield PropertySelector(id)(?),
      Gen.const(AllPropertiesSelector()(?))
    )

  def _mapProjection: Gen[MapProjection] = for {
    name <- _variable
    items <- _smallNonemptyListOf(_mapProjectionElement)
  } yield MapProjection(name, items)(?, None)


  def _list: Gen[ListLiteral] = for {
    parts <- _smallListOf(_expression)
  } yield ListLiteral(parts)(?)

  def _listSlice: Gen[ListSlice] = for {
    list <- _expression
    from <- Gen.option(_expression)
    to <- Gen.option(_expression)
  } yield ListSlice(list, from, to)(?)

  def _containerIndex: Gen[ContainerIndex] = for {
    expr <- _expression
    idx <- _expression
  } yield ContainerIndex(expr, idx)(?)

  def _parameter: Gen[Parameter] =
    _identifier.map(Parameter(_, AnyType.instance)(?))

  def _arithmeticUnary: Gen[Expression] = for {
    r <- _expression
    exp <- Gen.oneOf(
      UnaryAdd(r)(?),
      UnarySubtract(r)(?)
    )
  } yield exp

  def _arithmeticBinary: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    exp <- Gen.oneOf(
      Add(l, r)(?),
      Multiply(l, r)(?),
      Divide(l, r)(?),
      Pow(l, r)(?),
      Modulo(l, r)(?),
      Subtract(l, r)(?)
    )
  } yield exp

  def _case: Gen[CaseExpression] = for {
    expression <- Gen.option(_expression)
    alternatives <- _smallNonemptyListOf(_tuple(_expression, _expression))
    default <- Gen.option(_expression)
  } yield CaseExpression(expression, alternatives, default)(?)

  def _namespace: Gen[Namespace] = for {
    parts <- _smallListOf(_identifier)
  } yield Namespace(parts)(?)

  def _functionName: Gen[FunctionName] = for {
    name <- _identifier
  } yield FunctionName(name)(?)

  def _functionInvocation: Gen[FunctionInvocation] = for {
    namespace <- _namespace
    functionName <- _functionName
    distinct <- _boolean
    args <- _smallListOf(_expression)
  } yield FunctionInvocation(namespace, functionName, distinct, args.toIndexedSeq)(?)

  def _countStar: Gen[CountStar] =
    Gen.const(CountStar()(?))

  def _filterScope: Gen[FilterScope] = for {
    variable <- _variable
    innerPredicate <- Gen.option(_expression)
  } yield FilterScope(variable, innerPredicate)(?)

  def _filter: Gen[FilterExpression] = for {
    scope <- _filterScope
    expression <- _expression
  } yield FilterExpression(scope, expression)(?)

  def _extractScope: Gen[ExtractScope] = for {
    variable <- _variable
    innerPredicate <- Gen.option(_expression)
    extractExpression <- Gen.option(_expression)
  } yield ExtractScope(variable, innerPredicate, extractExpression)(?)

  def _extract: Gen[ExtractExpression] = for {
    scope <- _extractScope
    expression <- _expression
  } yield ExtractExpression(scope, expression)(?)

  def _listComprehension: Gen[ListComprehension] = for {
    scope <- _extractScope
    expression <- _expression
  } yield ListComprehension(scope, expression)(?)

  def _iterablePredicate: Gen[IterablePredicateExpression] = for {
    scope <- _filterScope
    expression <- _expression
    predicate <- Gen.oneOf(
      AllIterablePredicate(scope, expression)(?),
      AnyIterablePredicate(scope, expression)(?),
      NoneIterablePredicate(scope, expression)(?),
      SingleIterablePredicate(scope, expression)(?)
    )
  } yield predicate

  def _degree: Gen[GetDegree] = for {
    node <- _expression
    relType <- Gen.option(_relTypeName)
    dir <- _semanticDirection
  } yield GetDegree(node, relType, dir)(?)

  def _hasLabels: Gen[HasLabels] = for {
    expression <- _expression
    labels <- _smallNonemptyListOf(_labelName)
  } yield HasLabels(expression, labels)(?)

  def _reduceScope: Gen[ReduceScope] = for {
    accumulator <- _variable
    variable <- _variable
    expression <- _expression
  } yield ReduceScope(accumulator, variable, expression)(?)

  def _reduceExpr: Gen[ReduceExpression] = for {
    scope <- _reduceScope
    init <- _expression
    list <- _expression
  } yield ReduceExpression(scope, init, list)(?)

  def _relationshipsPattern: Gen[RelationshipsPattern] = for {
    chain <- _relationshipChain
  } yield RelationshipsPattern(chain)(?)

  def _patternExpr: Gen[PatternExpression] = for {
    pattern <- _relationshipsPattern
  } yield PatternExpression(pattern)

  def _shortestPaths: Gen[ShortestPaths] = for {
    element <- _patternElement
    single <- _boolean
  } yield ShortestPaths(element, single)(?)

  def _shortestPathExpr: Gen[ShortestPathExpression] = for {
    pattern <- _shortestPaths
  } yield ShortestPathExpression(pattern)

  def _existsSubClause: Gen[ExistsSubClause] = for {
    pattern <- _pattern
    where <- Gen.option(_expression)
    outerScope <- _smallListOf(_variable)
  } yield ExistsSubClause(pattern, where)(?, outerScope.toSet)

  def _patternComprehension: Gen[PatternComprehension] = for {
    namedPath <- Gen.option(_variable)
    pattern <- _relationshipsPattern
    predicate <- Gen.option(_expression)
    projection <- _expression
    outerScope <- _smallListOf(_variable)
  } yield PatternComprehension(namedPath, pattern, predicate, projection)(?, outerScope.toSet)

  def _expression: Gen[Expression] =
    Gen.frequency(
      10 -> Gen.oneOf(
        Gen.lzy(_nullLit),
        Gen.lzy(_stringLit),
        Gen.lzy(_booleanLit),
        Gen.lzy(_signedIntLit),
        Gen.lzy(_signedHexIntLit),
        Gen.lzy(_signedOctIntLit),
        Gen.lzy(_doubleLit),
        Gen.lzy(_variable),
        Gen.lzy(_parameter)
      ),
      1 -> Gen.oneOf(
        Gen.lzy(_predicateComparison),
        Gen.lzy(_predicateUnary),
        Gen.lzy(_predicateBinary),
        Gen.lzy(_predicateComparisonChain),
        Gen.lzy(_iterablePredicate),
        Gen.lzy(_hasLabels)
      ),
      1 -> Gen.oneOf(
        Gen.lzy(_arithmeticUnary),
        Gen.lzy(_arithmeticBinary)
      ),
      1 -> Gen.oneOf(
        Gen.lzy(_case),
        Gen.lzy(_functionInvocation),
        Gen.lzy(_countStar),
        Gen.lzy(_reduceExpr),
        Gen.lzy(_shortestPathExpr),
        Gen.lzy(_patternExpr)
      ),
      1 -> Gen.oneOf(
        Gen.lzy(_map),
        Gen.lzy(_mapProjection),
        Gen.lzy(_property),
        Gen.lzy(_list),
        Gen.lzy(_listSlice),
        Gen.lzy(_listComprehension),
        Gen.lzy(_containerIndex),
        Gen.lzy(_extract),
        Gen.lzy(_filter)
      ),
      1 -> Gen.oneOf(
        Gen.lzy(_existsSubClause),
        Gen.lzy(_patternComprehension)
      )
    )

  // PATTERNS
  // ==========================================================================

  def _nodePattern: Gen[NodePattern] = for {
    variable <- Gen.option(_variable)
    labels <- _smallListOf(_labelName)
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
    types <- _smallListOf(_relTypeName)
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
    parts <- _smallNonemptyListOf(_patternPart)
  } yield Pattern(parts)(?)

  // HINTS
  // ==========================================================================

  def _usingIndexHint: Gen[UsingIndexHint] = for {
    variable <- _variable
    label <- _labelName
    properties <- _smallNonemptyListOf(_propertyKeyName)
    spec <- Gen.oneOf(SeekOnly, SeekOrScan)
  } yield UsingIndexHint(variable, label, properties, spec)(?)

  def _usingJoinHint: Gen[UsingJoinHint] = for {
    variables <- _smallNonemptyListOf(_variable)
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
    items <- _smallNonemptyListOf(_sortItem)
  } yield OrderBy(items)(?)

  def _skip: Gen[Skip] =
    _expression.map(Skip(_)(?))

  def _limit: Gen[Limit] =
    _expression.map(Limit(_)(?))

  def _where: Gen[Where] =
    _expression.map(Where(_)(?))

  def _returnItems1: Gen[ReturnItems] = for {
    retItems <- _smallNonemptyListOf(_returnItem)
  } yield ReturnItems(includeExisting = false, retItems)(?)

  def _returnItems2: Gen[ReturnItems] = for {
    retItems <- _smallListOf(_returnItem)
  } yield ReturnItems(includeExisting = true, retItems)(?)

  def _returnItems: Gen[ReturnItems] =
    Gen.oneOf(_returnItems1, _returnItems2)

  def _with: Gen[With] = for {
    distinct <- _boolean
    inclExisting <- _boolean
    retItems <- _smallNonemptyListOf(_returnItem)
    orderBy <- Gen.option(_orderBy)
    skip <- Gen.option(_skip)
    limit <- Gen.option(_limit)
    where <- Gen.option(_where)
  } yield With(distinct, ReturnItems(inclExisting, retItems)(?), orderBy, skip, limit, where)(?)

  def _return: Gen[Return] = for {
    distinct <- _boolean
    inclExisting <- _boolean
    retItems <- _smallNonemptyListOf(_returnItem)
    orderBy <- Gen.option(_orderBy)
    skip <- Gen.option(_skip)
    limit <- Gen.option(_limit)
  } yield Return(distinct, ReturnItems(inclExisting, retItems)(?), orderBy, skip, limit)(?)

  def _match: Gen[Match] = for {
    optional <- _boolean
    pattern <- _pattern
    hints <- _smallListOf(_hint)
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
    s <- Gen.choose(1, 1)
    clauses <- Gen.listOfN(s, _clause)
  } yield Query(None, SingleQuery(clauses)(?))(?)


  object Shrinker {

    import com.neo4j.fabric.utils.Monoid._
    import com.neo4j.fabric.utils.Rewritten._
    import scala.util.Random

    implicit val IntAddMonoid: Monoid[Int] = Monoid.create(0)(_ + _)

    def shrinkOnce(q: Query): Option[Query] = {
      var splitPoints = 0
      q.rewritten.bottomUp {
        case l: List[_] if l.size > 1    =>
          splitPoints += 1
          l
        case o: Option[_] if o.isDefined =>
          splitPoints += 1
          o
      }
      if (splitPoints == 0) {
        None
      } else {
        var point = Random.nextInt(splitPoints)

        def onPoint[T, R >: T](i: T)(f: => R): R = if (point == 0) {
          point -= 1
          f
        } else {
          point -= 1
          i
        }

        Some(
          q.rewritten.bottomUp {
            case l: List[_] if l.size > 1    => onPoint(l)(List(l.head))
            case o: Option[_] if o.isDefined => onPoint(o)(Option.empty)
          })
      }
    }

    implicit val shrinkQuery: Shrink[Query] = Shrink[Query] { q =>
      Stream.iterate(shrinkOnce(q))(i => i.flatMap(shrinkOnce))
        .takeWhile(_.isDefined)
        .map(_.get)
    }
  }

}
