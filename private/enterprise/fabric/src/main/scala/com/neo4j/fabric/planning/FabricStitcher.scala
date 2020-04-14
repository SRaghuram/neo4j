/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.eval.UseEvaluation
import com.neo4j.fabric.util.Rewritten.RewritingOps
import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.InputDataStream
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryPart
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubQuery
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.SyntaxException

/**
 * @param queryString     For error reporting
 * @param allowMultiGraph When false, throws on multi-graph queries
 */
case class FabricStitcher(
  queryString: String,
  allowMultiGraph: Boolean,
  fabricContextName: Option[String],
) {

  /**
   * Convert to executable fragments
   * Exec fragments formed by recursively stitching same-graph Leaf:s together
   * and adding appropriate glue clauses
   */
  def convert(fragment: Fragment): Fragment = fragment match {
    case chain: Fragment.Chain => convertChain(chain)
    case union: Fragment.Union => convertUnion(union)
  }

  def convertUnion(union: Fragment.Union): Fragment =
    stitched(union)
      .getOrElse(union.copy(
        lhs = convert(union.lhs),
        rhs = convertChain(union.rhs)))

  def convertChain(chain: Fragment.Chain): Fragment.Chain =
    stitched(chain)
      .getOrElse(convertSeparate(chain))

  def convertSeparate(chain: Fragment.Chain, lastInChain: Boolean = true): Fragment.Chain = chain match {
    case init: Fragment.Init     => init
    case stitched: Fragment.Exec => stitched

    case leaf: Fragment.Leaf =>
      val input = convertSeparate(leaf.input, lastInChain = false)
      if (leaf.executable)
        single(leaf.copy(input = input), lastInChain)
      else
        input

    case apply: Fragment.Apply =>
      apply.copy(input = convertSeparate(apply.input, lastInChain = false), inner = convert(apply.inner))
  }

  /**
   * Transform a single leaf into exec
   */
  def single(leaf: Fragment.Leaf, lastInChain: Boolean): Fragment.Exec = {
    val pos = leaf.clauses.head.position

    val clauses = Seq(
      Ast.inputDataStream(leaf.input.outputColumns, pos).toSeq,
      Ast.paramBindings(leaf.importColumns, pos).toSeq,
      Ast.withoutGraphSelection(leaf.clauses),
      if (lastInChain)
        Seq.empty
      else
        Ast.aliasedReturn(leaf.clauses.last, leaf.outputColumns, pos).toSeq,
    ).flatten

    Fragment.Exec(
      input = leaf.input,
      query = Query(None, SingleQuery(clauses)(pos))(pos),
      outputColumns = leaf.outputColumns
    )
  }

  /**
   * Transform the entire fragment tree into exec, by stitching it back together.
   * Returns a value when the entire query targets the same graph, statically
   */
  def stitched(fragment: Fragment): Option[Fragment.Exec] = {
    val noPos = InputPosition.NONE
    val stitched = stitcher(fragment,
      clauseExpansion = {
        case Outer(init: Fragment.Init) => Ast.paramBindings(init.importColumns, noPos).toSeq
        case Outer(leaf: Fragment.Leaf) => Ast.withoutGraphSelection(leaf.clauses)
        case Inner(leaf: Fragment.Leaf) => Ast.withoutGraphSelection(leaf.clauses)
        case _                          => Seq()
      },
    )

    val nonStatic = stitched.useAppearances.flatMap(_.nonStatic).headOption
    val nonEqual = stitched.useAppearances.flatMap(_.nonEqual).headOption
    val invalidOverride = stitched.useAppearances.flatMap(_.isInvalidOverride).headOption

    (allowMultiGraph, nonStatic, nonEqual, invalidOverride) match {
      case (false, Some(use), _, _) => failDynamicGraph(use)
      case (false, _, Some(use), _) => failMultipleGraphs(use)
      case (true, _, _, Some(use))  => failInvalidOverride(use)

      case (_, _, None, None) =>
        val query = Query(None, stitched.queryPart)(stitched.queryPart.position)
        val init = Fragment.Init(stitched.lastUse, fragment.argumentColumns, fragment.importColumns)
        Some(Fragment.Exec(init, query, fragment.outputColumns))

      case (_, _, _, _) => None
    }
  }

  private def failDynamicGraph(use: Use): Nothing =
    throw new SyntaxException(
      s"""Dynamic graph lookup not allowed here. This feature is only available in a Fabric database
         |Attempted to access graph ${Use.show(use)}""".stripMargin,
      queryString, use.position.offset)

  private def failMultipleGraphs(use: Use): Nothing =
    throw new SyntaxException(
      s"""Multiple graphs in the same query not allowed here. This feature is only available in a Fabric database.
         |Attempted to access graph ${Use.show(use)}""".stripMargin,
      queryString, use.position.offset)

  private def failInvalidOverride(use: Use): Nothing =
    throw new SyntaxException(
      s"""Nested subqueries must use the same graph as their parent query.
         |Attempted to access graph ${Use.show(use)}""".stripMargin,
      queryString, use.position.offset)

  private case class StitchResult(
    queryPart: QueryPart,
    lastUse: Use,
    useAppearances: Seq[UseAppearance],
  )

  private case class StitchChainResult(
    clauses: Seq[Clause],
    lastUse: Use,
    useAppearances: Seq[UseAppearance],
  )

  private sealed trait NestedFragment
  private final case class Outer(fragment: Fragment) extends NestedFragment
  private final case class Inner(fragment: Fragment) extends NestedFragment

  private sealed trait UseAppearance {
    def nonStatic: Option[Use] = uses.find(use => !UseEvaluation.isStatic(use.graphSelection))
    def nonEqual: Option[Use] = uses.find(use => use.graphSelection != uses.head.graphSelection)
    def isInvalidOverride: Option[Use] = None
    def uses: Seq[Use]
  }
  private final case class UnionUse(lhs: Use, rhs: Use) extends UseAppearance {
    def uses: Seq[Use] = Seq(lhs, rhs)
  }
  private final case class ChainUse(outer: Option[Use], inner: Use) extends UseAppearance {
    def uses: Seq[Use] = outer.toSeq :+ inner
    override def isInvalidOverride: Option[Use] = outer match {
      case None        => None
      case Some(outer) =>
        def outerIsFabric = UseEvaluation.evaluateStatic(outer.graphSelection).exists(_.parts == fabricContextName.toSeq)
        def same = outer.graphSelection == inner.graphSelection
        if (!outerIsFabric && !same) Some(inner) else None
    }
  }

  private def stitcher(
    fragment: Fragment,
    clauseExpansion: NestedFragment => Seq[Clause],
  ): StitchResult = {

    def stitch(fragment: Fragment, outermost: Boolean, outerUse: Option[Use]): StitchResult = {
      val pos = InputPosition.NONE

      fragment match {
        case chain: Fragment.Chain =>
          val stitched = stitchChain(chain, outermost, outerUse)
          StitchResult(SingleQuery(stitched.clauses)(pos), stitched.lastUse, stitched.useAppearances)

        case union: Fragment.Union =>
          val lhs = stitch(union.lhs, outermost, outerUse)
          val rhs = stitchChain(union.rhs, outermost, outerUse)
          val uses = lhs.useAppearances ++ rhs.useAppearances :+ UnionUse(lhs.lastUse, rhs.lastUse)
          val query = SingleQuery(rhs.clauses)(pos)
          val result = if (union.distinct) {
            UnionDistinct(lhs.queryPart, query)(pos)
          } else {
            UnionAll(lhs.queryPart, query)(pos)
          }
          StitchResult(result, rhs.lastUse, uses)
      }
    }

    def stitchChain(chain: Fragment.Chain, outermost: Boolean, outerUse: Option[Use]): StitchChainResult = {
      val pos = InputPosition.NONE

      def wrapped: NestedFragment = if (outermost) Outer(chain) else Inner(chain)

      chain match {

        case init: Fragment.Init =>
          val clauses = clauseExpansion(wrapped)
          StitchChainResult(clauses, init.use, Seq(ChainUse(outerUse, init.use)))

        case leaf: Fragment.Leaf =>
          val before = stitchChain(leaf.input, outermost, outerUse)
          val clauses = clauseExpansion(wrapped)
          before.copy(clauses = before.clauses ++ clauses)

        case apply: Fragment.Apply =>
          val before = stitchChain(apply.input, outermost, outerUse)
          val inner = stitch(apply.inner, outermost = false, Some(before.lastUse))
          before.copy(
            clauses = before.clauses :+ SubQuery(inner.queryPart)(pos),
            useAppearances = before.useAppearances ++ inner.useAppearances)

      }
    }

    stitch(fragment, outermost = true, outerUse = None)
  }
}

private object Ast {

  private def conditionally[T](cond: Boolean, prod: => T) =
    if (cond) Some(prod) else None

  private def variable(name: String, pos: InputPosition) =
    internal.expressions.Variable(name)(pos)

  def paramBindings(columns: Seq[String], pos: InputPosition): Option[With] =
    conditionally(
      columns.nonEmpty,
      With(ReturnItems(
        includeExisting = false,
        items = for {
          varName <- columns
          parName = Columns.paramName(varName)
        } yield AliasedReturnItem(
          expression = Parameter(parName, CTAny)(pos),
          variable = variable(varName, pos),
        )(pos)
      )(pos))(pos)
    )

  def inputDataStream(names: Seq[String], pos: InputPosition): Option[InputDataStream] =
    conditionally(
      names.nonEmpty,
      InputDataStream(
        variables = for {
          name <- names
        } yield variable(name, pos)
      )(pos)
    )

  def aliasedReturn(lastClause: Clause, names: Seq[String], pos: InputPosition): Option[Return] =
    lastClause match {
      case _: Return => None
      case _         => Some(aliasedReturn(names, pos))
    }

  def aliasedReturn(names: Seq[String], pos: InputPosition): Return =
    Return(ReturnItems(
      includeExisting = false,
      items = for {
        name <- names
      } yield AliasedReturnItem(
        expression = variable(name, pos),
        variable = variable(name, pos),
      )(pos)
    )(pos))(pos)

  def unresolveCallables[T <: AnyRef](tree: T): T = {
    tree.rewritten.bottomUp {
      case rc: ResolvedCall               => unresolvedCall(rc)
      case rf: ResolvedFunctionInvocation => unresolvedFunction(rf)

    }
  }

  def unresolvedCall(rc: ResolvedCall): UnresolvedCall = {
    val pos = rc.position
    val name = rc.signature.name
    UnresolvedCall(
      procedureNamespace = Namespace(name.namespace.toList)(pos),
      procedureName = ProcedureName(name.name)(pos),
      declaredArguments = if (rc.declaredArguments) Some(rc.callArguments) else None,
      declaredResult = if (rc.declaredResults) Some(ProcedureResult(rc.callResults)(pos)) else None,
    )(pos)
  }

  def unresolvedFunction(rf: ResolvedFunctionInvocation): FunctionInvocation = {
    val pos = rf.position
    val name = rf.qualifiedName
    FunctionInvocation(
      namespace = Namespace(name.namespace.toList)(pos),
      functionName = FunctionName(name.name)(pos),
      distinct = false,
      args = rf.arguments.toIndexedSeq,
    )(pos)
  }

  def withoutGraphSelection(clauses: Seq[Clause]): Seq[Clause] =
    clauses.filter {
      case _: GraphSelection => false
      case _                 => true
    }

  def withoutGraphSelection(query: Query): Query = {
    query.rewritten.bottomUp {
      case sq: SingleQuery =>
        SingleQuery(clauses = withoutGraphSelection(sq.clauses))(sq.position)
    }
  }

  def chain[T <: AnyRef](rewrites: (T => T)*): T => T =
    rewrites.foldLeft(identity[T] _)(_ andThen _)

}
