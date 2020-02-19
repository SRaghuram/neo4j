/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.planning.FabricQuery.Columns
import com.neo4j.fabric.planning.Fragment.Chain
import com.neo4j.fabric.planning.Fragment.Leaf
import com.neo4j.fabric.planning.Fragment.Union
import com.neo4j.fabric.util.Errors
import org.neo4j.cypher.internal.ast

class FabricFragmenter(
  queryString: String,
  queryStatement: ast.Statement,
  semantics: ast.semantics.SemanticState,
) {

  def fragment: Fragment = queryStatement match {
    case ast.Query(_, part)  => fragment(part, Seq.empty, Seq.empty, Option.empty)
    case ddl: ast.CatalogDDL => Errors.ddlNotSupported(ddl)
    case _: ast.Command      => Errors.notSupported("Commands")
  }

  /**
   * Tracks fragmenter state as we walk through the query
   * @param incoming columns in incoming scope
   * @param locals columns added in current scope
   * @param imports columns imported from incoming scope
   * @param fragments sequence of fragments created so far
   */
  private case class State(
    incoming: Seq[String],
    locals: Seq[String],
    imports: Seq[String],
    fragments: Seq[Fragment] = Seq.empty
  ) {

    def columns: Columns = Columns(
      incoming = incoming,
      local = locals,
      imports = imports,
      output = Seq.empty,
    )

    def append(frag: Fragment): State =
      copy(
        incoming = frag.columns.output,
        locals = frag.columns.output,
        imports = Seq.empty,
        fragments = fragments :+ frag,
      )
  }

  private def fragment(
    part: ast.QueryPart,
    incoming: Seq[String],
    local: Seq[String],
    use: Option[ast.UseGraph]
  ): Fragment = part match {

    case uq: ast.Union =>
      val lhs = fragment(uq.part, incoming, local, use)
      val rhs = fragment(uq.query, incoming, local, use)
      val distinct = uq match {
        case _: ast.UnionAll      => false
        case _: ast.UnionDistinct => true
      }
      Union(distinct, lhs, rhs, rhs.columns)

    case sq: ast.SingleQuery =>
      val imports = sq.importColumns
      val localUse = leadingUse(sq).orElse(use)
      val parts = partitioned(sq.clauses)
      val initial = State(incoming, local, imports)

      val result = parts.foldLeft(initial) {

        // A segment of normal clauses -> a leaf
        case (state, Right(clauses)) =>
          val leaf = Leaf(
            use = localUse,
            clauses = clauses.filterNot(_.isInstanceOf[ast.UseGraph]),
            columns = state.columns.copy(
              output = produced(clauses.last),
            )
          )
          state.append(Fragment.Direct(leaf, leaf.columns))

        // A subquery -> recurse
        case (state, Left(sub)) =>
          if (localUse.isDefined)
            Errors.syntax("Nested subqueries in remote query-parts is not supported", queryString, sub.position)

          val frag = fragment(
            part = sub.part,
            incoming = state.incoming,
            local = Seq.empty,
            use = localUse
          )
          state.append(Fragment.Apply(
            frag,
            state.columns.copy(
              imports = Seq.empty,
              output = Columns.combine(state.locals, frag.columns.output),
            )
          ))
      }

      result.fragments match {
        case Seq(single) => single
        case many        => Chain(
          fragments = many,
          columns = Columns(
            incoming = incoming,
            local = local,
            imports = imports,
            output = many.last.columns.output,
          )
        )
      }
  }

  private def leadingUse(sq: ast.SingleQuery): Option[ast.UseGraph] = {
    val clauses = sq.clausesExceptImportWith
    val (use, rest) = clauses.headOption match {
      case Some(u: ast.UseGraph) => (Some(u), clauses.tail)
      case _                     => (None, clauses)
    }

    rest.filter(_.isInstanceOf[ast.UseGraph])
      .map(clause => Errors.syntax("USE can only appear at the beginning of a (sub-)query", queryString, clause.position))

    use
  }

  private def produced(clause: ast.Clause): Seq[String] = clause match {
    case r: ast.Return => r.returnColumns.map(_.name)
    case c             => semantics.scope(c).get.symbolNames.toSeq
  }

  /**
   * Returns a sequence where each element is either a subquery clause
   * or a segment of clauses with no subqueries
   */
  private def partitioned(clauses: Seq[ast.Clause]) =
    partition(clauses) {
      case s: ast.SubQuery => Left(s)
      case c               => Right(c)
    }

  /**
   * Partitions the elements of a sequence depending on a predicate.
   * The predicate returns either Left[H] or Right[M] for each element
   * Running lengths of Right[M]'s gets aggregated into sub-sequences
   * while Left[H]'s are left singular
   */
  private def partition[E, H, M](es: Seq[E])(pred: E => Either[H, M]): Seq[Either[H, Seq[M]]] = {
    es.map(pred).foldLeft(Seq[Either[H, Seq[M]]]()) {
      case (seq, Left(hit))   => seq :+ Left(hit)
      case (seq, Right(miss)) => seq.lastOption match {
        case None            => seq :+ Right(Seq(miss))
        case Some(Left(_))   => seq :+ Right(Seq(miss))
        case Some(Right(ms)) => seq.init :+ Right(ms :+ miss)
      }
    }
  }

}
