/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.planning.Fragment.Apply
import com.neo4j.fabric.planning.Fragment.Chain
import com.neo4j.fabric.planning.Fragment.Leaf
import com.neo4j.fabric.planning.Fragment.Init
import com.neo4j.fabric.planning.Fragment.Union
import com.neo4j.fabric.util.Errors
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition

class FabricFragmenter(
  defaultGraphName: String,
  queryString: String,
  queryStatement: ast.Statement,
  semantics: ast.semantics.SemanticState,
) {

  private val defaultGraphSelection = defaultUse(defaultGraphName, InputPosition.NONE)

  def fragment: Fragment = queryStatement match {
    case ast.Query(_, part)  => fragment(Init(defaultGraphSelection), part)
    case ddl: ast.CatalogDDL => Errors.ddlNotSupported(ddl)
    case _: ast.Command      => Errors.notSupported("Commands")
  }

  private def fragment(
    input: Fragment.Chain,
    part: ast.QueryPart,
  ): Fragment = part match {
    case sq: ast.SingleQuery => fragment(input, sq)
    case uq: ast.Union       => Union(isDistinct(uq), fragment(input, uq.part), fragment(input, uq.query))
  }

  private def fragment(
    input: Fragment.Chain,
    sq: ast.SingleQuery,
  ): Fragment.Chain = {
    val parts = partitioned(sq.clauses)
    parts.foldLeft(input) {

      case (in: Init, Right(clauses)) =>
        // Input is Init which means that we are at the start of a chain
        val use = leadingUse(sq).getOrElse(in.use)
        Leaf(Init(use, in.argumentColumns, sq.importColumns), clauses, produced(clauses))

      case (in, Right(clauses)) =>
        // Section of clauses in the middle of a query
        Leaf(in, clauses, produced(clauses))

      case (in, Left(subquery)) =>
        // Recurse and start the child chain with Init
        Apply(in, fragment(Init(in.use, in.outputColumns, Seq.empty), subquery.part))
    }
  }

  private def isDistinct(uq: ast.Union) =
    uq match {
      case _: ast.UnionAll      => false
      case _: ast.UnionDistinct => true
    }

  private def leadingUse(sq: ast.SingleQuery): Option[ast.GraphSelection] = {
    val clauses = sq.clausesExceptImportWith
    val (use, rest) = clauses.headOption match {
      case Some(u: ast.UseGraph) => (Some(u), clauses.tail)
      case _                     => (None, clauses)
    }

    rest.filter(_.isInstanceOf[ast.UseGraph])
      .map(clause => Errors.syntax("USE can only appear at the beginning of a (sub-)query", queryString, clause.position))

    use
  }

  def defaultUse(graphName: String, pos: InputPosition) =
    UseGraph(Variable(graphName)(pos))(pos)

  private def produced(clauses: Seq[ast.Clause]): Seq[String] =
    produced(clauses.last)

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
