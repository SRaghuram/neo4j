/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.planning.FabricQuery.Columns
import com.neo4j.fabric.util.PrettyPrinting
import org.neo4j.cypher.internal.ast

sealed trait Fragment {
  def columns: Columns
}

object Fragment {

  case class Direct(
    fragment: Fragment,
    columns: Columns,
  ) extends Fragment

  case class Apply(
    fragment: Fragment,
    columns: Columns,
  ) extends Fragment

  case class Chain(
    fragments: Seq[Fragment],
    columns: Columns,
  ) extends Fragment

  case class Union(
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment,
    columns: Columns,
  ) extends Fragment

  case class Leaf(
    use: Option[ast.UseGraph],
    clauses: Seq[ast.Clause],
    columns: Columns,
  ) extends Fragment

  val pretty: PrettyPrinting[Fragment] = new PrettyPrinting[Fragment] {
    def pretty: Fragment => Stream[String] = {
      case f: Direct => node(
        name = "direct",
        fields = Columns.fields(f.columns),
        children = Seq(f.fragment)
      )
      case f: Apply => node(
        name = "apply",
        fields = Columns.fields(f.columns),
        children = Seq(f.fragment)
      )

      case f: Fragment.Chain => node(
        name = "chain",
        fields = Columns.fields(f.columns),
        children = f.fragments
      )

      case f: Fragment.Union => node(
        name = "union",
        fields = Columns.fields(f.columns) ++Seq(
          "dist" -> f.distinct
        ),
        children = Seq(f.lhs, f.rhs)
      )

      case f: Fragment.Leaf => node(
        name = "leaf",
        fields = Columns.fields(f.columns) ++ Seq(
          "use" -> f.use.map(_.expression).map(expr).getOrElse(""),
          "qry" -> query(f.clauses)
        )
      )
    }
  }
}



