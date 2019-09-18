/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.planning.FabricQuery.Columns
import com.neo4j.fabric.util.PrettyPrinting
import org.neo4j.cypher.internal.v4_0.ast

sealed trait Fragment {

  def produced: Seq[String]
}

object Fragment {

  case class Chain(
    fragments: Seq[Fragment]
  ) extends Fragment {
    def produced: Seq[String] = fragments.last.produced
  }

  case class Union(
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment,
    produced: Seq[String],
  ) extends Fragment

  case class Leaf(
    from: Option[ast.FromGraph],
    clauses: Seq[ast.Clause],
    columns: Columns
  ) extends Fragment {
    def produced: Seq[String] = columns.produced
  }

  val pretty: PrettyPrinting[Fragment] = new PrettyPrinting[Fragment] {
    def pretty: Fragment => Stream[String] = {
      case f: Fragment.Chain => node(
        name = "chain",
        fields = Seq(
          "prod" -> list(f.produced)
        ),
        children = f.fragments
      )

      case f: Fragment.Union => node(
        name = "union",
        fields = Seq(
          "prod" -> list(f.produced),
          "dist" -> f.distinct
        ),
        children = Seq(f.lhs, f.rhs)
      )

      case f: Fragment.Leaf => node(
        name = "leaf",
        fields = Columns.fields(f.columns) ++ Seq(
          "from" -> f.from.map(_.expression).map(expr).getOrElse(""),
          "qry" -> query(f.clauses)
        )
      )
    }
  }
}



