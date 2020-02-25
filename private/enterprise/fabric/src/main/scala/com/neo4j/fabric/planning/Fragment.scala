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
  /** Graph selection for this fragment */
  def graph: ast.GraphSelection
  /** Columns available to this fragment from an applied argument */
  def argumentColumns: Seq[String]
  /** Columns imported from the argument */
  def importColumns: Seq[String]
  /** Produced columns */
  def outputColumns: Seq[String]
}

object Fragment {

  object Init {
    def unit(graph: ast.GraphSelection): Init = Init(graph, Seq.empty, Seq.empty)
  }

  case class Init(
    graph: ast.GraphSelection,
    argumentColumns: Seq[String],
    importColumns: Seq[String],
  ) extends Fragment {
    val outputColumns: Seq[String] = Seq.empty
  }

  sealed trait ChainedFragment {
    def input: Fragment
    val graph: ast.GraphSelection = input.graph
    val argumentColumns: Seq[String] = input.argumentColumns
    val importColumns: Seq[String] = input.importColumns
  }

  case class Apply(
    input: Fragment,
    fragment: Fragment,
  ) extends Fragment with ChainedFragment {
    val outputColumns: Seq[String] = Columns.combine(input.outputColumns, fragment.outputColumns)
  }

  case class Union(
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment,
    input: Fragment,
  ) extends Fragment with ChainedFragment {
    val outputColumns: Seq[String] = rhs.outputColumns
  }

  case class Leaf(
    input: Fragment,
    clauses: Seq[ast.Clause],
    outputColumns: Seq[String],
  ) extends Fragment with ChainedFragment {
    val parameters: Map[String, String] = importColumns.map(varName => varName -> Columns.paramName(varName)).toMap
  }

  val pretty: PrettyPrinting[Fragment] = new PrettyPrinting[Fragment] {
    def pretty: Fragment => Stream[String] = {
      case f: Init => node(
        name = "init",
        fields = Seq(
          "use" -> expr(f.graph.expression),
          "arg" -> list(f.argumentColumns),
          "imp" -> list(f.importColumns),
        )
      )

      case f: Apply => node(
        name = "apply",
        fields = Seq(
          "out" -> list(f.outputColumns),
        ),
        children = Seq(f.fragment, f.input)
      )

      case f: Fragment.Union => node(
        name = "union",
        fields = Seq(
          "out" -> list(f.outputColumns),
          "dist" -> f.distinct
        ),
        children = Seq(f.lhs, f.rhs)
      )

      case f: Fragment.Leaf => node(
        name = "leaf",
        fields = Seq(
          "use" -> expr(f.graph.expression),
          "arg" -> list(f.argumentColumns),
          "imp" -> list(f.importColumns),
          "out" -> list(f.outputColumns),
          "qry" -> query(f.clauses),
        ),
        children = Seq(f.input)
      )
    }
  }
}



