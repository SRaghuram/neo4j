/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import java.util

import com.neo4j.fabric.util.PrettyPrinting
import org.neo4j.cypher.internal.ast
import org.neo4j.graphdb.ExecutionPlanDescription

import scala.collection.JavaConverters.setAsJavaSet
import scala.collection.JavaConverters.mapAsJavaMapConverter

sealed trait Fragment {
  /** Graph selection for this fragment */
  def use: ast.GraphSelection
  /** Columns available to this fragment from an applied argument */
  def argumentColumns: Seq[String]
  /** Columns imported from the argument */
  def importColumns: Seq[String]
  /** Produced columns */
  def outputColumns: Seq[String]
  /** ExecutionPlanDescription */
  def description: Fragment.Description
}

object Fragment {

  object Init {
    def unit(graph: ast.GraphSelection): Init = Init(graph, Seq.empty, Seq.empty)
  }

  case class Init(
    use: ast.GraphSelection,
    argumentColumns: Seq[String],
    importColumns: Seq[String],
  ) extends Fragment {
    val outputColumns: Seq[String] = Seq.empty
    val description: Fragment.Description = Description.InitDesc(this)
  }

  sealed trait ChainedFragment {
    def input: Fragment
    val use: ast.GraphSelection = input.use
    val argumentColumns: Seq[String] = input.argumentColumns
    val importColumns: Seq[String] = input.importColumns
  }

  case class Apply(
    input: Fragment,
    inner: Fragment,
  ) extends Fragment with ChainedFragment {
    val outputColumns: Seq[String] = Columns.combine(input.outputColumns, inner.outputColumns)
    val description: Fragment.Description = Description.ApplyDesc(this)
  }

  case class Union(
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment,
    input: Fragment,
  ) extends Fragment with ChainedFragment {
    val outputColumns: Seq[String] = rhs.outputColumns
    val description: Fragment.Description = Description.UnionDesc(this)
  }

  case class Leaf(
    input: Fragment,
    clauses: Seq[ast.Clause],
    outputColumns: Seq[String],
  ) extends Fragment with ChainedFragment {
    val parameters: Map[String, String] = importColumns.map(varName => varName -> Columns.paramName(varName)).toMap
    val description: Fragment.Description = Description.LeafDesc(this)
  }


  sealed abstract class Description(name: String, fragment: Fragment) extends ExecutionPlanDescription {
    override def getName: String = name
    override def getIdentifiers: util.Set[String] = setAsJavaSet(fragment.outputColumns.toSet)
    override def hasProfilerStatistics: Boolean = false
    override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = null
  }

  object Description {
    final case class InitDesc(fragment: Fragment.Init) extends Description("Init", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list()
      override def getArguments: util.Map[String, AnyRef] = map(
        "argumentColumns" -> fragment.argumentColumns.mkString(","),
        "importColumns" -> fragment.importColumns.mkString(","),
      )
    }

    final case class ApplyDesc(fragment: Fragment.Apply) extends Description("Apply", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.input.description, fragment.inner.description)
      override def getArguments: util.Map[String, AnyRef] = map()
    }

    final case class LeafDesc(fragment: Fragment.Leaf) extends Description("Leaf", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.input.description)
      override def getArguments: util.Map[String, AnyRef] = map(
        "query" -> QueryRenderer.render(fragment.clauses)
      )
    }

    final case class UnionDesc(fragment: Fragment.Union) extends Description("Union", fragment) {
      override def getChildren: util.List[ExecutionPlanDescription] = list(fragment.lhs.description, fragment.rhs.description)
      override def getArguments: util.Map[String, AnyRef] = map()
    }

    private def list[E](es: E*): util.List[E] =
      util.List.of(es: _*)

    private def map[K, V](es: (K, V)*): util.Map[K, V] =
      Map(es: _*).asJava
  }

  val pretty: PrettyPrinting[Fragment] = new PrettyPrinting[Fragment] {
    def pretty: Fragment => Stream[String] = {
      case f: Init => node(
        name = "init",
        fields = Seq(
          "use" -> expr(f.use.expression),
          "arg" -> list(f.argumentColumns),
          "imp" -> list(f.importColumns),
        )
      )

      case f: Apply => node(
        name = "apply",
        fields = Seq(
          "out" -> list(f.outputColumns),
        ),
        children = Seq(f.inner, f.input)
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
          "use" -> expr(f.use.expression),
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





