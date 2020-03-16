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

  sealed trait Chain extends Fragment {
    /** Graph selection for this fragment */
    def use: Use
  }

  sealed trait Segment extends Fragment.Chain {
    def input: Fragment.Chain
    val use: Use = input.use
    val argumentColumns: Seq[String] = input.argumentColumns
    val importColumns: Seq[String] = input.importColumns
  }

  final case class Init(
    use: Use,
    argumentColumns: Seq[String] = Seq.empty,
    importColumns: Seq[String] = Seq.empty,
  ) extends Fragment.Chain {
    val outputColumns: Seq[String] = Seq.empty
    val description: Fragment.Description = Description.InitDesc(this)
  }

  final case class Apply(
    input: Fragment.Chain,
    inner: Fragment,
  ) extends Fragment.Segment {
    val outputColumns: Seq[String] = Columns.combine(input.outputColumns, inner.outputColumns)
    val description: Fragment.Description = Description.ApplyDesc(this)
  }

  final case class Leaf(
    input: Fragment.Chain,
    clauses: Seq[ast.Clause],
    outputColumns: Seq[String],
  ) extends Fragment.Segment {
    val parameters: Map[String, String] = importColumns.map(varName => varName -> Columns.paramName(varName)).toMap
    val description: Fragment.Description = Description.LeafDesc(this)
    val queryType: QueryType = QueryType.of(clauses)
  }

  final case class Union(
    distinct: Boolean,
    lhs: Fragment,
    rhs: Fragment.Chain,
  ) extends Fragment {
    val outputColumns: Seq[String] = rhs.outputColumns
    val argumentColumns: Seq[String] = Seq.empty
    val importColumns: Seq[String] = Seq.empty
    val description: Fragment.Description = Description.UnionDesc(this)
  }

  sealed abstract class Description(name: String, fragment: Fragment) extends ExecutionPlanDescription {
    override def getName: String = name
    override def getIdentifiers: util.Set[String] = setAsJavaSet(fragment.outputColumns.toSet)
    override def hasProfilerStatistics: Boolean = false
    override def getProfilerStatistics: ExecutionPlanDescription.ProfilerStatistics = null
  }

  final object Description {
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
          "use" -> use(f.use),
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
          "use" -> use(f.use),
          "arg" -> list(f.argumentColumns),
          "imp" -> list(f.importColumns),
          "out" -> list(f.outputColumns),
          "qry" -> query(f.clauses),
        ),
        children = Seq(f.input)
      )
    }

    private def use(u: Use) = u match {
      case d: Use.Declared  => "declared " + expr(d.graphSelection.expression)
      case i: Use.Inherited => "inherited" + expr(i.graphSelection.expression)
    }
  }
}





