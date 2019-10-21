/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.planning.FabricQuery.Columns
import com.neo4j.fabric.util.PrettyPrinting
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.{Query, UseGraph}

case class FabricPlan(
  query: FabricQuery,
  queryType: FabricPlan.QueryType,
  executionType: FabricPlan.ExecutionType,
)

object FabricPlan {
  sealed trait QueryType
  case object Read extends QueryType
  case object ReadWrite extends QueryType
  val READ: QueryType = Read
  val READ_WRITE: QueryType = ReadWrite

  sealed trait ExecutionType
  case object Execute extends ExecutionType
  case object Explain extends ExecutionType
  case object Profile extends ExecutionType
  val EXECUTE: ExecutionType = Execute
  val EXPLAIN: ExecutionType = Explain
  val PROFILE: ExecutionType = Profile
}

sealed trait FabricQuery {
  def columns: Columns
  def children: Seq[FabricQuery]
}

object FabricQuery {

  private val renderer = Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true))

  case class Columns(
    incoming: Seq[String],
    local: Seq[String],
    imports: Seq[String],
    output: Seq[String],
  )

  object Columns {

    def combine(left: Seq[String], right: Seq[String]): Seq[String] =
      left.filterNot(right.contains) ++ right

    def paramName(varName: String): String =
      s"@@$varName"

    def fields(c: Columns): Seq[(String, String)] = Seq(
      "in" -> PrettyPrinting.list(c.incoming),
      "loc" -> PrettyPrinting.list(c.local),
      "imp" -> PrettyPrinting.list(c.imports),
      "out" -> PrettyPrinting.list(c.output),
    )
  }

  case class Direct(
    query: FabricQuery,
    columns: Columns
  ) extends FabricQuery {
    def children: Seq[FabricQuery] = Seq(query)
  }

  case class Apply(
    query: FabricQuery,
    columns: Columns
  ) extends FabricQuery {
    def children: Seq[FabricQuery] = Seq(query)
  }

  sealed trait LeafQuery extends FabricQuery

  case class LocalQuery(
    query: FullyParsedQuery,
    columns: Columns,
  ) extends LeafQuery {

    def input: Seq[String] =
      Columns.combine(columns.local, columns.imports)

    def children: Seq[FabricQuery] = Seq.empty
  }

  case class RemoteQuery(
    use: UseGraph,
    query: Query,
    columns: Columns,
  ) extends LeafQuery {

    def parameters: Map[String, String] =
      Columns.combine(columns.local, columns.imports)
        .map(n => n -> Columns.paramName(n)).toMap

    def queryString: String = renderer.asString(query)

    def children: Seq[FabricQuery] = Seq.empty
  }

  sealed trait CompositeQuery
    extends FabricQuery

  case class UnionQuery(
    lhs: FabricQuery,
    rhs: FabricQuery,
    distinct: Boolean,
    columns: Columns
  ) extends CompositeQuery {
    def children: Seq[FabricQuery] = Seq(lhs, rhs)
  }

  case class ChainedQuery(
    queries: Seq[FabricQuery],
    columns: Columns
  ) extends CompositeQuery {
    def children: Seq[FabricQuery] = queries
  }

  val pretty: PrettyPrinting[FabricQuery] = new PrettyPrinting[FabricQuery] {
    def pretty: FabricQuery => Stream[String] = {
      case q: Direct       => node(
        name = "direct",
        fields = Columns.fields(q.columns),
        children = Seq(q.query)
      )
      case q: Apply        => node(
        name = "apply",
        fields = Columns.fields(q.columns),
        children = Seq(q.query)
      )
      case q: LocalQuery   => node(
        name = "local",
        fields = Columns.fields(q.columns) ++ Seq(
          "qry" -> query(q.query.state.statement())
        )
      )
      case q: RemoteQuery  => node(
        name = "use: " + expr(q.use.expression),
        fields = Columns.fields(q.columns) ++ Seq(
          "params" -> list(q.parameters.toSeq),
          "qry" -> query(q.query))
      )
      case q: ChainedQuery => node(
        name = "chain",
        Columns.fields(q.columns),
        children = q.queries
      )
      case q: UnionQuery   => node(
        name = "union",
        fields = Columns.fields(q.columns) ++ Seq(
          "distinct" -> q.distinct
        ),
        children = Seq(q.lhs, q.rhs)
      )
    }
  }

}
