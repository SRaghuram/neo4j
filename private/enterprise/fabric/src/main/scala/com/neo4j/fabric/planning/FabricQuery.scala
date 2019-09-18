/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.util.PrettyPrinting
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.{FromGraph, Query}

sealed trait FabricQuery {
  def produced: Seq[String]
}

object FabricQuery {

  private val renderer = Prettifier(ExpressionStringifier())

  case class Columns(
    input: Seq[String],
    local: Seq[String],
    imports: Seq[String],
    produced: Seq[String],
    passThrough: Seq[String],
  )

  object Columns {

    def combine(left: Seq[String], right: Seq[String]): Seq[String] =
      left.filterNot(right.contains) ++ right

    def paramName(varName: String): String =
      s"@@$varName"

    def fields(c: Columns): Seq[(String, String)] = Seq(
      "input" -> PrettyPrinting.list(c.input),
      "local" -> PrettyPrinting.list(c.local),
      "imprt" -> PrettyPrinting.list(c.imports),
      "prod" -> PrettyPrinting.list(c.produced),
      "pass" -> PrettyPrinting.list(c.passThrough),
    )
  }

  sealed trait LeafQuery extends FabricQuery

  case class LocalQuery(
    query: FullyParsedQuery,
    columns: Columns,
  ) extends LeafQuery {
    def produced: Seq[String] = columns.produced
  }

  case class ShardQuery(
    from: FromGraph,
    query: Query,
    columns: Columns,
  ) extends LeafQuery {

    def produced: Seq[String] = columns.produced

    def queryString: String = renderer.asString(query)

    def parameters: Map[String, String] =
      Columns.combine(columns.local, columns.imports)
        .map(n => n -> Columns.paramName(n)).toMap
  }

  sealed trait CompositeQuery
    extends FabricQuery

  case class UnionQuery(
    lhs: FabricQuery,
    rhs: FabricQuery,
    distinct: Boolean
  ) extends CompositeQuery {
    def produced: Seq[String] = rhs.produced
  }

  case class ChainedQuery(
    queries: Seq[FabricQuery]
  ) extends CompositeQuery {
    def produced: Seq[String] = queries.last.produced
  }

  val pretty: PrettyPrinting[FabricQuery] = new PrettyPrinting[FabricQuery] {
    def pretty: FabricQuery => Stream[String] = {
      case q: LocalQuery   => node(
        name = "local",
        fields = Columns.fields(q.columns) ++ Seq(
          "qry" -> query(q.query.state.statement())
        )
      )
      case q: ShardQuery   => node(
        name = "shard: " + expr(q.from.expression),
        fields = Columns.fields(q.columns) ++ Seq(
          "qry" -> query(q.query))
      )
      case q: ChainedQuery => node(
        name = "chain",
        fields = Seq(
          "prod" -> list(q.produced)
        ),
        children = q.queries
      )
      case q: UnionQuery   => node(
        name = "union",
        fields = Seq(
          "prod" -> list(q.produced),
          "distinct" -> q.distinct
        ),
        children = Seq(q.lhs, q.rhs)
      )
    }
  }

}
