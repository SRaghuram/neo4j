/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner

import org.neo4j.cypher.internal.{FullyParsedQuery}
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.{FromGraph, Query}

sealed trait FabricQuery {
  def output: Seq[String]

  def subquery: Boolean
}

object FabricQuery {

  sealed trait LeafQuery
    extends FabricQuery

  case class LocalQuery(
    query: FullyParsedQuery,
    input: Seq[String],
    output: Seq[String],
    subquery: Boolean
  ) extends LeafQuery

  case class ShardQuery(
    from: FromGraph,
    query: Query,
    input: Seq[String],
    output: Seq[String],
    hasOutput: Boolean,
    queryString: String,
    subquery: Boolean
  ) extends LeafQuery

  sealed trait CompositeQuery
    extends FabricQuery

  case class UnionQuery(
    lhs: FabricQuery,
    rhs: FabricQuery,
    distinct: Boolean
  ) extends CompositeQuery {

    def output: Seq[String] = rhs.output

    def subquery: Boolean = false

  }

  case class ChainedQuery(
    lhs: FabricQuery,
    rhs: FabricQuery
  ) extends CompositeQuery {

    def output: Seq[String] = rhs.output

    def subquery: Boolean = false
  }

  private val printer = Prettifier(ExpressionStringifier())

  private def indent(s: String) = s.linesIterator.map(l => "  " + l).mkString("\n")

  def show(q: FabricQuery): String = q match {
    case lq: LocalQuery   =>
      "[local] " + lq.input.mkString(",") + " -> " + lq.output.mkString(",") + "\n" + indent(printer.asString(lq.query.state.statement()))
    case sq: ShardQuery   =>
      "[shard] " + sq.input.mkString(",") + " -> " + sq.output.mkString(",") + "\n" + indent(printer.asString(sq.query))
    case cq: ChainedQuery =>
      "[chain]\n" + indent(show(cq.lhs)) + "\n" + indent(show(cq.rhs))
    case uq: UnionQuery =>
      "[union]\n" + indent(show(uq.lhs)) + "\n" + indent(show(uq.rhs))

    case _ => "other"
  }
}
