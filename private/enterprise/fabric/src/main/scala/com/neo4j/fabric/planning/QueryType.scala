/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.util.Folded.FoldableOps
import com.neo4j.fabric.util.Folded.Stop
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.util.ASTNode


sealed trait QueryType

object QueryType {

  case object Read extends QueryType
  case object ReadPlusUnresolved extends QueryType
  case object Write extends QueryType

  val READ: QueryType = Read
  val READ_PLUS_UNRESOLVED: QueryType = ReadPlusUnresolved
  val WRITE: QueryType = Write

  def of(ast: ASTNode): QueryType =
    ast.folded(Read: QueryType)(merge) {
      case _: UpdateClause   => Stop(Write)
      case _: UnresolvedCall => Stop(ReadPlusUnresolved)
      case c: CallClause     => Stop(if (c.containsNoUpdates) Read else Write)
    }

  def of(ast: Seq[ASTNode]): QueryType =
    ast.map(of).fold(Read)(merge)

  def of(query: FabricQuery): QueryType =
    query match {
      case leaf: FabricQuery.LeafQuery => leaf.queryType
      case _                           => query.children.map(of).fold(Read)(merge)
    }

  def global(fragment: Fragment): QueryType =
    fragment match {
      case _: Fragment.Init    => Read
      case leaf: Fragment.Leaf => merge(global(leaf.input), of(leaf.clauses))
      case apply: Fragment.Apply => merge(global(apply.input), global(apply.inner))
      case union: Fragment.Union => merge(global(union.lhs), global(union.rhs))
    }

  def local(fragment: Fragment): QueryType =
    fragment match {
      case _: Fragment.Init    => Read
      case leaf: Fragment.Leaf => of(leaf.clauses)
      case apply: Fragment.Apply => local(apply.inner)
      case union: Fragment.Union => merge(local(union.lhs), local(union.rhs))
    }

  def merge(a: QueryType, b: QueryType): QueryType =
    Seq(a, b).maxBy {
      case Read               => 1
      case ReadPlusUnresolved => 2
      case Write              => 3
    }
}
