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

  val default: QueryType = Read

  def of(ast: ASTNode): QueryType =
    ast.folded(default)(merge) {
      case _: UpdateClause   => Stop(Write)
      case _: UnresolvedCall => Stop(ReadPlusUnresolved)
      case c: CallClause     => Stop(if (c.containsNoUpdates) Read else Write)
    }

  def of(ast: Seq[ASTNode]): QueryType =
    ast.map(of).fold(Read)(merge)

  def recursive(fragment: Fragment): QueryType =
    fragment match {
      case _: Fragment.Init      => default
      case apply: Fragment.Apply => merge(recursive(apply.input), recursive(apply.inner))
      case union: Fragment.Union => merge(recursive(union.lhs), recursive(union.rhs))
      case leaf: Fragment.Leaf   => merge(recursive(leaf.input), leaf.queryType)
      case exec: Fragment.Exec   => merge(recursive(exec.input), exec.queryType)
    }

  def local(fragment: Fragment): QueryType =
    fragment match {
      case _: Fragment.Init      => default
      case apply: Fragment.Apply => local(apply.inner)
      case union: Fragment.Union => merge(local(union.lhs), local(union.rhs))
      case leaf: Fragment.Leaf   => leaf.queryType
      case exec: Fragment.Exec   => exec.queryType
    }

  def merge(a: QueryType, b: QueryType): QueryType =
    Seq(a, b).maxBy {
      case Read               => 1
      case ReadPlusUnresolved => 2
      case Write              => 3
    }
}
