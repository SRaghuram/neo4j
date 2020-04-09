/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.util.Folded.FoldableOps
import com.neo4j.fabric.util.Folded.Stop
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.logical.plans.ProcedureDbmsAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.util.ASTNode


sealed trait QueryType

object QueryType {

  case object Read extends QueryType
  case object ReadPlusUnresolved extends QueryType
  case object Write extends QueryType

  // Java access helpers
  val READ: QueryType = Read
  val READ_PLUS_UNRESOLVED: QueryType = ReadPlusUnresolved
  val WRITE: QueryType = Write

  val default: QueryType = Read

  def of(ast: ASTNode): QueryType =
    ast.folded(default)(merge) {
      case _: UpdateClause                => Stop(Write)
      case c: CallClause                  => Stop(of(c))
      case _: SchemaCommand               => Stop(Write)
      case a: AdministrationCommand       => Stop(if (a.isReadOnly) Read else Write)
    }

  def of(ast: Seq[Clause]): QueryType =
    ast.map(of).fold(default)(merge)

  def of(ast: CallClause): QueryType = ast match {
    case _: UnresolvedCall => ReadPlusUnresolved
    case c: ResolvedCall =>
      c.signature.accessMode match {
        case ProcedureReadOnlyAccess(_) => Read
        case ProcedureDbmsAccess(_) => Read
        case _ => Write
      }
  }

  def recursive(fragment: Fragment): QueryType =
    fragment match {
      case _: Fragment.Init          => default
      case apply: Fragment.Apply     => merge(recursive(apply.input), recursive(apply.inner))
      case union: Fragment.Union     => merge(recursive(union.lhs), recursive(union.rhs))
      case leaf: Fragment.Leaf       => merge(recursive(leaf.input), leaf.queryType)
      case exec: Fragment.Exec       => merge(recursive(exec.input), exec.queryType)
      case command: Fragment.Command => command.queryType
    }

  def local(fragment: Fragment): QueryType =
    fragment match {
      case _: Fragment.Init          => default
      case apply: Fragment.Apply     => local(apply.inner)
      case union: Fragment.Union     => merge(local(union.lhs), local(union.rhs))
      case leaf: Fragment.Leaf       => leaf.queryType
      case exec: Fragment.Exec       => exec.queryType
      case command: Fragment.Command => command.queryType
    }

  def merge(a: QueryType, b: QueryType): QueryType =
    Seq(a, b).maxBy {
      case Read               => 1
      case ReadPlusUnresolved => 2
      case Write              => 3
    }
}
