/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast._
import com.neo4j.fabric.util.Folded._


sealed trait QueryType

object QueryType {

  case object Read extends QueryType
  case object ReadPlusUnresolved extends QueryType
  case object Write extends QueryType

  val READ: QueryType = Read
  val READ_PLUS_UNRESOLVED: QueryType = ReadPlusUnresolved
  val WRITE: QueryType = Write

  def of(query: Query): QueryType =
    query.folded(Read: QueryType)(merge) {
      case _: UpdateClause   => Stop(Write)
      case _: UnresolvedCall => Stop(ReadPlusUnresolved)
      case c: CallClause     => Stop(if (c.containsNoUpdates) Read else Write)
    }

  def of(query: FabricQuery): QueryType =
    query match {
      case leaf: FabricQuery.LeafQuery => leaf.queryType
      case _                           => query.children.map(of).fold(Read)(merge)
    }

  def merge(a: QueryType, b: QueryType): QueryType =
    Seq(a, b).maxBy {
      case Read               => 1
      case ReadPlusUnresolved => 2
      case Write              => 3
    }
}
