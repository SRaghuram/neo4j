/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.planning.FabricPlan.DebugOptions
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.util.ObfuscationMetadata

case class FabricPlan(
  query: Fragment,
  queryType: QueryType,
  executionType: FabricPlan.ExecutionType,
  queryString: String,
  debugOptions: DebugOptions,
  obfuscationMetadata: ObfuscationMetadata,
  inFabricContext: Boolean
)

object FabricPlan {

  sealed trait ExecutionType
  case object Execute extends ExecutionType
  case object Explain extends ExecutionType
  case object Profile extends ExecutionType
  val EXECUTE: ExecutionType = Execute
  val EXPLAIN: ExecutionType = Explain
  val PROFILE: ExecutionType = Profile

  object DebugOptions {
    def from(debugOptions: Set[String]): DebugOptions = DebugOptions(
      logPlan = debugOptions.contains("fabriclogplan"),
      logRecords = debugOptions.contains("fabriclogrecords"),
    )
  }

  case class DebugOptions(
    logPlan: Boolean,
    logRecords: Boolean,
  )
}

sealed trait FabricQuery

object FabricQuery {

  final case class LocalQuery(
    query: FullyParsedQuery,
    queryType: QueryType,
  ) extends FabricQuery

  final case class RemoteQuery(
    query: String,
    queryType: QueryType,
  ) extends FabricQuery


}
