/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.causalclustering.discovery.TopologyService
import com.neo4j.causalclustering.identity.MemberId
import com.neo4j.causalclustering.routing.load_balancing.LeaderService
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.kernel.database.NamedDatabaseId

import scala.compat.java8.OptionConverters.RichOptionalGeneric

trait LeaderLookup {

  def memberId: MemberId
  def leaderId(databaseId: NamedDatabaseId): Option[MemberId]
  def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress]
}

object LeaderLookup {

  class Default(
    topologyService: TopologyService,
    leaderService: LeaderService,
  ) extends LeaderLookup {

    def memberId: MemberId =
      topologyService.memberId()

    def leaderId(databaseId: NamedDatabaseId): Option[MemberId] =
      leaderService.getLeaderId(databaseId).asScala

    def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress] =
      leaderService.getLeaderBoltAddress(databaseId).asScala
  }
}
