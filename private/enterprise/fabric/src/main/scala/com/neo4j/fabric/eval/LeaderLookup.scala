/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.causalclustering.discovery.RoleInfo
import com.neo4j.causalclustering.discovery.TopologyService
import com.neo4j.causalclustering.identity.MemberId
import com.neo4j.causalclustering.routing.load_balancing.LeaderService
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.kernel.database.NamedDatabaseId

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.compat.java8.OptionConverters.RichOptionalGeneric

trait LeaderLookup {

  def memberId: MemberId
  def leaderId(databaseId: NamedDatabaseId): Option[MemberId]
  def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress]
}

object LeaderLookup {

  class Core(
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

  class ReadReplica(
    topologyService: TopologyService,
  ) extends LeaderLookup {

    def memberId: MemberId =
      topologyService.memberId()

    def leaderId(databaseId: NamedDatabaseId): Option[MemberId] =
      topologyService.allCoreServers().keySet().asScala
        .find(memberId => topologyService.lookupRole(databaseId, memberId) == RoleInfo.LEADER)

    def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress] =
      for {
        leader <- leaderId(databaseId)
        members = topologyService.coreTopologyForDatabase(databaseId).members().asScala
        leaderInfo <- members.get(leader)
      } yield leaderInfo.connectors().clientBoltAddress()
  }
}
