/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.causalclustering.discovery.ConnectorAddresses
import com.neo4j.causalclustering.discovery.RoleInfo
import com.neo4j.causalclustering.discovery.TopologyService
import com.neo4j.causalclustering.routing.load_balancing.LeaderService
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.identity.ServerId
import org.neo4j.kernel.database.NamedDatabaseId

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.compat.java8.OptionConverters.RichOptionalGeneric

trait LeaderLookup {

  def serverId: ServerId
  def leaderId(databaseId: NamedDatabaseId): Option[ServerId]
  def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress]
}

object LeaderLookup {

  class Core(
    topologyService: TopologyService,
    leaderService: LeaderService,
  ) extends LeaderLookup {

    def serverId: ServerId =
      topologyService.serverId()

    def leaderId(databaseId: NamedDatabaseId): Option[ServerId] =
      leaderService.getLeaderId(databaseId).asScala

    def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress] =
      LeaderLookup.leaderBoltAddress(topologyService, leaderId(databaseId), databaseId)
  }

  class ReadReplica(
    topologyService: TopologyService,
  ) extends LeaderLookup {

    def serverId: ServerId =
      topologyService.serverId()

    def leaderId(databaseId: NamedDatabaseId): Option[ServerId] =
      topologyService.allCoreServers().keySet().asScala
        .find(memberId => topologyService.lookupRole(databaseId, memberId) == RoleInfo.LEADER)

    def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress] =
      LeaderLookup.leaderBoltAddress(topologyService, leaderId(databaseId), databaseId)
  }

  private def leaderBoltAddress(topologyService: TopologyService, leaderId: Option[ServerId], databaseId: NamedDatabaseId): Option[SocketAddress] = {
    for {
      leader <- leaderId
      members = topologyService.coreTopologyForDatabase(databaseId).servers().asScala
      leaderInfo <- members.get(leader)
      address <- getAddress(leaderInfo.connectors())
    } yield address
  }

  private def getAddress(connectors: ConnectorAddresses): Option[SocketAddress] = {
    val address = connectors.intraClusterBoltAddress()
    if (address.isPresent) {
      Some(address.get())
    } else {
      None
    }
  }
}
