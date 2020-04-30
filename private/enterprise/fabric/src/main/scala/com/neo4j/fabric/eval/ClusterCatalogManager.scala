/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.fabric.config.FabricEnterpriseConfig
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.eval.DatabaseLookup
import org.neo4j.fabric.executor.FabricException
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId

import scala.collection.JavaConverters.seqAsJavaListConverter

class ClusterCatalogManager(
  databaseLookup: DatabaseLookup,
  leaderLookup: LeaderLookup,
  fabricConfig: FabricEnterpriseConfig,
) extends EnterpriseSingleCatalogManager(databaseLookup, fabricConfig) {

  override def locationOf(graph: Catalog.Graph, requireWritable: Boolean): Location = (graph, requireWritable) match {

    case (Catalog.InternalGraph(id, uuid, _, databaseName), true) =>
      determineLeader(databaseName) match {
        case LeaderIsLocal           => new Location.Local(id, uuid, databaseName.name())
        case LeaderIsRemote(address) => new Location.Remote.Internal(id, uuid, internalRemoteUri(address), databaseName.name())
      }

    case _ =>
      super.locationOf(graph, requireWritable)
  }

  private def internalRemoteUri(socketAddress: SocketAddress): Location.RemoteUri =
    new Location.RemoteUri("bolt", Seq(socketAddress).asJava, null)

  private sealed trait LeaderStatus
  private final case object LeaderIsLocal extends LeaderStatus
  private final case class LeaderIsRemote(socketAddress: SocketAddress) extends LeaderStatus

  private def determineLeader(databaseName: NormalizedDatabaseName): LeaderStatus = {
    val dbId = databaseId(databaseName)
    val myId = leaderLookup.memberId
    leaderLookup.leaderId(dbId) match {
      case Some(`myId`) => LeaderIsLocal
      case Some(_)      => LeaderIsRemote(leaderBoltAddress(dbId))
      case None         => routingFailed("Unable to get bolt address for database {}", databaseName.name())
    }
  }

  private def databaseId(databaseName: NormalizedDatabaseName) =
    databaseLookup.databaseId(databaseName)
      .getOrElse(routingFailed("Unable to get database id for database {}", databaseName.name()))

  private def leaderBoltAddress(databaseId: NamedDatabaseId) =
    leaderLookup.leaderBoltAddress(databaseId)
      .getOrElse(routingFailed("Unable to get bolt address of LEADER for database {}", databaseId.name()))

  private def routingFailed(msg: String, dbName: String): Nothing =
    throw new FabricException(Status.Fabric.Routing, msg, dbName)
}
