/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.configuration.FabricEnterpriseConfig
import com.neo4j.fabric.eval.ClusterCatalogManager.LeaderIsLocal
import com.neo4j.fabric.eval.ClusterCatalogManager.LeaderIsRemote
import com.neo4j.fabric.eval.ClusterCatalogManager.LeaderNotFound
import com.neo4j.fabric.eval.ClusterCatalogManager.LeaderStatus
import com.neo4j.fabric.eval.ClusterCatalogManager.RoutingDisabled
import com.neo4j.fabric.eval.ClusterCatalogManager.RoutingDisallowed
import com.neo4j.fabric.eval.ClusterCatalogManager.RoutingPossible
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.eval.DatabaseLookup
import org.neo4j.fabric.executor.FabricException
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId

import scala.collection.JavaConverters.seqAsJavaListConverter

object ClusterCatalogManager {
  sealed trait RoutingMode
  final case object RoutingPossible extends RoutingMode
  final case object RoutingDisallowed extends RoutingMode
  final case object RoutingDisabled extends RoutingMode

  private sealed trait LeaderStatus
  private final case object LeaderIsLocal extends LeaderStatus
  private final case object LeaderIsRemote extends LeaderStatus
  private final case object LeaderNotFound extends LeaderStatus
}

class ClusterCatalogManager(
  databaseLookup: DatabaseLookup,
  databaseManagementService: DatabaseManagementService,
  leaderLookup: LeaderLookup,
  fabricConfig: FabricEnterpriseConfig,
  routingEnabled: Boolean,
) extends EnterpriseSingleCatalogManager(databaseLookup, databaseManagementService, fabricConfig) {

  override def locationOf(graph: Catalog.Graph, requireWritable: Boolean, canRoute: Boolean): Location = {
    val routingMode = (canRoute, routingEnabled) match {
      case (false, _)    => RoutingDisallowed
      case (true, false) => RoutingDisabled
      case (true, true)  => RoutingPossible
    }

    (graph, requireWritable) match {

      case (Catalog.InternalGraph(id, uuid, _, databaseName), true) =>
        val dbId = databaseId(databaseName)

        (determineLeader(dbId), routingMode) match {
          case (LeaderIsLocal, _) =>
            new Location.Local(id, uuid, databaseName.name())

          case (_, RoutingDisallowed) =>
            // If routing is disallowed by the client we should attempt executing locally, regardless
            // The rationale being that if topology info is somehow broken, admin should be able to do local things using bolt://
            new Location.Local(id, uuid, databaseName.name())

          case (LeaderIsRemote, RoutingPossible) =>
            new Location.Remote.Internal(id, uuid, internalRemoteUri(leaderBoltAddress(dbId)), databaseName.name())

          case (LeaderIsRemote, RoutingDisabled) =>
            routingDisabledError(databaseName.name())

          case (LeaderNotFound, _) =>
            noLeaderAddressFailure(databaseName.name())
        }

      case _ =>
        super.locationOf(graph, requireWritable, canRoute)
    }
  }

  private def internalRemoteUri(socketAddress: SocketAddress): Location.RemoteUri =
    new Location.RemoteUri("bolt", Seq(socketAddress).asJava, null)

  private def databaseId(databaseName: NormalizedDatabaseName) =
    databaseLookup.databaseId(databaseName)
                  .getOrElse(noDatabaseIdFailure(databaseName.name()))

  private def determineLeader(databaseId: NamedDatabaseId): LeaderStatus = {
    val myId = leaderLookup.serverId
    leaderLookup.leaderId(databaseId) match {
      case Some(`myId`) => LeaderIsLocal
      case Some(_)      => LeaderIsRemote
      case None         => LeaderNotFound
    }
  }

  private def leaderBoltAddress(databaseId: NamedDatabaseId) =
    leaderLookup.leaderBoltAddress(databaseId)
                .getOrElse(noLeaderAddressFailure(databaseId.name()))

  private def routingDisabledError(dbName: String): Nothing = {
    val setting = GraphDatabaseSettings.routing_enabled.name()
    throw new FabricException(
      Status.Cluster.NotALeader,
      s"""Unable to route write operation to leader for database '%s'. Server-side routing is disabled.
         |Either connect to the database directly using the driver (or interactively with the :use command),
         |or enable server-side routing by setting `$setting=true`""".stripMargin,
      dbName)
  }

  private def noLeaderAddressFailure(dbName: String): Nothing =
    throw new FabricException(
      Status.Cluster.Routing,
      "Unable to get bolt address of LEADER for database '%s'",
      dbName)

  private def noDatabaseIdFailure(dbName: String): Nothing =
    throw new FabricException(
      Status.Cluster.Routing,
      "Unable to get database id for database '%s'",
      dbName)
}
