/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.util.function.Supplier

import com.neo4j.causalclustering.discovery.TopologyService
import com.neo4j.causalclustering.routing.load_balancing.LeaderService
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.executor.FabricException
import com.neo4j.fabric.executor.Location
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.compat.java8.OptionConverters.RichOptionalGeneric

class ClusterCatalogManager(
  databaseManager: Supplier[DatabaseManager[DatabaseContext]],
  topologyService: TopologyService,
  leaderService: LeaderService,
  fabricConfig: FabricConfig,
) extends SingleCatalogManager(databaseManager, fabricConfig) {

  override def locationOf(graph: Catalog.Graph, requireWritable: Boolean): Location = (graph, requireWritable) match {
    case (Catalog.InternalGraph(id, _, databaseName), false) =>
      new Location.Local(id, databaseName.name())

    case (Catalog.InternalGraph(id, _, databaseName), true) =>
      determineLeader(databaseName) match {
        case LeaderIsLocal           => new Location.Local(id, databaseName.name())
        case LeaderIsRemote(address) => new Location.Remote(id, internalRemoteUri(address), databaseName.name())
        case LeaderNotFound          => throw new Exception
      }

    case (Catalog.ExternalGraph(graphConfig), _) =>
      new Location.Remote(graphConfig.getId, externalRemoteUri(graphConfig.getUri), graphConfig.getDatabase)
  }

  private def internalRemoteUri(socketAddress: SocketAddress): Location.RemoteUri =
    new Location.RemoteUri("bolt", Seq(socketAddress).asJava, null)

  private def externalRemoteUri(configUri: FabricConfig.RemoteUri): Location.RemoteUri =
    new Location.RemoteUri(configUri.getScheme, configUri.getAddresses, configUri.getQuery)

  private sealed trait LeaderStatus
  private final case object LeaderIsLocal extends LeaderStatus
  private final case class LeaderIsRemote(socketAddress: SocketAddress) extends LeaderStatus
  private final case object LeaderNotFound extends LeaderStatus

  private def determineLeader(databaseName: NormalizedDatabaseName): LeaderStatus = {
    val dbId = databaseId(databaseName)
    val myId = topologyService.memberId()
    leaderService.getLeaderId(dbId).asScala match {
      case Some(`myId`) => LeaderIsLocal
      case Some(_)      => LeaderIsRemote(leaderBoltAddress(dbId))
      case None         => LeaderNotFound
    }
  }

  private def leaderBoltAddress(databaseId: NamedDatabaseId) =
    leaderService.getLeaderBoltAddress(databaseId)
      .orElseThrow(() => new FabricException(Status.Fabric.RemoteExecutionFailed, "Unable to get bolt address for database {}", databaseId))

  private def databaseId(databaseName: NormalizedDatabaseName) =
    databaseManager.get().databaseIdRepository().getByName(databaseName)
      .orElseThrow(() => new FabricException(Status.Fabric.RemoteExecutionFailed, "Unable to get database id for database {}", databaseName))


}
