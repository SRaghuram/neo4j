/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.executor.Location
import com.neo4j.fabric.executor.Location.Remote

class SingleCatalogManager(
  databaseLookup: DatabaseLookup,
  fabricConfig: FabricConfig,
) extends CatalogManager {

  def currentCatalog(): Catalog =
    Catalog.create(fabricConfig, databaseLookup.databaseIds)

  def locationOf(graph: Catalog.Graph, requireWritable: Boolean): Location = (graph, requireWritable) match {
    case (Catalog.InternalGraph(id, uuid, _, databaseName), _) =>
      new Location.Local(id, uuid, databaseName.name())

    case (Catalog.ExternalGraph(graphConfig, uuid), _) =>
      new Remote.External(graphConfig.getId, uuid, externalRemoteUri(graphConfig.getUri), graphConfig.getDatabase)
  }

  private def externalRemoteUri(configUri: FabricConfig.RemoteUri): Location.RemoteUri =
    new Location.RemoteUri(configUri.getScheme, configUri.getAddresses, configUri.getQuery)
}
