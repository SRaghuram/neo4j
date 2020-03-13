/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.executor.Location

class SingleCatalogManager(
  databaseLookup: DatabaseLookup,
  fabricConfig: FabricConfig,
) extends CatalogManager {

  def currentCatalog(): Catalog =
    Catalog.create(fabricConfig, internalDatabaseNames)

  def locationOf(graph: Catalog.Graph, requireWritable: Boolean): Location = (graph, requireWritable) match {
    case (Catalog.InternalGraph(id, _, databaseName), _) =>
      new Location.Local(id, databaseName.name())

    case (Catalog.ExternalGraph(graphConfig), _) =>
      new Location.Remote(graphConfig.getId, externalRemoteUri(graphConfig.getUri), graphConfig.getDatabase)
  }

  private def externalRemoteUri(configUri: FabricConfig.RemoteUri): Location.RemoteUri =
    new Location.RemoteUri(configUri.getScheme, configUri.getAddresses, configUri.getQuery)

  private def internalDatabaseNames: Set[String] =
    databaseLookup.databaseIds.map(db => db.name())
}
