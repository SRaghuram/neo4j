/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.util.UUID

import com.neo4j.fabric.config.FabricEnterpriseConfig
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.eval.Catalog.ExternalGraph
import org.neo4j.fabric.eval.CommunityCatalogManager
import org.neo4j.fabric.eval.DatabaseLookup
import org.neo4j.fabric.executor.Location
import org.neo4j.fabric.executor.Location.Remote

import scala.collection.JavaConverters.asScalaSetConverter

class EnterpriseSingleCatalogManager(
                            databaseLookup: DatabaseLookup,
                            fabricConfig: FabricEnterpriseConfig,
) extends CommunityCatalogManager(databaseLookup) {

  private val graphsById: Map[Long, FabricEnterpriseConfig.Graph] =
    Option(fabricConfig.getDatabase)
      .map(database => database.getGraphs.asScala)
      .map(graphs => graphs.map(graph => graph.getId -> graph))
      .map(_.toMap).getOrElse(Map.empty)

  override def currentCatalog(): Catalog = {
    if (fabricConfig.getDatabase == null) {
      super.currentCatalog()
    } else {
      val fabricNamespace = fabricConfig.getDatabase.getName.name()
      val fabricGraphs = fabricConfig.getDatabase.getGraphs.asScala.toSet

      val external = asExternal(fabricGraphs)
      val maxId = external
        .collect { case g: Catalog.Graph => g.id }
        .reduceOption((a, b) => Math.max(a, b)).getOrElse(-1L)
      val internal = asInternal(maxId + 1)

      Catalog.create(internal, external, Some(fabricNamespace))
    }
  }

  private def asExternal(graphs: Set[FabricEnterpriseConfig.Graph]) = for {
    graph <- graphs.toSeq
  } yield ExternalGraph(graph.getId, Option(graph.getName).map(_.name) , new UUID(graph.getId, 0))

  override def locationOf(graph: Catalog.Graph, requireWritable: Boolean): Location = graph match {
    case Catalog.ExternalGraph(id, _, uuid) =>
      val graph = graphsById(id)
      new Remote.External(id, uuid, externalRemoteUri(graph.getUri), Option(graph.getDatabase).orNull)
    case _ => super.locationOf(graph, requireWritable )
  }

  private def externalRemoteUri(configUri: FabricEnterpriseConfig.RemoteUri): Location.RemoteUri =
    new Location.RemoteUri(configUri.getScheme, configUri.getAddresses, configUri.getQuery)
}
