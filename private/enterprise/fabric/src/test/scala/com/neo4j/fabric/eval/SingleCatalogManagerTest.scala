/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.time.Duration
import java.util
import java.util.UUID

import com.neo4j.fabric.config.FabricEnterpriseConfig
import com.neo4j.fabric.config.FabricEnterpriseConfig.GlobalDriverConfig
import com.neo4j.fabric.config.FabricEnterpriseConfig.Graph
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.config.FabricConfig
import org.neo4j.fabric.eval.Catalog.ExternalGraph
import org.neo4j.fabric.eval.Catalog.InternalGraph
import org.neo4j.fabric.eval.DatabaseLookup
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.values.storable.Values
import org.scalatest.mockito.MockitoSugar

class SingleCatalogManagerTest extends FabricTest {

  private val mega0 = new Graph(0L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:1111"), "neo4j0", new NormalizedGraphName("extA"), null)
  private val mega1 = new Graph(1L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:2222"), "neo4j1", null, null)
  private val mega2 = new Graph(2L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:3333"), "neo4j2", new NormalizedGraphName("extB"), null)

  private val intAUuid = UUID.randomUUID()
  private val intBUuid = UUID.randomUUID()
  private val megaUuid = UUID.randomUUID()

  private val config = new FabricEnterpriseConfig(
    new FabricEnterpriseConfig.Database(new NormalizedDatabaseName("mega"), util.Set.of(mega0, mega1, mega2)),
    util.List.of(), Duration.ZERO, Duration.ZERO,
    new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 0, null),
    new FabricConfig.DataStream(300, 1000, 50, 10)
  )

  private val internalDbs = Set(
    DatabaseIdFactory.from("intA", intAUuid),
    DatabaseIdFactory.from("intB", intBUuid),
    DatabaseIdFactory.from("mega", megaUuid),
  )

  private val databaseLookup = new DatabaseLookup {
    override def databaseIds: Set[NamedDatabaseId] =
      internalDbs

    override def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] =
      internalDbs.find(_.name() == databaseName.name())
  }
  
  private val databaseManagementService = MockitoSugar.mock[DatabaseManagementService]

  private val managerWithFabricDatabase = new EnterpriseSingleCatalogManager(
    databaseLookup = databaseLookup,
    databaseManagementService,
    fabricConfig = config,
  )

  private val managerWithoutFabricDatabase = new EnterpriseSingleCatalogManager(
    databaseLookup,
    databaseManagementService,
    fabricConfig = new FabricEnterpriseConfig(null, util.List.of(), null, null, null, null),
  )

  "catalog resolution" in {

    val catalog = managerWithFabricDatabase.currentCatalog()
    catalog.resolve(CatalogName("mega", "extA"))
      .shouldEqual(external(mega0))

    catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1)))
      .shouldEqual(external(mega1))

    catalog.resolve(CatalogName("mega", "extB"))
      .shouldEqual(external(mega2))

    catalog.resolve(CatalogName("intA"))
      .shouldEqual(internal(3, intAUuid, "intA"))

    catalog.resolve(CatalogName("intB"))
      .shouldEqual(internal(4, intBUuid, "intB"))

    catalog.resolve(CatalogName("mega"))
      .shouldEqual(internal(5, megaUuid, "mega"))
  }

  "location resolution" - {

    val manager = managerWithFabricDatabase
    val catalog = manager.currentCatalog()

    def resolveAll(writable: Boolean) = {
      manager.locationOf(catalog.resolve(CatalogName("mega", "extA")), writable)
        .shouldEqual(new Location.Remote.External(0, uuid(0), remoteUri(mega0.getUri), "neo4j0"))

      manager.locationOf(catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1))), writable)
        .shouldEqual(new Location.Remote.External(1, uuid(1), remoteUri(mega1.getUri), "neo4j1"))

      manager.locationOf(catalog.resolve(CatalogName("mega", "extB")), writable)
        .shouldEqual(new Location.Remote.External(2, uuid(2), remoteUri(mega2.getUri), "neo4j2"))

      manager.locationOf(catalog.resolve(CatalogName("intA")), writable)
        .shouldEqual(new Location.Local(3, intAUuid, "inta"))

      manager.locationOf(catalog.resolve(CatalogName("intB")), writable)
        .shouldEqual(new Location.Local(4, intBUuid, "intb"))

      manager.locationOf(catalog.resolve(CatalogName("mega")), writable)
        .shouldEqual(new Location.Local(5, megaUuid, "mega"))
    }

    "when writable required" in {
      resolveAll(true)
    }

    "when writable not required" in {
      resolveAll(false)
    }
  }

  "location resolution without fabric database" in {

    val manager = managerWithoutFabricDatabase
    val catalog = manager.currentCatalog()

    manager.locationOf(catalog.resolve(CatalogName("intA")), false)
      .shouldEqual(new Location.Local(0, intAUuid, "inta"))

    manager.locationOf(catalog.resolve(CatalogName("intB")), false)
      .shouldEqual(new Location.Local(1, intBUuid, "intb"))
  }

  private def external(graph: FabricEnterpriseConfig.Graph) = ExternalGraph(graph.getId, Option(graph.getName).map(_.name()), uuid(graph.getId))
  private def internal(id: Long, uuid:UUID, name: String) = InternalGraph(id, uuid, new NormalizedGraphName(name), new NormalizedDatabaseName(name))
  private def remoteUri(uri: FabricEnterpriseConfig.RemoteUri): Location.RemoteUri = new Location.RemoteUri(uri.getScheme, uri.getAddresses, uri.getQuery)
  private def uuid(id: Long) = new UUID(id, 0)
}
