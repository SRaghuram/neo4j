/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.time.Duration
import java.util
import java.util.UUID

import com.neo4j.causalclustering.identity.MemberId
import com.neo4j.fabric.config.FabricEnterpriseConfig
import com.neo4j.fabric.config.FabricEnterpriseConfig.GlobalDriverConfig
import com.neo4j.fabric.config.FabricEnterpriseConfig.Graph
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.config.FabricConfig
import org.neo4j.fabric.eval.Catalog.ExternalGraph
import org.neo4j.fabric.eval.Catalog.InternalGraph
import org.neo4j.fabric.eval.DatabaseLookup
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.seqAsJavaListConverter

class ClusterCatalogManagerTest extends FabricTest {

  private val mega0 = new Graph(0L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:1111"), "neo4j0", new NormalizedGraphName("extA"), null)
  private val mega1 = new Graph(1L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:2222"), "neo4j1", null, null)
  private val mega2 = new Graph(2L, FabricEnterpriseConfig.RemoteUri.create("bolt://mega:3333"), "neo4j2", new NormalizedGraphName("extB"), null)

  private val config = new FabricEnterpriseConfig(
    new FabricEnterpriseConfig.Database(new NormalizedDatabaseName("mega"), util.Set.of(mega0, mega1, mega2)),
    util.List.of(), Duration.ZERO, Duration.ZERO,
    new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 0, null),
    new FabricConfig.DataStream(300, 1000, 50, 10)
  )

  private val intAUuid = UUID.randomUUID()
  private val intBUuid = UUID.randomUUID()
  private val megaUuid = UUID.randomUUID()

  private val intA = DatabaseIdFactory.from("intA", intAUuid)
  private val intB = DatabaseIdFactory.from("intB", intBUuid)
  private val mega = DatabaseIdFactory.from("mega", megaUuid)

  private val internalDbs = Set(intA, intB, mega)

  private val myId = new MemberId(UUID.randomUUID())
  private val remoteId = new MemberId(UUID.randomUUID())
  private val remoteAddress = new SocketAddress("remote", 1234)
  private val remoteAddresses = Map(remoteId -> remoteAddress)

  def createManager(leaderMapping: Map[NamedDatabaseId, MemberId]) = new ClusterCatalogManager(
    databaseLookup = new DatabaseLookup {
      def databaseIds: Set[NamedDatabaseId] = internalDbs
      def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] = internalDbs.find(_.name() == databaseName.name())
    },
    leaderLookup = new LeaderLookup {
      def memberId: MemberId = myId
      def leaderId(databaseId: NamedDatabaseId): Option[MemberId] = leaderMapping.get(databaseId)
      def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress] = leaderId(databaseId).flatMap(remoteAddresses.get)
    },
    fabricConfig = config,
  )

  "catalog resolution" in {

    val catalog = createManager(Map.empty).currentCatalog()

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

    "when all leaders are local" - {

      val manager = createManager(Map(
        intA -> myId,
        intB -> myId,
        mega -> myId,
      ))

      val catalog = manager.currentCatalog()

      "and writable required" in {

        val writable = true

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

      "and writable not required" in {

        val writable = false

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
    }

    "when some leaders are remote" - {

      val manager = createManager(Map(
        intA -> remoteId,
        intB -> remoteId,
        mega -> myId,
      ))
      val catalog = manager.currentCatalog()

      "and writable required" in {

        val writable = true

        manager.locationOf(catalog.resolve(CatalogName("mega", "extA")), writable)
          .shouldEqual(new Location.Remote.External(0, uuid(0), remoteUri(mega0.getUri), "neo4j0"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1))), writable)
          .shouldEqual(new Location.Remote.External(1, uuid(1), remoteUri(mega1.getUri), "neo4j1"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "extB")), writable)
          .shouldEqual(new Location.Remote.External(2, uuid(2), remoteUri(mega2.getUri), "neo4j2"))

        manager.locationOf(catalog.resolve(CatalogName("intA")), writable)
          .shouldEqual(new Location.Remote.Internal(3, intAUuid, remoteUri("bolt", remoteAddress), "inta"))

        manager.locationOf(catalog.resolve(CatalogName("intB")), writable)
          .shouldEqual(new Location.Remote.Internal(4, intBUuid, remoteUri("bolt", remoteAddress), "intb"))

        manager.locationOf(catalog.resolve(CatalogName("mega")), writable)
          .shouldEqual(new Location.Local(5, megaUuid, "mega"))
      }

      "and writable not required" in {

        val writable = false

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
    }
  }

  private def external(graph: FabricEnterpriseConfig.Graph) = ExternalGraph(graph.getId, Option(graph.getName).map(_.name()), uuid(graph.getId))
  private def internal(id: Long, uuid: UUID, name: String) = InternalGraph(id, uuid, new NormalizedGraphName(name), new NormalizedDatabaseName(name))

  private def remoteUri(uri: FabricEnterpriseConfig.RemoteUri): Location.RemoteUri = new Location.RemoteUri(uri.getScheme, uri.getAddresses, uri.getQuery)
  private def remoteUri(scheme: String, address: SocketAddress): Location.RemoteUri = new Location.RemoteUri(scheme, Seq(address).asJava, null)

  private def uuid(graphId: Long): UUID = new UUID(graphId, 0);
}
